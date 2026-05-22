"""Repository abstraction for Notion page CRUD operations.

A ``Protocol`` defines the surface used by ``SyncService``. The production
implementation calls the Notion REST API directly with ``requests``. The
fake implementation keeps state in-memory for tests.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional, Protocol

import requests

from notion_export.exceptions import NotionApiError, ParentPageNotFoundError

logger = logging.getLogger(__name__)

_NOTION_API_BASE = "https://api.notion.com"
_BACKOFF_SECONDS = (1.0, 2.0, 4.0)
_BLOCKS_PER_REQUEST = 100


class NotionPageRepository(Protocol):
    def fetch_parent(self, page_id: str) -> dict: ...

    def search_children_by_title_prefix(
        self, parent_id: str, prefix: str
    ) -> list[dict]: ...

    def create_page(self, parent_id: str, title: str, blocks: list[dict]) -> str: ...

    def archive_page(self, page_id: str) -> None: ...

    def append_blocks(self, page_id: str, blocks: list[dict]) -> None: ...


class RestNotionPageRepository:
    """Notion REST API client implementation."""

    def __init__(
        self,
        api_token: str,
        *,
        timeout_seconds: int = 30,
        api_version: str = "2022-06-28",
        throttle_seconds: float = 0.4,
        http_session: Optional[requests.Session] = None,
    ) -> None:
        self._token = api_token
        self._timeout = timeout_seconds
        self._api_version = api_version
        self._throttle = throttle_seconds
        self._session = http_session or requests.Session()

    # -- public API -----------------------------------------------------

    def fetch_parent(self, page_id: str) -> dict:
        resp = self._request("GET", f"/v1/pages/{page_id}")
        return resp

    def search_children_by_title_prefix(
        self, parent_id: str, prefix: str
    ) -> list[dict]:
        body = {
            "query": prefix,
            "filter": {"property": "object", "value": "page"},
        }
        normalized_parent = _normalize_id(parent_id)
        out: list[dict] = []
        cursor: Optional[str] = None
        while True:
            payload = dict(body)
            if cursor:
                payload["start_cursor"] = cursor
            resp = self._request("POST", "/v1/search", json_body=payload)
            for hit in resp.get("results", []):
                parent = hit.get("parent") or {}
                if _normalize_id(parent.get("page_id", "")) != normalized_parent:
                    continue
                title = _extract_title(hit)
                if title.startswith(prefix):
                    out.append({"id": hit["id"], "title": title})
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
            if not cursor:
                break
        return out

    def create_page(self, parent_id: str, title: str, blocks: list[dict]) -> str:
        payload = {
            "parent": {"page_id": parent_id},
            "properties": {
                "title": {"title": [{"type": "text", "text": {"content": title}}]}
            },
            "children": blocks[:_BLOCKS_PER_REQUEST],
        }
        resp = self._request("POST", "/v1/pages", json_body=payload)
        page_id = resp.get("id")
        if not isinstance(page_id, str):
            raise NotionApiError(0, f"unexpected create_page response: {resp!r}")
        return page_id

    def archive_page(self, page_id: str) -> None:
        self._request(
            "PATCH",
            f"/v1/pages/{page_id}",
            json_body={"archived": True},
        )

    def append_blocks(self, page_id: str, blocks: list[dict]) -> None:
        for i in range(0, len(blocks), _BLOCKS_PER_REQUEST):
            chunk = blocks[i : i + _BLOCKS_PER_REQUEST]
            self._request(
                "PATCH",
                f"/v1/blocks/{page_id}/children",
                json_body={"children": chunk},
            )
            if self._throttle > 0:
                time.sleep(self._throttle)

    # -- internals ------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
    ) -> dict:
        url = _NOTION_API_BASE + path
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Notion-Version": self._api_version,
            "Content-Type": "application/json",
        }
        last_status = 0
        last_body = ""
        for attempt, backoff in enumerate(_BACKOFF_SECONDS):
            resp = self._session.request(
                method,
                url,
                json=json_body,
                headers=headers,
                timeout=self._timeout,
            )
            status = getattr(resp, "status_code", 0)
            if 200 <= status < 300:
                if not resp.text:
                    return {}
                try:
                    return resp.json()
                except Exception:
                    return {}
            if status == 404 and method == "GET" and path.startswith("/v1/pages/"):
                raise ParentPageNotFoundError(f"Notion page not found: {path}")
            last_status = status
            last_body = _safe_body(resp)
            if 500 <= status < 600 and attempt < len(_BACKOFF_SECONDS) - 1:
                time.sleep(backoff)
                continue
            raise NotionApiError(status, last_body)
        raise NotionApiError(last_status, last_body)


# ---------------------------------------------------------------------------
# Fake (test) implementation
# ---------------------------------------------------------------------------


class FakeNotionPageRepository:
    """In-memory fake. Tracks pages by id, supports archiving and appends."""

    def __init__(self, *, parent_pages: Optional[dict[str, dict]] = None) -> None:
        # parent_pages: {parent_id: {...}} - presence means "page exists".
        self._parents = parent_pages or {}
        self.pages: dict[str, dict] = {}
        self.append_calls: list[tuple[str, list[dict]]] = []
        self.archived_ids: list[str] = []
        self._next_id = 0

    def fetch_parent(self, page_id: str) -> dict:
        if page_id not in self._parents:
            raise ParentPageNotFoundError(page_id)
        return self._parents[page_id]

    def add_parent(self, page_id: str) -> None:
        self._parents[page_id] = {"id": page_id, "object": "page"}

    def search_children_by_title_prefix(
        self, parent_id: str, prefix: str
    ) -> list[dict]:
        hits: list[dict] = []
        for pid, page in self.pages.items():
            if page.get("archived"):
                continue
            if page.get("parent_id") != parent_id:
                continue
            if str(page.get("title", "")).startswith(prefix):
                hits.append({"id": pid, "title": page["title"]})
        return hits

    def create_page(self, parent_id: str, title: str, blocks: list[dict]) -> str:
        self._next_id += 1
        pid = f"fake-{self._next_id:04d}"
        self.pages[pid] = {
            "parent_id": parent_id,
            "title": title,
            "blocks": list(blocks[:_BLOCKS_PER_REQUEST]),
            "archived": False,
        }
        return pid

    def archive_page(self, page_id: str) -> None:
        if page_id not in self.pages:
            raise NotionApiError(404, page_id)
        self.pages[page_id]["archived"] = True
        self.archived_ids.append(page_id)

    def append_blocks(self, page_id: str, blocks: list[dict]) -> None:
        if page_id not in self.pages:
            raise NotionApiError(404, page_id)
        self.append_calls.append((page_id, list(blocks)))
        self.pages[page_id]["blocks"].extend(blocks)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_body(resp: Any) -> str:
    try:
        return resp.text[:300]
    except Exception:  # pragma: no cover - defensive
        return "<no body>"


def _normalize_id(page_id: str) -> str:
    """Notion IDs may appear with or without hyphens; compare without."""
    return (page_id or "").replace("-", "")


def _extract_title(page: dict) -> str:
    props = page.get("properties") or {}
    for prop in props.values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            titles = prop.get("title") or []
            return "".join(t.get("plain_text", "") for t in titles)
    return ""


__all__ = [
    "NotionPageRepository",
    "RestNotionPageRepository",
    "FakeNotionPageRepository",
]
