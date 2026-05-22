"""Notion File Upload API wrapper.

Notion's File Upload API is a two-step protocol:

1. ``POST /v1/file_uploads`` (JSON body empty) -> returns ``{"id": ..., "upload_url": ...}``
2. ``POST {upload_url}`` (multipart, ``file`` part) -> finalises the upload.

The returned ``id`` is what subsequent block payloads reference via
``{"type": "file_upload", "file_upload": {"id": <id>}}``.

For non-fatal failure we log a WARN and return ``None`` so the caller can
skip the image block instead of aborting the whole sync.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

_NOTION_FILE_UPLOAD_URL = "https://api.notion.com/v1/file_uploads"

_BACKOFF_SECONDS = (1.0, 2.0, 4.0)


class ImageUploader:
    """Upload images to Notion File Upload API."""

    def __init__(
        self,
        api_token: str,
        *,
        timeout_seconds: int = 30,
        api_version: str = "2022-06-28",
        http_session: Optional[requests.Session] = None,
    ) -> None:
        self._api_token = api_token
        self._timeout = timeout_seconds
        self._api_version = api_version
        self._session = http_session or requests.Session()

    def upload(self, path: Path) -> Optional[str]:
        """Upload ``path`` and return the file_upload id. ``None`` on failure."""
        if not self._api_token:
            logger.warning("NOTION_API_TOKEN unset; image upload skipped")
            return None
        if not path.exists():
            logger.warning("Image not found for upload: %s", path)
            return None

        try:
            file_id, upload_url = self._create_upload_object()
        except _UploadFailure as exc:
            logger.warning("Failed to upload image: %s", exc)
            return None

        try:
            self._send_file_body(upload_url, path)
        except _UploadFailure as exc:
            logger.warning("Failed to upload image: %s", exc)
            return None
        return file_id

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_token}",
            "Notion-Version": self._api_version,
        }

    def _create_upload_object(self) -> tuple[str, str]:
        for attempt, backoff in enumerate(_BACKOFF_SECONDS):
            resp = self._session.post(
                _NOTION_FILE_UPLOAD_URL,
                json={},
                headers={**self._headers(), "Content-Type": "application/json"},
                timeout=self._timeout,
            )
            status = getattr(resp, "status_code", 0)
            if 200 <= status < 300:
                body = resp.json()
                upload_url = body.get("upload_url")
                file_id = body.get("id")
                if not upload_url or not file_id:
                    raise _UploadFailure(f"unexpected file_uploads response: {body!r}")
                return file_id, upload_url
            if 500 <= status < 600 and attempt < len(_BACKOFF_SECONDS) - 1:
                time.sleep(backoff)
                continue
            body_text = _safe_body(resp)
            raise _UploadFailure(f"file_uploads {status}: {body_text}")
        raise _UploadFailure("file_uploads exhausted retries")

    def _send_file_body(self, upload_url: str, path: Path) -> None:
        for attempt, backoff in enumerate(_BACKOFF_SECONDS):
            with path.open("rb") as fh:
                files = {"file": (path.name, fh, _guess_content_type(path))}
                resp = self._session.post(
                    upload_url,
                    files=files,
                    headers=self._headers(),
                    timeout=self._timeout,
                )
            status = getattr(resp, "status_code", 0)
            if 200 <= status < 300:
                return
            if 500 <= status < 600 and attempt < len(_BACKOFF_SECONDS) - 1:
                time.sleep(backoff)
                continue
            body_text = _safe_body(resp)
            raise _UploadFailure(f"file body {status}: {body_text}")
        raise _UploadFailure("file body exhausted retries")


class DryRunImageUploader:
    """Test/CLI dry-run stand-in. ``upload`` returns a placeholder file id."""

    def upload(self, path: Path) -> str:
        return f"<dry-run:{path.name}>"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _UploadFailure(Exception):
    pass


def _safe_body(resp: Any) -> str:
    try:
        return resp.text[:300]
    except Exception:  # pragma: no cover - defensive
        return "<no body>"


def _guess_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    mapping = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".svg": "image/svg+xml",
    }
    return mapping.get(suffix, "application/octet-stream")


__all__ = ["ImageUploader", "DryRunImageUploader"]
