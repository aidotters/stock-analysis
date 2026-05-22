"""Tests for ``notion_export.page_repository``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from notion_export.exceptions import NotionApiError, ParentPageNotFoundError
from notion_export.page_repository import (
    FakeNotionPageRepository,
    RestNotionPageRepository,
)


def _mk_response(status: int, json_body=None, text: str = "{}") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.json.return_value = json_body or {}
    return r


@pytest.fixture
def session() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(session: MagicMock) -> RestNotionPageRepository:
    return RestNotionPageRepository(
        api_token="tok",
        http_session=session,
        throttle_seconds=0,
    )


# ---------------------------------------------------------------------------
# fetch_parent
# ---------------------------------------------------------------------------


def test_fetch_parent_200(repo: RestNotionPageRepository, session: MagicMock) -> None:
    session.request.return_value = _mk_response(200, {"id": "page1"})
    out = repo.fetch_parent("page1")
    assert out["id"] == "page1"
    args = session.request.call_args
    assert args.args[0] == "GET"
    assert args.args[1].endswith("/v1/pages/page1")


def test_fetch_parent_404_raises(
    repo: RestNotionPageRepository, session: MagicMock
) -> None:
    session.request.return_value = _mk_response(404, text="not found")
    with pytest.raises(ParentPageNotFoundError):
        repo.fetch_parent("missing")


# ---------------------------------------------------------------------------
# search_children_by_title_prefix
# ---------------------------------------------------------------------------


def test_search_returns_only_prefix_matches(
    repo: RestNotionPageRepository, session: MagicMock
) -> None:
    response = {
        "results": [
            {
                "id": "p-A",
                "parent": {"page_id": "ROOT"},
                "properties": {
                    "title": {
                        "type": "title",
                        "title": [{"plain_text": "ビーアンドピー（7804）投資分析"}],
                    }
                },
            },
            {
                "id": "p-B",
                "parent": {"page_id": "ROOT"},
                "properties": {
                    "title": {
                        "type": "title",
                        "title": [{"plain_text": "別銘柄（9999）投資分析"}],
                    }
                },
            },
            {
                "id": "p-C",
                "parent": {"page_id": "OTHER"},
                "properties": {
                    "title": {
                        "type": "title",
                        "title": [{"plain_text": "ビーアンドピー（7804）投資分析"}],
                    }
                },
            },
        ],
        "has_more": False,
    }
    session.request.return_value = _mk_response(200, response)
    out = repo.search_children_by_title_prefix("ROOT", "ビーアンドピー（7804）")
    assert [h["id"] for h in out] == ["p-A"]


# ---------------------------------------------------------------------------
# create_page
# ---------------------------------------------------------------------------


def test_create_page_caps_at_100_blocks(
    repo: RestNotionPageRepository, session: MagicMock
) -> None:
    session.request.return_value = _mk_response(200, {"id": "NEW_PAGE"})
    blocks = [{"type": "paragraph", "paragraph": {"rich_text": []}} for _ in range(150)]
    pid = repo.create_page("ROOT", "title", blocks)
    assert pid == "NEW_PAGE"
    sent = session.request.call_args.kwargs["json"]
    assert len(sent["children"]) == 100


# ---------------------------------------------------------------------------
# archive_page
# ---------------------------------------------------------------------------


def test_archive_page_sends_archived_true(
    repo: RestNotionPageRepository, session: MagicMock
) -> None:
    session.request.return_value = _mk_response(200, {"id": "p"})
    repo.archive_page("p")
    args = session.request.call_args
    assert args.args[0] == "PATCH"
    assert args.kwargs["json"] == {"archived": True}


# ---------------------------------------------------------------------------
# append_blocks
# ---------------------------------------------------------------------------


def test_append_blocks_chunks_in_100s(
    repo: RestNotionPageRepository, session: MagicMock
) -> None:
    session.request.return_value = _mk_response(200, {})
    blocks = [{"type": "paragraph", "paragraph": {"rich_text": []}} for _ in range(250)]
    repo.append_blocks("PAGE", blocks)
    assert session.request.call_count == 3
    # Each call should send <= 100 children
    for call in session.request.call_args_list:
        assert len(call.kwargs["json"]["children"]) <= 100


# ---------------------------------------------------------------------------
# 5xx behaviour
# ---------------------------------------------------------------------------


def test_5xx_eventually_raises(
    repo: RestNotionPageRepository, session: MagicMock, mocker
) -> None:
    mocker.patch("notion_export.page_repository.time.sleep")
    session.request.return_value = _mk_response(503, text="boom")
    with pytest.raises(NotionApiError):
        repo.archive_page("p")


# ---------------------------------------------------------------------------
# Fake
# ---------------------------------------------------------------------------


def test_fake_create_and_archive_flow() -> None:
    fake = FakeNotionPageRepository()
    fake.add_parent("ROOT")
    pid = fake.create_page("ROOT", "ビーアンドピー（7804）投資分析", [])
    assert pid in fake.pages
    hits = fake.search_children_by_title_prefix("ROOT", "ビーアンドピー（7804）")
    assert len(hits) == 1
    fake.archive_page(pid)
    assert fake.pages[pid]["archived"] is True
    hits2 = fake.search_children_by_title_prefix("ROOT", "ビーアンドピー（7804）")
    assert hits2 == []
