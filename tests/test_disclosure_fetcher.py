"""DisclosureFetcher のテスト。requests.Session を monkeypatch して HTTP を制御する。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from market_pipeline.news_delivery.exceptions import DisclosureFetchError
from market_pipeline.news_delivery.fetchers.disclosure_fetcher import (
    DisclosureFetcher,
)

FIXTURES = Path(__file__).parent / "fixtures" / "news_delivery"
CONFIG_PATH = "config/news_sources.yaml"


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if 400 <= self.status_code < 600:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)  # type: ignore[arg-type]


class _FakeSession:
    """status_code を順番に返すフェイクセッション。"""

    def __init__(self, responses: list[Any]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get(
        self, url: str, timeout: float = 0, headers: dict | None = None
    ) -> _FakeResponse:
        self.calls.append((url, headers or {}))
        if not self._responses:
            raise RuntimeError("FakeSession exhausted")
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        if isinstance(nxt, _FakeResponse):
            return nxt
        # 整数なら status_code とみなす(本文は空)
        return _FakeResponse(int(nxt))


def _make_fetcher(session: _FakeSession, **kwargs: Any) -> DisclosureFetcher:
    return DisclosureFetcher(
        config_path=CONFIG_PATH,
        max_workers=1,
        session=session,
        sleep_fn=lambda s: None,  # リトライ間スリープを無効化
        **kwargs,
    )


def test_parse_success_excludes_governance(tmp_path: Path) -> None:
    html = (FIXTURES / "shikiho_disclosure_success.html").read_text(encoding="utf-8")
    session = _FakeSession([_FakeResponse(200, html)])
    fetcher = _make_fetcher(session)

    items = fetcher.fetch_one("7203")
    titles = [i.title for i in items]
    # 業績予想・決算短信・自己株式取得は通る、ガバナンス報告書はexclude
    assert any("業績予想の修正" in t for t in titles)
    assert any("決算短信" in t for t in titles)
    assert any("自己株式取得" in t for t in titles)
    assert all("ガバナンスに関する報告書" not in t for t in titles)
    # importance: include キーワード一致のものは high
    by_title = {i.title: i for i in items}
    assert any(i.importance == "high" for i in by_title.values())


def test_parse_empty(tmp_path: Path) -> None:
    html = (FIXTURES / "shikiho_disclosure_empty.html").read_text(encoding="utf-8")
    session = _FakeSession([_FakeResponse(200, html)])
    fetcher = _make_fetcher(session)
    assert fetcher.fetch_one("7203") == []


def test_503_then_failure(tmp_path: Path) -> None:
    # 4回 503 を返して最終的に DisclosureFetchError を期待
    session = _FakeSession(
        [
            _FakeResponse(503, ""),
            _FakeResponse(503, ""),
            _FakeResponse(503, ""),
            _FakeResponse(503, ""),
        ]
    )
    fetcher = _make_fetcher(session)
    with pytest.raises(DisclosureFetchError) as excinfo:
        fetcher.fetch_one("7203")
    assert excinfo.value.code == "7203"
    assert excinfo.value.status == 503


def test_503_then_success(tmp_path: Path) -> None:
    html = (FIXTURES / "shikiho_disclosure_success.html").read_text(encoding="utf-8")
    session = _FakeSession(
        [_FakeResponse(503, ""), _FakeResponse(503, ""), _FakeResponse(200, html)]
    )
    fetcher = _make_fetcher(session)
    items = fetcher.fetch_one("7203")
    assert len(items) >= 1


def test_fetch_for_codes_isolates_failures(tmp_path: Path) -> None:
    html = (FIXTURES / "shikiho_disclosure_success.html").read_text(encoding="utf-8")
    # 7203: 503*4 で失敗、9984: 200 で成功
    # _FakeSession は順番に取り出すため、max_workers=1 + 直列で並ぶ前提
    session = _FakeSession(
        [
            _FakeResponse(503, ""),
            _FakeResponse(503, ""),
            _FakeResponse(503, ""),
            _FakeResponse(503, ""),
            _FakeResponse(200, html),
        ]
    )
    fetcher = _make_fetcher(session)
    items, errors = fetcher.fetch_for_codes_with_errors(["7203", "9984"])
    # 9984 由来のアイテムが返る
    assert any(i.code == "9984" for i in items)
    # 7203 はエラー
    assert "7203" in errors
    assert isinstance(errors["7203"], DisclosureFetchError)


def test_user_agent_is_set(tmp_path: Path) -> None:
    html = (FIXTURES / "shikiho_disclosure_empty.html").read_text(encoding="utf-8")
    session = _FakeSession([_FakeResponse(200, html)])
    fetcher = _make_fetcher(session)
    fetcher.fetch_one("7203")
    assert session.calls[0][1].get("User-Agent", "").startswith("Stock-Analysis/")
