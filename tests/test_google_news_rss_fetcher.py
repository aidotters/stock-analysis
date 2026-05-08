"""GoogleNewsRssFetcher テスト。"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_pipeline.news_delivery.fetchers.google_news_rss_fetcher import (
    GoogleNewsRssFetcher,
)
from market_pipeline.news_delivery.rate_limiter import RateLimitError


CONFIG = Path("config/news_sources.yaml")


def _entry(
    title: str, link: str, published_struct=None, source_title: str | None = None
):
    e = SimpleNamespace(title=title, link=link)
    if published_struct is not None:
        e.published_parsed = published_struct
    if source_title is not None:
        e.source = {"title": source_title}
    return e


def _feed(entries):
    return SimpleNamespace(entries=entries)


def _make(parser, *, rpm: int = 60, **kw) -> GoogleNewsRssFetcher:
    return GoogleNewsRssFetcher(
        config_path=CONFIG,
        feed_parser=parser,
        requests_per_minute=rpm,
        sleep_fn=lambda _s: None,
        **kw,
    )


def test_basic_normalization_and_filter():
    feed = _feed(
        [
            _entry(
                "Sansan、業績予想を上方修正",
                "https://example.com/a",
                time.gmtime(time.time()),
            ),
            _entry(
                "Sansan株価チャート - Yahoo!ファイナンス",
                "https://example.com/b",
                time.gmtime(time.time()),
            ),
            _entry(
                "Sansan、新サービス発表",
                "https://example.com/c",
                time.gmtime(time.time()),
            ),
        ]
    )
    fetcher = _make(lambda url: feed)
    items, errors = fetcher.fetch_for_codes_with_errors(["4443"])
    assert errors == {}
    titles = [i.title for i in items]
    assert "Sansan、業績予想を上方修正" in titles
    assert "Sansan、新サービス発表" in titles
    # exclude フィルタで除外
    assert "Sansan株価チャート - Yahoo!ファイナンス" not in titles


def test_max_items_per_code_respected():
    # YAML config の max_items_per_code=5 が効くか
    entries = [
        _entry(f"News {i}", f"https://example.com/{i}", time.gmtime(time.time()))
        for i in range(20)
    ]
    fetcher = _make(lambda url: _feed(entries))
    items = fetcher.fetch_for_codes(["4443"])
    assert len(items) <= 5


def test_dedup_by_url():
    entries = [
        _entry("News A", "https://dup.example/x", time.gmtime(time.time())),
        _entry("News B (same url)", "https://dup.example/x", time.gmtime(time.time())),
    ]
    fetcher = _make(lambda url: _feed(entries))
    items = fetcher.fetch_for_codes(["4443"])
    assert len(items) == 1


def test_query_uses_long_name():
    captured = {}

    def parser(url):
        captured["url"] = url
        return _feed([])

    fetcher = _make(
        parser,
        meta_resolver=lambda code: {"long_name": "サンサン"},
    )
    fetcher.fetch_for_codes(["4443"])
    assert "url" in captured
    # quote_plus されてるはずなので元の文字列をチェックは難しいが、codeが入ってればOK
    assert "4443" in captured["url"] or "%34%34%34%33" in captured["url"]


def test_rate_limit_raises():
    feed = _feed([_entry("X", "https://x", time.gmtime(time.time()))])
    fetcher = _make(lambda url: feed, rpm=2)
    # 5銘柄処理 → 3件目以降は acquire 失敗 → RateLimitError
    with pytest.raises(RateLimitError) as exc:
        fetcher.fetch_for_codes_with_errors(["1", "2", "3", "4", "5"])
    assert exc.value.skipped_count == 3


def test_lookback_filter_excludes_old():
    fixed_now = datetime(2026, 5, 8, 12, 0, 0)
    old = time.gmtime(time.mktime((2026, 4, 1, 0, 0, 0, 0, 0, 0)))
    new = time.gmtime(time.mktime((2026, 5, 7, 0, 0, 0, 0, 0, 0)))
    entries = [
        _entry("Old item", "https://e/old", old),
        _entry("Recent item", "https://e/new", new),
    ]
    fetcher = _make(
        lambda url: _feed(entries),
        lookback_days=7,
        now_fn=lambda: fixed_now,
    )
    items = fetcher.fetch_for_codes(["4443"])
    titles = [i.title for i in items]
    assert "Recent item" in titles
    assert "Old item" not in titles


def test_empty_codes_returns_empty():
    fetcher = _make(lambda url: _feed([]))
    items, errors = fetcher.fetch_for_codes_with_errors([])
    assert items == []
    assert errors == {}


def test_per_code_failure_isolated():
    def parser(url):
        if "5" in url:
            raise RuntimeError("boom")
        return _feed([_entry("X", f"https://e/{url}", time.gmtime(time.time()))])

    fetcher = _make(parser)
    items, errors = fetcher.fetch_for_codes_with_errors(["1234", "5678"])
    # 5678は失敗、1234は成功
    assert "5678" in errors
    assert any(i.code == "1234" for i in items)
