"""TdnetRssFetcher テスト。"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from market_pipeline.news_delivery.fetchers.tdnet_rss_fetcher import TdnetRssFetcher


CONFIG = Path("config/news_sources.yaml")


def _entry(title: str, link: str, published_struct=None):
    e = SimpleNamespace(title=title, link=link)
    if published_struct is not None:
        e.published_parsed = published_struct
    return e


def _feed(entries):
    return SimpleNamespace(entries=entries)


def _make(parser, **kw) -> TdnetRssFetcher:
    return TdnetRssFetcher(
        config_path=CONFIG,
        feed_parser=parser,
        requests_per_minute=60,
        sleep_fn=lambda _s: None,
        **kw,
    )


def test_strips_company_prefix():
    feed = _feed(
        [
            _entry(
                "Ｓａｎｓａｎ:業績予想の修正に関するお知らせ",
                "https://t/1",
                time.gmtime(time.time()),
            ),
        ]
    )
    fetcher = _make(lambda url: feed)
    items = fetcher.fetch_for_codes(["4443"])
    assert items[0].title == "業績予想の修正に関するお知らせ"


def test_url_template_uses_code():
    captured = {}

    def parser(url):
        captured["url"] = url
        return _feed([])

    fetcher = _make(parser)
    fetcher.fetch_for_codes(["7203"])
    assert "7203" in captured["url"]
    assert ".atom" in captured["url"]


def test_dedup_by_url():
    feed = _feed(
        [
            _entry("A:T1", "https://x.example/1", time.gmtime(time.time())),
            _entry("A:T2", "https://x.example/1", time.gmtime(time.time())),  # 同URL
        ]
    )
    fetcher = _make(lambda url: feed)
    items = fetcher.fetch_for_codes(["1234"])
    assert len(items) == 1


def test_lookback_excludes_old(tmp_path):
    fixed_now = datetime(2026, 5, 8, 12, 0, 0)
    old = time.gmtime(time.mktime((2026, 1, 1, 0, 0, 0, 0, 0, 0)))
    new = time.gmtime(time.mktime((2026, 5, 7, 0, 0, 0, 0, 0, 0)))
    feed = _feed(
        [
            _entry("A:Old", "https://e/old", old),
            _entry("A:Recent", "https://e/new", new),
        ]
    )
    fetcher = _make(lambda url: feed, lookback_days=7, now_fn=lambda: fixed_now)
    items = fetcher.fetch_for_codes(["1234"])
    assert [i.title for i in items] == ["Recent"]


def test_per_code_failure_isolated():
    def parser(url):
        if "9999" in url:
            raise RuntimeError("boom")
        return _feed(
            [_entry("A:Ok", f"https://e/{url[-10:]}", time.gmtime(time.time()))]
        )

    fetcher = _make(parser)
    items, errors = fetcher.fetch_for_codes_with_errors(["1234", "9999"])
    assert "9999" in errors
    assert any(it.code == "1234" for it in items)


def test_empty_codes_returns_empty():
    fetcher = _make(lambda url: _feed([]))
    items, errors = fetcher.fetch_for_codes_with_errors([])
    assert items == []
    assert errors == {}
