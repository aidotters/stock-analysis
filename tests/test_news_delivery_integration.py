"""DeliveryService の統合テスト。"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import pytest

from market_pipeline.news_delivery.deduplicator import Deduplicator
from market_pipeline.news_delivery.delivery_service import DeliveryService
from market_pipeline.news_delivery.fetchers.base import BaseFetcher
from market_pipeline.news_delivery.formatter import SlackFormatter
from market_pipeline.news_delivery.models import NewsItem
from market_pipeline.news_delivery.watchlist import WatchList, make_entry


class FakeFetcher(BaseFetcher):
    source_name = "fake"
    category = "disclosure"

    def __init__(
        self,
        items_by_code: dict[str, list[NewsItem]],
        errors: Optional[dict[str, Exception]] = None,
    ) -> None:
        self._by_code = items_by_code
        self._errors = errors or {}

    def fetch_for_codes(self, codes: list[str]) -> list[NewsItem]:
        out: list[NewsItem] = []
        for c in codes:
            if c in self._errors:
                continue
            out.extend(self._by_code.get(c, []))
        return out

    def fetch_for_codes_with_errors(
        self, codes: list[str]
    ) -> tuple[list[NewsItem], dict[str, Exception]]:
        items: list[NewsItem] = []
        errs: dict[str, Exception] = {}
        for c in codes:
            if c in self._errors:
                errs[c] = self._errors[c]
                continue
            items.extend(self._by_code.get(c, []))
        return items, errs


def _setup_master_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE stocks_master (
            code TEXT PRIMARY KEY,
            name TEXT,
            sector TEXT,
            market TEXT,
            market_product_category TEXT,
            yfinance_symbol TEXT,
            jquants_code TEXT,
            is_active BOOLEAN DEFAULT 1
        )"""
    )
    conn.execute(
        "INSERT INTO stocks_master (code, name, sector) VALUES (?, ?, ?)",
        ("7203", "トヨタ自動車", "輸送用機器"),
    )
    conn.execute(
        "INSERT INTO stocks_master (code, name, sector) VALUES (?, ?, ?)",
        ("9984", "ソフトバンクG", "情報・通信業"),
    )
    conn.commit()
    conn.close()


def _item(code: str, url: str, title: str = "業績予想の修正") -> NewsItem:
    return NewsItem(
        code=code,
        title=title,
        url=url,
        source="fake",
        category="disclosure",
        published_at=datetime(2026, 5, 1),
    )


@pytest.fixture
def integration_env(tmp_path: Path) -> dict[str, Any]:
    master_db = tmp_path / "master.db"
    _setup_master_db(master_db)
    watchlists_dir = tmp_path / "watchlists"
    watchlists_dir.mkdir()
    news_db = tmp_path / "news_delivery.db"

    wl = WatchList.load(
        "default", watchlists_dir=watchlists_dir, master_db_path=master_db
    )
    wl.add(
        make_entry(
            code="7203", tag="holding", priority="high", added_at=date(2026, 5, 8)
        )
    )
    wl.add(
        make_entry(
            code="9984", tag="considering", priority="mid", added_at=date(2026, 5, 8)
        )
    )
    wl.save()

    return {
        "watchlist_dir": watchlists_dir,
        "master_db": master_db,
        "news_db": news_db,
    }


def _build_service(
    env: dict, fetcher: FakeFetcher, sent: list[list[dict]]
) -> DeliveryService:
    wl = WatchList.load(
        "default", watchlists_dir=env["watchlist_dir"], master_db_path=env["master_db"]
    )
    dedup = Deduplicator(db_path=env["news_db"])
    fmt = SlackFormatter()

    def fake_post(_url: str, blocks: list[dict]) -> None:
        sent.append(blocks)

    return DeliveryService(
        watchlist=wl,
        fetchers=[fetcher],
        deduplicator=dedup,
        formatter=fmt,
        slack_post=fake_post,
        webhook_url="https://hooks.example/test",
        quiet_when_empty=False,
    )


def test_run_sends_messages_and_marks_delivered(integration_env: dict) -> None:
    items = {
        "7203": [_item("7203", "https://example.com/7203/a")],
        "9984": [_item("9984", "https://example.com/9984/b")],
    }
    fetcher = FakeFetcher(items)
    sent: list[list[dict]] = []
    svc = _build_service(integration_env, fetcher, sent)

    result = svc.run("evening")
    assert result["new_items"] == 2
    assert result["messages"] >= 1
    assert len(sent) >= 1

    dedup = Deduplicator(db_path=integration_env["news_db"])
    assert dedup.count() == 2


def test_run_excludes_already_delivered(integration_env: dict) -> None:
    delivered = _item("7203", "https://example.com/7203/old")
    new_item = _item("9984", "https://example.com/9984/new")

    dedup = Deduplicator(db_path=integration_env["news_db"])
    dedup.mark_delivered([delivered], slot="evening")

    fetcher = FakeFetcher({"7203": [delivered], "9984": [new_item]})
    sent: list[list[dict]] = []
    svc = _build_service(integration_env, fetcher, sent)
    result = svc.run("evening")

    assert result["new_items"] == 1
    assert result["skipped_items"] == 1
    # Slack には送信される(新規があるため)
    assert len(sent) >= 1


def test_fetcher_error_recorded_as_warning(integration_env: dict) -> None:
    from market_pipeline.news_delivery.exceptions import DisclosureFetchError

    items = {"9984": [_item("9984", "https://example.com/9984/a")]}
    err = DisclosureFetchError(code="7203", source="fake", status=503)
    fetcher = FakeFetcher(items, errors={"7203": err})

    warnings: list[str] = []
    wl = WatchList.load(
        "default",
        watchlists_dir=integration_env["watchlist_dir"],
        master_db_path=integration_env["master_db"],
    )
    dedup = Deduplicator(db_path=integration_env["news_db"])
    fmt = SlackFormatter()
    sent: list[list[dict]] = []
    svc = DeliveryService(
        watchlist=wl,
        fetchers=[fetcher],
        deduplicator=dedup,
        formatter=fmt,
        slack_post=lambda u, b: sent.append(b),
        webhook_url="https://hooks.example/test",
        warning_handler=warnings.append,
        quiet_when_empty=False,
    )
    result = svc.run("evening")
    assert result["new_items"] == 1
    assert any("7203" in w for w in warnings)


def test_rate_limit_retries_high_priority_only(integration_env: dict) -> None:
    """RateLimitError 発生時は priority=high の銘柄のみで再試行される。"""
    from market_pipeline.news_delivery.fetchers.base import BaseFetcher
    from market_pipeline.news_delivery.rate_limiter import RateLimitError

    items_high = _item("7203", "https://example.com/7203/hp")

    class RateLimitedFetcher(BaseFetcher):
        source_name = "rl"
        category = "general_news"

        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def fetch_for_codes(self, codes):
            self.calls.append(list(codes))
            if len(self.calls) == 1:
                # 初回呼び出しで RateLimitError
                raise RateLimitError("hit", skipped_count=1)
            # 再試行は priority=high のみ
            return [items_high] if "7203" in codes else []

        def fetch_for_codes_with_errors(self, codes):
            return self.fetch_for_codes(codes), {}

    fetcher = RateLimitedFetcher()
    warnings: list[str] = []
    metrics: list[tuple[str, str]] = []
    sent: list[list[dict]] = []

    wl = WatchList.load(
        "default",
        watchlists_dir=integration_env["watchlist_dir"],
        master_db_path=integration_env["master_db"],
    )
    dedup = Deduplicator(db_path=integration_env["news_db"])
    fmt = SlackFormatter()
    svc = DeliveryService(
        watchlist=wl,
        fetchers=[fetcher],
        deduplicator=dedup,
        formatter=fmt,
        slack_post=lambda u, b: sent.append(b),
        webhook_url="https://hooks.example/test",
        warning_handler=warnings.append,
        metric_handler=lambda k, v: metrics.append((k, v)),
        quiet_when_empty=False,
    )
    result = svc.run("morning")

    # 初回 (全銘柄) と再試行 (high のみ) で2回呼ばれている
    assert len(fetcher.calls) == 2
    assert set(fetcher.calls[0]) == {"7203", "9984"}
    assert fetcher.calls[1] == ["7203"]
    # high 銘柄のニュースのみ取得
    assert result["new_items"] == 1
    # 警告とメトリクスが記録されている
    assert any("レート制限到達" in w for w in warnings)
    assert any(k.startswith("レート制限到達") for k, _ in metrics)


def test_dry_run_does_not_send_or_mark(integration_env: dict) -> None:
    items = {"7203": [_item("7203", "https://example.com/7203/dr")]}
    fetcher = FakeFetcher(items)
    sent: list[list[dict]] = []
    svc = _build_service(integration_env, fetcher, sent)
    svc.run("evening", dry_run=True)
    assert sent == []
    dedup = Deduplicator(db_path=integration_env["news_db"])
    assert dedup.count() == 0
