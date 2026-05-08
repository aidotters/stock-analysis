"""SlackFormatter のテスト。

スナップショット完全一致ではなく、構造的な確認(代表的なテキストや件数) で検証する。
ヘッダ・銘柄セクション数・分割数・メトリクス文字列の一致をチェック。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Optional

from market_pipeline.news_delivery.formatter import (
    DeliveryStats,
    SlackFormatter,
    StockMetaInfo,
)
from market_pipeline.news_delivery.models import NewsItem, WatchListEntry


def _entry(code: str, tag: str = "holding", priority: str = "high") -> WatchListEntry:
    return WatchListEntry(
        code=code,
        tag=tag,
        priority=priority,
        added_at=date(2026, 5, 8),  # type: ignore[arg-type]
    )


def _item(
    code: str,
    title: str = "業績予想の修正",
    url: Optional[str] = None,
    category: str = "disclosure",
) -> NewsItem:
    return NewsItem(
        code=code,
        title=title,
        url=url or f"https://example.com/{code}/{abs(hash(title))}",
        source="shikiho_disclosure",
        category=category,
        published_at=datetime(2026, 5, 1),
        importance="high",
    )


def _meta(_code: str) -> StockMetaInfo:
    return StockMetaInfo(long_name="トヨタ", sector="輸送用機器")


def _flatten_text(blocks: list[dict]) -> str:
    return json.dumps(blocks, ensure_ascii=False)


def test_single_stock_single_news() -> None:
    fmt = SlackFormatter()
    entries = [_entry("7203")]
    items_by_code = {"7203": [_item("7203")]}
    stats = DeliveryStats(code_count=1, new_items=1, skipped_items=0)
    msgs = fmt.build(
        slot="evening",
        watchlist=entries,
        items_by_code=items_by_code,
        stats=stats,
        meta_resolver=_meta,
        now=datetime(2026, 5, 8, 19, 30),
    )
    assert len(msgs) == 1
    text = _flatten_text(msgs[0])
    assert "🌙 夜のニュース配信" in text
    assert "7203" in text
    assert "📊" in text
    assert "📋" in text
    assert "配信銘柄: 1件" in text
    assert "新規ニュース: 1件" in text


def test_multi_stocks_multi_news() -> None:
    fmt = SlackFormatter()
    entries = [_entry("7203"), _entry("9984", tag="considering", priority="mid")]
    items_by_code = {
        "7203": [_item("7203", "決算短信"), _item("7203", "自己株式取得")],
        "9984": [_item("9984", "業績予想の修正")],
    }
    stats = DeliveryStats(code_count=2, new_items=3, skipped_items=1)
    msgs = fmt.build(
        slot="evening",
        watchlist=entries,
        items_by_code=items_by_code,
        stats=stats,
        meta_resolver=_meta,
        now=datetime(2026, 5, 8, 19, 30),
    )
    assert len(msgs) == 1
    text = _flatten_text(msgs[0])
    assert "7203" in text and "9984" in text
    assert "配信銘柄: 2件 | 新規ニュース: 3件 | スキップ(重複): 1件" in text


def test_split_when_exceeding_threshold() -> None:
    fmt = SlackFormatter()
    # 大量の銘柄 + 長文タイトルでしきい値超過を誘発
    big_title = "業績予想の修正に関するお知らせ " + ("詳細" * 200)
    entries = []
    items_by_code: dict[str, list[NewsItem]] = {}
    for i in range(40):
        code = f"{1000 + i:04d}"
        entries.append(_entry(code))
        items_by_code[code] = [
            _item(code, big_title + f" #{j}", url=f"https://example.com/{code}/{j}")
            for j in range(20)
        ]
    stats = DeliveryStats(code_count=40, new_items=800, skipped_items=0)

    msgs = fmt.build(
        slot="evening",
        watchlist=entries,
        items_by_code=items_by_code,
        stats=stats,
        meta_resolver=_meta,
        now=datetime(2026, 5, 8, 19, 30),
    )
    assert len(msgs) >= 2
    # メトリクス行は最終メッセージにのみ含まれる
    last = _flatten_text(msgs[-1])
    assert "配信銘柄: 40件" in last
    for prev in msgs[:-1]:
        assert "配信銘柄: 40件" not in _flatten_text(prev)


def test_empty_with_quiet_returns_no_messages() -> None:
    fmt = SlackFormatter()
    stats = DeliveryStats(code_count=3, new_items=0, skipped_items=0)
    msgs = fmt.build(
        slot="evening",
        watchlist=[_entry("7203")],
        items_by_code={},
        stats=stats,
        quiet_when_empty=True,
    )
    assert msgs == []


def test_empty_without_quiet_returns_one_message() -> None:
    fmt = SlackFormatter()
    stats = DeliveryStats(code_count=3, new_items=0, skipped_items=0)
    msgs = fmt.build(
        slot="evening",
        watchlist=[_entry("7203")],
        items_by_code={},
        stats=stats,
        quiet_when_empty=False,
        now=datetime(2026, 5, 8, 19, 30),
    )
    assert len(msgs) == 1
    text = _flatten_text(msgs[0])
    assert "📭 本日新着なし" in text
    assert "配信銘柄: 3件" in text
