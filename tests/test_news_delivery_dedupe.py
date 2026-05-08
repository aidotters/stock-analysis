"""Deduplicator のテスト。"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from market_pipeline.news_delivery.deduplicator import Deduplicator
from market_pipeline.news_delivery.models import NewsItem


@pytest.fixture
def dedup(tmp_path: Path) -> Deduplicator:
    db_path = tmp_path / "news_delivery.db"
    d = Deduplicator(db_path=db_path)
    d.initialize()
    return d


def _item(code: str = "7203", url: str = "https://example.com/a") -> NewsItem:
    return NewsItem(
        code=code,
        title="title",
        url=url,
        source="shikiho_disclosure",
        category="disclosure",
        published_at=datetime(2026, 5, 8),
        summary=None,
        importance="mid",
    )


def test_initialize_creates_table_and_indexes(dedup: Deduplicator) -> None:
    with sqlite3.connect(dedup._db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        names = {r[0] for r in rows}
        assert "idx_delivered_news_code" in names
        assert "idx_delivered_news_delivered_at" in names


def test_mark_delivered_upserts(dedup: Deduplicator) -> None:
    item = _item()
    dedup.mark_delivered([item], slot="evening")
    dedup.mark_delivered([item], slot="evening")  # second call should be no-op
    assert dedup.count() == 1


def test_filter_unseen_excludes_delivered(dedup: Deduplicator) -> None:
    a = _item(url="https://example.com/a")
    b = _item(url="https://example.com/b")
    dedup.mark_delivered([a], slot="evening")

    unseen = dedup.filter_unseen([a, b])
    urls = [it.url for it in unseen]
    assert urls == ["https://example.com/b"]


def test_cleanup_older_than_90_days(dedup: Deduplicator) -> None:
    # 直接古いレコードを挿入
    old_dt = (datetime.now() - timedelta(days=100)).isoformat(timespec="seconds")
    new_dt = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(dedup._db_path) as conn:
        conn.execute(
            "INSERT INTO delivered_news (url_hash, code, title, url, source, category, delivered_at, slot)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("h1", "7203", "t", "u1", "s", "disclosure", old_dt, "evening"),
        )
        conn.execute(
            "INSERT INTO delivered_news (url_hash, code, title, url, source, category, delivered_at, slot)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("h2", "7203", "t", "u2", "s", "disclosure", new_dt, "evening"),
        )
        conn.commit()

    deleted = dedup.cleanup_older_than(90)
    assert deleted == 1
    assert dedup.count() == 1


def test_filter_unseen_empty_input(dedup: Deduplicator) -> None:
    assert dedup.filter_unseen([]) == []
