"""data/news_delivery.db に対する重複排除処理。"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional

from market_pipeline.config import get_settings
from market_pipeline.news_delivery.models import NewsItem

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS delivered_news (
    url_hash TEXT PRIMARY KEY,
    code TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    delivered_at TIMESTAMP NOT NULL,
    slot TEXT NOT NULL
)
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_delivered_news_code ON delivered_news(code)",
    "CREATE INDEX IF NOT EXISTS idx_delivered_news_delivered_at "
    "ON delivered_news(delivered_at)",
]


class Deduplicator:
    """`delivered_news` テーブルへの登録と未配信フィルタを提供する。"""

    def __init__(self, db_path: Optional[str | Path] = None) -> None:
        settings = get_settings()
        if db_path is None:
            data_dir = settings.paths.data_dir
            assert data_dir is not None
            db_path = data_dir / "news_delivery.db"
        self._db_path = str(db_path)
        self._pragmas = settings.database.get_pragma_statements()
        self._initialized = False

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path)
        try:
            for pragma in self._pragmas:
                conn.execute(pragma)
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self) -> None:
        """delivered_news テーブルとインデックスを作成する。"""
        with self._connect() as conn:
            conn.execute(_SCHEMA)
            for sql in _INDEXES:
                conn.execute(sql)
        self._initialized = True

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    def filter_unseen(self, items: Iterable[NewsItem]) -> list[NewsItem]:
        """既配信URLハッシュを除いた未配信リストを返す。"""
        self._ensure_initialized()
        items_list = list(items)
        if not items_list:
            return []

        hashes = [it.url_hash for it in items_list]
        with self._connect() as conn:
            placeholders = ",".join("?" for _ in hashes)
            rows = conn.execute(
                f"SELECT url_hash FROM delivered_news WHERE url_hash IN ({placeholders})",
                hashes,
            ).fetchall()
            seen = {r["url_hash"] for r in rows}
        return [it for it in items_list if it.url_hash not in seen]

    def mark_delivered(self, items: Iterable[NewsItem], slot: str) -> int:
        """配信済みを INSERT OR IGNORE で記録する。挿入件数を返す。"""
        self._ensure_initialized()
        items_list = list(items)
        if not items_list:
            return 0

        now = datetime.now().isoformat(timespec="seconds")
        rows = [
            (
                it.url_hash,
                it.code,
                it.title,
                it.url,
                it.source,
                it.category,
                now,
                slot,
            )
            for it in items_list
        ]
        with self._connect() as conn:
            cur = conn.executemany(
                """
                INSERT OR IGNORE INTO delivered_news
                    (url_hash, code, title, url, source, category, delivered_at, slot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            inserted = cur.rowcount if cur.rowcount is not None else 0
        return max(inserted, 0)

    def cleanup_older_than(self, days: int = 90) -> int:
        """delivered_at が指定日数より古いレコードを削除する。"""
        self._ensure_initialized()
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM delivered_news WHERE delivered_at < ?", (cutoff,)
            )
            deleted = cur.rowcount if cur.rowcount is not None else 0
        return max(deleted, 0)

    def count(self) -> int:
        self._ensure_initialized()
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM delivered_news").fetchone()
            return int(row["n"])
