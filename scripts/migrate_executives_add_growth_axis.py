#!/usr/bin/env python3
"""マイグレーション: executive_evaluations テーブルに growth_ambition カラムを追加.

- PRAGMA で存在確認
- 無ければ ALTER TABLE ADD COLUMN
- 既存レコードは growth_ambition=NULL のまま
- 次回 /research-executives --force-refresh で埋まる

冪等なので何度実行しても安全。
"""

from __future__ import annotations

import logging
import sqlite3
import sys

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def migrate(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        if has_column(conn, "executive_evaluations", "growth_ambition"):
            logger.info("growth_ambition カラムは既に存在します。スキップします。")
            return
        logger.info(
            "executive_evaluations テーブルに growth_ambition カラムを追加します。"
        )
        conn.execute(
            "ALTER TABLE executive_evaluations ADD COLUMN growth_ambition REAL"
        )
        conn.commit()
        logger.info(
            "完了しました。既存評価は NULL のまま。次回 /research-executives --force-refresh で埋まります。"
        )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    settings = get_settings()
    db_path = str(settings.paths.statements_db)
    logger.info(f"マイグレーション開始: {db_path}")
    migrate(db_path)
    logger.info("マイグレーション完了")
    return 0


if __name__ == "__main__":
    sys.exit(main())
