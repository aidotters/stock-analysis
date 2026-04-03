#!/usr/bin/env python3
"""
マイグレーション: daily_quotesテーブルにsourceカラムを追加

- ALTER TABLE ADD COLUMN でsourceカラム（TEXT型）を追加
- 既存レコードのsourceを'jquants'に一括UPDATE（10万件単位で分割）
- 冪等性: カラムが既に存在する場合はスキップ
"""

import logging
import sqlite3
from datetime import datetime

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


def has_source_column(conn: sqlite3.Connection) -> bool:
    """daily_quotesテーブルにsourceカラムが存在するか確認"""
    cursor = conn.execute("PRAGMA table_info(daily_quotes)")
    columns = [row[1] for row in cursor.fetchall()]
    return "source" in columns


def migrate(db_path: str) -> None:
    """sourceカラムを追加し、既存レコードを'jquants'に更新"""
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")

        if has_source_column(conn):
            logger.info("sourceカラムは既に存在します。スキップします。")
            # 既存レコードでsource=NULLのものがあれば更新
            null_count = conn.execute(
                "SELECT COUNT(*) FROM daily_quotes WHERE source IS NULL"
            ).fetchone()[0]
            if null_count > 0:
                logger.info(
                    f"source=NULLのレコードが{null_count}件あります。'jquants'に更新します。"
                )
            else:
                logger.info("全レコードにsourceが設定済みです。")
                return
        else:
            logger.info("daily_quotesテーブルにsourceカラムを追加します。")
            conn.execute("ALTER TABLE daily_quotes ADD COLUMN source TEXT")
            conn.commit()
            logger.info("sourceカラムを追加しました。")

        # 既存レコードのsourceを'jquants'に一括UPDATE（10万件単位）
        total = conn.execute(
            "SELECT COUNT(*) FROM daily_quotes WHERE source IS NULL"
        ).fetchone()[0]
        logger.info(f"source=NULLのレコード数: {total}")

        if total == 0:
            return

        batch_size = 100_000
        updated = 0
        while updated < total:
            conn.execute(
                """
                UPDATE daily_quotes SET source = 'jquants'
                WHERE rowid IN (
                    SELECT rowid FROM daily_quotes WHERE source IS NULL LIMIT ?
                )
                """,
                (batch_size,),
            )
            conn.commit()
            batch_updated = conn.execute("SELECT changes()").fetchone()[0]
            updated += batch_updated
            logger.info(f"更新進捗: {updated}/{total} ({updated / total * 100:.1f}%)")
            if batch_updated == 0:
                break

        # 確認
        final_count = conn.execute(
            "SELECT COUNT(*) FROM daily_quotes WHERE source = 'jquants'"
        ).fetchone()[0]
        null_remaining = conn.execute(
            "SELECT COUNT(*) FROM daily_quotes WHERE source IS NULL"
        ).fetchone()[0]
        logger.info(
            f"マイグレーション完了: source='jquants'={final_count}件, source=NULL={null_remaining}件"
        )


def main() -> None:
    from market_pipeline.config import get_settings as _get_settings

    _settings = _get_settings()
    log_dir = _settings.paths.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_dir / f"migrate_source_{datetime.now().strftime('%Y%m%d')}.log",
                encoding="utf-8",
            ),
        ],
    )

    settings = get_settings()
    db_path = str(settings.paths.jquants_db)

    logger.info(f"マイグレーション開始: {db_path}")
    migrate(db_path)
    logger.info("マイグレーション完了")


if __name__ == "__main__":
    main()
