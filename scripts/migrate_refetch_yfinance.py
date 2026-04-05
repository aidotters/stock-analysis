#!/usr/bin/env python3
"""
マイグレーション: yfinanceデータを削除して再取得

既存のyfinanceレコード（Open/High/Low/Close が NULL）を削除し、
auto_adjust=False で再取得して生OHLCV + 調整済み価格を正しく格納する。

Usage:
    python scripts/migrate_refetch_yfinance.py
    python scripts/migrate_refetch_yfinance.py --dry-run
    python scripts/migrate_refetch_yfinance.py --symbols 7203 9984
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_pipeline.config import get_settings
from market_pipeline.yfinance.historical_price_fetcher import HistoricalPriceFetcher


def setup_logging() -> logging.Logger:
    settings = get_settings()
    log_dir = settings.paths.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = (
        log_dir / f"migrate_refetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger(__name__)


def count_yfinance_records(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM daily_quotes WHERE source = 'yfinance'"
        ).fetchone()[0]


def delete_yfinance_records(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        before = count_yfinance_records(db_path)
        conn.execute("DELETE FROM daily_quotes WHERE source = 'yfinance'")
        conn.commit()
        return before


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="yfinanceデータを削除して auto_adjust=False で再取得"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="削除・再取得せず件数のみ表示",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="指定銘柄のみ対象（例: 7203 9984）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="並列ワーカー数（デフォルト: 4）",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=20,
        help="取得年数（デフォルト: 20）",
    )
    return parser.parse_args()


def main() -> None:
    logger = setup_logging()
    args = parse_args()
    settings = get_settings()
    db_path = str(settings.paths.jquants_db)

    current_count = count_yfinance_records(db_path)
    logger.info(f"現在のyfinanceレコード数: {current_count:,}")

    if args.dry_run:
        logger.info("*** DRY RUN — 削除・再取得は行いません ***")
        return

    if current_count == 0:
        logger.info("yfinanceレコードなし。再取得のみ実行します。")
    else:
        # Step 1: 削除
        if args.symbols:
            # 特定銘柄のみ削除
            with sqlite3.connect(db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                for sym in args.symbols:
                    code_5 = f"{sym}0" if len(sym) == 4 else sym
                    deleted = conn.execute(
                        "SELECT COUNT(*) FROM daily_quotes WHERE Code = ? AND source = 'yfinance'",
                        (code_5,),
                    ).fetchone()[0]
                    conn.execute(
                        "DELETE FROM daily_quotes WHERE Code = ? AND source = 'yfinance'",
                        (code_5,),
                    )
                    logger.info(f"{sym}: {deleted:,}件削除")
                conn.commit()
        else:
            deleted = delete_yfinance_records(db_path)
            logger.info(f"yfinanceレコード {deleted:,}件を削除しました")

    # Step 2: 再取得
    logger.info("auto_adjust=False で再取得を開始します...")
    fetcher = HistoricalPriceFetcher(
        jquants_db_path=db_path,
        master_db_path=str(settings.paths.master_db),
        max_workers=args.max_workers,
        years=args.years,
    )

    result = fetcher.run(symbols=args.symbols)

    logger.info(
        f"完了: 成功={result['success']}, 失敗={result['failed']}, "
        f"スキップ={result['skipped']}, レコード数={result['total_records']:,}, "
        f"経過={result['elapsed']:.1f}s"
    )

    # Step 3: 検証
    new_count = count_yfinance_records(db_path)
    logger.info(f"マイグレーション後のyfinanceレコード数: {new_count:,}")

    # NULL OHLCの残存チェック
    with sqlite3.connect(db_path) as conn:
        null_count = conn.execute(
            "SELECT COUNT(*) FROM daily_quotes WHERE source = 'yfinance' AND Open IS NULL"
        ).fetchone()[0]
    if null_count > 0:
        logger.warning(
            f"Open が NULL のyfinanceレコードが {null_count:,}件残存しています"
        )
    else:
        logger.info("全yfinanceレコードに生OHLCV値が格納されています")


if __name__ == "__main__":
    main()
