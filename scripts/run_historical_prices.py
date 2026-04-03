#!/usr/bin/env python3
"""
yfinance過去データ一括取得スクリプト

master.dbのアクティブ銘柄について、yfinanceから過去20年分の日足データを取得し、
jquants.dbのdaily_quotesテーブルに挿入する。
J-Quantsデータが既に存在する期間は取得対象から除外（J-Quants優先）。
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from market_pipeline.config import get_settings
from market_pipeline.yfinance.historical_price_fetcher import HistoricalPriceFetcher
from market_pipeline.utils.slack_notifier import JobContext

sys.path.insert(0, str(Path(__file__).parent))
from migrate_add_source_column import migrate


def setup_logging(settings) -> logging.Logger:
    """ログ設定"""
    log_dir = settings.paths.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"historical_prices_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=getattr(logging, settings.logging.level),
        format=settings.logging.format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="yfinanceから過去の株価データを取得してjquants.dbに挿入"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB書き込みなしで取得件数と対象期間を表示",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="指定銘柄のみ取得（例: 7203 9984）",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=20,
        help="取得年数（デフォルト: 20）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="並列ワーカー数（デフォルト: 4）",
    )
    return parser.parse_args()


def main() -> None:
    settings = get_settings()
    logger = setup_logging(settings)
    args = parse_args()

    logger.info("=== yfinance過去データ取得開始 ===")
    if args.dry_run:
        logger.info("*** DRY RUN モード ***")

    try:
        with JobContext("yfinance過去データ取得") as job:
            jquants_db_path = str(settings.paths.jquants_db)

            # マイグレーション実行（sourceカラム追加）
            logger.info("マイグレーションを確認中...")
            migrate(jquants_db_path)

            # データ取得
            fetcher = HistoricalPriceFetcher(
                jquants_db_path=jquants_db_path,
                master_db_path=str(settings.paths.master_db),
                max_workers=args.max_workers,
                years=args.years,
            )

            result = fetcher.run(
                symbols=args.symbols,
                dry_run=args.dry_run,
            )

            # Slack通知にメトリクス追加
            job.add_metric("成功", str(result["success"]))
            job.add_metric("失敗", str(result["failed"]))
            job.add_metric("スキップ", str(result["skipped"]))
            job.add_metric("yfinanceレコード数", str(result["total_records"]))
            job.add_metric("経過時間", f"{result['elapsed']:.1f}s")
            if args.dry_run:
                job.add_metric("モード", "dry-run")

        logger.info("=== yfinance過去データ取得完了 ===")

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
