#!/usr/bin/env python3
"""
日次株価データ取得スクリプト（J-Quants API）
cronから実行される日次タスク
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from market_pipeline.config import get_settings
from market_pipeline.jquants.data_processor import JQuantsDataProcessor
from market_pipeline.utils.slack_notifier import JobContext


def setup_logging(settings):
    """ログ設定"""
    log_dir = settings.paths.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"jquants_daily_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=getattr(logging, settings.logging.level),
        format=settings.logging.format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger(__name__)


def main(chain: bool = True):
    """日次株価データ取得処理

    Args:
        chain: True の場合、完了後に Daily Analysis → Integrated Analysis を起動
    """
    settings = get_settings()
    logger = setup_logging(settings)

    logger.info("=== J-Quants日次データ取得開始 ===")

    try:
        with JobContext("J-Quants日次データ取得") as job:
            # データディレクトリ作成
            settings.paths.data_dir.mkdir(parents=True, exist_ok=True)
            db_path = settings.paths.jquants_db

            logger.info(f"データベースパス: {db_path}")
            logger.info(
                f"設定: max_concurrent_requests={settings.jquants.max_concurrent_requests}, "
                f"batch_size={settings.jquants.batch_size}, "
                f"request_delay={settings.jquants.request_delay}s, "
                f"timeout={settings.jquants.timeout_seconds}s"
            )

            processor = JQuantsDataProcessor(
                max_concurrent_requests=settings.jquants.max_concurrent_requests,
                batch_size=settings.jquants.batch_size,
                request_delay=settings.jquants.request_delay,
                timeout_seconds=settings.jquants.timeout_seconds,
            )

            # データベースの存在確認
            db_exists = db_path.exists()
            logger.info(f"データベース存在: {'はい' if db_exists else 'いいえ'}")

            if not db_exists:
                logger.info("初回実行: 過去5年分のデータを取得します")
                result = processor.get_all_prices_for_past_5_years_to_db_optimized(
                    str(db_path)
                )
            else:
                logger.info("差分更新を実行します")
                result = processor.update_prices_to_db_optimized(str(db_path))

            # ジョブ実績メトリクスを通知に追加
            if result:
                job.add_metric(
                    "対象銘柄数",
                    f"{result['codes_to_update']}/{result['total_listed']}",
                )
                job.add_metric("更新銘柄数", str(result["codes_updated"]))
                job.add_metric("新規レコード数", str(result["records_inserted"]))
                if result["codes_failed"] > 0:
                    job.add_metric("失敗銘柄数", str(result["codes_failed"]))
                    job.add_warning(f"{result['codes_failed']}銘柄の取得に失敗しました")
                if result["total_listed"] < 100:
                    job.add_warning(
                        f"上場銘柄数が異常に少ないです: "
                        f"{result['total_listed']}（通常4000+）"
                    )

            # DB統計情報を表示・通知に追加
            stats = processor.get_database_stats(str(db_path))
            if stats:
                logger.info("データベース統計:")
                logger.info(f"  レコード数: {stats.get('record_count', 'N/A')}")
                logger.info(f"  銘柄数: {stats.get('code_count', 'N/A')}")
                logger.info(f"  データ期間: {stats.get('date_range', 'N/A')}")
                job.add_metric("DBレコード数", str(stats.get("record_count", "N/A")))
                job.add_metric("DB銘柄数", str(stats.get("code_count", "N/A")))
                job.add_metric("データ期間", str(stats.get("date_range", "N/A")))

        logger.info("=== J-Quants日次データ取得完了 ===")

        # チェーン実行: J-Quants完了後にDaily Analysisを起動（DB競合回避）
        if chain:
            logger.info("=== チェーン実行: Daily Analysis開始 ===")
            try:
                import subprocess

                result = subprocess.run(
                    [sys.executable, "scripts/run_daily_analysis.py"],
                    cwd=str(Path(__file__).parent.parent),
                    capture_output=False,
                )
                if result.returncode != 0:
                    logger.error(
                        f"Daily Analysisが終了コード {result.returncode} で終了"
                    )
            except Exception as e:
                logger.error(f"Daily Analysisでエラー: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        logger.error(
            "環境変数 EMAIL, PASSWORD が正しく設定されているか確認してください"
        )
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="J-Quants日次データ取得")
    parser.add_argument(
        "--no-chain",
        action="store_true",
        help="後続ジョブ（Daily Analysis, Integrated Analysis）を起動しない",
    )
    args = parser.parse_args()

    main(chain=not args.no_chain)
