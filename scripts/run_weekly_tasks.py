#!/usr/bin/env python3
"""
週次財務データ取得スクリプト（J-Quants Statements API）
cronから実行される週次タスク

yfinanceから J-Quants Statements API に移行
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime

from market_pipeline.config import get_settings
from market_pipeline.utils.slack_notifier import JobContext


class ColoredFormatter(logging.Formatter):
    """カラー付きログフォーマッター"""

    COLORS = {
        "DEBUG": "\033[36m",  # シアン
        "INFO": "\033[32m",  # 緑
        "WARNING": "\033[33m",  # 黄
        "ERROR": "\033[31m",  # 赤
        "CRITICAL": "\033[35m",  # マゼンタ
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging():
    """ログ設定 - バックエンドモジュールのログをカラー表示"""
    handler = logging.StreamHandler()
    handler.setFormatter(
        ColoredFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def parse_args():
    """コマンドライン引数の解析"""
    parser = argparse.ArgumentParser(description="週次財務データ取得スクリプト")
    parser.add_argument(
        "--statements-only", action="store_true", help="財務諸表データ取得のみ実行"
    )
    parser.add_argument("--analysis-only", action="store_true", help="統合分析のみ実行")
    return parser.parse_args()


def main():
    """週次財務データ取得処理"""
    setup_logging()
    args = parse_args()
    settings = get_settings()

    # オプション競合チェック
    if sum([args.statements_only, args.analysis_only]) > 1:
        print("エラー: --statements-only, --analysis-only は同時に指定できません")
        sys.exit(1)

    # デフォルトは全て実行
    run_statements = not (args.analysis_only)
    run_analysis = not (args.statements_only)

    print(f"=== 週次タスク開始 {datetime.now()} ===")

    try:
        with JobContext("週次タスク") as job:
            if run_statements:
                print(f"=== J-Quants Statements データ取得開始 {datetime.now()} ===")
                print(
                    f"    設定: max_concurrent_requests={settings.jquants.max_concurrent_requests}, "
                    f"batch_size={settings.jquants.batch_size}, "
                    f"request_delay={settings.jquants.request_delay}s"
                )

                # 1. 財務諸表データを取得
                from market_pipeline.jquants.statements_processor import (
                    JQuantsStatementsProcessor,
                )

                processor = JQuantsStatementsProcessor(
                    max_concurrent_requests=settings.jquants.max_concurrent_requests,
                    batch_size=settings.jquants.batch_size,
                    request_delay=settings.jquants.request_delay,
                )
                processor.get_all_statements(str(settings.paths.statements_db))
                job.add_metric("財務データ取得", "完了")
                print(f"=== 財務諸表データ取得完了 {datetime.now()} ===")

                # 2. 財務指標を計算
                print(f"=== 財務指標計算開始 {datetime.now()} ===")
                from market_pipeline.jquants.fundamentals_calculator import (
                    FundamentalsCalculator,
                )

                calculator = FundamentalsCalculator(
                    statements_db_path=str(settings.paths.statements_db),
                    jquants_db_path=str(settings.paths.jquants_db),
                )
                processed = calculator.update_all_fundamentals(
                    str(settings.paths.statements_db)
                )
                job.add_metric("財務指標計算", f"{processed}銘柄")
                print(f"    {processed} 銘柄の財務指標を計算しました")
                print(f"=== 財務指標計算完了 {datetime.now()} ===")

            if run_analysis:
                print(f"=== 統合分析処理開始 {datetime.now()} ===")
                analysis_script_path = (
                    settings.paths.base_dir
                    / "src"
                    / "market_pipeline"
                    / "analysis"
                    / "integrated_analysis2.py"
                )
                subprocess.run([sys.executable, str(analysis_script_path)], check=True)
                job.add_metric("統合分析", "完了")
                print(f"=== 統合分析処理完了 {datetime.now()} ===")

        print(f"=== 週次タスク完了 {datetime.now()} ===")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
