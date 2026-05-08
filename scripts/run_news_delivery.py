"""launchd エントリポイント: ウォッチリストニュース配信。

使用例:
    python scripts/run_news_delivery.py --slot evening
    python scripts/run_news_delivery.py --slot evening --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_pipeline.news_delivery.delivery_service import (  # noqa: E402
    DeliveryService,
    build_default_service,
)
from market_pipeline.utils.slack_notifier import JobContext  # noqa: E402

SLOT_LABEL_JA = {"morning": "朝", "noon": "昼", "evening": "夜"}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watchlist news delivery")
    p.add_argument("--slot", required=True, choices=["morning", "noon", "evening"])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--watchlist", default="default")
    p.add_argument("--config", default="config/news_sources.yaml")
    p.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="適時開示の取得対象期間 (日)。未指定時は設定値 (既定7日) を使用",
    )
    p.add_argument(
        "--sources",
        default=None,
        help=(
            "取得ソースの CSV (例: 'disclosure,general_news,ir_release')。"
            "未指定時は全て有効"
        ),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    slot_ja = SLOT_LABEL_JA.get(args.slot, args.slot)
    job_name = f"ウォッチリストニュース配信({slot_ja})"

    with JobContext(job_name) as job:
        sources_arg = (
            tuple(s.strip() for s in args.sources.split(",") if s.strip())
            if args.sources
            else None
        )
        service: DeliveryService = build_default_service(
            watchlist_name=args.watchlist,
            config_path=args.config,
            lookback_days=args.lookback_days,
            **({"sources": sources_arg} if sources_arg else {}),
        )
        # ジョブ通知へ警告/メトリクスを橋渡しするため、
        # サービスのhandlerをJobContextへ差し替えた上で再構築する。
        service._warn = job.add_warning  # type: ignore[attr-defined]
        service._metric = job.add_metric  # type: ignore[attr-defined]

        result = service.run(args.slot, dry_run=args.dry_run)
        logging.info(
            "Delivery complete: codes=%d new=%d skipped=%d messages=%d",
            result["code_count"],
            result["new_items"],
            result["skipped_items"],
            result["messages"],
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
