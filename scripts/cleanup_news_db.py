"""data/news_delivery.db の古いレコードを削除するクリーンアップスクリプト。

使用例:
    python scripts/cleanup_news_db.py            # デフォルト90日
    python scripts/cleanup_news_db.py --days 60
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_pipeline.news_delivery.deduplicator import Deduplicator  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Cleanup old delivered_news rows")
    p.add_argument("--days", type=int, default=90)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    dedup = Deduplicator()
    deleted = dedup.cleanup_older_than(days=args.days)
    print(f"Deleted {deleted} row(s) older than {args.days} days")
    return 0


if __name__ == "__main__":
    sys.exit(main())
