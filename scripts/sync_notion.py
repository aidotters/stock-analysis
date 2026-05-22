"""CLI entry point for the ``/sync-notion`` skill.

Usage:
    python scripts/sync_notion.py 7804
    python scripts/sync_notion.py 7804 --dry-run
    python scripts/sync_notion.py --report-dir output/reports/stocks/...
    python scripts/sync_notion.py 7804 --skip-deep-research
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_pipeline.config import get_settings  # noqa: E402
from market_pipeline.master.master_db import StockMasterDB  # noqa: E402
from notion_export.exceptions import (  # noqa: E402
    NotionApiError,
    NotionExportError,
    ParentPageNotFoundError,
    ReportDirectoryNotFoundError,
    TooManyExistingPagesError,
)
from notion_export.sync_service import build_sync_service  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sync_notion",
        description="投資分析レポートを Notion に投入する",
    )
    parser.add_argument(
        "code",
        nargs="?",
        help="4桁銘柄コード (省略時は --report-dir を必須とする)",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=None,
        help="投入対象のレポートディレクトリを明示指定する",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Notion API を呼ばずに blocks JSON のみ出力する",
    )
    parser.add_argument(
        "--skip-deep-research",
        action="store_true",
        help="deep_research_report.md を取り込まない",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _build_parser().parse_args(argv)

    if not args.code and not args.report_dir:
        print(
            "銘柄コードまたは --report-dir のいずれかを指定してください",
            file=sys.stderr,
        )
        return 2

    settings = get_settings()
    notion = settings.notion
    if not args.dry_run:
        if not notion.parent_page_id:
            print(
                "NOTION_PARENT_PAGE_ID が設定されていません",
                file=sys.stderr,
            )
            return 2
        if not notion.api_token:
            print(
                "NOTION_API_TOKEN が設定されていません",
                file=sys.stderr,
            )
            return 2

    reports_root = settings.paths.output_dir / "reports" / "stocks"

    master_db_path = settings.paths.master_db
    master_db = None
    try:
        if master_db_path and Path(master_db_path).exists():
            master_db = StockMasterDB(db_path=str(master_db_path))
    except Exception as exc:  # pragma: no cover - defensive
        print(f"master.db 読み込み失敗 (フォールバック使用): {exc}", file=sys.stderr)

    service = build_sync_service(
        parent_page_id=notion.parent_page_id,
        api_token=notion.api_token,
        dry_run=args.dry_run,
        reports_root=reports_root,
        master_db=master_db,
        timeout_seconds=notion.api_timeout_seconds,
        api_version=notion.api_version,
        throttle_seconds=notion.api_throttle_seconds,
    )

    try:
        result = service.run(
            code=args.code,
            report_dir=args.report_dir,
            dry_run=args.dry_run,
            skip_deep=args.skip_deep_research,
        )
    except ReportDirectoryNotFoundError as exc:
        print(f"レポートディレクトリが見つかりません: {exc}", file=sys.stderr)
        return 3
    except ParentPageNotFoundError as exc:
        print(f"親ページが見つかりません: {exc}", file=sys.stderr)
        return 4
    except TooManyExistingPagesError as exc:
        print(str(exc), file=sys.stderr)
        return 5
    except NotionApiError as exc:
        print(f"Notion API エラー: {exc}", file=sys.stderr)
        return 6
    except NotionExportError as exc:
        print(f"sync-notion エラー: {exc}", file=sys.stderr)
        return 1

    if not args.dry_run:
        # dry-run already wrote blocks JSON to stdout; print just the SyncResult here.
        print(json.dumps(result.to_dict(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
