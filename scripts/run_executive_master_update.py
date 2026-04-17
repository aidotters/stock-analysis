#!/usr/bin/env python3
"""役員マスター月次更新スクリプト.

指定銘柄（デフォルト: master.db の is_active=1 全件）について、
EDINET 有価証券報告書から最新の法定役員情報を取得し、
`statements.db` の `executives` テーブルへ UPSERT する。

Phase 0 PoC 確定事項:
- `executives.edinet_source_doc_id` のキャッシュを使って2回目以降のバッチを高速化
- 並列度は `settings.executives.max_parallel_fetch` (デフォルト3)
- Slack通知は `JobContext` で統一

Usage:
    python scripts/run_executive_master_update.py                    # 全銘柄
    python scripts/run_executive_master_update.py --codes 7203 9984  # 指定銘柄のみ
    python scripts/run_executive_master_update.py --limit 10         # 先頭10銘柄
    python scripts/run_executive_master_update.py --dry-run          # DB書き込みなし
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

from market_pipeline.config import get_settings
from market_pipeline.executives import (
    EdinetDocResolver,
    EdinetExecutiveFetcher,
    ExecutiveRepository,
)
from market_pipeline.executives.exceptions import EdinetFetchError, ExecutiveError
from market_pipeline.utils.slack_notifier import JobContext

logger = logging.getLogger(__name__)


def load_target_codes(
    codes_arg: Optional[list[str]], limit: Optional[int]
) -> list[str]:
    """バッチ対象の銘柄コード一覧を取得する."""
    if codes_arg:
        return [c.strip() for c in codes_arg if c.strip()]

    settings = get_settings()
    with sqlite3.connect(str(settings.paths.master_db)) as conn:
        query = "SELECT code FROM stocks_master WHERE is_active = 1 ORDER BY code"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        return [row[0] for row in conn.execute(query).fetchall()]


def process_code(
    code: str,
    resolver: EdinetDocResolver,
    fetcher: EdinetExecutiveFetcher,
    repository: ExecutiveRepository,
    *,
    dry_run: bool,
) -> dict:
    """1銘柄の役員マスター更新処理."""
    result: dict = {
        "code": code,
        "doc_id": None,
        "exec_count": 0,
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
        "status": "unknown",
        "error": None,
    }
    try:
        cached_doc_id = repository.get_latest_doc_id(code)
        doc_id = resolver.resolve(code)
        if doc_id is None:
            result["status"] = "no_document"
            logger.warning("有報未提出: code=%s", code)
            return result

        result["doc_id"] = doc_id

        if doc_id == cached_doc_id:
            result["status"] = "unchanged"
            logger.info("新規有報なし（既存継続）: code=%s doc_id=%s", code, doc_id)
            return result

        executives, _ = fetcher.fetch_from_doc_id(doc_id, code=code)
        result["exec_count"] = len(executives)

        if not executives:
            result["status"] = "empty"
            logger.warning("役員ゼロ: code=%s doc_id=%s", code, doc_id)
            return result

        if dry_run:
            result["status"] = "dry_run"
            return result

        counts = repository.upsert_executives(executives, replace_for_code=code)
        result.update(counts)
        result["status"] = "ok"
    except EdinetFetchError as exc:
        result["status"] = "fetch_error"
        result["error"] = str(exc)
        logger.error("EDINET取得失敗 code=%s error=%s", code, exc)
    except ExecutiveError as exc:
        result["status"] = "parse_error"
        result["error"] = str(exc)
        logger.error("解析失敗 code=%s error=%s", code, exc)
    except Exception as exc:  # noqa: BLE001 - バッチで個別エラーを吸収
        result["status"] = "unexpected_error"
        result["error"] = str(exc)
        logger.exception("予期せぬエラー code=%s", code)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="役員マスター月次更新")
    parser.add_argument("--codes", nargs="*", help="対象銘柄コード（空なら全件）")
    parser.add_argument("--limit", type=int, help="対象件数上限（テスト用）")
    parser.add_argument("--dry-run", action="store_true", help="DB書き込みなし")
    parser.add_argument("--parallel", type=int, help="並列度（デフォルト: 設定値）")
    args = parser.parse_args()

    settings = get_settings()
    parallel = args.parallel or settings.executives.max_parallel_fetch

    codes = load_target_codes(args.codes, args.limit)
    if not codes:
        print("対象銘柄が0件です", file=sys.stderr)
        return 1

    print(
        f"=== 役員マスター更新開始 {datetime.now()} 対象={len(codes)}銘柄 並列度={parallel} ===",
        file=sys.stderr,
    )

    if not settings.edinet.api_key:
        print("ERROR: EDINET_API_KEY が未設定です（.envを確認）", file=sys.stderr)
        return 2

    with JobContext("役員マスター月次更新") as job:
        repository = ExecutiveRepository()
        repository.initialize_tables()
        resolver = EdinetDocResolver(repository=repository)
        fetcher = EdinetExecutiveFetcher()

        summary = {
            "total": len(codes),
            "ok": 0,
            "unchanged": 0,
            "no_document": 0,
            "empty": 0,
            "fetch_error": 0,
            "parse_error": 0,
            "dry_run": 0,
        }
        total_inserted = 0
        total_updated = 0
        total_deleted = 0

        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(
                    process_code,
                    code,
                    resolver,
                    fetcher,
                    repository,
                    dry_run=args.dry_run,
                ): code
                for code in codes
            }
            for future in as_completed(futures):
                r = future.result()
                status = r["status"]
                summary[status] = summary.get(status, 0) + 1
                total_inserted += r.get("inserted", 0) or 0
                total_updated += r.get("updated", 0) or 0
                total_deleted += r.get("deleted", 0) or 0
                print(
                    f"[{r['code']}] status={status} doc_id={r.get('doc_id')} "
                    f"count={r['exec_count']} inserted={r.get('inserted', 0)} "
                    f"updated={r.get('updated', 0)} deleted={r.get('deleted', 0)}",
                    file=sys.stderr,
                )

        job.add_metric("対象銘柄数", str(summary["total"]))
        job.add_metric("成功", str(summary.get("ok", 0)))
        job.add_metric("スキップ（有報未更新）", str(summary.get("unchanged", 0)))
        if args.dry_run:
            job.add_metric("dry-run（パース成功）", str(summary.get("dry_run", 0)))
        job.add_metric("有報なし", str(summary.get("no_document", 0)))
        if summary.get("empty", 0):
            job.add_metric("役員ゼロ", str(summary["empty"]))
        job.add_metric("取得失敗", str(summary.get("fetch_error", 0)))
        job.add_metric("解析失敗", str(summary.get("parse_error", 0)))
        if summary.get("unexpected_error", 0):
            job.add_metric("予期せぬエラー", str(summary["unexpected_error"]))
        job.add_metric("INSERT", str(total_inserted))
        job.add_metric("UPDATE", str(total_updated))
        job.add_metric("DELETE(旧レコ)", str(total_deleted))

        total_errors = (
            summary.get("fetch_error", 0)
            + summary.get("parse_error", 0)
            + summary.get("unexpected_error", 0)
        )
        if total_errors > 0:
            job.add_warning(
                f"失敗あり: fetch={summary.get('fetch_error', 0)} "
                f"parse={summary.get('parse_error', 0)} "
                f"unexpected={summary.get('unexpected_error', 0)}"
            )

    print(f"=== 役員マスター更新完了 {datetime.now()} ===", file=sys.stderr)
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
