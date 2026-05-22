"""Orchestration layer for the ``/sync-notion`` skill.

``SyncService`` ties together the markdown converter, image uploader and
page repository so a single ``run()`` call performs:

1. Resolve target report directory (``output/reports/stocks/...``).
2. Read ``base_report.md`` / ``deep_research_report.md`` / ``chart.png``.
3. Convert Markdown to Notion blocks (with image upload).
4. Search existing pages, archive duplicates, create the new page and
   append remaining blocks 100 at a time.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Protocol

from notion_export.exceptions import (
    ReportDirectoryNotFoundError,
    TooManyExistingPagesError,
)
from notion_export.image_uploader import DryRunImageUploader, ImageUploader
from notion_export.markdown_converter import convert
from notion_export.page_repository import NotionPageRepository

logger = logging.getLogger(__name__)


_BLOCKS_PER_CREATE = 100
_IMAGE_FILENAME = "chart.png"
_BASE_REPORT_FILENAME = "base_report.md"
_DEEP_REPORT_FILENAME = "deep_research_report.md"


@dataclass(frozen=True)
class SyncResult:
    page_id: Optional[str]
    page_title: str
    block_count: int
    archived_page_ids: list[str]
    image_uploaded: bool
    warnings: list[str]
    dry_run: bool
    report_dir: str

    def to_dict(self) -> dict:
        return {
            "page_id": self.page_id,
            "page_title": self.page_title,
            "block_count": self.block_count,
            "archived_page_ids": list(self.archived_page_ids),
            "image_uploaded": self.image_uploaded,
            "warnings": list(self.warnings),
            "dry_run": self.dry_run,
            "report_dir": self.report_dir,
        }


class _NameResolver(Protocol):
    def get_stock_by_code(self, code: str) -> Optional[dict]: ...


class SyncService:
    def __init__(
        self,
        *,
        repository: NotionPageRepository,
        image_uploader: Any,
        master_db: Optional[_NameResolver],
        parent_page_id: str,
        reports_root: Path,
    ) -> None:
        self._repo = repository
        self._image_uploader = image_uploader
        self._master_db = master_db
        self._parent_page_id = parent_page_id
        self._reports_root = reports_root

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        code: Optional[str],
        report_dir: Optional[Path],
        dry_run: bool,
        skip_deep: bool,
        stdout=None,
    ) -> SyncResult:
        warnings: list[str] = []
        resolved_dir = self._resolve_report_dir(code=code, report_dir=report_dir)
        logger.info("Resolved report directory: %s", resolved_dir)

        resolved_code = code or self._infer_code_from_dir(resolved_dir)
        stock_name = self._resolve_stock_name(resolved_code)
        title = f"{stock_name}（{resolved_code}）投資分析"

        base_path = resolved_dir / _BASE_REPORT_FILENAME
        if not base_path.exists():
            raise ReportDirectoryNotFoundError(
                f"base_report.md が見つかりません: {base_path}"
            )
        base_md = base_path.read_text(encoding="utf-8")

        deep_path = resolved_dir / _DEEP_REPORT_FILENAME
        deep_md = (
            deep_path.read_text(encoding="utf-8")
            if (deep_path.exists() and not skip_deep)
            else None
        )

        image_path = resolved_dir / _IMAGE_FILENAME
        file_id: Optional[str] = None
        image_uploaded = False
        if image_path.exists():
            file_id = self._image_uploader.upload(image_path)
            if file_id is None:
                warnings.append(f"画像アップロード失敗: {image_path.name}")
            else:
                image_uploaded = not str(file_id).startswith("<dry-run:")

        def image_resolver(src: str) -> Optional[str]:
            base = src.split("/")[-1].split("\\")[-1]
            if base == _IMAGE_FILENAME and file_id:
                return file_id
            return None

        blocks = convert(base_md, image_resolver=image_resolver)
        if deep_md:
            deep_blocks = convert(
                deep_md,
                image_resolver=image_resolver,
                wrap_in_toggle="Deep Research",
            )
            blocks.extend(deep_blocks)

        if dry_run:
            payload = {
                "title": title,
                "block_count": len(blocks),
                "blocks": blocks,
                "warnings": warnings,
            }
            stream = stdout or sys.stdout
            stream.write(json.dumps(payload, ensure_ascii=False))
            stream.write("\n")
            return SyncResult(
                page_id=None,
                page_title=title,
                block_count=len(blocks),
                archived_page_ids=[],
                image_uploaded=image_uploaded,
                warnings=warnings,
                dry_run=True,
                report_dir=str(resolved_dir),
            )

        archived = self._handle_existing_pages(
            parent_id=self._parent_page_id,
            title=title,
            code=resolved_code,
            stock_name=stock_name,
        )

        page_id = self._repo.create_page(
            self._parent_page_id, title, blocks[:_BLOCKS_PER_CREATE]
        )
        if len(blocks) > _BLOCKS_PER_CREATE:
            self._repo.append_blocks(page_id, blocks[_BLOCKS_PER_CREATE:])

        return SyncResult(
            page_id=page_id,
            page_title=title,
            block_count=len(blocks),
            archived_page_ids=archived,
            image_uploaded=image_uploaded,
            warnings=warnings,
            dry_run=False,
            report_dir=str(resolved_dir),
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_report_dir(
        self, *, code: Optional[str], report_dir: Optional[Path]
    ) -> Path:
        if report_dir is not None:
            p = Path(report_dir)
            if not p.exists() or not p.is_dir():
                raise ReportDirectoryNotFoundError(
                    f"レポートディレクトリが見つかりません: {report_dir}"
                )
            return p
        if not code:
            raise ReportDirectoryNotFoundError(
                "銘柄コードまたは --report-dir のいずれかを指定してください"
            )
        root = self._reports_root
        if not root.exists():
            raise ReportDirectoryNotFoundError(
                f"レポート格納ディレクトリが見つかりません: {root}"
            )
        suffix = f"-{code}-analysis"
        candidates = [
            d for d in root.iterdir() if d.is_dir() and d.name.endswith(suffix)
        ]
        if not candidates:
            raise ReportDirectoryNotFoundError(
                f"レポートディレクトリが見つかりません (code={code})"
            )
        candidates.sort(key=lambda p: p.name, reverse=True)
        return candidates[0]

    def _infer_code_from_dir(self, report_dir: Path) -> str:
        # Names look like YYYYMMDD-HHMM-{code}-analysis
        match = re.search(r"-(\d{4,5})-analysis$", report_dir.name)
        if match:
            return match.group(1)
        return "0000"

    def _resolve_stock_name(self, code: str) -> str:
        if self._master_db is None:
            return f"銘柄{code}"
        try:
            row = self._master_db.get_stock_by_code(code)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("master_db lookup failed: %s", exc)
            row = None
        if not row:
            return f"銘柄{code}"
        name = row.get("name") or row.get("company_name") or ""
        return str(name) if name else f"銘柄{code}"

    def _handle_existing_pages(
        self, *, parent_id: str, title: str, code: str, stock_name: str
    ) -> list[str]:
        # Confirm parent exists - this raises ParentPageNotFoundError on 404
        self._repo.fetch_parent(parent_id)
        prefix = f"{stock_name}（{code}）"
        existing = self._repo.search_children_by_title_prefix(parent_id, prefix)
        if len(existing) >= 3:
            raise TooManyExistingPagesError(
                count=len(existing),
                page_ids=[p["id"] for p in existing],
            )
        archived_ids: list[str] = []
        for hit in existing:
            self._repo.archive_page(hit["id"])
            archived_ids.append(hit["id"])
        return archived_ids


# ---------------------------------------------------------------------------
# Convenience builder used by the CLI
# ---------------------------------------------------------------------------


def build_sync_service(
    *,
    parent_page_id: str,
    api_token: str,
    dry_run: bool,
    reports_root: Path,
    master_db: Optional[_NameResolver],
    timeout_seconds: int,
    api_version: str,
    throttle_seconds: float,
) -> SyncService:
    from notion_export.page_repository import RestNotionPageRepository

    repo = RestNotionPageRepository(
        api_token=((api_token or "<dry-run>") if dry_run else api_token),
        timeout_seconds=timeout_seconds,
        api_version=api_version,
        throttle_seconds=throttle_seconds,
    )
    uploader: Any
    if dry_run:
        uploader = DryRunImageUploader()
    else:
        uploader = ImageUploader(
            api_token=api_token,
            timeout_seconds=timeout_seconds,
            api_version=api_version,
        )

    return SyncService(
        repository=repo,
        image_uploader=uploader,
        master_db=master_db,
        parent_page_id=parent_page_id,
        reports_root=reports_root,
    )


__all__ = ["SyncService", "SyncResult", "build_sync_service"]
