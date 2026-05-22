"""Integration tests for ``notion_export.sync_service``."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Optional

import pytest

from notion_export.exceptions import TooManyExistingPagesError
from notion_export.image_uploader import DryRunImageUploader
from notion_export.page_repository import FakeNotionPageRepository
from notion_export.sync_service import SyncService

_FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "notion_reports"
_SAMPLE_CODE = "7804"
_SAMPLE_NAME = "ビーアンドピー"


class _StubMaster:
    def __init__(self, name: Optional[str] = _SAMPLE_NAME) -> None:
        self._name = name

    def get_stock_by_code(self, code: str) -> Optional[dict]:
        if self._name is None:
            return None
        return {"code": code, "name": self._name}


@pytest.fixture
def reports_root(tmp_path: Path) -> Path:
    # Copy fixture into a timestamped dir so SyncService's auto-resolution works
    target_dir = tmp_path / f"20260413-2244-{_SAMPLE_CODE}-analysis"
    target_dir.mkdir()
    src = _FIXTURE_ROOT / "sample_7804"
    for name in ("base_report.md", "deep_research_report.md", "chart.png"):
        (target_dir / name).write_bytes((src / name).read_bytes())
    return tmp_path


def _build_service(
    repo: FakeNotionPageRepository,
    reports_root: Path,
    *,
    master_db=None,
) -> SyncService:
    repo.add_parent("PARENT")
    return SyncService(
        repository=repo,
        image_uploader=_FakeImageUploader(),
        master_db=master_db or _StubMaster(),
        parent_page_id="PARENT",
        reports_root=reports_root,
    )


class _FakeImageUploader:
    def __init__(self) -> None:
        self.calls: list[Path] = []

    def upload(self, path: Path) -> str:
        self.calls.append(path)
        return f"FAKE_FILE_{path.name}"


# ---------------------------------------------------------------------------
# Case 1: new page (existing 0)
# ---------------------------------------------------------------------------


def test_new_page_created_and_append_called_for_large_reports(
    reports_root: Path,
) -> None:
    repo = FakeNotionPageRepository()
    service = _build_service(repo, reports_root)

    result = service.run(
        code=_SAMPLE_CODE,
        report_dir=None,
        dry_run=False,
        skip_deep=False,
    )

    assert result.page_id is not None
    assert result.page_title == f"{_SAMPLE_NAME}（{_SAMPLE_CODE}）投資分析"
    assert result.archived_page_ids == []
    assert result.image_uploaded is True
    assert result.block_count > 0
    # Notion only allows 100 blocks per create; expect append for the rest.
    assert len(repo.pages) == 1
    page = repo.pages[result.page_id]
    # Title prefix in the created page should match
    assert page["title"].startswith(f"{_SAMPLE_NAME}（{_SAMPLE_CODE}）")
    # The Deep Research sample is big enough to push us past 100 blocks
    if result.block_count > 100:
        assert repo.append_calls, "expected append_blocks to be called for >100 blocks"


# ---------------------------------------------------------------------------
# Case 2: overwrite (1 existing)
# ---------------------------------------------------------------------------


def test_overwrite_archives_then_creates(reports_root: Path) -> None:
    repo = FakeNotionPageRepository()
    service = _build_service(repo, reports_root)
    # 1st run
    first = service.run(
        code=_SAMPLE_CODE, report_dir=None, dry_run=False, skip_deep=True
    )
    # 2nd run should archive the previous one
    second = service.run(
        code=_SAMPLE_CODE, report_dir=None, dry_run=False, skip_deep=True
    )
    assert second.archived_page_ids == [first.page_id]
    assert repo.pages[first.page_id]["archived"] is True
    assert second.page_id != first.page_id


# ---------------------------------------------------------------------------
# Case 3: too many existing pages
# ---------------------------------------------------------------------------


def test_three_existing_raises(reports_root: Path) -> None:
    repo = FakeNotionPageRepository()
    repo.add_parent("PARENT")
    title = f"{_SAMPLE_NAME}（{_SAMPLE_CODE}）投資分析"
    for _ in range(3):
        repo.create_page("PARENT", title, [])
    service = SyncService(
        repository=repo,
        image_uploader=_FakeImageUploader(),
        master_db=_StubMaster(),
        parent_page_id="PARENT",
        reports_root=reports_root,
    )
    with pytest.raises(TooManyExistingPagesError):
        service.run(code=_SAMPLE_CODE, report_dir=None, dry_run=False, skip_deep=True)


# ---------------------------------------------------------------------------
# Case 4: missing chart.png -> image block omitted, warnings recorded
# ---------------------------------------------------------------------------


def test_missing_chart_png_skips_image(reports_root: Path) -> None:
    # Remove the image
    (reports_root / f"20260413-2244-{_SAMPLE_CODE}-analysis" / "chart.png").unlink()
    repo = FakeNotionPageRepository()
    service = _build_service(repo, reports_root)

    result = service.run(
        code=_SAMPLE_CODE, report_dir=None, dry_run=False, skip_deep=True
    )
    assert result.image_uploaded is False
    # The image block should fall back to a placeholder; no file_upload id present
    page = repo.pages[result.page_id]
    blocks = page["blocks"] + [b for _, chunk in repo.append_calls for b in chunk]
    file_uploads = [
        b
        for b in blocks
        if b.get("type") == "image" and b["image"].get("type") == "file_upload"
    ]
    assert file_uploads == []


# ---------------------------------------------------------------------------
# Case 5: skip_deep removes toggle block
# ---------------------------------------------------------------------------


def test_skip_deep_excludes_toggle(reports_root: Path) -> None:
    repo = FakeNotionPageRepository()
    service = _build_service(repo, reports_root)
    result = service.run(
        code=_SAMPLE_CODE, report_dir=None, dry_run=False, skip_deep=True
    )
    page = repo.pages[result.page_id]
    all_blocks = page["blocks"] + [b for _, chunk in repo.append_calls for b in chunk]
    assert not any(b["type"] == "toggle" for b in all_blocks)


# ---------------------------------------------------------------------------
# Case 6: dry-run does not call Notion API and emits blocks JSON
# ---------------------------------------------------------------------------


def test_dry_run_no_api_call_and_stdout_json(reports_root: Path) -> None:
    repo = FakeNotionPageRepository()
    service = SyncService(
        repository=repo,
        image_uploader=DryRunImageUploader(),
        master_db=_StubMaster(),
        parent_page_id="PARENT",  # not added to fake -> proves we don't call fetch_parent
        reports_root=reports_root,
    )
    stdout = io.StringIO()
    result = service.run(
        code=_SAMPLE_CODE,
        report_dir=None,
        dry_run=True,
        skip_deep=False,
        stdout=stdout,
    )
    assert result.dry_run is True
    assert result.page_id is None
    assert repo.pages == {}
    assert repo.append_calls == []
    output = stdout.getvalue().rstrip("\n")
    payload = json.loads(output)
    assert payload["title"].endswith("投資分析")
    assert payload["block_count"] == result.block_count
    assert isinstance(payload["blocks"], list)


# ---------------------------------------------------------------------------
# Snapshot test: dry-run JSON structure stable
# ---------------------------------------------------------------------------


def test_dry_run_snapshot(reports_root: Path) -> None:
    """Confirm the dry-run blocks JSON has the expected top-level types
    in order. Full byte-for-byte snapshot is brittle because the Deep
    Research toggle is a single block at the tail.
    """
    repo = FakeNotionPageRepository()
    service = SyncService(
        repository=repo,
        image_uploader=DryRunImageUploader(),
        master_db=_StubMaster(),
        parent_page_id="PARENT",
        reports_root=reports_root,
    )
    stdout = io.StringIO()
    service.run(
        code=_SAMPLE_CODE,
        report_dir=None,
        dry_run=True,
        skip_deep=False,
        stdout=stdout,
    )
    payload = json.loads(stdout.getvalue())
    types = [b["type"] for b in payload["blocks"]]
    # Frontmatter callout -> heading_1 -> ... -> trailing toggle
    assert types[0] == "callout"
    assert types[1] == "heading_1"
    assert types[-1] == "toggle"
