"""役員マスター月次バッチ (`scripts/run_executive_master_update.py`) のテスト.

doc_id 比較による DL スキップ最適化の動作確認。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "run_executive_master_update.py"
)


@pytest.fixture(scope="module")
def batch_module():
    """scripts/run_executive_master_update.py を import する."""
    spec = importlib.util.spec_from_file_location(
        "run_executive_master_update", _SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_executive_master_update"] = module
    spec.loader.exec_module(module)
    return module


def _make_mocks(cached_doc_id, resolved_doc_id, executives=None):
    """resolver / fetcher / repository のモックを生成."""
    repository = MagicMock()
    repository.get_latest_doc_id.return_value = cached_doc_id
    repository.upsert_executives.return_value = {
        "inserted": len(executives or []),
        "updated": 0,
        "deleted": 0,
    }

    resolver = MagicMock()
    resolver.resolve.return_value = resolved_doc_id

    fetcher = MagicMock()
    fetcher.fetch_from_doc_id.return_value = (executives or [], resolved_doc_id)

    return resolver, fetcher, repository


def test_process_code_skips_when_doc_id_unchanged(batch_module):
    """cached と resolved が同じ doc_id ならスキップし、fetcher を呼ばない."""
    resolver, fetcher, repository = _make_mocks(
        cached_doc_id="S100ABCD", resolved_doc_id="S100ABCD"
    )

    result = batch_module.process_code(
        "7203", resolver, fetcher, repository, dry_run=False
    )

    assert result["status"] == "unchanged"
    assert result["doc_id"] == "S100ABCD"
    assert result["exec_count"] == 0
    fetcher.fetch_from_doc_id.assert_not_called()
    repository.upsert_executives.assert_not_called()


def test_process_code_fetches_when_doc_id_is_new(batch_module):
    """新規 doc_id が返るケースでは DL・upsert が実行される."""
    executives = [MagicMock()]
    resolver, fetcher, repository = _make_mocks(
        cached_doc_id="S100OLD0",
        resolved_doc_id="S100NEW0",
        executives=executives,
    )

    result = batch_module.process_code(
        "7203", resolver, fetcher, repository, dry_run=False
    )

    assert result["status"] == "ok"
    assert result["doc_id"] == "S100NEW0"
    assert result["exec_count"] == 1
    fetcher.fetch_from_doc_id.assert_called_once_with("S100NEW0", code="7203")
    repository.upsert_executives.assert_called_once()


def test_process_code_fetches_when_no_cached_doc_id(batch_module):
    """初回（cached=None）で resolver が doc_id を返せば DL・upsert が実行される."""
    executives = [MagicMock(), MagicMock()]
    resolver, fetcher, repository = _make_mocks(
        cached_doc_id=None,
        resolved_doc_id="S100FIRST",
        executives=executives,
    )

    result = batch_module.process_code(
        "7203", resolver, fetcher, repository, dry_run=False
    )

    assert result["status"] == "ok"
    assert result["doc_id"] == "S100FIRST"
    assert result["exec_count"] == 2
    fetcher.fetch_from_doc_id.assert_called_once()
    repository.upsert_executives.assert_called_once()


def test_process_code_returns_no_document_when_resolver_returns_none(batch_module):
    """resolver が None を返した場合は no_document でスキップ."""
    resolver, fetcher, repository = _make_mocks(
        cached_doc_id=None, resolved_doc_id=None
    )

    result = batch_module.process_code(
        "9999", resolver, fetcher, repository, dry_run=False
    )

    assert result["status"] == "no_document"
    fetcher.fetch_from_doc_id.assert_not_called()
    repository.upsert_executives.assert_not_called()


def test_process_code_dry_run_does_not_upsert(batch_module):
    """dry_run=True かつ新規 doc_id のケースは fetch はするが upsert はスキップ."""
    executives = [MagicMock()]
    resolver, fetcher, repository = _make_mocks(
        cached_doc_id=None,
        resolved_doc_id="S100DRY00",
        executives=executives,
    )

    result = batch_module.process_code(
        "7203", resolver, fetcher, repository, dry_run=True
    )

    assert result["status"] == "dry_run"
    fetcher.fetch_from_doc_id.assert_called_once()
    repository.upsert_executives.assert_not_called()
