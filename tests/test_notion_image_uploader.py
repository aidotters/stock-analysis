"""Tests for ``notion_export.image_uploader``."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from notion_export.image_uploader import DryRunImageUploader, ImageUploader


def _mk_response(status: int, json_body=None, text="") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_body or {}
    resp.text = text
    return resp


@pytest.fixture
def tmp_image(tmp_path: Path) -> Path:
    p = tmp_path / "chart.png"
    p.write_bytes(b"\x89PNG\r\n\x1a\nfakepngdata")
    return p


def test_upload_success(tmp_image: Path) -> None:
    session = MagicMock()
    session.post.side_effect = [
        _mk_response(
            200,
            {"id": "FILE_123", "upload_url": "https://upload.notion.com/abc"},
        ),
        _mk_response(200),
    ]
    uploader = ImageUploader(
        api_token="tok",
        http_session=session,
    )
    file_id = uploader.upload(tmp_image)
    assert file_id == "FILE_123"
    assert session.post.call_count == 2
    # First call: file_uploads endpoint
    first_call = session.post.call_args_list[0]
    assert first_call.args[0].endswith("/v1/file_uploads")


def test_upload_5xx_retries_then_succeeds(tmp_image: Path, mocker) -> None:
    mocker.patch("notion_export.image_uploader.time.sleep")
    session = MagicMock()
    # 1st 500, 2nd 500, 3rd OK on file_uploads
    session.post.side_effect = [
        _mk_response(500, text="err"),
        _mk_response(500, text="err"),
        _mk_response(
            200,
            {"id": "FILE_OK", "upload_url": "https://upload.notion.com/u"},
        ),
        _mk_response(200),
    ]
    uploader = ImageUploader(api_token="tok", http_session=session)
    assert uploader.upload(tmp_image) == "FILE_OK"


def test_upload_5xx_exhausts_returns_none(tmp_image: Path, mocker, caplog) -> None:
    mocker.patch("notion_export.image_uploader.time.sleep")
    session = MagicMock()
    session.post.return_value = _mk_response(503, text="boom")
    uploader = ImageUploader(api_token="tok", http_session=session)
    with caplog.at_level(logging.WARNING):
        result = uploader.upload(tmp_image)
    assert result is None
    assert any("Failed to upload image" in r.message for r in caplog.records)


def test_upload_token_missing_warns_and_skips(tmp_image: Path, caplog) -> None:
    session = MagicMock()
    uploader = ImageUploader(api_token="", http_session=session)
    with caplog.at_level(logging.WARNING):
        result = uploader.upload(tmp_image)
    assert result is None
    assert session.post.call_count == 0
    assert any("NOTION_API_TOKEN unset" in r.message for r in caplog.records)


def test_dry_run_returns_placeholder(tmp_image: Path) -> None:
    uploader = DryRunImageUploader()
    assert uploader.upload(tmp_image) == f"<dry-run:{tmp_image.name}>"
