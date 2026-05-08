"""WatchList CRUD + スキーマ検証テスト。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_pipeline.news_delivery.exceptions import (
    WatchListError,
    WatchListSchemaError,
)
from market_pipeline.news_delivery.models import WatchListEntry
from market_pipeline.news_delivery.watchlist import WatchList, make_entry


@pytest.fixture
def tmp_watchlists(tmp_path: Path) -> Path:
    d = tmp_path / "watchlists"
    d.mkdir()
    return d


def _new_wl(tmp_dir: Path) -> WatchList:
    return WatchList.load(
        "default", watchlists_dir=tmp_dir, master_db_path=tmp_dir / "master.db"
    )


def test_add_normal(tmp_watchlists: Path) -> None:
    wl = _new_wl(tmp_watchlists)
    entry = make_entry(code="7203", tag="holding", priority="high", note="test")
    wl.add(entry)
    wl.save()

    raw = json.loads((tmp_watchlists / "default.json").read_text(encoding="utf-8"))
    assert len(raw) == 1
    assert raw[0]["code"] == "7203"
    assert raw[0]["tag"] == "holding"


def test_add_invalid_5digit_code(tmp_watchlists: Path) -> None:
    with pytest.raises(WatchListSchemaError):
        make_entry(code="72031", tag="holding", priority="high")


def test_add_invalid_alpha_code(tmp_watchlists: Path) -> None:
    with pytest.raises(WatchListSchemaError):
        make_entry(code="ABCD", tag="holding", priority="high")


def test_add_invalid_tag(tmp_watchlists: Path) -> None:
    with pytest.raises(WatchListSchemaError):
        make_entry(code="7203", tag="invalid", priority="high")


def test_add_invalid_priority(tmp_watchlists: Path) -> None:
    with pytest.raises(WatchListSchemaError):
        make_entry(code="7203", tag="holding", priority="urgent")


def test_update_only_priority_preserves_other_fields(tmp_watchlists: Path) -> None:
    wl = _new_wl(tmp_watchlists)
    entry = make_entry(code="7203", tag="holding", priority="high", note="memo")
    wl.add(entry)
    wl.save()

    wl2 = _new_wl(tmp_watchlists)
    updated = wl2.update("7203", priority="mid")
    assert updated.priority == "mid"
    assert updated.tag == "holding"
    assert updated.note == "memo"
    assert updated.added_at == entry.added_at


def test_remove(tmp_watchlists: Path) -> None:
    wl = _new_wl(tmp_watchlists)
    wl.add(make_entry(code="7203", tag="holding", priority="high"))
    wl.add(make_entry(code="9984", tag="considering", priority="mid"))
    wl.save()

    wl2 = _new_wl(tmp_watchlists)
    assert wl2.remove("7203") is True
    wl2.save()

    wl3 = _new_wl(tmp_watchlists)
    codes = [e.code for e in wl3.list()]
    assert "7203" not in codes
    assert "9984" in codes


def test_filter_by_tag(tmp_watchlists: Path) -> None:
    wl = _new_wl(tmp_watchlists)
    wl.add(make_entry(code="7203", tag="holding", priority="high"))
    wl.add(make_entry(code="9984", tag="considering", priority="mid"))
    wl.add(make_entry(code="6758", tag="holding", priority="low"))
    holdings = wl.filter_by_tag("holding")
    codes = [e.code for e in holdings]
    assert set(codes) == {"7203", "6758"}


def test_duplicate_add_raises(tmp_watchlists: Path) -> None:
    wl = _new_wl(tmp_watchlists)
    wl.add(make_entry(code="7203", tag="holding", priority="high"))
    with pytest.raises(WatchListError):
        wl.add(make_entry(code="7203", tag="considering", priority="mid"))


def test_load_invalid_json(tmp_watchlists: Path) -> None:
    (tmp_watchlists / "default.json").write_text("not json", encoding="utf-8")
    with pytest.raises(WatchListSchemaError):
        WatchList.load("default", watchlists_dir=tmp_watchlists)


def test_entry_url_hash_independence() -> None:
    # NewsItem.url_hash determinism is tested elsewhere; here just sanity.
    e1 = WatchListEntry(
        code="7203",
        tag="holding",
        priority="high",
        added_at="2026-05-08",  # type: ignore[arg-type]
    )
    e2 = WatchListEntry(
        code="7203",
        tag="holding",
        priority="high",
        added_at="2026-05-08",  # type: ignore[arg-type]
    )
    assert e1 == e2
