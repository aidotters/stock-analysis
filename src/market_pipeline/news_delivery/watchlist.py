"""ウォッチリスト(`data/watchlists/{name}.json`)のCRUDと検索。"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterator, List, Optional

from market_pipeline.config import get_settings
from market_pipeline.news_delivery.exceptions import (
    WatchListError,
    WatchListSchemaError,
)
from market_pipeline.news_delivery.models import WatchListEntry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StockMeta:
    """master.db から解決した銘柄メタデータ。"""

    code: str
    long_name: Optional[str] = None
    sector: Optional[str] = None


class WatchList:
    """ウォッチリストJSONを読み書きする。"""

    def __init__(
        self,
        name: str = "default",
        *,
        watchlists_dir: Optional[Path] = None,
        master_db_path: Optional[Path] = None,
    ) -> None:
        self.name = name
        settings = get_settings()
        if watchlists_dir is None:
            data_dir = settings.paths.data_dir
            assert data_dir is not None
            watchlists_dir = data_dir / "watchlists"
        self._dir = Path(watchlists_dir)
        self._path = self._dir / f"{name}.json"
        self._master_db_path = (
            Path(master_db_path) if master_db_path else settings.paths.master_db
        )
        self._entries: list[WatchListEntry] = []
        self._loaded = False

    @property
    def path(self) -> Path:
        return self._path

    @classmethod
    def load(
        cls,
        name: str = "default",
        *,
        watchlists_dir: Optional[Path] = None,
        master_db_path: Optional[Path] = None,
    ) -> "WatchList":
        wl = cls(
            name,
            watchlists_dir=watchlists_dir,
            master_db_path=master_db_path,
        )
        wl._load_from_disk()
        return wl

    def _load_from_disk(self) -> None:
        self._loaded = True
        if not self._path.exists():
            self._entries = []
            return

        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise WatchListSchemaError(f"Invalid JSON in {self._path}: {e}") from e

        if not isinstance(raw, list):
            raise WatchListSchemaError(
                f"Watchlist root must be a list, got {type(raw).__name__}"
            )

        entries: list[WatchListEntry] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                raise WatchListSchemaError(
                    f"Entry #{i} must be a mapping, got {type(item).__name__}"
                )
            entries.append(WatchListEntry.create(**item))
        self._entries = entries

    def save(self) -> None:
        """アトミックにJSONへ書き込む。"""
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = [e.to_dict() for e in self._entries]
        text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{self.name}.", suffix=".json.tmp", dir=str(self._dir)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp_path, self._path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def list(self) -> list[WatchListEntry]:
        return list(self._entries)

    def get(self, code: str) -> Optional[WatchListEntry]:
        for e in self._entries:
            if e.code == code:
                return e
        return None

    def add(self, entry: WatchListEntry) -> None:
        if self.get(entry.code) is not None:
            raise WatchListError(f"Code {entry.code} already exists in watchlist")
        self._entries.append(entry)

    def remove(self, code: str) -> bool:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.code != code]
        return len(self._entries) < before

    def update(self, code: str, **changes: Any) -> WatchListEntry:
        idx = next((i for i, e in enumerate(self._entries) if e.code == code), None)
        if idx is None:
            raise WatchListError(f"Code {code} not found in watchlist")

        current = self._entries[idx].to_dict()
        for k, v in changes.items():
            if v is None:
                continue
            current[k] = v
        new_entry = WatchListEntry.create(**current)
        self._entries[idx] = new_entry
        return new_entry

    def filter_by_tag(self, tag: str) -> List[WatchListEntry]:
        return [e for e in self._entries if e.tag == tag]

    def filter_by_priority(self, priority: str) -> List[WatchListEntry]:
        return [e for e in self._entries if e.priority == priority]

    def codes(self) -> List[str]:
        return [e.code for e in self._entries]

    @contextmanager
    def _master_conn(self) -> Iterator[Optional[sqlite3.Connection]]:
        if not self._master_db_path or not Path(self._master_db_path).exists():
            yield None
            return
        conn = sqlite3.connect(str(self._master_db_path))
        try:
            conn.row_factory = sqlite3.Row
            yield conn
        finally:
            conn.close()

    def resolve_meta(self, code: str) -> StockMeta:
        """master.db から銘柄名・セクターを解決する。失敗時は code のみ返す。"""
        try:
            with self._master_conn() as conn:
                if conn is None:
                    return StockMeta(code=code)
                row = conn.execute(
                    "SELECT name, sector FROM stocks_master WHERE code = ? LIMIT 1",
                    (code,),
                ).fetchone()
                if row is None:
                    return StockMeta(code=code)
                return StockMeta(code=code, long_name=row["name"], sector=row["sector"])
        except sqlite3.Error as e:
            logger.warning("master.db lookup failed for %s: %s", code, e)
            return StockMeta(code=code)


def make_entry(
    code: str,
    tag: str,
    priority: str,
    *,
    note: Optional[str] = None,
    added_at: Optional[date] = None,
    target_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
) -> WatchListEntry:
    """CLI から呼びやすいファクトリ。"""
    return WatchListEntry.create(
        code=code,
        tag=tag,
        priority=priority,
        added_at=added_at or date.today(),
        note=note,
        target_price=target_price,
        stop_loss=stop_loss,
    )
