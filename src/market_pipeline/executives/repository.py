"""Executives 関連の3テーブル（executives / communications / evaluations）のリポジトリ."""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional

from market_pipeline.config import get_settings
from market_pipeline.executives.edinet_executive_fetcher import (
    Executive,
    normalize_text,
)

logger = logging.getLogger(__name__)


_SCHEMA_EXECUTIVES = """
CREATE TABLE IF NOT EXISTS executives (
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    is_representative INTEGER NOT NULL DEFAULT 0,
    appointed_date TEXT,
    birthdate TEXT,
    edinet_source_doc_id TEXT,
    career_summary TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (code, name, role)
)
"""

_SCHEMA_EXECUTIVE_COMMUNICATIONS = """
CREATE TABLE IF NOT EXISTS executive_communications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    executive_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_type TEXT,
    published_date TEXT,
    title TEXT,
    summary TEXT,
    collected_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE (executive_name, source_url)
)
"""

_SCHEMA_EXECUTIVE_EVALUATIONS = """
CREATE TABLE IF NOT EXISTS executive_evaluations (
    code TEXT NOT NULL,
    executive_name TEXT NOT NULL,
    evaluation_date TEXT NOT NULL,
    vision_consistency REAL,
    execution_track_record REAL,
    market_awareness REAL,
    risk_disclosure_honesty REAL,
    communication_clarity REAL,
    growth_ambition REAL,
    overall_score REAL,
    rationale TEXT,
    PRIMARY KEY (code, executive_name, evaluation_date)
)
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_executives_code ON executives (code)",
    "CREATE INDEX IF NOT EXISTS idx_executives_repr ON executives (code, is_representative)",
    "CREATE INDEX IF NOT EXISTS idx_comm_code_name ON executive_communications (code, executive_name)",
    "CREATE INDEX IF NOT EXISTS idx_comm_collected ON executive_communications (executive_name, collected_at)",
    "CREATE INDEX IF NOT EXISTS idx_eval_code ON executive_evaluations (code)",
]


class ExecutiveRepository:
    """`statements.db` の3テーブル（executives系）に対するCRUDリポジトリ.

    Example:
        repo = ExecutiveRepository()
        repo.initialize_tables()
        repo.upsert_executives([Executive(...), ...])
        reprs = repo.get_executives("7203", is_representative=True)
    """

    def __init__(self, db_path: Optional[str | Path] = None) -> None:
        settings = get_settings()
        self._db_path = str(db_path or settings.paths.statements_db)
        self._pragmas = settings.database.get_pragma_statements()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        try:
            for pragma in self._pragmas:
                conn.execute(pragma)
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_tables(self) -> None:
        """必要な3テーブル＋インデックスを作成する."""
        with self._connect() as conn:
            conn.execute(_SCHEMA_EXECUTIVES)
            conn.execute(_SCHEMA_EXECUTIVE_COMMUNICATIONS)
            conn.execute(_SCHEMA_EXECUTIVE_EVALUATIONS)
            for idx_sql in _INDEXES:
                conn.execute(idx_sql)

    # ------------------------------------------------------------------
    # executives
    # ------------------------------------------------------------------

    def upsert_executives(
        self, executives: Iterable[Executive], *, replace_for_code: Optional[str] = None
    ) -> dict[str, int]:
        """役員レコードを UPSERT する.

        Args:
            executives: UPSERT対象のレコード
            replace_for_code: 指定すると、そのコードの既存レコードを先に削除し、
                最新スナップショットで置き換える（月次バッチでの差分同期用）

        Returns:
            {"inserted": N, "updated": N, "deleted": N}
        """
        records = [self._normalize_executive(e) for e in executives]
        with self._connect() as conn:
            deleted = 0
            if replace_for_code is not None:
                cur = conn.execute(
                    "DELETE FROM executives WHERE code = ?", (replace_for_code,)
                )
                deleted = cur.rowcount

            inserted = 0
            updated = 0
            for rec in records:
                exists = (
                    conn.execute(
                        "SELECT 1 FROM executives WHERE code=? AND name=? AND role=?",
                        (rec.code, rec.name, rec.role),
                    ).fetchone()
                    is not None
                )
                conn.execute(
                    """
                    INSERT INTO executives (
                        code, name, role, is_representative,
                        appointed_date, birthdate, edinet_source_doc_id,
                        career_summary, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                    ON CONFLICT (code, name, role) DO UPDATE SET
                        is_representative = excluded.is_representative,
                        appointed_date = excluded.appointed_date,
                        birthdate = excluded.birthdate,
                        edinet_source_doc_id = excluded.edinet_source_doc_id,
                        career_summary = excluded.career_summary,
                        updated_at = datetime('now', 'localtime')
                    """,
                    (
                        rec.code,
                        rec.name,
                        rec.role,
                        1 if rec.is_representative else 0,
                        rec.appointed_date,
                        rec.birthdate,
                        rec.edinet_source_doc_id,
                        rec.career_summary,
                    ),
                )
                if exists:
                    updated += 1
                else:
                    inserted += 1
        return {"inserted": inserted, "updated": updated, "deleted": deleted}

    def get_executives(
        self,
        code: str,
        *,
        is_representative: Optional[bool] = None,
        role_contains: Optional[str] = None,
        persons: Optional[list[str]] = None,
    ) -> list[Executive]:
        """フィルタ付きで役員レコードを取得する.

        Args:
            code: 銘柄コード
            is_representative: True=代表のみ / False=非代表のみ / None=全員
            role_contains: 役職文字列の部分一致フィルタ（例: "取締役" / "執行役員"）
            persons: 氏名での完全一致フィルタ
        """
        query = "SELECT * FROM executives WHERE code = ?"
        params: list[object] = [code]
        if is_representative is not None:
            query += " AND is_representative = ?"
            params.append(1 if is_representative else 0)
        if role_contains:
            query += " AND role LIKE ?"
            params.append(f"%{role_contains}%")
        if persons:
            placeholders = ",".join("?" for _ in persons)
            query += f" AND name IN ({placeholders})"
            params.extend(persons)
        query += " ORDER BY is_representative DESC, name"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_executive(row) for row in rows]

    def get_latest_doc_id(self, code: str) -> Optional[str]:
        """指定銘柄の最新 edinet_source_doc_id を返す（doc_idキャッシュ用）."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT edinet_source_doc_id
                FROM executives
                WHERE code = ? AND edinet_source_doc_id IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (code,),
            ).fetchone()
        return row["edinet_source_doc_id"] if row else None

    @staticmethod
    def _normalize_executive(e: Executive) -> Executive:
        """UPSERT前に氏名・役職を正規化する."""
        return Executive(
            code=e.code,
            name=normalize_text(e.name),
            role=normalize_text(e.role),
            is_representative=e.is_representative,
            birthdate=e.birthdate,
            appointed_date=e.appointed_date,
            edinet_source_doc_id=e.edinet_source_doc_id,
            career_summary=e.career_summary,
        )

    @staticmethod
    def _row_to_executive(row: sqlite3.Row) -> Executive:
        row_keys = set(row.keys())
        return Executive(
            code=row["code"],
            name=row["name"],
            role=row["role"],
            is_representative=bool(row["is_representative"]),
            birthdate=row["birthdate"],
            appointed_date=row["appointed_date"],
            edinet_source_doc_id=row["edinet_source_doc_id"],
            career_summary=row["career_summary"]
            if "career_summary" in row_keys
            else None,
        )

    # ------------------------------------------------------------------
    # executive_communications (Phase 2 で利用)
    # ------------------------------------------------------------------

    def upsert_communications(self, records: Iterable[dict]) -> int:
        """発信コンテンツを UPSERT する（重複URLは INSERT OR IGNORE）.

        Args:
            records: 各辞書は code, executive_name, source_url, source_type,
                published_date, title, summary を含む

        Returns:
            新規挿入された件数
        """
        inserted = 0
        with self._connect() as conn:
            for rec in records:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO executive_communications (
                        code, executive_name, source_url, source_type,
                        published_date, title, summary, collected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                    """,
                    (
                        rec.get("code"),
                        rec.get("executive_name"),
                        rec.get("source_url"),
                        rec.get("source_type"),
                        rec.get("published_date"),
                        rec.get("title"),
                        rec.get("summary"),
                    ),
                )
                inserted += cur.rowcount
                # 既存レコードは collected_at のみ更新（キャッシュ有効期限判定用）
                if cur.rowcount == 0:
                    conn.execute(
                        """
                        UPDATE executive_communications
                        SET collected_at = datetime('now', 'localtime')
                        WHERE executive_name = ? AND source_url = ?
                        """,
                        (rec.get("executive_name"), rec.get("source_url")),
                    )
        return inserted

    def is_cache_valid(self, executive_name: str, ttl_days: int) -> bool:
        """指定役員の発信コレクションが TTL内であれば True を返す."""
        cutoff = (datetime.now() - timedelta(days=ttl_days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(collected_at) AS latest
                FROM executive_communications
                WHERE executive_name = ?
                """,
                (executive_name,),
            ).fetchone()
        latest = row["latest"] if row else None
        if latest is None:
            return False
        return str(latest) >= cutoff

    def get_communications(
        self, executive_name: str, since_date: Optional[str] = None
    ) -> list[dict]:
        query = "SELECT * FROM executive_communications WHERE executive_name = ?"
        params: list[object] = [executive_name]
        if since_date:
            query += " AND (published_date IS NULL OR published_date >= ?)"
            params.append(since_date)
        query += " ORDER BY published_date DESC"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # executive_evaluations (Phase 3 で利用)
    # ------------------------------------------------------------------

    def upsert_evaluation(self, record: dict) -> None:
        """LLM評価結果を UPSERT する.

        record は以下のキーを含む dict:
            code, executive_name, evaluation_date,
            vision_consistency, execution_track_record, market_awareness,
            risk_disclosure_honesty, communication_clarity,
            overall_score, rationale (dict or JSON string)
        """
        rationale = record.get("rationale")
        if isinstance(rationale, dict):
            rationale = json.dumps(rationale, ensure_ascii=False)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO executive_evaluations (
                    code, executive_name, evaluation_date,
                    vision_consistency, execution_track_record, market_awareness,
                    risk_disclosure_honesty, communication_clarity,
                    growth_ambition, overall_score, rationale
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (code, executive_name, evaluation_date) DO UPDATE SET
                    vision_consistency = excluded.vision_consistency,
                    execution_track_record = excluded.execution_track_record,
                    market_awareness = excluded.market_awareness,
                    risk_disclosure_honesty = excluded.risk_disclosure_honesty,
                    communication_clarity = excluded.communication_clarity,
                    growth_ambition = excluded.growth_ambition,
                    overall_score = excluded.overall_score,
                    rationale = excluded.rationale
                """,
                (
                    record["code"],
                    record["executive_name"],
                    record["evaluation_date"],
                    record.get("vision_consistency"),
                    record.get("execution_track_record"),
                    record.get("market_awareness"),
                    record.get("risk_disclosure_honesty"),
                    record.get("communication_clarity"),
                    record.get("growth_ambition"),
                    record.get("overall_score"),
                    rationale,
                ),
            )

    def get_latest_evaluation(self, code: str, executive_name: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM executive_evaluations
                WHERE code = ? AND executive_name = ?
                ORDER BY evaluation_date DESC
                LIMIT 1
                """,
                (code, executive_name),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        if result.get("rationale"):
            try:
                result["rationale"] = json.loads(result["rationale"])
            except (TypeError, json.JSONDecodeError):
                pass
        return result


def _executive_to_dict(e: Executive) -> dict:
    """互換性のための変換ヘルパ（テスト用）."""
    return asdict(e)
