"""ExecutiveRepository のユニットテスト.

インメモリSQLiteは複数接続で共有できないため、一時ファイルDBを使用する。
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from market_pipeline.executives.edinet_executive_fetcher import Executive
from market_pipeline.executives.repository import ExecutiveRepository


@pytest.fixture
def repo() -> ExecutiveRepository:
    """一時DBファイル + 初期化済みのリポジトリ."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    repository = ExecutiveRepository(db_path=tmp.name)
    repository.initialize_tables()
    yield repository
    Path(tmp.name).unlink(missing_ok=True)


def make_exec(
    code: str = "7203",
    name: str = "豊田章男",
    role: str = "取締役会長（代表取締役）",
    is_rep: bool = True,
    doc_id: str = "S100VWVY",
    birth: str = "1956年5月3日",
    career: str | None = None,
) -> Executive:
    return Executive(
        code=code,
        name=name,
        role=role,
        is_representative=is_rep,
        birthdate=birth,
        appointed_date="4年",
        edinet_source_doc_id=doc_id,
        career_summary=career,
    )


class TestInitializeTables:
    def test_creates_three_tables(self, repo: ExecutiveRepository) -> None:
        with sqlite3.connect(repo._db_path) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "executives" in tables
        assert "executive_communications" in tables
        assert "executive_evaluations" in tables


class TestUpsertExecutives:
    def test_initial_insert(self, repo: ExecutiveRepository) -> None:
        counts = repo.upsert_executives([make_exec()])
        assert counts["inserted"] == 1
        assert counts["updated"] == 0

    def test_upsert_updates_existing(self, repo: ExecutiveRepository) -> None:
        repo.upsert_executives([make_exec(doc_id="OLD")])
        counts = repo.upsert_executives([make_exec(doc_id="NEW")])
        # PKが同じ場合はUPDATE扱い
        assert counts["updated"] == 1
        got = repo.get_executives("7203")
        assert got[0].edinet_source_doc_id == "NEW"

    def test_replace_for_code_removes_obsolete(self, repo: ExecutiveRepository) -> None:
        """replace_for_code=X を渡すと、X の既存レコードが削除される."""
        repo.upsert_executives(
            [
                make_exec(name="退任者A", role="取締役"),
                make_exec(name="留任者B", role="取締役"),
            ]
        )
        counts = repo.upsert_executives(
            [
                make_exec(name="留任者B", role="取締役"),
                make_exec(name="新任者C", role="取締役"),
            ],
            replace_for_code="7203",
        )
        assert counts["deleted"] == 2
        assert counts["inserted"] == 2

        names = {e.name for e in repo.get_executives("7203")}
        assert names == {"留任者B", "新任者C"}

    def test_normalizes_name_before_storage(self, repo: ExecutiveRepository) -> None:
        """氏名のノーブレークスペースは正規化されて保存される."""
        repo.upsert_executives(
            [make_exec(name="豊\u00a0田\u00a0章\u00a0男", role="取締役")]
        )
        rows = repo.get_executives("7203")
        assert rows[0].name == "豊 田 章 男"

    def test_career_summary_persisted(self, repo: ExecutiveRepository) -> None:
        """career_summary が保存・取得できる."""
        career_text = "1984年4月 トヨタ自動車入社 2000年6月 取締役 2023年4月 会長"
        repo.upsert_executives([make_exec(career=career_text)])
        rows = repo.get_executives("7203")
        assert rows[0].career_summary == career_text

    def test_career_summary_updated_on_upsert(self, repo: ExecutiveRepository) -> None:
        """UPSERT で career_summary が上書きされる."""
        repo.upsert_executives([make_exec(career="旧略歴")])
        repo.upsert_executives([make_exec(career="新略歴2026")])
        rows = repo.get_executives("7203")
        assert rows[0].career_summary == "新略歴2026"


class TestGetExecutives:
    @pytest.fixture(autouse=True)
    def _seed(self, repo: ExecutiveRepository) -> None:
        repo.upsert_executives(
            [
                make_exec(code="7203", name="A", role="取締役 代表取締役", is_rep=True),
                make_exec(code="7203", name="B", role="取締役", is_rep=False),
                make_exec(
                    code="7203", name="C", role="取締役 監査等委員", is_rep=False
                ),
                make_exec(code="9984", name="孫正義", role="代表取締役", is_rep=True),
            ]
        )

    def test_get_all_for_code(self, repo: ExecutiveRepository) -> None:
        assert len(repo.get_executives("7203")) == 3

    def test_filter_representative_only(self, repo: ExecutiveRepository) -> None:
        result = repo.get_executives("7203", is_representative=True)
        assert len(result) == 1
        assert result[0].name == "A"

    def test_filter_role_contains(self, repo: ExecutiveRepository) -> None:
        result = repo.get_executives("7203", role_contains="監査等委員")
        assert len(result) == 1
        assert result[0].name == "C"

    def test_filter_persons(self, repo: ExecutiveRepository) -> None:
        result = repo.get_executives("7203", persons=["A", "C"])
        assert {e.name for e in result} == {"A", "C"}

    def test_cross_code_isolation(self, repo: ExecutiveRepository) -> None:
        assert {e.code for e in repo.get_executives("7203")} == {"7203"}


class TestDocIdCache:
    def test_returns_latest_doc_id(self, repo: ExecutiveRepository) -> None:
        repo.upsert_executives([make_exec(doc_id="OLD_DOC")])
        assert repo.get_latest_doc_id("7203") == "OLD_DOC"

    def test_none_when_no_records(self, repo: ExecutiveRepository) -> None:
        assert repo.get_latest_doc_id("0000") is None


class TestCommunicationsAndEvaluations:
    def test_insert_communication_then_duplicate_ignored(
        self, repo: ExecutiveRepository
    ) -> None:
        rec = {
            "code": "7203",
            "executive_name": "A",
            "source_url": "https://example.com/a",
            "source_type": "interview",
            "published_date": "2026-01-01",
            "title": "インタビュー",
            "summary": "概要",
        }
        assert repo.upsert_communications([rec]) == 1
        # 2回目は UNIQUE 制約で INSERT OR IGNORE、既存は collected_at 更新
        assert repo.upsert_communications([rec]) == 0

    def test_get_communications_since_date_filter(
        self, repo: ExecutiveRepository
    ) -> None:
        """since_date 指定時、古い published_date は除外され NULL は含まれる."""
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "A",
                    "source_url": "https://example.com/old",
                    "source_type": "article",
                    "published_date": "2021-01-01",
                    "title": "old",
                },
                {
                    "code": "7203",
                    "executive_name": "A",
                    "source_url": "https://example.com/new",
                    "source_type": "article",
                    "published_date": "2025-06-01",
                    "title": "new",
                },
                {
                    "code": "7203",
                    "executive_name": "A",
                    "source_url": "https://example.com/undated",
                    "source_type": "article",
                    "published_date": None,
                    "title": "undated",
                },
            ]
        )
        rows = repo.get_communications("A", since_date="2023-04-17")
        urls = {r["source_url"] for r in rows}
        # 2021年のレコードは除外。新しいレコードと published_date=NULL は残る
        assert urls == {
            "https://example.com/new",
            "https://example.com/undated",
        }

    def test_cache_valid_after_fresh_insert(self, repo: ExecutiveRepository) -> None:
        repo.upsert_communications(
            [
                {
                    "code": "7203",
                    "executive_name": "A",
                    "source_url": "https://example.com/a",
                    "source_type": "interview",
                    "title": "t",
                }
            ]
        )
        assert repo.is_cache_valid("A", ttl_days=30) is True

    def test_upsert_evaluation_serializes_rationale(
        self, repo: ExecutiveRepository
    ) -> None:
        repo.upsert_evaluation(
            {
                "code": "7203",
                "executive_name": "A",
                "evaluation_date": "2026-04-17",
                "vision_consistency": 7.5,
                "execution_track_record": 8.0,
                "market_awareness": 7.0,
                "risk_disclosure_honesty": 6.5,
                "communication_clarity": 8.5,
                "overall_score": 7.5,
                "rationale": {"vision_consistency": "明確なビジョンを継続発信"},
            }
        )
        got = repo.get_latest_evaluation("7203", "A")
        assert got["overall_score"] == 7.5
        assert got["rationale"]["vision_consistency"].startswith("明確")
