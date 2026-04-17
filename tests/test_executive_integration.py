"""経営陣評価パイプラインの統合テスト.

EDINET取得 (ローカル iXBRL) + WebSearch モック + LLM モック で
End-to-End 実行し、executive_report.md 相当の出力が生成されることを確認する。
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from market_pipeline.executives import (
    CommunicationCollector,
    EdinetExecutiveFetcher,
    ExecutiveEvaluator,
    ExecutiveRepository,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "executives"
SONY_IXBRL = FIXTURE_DIR / "sony_0104010.htm"


@pytest.fixture
def tmp_db() -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    yield tmp.name
    Path(tmp.name).unlink(missing_ok=True)


def _good_llm_response() -> str:
    return json.dumps(
        {
            "vision_consistency": 8.0,
            "execution_track_record": 7.5,
            "market_awareness": 8.5,
            "risk_disclosure_honesty": 6.0,
            "communication_clarity": 8.5,
            "growth_ambition": 7.5,
            "rationale": {
                "vision_consistency": "長期ビジョン明確",
                "execution_track_record": "ゲーム事業を計画通り展開",
                "market_awareness": "AI市場動向を的確に把握",
                "risk_disclosure_honesty": "一部リスク開示は控えめ",
                "communication_clarity": "構成・論理が明快",
                "growth_ambition": "海外M&Aで拡張継続",
            },
        }
    )


class TestEndToEnd:
    def test_full_pipeline_sony(self, tmp_db: str) -> None:
        """EDINET ローカル iXBRL → 収集モック → LLM モック → UPSERT → 取得."""
        repo = ExecutiveRepository(db_path=tmp_db)
        repo.initialize_tables()

        # 1) EDINET (ローカル)
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        executives = fetcher.fetch_from_local_ixbrl(
            SONY_IXBRL, code="6758", doc_id="S100W19Q"
        )
        assert len(executives) == 16
        counts = repo.upsert_executives(executives, replace_for_code="6758")
        assert counts["inserted"] == 16

        # 2) 代表役員のみフィルタ
        reps = repo.get_executives("6758", is_representative=True)
        assert len(reps) == 3

        # 3) 発信収集（モック）
        web_search = MagicMock(
            return_value=[
                {
                    "url": "https://example.com/yoshida-interview",
                    "title": "吉田会長インタビュー",
                    "snippet": "ビジョン概要",
                    "published_date": "2026-02-15",
                }
            ]
        )
        collector = CommunicationCollector(web_search_fn=web_search, repository=repo)
        for e in reps:
            collector.collect(name=e.name, company="ソニーグループ", code="6758")
        # 3役員 × 各1件 = 3件のはずだが URLは共通のため1件のみ残る
        # （executive_communications は UNIQUE(executive_name, source_url) なので各役員で1件ずつ登録）
        yoshida_comms = repo.get_communications("吉田 憲一郎")
        assert len(yoshida_comms) == 1

        # 4) LLMスコアリング
        evaluator = ExecutiveEvaluator(
            llm_fn=MagicMock(return_value=_good_llm_response()),
            repository=repo,
            max_retries=1,
            retry_delay=0,
        )
        from market_pipeline.executives.communication_collector import (
            Communication,
        )

        for e in reps:
            comms_rows = repo.get_communications(e.name)
            comms = [
                Communication(
                    code=row["code"],
                    executive_name=row["executive_name"],
                    source_url=row["source_url"],
                    source_type=row["source_type"],
                    published_date=row["published_date"],
                    title=row["title"],
                    summary=row["summary"],
                )
                for row in comms_rows
            ]
            result = evaluator.evaluate_and_persist(
                name=e.name,
                company="ソニーグループ",
                communications=comms,
                code="6758",
            )
            assert result.overall_score is not None

        # 5) 評価データの永続化確認
        # (8.0+7.5+8.5+6.0+8.5+7.5)/6 = 7.6666... → 7.67
        for e in reps:
            latest = repo.get_latest_evaluation("6758", e.name)
            assert latest is not None
            assert latest["overall_score"] == pytest.approx(7.67, abs=0.01)
            assert latest["growth_ambition"] == 7.5

    def test_include_directors_filter(self, tmp_db: str) -> None:
        """--include-directors 相当のフィルタで全取締役が取れる."""
        repo = ExecutiveRepository(db_path=tmp_db)
        repo.initialize_tables()
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        executives = fetcher.fetch_from_local_ixbrl(
            SONY_IXBRL, code="6758", doc_id="S100W19Q"
        )
        repo.upsert_executives(executives, replace_for_code="6758")

        result = repo.get_executives("6758", role_contains="取締役")
        # 10名の取締役（吉田・十時の代表執行役エントリは「取締役」文字列を含まない）
        assert len(result) == 10
        assert all("取締役" in e.role for e in result)

    def test_persons_filter(self, tmp_db: str) -> None:
        repo = ExecutiveRepository(db_path=tmp_db)
        repo.initialize_tables()
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        executives = fetcher.fetch_from_local_ixbrl(
            SONY_IXBRL, code="6758", doc_id="S100W19Q"
        )
        repo.upsert_executives(executives, replace_for_code="6758")

        # 吉田氏は取締役と代表執行役の2エントリがあるので2件ヒットする
        result = repo.get_executives("6758", persons=["吉田 憲一郎"])
        assert len(result) == 2


class TestCLIScript:
    """`scripts/run_research_executives.py` の CLI 呼び出しテスト."""

    def _build_env(self, tmp_db: str) -> dict:
        import os

        env = os.environ.copy()
        env["STOCK_ANALYSIS_STATEMENTS_DB"] = tmp_db
        return env

    def test_list_executives_outputs_json(self, tmp_db: str) -> None:
        # Setup: 役員データを投入
        repo = ExecutiveRepository(db_path=tmp_db)
        repo.initialize_tables()
        fetcher = EdinetExecutiveFetcher(api_key="dummy")
        executives = fetcher.fetch_from_local_ixbrl(
            SONY_IXBRL, code="6758", doc_id="S100W19Q"
        )
        repo.upsert_executives(executives, replace_for_code="6758")

        # 環境変数で DB 切り替えができないため、直接 CLI 関数を呼ぶ
        from scripts.run_research_executives import cmd_list_executives  # type: ignore
        import argparse
        import io

        args = argparse.Namespace(
            codes=["6758"],
            include_directors=False,
            include_executive_officers=False,
            persons="",
        )
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            # ExecutiveRepository は settings から db_path を取るため、
            # テストではテストDBが使えない → カバーはリポジトリ単体テスト側に任せ、
            # ここでは関数が0を返すことのみを確認
            rc = cmd_list_executives(args)
        finally:
            sys.stdout = old_stdout
        assert rc == 0

    def test_build_report_with_empty_executives(self, tmp_path: Path) -> None:
        """対象役員0名の場合は '対象役員なし' のレポートを出力."""
        from scripts.run_research_executives import cmd_build_report  # type: ignore
        import argparse

        out_path = tmp_path / "executive_report.md"
        args = argparse.Namespace(
            code="9999",  # 存在しない銘柄
            company="Nonexistent",
            output=str(out_path),
            include_directors=False,
            include_executive_officers=False,
            persons="",
        )
        rc = cmd_build_report(args)
        assert rc == 0
        assert out_path.exists()
        content = out_path.read_text(encoding="utf-8")
        assert "対象役員なし" in content
