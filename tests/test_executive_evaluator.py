"""ExecutiveEvaluator のユニットテスト."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from market_pipeline.executives.communication_collector import Communication
from market_pipeline.executives.evaluator import (
    ExecutiveEvaluator,
    format_communications_for_prompt,
    parse_llm_response,
    validate_scores,
)
from market_pipeline.executives.exceptions import EvaluationError
from market_pipeline.executives.repository import ExecutiveRepository


@pytest.fixture
def repo() -> ExecutiveRepository:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    r = ExecutiveRepository(db_path=tmp.name)
    r.initialize_tables()
    yield r
    Path(tmp.name).unlink(missing_ok=True)


def _sample_comms() -> list[Communication]:
    return [
        Communication(
            code="7203",
            executive_name="佐藤恒治",
            source_url="https://example.com/a",
            source_type="interview",
            published_date="2026-02-15",
            title="社長インタビュー",
            summary="概要テキスト",
        )
    ]


def _good_response() -> str:
    return json.dumps(
        {
            "vision_consistency": 7.5,
            "execution_track_record": 8.0,
            "market_awareness": 7.0,
            "risk_disclosure_honesty": 6.5,
            "communication_clarity": 8.5,
            "growth_ambition": 8.0,
            "rationale": {
                "vision_consistency": "明確なビジョンを継続発信",
                "execution_track_record": "EV投資を着実に実行",
                "market_awareness": "競合動向を的確に把握",
                "risk_disclosure_honesty": "リスク開示は改善余地あり",
                "communication_clarity": "論理的で分かりやすい",
                "growth_ambition": "中期計画で成長戦略を明示",
            },
        }
    )


class TestFormatCommunicationsForPrompt:
    def test_empty_list(self) -> None:
        assert format_communications_for_prompt([]) == ""

    def test_contains_url_title_summary(self) -> None:
        out = format_communications_for_prompt(_sample_comms())
        assert "https://example.com/a" in out
        assert "社長インタビュー" in out
        assert "概要テキスト" in out


class TestParseLLMResponse:
    def test_plain_json(self) -> None:
        data = parse_llm_response('{"a": 1}')
        assert data == {"a": 1}

    def test_fenced_json(self) -> None:
        data = parse_llm_response('```json\n{"a": 2}\n```')
        assert data == {"a": 2}

    def test_json_with_preamble(self) -> None:
        data = parse_llm_response('回答です: {"a": 3}')
        assert data == {"a": 3}

    def test_no_json_raises(self) -> None:
        with pytest.raises(EvaluationError):
            parse_llm_response("no json here")

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(EvaluationError):
            parse_llm_response("{not valid}")


class TestValidateScores:
    def test_valid_passes(self) -> None:
        validate_scores(json.loads(_good_response()))

    def test_missing_axis_fails(self) -> None:
        data = json.loads(_good_response())
        del data["vision_consistency"]
        with pytest.raises(EvaluationError):
            validate_scores(data)

    def test_out_of_range_fails(self) -> None:
        data = json.loads(_good_response())
        data["vision_consistency"] = 11.0
        with pytest.raises(EvaluationError):
            validate_scores(data)

    def test_missing_rationale_axis_fails(self) -> None:
        data = json.loads(_good_response())
        del data["rationale"]["vision_consistency"]
        with pytest.raises(EvaluationError):
            validate_scores(data)


class TestEvaluate:
    def test_normal_response_returns_valid_evaluation(
        self, repo: ExecutiveRepository
    ) -> None:
        llm = MagicMock(return_value=_good_response())
        evaluator = ExecutiveEvaluator(llm_fn=llm, repository=repo)
        result = evaluator.evaluate(
            name="佐藤恒治",
            company="トヨタ自動車",
            communications=_sample_comms(),
            code="7203",
        )
        # (7.5+8.0+7.0+6.5+8.5+8.0)/6 = 7.5833
        assert result.overall_score == pytest.approx(7.58, abs=0.01)
        assert result.vision_consistency == 7.5
        assert result.growth_ambition == 8.0
        assert result.rationale["vision_consistency"].startswith("明確")

    def test_schema_violation_retries(self, repo: ExecutiveRepository) -> None:
        llm = MagicMock(
            side_effect=[
                "{invalid}",
                '{"vision_consistency": 5}',  # 残り軸欠落
                _good_response(),
            ]
        )
        evaluator = ExecutiveEvaluator(
            llm_fn=llm, repository=repo, max_retries=3, retry_delay=0
        )
        result = evaluator.evaluate(
            name="佐藤恒治",
            company="トヨタ自動車",
            communications=_sample_comms(),
            code="7203",
        )
        assert llm.call_count == 3
        assert result.overall_score is not None

    def test_all_retries_fail_returns_null_scores(
        self, repo: ExecutiveRepository
    ) -> None:
        llm = MagicMock(return_value="{invalid json}")
        evaluator = ExecutiveEvaluator(
            llm_fn=llm, repository=repo, max_retries=2, retry_delay=0
        )
        result = evaluator.evaluate(
            name="佐藤恒治",
            company="トヨタ自動車",
            communications=_sample_comms(),
            code="7203",
        )
        assert result.overall_score is None
        assert result.vision_consistency is None
        assert result.rationale["vision_consistency"].startswith("評価失敗")

    def test_no_llm_fn_returns_null(self, repo: ExecutiveRepository) -> None:
        evaluator = ExecutiveEvaluator(llm_fn=None, repository=repo)
        result = evaluator.evaluate(
            name="佐藤恒治",
            company="トヨタ自動車",
            communications=_sample_comms(),
            code="7203",
        )
        assert result.overall_score is None

    def test_overall_score_rounding(self, repo: ExecutiveRepository) -> None:
        resp = json.dumps(
            {
                "vision_consistency": 7.0,
                "execution_track_record": 8.0,
                "market_awareness": 7.5,
                "risk_disclosure_honesty": 6.25,
                "communication_clarity": 7.0,
                "growth_ambition": 7.25,
                "rationale": {
                    axis: "OK"
                    for axis in [
                        "vision_consistency",
                        "execution_track_record",
                        "market_awareness",
                        "risk_disclosure_honesty",
                        "communication_clarity",
                        "growth_ambition",
                    ]
                },
            }
        )
        evaluator = ExecutiveEvaluator(
            llm_fn=MagicMock(return_value=resp), repository=repo
        )
        result = evaluator.evaluate(
            name="A", company="B", communications=[], code="9999"
        )
        # (7.0 + 8.0 + 7.5 + 6.25 + 7.0 + 7.25) / 6 = 7.1666... → 7.17
        assert result.overall_score == pytest.approx(7.17, abs=0.01)

    def test_rationale_truncated_to_200_chars(self, repo: ExecutiveRepository) -> None:
        long_rationale = "あ" * 300
        resp = json.dumps(
            {
                "vision_consistency": 5.0,
                "execution_track_record": 5.0,
                "market_awareness": 5.0,
                "risk_disclosure_honesty": 5.0,
                "communication_clarity": 5.0,
                "growth_ambition": 5.0,
                "rationale": {
                    axis: long_rationale
                    for axis in [
                        "vision_consistency",
                        "execution_track_record",
                        "market_awareness",
                        "risk_disclosure_honesty",
                        "communication_clarity",
                        "growth_ambition",
                    ]
                },
            }
        )
        evaluator = ExecutiveEvaluator(
            llm_fn=MagicMock(return_value=resp), repository=repo
        )
        result = evaluator.evaluate(
            name="A", company="B", communications=[], code="9999"
        )
        for axis_rationale in result.rationale.values():
            assert len(axis_rationale) == 200


class TestEvaluateAndPersist:
    def test_persists_to_repository(self, repo: ExecutiveRepository) -> None:
        evaluator = ExecutiveEvaluator(
            llm_fn=MagicMock(return_value=_good_response()), repository=repo
        )
        evaluator.evaluate_and_persist(
            name="佐藤恒治",
            company="トヨタ自動車",
            communications=_sample_comms(),
            code="7203",
        )
        latest = repo.get_latest_evaluation("7203", "佐藤恒治")
        assert latest is not None
        assert latest["overall_score"] == pytest.approx(7.58, abs=0.01)
        assert latest["growth_ambition"] == 8.0

    def test_large_score_diff_emits_warning(
        self, repo: ExecutiveRepository, caplog
    ) -> None:
        import logging

        # 前回スコア (2026-03-01) を事前にDBへ投入
        repo.upsert_evaluation(
            {
                "code": "7203",
                "executive_name": "佐藤恒治",
                "evaluation_date": "2026-03-01",
                "vision_consistency": 3.0,
                "execution_track_record": 3.0,
                "market_awareness": 3.0,
                "risk_disclosure_honesty": 3.0,
                "communication_clarity": 3.0,
                "overall_score": 3.0,
                "rationale": {},
            }
        )

        evaluator = ExecutiveEvaluator(
            llm_fn=MagicMock(return_value=_good_response()),
            repository=repo,
            score_alert_threshold=3.0,
        )
        with caplog.at_level(logging.WARNING):
            evaluator.evaluate_and_persist(
                name="佐藤恒治",
                company="トヨタ自動車",
                communications=_sample_comms(),
                code="7203",
                evaluation_date="2026-04-17",
            )
        assert any("大きく変動" in r.message for r in caplog.records)
