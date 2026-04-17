"""LLMによる経営陣5軸スコアリング.

LLM呼び出しは `llm_fn` として注入可能にする DI パターン:
- Claude Code スキル層: 内蔵LLM経由で呼び出し
- anthropic SDK経由: Phase 3 で必要な場合にアダプタを作成
- テスト: モック関数

スコアリング失敗時は score=None + rationale に理由を記録する。
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Optional

from market_pipeline.executives.communication_collector import Communication
from market_pipeline.executives.exceptions import EvaluationError
from market_pipeline.executives.repository import ExecutiveRepository

logger = logging.getLogger(__name__)

# LLM呼び出し関数: プロンプト全文を受け取り、生のレスポンス文字列を返す
LLMFn = Callable[[str], str]

_DEFAULT_PROMPT_PATH = (
    Path(__file__).parent.parent / "prompts" / "executive_evaluation.md"
)
_AXES = (
    "vision_consistency",
    "execution_track_record",
    "market_awareness",
    "risk_disclosure_honesty",
    "communication_clarity",
    "growth_ambition",
)


@dataclass
class Evaluation:
    """6軸評価の結果."""

    code: str
    executive_name: str
    evaluation_date: str
    vision_consistency: Optional[float]
    execution_track_record: Optional[float]
    market_awareness: Optional[float]
    risk_disclosure_honesty: Optional[float]
    communication_clarity: Optional[float]
    growth_ambition: Optional[float]
    overall_score: Optional[float]
    rationale: dict[str, str]

    def to_record(self) -> dict:
        return {
            "code": self.code,
            "executive_name": self.executive_name,
            "evaluation_date": self.evaluation_date,
            "vision_consistency": self.vision_consistency,
            "execution_track_record": self.execution_track_record,
            "market_awareness": self.market_awareness,
            "risk_disclosure_honesty": self.risk_disclosure_honesty,
            "communication_clarity": self.communication_clarity,
            "growth_ambition": self.growth_ambition,
            "overall_score": self.overall_score,
            "rationale": self.rationale,
        }


def load_prompt_template(path: Optional[Path] = None) -> str:
    return (path or _DEFAULT_PROMPT_PATH).read_text(encoding="utf-8")


def format_communications_for_prompt(communications: list[Communication]) -> str:
    """プロンプトインジェクション対策: 収集テキストはフラット文字列で列挙."""
    lines: list[str] = []
    for idx, c in enumerate(communications, start=1):
        lines.append(f"--- [{idx}] ({c.source_type or 'article'}) ---")
        if c.published_date:
            lines.append(f"日付: {c.published_date}")
        if c.title:
            lines.append(f"タイトル: {c.title}")
        if c.source_url:
            lines.append(f"URL: {c.source_url}")
        if c.summary:
            lines.append(f"要約: {c.summary}")
        lines.append("")
    return "\n".join(lines).strip()


def parse_llm_response(response_text: str) -> dict:
    """LLMレスポンスからJSONを抽出して辞書化する.

    ```json 〜 ``` フェンスが付いていても外側の波括弧で括られたJSONを抽出する。

    Raises:
        EvaluationError: 有効なJSONが見つからない場合
    """
    # ```json ... ``` フェンスを除去
    match = re.search(r"\{.*\}", response_text, flags=re.DOTALL)
    if not match:
        raise EvaluationError("レスポンスにJSONが含まれていません")
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise EvaluationError(f"JSONデコード失敗: {exc}") from exc


def validate_scores(data: dict) -> None:
    """5軸スコアと rationale の存在・範囲を検証する."""
    missing_axes = [axis for axis in _AXES if axis not in data]
    if missing_axes:
        raise EvaluationError(f"スコア軸の欠落: {missing_axes}")
    for axis in _AXES:
        value = data[axis]
        if not isinstance(value, (int, float)):
            raise EvaluationError(f"{axis} はfloatである必要があります: {value!r}")
        if not 0.0 <= float(value) <= 10.0:
            raise EvaluationError(f"{axis} は0.0〜10.0の範囲外: {value}")
    rationale = data.get("rationale")
    if not isinstance(rationale, dict):
        raise EvaluationError("rationaleはdictである必要があります")
    for axis in _AXES:
        if axis not in rationale:
            raise EvaluationError(f"rationaleに軸の欠落: {axis}")


class ExecutiveEvaluator:
    """LLMスコアリングを担当する.

    Example:
        def my_llm(prompt: str) -> str:
            return json.dumps({...})

        evaluator = ExecutiveEvaluator(llm_fn=my_llm)
        result = evaluator.evaluate(
            name="佐藤恒治",
            company="トヨタ自動車",
            communications=[...],
            code="7203",
        )
    """

    def __init__(
        self,
        *,
        llm_fn: Optional[LLMFn] = None,
        repository: Optional[ExecutiveRepository] = None,
        prompt_template: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        score_alert_threshold: float = 3.0,
    ) -> None:
        self._llm_fn = llm_fn
        self._repo = repository or ExecutiveRepository()
        self._prompt_template = prompt_template or load_prompt_template()
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._score_alert_threshold = score_alert_threshold

    def evaluate(
        self,
        *,
        name: str,
        company: str,
        communications: list[Communication],
        code: str,
        evaluation_date: Optional[str] = None,
    ) -> Evaluation:
        """LLMで5軸スコアを算出する.

        スキーマ違反はリトライ（最大 max_retries 回）。失敗時はスコアNULLで返す。
        """
        eval_date = evaluation_date or date.today().isoformat()
        prompt = self._build_prompt(name, company, communications)

        if self._llm_fn is None:
            logger.warning("llm_fn 未設定のため評価スキップ name=%s", name)
            return self._failure_evaluation(
                code=code,
                name=name,
                evaluation_date=eval_date,
                reason="llm_fn が注入されていません",
            )

        last_error: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                raw = self._llm_fn(prompt)
                data = parse_llm_response(raw)
                validate_scores(data)
                return self._to_evaluation(
                    data, code=code, name=name, evaluation_date=eval_date
                )
            except Exception as exc:  # noqa: BLE001 - リトライで回復
                last_error = exc
                logger.warning(
                    "LLM評価リトライ %d/%d name=%s error=%s",
                    attempt + 1,
                    self._max_retries,
                    name,
                    exc,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)

        error_msg = str(last_error) if last_error else "unknown error"
        logger.error("LLM評価最終失敗 name=%s error=%s", name, error_msg)
        return self._failure_evaluation(
            code=code,
            name=name,
            evaluation_date=eval_date,
            reason=f"評価失敗: {error_msg}",
        )

    def evaluate_and_persist(
        self,
        *,
        name: str,
        company: str,
        communications: list[Communication],
        code: str,
        evaluation_date: Optional[str] = None,
    ) -> Evaluation:
        """評価を算出してDBに UPSERT し、前回スコアとの差分が大きい場合は警告を出す."""
        result = self.evaluate(
            name=name,
            company=company,
            communications=communications,
            code=code,
            evaluation_date=evaluation_date,
        )

        if result.overall_score is not None:
            previous = self._repo.get_latest_evaluation(code, name)
            if (
                previous
                and previous.get("overall_score") is not None
                and previous["evaluation_date"] != result.evaluation_date
            ):
                diff = abs(result.overall_score - previous["overall_score"])
                if diff >= self._score_alert_threshold:
                    logger.warning(
                        "総合スコアが大きく変動 name=%s 前回=%.2f 今回=%.2f diff=%.2f",
                        name,
                        previous["overall_score"],
                        result.overall_score,
                        diff,
                    )

        self._repo.upsert_evaluation(result.to_record())
        return result

    def _build_prompt(
        self, name: str, company: str, communications: list[Communication]
    ) -> str:
        comm_text = (
            format_communications_for_prompt(communications) or "(発信コンテンツなし)"
        )
        return self._prompt_template.format(
            name=name, company=company, communications=comm_text
        )

    def _to_evaluation(
        self, data: dict, *, code: str, name: str, evaluation_date: str
    ) -> Evaluation:
        scores = [float(data[a]) for a in _AXES]
        overall = round(sum(scores) / len(scores), 2)
        rationale = {k: str(v)[:200] for k, v in data.get("rationale", {}).items()}
        return Evaluation(
            code=code,
            executive_name=name,
            evaluation_date=evaluation_date,
            vision_consistency=float(data["vision_consistency"]),
            execution_track_record=float(data["execution_track_record"]),
            market_awareness=float(data["market_awareness"]),
            risk_disclosure_honesty=float(data["risk_disclosure_honesty"]),
            communication_clarity=float(data["communication_clarity"]),
            growth_ambition=float(data["growth_ambition"]),
            overall_score=overall,
            rationale=rationale,
        )

    @staticmethod
    def _failure_evaluation(
        *, code: str, name: str, evaluation_date: str, reason: str
    ) -> Evaluation:
        return Evaluation(
            code=code,
            executive_name=name,
            evaluation_date=evaluation_date,
            vision_consistency=None,
            execution_track_record=None,
            market_awareness=None,
            risk_disclosure_honesty=None,
            communication_clarity=None,
            growth_ambition=None,
            overall_score=None,
            rationale={axis: reason for axis in _AXES},
        )
