"""Executive communication analysis module.

EDINET有価証券報告書から法定役員（取締役・監査役・執行役）情報を取得・永続化し、
その外部発信テキストを収集してLLMで6軸スコアリング（ビジョン一貫性・実行力・市場認識・
リスク開示誠実性・コミュニケーション能力・成長志向）するためのモジュール。

主要コンポーネント:
- EdinetExecutiveFetcher: EDINET APIから役員情報を取得しiXBRLパース
- EdinetDocResolver: doc_idキャッシュ戦略によるバッチ高速化
- ExecutiveRepository: `executives` / `executive_communications` / `executive_evaluations` テーブルCRUD
"""

from market_pipeline.executives.communication_collector import (
    Communication,
    CommunicationCollector,
    build_search_query,
    classify_source_type,
)
from market_pipeline.executives.edinet_doc_resolver import EdinetDocResolver
from market_pipeline.executives.edinet_executive_fetcher import (
    EdinetExecutiveFetcher,
    Executive,
)
from market_pipeline.executives.evaluator import (
    Evaluation,
    ExecutiveEvaluator,
    format_communications_for_prompt,
    load_prompt_template,
    parse_llm_response,
    validate_scores,
)
from market_pipeline.executives.exceptions import (
    CommunicationCollectionError,
    EdinetFetchError,
    EvaluationError,
    ExecutiveError,
)
from market_pipeline.executives.published_date_extractor import (
    extract_published_date,
)
from market_pipeline.executives.repository import ExecutiveRepository

# 収集・表示の期間定数
# LOOKBACK_DAYS_TOTAL: WebSearch・DB読み出し・レポート表示の対象期間（過去3年）
# HIGHLIGHT_DAYS_RECENT: タイムラインでハイライト表示する直近期間（過去1年）
LOOKBACK_DAYS_TOTAL = 1095
HIGHLIGHT_DAYS_RECENT = 365

__all__ = [
    "HIGHLIGHT_DAYS_RECENT",
    "LOOKBACK_DAYS_TOTAL",
    "Communication",
    "CommunicationCollectionError",
    "CommunicationCollector",
    "EdinetDocResolver",
    "EdinetFetchError",
    "EdinetExecutiveFetcher",
    "Evaluation",
    "EvaluationError",
    "Executive",
    "ExecutiveError",
    "ExecutiveEvaluator",
    "ExecutiveRepository",
    "build_search_query",
    "classify_source_type",
    "extract_published_date",
    "format_communications_for_prompt",
    "load_prompt_template",
    "parse_llm_response",
    "validate_scores",
]
