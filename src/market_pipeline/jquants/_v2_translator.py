"""J-Quants API V2 レスポンス → V1 互換 DataFrame 変換層。

V2 では `/v2/equities/bars/daily`・`/v2/equities/master`・`/v2/fins/summary`
のフィールド名が短縮形に変わったため、本モジュールで V1 ロング名へ rename し、
既存の DB スキーマ・分析モジュール・テストへの波及を吸収する。

設計方針:
- 純粋関数。I/O 無し。`list[dict]` を受け取り `pd.DataFrame` を返す。
- V2 にしかないフィールド(前場/後場・ストップ高フラグ・翌期予想等)は落とす
  (V1 互換のカラム集合のみを返す)。
- 未知フィールドは握り潰さず `logger.warning` に記録する。

カラム差分の根拠は `tmp/v2_diff.md` を参照。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# /v2/equities/bars/daily ↔ /v1/prices/daily_quotes
# ---------------------------------------------------------------------------

DAILY_QUOTES_COLUMN_MAP: dict[str, str] = {
    # 基本(rename 不要)
    "Date": "Date",
    "Code": "Code",
    # 通日(全場)四本値
    "O": "Open",
    "H": "High",
    "L": "Low",
    "C": "Close",
    "Vo": "Volume",
    "Va": "TurnoverValue",
    # 調整係数・調整済み
    "AdjFactor": "AdjustmentFactor",
    "AdjO": "AdjustmentOpen",
    "AdjH": "AdjustmentHigh",
    "AdjL": "AdjustmentLow",
    "AdjC": "AdjustmentClose",
    "AdjVo": "AdjustmentVolume",
}

# 本プロジェクトでは使用しないフィールド(前場/後場、ストップ高/安フラグ)。
# 未知フィールド検出時の warning ノイズを抑制するために明示。
DAILY_QUOTES_IGNORED_FIELDS: frozenset[str] = frozenset(
    {
        # ストップ高/安フラグ
        "UL",
        "LL",
        # 前場(Premium only)
        "MO",
        "MH",
        "ML",
        "MC",
        "MUL",
        "MLL",
        "MVo",
        "MVa",
        "MAdjO",
        "MAdjH",
        "MAdjL",
        "MAdjC",
        "MAdjVo",
        # 後場(Premium only)
        "AO",
        "AH",
        "AL",
        "AC",
        "AUL",
        "ALL",
        "AVo",
        "AVa",
        "AAdjO",
        "AAdjH",
        "AAdjL",
        "AAdjC",
        "AAdjVo",
    }
)


# ---------------------------------------------------------------------------
# /v2/equities/master ↔ /v1/listed/info
# ---------------------------------------------------------------------------

LISTED_INFO_COLUMN_MAP: dict[str, str] = {
    "Date": "Date",
    "Code": "Code",
    "CoName": "CompanyName",
    "CoNameEn": "CompanyNameEnglish",
    "S17": "Sector17Code",
    "S17Nm": "Sector17CodeName",
    "S33": "Sector33Code",
    "S33Nm": "Sector33CodeName",
    "ScaleCat": "ScaleCategory",
    "Mkt": "MarketCode",
    "MktNm": "MarketCodeName",
    "Mrgn": "MarginCode",
    "MrgnNm": "MarginCodeName",
}

LISTED_INFO_IGNORED_FIELDS: frozenset[str] = frozenset({"ProdCat"})


# ---------------------------------------------------------------------------
# /v2/fins/summary ↔ /v1/fins/statements
# ---------------------------------------------------------------------------

STATEMENTS_COLUMN_MAP: dict[str, str] = {
    # 開示メタ
    "Code": "LocalCode",
    "DiscDate": "DisclosedDate",
    "CurPerType": "TypeOfCurrentPeriod",
    "DiscNo": "DisclosureNumber",
    "DocType": "TypeOfDocument",
    # 会計期間
    "CurPerSt": "CurrentPeriodStartDate",
    "CurPerEn": "CurrentPeriodEndDate",
    "CurFYSt": "CurrentFiscalYearStartDate",
    "CurFYEn": "CurrentFiscalYearEndDate",
    # 損益
    "Sales": "NetSales",
    "OP": "OperatingProfit",
    "OdP": "OrdinaryProfit",
    "NP": "Profit",
    "EPS": "EarningsPerShare",
    "DEPS": "DilutedEarningsPerShare",
    # 貸借対照表
    "TA": "TotalAssets",
    "Eq": "Equity",
    "EqAR": "EquityToAssetRatio",
    "BPS": "BookValuePerShare",
    # キャッシュフロー
    "CFO": "CashFlowsFromOperatingActivities",
    "CFI": "CashFlowsFromInvestingActivities",
    "CFF": "CashFlowsFromFinancingActivities",
    "CashEq": "CashAndEquivalents",
    # 配当(年合計のみ V1 が参照)
    "DivAnn": "ResultDividendPerShareAnnual",
    "FDivAnn": "ForecastDividendPerShareAnnual",
    "PayoutRatioAnn": "PayoutRatioAnnual",
    # 株式数
    "ShOutFY": "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
    "TrShFY": "NumberOfTreasuryStockAtTheEndOfFiscalYear",
    # 通期予想
    "FSales": "ForecastNetSales",
    "FOP": "ForecastOperatingProfit",
    "FOdP": "ForecastOrdinaryProfit",
    "FNP": "ForecastProfit",
    "FEPS": "ForecastEarningsPerShare",
}

# V2 で追加されたが本プロジェクトでは未使用のフィールド(warning 抑制対象)。
STATEMENTS_IGNORED_FIELDS: frozenset[str] = frozenset(
    {
        # 開示時刻(V1 では DisclosedTime もあるが本プロジェクトでは未使用)
        "DiscTime",
        # 翌事業年度
        "NxtFYSt",
        "NxtFYEn",
        # 四半期配当(年合計のみ使用)
        "Div1Q",
        "Div2Q",
        "Div3Q",
        "DivFY",
        "DivUnit",
        "DivTotalAnn",
        "FDiv1Q",
        "FDiv2Q",
        "FDiv3Q",
        "FDivFY",
        "FDivUnit",
        "FDivTotalAnn",
        "FPayoutRatioAnn",
        "NxFDiv1Q",
        "NxFDiv2Q",
        "NxFDiv3Q",
        "NxFDivFY",
        "NxFDivAnn",
        "NxFDivUnit",
        "NxFPayoutRatioAnn",
        # 中間予想(通期のみ使用)
        "FSales2Q",
        "FOP2Q",
        "FOdP2Q",
        "FNP2Q",
        "FEPS2Q",
        "NxFSales2Q",
        "NxFOP2Q",
        "NxFOdP2Q",
        "NxFNp2Q",
        "NxFEPS2Q",
        # 翌期通期予想
        "NxFSales",
        "NxFOP",
        "NxFOdP",
        "NxFNp",
        "NxFEPS",
        # 期中平均株式数
        "AvgSh",
        # 非連結
        "NCSales",
        "NCOP",
        "NCOdP",
        "NCNP",
        "NCEPS",
        "NCTA",
        "NCEq",
        "NCEqAR",
        "NCBPS",
        "FNCSales2Q",
        "FNCOP2Q",
        "FNCOdP2Q",
        "FNCNP2Q",
        "FNCEPS2Q",
        "NxFNCSales2Q",
        "NxFNCOP2Q",
        "NxFNCOdP2Q",
        "NxFNCNP2Q",
        "NxFNCEPS2Q",
        "FNCSales",
        "FNCOP",
        "FNCOdP",
        "FNCNP",
        "FNCEPS",
        "NxFNCSales",
        "NxFNCOP",
        "NxFNCOdP",
        "NxFNCNp",
        "NxFNCNP",  # 実 API は大文字 P を返す(ドキュメント上は NxFNCNp と揺れ)
        "NxFNCEPS",
        # 会計方針変更等
        "MatChgSub",
        "SigChgInC",
        "ChgByASRev",
        "ChgNoASRev",
        "ChgAcEst",
        "RetroRst",
    }
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _warn_unknown_fields(
    raw: list[dict[str, Any]],
    column_map: dict[str, str],
    ignored: frozenset[str],
    label: str,
) -> None:
    """raw に column_map にも ignored にも含まれないフィールドがあれば warning。

    1 ページに同じ未知フィールドが複数行あっても 1 回だけログを出す。
    """
    if not raw:
        return
    known = set(column_map.keys()) | set(ignored)
    seen_unknown: set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in known:
                seen_unknown.add(key)
    if seen_unknown:
        logger.warning(
            "%s: 未知のフィールドを検出(無視): %s", label, sorted(seen_unknown)
        )


def _project_and_rename(
    raw: list[dict[str, Any]], column_map: dict[str, str]
) -> pd.DataFrame:
    """raw を column_map で射影 + rename し DataFrame を返す。"""
    if not raw:
        # 列名は持たない空 DataFrame を返す(下流の `if df.empty` 判定で問題なし)
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    keep = [c for c in column_map.keys() if c in df.columns]
    df = df[keep].rename(columns=column_map)
    return df


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def normalize_daily_quotes(raw: list[dict[str, Any]]) -> pd.DataFrame:
    """`/v2/equities/bars/daily` の `data` 配列を V1 互換 DataFrame に変換。

    出力カラムは V1 `/v1/prices/daily_quotes` 相当(Date/Code/Open/.../AdjustmentVolume)。
    存在しない V2 フィールド(前場/後場・ストップ高フラグ等)は落とす。
    """
    _warn_unknown_fields(
        raw, DAILY_QUOTES_COLUMN_MAP, DAILY_QUOTES_IGNORED_FIELDS, "daily_quotes"
    )
    return _project_and_rename(raw, DAILY_QUOTES_COLUMN_MAP)


def normalize_listed_info(raw: list[dict[str, Any]]) -> pd.DataFrame:
    """`/v2/equities/master` の `data` 配列を V1 互換 DataFrame に変換。

    出力カラムは V1 `/v1/listed/info` 相当
    (Date/Code/CompanyName/.../MarginCodeName)。
    """
    _warn_unknown_fields(
        raw, LISTED_INFO_COLUMN_MAP, LISTED_INFO_IGNORED_FIELDS, "listed_info"
    )
    return _project_and_rename(raw, LISTED_INFO_COLUMN_MAP)


def normalize_statements(raw: list[dict[str, Any]]) -> pd.DataFrame:
    """`/v2/fins/summary` の `data` 配列を V1 互換 DataFrame に変換。

    出力カラムは V1 `/v1/fins/statements` 相当(本プロジェクトが利用するフィールドのみ)。
    """
    _warn_unknown_fields(
        raw, STATEMENTS_COLUMN_MAP, STATEMENTS_IGNORED_FIELDS, "statements"
    )
    return _project_and_rename(raw, STATEMENTS_COLUMN_MAP)
