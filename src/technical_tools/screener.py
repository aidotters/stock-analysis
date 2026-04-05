"""
Stock screener for filtering and analyzing integrated analysis results.

Provides a Jupyter Notebook-friendly interface for:
- Filtering stocks by technical and fundamental criteria
- Tracking rank changes over time
- Retrieving historical score data
"""

import logging
import re
import sqlite3
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import ClassVar, Optional, Union

import numpy as np
import pandas as pd

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)

# Type for user-facing window specification
WindowInput = Union[int, str, tuple[int, int]]

# Filter parameter -> result column mapping
FILTER_TO_COLUMN = {
    "composite_score_min": "composite_score",
    "composite_score_max": "composite_score",
    "hl_ratio_min": "hl_ratio",
    "hl_ratio_max": "hl_ratio",
    "rsi_min": "rsi",
    "rsi_max": "rsi",
    "market_cap_min": "market_cap",
    "market_cap_max": "market_cap",
    "per_min": "trailing_pe",
    "per_max": "trailing_pe",
    "pbr_max": "price_to_book",
    "roe_min": "return_on_equity",
    "roe_max": "return_on_equity",
    "dividend_yield_min": "dividend_yield",
    "equity_ratio_min": "equity_ratio",
    "equity_ratio_max": "equity_ratio",
    "roa_min": "return_on_assets",
    "roa_max": "return_on_assets",
    "net_cash_ratio_min": "net_cash_ratio",
    "net_cash_ratio_max": "net_cash_ratio",
    "cash_neutral_per_min": "cash_neutral_per",
    "cash_neutral_per_max": "cash_neutral_per",
}

# Include group -> column list mapping
INCLUDE_GROUPS = {
    "scores": [
        "composite_score",
        "composite_score_rank",
        "hl_ratio_rank",
        "rsp_rank",
        "hl_ratio",
        "median_ratio",
        "rsp",
        "rsi",
    ],
    "fundamentals": [
        "trailing_pe",
        "price_to_book",
        "dividend_yield",
        "return_on_equity",
        "equity_ratio",
        "return_on_assets",
    ],
    "valuation": [
        "net_cash_ratio",
        "cash_neutral_per",
        "yf_per",
    ],
}

# Columns always returned
ALWAYS_COLUMNS = ["date", "code", "long_name", "sector", "market_cap"]

# Standard chart classification windows
# Cumulative: 20, 60, 120, 240
# Slice: 2400480=(240,480), 4801200=(480,1200), 12002400=(1200,2400), 24004800=(2400,4800)
STANDARD_CUMULATIVE_WINDOWS = [20, 60, 120, 240]
STANDARD_SLICE_WINDOWS = [2400480, 4801200, 12002400, 24004800]
STANDARD_CHART_WINDOWS = STANDARD_CUMULATIVE_WINDOWS + STANDARD_SLICE_WINDOWS

# Available chart pattern labels from chart_classification templates
PATTERN_LABELS = [
    "上昇ストップ",
    "上昇",
    "急上昇",
    "調整",
    "もみ合い",
    "リバウンド",
    "急落",
    "下落",
    "下げとまった",
    "不明",
]

# Scores-group columns (used to determine if scores tables need JOIN)
_SCORES_COLUMNS = set(INCLUDE_GROUPS["scores"])


def _format_pattern_column(db_window: int) -> str:
    """Convert DB window integer to display column name.

    Cumulative (<=10000): pattern_w20, pattern_w60, etc.
    Slice (>10000): pattern_w240_480, pattern_w480_1200, etc.
    """
    if db_window > 10000:
        start = db_window // 10000
        end = db_window % 10000
        return f"pattern_w{start}_{end}"
    return f"pattern_w{db_window}"


def _format_score_column(db_window: int) -> str:
    """Convert DB window integer to score column name."""
    if db_window > 10000:
        start = db_window // 10000
        end = db_window % 10000
        return f"score_w{start}_{end}"
    return f"score_w{db_window}"


def _normalize_window_value(value: WindowInput) -> int:
    """Convert a user-facing window specification to the DB integer encoding.

    Accepted formats:
        int: 20, 60, 120, 240 (cumulative) or 2400480 (legacy slice encoding)
        tuple: (240, 480) -> 2400480
        str: "240-480", "240_480", "w240_480", "pattern_w240_480",
             "score_w240_480", "20"

    Raises:
        ValueError: If the value cannot be parsed.
        TypeError: If the type is not supported.
    """
    if isinstance(value, int):
        return value

    if isinstance(value, tuple):
        if len(value) != 2:
            raise ValueError(f"Tuple must have exactly 2 elements, got {len(value)}")
        start, end = value
        if not (isinstance(start, int) and isinstance(end, int)):
            raise ValueError(
                f"Tuple elements must be int, got ({type(start).__name__}, {type(end).__name__})"
            )
        if start <= 0 or end <= 0:
            raise ValueError(f"Tuple elements must be positive, got ({start}, {end})")
        if end <= start:
            raise ValueError(
                f"Slice end must be greater than start, got ({start}, {end})"
            )
        return start * 10000 + end

    if isinstance(value, str):
        s = value.strip()
        # Strip known prefixes: "pattern_w", "score_w", "w"
        s = re.sub(r"^(?:pattern_|score_)?w", "", s)
        # Try to parse as range (contains - or _)
        m = re.match(r"^(\d+)[-_](\d+)$", s)
        if m:
            start_val, end_val = int(m.group(1)), int(m.group(2))
            if end_val <= start_val:
                raise ValueError(f"Slice end must be greater than start: '{value}'")
            return start_val * 10000 + end_val
        # Try as plain integer
        if re.match(r"^\d+$", s):
            return int(s)
        raise ValueError(
            f"Cannot parse window value: '{value}'. "
            f"Accepted formats: int, (start, end), "
            f"'240-480', 'w240_480', 'pattern_w240_480'"
        )

    raise TypeError(
        f"Unsupported window type: {type(value).__name__}. "
        f"Expected int, str, or tuple[int, int]."
    )


def _normalize_pattern_window(
    pattern_window: WindowInput | list[WindowInput],
) -> list[int] | None:
    """Normalize pattern_window parameter to list[int] for DB query.

    Returns:
        list[int] of DB-encoded window values, or None if "all".

    Raises:
        ValueError: If any window value is invalid.
    """
    if isinstance(pattern_window, str) and pattern_window.strip().lower() == "all":
        return None

    if isinstance(pattern_window, list):
        result = []
        for v in pattern_window:
            if isinstance(v, str) and v.strip().lower() == "all":
                raise ValueError("'all' cannot be used inside a list")
            result.append(_normalize_window_value(v))
        return result

    return [_normalize_window_value(pattern_window)]


@dataclass
class ScreenerFilter:
    """Filter configuration for StockScreener.

    Groups filter parameters into logical categories for better organization.
    Can be used with StockScreener.filter() method.

    Example:
        >>> filter_config = ScreenerFilter(
        ...     composite_score_min=70.0,
        ...     hl_ratio_min=80.0,
        ...     market_cap_min=100_000_000_000,
        ...     per_max=15.0,
        ... )
        >>> results = screener.filter(filter_config)
    """

    # Date selection
    date: Optional[str] = None

    # Technical indicators
    composite_score_min: Optional[float] = None
    composite_score_max: Optional[float] = None
    hl_ratio_min: Optional[float] = None
    hl_ratio_max: Optional[float] = None
    rsi_min: Optional[float] = None
    rsi_max: Optional[float] = None

    # Fundamental indicators
    market_cap_min: Optional[float] = None
    market_cap_max: Optional[float] = None
    per_min: Optional[float] = None
    per_max: Optional[float] = None
    pbr_max: Optional[float] = None
    roe_min: Optional[float] = None
    roe_max: Optional[float] = None
    dividend_yield_min: Optional[float] = None
    equity_ratio_min: Optional[float] = None
    equity_ratio_max: Optional[float] = None
    roa_min: Optional[float] = None
    roa_max: Optional[float] = None

    # Valuation (yfinance_valuation)
    net_cash_ratio_min: Optional[float] = None
    net_cash_ratio_max: Optional[float] = None
    cash_neutral_per_min: Optional[float] = None
    cash_neutral_per_max: Optional[float] = None

    # Chart pattern
    pattern_window: Optional[WindowInput | list[WindowInput]] = None
    pattern_labels: Optional[list[str]] = field(default=None)

    # Other
    sector: Optional[str] = None
    include: Optional[list[str] | str] = None
    limit: int = 100

    _FIELD_CATEGORIES: ClassVar[dict[str, str]] = {
        "date": "日付",
        "composite_score_min": "テクニカル",
        "composite_score_max": "テクニカル",
        "hl_ratio_min": "テクニカル",
        "hl_ratio_max": "テクニカル",
        "rsi_min": "テクニカル",
        "rsi_max": "テクニカル",
        "market_cap_min": "ファンダメンタル",
        "market_cap_max": "ファンダメンタル",
        "per_min": "ファンダメンタル",
        "per_max": "ファンダメンタル",
        "pbr_max": "ファンダメンタル",
        "roe_min": "ファンダメンタル",
        "roe_max": "ファンダメンタル",
        "dividend_yield_min": "ファンダメンタル",
        "equity_ratio_min": "ファンダメンタル",
        "equity_ratio_max": "ファンダメンタル",
        "roa_min": "ファンダメンタル",
        "roa_max": "ファンダメンタル",
        "net_cash_ratio_min": "バリュエーション",
        "net_cash_ratio_max": "バリュエーション",
        "cash_neutral_per_min": "バリュエーション",
        "cash_neutral_per_max": "バリュエーション",
        "pattern_window": "チャートパターン",
        "pattern_labels": "チャートパターン",
        "sector": "その他",
        "include": "その他",
        "limit": "その他",
    }

    @classmethod
    def available_filters(cls) -> pd.DataFrame:
        """Return all available filter parameters as a DataFrame.

        Returns:
            DataFrame with columns: parameter, type, category, default
        """
        rows = []
        for f in fields(cls):
            if f.name.startswith("_"):
                continue
            raw = str(f.type).replace("typing.", "")
            # Remove Optional wrapper: Optional[X] -> X
            if raw.startswith("Optional[") and raw.endswith("]"):
                raw = raw[len("Optional[") : -1]
            type_str = raw
            rows.append(
                {
                    "parameter": f.name,
                    "type": type_str,
                    "category": cls._FIELD_CATEGORIES.get(f.name, ""),
                    "default": f.default if f.default is not field else None,
                }
            )
        return pd.DataFrame(rows)

    @classmethod
    def available_categories(cls) -> list[str]:
        """Return list of unique filter categories."""
        return sorted(set(cls._FIELD_CATEGORIES.values()))

    @classmethod
    def filters_by_category(cls, category: str) -> list[str]:
        """Return filter parameter names for a given category.

        Args:
            category: Category name (e.g., 'テクニカル', 'ファンダメンタル')
        """
        return [k for k, v in cls._FIELD_CATEGORIES.items() if v == category]

    def to_dict(self) -> dict:
        """Convert filter to dictionary for use with filter() method."""
        return {k: v for k, v in self.__dict__.items() if v is not None}


def _normalize_code(code: str) -> str:
    """Normalize stock code to 5-digit format (with trailing '0').

    The analysis_results.db stores codes in J-Quants 5-digit format (e.g., '36630'),
    but users typically pass 4-digit codes (e.g., '3663').
    """
    if len(code) == 4 and code.isdigit():
        return code + "0"
    return code


class StockScreener:
    """Stock screener for integrated analysis results."""

    def __init__(
        self,
        analysis_db_path: Optional[Path] = None,
        statements_db_path: Optional[Path] = None,
    ):
        """Initialize the screener.

        Args:
            analysis_db_path: Path to analysis_results.db. If None, uses settings default.
            statements_db_path: Path to statements.db. If None, uses settings default.
        """
        settings = get_settings()
        self.analysis_db_path = analysis_db_path or settings.paths.analysis_db
        self.statements_db_path = statements_db_path or settings.paths.statements_db

    def _get_analysis_connection(self) -> sqlite3.Connection:
        """Get a connection to the analysis database."""
        conn = sqlite3.connect(self.analysis_db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _get_statements_connection(self) -> sqlite3.Connection:
        """Get a connection to the statements database."""
        conn = sqlite3.connect(self.statements_db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _get_latest_date(self) -> Optional[str]:
        """Get the latest date from integrated_scores table."""
        with self._get_analysis_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(Date) FROM integrated_scores")
            result = cursor.fetchone()
        return result[0] if result and result[0] else None

    def filter(
        self,
        filter_config: Optional[ScreenerFilter] = None,
        *,
        date: Optional[str] = None,
        # Technical indicators
        composite_score_min: Optional[float] = None,
        composite_score_max: Optional[float] = None,
        hl_ratio_min: Optional[float] = None,
        hl_ratio_max: Optional[float] = None,
        rsi_min: Optional[float] = None,
        rsi_max: Optional[float] = None,
        # Fundamental indicators (calculated_fundamentals JOIN)
        market_cap_min: Optional[float] = None,
        market_cap_max: Optional[float] = None,
        per_min: Optional[float] = None,
        per_max: Optional[float] = None,
        pbr_max: Optional[float] = None,
        roe_min: Optional[float] = None,
        roe_max: Optional[float] = None,
        dividend_yield_min: Optional[float] = None,
        equity_ratio_min: Optional[float] = None,
        equity_ratio_max: Optional[float] = None,
        roa_min: Optional[float] = None,
        roa_max: Optional[float] = None,
        # Valuation (yfinance_valuation JOIN)
        net_cash_ratio_min: Optional[float] = None,
        net_cash_ratio_max: Optional[float] = None,
        cash_neutral_per_min: Optional[float] = None,
        cash_neutral_per_max: Optional[float] = None,
        # Chart pattern (classification_results JOIN)
        pattern_window: Optional[WindowInput | list[WindowInput]] = None,
        pattern_labels: Optional[list[str]] = None,
        # Other
        sector: Optional[str] = None,
        include: Optional[list[str] | str] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Filter stocks by multiple criteria.

        Returns only the columns relevant to the applied filters and include
        groups, plus the 5 always-returned columns (date, code, long_name,
        sector, market_cap).

        Args:
            filter_config: ScreenerFilter object with filter parameters.
                If provided, other keyword arguments are ignored.
            date: Analysis date. If None, uses latest date.
            composite_score_min: Minimum composite score.
            composite_score_max: Maximum composite score.
            hl_ratio_min: Minimum HL ratio.
            hl_ratio_max: Maximum HL ratio.
            rsi_min: Minimum RSI.
            rsi_max: Maximum RSI.
            market_cap_min: Minimum market cap.
            market_cap_max: Maximum market cap.
            per_min: Minimum P/E ratio.
            per_max: Maximum P/E ratio.
            pbr_max: Maximum P/B ratio.
            roe_min: Minimum ROE.
            roe_max: Maximum ROE.
            dividend_yield_min: Minimum dividend yield.
            equity_ratio_min: Minimum equity ratio.
            equity_ratio_max: Maximum equity ratio.
            roa_min: Minimum ROA.
            roa_max: Maximum ROA.
            net_cash_ratio_min: Minimum net cash ratio.
            net_cash_ratio_max: Maximum net cash ratio.
            cash_neutral_per_min: Minimum cash neutral PER.
            cash_neutral_per_max: Maximum cash neutral PER.
            pattern_window: Chart pattern window. Accepts:
                - int: 20, 60, 120, 240 (cumulative)
                - tuple: (240, 480) for slice windows
                - str: "240-480", "240_480", "w240_480", "pattern_w240_480"
                - "all": all standard windows
                - list of the above for multi-window AND filtering
            pattern_labels: List of pattern labels to include.
            sector: Sector filter (from calculated_fundamentals).
            include: Column groups to include. List of group names
                ("scores", "fundamentals", "valuation") or "all".
            limit: Maximum number of results.

        Returns:
            DataFrame with filtered stocks.

        Example:
            >>> results = screener.filter(composite_score_min=70.0)
            >>> results = screener.filter(include=["fundamentals"])
            >>> results = screener.filter(composite_score_min=70.0, include="all")
        """
        # If ScreenerFilter object is provided, extract parameters from it
        if filter_config is not None:
            date = filter_config.date
            composite_score_min = filter_config.composite_score_min
            composite_score_max = filter_config.composite_score_max
            hl_ratio_min = filter_config.hl_ratio_min
            hl_ratio_max = filter_config.hl_ratio_max
            rsi_min = filter_config.rsi_min
            rsi_max = filter_config.rsi_max
            market_cap_min = filter_config.market_cap_min
            market_cap_max = filter_config.market_cap_max
            per_min = filter_config.per_min
            per_max = filter_config.per_max
            pbr_max = filter_config.pbr_max
            roe_min = filter_config.roe_min
            roe_max = filter_config.roe_max
            dividend_yield_min = filter_config.dividend_yield_min
            equity_ratio_min = filter_config.equity_ratio_min
            equity_ratio_max = filter_config.equity_ratio_max
            roa_min = filter_config.roa_min
            roa_max = filter_config.roa_max
            net_cash_ratio_min = filter_config.net_cash_ratio_min
            net_cash_ratio_max = filter_config.net_cash_ratio_max
            cash_neutral_per_min = filter_config.cash_neutral_per_min
            cash_neutral_per_max = filter_config.cash_neutral_per_max
            pattern_window = filter_config.pattern_window
            pattern_labels = filter_config.pattern_labels
            sector = filter_config.sector
            include = filter_config.include
            limit = filter_config.limit

        if date is None:
            date = self._get_latest_date()
            if date is None:
                logger.warning("No data available in integrated_scores")
                return pd.DataFrame()

        # --- Collect requested columns from filters ---
        filter_params = {
            "composite_score_min": composite_score_min,
            "composite_score_max": composite_score_max,
            "hl_ratio_min": hl_ratio_min,
            "hl_ratio_max": hl_ratio_max,
            "rsi_min": rsi_min,
            "rsi_max": rsi_max,
            "market_cap_min": market_cap_min,
            "market_cap_max": market_cap_max,
            "per_min": per_min,
            "per_max": per_max,
            "pbr_max": pbr_max,
            "roe_min": roe_min,
            "roe_max": roe_max,
            "dividend_yield_min": dividend_yield_min,
            "equity_ratio_min": equity_ratio_min,
            "equity_ratio_max": equity_ratio_max,
            "roa_min": roa_min,
            "roa_max": roa_max,
            "net_cash_ratio_min": net_cash_ratio_min,
            "net_cash_ratio_max": net_cash_ratio_max,
            "cash_neutral_per_min": cash_neutral_per_min,
            "cash_neutral_per_max": cash_neutral_per_max,
        }

        requested_columns: set[str] = set()
        for param_name, value in filter_params.items():
            if value is not None and param_name in FILTER_TO_COLUMN:
                requested_columns.add(FILTER_TO_COLUMN[param_name])

        # --- Expand include groups ---
        if include == "all":
            include_list = list(INCLUDE_GROUPS.keys())
        elif isinstance(include, list):
            include_list = include
        elif isinstance(include, str):
            include_list = [include]
        else:
            include_list = []

        for group in include_list:
            if group in INCLUDE_GROUPS:
                requested_columns.update(INCLUDE_GROUPS[group])
            else:
                logger.warning(f"Unknown include group '{group}', ignoring.")

        # --- Base query: date, code only ---
        base_query = """
            SELECT
                i.Date as date,
                i.Code as code
            FROM integrated_scores i
            WHERE i.Date = ?
        """
        params: list = [date]

        # Technical filters on integrated_scores
        if composite_score_min is not None:
            base_query += " AND i.composite_score >= ?"
            params.append(composite_score_min)
        if composite_score_max is not None:
            base_query += " AND i.composite_score <= ?"
            params.append(composite_score_max)

        base_query += " ORDER BY i.composite_score DESC"

        # Execute base query
        with self._get_analysis_connection() as conn:
            df = pd.read_sql(base_query, conn, params=params)

        if df.empty:
            return df

        # --- Always JOIN: fundamentals (long_name, sector, cf_market_cap) ---
        try:
            with self._get_statements_connection() as conn:
                fundamentals_query = """
                    SELECT
                        code,
                        company_name as long_name,
                        sector_33 as sector,
                        market_cap as cf_market_cap,
                        per as trailing_pe,
                        pbr as price_to_book,
                        dividend_yield,
                        roe as return_on_equity,
                        equity_ratio,
                        roa as return_on_assets
                    FROM calculated_fundamentals
                """
                fundamentals_df = pd.read_sql(fundamentals_query, conn)

            if not fundamentals_df.empty:
                df = df.merge(fundamentals_df, on="code", how="left")
        except Exception as e:
            logger.warning(f"Could not load fundamentals data: {e}")

        # --- Always JOIN: yfinance_valuation (yf_market_cap for COALESCE) ---
        try:
            with self._get_statements_connection() as conn:
                valuation_query = """
                    SELECT
                        CASE
                            WHEN LENGTH(code) = 4 THEN code || '0'
                            ELSE code
                        END as code,
                        net_cash_ratio,
                        cash_neutral_per,
                        market_cap as yf_market_cap,
                        per as yf_per
                    FROM yfinance_valuation
                """
                valuation_df = pd.read_sql(valuation_query, conn)

            if not valuation_df.empty:
                df = df.merge(valuation_df, on="code", how="left")
        except Exception as e:
            logger.warning(f"Could not load valuation data: {e}")

        # --- COALESCE market_cap: yf_market_cap preferred, fallback cf_market_cap ---
        if "yf_market_cap" in df.columns and "cf_market_cap" in df.columns:
            df["market_cap"] = df["yf_market_cap"].fillna(df["cf_market_cap"])
        elif "cf_market_cap" in df.columns:
            df["market_cap"] = df["cf_market_cap"]
        elif "yf_market_cap" in df.columns:
            df["market_cap"] = df["yf_market_cap"]

        # Ensure ALWAYS_COLUMNS exist (NaN if no data available)
        for col in ALWAYS_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        # --- Apply fundamental filters ---
        if market_cap_min is not None and "market_cap" in df.columns:
            df = df[df["market_cap"] >= market_cap_min]
        if market_cap_max is not None and "market_cap" in df.columns:
            df = df[df["market_cap"] <= market_cap_max]
        if per_min is not None and "trailing_pe" in df.columns:
            df = df[df["trailing_pe"] >= per_min]
        if per_max is not None and "trailing_pe" in df.columns:
            df = df[df["trailing_pe"] <= per_max]
        if pbr_max is not None and "price_to_book" in df.columns:
            df = df[df["price_to_book"] <= pbr_max]
        if roe_min is not None and "return_on_equity" in df.columns:
            df = df[df["return_on_equity"] >= roe_min]
        if roe_max is not None and "return_on_equity" in df.columns:
            df = df[df["return_on_equity"] <= roe_max]
        if dividend_yield_min is not None and "dividend_yield" in df.columns:
            df = df[df["dividend_yield"] >= dividend_yield_min]
        if equity_ratio_min is not None and "equity_ratio" in df.columns:
            df = df[df["equity_ratio"] >= equity_ratio_min]
        if equity_ratio_max is not None and "equity_ratio" in df.columns:
            df = df[df["equity_ratio"] <= equity_ratio_max]
        if roa_min is not None and "return_on_assets" in df.columns:
            df = df[df["return_on_assets"] >= roa_min]
        if roa_max is not None and "return_on_assets" in df.columns:
            df = df[df["return_on_assets"] <= roa_max]
        if sector is not None and "sector" in df.columns:
            df = df[df["sector"] == sector]

        # --- Apply valuation filters ---
        if net_cash_ratio_min is not None and "net_cash_ratio" in df.columns:
            df = df[df["net_cash_ratio"] >= net_cash_ratio_min]
        if net_cash_ratio_max is not None and "net_cash_ratio" in df.columns:
            df = df[df["net_cash_ratio"] <= net_cash_ratio_max]
        if cash_neutral_per_min is not None and "cash_neutral_per" in df.columns:
            df = df[df["cash_neutral_per"] >= cash_neutral_per_min]
        if cash_neutral_per_max is not None and "cash_neutral_per" in df.columns:
            df = df[df["cash_neutral_per"] <= cash_neutral_per_max]

        # --- Conditionally JOIN scores tables (hl_ratio, relative_strength) ---
        needs_scores = bool(requested_columns & _SCORES_COLUMNS) or any(
            v is not None for v in [hl_ratio_min, hl_ratio_max, rsi_min, rsi_max]
        )

        if needs_scores and not df.empty:
            with self._get_analysis_connection() as conn:
                # Fetch scores columns from integrated_scores
                scores_query = """
                    SELECT Code as code, composite_score, composite_score_rank,
                           hl_ratio_rank, rsp_rank
                    FROM integrated_scores
                    WHERE Date = ?
                """
                scores_df = pd.read_sql(scores_query, conn, params=[date])

                hl_query = """
                    SELECT Code as code, HlRatio as hl_ratio,
                           MedianRatio as median_ratio
                    FROM hl_ratio
                    WHERE Date = ?
                """
                hl_df = pd.read_sql(hl_query, conn, params=[date])

                rs_query = """
                    SELECT Code as code,
                           RelativeStrengthPercentage as rsp,
                           RelativeStrengthIndex as rsi
                    FROM relative_strength
                    WHERE Date = ?
                """
                rs_df = pd.read_sql(rs_query, conn, params=[date])

            if not scores_df.empty:
                df = df.merge(scores_df, on="code", how="left")
            if not hl_df.empty:
                df = df.merge(hl_df, on="code", how="left")
            if not rs_df.empty:
                df = df.merge(rs_df, on="code", how="left")

            # Apply HL ratio filters
            if hl_ratio_min is not None and "hl_ratio" in df.columns:
                df = df[df["hl_ratio"] >= hl_ratio_min]
            if hl_ratio_max is not None and "hl_ratio" in df.columns:
                df = df[df["hl_ratio"] <= hl_ratio_max]

            # Apply RSI filters
            if rsi_min is not None and "rsi" in df.columns:
                df = df[df["rsi"] >= rsi_min]
            if rsi_max is not None and "rsi" in df.columns:
                df = df[df["rsi"] <= rsi_max]

        # --- JOIN with pattern data if needed ---
        resolved_windows: list[int] | None = None
        if pattern_window is not None and not df.empty:
            # Normalize before try/except so validation errors propagate
            windows_list = _normalize_pattern_window(pattern_window)
            is_multi = windows_list is None or len(windows_list) > 1
            try:
                with self._get_analysis_connection() as conn:
                    if windows_list is None:
                        # "all": fetch standard windows only
                        placeholders = ",".join("?" * len(STANDARD_CHART_WINDOWS))
                        pattern_query = f"""
                            SELECT ticker AS code, window, pattern_label, score
                            FROM classification_results
                            WHERE date = ? AND window IN ({placeholders})
                        """
                        pattern_params: list[str | int] = [
                            date
                        ] + STANDARD_CHART_WINDOWS
                        pattern_df = pd.read_sql(
                            pattern_query,
                            conn,
                            params=pattern_params,
                        )
                    else:
                        placeholders = ",".join("?" * len(windows_list))
                        pattern_query = f"""
                            SELECT ticker AS code, window, pattern_label, score
                            FROM classification_results
                            WHERE date = ? AND window IN ({placeholders})
                        """
                        window_params: list[str | int] = [date] + windows_list
                        pattern_df = pd.read_sql(
                            pattern_query,
                            conn,
                            params=window_params,
                        )

                if not pattern_df.empty:
                    if not is_multi:
                        # Single window: cell-level filter (inner join)
                        if pattern_labels is not None:
                            pattern_df = pattern_df[
                                pattern_df["pattern_label"].isin(pattern_labels)
                            ]
                        single_df = pattern_df.drop(columns=["window"])
                        df = df.merge(single_df, on="code", how="inner")
                    else:
                        # Multi-window mode
                        if windows_list is not None:
                            # Explicit list: AND logic — require matching
                            # labels in ALL specified windows
                            if pattern_labels is not None:
                                filtered = pattern_df[
                                    pattern_df["pattern_label"].isin(pattern_labels)
                                ]
                                required = len(windows_list)
                                counts = filtered.groupby("code")["window"].nunique()
                                valid_codes = counts[counts == required].index.tolist()
                            else:
                                required = len(windows_list)
                                counts = pattern_df.groupby("code")["window"].nunique()
                                valid_codes = counts[counts == required].index.tolist()
                        else:
                            # "all": AND logic — all existing windows must
                            # match pattern_labels (NaN windows ignored)
                            if pattern_labels is not None:
                                filtered = pattern_df[
                                    pattern_df["pattern_label"].isin(pattern_labels)
                                ]
                                # Count existing windows vs matching windows
                                total_per_code = pattern_df.groupby("code")[
                                    "window"
                                ].nunique()
                                match_per_code = filtered.groupby("code")[
                                    "window"
                                ].nunique()
                                # Stock passes if all its windows match
                                valid_codes = [
                                    code
                                    for code in total_per_code.index
                                    if match_per_code.get(code, 0)
                                    == total_per_code[code]
                                ]
                            else:
                                valid_codes = pattern_df["code"].unique().tolist()

                        pattern_df = pattern_df[pattern_df["code"].isin(valid_codes)]
                        if windows_list is None:
                            resolved_windows = STANDARD_CHART_WINDOWS
                        else:
                            resolved_windows = sorted(
                                pattern_df["window"].unique().tolist()
                            )

                        # Pivot to wide format
                        label_pivot = pattern_df.pivot(
                            index="code",
                            columns="window",
                            values="pattern_label",
                        )
                        score_pivot = pattern_df.pivot(
                            index="code",
                            columns="window",
                            values="score",
                        )

                        # For "all" mode, ensure all standard windows
                        # have columns (NaN for missing)
                        if windows_list is None:
                            for w in STANDARD_CHART_WINDOWS:
                                if w not in label_pivot.columns:
                                    label_pivot[w] = np.nan
                                if w not in score_pivot.columns:
                                    score_pivot[w] = np.nan
                            label_pivot = label_pivot[STANDARD_CHART_WINDOWS]
                            score_pivot = score_pivot[STANDARD_CHART_WINDOWS]

                        label_pivot.columns = [
                            _format_pattern_column(int(w)) for w in label_pivot.columns
                        ]
                        score_pivot.columns = [
                            _format_score_column(int(w)) for w in score_pivot.columns
                        ]
                        pivot_df = pd.concat(
                            [label_pivot, score_pivot], axis=1
                        ).reset_index()
                        df = df.merge(pivot_df, on="code", how="inner")
            except Exception as e:
                logger.warning(f"Could not load pattern data: {e}")

        # Apply limit
        df = df.head(limit)

        # --- Select only requested columns ---
        # Always include base 5 columns + requested columns
        select_cols: list[str] = []
        preferred_order = [
            # Basic info (always)
            "code",
            "long_name",
            "sector",
            "market_cap",
            # Scores
            "composite_score",
            "composite_score_rank",
            # Technical indicators
            "hl_ratio",
            "hl_ratio_rank",
            "rsp",
            "rsi",
            "rsp_rank",
            "median_ratio",
            # Fundamentals
            "trailing_pe",
            "price_to_book",
            "dividend_yield",
            "return_on_equity",
            "equity_ratio",
            "return_on_assets",
            # Valuation
            "net_cash_ratio",
            "cash_neutral_per",
            "yf_per",
            # Meta
            "date",
        ]

        # Build the allowed column set: always columns + requested columns
        allowed = set(ALWAYS_COLUMNS) | requested_columns
        # Pattern columns are always included when pattern_window is used
        if pattern_window is not None:
            if resolved_windows is not None:
                # Multi-window: pivoted columns
                for w in resolved_windows:
                    allowed.update([_format_pattern_column(w), _format_score_column(w)])
            else:
                # Single window: legacy columns
                allowed.update(["pattern_label", "score"])

        for col in preferred_order:
            if col in allowed and col in df.columns:
                select_cols.append(col)

        # Add any remaining allowed columns not in preferred_order
        for col in df.columns:
            if col in allowed and col not in select_cols:
                select_cols.append(col)

        df = df[select_cols]

        # Reset index for clean output
        df = df.reset_index(drop=True)

        return df

    # Valid metrics for rank_changes
    VALID_METRICS = frozenset({"composite_score", "hl_ratio", "rsp"})

    def rank_changes(
        self,
        metric: str = "composite_score",
        days: int = 7,
        direction: str = "up",
        min_change: int = 1,
        limit: int = 50,
    ) -> pd.DataFrame:
        """Get stocks with significant rank changes.

        Args:
            metric: Rank metric (composite_score, hl_ratio, rsp).
            days: Number of days to compare.
            direction: 'up' for improved, 'down' for worsened, 'both' for all.
            min_change: Minimum rank change to include.
            limit: Maximum number of results.

        Returns:
            DataFrame with rank changes.

        Raises:
            ValueError: If metric is not one of the valid values.
        """
        if metric not in self.VALID_METRICS:
            raise ValueError(
                f"Invalid metric '{metric}'. Must be one of: {sorted(self.VALID_METRICS)}"
            )

        rank_column = f"{metric}_rank"

        latest_date = self._get_latest_date()
        if not latest_date:
            return pd.DataFrame()

        with self._get_analysis_connection() as conn:
            # Use SQL OFFSET to get the historical date correctly
            query = f"""
                WITH latest AS (
                    SELECT Code, {rank_column} as current_rank
                    FROM integrated_scores
                    WHERE Date = ?
                ),
                historical AS (
                    SELECT Code, {rank_column} as past_rank
                    FROM integrated_scores
                    WHERE Date = (
                        SELECT Date FROM integrated_scores
                        WHERE Date < ?
                        ORDER BY Date DESC
                        LIMIT 1 OFFSET ?
                    )
                )
                SELECT
                    l.Code as code,
                    l.current_rank,
                    h.past_rank,
                    (h.past_rank - l.current_rank) as rank_change
                FROM latest l
                JOIN historical h ON l.Code = h.Code
                WHERE l.current_rank IS NOT NULL
                  AND h.past_rank IS NOT NULL
            """

            df = pd.read_sql(query, conn, params=[latest_date, latest_date, days - 1])

        if df.empty:
            return df

        # Filter by direction
        if direction == "up":
            df = df[df["rank_change"] > 0]
        elif direction == "down":
            df = df[df["rank_change"] < 0]

        # Filter by minimum change
        df = df[abs(df["rank_change"]) >= min_change]

        # Sort by absolute change
        df = df.sort_values("rank_change", ascending=False, key=abs).head(limit)

        return df

    def history(
        self,
        code: str,
        days: int = 30,
    ) -> pd.DataFrame:
        """Get historical scores for a specific stock.

        Args:
            code: Stock code (4-digit or 5-digit).
            days: Number of days of history.

        Returns:
            DataFrame with historical scores.
        """
        db_code = _normalize_code(code)
        with self._get_analysis_connection() as conn:
            query = """
                SELECT
                    Date as date,
                    Code as code,
                    composite_score,
                    composite_score_rank,
                    hl_ratio_rank,
                    rsp_rank
                FROM integrated_scores
                WHERE Code = ?
                ORDER BY Date DESC
                LIMIT ?
            """
            df = pd.read_sql(query, conn, params=[db_code, days])

        return df
