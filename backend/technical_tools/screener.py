"""
Stock screener for filtering and analyzing integrated analysis results.

Provides a Jupyter Notebook-friendly interface for:
- Filtering stocks by technical and fundamental criteria
- Tracking rank changes over time
- Retrieving historical score data
"""

import logging
import sqlite3
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import ClassVar, Optional

import pandas as pd

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


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
    dividend_yield_min: Optional[float] = None

    # Valuation (yfinance_valuation)
    net_cash_ratio_min: Optional[float] = None
    net_cash_ratio_max: Optional[float] = None
    cash_neutral_per_min: Optional[float] = None
    cash_neutral_per_max: Optional[float] = None

    # Chart pattern
    pattern_window: Optional[int] = None
    pattern_labels: Optional[list[str]] = field(default=None)

    # Other
    sector: Optional[str] = None
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
        "dividend_yield_min": "ファンダメンタル",
        "net_cash_ratio_min": "バリュエーション",
        "net_cash_ratio_max": "バリュエーション",
        "cash_neutral_per_min": "バリュエーション",
        "cash_neutral_per_max": "バリュエーション",
        "pattern_window": "チャートパターン",
        "pattern_labels": "チャートパターン",
        "sector": "その他",
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
        dividend_yield_min: Optional[float] = None,
        # Valuation (yfinance_valuation JOIN)
        net_cash_ratio_min: Optional[float] = None,
        net_cash_ratio_max: Optional[float] = None,
        cash_neutral_per_min: Optional[float] = None,
        cash_neutral_per_max: Optional[float] = None,
        # Chart pattern (classification_results JOIN)
        pattern_window: Optional[int] = None,
        pattern_labels: Optional[list[str]] = None,
        # Other
        sector: Optional[str] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Filter stocks by multiple criteria.

        Technical indicators are from integrated_scores table.
        Fundamental indicators are from statements.db/calculated_fundamentals.
        Chart patterns are from classification_results table.

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
            dividend_yield_min: Minimum dividend yield.
            pattern_window: Chart pattern window (20, 60, 120, 240, 960, 1200).
            pattern_labels: List of pattern labels to include.
            sector: Sector filter (from calculated_fundamentals).
            limit: Maximum number of results.

        Returns:
            DataFrame with filtered stocks.

        Example:
            Using keyword arguments (original API):
                >>> results = screener.filter(composite_score_min=70.0)

            Using ScreenerFilter object:
                >>> config = ScreenerFilter(composite_score_min=70.0, hl_ratio_min=80.0)
                >>> results = screener.filter(config)
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
            dividend_yield_min = filter_config.dividend_yield_min
            net_cash_ratio_min = filter_config.net_cash_ratio_min
            net_cash_ratio_max = filter_config.net_cash_ratio_max
            cash_neutral_per_min = filter_config.cash_neutral_per_min
            cash_neutral_per_max = filter_config.cash_neutral_per_max
            pattern_window = filter_config.pattern_window
            pattern_labels = filter_config.pattern_labels
            sector = filter_config.sector
            limit = filter_config.limit

        if date is None:
            date = self._get_latest_date()
            if date is None:
                logger.warning("No data available in integrated_scores")
                return pd.DataFrame()

        # Build base query from integrated_scores
        base_query = """
            SELECT
                i.Date,
                i.Code,
                i.composite_score,
                i.composite_score_rank,
                i.hl_ratio_rank,
                i.rsp_rank
            FROM integrated_scores i
            WHERE i.Date = ?
        """
        params = [date]

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

        # JOIN with hl_ratio and relative_strength tables
        with self._get_analysis_connection() as conn:
            hl_query = """
                SELECT Code, HlRatio, MedianRatio
                FROM hl_ratio
                WHERE Date = ?
            """
            hl_df = pd.read_sql(hl_query, conn, params=[date])

            rs_query = """
                SELECT Code, RelativeStrengthPercentage, RelativeStrengthIndex
                FROM relative_strength
                WHERE Date = ?
            """
            rs_df = pd.read_sql(rs_query, conn, params=[date])

        # Merge technical data
        if not hl_df.empty:
            df = df.merge(hl_df, on="Code", how="left")
        if not rs_df.empty:
            df = df.merge(rs_df, on="Code", how="left")

        # Apply HL ratio filters
        if hl_ratio_min is not None and "HlRatio" in df.columns:
            df = df[df["HlRatio"] >= hl_ratio_min]
        if hl_ratio_max is not None and "HlRatio" in df.columns:
            df = df[df["HlRatio"] <= hl_ratio_max]

        # Apply RSI filters
        if rsi_min is not None and "RelativeStrengthIndex" in df.columns:
            df = df[df["RelativeStrengthIndex"] >= rsi_min]
        if rsi_max is not None and "RelativeStrengthIndex" in df.columns:
            df = df[df["RelativeStrengthIndex"] <= rsi_max]

        # JOIN with fundamentals if needed
        needs_fundamentals = any(
            [
                market_cap_min,
                market_cap_max,
                per_min,
                per_max,
                pbr_max,
                roe_min,
                dividend_yield_min,
                sector,
            ]
        )

        if needs_fundamentals and not df.empty:
            try:
                with self._get_statements_connection() as conn:
                    fundamentals_query = """
                        SELECT
                            code as Code,
                            company_name as longName,
                            sector_33 as sector,
                            market_cap as marketCap,
                            per as trailingPE,
                            pbr as priceToBook,
                            dividend_yield as dividendYield,
                            roe as returnOnEquity
                        FROM calculated_fundamentals
                    """
                    fundamentals_df = pd.read_sql(fundamentals_query, conn)

                if not fundamentals_df.empty:
                    df = df.merge(fundamentals_df, on="Code", how="left")

                    # Apply fundamental filters
                    if market_cap_min is not None:
                        df = df[df["marketCap"] >= market_cap_min]
                    if market_cap_max is not None:
                        df = df[df["marketCap"] <= market_cap_max]
                    if per_min is not None:
                        df = df[df["trailingPE"] >= per_min]
                    if per_max is not None:
                        df = df[df["trailingPE"] <= per_max]
                    if pbr_max is not None:
                        df = df[df["priceToBook"] <= pbr_max]
                    if roe_min is not None:
                        df = df[df["returnOnEquity"] >= roe_min]
                    if dividend_yield_min is not None:
                        df = df[df["dividendYield"] >= dividend_yield_min]
                    if sector is not None:
                        df = df[df["sector"] == sector]
            except Exception as e:
                logger.warning(f"Could not load fundamentals data: {e}")

        # JOIN with yfinance_valuation if needed
        needs_valuation = any(
            v is not None
            for v in [
                net_cash_ratio_min,
                net_cash_ratio_max,
                cash_neutral_per_min,
                cash_neutral_per_max,
            ]
        )

        if needs_valuation and not df.empty:
            try:
                with self._get_statements_connection() as conn:
                    valuation_query = """
                        SELECT
                            CASE
                                WHEN LENGTH(code) = 4 THEN code || '0'
                                ELSE code
                            END as Code,
                            net_cash_ratio,
                            cash_neutral_per,
                            market_cap as yf_market_cap,
                            per as yf_per
                        FROM yfinance_valuation
                    """
                    valuation_df = pd.read_sql(valuation_query, conn)

                if not valuation_df.empty:
                    df = df.merge(valuation_df, on="Code", how="left")

                    if net_cash_ratio_min is not None:
                        df = df[df["net_cash_ratio"] >= net_cash_ratio_min]
                    if net_cash_ratio_max is not None:
                        df = df[df["net_cash_ratio"] <= net_cash_ratio_max]
                    if cash_neutral_per_min is not None:
                        df = df[df["cash_neutral_per"] >= cash_neutral_per_min]
                    if cash_neutral_per_max is not None:
                        df = df[df["cash_neutral_per"] <= cash_neutral_per_max]
            except Exception as e:
                logger.warning(f"Could not load valuation data: {e}")

        # JOIN with pattern data if needed
        if pattern_window is not None and not df.empty:
            try:
                with self._get_analysis_connection() as conn:
                    pattern_query = """
                        SELECT ticker as Code, pattern_label, score
                        FROM classification_results
                        WHERE date = ? AND window = ?
                    """
                    pattern_df = pd.read_sql(
                        pattern_query, conn, params=[date, pattern_window]
                    )

                if not pattern_df.empty:
                    df = df.merge(pattern_df, on="Code", how="inner")

                    if pattern_labels is not None:
                        df = df[df["pattern_label"].isin(pattern_labels)]
            except Exception as e:
                logger.warning(f"Could not load pattern data: {e}")

        # Apply limit
        df = df.head(limit)

        # Reorder columns for readability
        preferred_order = [
            # Basic info
            "Code",
            "longName",
            "sector",
            "marketCap",
            # Scores
            "composite_score",
            "composite_score_rank",
            # Technical indicators
            "HlRatio",
            "hl_ratio_rank",
            "RelativeStrengthPercentage",
            "RelativeStrengthIndex",
            "rsp_rank",
            "MedianRatio",
            # Fundamentals
            "trailingPE",
            "priceToBook",
            "dividendYield",
            "returnOnEquity",
            # Valuation
            "net_cash_ratio",
            "cash_neutral_per",
            # Meta
            "Date",
        ]
        # Keep only columns that exist in the dataframe
        existing_cols = [col for col in preferred_order if col in df.columns]
        # Add any remaining columns not in preferred_order
        remaining_cols = [col for col in df.columns if col not in preferred_order]
        df = df[existing_cols + remaining_cols]

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
                    l.Code,
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
            code: Stock code.
            days: Number of days of history.

        Returns:
            DataFrame with historical scores.
        """
        with self._get_analysis_connection() as conn:
            query = """
                SELECT
                    Date,
                    Code,
                    composite_score,
                    composite_score_rank,
                    hl_ratio_rank,
                    rsp_rank
                FROM integrated_scores
                WHERE Code = ?
                ORDER BY Date DESC
                LIMIT ?
            """
            df = pd.read_sql(query, conn, params=[code, days])

        return df
