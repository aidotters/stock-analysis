"""
Repository for integrated_scores table operations.

Handles CRUD operations, ranking logic, and UPSERT processing
for the integrated analysis scores.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from market_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class IntegratedScoresRepository:
    """Repository for integrated_scores table operations."""

    # Valid metrics for get_rank_changes
    VALID_METRICS = frozenset({"composite_score", "hl_ratio", "rsp"})

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize the repository.

        Args:
            db_path: Path to the analysis database. If None, uses settings default.
        """
        settings = get_settings()
        self.db_path = db_path or settings.paths.analysis_db

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with optimized PRAGMA settings."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        return conn

    def _ensure_table_exists(self) -> None:
        """Create the integrated_scores table if it doesn't exist."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS integrated_scores (
                    Date TEXT NOT NULL,
                    Code TEXT NOT NULL,
                    composite_score REAL,
                    composite_score_rank INTEGER,
                    hl_ratio_rank INTEGER,
                    rsp_rank INTEGER,
                    created_at TEXT DEFAULT (datetime('now', 'localtime')),
                    PRIMARY KEY (Date, Code)
                )
            """
            )

            # Create indexes for performance
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_integrated_scores_date
                ON integrated_scores (Date)
            """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_integrated_scores_code
                ON integrated_scores (Code)
            """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_integrated_scores_composite_rank
                ON integrated_scores (Date, composite_score_rank)
            """
            )
            conn.commit()

    def save_scores(self, df: pd.DataFrame, date: str) -> int:
        """Save scores and ranks to the database.

        Calculates ranks from the input data and performs UPSERT operation.

        Args:
            df: DataFrame with at least Code, composite_score, HlRatio,
                and RelativeStrengthPercentage columns.
            date: Analysis date in YYYY-MM-DD format.

        Returns:
            Number of records saved.
        """
        self._ensure_table_exists()

        if df.empty:
            logger.warning(f"Empty DataFrame provided for date {date}")
            return 0

        # Prepare data with rankings
        save_df = df[["Code"]].copy()
        save_df["Date"] = date

        # Copy scores with proper column names
        if "composite_score" in df.columns:
            save_df["composite_score"] = df["composite_score"]
        else:
            save_df["composite_score"] = None

        # Calculate ranks using min method (ties get same rank)
        if "composite_score" in df.columns:
            save_df["composite_score_rank"] = (
                df["composite_score"]
                .rank(method="min", ascending=False, na_option="keep")
                .astype("Int64")
            )
        else:
            save_df["composite_score_rank"] = None

        if "HlRatio" in df.columns:
            save_df["hl_ratio_rank"] = (
                df["HlRatio"]
                .rank(method="min", ascending=False, na_option="keep")
                .astype("Int64")
            )
        else:
            save_df["hl_ratio_rank"] = None

        if "RelativeStrengthPercentage" in df.columns:
            save_df["rsp_rank"] = (
                df["RelativeStrengthPercentage"]
                .rank(method="min", ascending=False, na_option="keep")
                .astype("Int64")
            )
        else:
            save_df["rsp_rank"] = None

        # Perform UPSERT using INSERT OR REPLACE
        with self._get_connection() as conn:
            records = save_df.to_dict("records")

            conn.executemany(
                """
                INSERT OR REPLACE INTO integrated_scores
                (Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank)
                VALUES (:Date, :Code, :composite_score, :composite_score_rank,
                        :hl_ratio_rank, :rsp_rank)
            """,
                records,
            )
            conn.commit()

        logger.info(f"Saved {len(records)} scores for date {date}")
        return len(records)

    def get_scores(self, date: Optional[str] = None) -> pd.DataFrame:
        """Get scores for a specific date.

        Args:
            date: Analysis date in YYYY-MM-DD format. If None, returns latest date.

        Returns:
            DataFrame with scores for the specified date.
        """
        self._ensure_table_exists()

        with self._get_connection() as conn:
            if date is None:
                # Get latest date
                date = self.get_latest_date()
                if date is None:
                    return pd.DataFrame()

            query = """
                SELECT Date, Code, composite_score, composite_score_rank,
                       hl_ratio_rank, rsp_rank, created_at
                FROM integrated_scores
                WHERE Date = ?
                ORDER BY composite_score_rank ASC
            """
            df = pd.read_sql(query, conn, params=[date])

        return df

    def get_history(self, code: str, days: int = 30) -> pd.DataFrame:
        """Get historical scores for a specific stock.

        Args:
            code: Stock code.
            days: Number of days of history to retrieve.

        Returns:
            DataFrame with historical scores, ordered by date descending.
        """
        self._ensure_table_exists()

        with self._get_connection() as conn:
            query = """
                SELECT Date, Code, composite_score, composite_score_rank,
                       hl_ratio_rank, rsp_rank
                FROM integrated_scores
                WHERE Code = ?
                ORDER BY Date DESC
                LIMIT ?
            """
            df = pd.read_sql(query, conn, params=[code, days])

        return df

    def get_latest_date(self) -> Optional[str]:
        """Get the latest date in the database.

        Returns:
            Latest date string or None if no data exists.
        """
        self._ensure_table_exists()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(Date) FROM integrated_scores")
            result = cursor.fetchone()

        return result[0] if result and result[0] else None

    def get_rank_changes(
        self,
        metric: str = "composite_score",
        days: int = 7,
        direction: str = "up",
        min_change: int = 1,
        limit: int = 50,
    ) -> pd.DataFrame:
        """Get stocks with significant rank changes.

        Args:
            metric: Rank metric to analyze (composite_score, hl_ratio, rsp).
            days: Number of days to compare.
            direction: 'up' for improved ranks, 'down' for worsened, 'both' for all.
            min_change: Minimum rank change to include.
            limit: Maximum number of results.

        Returns:
            DataFrame with rank changes.

        Raises:
            ValueError: If metric is not one of VALID_METRICS.
        """
        if metric not in self.VALID_METRICS:
            raise ValueError(
                f"Invalid metric '{metric}'. Must be one of: {sorted(self.VALID_METRICS)}"
            )

        self._ensure_table_exists()

        rank_column = f"{metric}_rank"

        with self._get_connection() as conn:
            # Get latest date
            latest_date = self.get_latest_date()
            if not latest_date:
                return pd.DataFrame()

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
