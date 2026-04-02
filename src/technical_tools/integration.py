"""Integration with existing analysis results."""

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


def load_existing_analysis(
    ticker: str,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load existing analysis results from database.

    Args:
        ticker: Stock code (4-digit or 5-digit)
        db_path: Path to analysis_results.db.
                If None, uses config settings.

    Returns:
        Dictionary with analysis results:
        - minervini: Latest Minervini analysis or None
        - relative_strength: Latest RSP analysis or None
    """
    if db_path is None:
        from market_pipeline.config import get_settings

        db_path = Path(get_settings().paths.analysis_db)
    else:
        db_path = Path(db_path)

    result: dict[str, Any] = {
        "minervini": None,
        "relative_strength": None,
    }

    if not db_path.exists():
        return result

    # Normalize ticker to 4-digit
    ticker_4digit = ticker[:4] if len(ticker) >= 4 else ticker

    with sqlite3.connect(db_path) as conn:
        # Load Minervini analysis
        try:
            minervini_df = pd.read_sql_query(
                """
                SELECT * FROM minervini
                WHERE Code = ? OR Code = ?
                ORDER BY Date DESC
                LIMIT 1
                """,
                conn,
                params=[ticker_4digit, f"{ticker_4digit}0"],
            )
            if not minervini_df.empty:
                result["minervini"] = minervini_df.to_dict("records")[0]
        except (sqlite3.OperationalError, pd.errors.DatabaseError):
            # Table might not exist
            pass

        # Load Relative Strength analysis
        try:
            rs_df = pd.read_sql_query(
                """
                SELECT * FROM relative_strength
                WHERE Code = ? OR Code = ?
                ORDER BY Date DESC
                LIMIT 1
                """,
                conn,
                params=[ticker_4digit, f"{ticker_4digit}0"],
            )
            if not rs_df.empty:
                result["relative_strength"] = rs_df.to_dict("records")[0]
        except (sqlite3.OperationalError, pd.errors.DatabaseError):
            # Table might not exist
            pass

    return result
