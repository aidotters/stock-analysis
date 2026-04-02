import sqlite3
import datetime
import logging
import os
import pandas as pd
from typing import Optional, Dict, Union

# --- Constants ---
DATA_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/data"
LOGS_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/logs"
RESULTS_DB_PATH = os.path.join(DATA_DIR, "analysis_results.db")


def setup_logging():
    """Setup logging configuration"""
    log_filename = os.path.join(
        LOGS_DIR,
        f"integrated_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return logger


def get_comprehensive_analysis(
    date: str, code: Optional[str] = None, db_path: str = RESULTS_DB_PATH
) -> pd.DataFrame:
    """
    Get comprehensive analysis results for a specific date and optionally a specific code.
    Combines HL ratio, Minervini criteria, and Relative Strength data using optimized queries.

    Args:
        date: Analysis date in YYYY-MM-DD format
        code: Optional stock code filter
        db_path: Path to the analysis results database

    Returns:
        DataFrame with combined analysis results
    """
    logger = logging.getLogger(__name__)
    logger.info(
        f"Getting comprehensive analysis for date: {date}"
        + (f", code: {code}" if code else " (all codes)")
    )

    try:
        with sqlite3.connect(db_path) as conn:
            # Enable optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")

            # Optimized query using exact date matching and avoiding substr()
            base_query = """
            SELECT 
                h.Date,
                h.Code,
                h.HlRatio,
                h.MedianRatio,
                h.Weeks as hl_weeks,
                COALESCE(m.Close, 0) as minervini_close,
                COALESCE(m.Sma50, 0) as Sma50,
                COALESCE(m.Sma150, 0) as Sma150, 
                COALESCE(m.Sma200, 0) as Sma200,
                COALESCE(m.Type_1, 0) as minervini_type_1,
                COALESCE(m.Type_2, 0) as minervini_type_2,
                COALESCE(m.Type_3, 0) as minervini_type_3,
                COALESCE(m.Type_4, 0) as minervini_type_4,
                COALESCE(m.Type_5, 0) as minervini_type_5,
                COALESCE(m.Type_6, 0) as minervini_type_6,
                COALESCE(m.Type_7, 0) as minervini_type_7,
                COALESCE(m.Type_8, 0) as minervini_type_8,
                COALESCE(r.RelativeStrengthPercentage, 0) as RelativeStrengthPercentage,
                COALESCE(r.RelativeStrengthIndex, 0) as RelativeStrengthIndex
            FROM hl_ratio h
            LEFT JOIN minervini m ON m.Date = h.Date AND h.Code = m.Code
            LEFT JOIN relative_strength r ON r.Date = h.Date AND h.Code = r.Code
            WHERE h.Date = ?
            """

            params = [date]
            if code:
                base_query += " AND h.Code = ?"
                params.append(code)

            base_query += " ORDER BY h.HlRatio DESC"

            # Use pandas read_sql with optimized connection
            df = pd.read_sql(base_query, conn, params=params)

        if df.empty:
            logger.warning(f"No comprehensive analysis data found for date: {date}")
            return pd.DataFrame()

        # Calculate composite scores using vectorized operations
        df = _calculate_composite_scores(df)

        logger.info(f"Retrieved comprehensive analysis for {len(df)} stocks")
        return df

    except sqlite3.Error as e:
        logger.error(f"Database error retrieving comprehensive analysis: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in comprehensive analysis: {e}")
        raise


def get_multi_date_analysis(
    start_date: str, end_date: str, code: str, db_path: str = RESULTS_DB_PATH
) -> pd.DataFrame:
    """
    Get analysis results for a specific stock across multiple dates.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        code: Stock code
        db_path: Path to the analysis results database

    Returns:
        DataFrame with time series analysis results
    """
    logger = logging.getLogger(__name__)
    logger.info(
        f"Getting multi-date analysis for {code} from {start_date} to {end_date}"
    )

    try:
        with sqlite3.connect(db_path) as conn:
            query = """
            SELECT 
                h.Date,
                h.Code,
                h.HlRatio,
                MAX(m.Close) as minervini_close,
                MAX(m.Sma50) as Sma50,
                MAX(m.Sma150) as Sma150,
                MAX(m.Sma200) as Sma200,
                MAX(m.Type_1) + MAX(m.Type_2) + MAX(m.Type_3) + MAX(m.Type_4) + MAX(m.Type_5) + 
                MAX(m.Type_6) + MAX(m.Type_7) + MAX(m.Type_8) as minervini_score,
                MAX(r.RelativeStrengthPercentage) as RelativeStrengthPercentage,
                MAX(r.RelativeStrengthIndex) as RelativeStrengthIndex
            FROM hl_ratio h
            LEFT JOIN minervini m ON substr(m.Date, 1, 10) = h.Date AND h.Code = m.Code
            LEFT JOIN relative_strength r ON substr(r.Date, 1, 10) = h.Date AND h.Code = r.Code
            WHERE h.Code = ? AND h.Date BETWEEN ? AND ?
            GROUP BY h.Date, h.Code, h.HlRatio
            ORDER BY h.Date ASC
            """

            df = pd.read_sql(query, conn, params=[code, start_date, end_date])

        if df.empty:
            logger.warning(f"No multi-date analysis data found for {code}")
            return pd.DataFrame()

        df["Date"] = pd.to_datetime(df["Date"])
        logger.info(f"Retrieved {len(df)} records for {code}")
        return df

    except sqlite3.Error as e:
        logger.error(f"Database error retrieving multi-date analysis: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in multi-date analysis: {e}")
        raise


def get_top_stocks_by_criteria(
    date: str,
    criteria: str = "composite",
    limit: int = 50,
    include_median_ratio: bool = False,
    db_path: str = RESULTS_DB_PATH,
) -> pd.DataFrame:
    """
    Get top stocks based on various criteria.

    Args:
        date: Analysis date in YYYY-MM-DD format
        criteria: Ranking criteria ('hl_ratio', 'rsi', 'minervini', 'composite')
        limit: Number of top stocks to return
        include_median_ratio: Whether to include MedianRatio in hl_ratio sorting
        db_path: Path to the analysis results database

    Returns:
        DataFrame with top stocks ranked by criteria
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Getting top {limit} stocks by {criteria} for date: {date}")

    df = get_comprehensive_analysis(date, db_path=db_path)

    if df.empty:
        logger.warning(f"No data available for ranking on {date}")
        return pd.DataFrame()

    # Apply ranking based on criteria
    if criteria == "hl_ratio":
        if include_median_ratio and "MedianRatio" in df.columns:
            df_ranked = df.sort_values(
                ["HlRatio", "MedianRatio"], ascending=[False, True]
            )
        else:
            df_ranked = df.sort_values("HlRatio", ascending=False)
    elif criteria == "rsi":
        df_ranked = df.sort_values(
            "RelativeStrengthIndex", ascending=False, na_position="last"
        )
    elif criteria == "minervini":
        df_ranked = df.sort_values(
            "minervini_score", ascending=False, na_position="last"
        )
    elif criteria == "composite":
        df_ranked = df.sort_values(
            "composite_score", ascending=False, na_position="last"
        )
    else:
        logger.error(f"Unknown criteria: {criteria}")
        raise ValueError(f"Unknown criteria: {criteria}")

    result = df_ranked.head(limit)
    logger.info(f"Ranked {len(result)} stocks by {criteria}")
    return result


def get_stocks_meeting_criteria(
    date: str,
    hl_ratio_min: float = 80.0,
    rsi_min: float = 70.0,
    minervini_min: int = 5,
    db_path: str = RESULTS_DB_PATH,
) -> pd.DataFrame:
    """
    Get stocks that meet minimum criteria across all indicators.

    Args:
        date: Analysis date in YYYY-MM-DD format
        hl_ratio_min: Minimum HL ratio threshold
        rsi_min: Minimum RSI threshold
        minervini_min: Minimum number of Minervini criteria met
        db_path: Path to the analysis results database

    Returns:
        DataFrame with stocks meeting all criteria
    """
    logger = logging.getLogger(__name__)
    logger.info(
        f"Finding stocks meeting criteria on {date}: HL>{hl_ratio_min}, RSI>{rsi_min}, Minervini>{minervini_min}"
    )

    df = get_comprehensive_analysis(date, db_path=db_path)

    if df.empty:
        logger.warning(f"No data available for filtering on {date}")
        return pd.DataFrame()

    # Apply filters
    filtered_df = df[
        (df["HlRatio"] >= hl_ratio_min)
        & (df["RelativeStrengthIndex"] >= rsi_min)
        & (df["minervini_score"] >= minervini_min)
    ].copy()

    # Sort by composite score
    filtered_df = filtered_df.sort_values(
        "composite_score", ascending=False, na_position="last"
    )

    logger.info(f"Found {len(filtered_df)} stocks meeting all criteria")
    return filtered_df


def _calculate_composite_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate composite scores combining all analysis indicators using optimized vectorized operations.

    Args:
        df: DataFrame with individual analysis results

    Returns:
        DataFrame with added composite score columns
    """
    if df.empty:
        return df

    # Create a copy to avoid modifying original
    result_df = df.copy()

    # Use vectorized operations for Minervini score calculation
    minervini_cols = [f"minervini_type_{i}" for i in range(1, 9)]

    # Ensure all minervini columns exist and convert to numeric
    for col in minervini_cols:
        if col not in result_df.columns:
            result_df[col] = 0
        else:
            result_df[col] = pd.to_numeric(result_df[col], errors="coerce").fillna(0)

    # Vectorized sum calculation
    result_df["minervini_score"] = result_df[minervini_cols].sum(axis=1)

    # Vectorized composite score calculation (avoid intermediate columns)
    # Convert to numeric and fill NaN with 0 for reliable calculations
    hl_ratio = pd.to_numeric(result_df["HlRatio"], errors="coerce").fillna(0)
    rsi = pd.to_numeric(result_df["RelativeStrengthIndex"], errors="coerce").fillna(0)
    minervini_norm = (result_df["minervini_score"] / 8.0) * 100

    # Single vectorized composite score calculation
    # HL ratio: 40%, RSI: 40%, Minervini: 20%
    result_df["composite_score"] = hl_ratio * 0.4 + rsi * 0.4 + minervini_norm * 0.2

    return result_df


def create_analysis_summary(
    date: str, db_path: str = RESULTS_DB_PATH
) -> Dict[str, Union[int, float]]:
    """
    Create a summary of analysis results for a given date.

    Args:
        date: Analysis date in YYYY-MM-DD format
        db_path: Path to the analysis results database

    Returns:
        Dictionary with summary statistics
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating analysis summary for date: {date}")

    df = get_comprehensive_analysis(date, db_path=db_path)

    if df.empty:
        logger.warning(f"No data available for summary on {date}")
        return {}

    summary = {
        "total_stocks": len(df),
        "avg_hl_ratio": df["HlRatio"].mean(),
        "avg_rsi": df["RelativeStrengthIndex"].mean(),
        "avg_minervini_score": df["minervini_score"].mean(),
        "avg_composite_score": df["composite_score"].mean(),
        "high_hl_ratio_count": len(df[df["HlRatio"] >= 80]),
        "high_rsi_count": len(df[df["RelativeStrengthIndex"] >= 70]),
        "strong_minervini_count": len(df[df["minervini_score"] >= 5]),
        "strong_composite_count": len(df[df["composite_score"] >= 70]),
    }

    logger.info(f"Analysis summary completed for {summary['total_stocks']} stocks")
    return summary


def check_database_coverage(db_path: str = RESULTS_DB_PATH) -> Dict[str, int]:
    """
    Check the coverage of data across different analysis tables using optimized batch queries.

    Args:
        db_path: Path to the analysis results database

    Returns:
        Dictionary with coverage information
    """
    logger = logging.getLogger(__name__)
    logger.info("Checking database coverage across analysis tables")

    try:
        with sqlite3.connect(db_path) as conn:
            # Enable optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Single optimized query to get all coverage info at once
            coverage_query = """
            WITH coverage_stats AS (
                SELECT 
                    'hl_ratio' as table_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT Date) as date_count,
                    COUNT(DISTINCT Code) as code_count
                FROM hl_ratio
                UNION ALL
                SELECT 
                    'minervini' as table_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT Date) as date_count,
                    COUNT(DISTINCT Code) as code_count
                FROM minervini
                UNION ALL
                SELECT 
                    'relative_strength' as table_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT Date) as date_count,
                    COUNT(DISTINCT Code) as code_count
                FROM relative_strength
            )
            SELECT * FROM coverage_stats
            """

            results = pd.read_sql(coverage_query, conn)

        # Convert results to the expected format
        coverage = {}
        for _, row in results.iterrows():
            table = row["table_name"]
            coverage[f"{table}_records"] = int(row["record_count"])
            coverage[f"{table}_dates"] = int(row["date_count"])
            coverage[f"{table}_codes"] = int(row["code_count"])

        logger.info("Database coverage check completed using optimized query")
        return coverage

    except sqlite3.Error as e:
        logger.error(f"Database error checking coverage: {e}")
        raise
    except Exception as e:
        logger.error(f"Error checking database coverage: {e}")
        raise
