# -*- coding: utf-8 -*-
"""
OPTIMIZED data analysis script for fetching and combining data from multiple
SQLite databases with improved performance and error handling.

The script performs the following steps:
1. Sets up paths to the various databases.
2. Fetches the latest available analysis date.
3. Retrieves comprehensive analysis data for the latest date.
4. Retrieves and pivots chart classification data.
5. Retrieves fundamental data from J-Quants Statements API (calculated_fundamentals).
6. Merges these data sources into a single DataFrame using optimized operations.
7. Outputs the final combined DataFrame to Excel.
"""

import sys
import os
import sqlite3
import pandas as pd
import warnings
import logging
from datetime import datetime
from typing import List

# Add project root to sys.path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from market_pipeline.analysis.integrated_analysis import get_comprehensive_analysis
from market_pipeline.analysis.integrated_scores_repository import (
    IntegratedScoresRepository,
)

# Ignore warnings for cleaner output
warnings.filterwarnings("ignore")

# --- Constants and Configuration ---

# Determine the project root directory based on the script's location
# The script is in src/market_pipeline/analysis, so the root is three levels up.
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Database paths
RESULTS_DB_PATH = os.path.join(DATA_DIR, "analysis_results.db")
MASTER_DB_PATH = os.path.join(DATA_DIR, "master.db")
JQUANTS_DB_PATH = os.path.join(DATA_DIR, "jquants.db")
STATEMENTS_DB_PATH = os.path.join(DATA_DIR, "statements.db")


# --- Setup Functions ---


def setup_logging() -> logging.Logger:
    """Setup logging configuration for optimized processing."""
    log_filename = os.path.join(
        PROJECT_ROOT,
        "logs",
        f"integrated_analysis2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    logger = logging.getLogger(__name__)
    logger.info(
        f"Optimized integrated analysis logging initialized. Log file: {log_filename}"
    )
    return logger


# --- Data Fetching Functions ---


def get_available_dates(db_path: str, logger: logging.Logger) -> List[str]:
    """Gets all available analysis dates from the database with optimized query."""
    try:
        with sqlite3.connect(db_path) as conn:
            # Enable optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            dates_query = (
                "SELECT DISTINCT Date FROM hl_ratio ORDER BY Date DESC LIMIT 10"
            )
            dates_df = pd.read_sql(dates_query, conn)
            dates = dates_df["Date"].tolist()

        logger.info(f"Retrieved {len(dates)} available dates")
        return dates
    except Exception as e:
        logger.error(f"Error getting available dates: {e}")
        return []


def get_chart_classification_data(db_path: str, logger: logging.Logger) -> pd.DataFrame:
    """Fetches the latest chart classification results with optimized query."""
    try:
        with sqlite3.connect(db_path) as conn:
            # Enable optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Check if table exists first
            table_check = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='classification_results'"
            ).fetchone()

            if not table_check:
                logger.warning("classification_results table not found in database")
                return pd.DataFrame()

            query = """
            SELECT *
            FROM classification_results
            WHERE date = (SELECT MAX(date) FROM classification_results)
            """
            df = pd.read_sql(query, conn)

        logger.info(f"Retrieved {len(df)} chart classification records")
        return df
    except Exception as e:
        logger.error(f"Error fetching chart classification data: {e}")
        return pd.DataFrame()


def pivot_chart_classification_data(
    df: pd.DataFrame, logger: logging.Logger
) -> pd.DataFrame:
    """Pivots the chart classification data to have one row per ticker using optimized operations."""
    if df is None or df.empty:
        logger.info("No chart classification data to pivot")
        return pd.DataFrame()

    try:
        # Optimized pivot using vectorized operations
        pivot_df = df.pivot_table(
            index=["date", "ticker"],
            columns="window",
            values=["pattern_label", "score"],
            aggfunc="first",
        )
        # Efficient column flattening
        pivot_df.columns = [f"{col[0]}_{col[1]}" for col in pivot_df.columns]
        pivot_df = pivot_df.reset_index()

        # Batch rename operation
        pivot_df.rename(columns={"date": "Date", "ticker": "Code"}, inplace=True)

        fixed_columns = ["Date", "Code"]
        selected_columns = [col for col in pivot_df.columns if col not in fixed_columns]
        selected_columns = sorted(
            selected_columns, key=lambda x: int(x.split("_")[-1]), reverse=True
        )
        selected_columns = sorted(
            selected_columns, key=lambda x: x.split("_")[0], reverse=False
        )
        final_columns = fixed_columns + selected_columns
        pivot_df = pivot_df[final_columns]

        logger.info(f"Pivoted chart classification data: {len(pivot_df)} records")
        return pd.DataFrame(pivot_df)

    except Exception as e:
        logger.error(f"Error pivoting chart classification data: {e}")
        return pd.DataFrame()


def get_fundamentals_data(db_path: str, logger: logging.Logger) -> pd.DataFrame:
    """Fetches calculated fundamentals data from statements database.

    Data comes from J-Quants Statements API with calculated metrics like PER, PBR, ROE.
    Column names are mapped for backward compatibility with yfinance-based analysis.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            # Enable optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Check if table exists first
            table_check = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='calculated_fundamentals'"
            ).fetchone()

            if not table_check:
                logger.warning(
                    "calculated_fundamentals table not found in statements database"
                )
                return pd.DataFrame()

            # Query with column aliases for backward compatibility
            query = """
            SELECT
                code as Code,
                company_name as longName,
                sector_33 as sector,
                sector_17,
                market_segment,
                market_cap as marketCap,
                per as trailingPE,
                forward_per as forwardPE,
                pbr as priceToBook,
                dividend_yield as dividendYield,
                roe as returnOnEquity,
                roa as returnOnAssets,
                equity_ratio,
                operating_margin as operatingMargins,
                profit_margin as profitMargins,
                eps,
                bps,
                dps,
                total_assets,
                equity,
                operating_cf,
                free_cash_flow as freeCashflow,
                net_sales,
                operating_profit,
                ordinary_profit,
                profit,
                payout_ratio,
                reference_price,
                reference_date,
                latest_period,
                latest_fiscal_year_end,
                last_updated
            FROM calculated_fundamentals
            """
            df = pd.read_sql(query, conn)

        logger.info(f"Retrieved {len(df)} fundamentals records")
        return df

    except Exception as e:
        logger.error(f"Error fetching fundamentals data: {e}")
        return pd.DataFrame()


# --- Main Analysis Logic ---


def main(target_date=None, output_csv=False, output_excel=False):
    """Main function to run the optimized data analysis pipeline.

    Args:
        target_date: Optional target date in 'YYYY-MM-DD' format. If None, uses latest date.
        output_csv: If True, output results to CSV file.
        output_excel: If True, output results to Excel file.
    """
    logger = setup_logging()
    logger.info("Starting OPTIMIZED integrated data analysis...")

    try:
        # Get available dates
        available_dates = get_available_dates(RESULTS_DB_PATH, logger)
        if not available_dates:
            logger.error("No available analysis dates found. Exiting.")
            return

        # Use target_date if provided and valid, otherwise use latest
        if target_date and target_date in available_dates:
            analysis_date = target_date
            logger.info(f"Using specified analysis date: {analysis_date}")
        else:
            analysis_date = available_dates[0]
            if target_date:
                logger.warning(
                    f"Specified date {target_date} not found in available dates. Using latest: {analysis_date}"
                )
            else:
                logger.info(f"Using latest analysis date: {analysis_date}")

        # 1. Get comprehensive analysis data (this is already optimized)
        logger.info("Fetching comprehensive analysis data...")
        comprehensive_df = get_comprehensive_analysis(analysis_date)
        if comprehensive_df.empty:
            logger.error("Could not retrieve comprehensive analysis data.")
            return
        logger.info(
            f"Retrieved {len(comprehensive_df)} records in comprehensive analysis"
        )

        # 2. Get and process chart classification data
        logger.info("Fetching and processing chart classification data...")
        chart_df = get_chart_classification_data(RESULTS_DB_PATH, logger)
        pivot_df = pivot_chart_classification_data(chart_df, logger)

        # 3. Get fundamentals data (from J-Quants Statements)
        logger.info("Fetching fundamentals data...")
        fundamentals_df = get_fundamentals_data(STATEMENTS_DB_PATH, logger)

        # 4. Optimized merge operations
        logger.info("Performing optimized data merging...")

        # Start with chart classification data (pivot_df)
        all_df = pivot_df.copy()
        logger.info(f"Started with chart classification data: {len(all_df)} rows")

        # Merge with comprehensive analysis data
        if not comprehensive_df.empty:
            all_df = pd.merge(all_df, comprehensive_df, on="Code", how="left")
            logger.info(f"Merged with comprehensive analysis data: {len(all_df)} rows")

        # Merge with fundamentals data if available
        if not fundamentals_df.empty:
            all_df = pd.merge(all_df, fundamentals_df, on="Code", how="left")
            logger.info(f"Merged with fundamentals data: {len(all_df)} rows")

        # 5. Optimized column reordering and sorting
        logger.info("Optimizing final dataframe structure...")

        # Priority columns for reordering (most important first)
        priority_columns = [
            "Code",
            "longName",
            "sector",
            "sector_17",
            "market_segment",
            "marketCap",
            "composite_score",
            "HlRatio",
            "RelativeStrengthIndex",
            "minervini_score",
            "trailingPE",
            "priceToBook",
            "dividendYield",
            "returnOnEquity",
            "returnOnAssets",
        ]

        # Additional columns in logical order
        additional_columns = [
            col for col in all_df.columns if col not in priority_columns
        ]

        # Combine and filter for existing columns
        final_columns = priority_columns + additional_columns
        # existing_columns = [col for col in final_columns if col in all_df.columns]

        # # Add any remaining columns not in our predefined list
        # remaining_columns = [col for col in all_df.columns if col not in existing_columns]
        # final_column_order = existing_columns + remaining_columns

        # Reorder columns efficiently
        all_df = all_df[final_columns]

        # Optimized sorting by composite score
        if "composite_score" in all_df.columns:
            all_df["composite_score"] = pd.to_numeric(
                all_df["composite_score"], errors="coerce"
            )
            all_df = all_df.sort_values(
                by=["composite_score"], ascending=False, na_position="last"
            ).reset_index(drop=True)
            logger.info("Sorted by composite score (descending)")

        logger.info("Displaying top 10 results:")
        with pd.option_context("display.max_rows", 10, "display.max_columns", 15):
            logger.info(f"\n{all_df}")

        # --- Save to Database ---
        logger.info("Saving scores to database...")
        try:
            repository = IntegratedScoresRepository()
            save_count = repository.save_scores(all_df, analysis_date)
            logger.info(
                f"Successfully saved {save_count} scores to integrated_scores table"
            )
        except Exception as e:
            logger.error(f"Error saving to database: {e}")

        # --- Output Directory Setup ---
        output_dir = os.path.join(PROJECT_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)

        # --- CSV Output (optional) ---
        if output_csv:
            csv_filename = f"integrated_analysis_{analysis_date.replace('-', '')}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            try:
                all_df.to_csv(csv_path, index=True, encoding="utf-8-sig")
                logger.info(f"Successfully saved CSV to: {csv_path}")
            except Exception as e:
                logger.error(f"Error saving to CSV: {e}")

        # --- Excel Output (optional) ---
        if output_excel:
            output_filename = (
                f"integrated_analysis_{analysis_date.replace('-', '')}.xlsx"
            )
            output_path = os.path.join(output_dir, output_filename)
            try:
                all_df.to_excel(output_path, index=True, engine="openpyxl")
                logger.info(f"Successfully saved Excel to: {output_path}")
            except Exception as e:
                logger.error(f"Error saving to Excel: {e}")

        logger.info(
            f"Final output contains {len(all_df)} rows and {len(all_df.columns)} columns"
        )
        logger.info("OPTIMIZED analysis script finished successfully.")

    except Exception as e:
        logger.error(f"Error in main analysis pipeline: {e}")
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run optimized integrated analysis")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYY-MM-DD format (default: latest from database)",
    )
    parser.add_argument(
        "--output-csv", action="store_true", help="Output results to CSV file"
    )
    parser.add_argument(
        "--output-excel", action="store_true", help="Output results to Excel file"
    )
    args = parser.parse_args()

    main(
        target_date=args.date,
        output_csv=args.output_csv,
        output_excel=args.output_excel,
    )
