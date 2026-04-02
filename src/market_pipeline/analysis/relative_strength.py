import numpy as np
import pandas as pd
import sqlite3
import datetime
import logging
import os
import sys
from datetime import timedelta
from typing import List, Dict

# Add project root to sys.path for imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

from market_pipeline.utils.parallel_processor import (
    BatchDatabaseProcessor,
    measure_performance,
)  # noqa: E402

# --- Constants ---
JQUANTS_DB_PATH = "/Users/tak/Markets/Stocks/Stock-Analysis/data/jquants.db"
DATA_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/data"
LOGS_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/logs"
OUTPUT_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/output"
RESULTS_DB_PATH = os.path.join(DATA_DIR, "analysis_results.db")


def setup_logging():
    """Setup logging configuration"""
    log_filename = os.path.join(
        LOGS_DIR,
        f"relative_strength_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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


def relative_strength_percentage_vectorized(
    df: pd.DataFrame, period: int = 200
) -> pd.DataFrame:
    """
    Calculate Relative Strength Percentage using vectorized operations.

    Args:
        df: DataFrame with Date index and AdjustmentClose column
        period: Period for RSP calculation

    Returns:
        DataFrame with RSP values
    """
    if len(df) < period:
        df["RelativeStrengthPercentage"] = np.nan
        return df

    # Ensure numeric and handle missing values
    close_prices = pd.to_numeric(df["AdjustmentClose"], errors="coerce").ffill()

    # Calculate indices for quarters
    q1_idx = int(period * 3 / 4)
    q2_idx = int(period * 2 / 4)
    q3_idx = int(period * 1 / 4)

    # Use rolling window for efficient calculation
    def calculate_quarter_returns(close_series, start_idx, end_idx):
        """Calculate returns for a quarter period"""
        start_prices = close_series.shift(start_idx)
        end_prices = close_series.shift(end_idx) if end_idx > 0 else close_series
        return (end_prices - start_prices) / start_prices

    # Calculate quarterly returns
    q1_returns = calculate_quarter_returns(close_prices, period, q1_idx)
    q2_returns = calculate_quarter_returns(close_prices, q1_idx, q2_idx)
    q3_returns = calculate_quarter_returns(close_prices, q2_idx, q3_idx)
    q4_returns = calculate_quarter_returns(close_prices, q3_idx, 0)

    # Calculate RSP with weighted average
    rsp = ((q1_returns + q2_returns + q3_returns) * 0.2 + q4_returns * 0.4) * 100

    # Set first 'period' values to NaN
    rsp.iloc[:period] = np.nan

    df["RelativeStrengthPercentage"] = rsp
    return df


def process_stock_batch_rsp(
    stock_codes: List[str], price_data: pd.DataFrame, period: int = 200
) -> Dict[str, pd.DataFrame]:
    """
    Process a batch of stocks for RSP calculation.

    Args:
        stock_codes: List of stock codes to process
        price_data: DataFrame with all price data
        period: Period for RSP calculation

    Returns:
        Dictionary mapping stock codes to their RSP DataFrames
    """
    results = {}

    for code in stock_codes:
        stock_data = price_data[price_data["Code"] == code].copy()

        if len(stock_data) < period:
            continue

        stock_data = stock_data.set_index("Date").sort_index()
        stock_data = relative_strength_percentage_vectorized(stock_data, period)

        # Keep only Code and RSP columns
        result_df = stock_data[["Code", "RelativeStrengthPercentage"]].copy()
        result_df.index = result_df.index.date  # Convert to date for storage

        results[code] = result_df

    return results


def init_results_db(db_path):
    """Initialize results database with indexes for performance"""
    logger = logging.getLogger(__name__)
    logger.info(f"Initializing relative strength results database at {db_path}")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS relative_strength (
            Date TEXT NOT NULL,
            Code TEXT NOT NULL,
            RelativeStrengthPercentage REAL,
            RelativeStrengthIndex REAL,
            PRIMARY KEY (Date, Code)
        )
        """)
        conn.commit()

    # Create indexes for performance
    db_processor = BatchDatabaseProcessor(db_path)
    db_processor.create_indexes(
        [
            {"name": "idx_rs_date", "table": "relative_strength", "columns": ["Date"]},
            {"name": "idx_rs_code", "table": "relative_strength", "columns": ["Code"]},
            {
                "name": "idx_rs_date_code",
                "table": "relative_strength",
                "columns": ["Date", "Code"],
                "unique": True,
            },
        ]
    )

    logger.info("Relative strength results database initialized successfully")


@measure_performance
def init_rsp_db(
    db_path=JQUANTS_DB_PATH, result_db_path=RESULTS_DB_PATH, n_workers=None
):
    """Initialize relative strength database with all stock data using parallel processing"""
    logger = setup_logging()

    # Get all tickers from jquants database
    try:
        db_processor = BatchDatabaseProcessor(db_path)
        code_df = db_processor.batch_fetch(
            "SELECT DISTINCT Code FROM daily_quotes ORDER BY Code", as_dataframe=True
        )
        code_list = code_df["Code"].tolist()
    except sqlite3.Error as e:
        logger.error(f"Error getting code list: {e}")
        raise

    logger.info(
        f"Initializing relative strength analysis for {len(code_list)} stocks using parallel processing"
    )

    # Initialize results database
    init_results_db(result_db_path)

    # Load all price data at once
    try:
        price_data = db_processor.batch_fetch(
            """
            SELECT Code, Date, AdjustmentClose
            FROM daily_quotes
            ORDER BY Code, Date
            """,
            as_dataframe=True,
        )
        price_data["Date"] = pd.to_datetime(price_data["Date"])
    except sqlite3.Error as e:
        logger.error(f"Error loading price data: {e}")
        raise

    # Replace empty strings with NaN
    price_data["AdjustmentClose"] = price_data["AdjustmentClose"].replace("", np.nan)

    from functools import partial

    process_func = partial(process_stock_batch_rsp, price_data=price_data)

    # Process in batches
    all_results = []
    errors = []

    batch_size = 100
    for i in range(0, len(code_list), batch_size):
        batch = code_list[i : i + batch_size]
        try:
            batch_results = process_func(batch)

            # Flatten results for batch insert
            for code, df in batch_results.items():
                for date, row in df.iterrows():
                    all_results.append(
                        {
                            "Date": str(date),
                            "Code": row["Code"],
                            "RelativeStrengthPercentage": row[
                                "RelativeStrengthPercentage"
                            ],
                        }
                    )
        except Exception as e:
            logger.error(f"Error processing batch {i // batch_size}: {e}")
            for code in batch:
                errors.append([code, str(e)])

    # Batch insert all results
    if all_results:
        result_db_processor = BatchDatabaseProcessor(result_db_path)
        inserted = result_db_processor.batch_insert("relative_strength", all_results)
        logger.info(f"Inserted {inserted} RSP records")

    if errors:
        error_file = os.path.join(
            OUTPUT_DIR,
            f"errors_relative_strength_init_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
        )
        error_df = pd.DataFrame(errors, columns=["code", "error"])
        error_df.to_csv(error_file, index=False)
        logger.warning(f"Errors saved to {error_file}")

    processed = len(code_list) - len(errors)
    logger.info(
        f"Relative strength initialization completed. Processed {processed} stocks successfully"
    )
    return processed, len(errors)


@measure_performance
def update_rsp_db(
    db_path=JQUANTS_DB_PATH,
    result_db_path=RESULTS_DB_PATH,
    calc_start_date=None,
    calc_end_date=None,
    period=-5,
    n_workers=None,
):
    """Update relative strength database with recent data using parallel processing"""
    logger = setup_logging()

    if calc_end_date is None:
        calc_end_date = datetime.datetime.today().strftime("%Y-%m-%d")
    if calc_start_date is None:
        calc_start_date = (
            datetime.datetime.strptime(calc_end_date, "%Y-%m-%d") - timedelta(days=250)
        ).strftime("%Y-%m-%d")

    # Get all tickers
    try:
        db_processor = BatchDatabaseProcessor(db_path)
        code_df = db_processor.batch_fetch(
            "SELECT DISTINCT Code FROM daily_quotes ORDER BY Code", as_dataframe=True
        )
        code_list = code_df["Code"].tolist()
    except sqlite3.Error as e:
        logger.error(f"Error getting code list: {e}")
        raise

    logger.info(
        f"Updating relative strength for {len(code_list)} stocks from {calc_start_date} to {calc_end_date}"
    )

    # Load all price data for the period
    try:
        price_data = db_processor.batch_fetch(
            """
            SELECT Code, Date, AdjustmentClose
            FROM daily_quotes
            WHERE Date BETWEEN ? AND ?
            ORDER BY Code, Date
            """,
            params=[calc_start_date, calc_end_date],
            as_dataframe=True,
        )
        price_data["Date"] = pd.to_datetime(price_data["Date"])
    except sqlite3.Error as e:
        logger.error(f"Error loading price data: {e}")
        raise

    # Replace empty strings with NaN
    price_data["AdjustmentClose"] = price_data["AdjustmentClose"].replace("", np.nan)

    # Process stocks in parallel
    from functools import partial

    process_func = partial(process_stock_batch_rsp, price_data=price_data)

    # Process in batches
    all_results = []
    errors = []

    batch_size = 100
    for i in range(0, len(code_list), batch_size):
        batch = code_list[i : i + batch_size]
        try:
            batch_results = process_func(batch)

            # Flatten results and filter by period
            for code, df in batch_results.items():
                df_filtered = df.iloc[period:] if period < 0 else df
                for date, row in df_filtered.iterrows():
                    if pd.notna(row["RelativeStrengthPercentage"]):
                        all_results.append(
                            {
                                "Date": str(date),
                                "Code": row["Code"],
                                "RelativeStrengthPercentage": row[
                                    "RelativeStrengthPercentage"
                                ],
                            }
                        )
        except Exception as e:
            logger.error(f"Error processing batch {i // batch_size}: {e}")
            for code in batch:
                errors.append([calc_end_date, code, str(e)])

    # Batch insert all results
    if all_results:
        result_db_processor = BatchDatabaseProcessor(result_db_path)
        inserted = result_db_processor.batch_insert(
            "relative_strength", all_results, on_conflict="REPLACE"
        )
        logger.info(f"Updated {inserted} RSP records")

    if errors:
        error_file = os.path.join(
            OUTPUT_DIR,
            f"errors_relative_strength_update_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
        )
        error_df = pd.DataFrame(errors, columns=["Date", "Code", "error"])
        error_df.to_csv(error_file, index=False)
        logger.warning(f"Errors saved to {error_file}")

    processed = len(code_list) - len([e for e in errors if e[1] != "ALL"])
    logger.info(
        f"Relative strength update completed. Processed {processed} stocks successfully"
    )
    return processed, len(errors)


@measure_performance
def update_rsi_db(result_db_path=RESULTS_DB_PATH, date_list=None, period=-5):
    """Update relative strength index for multiple dates using optimized batch operations"""
    logger = setup_logging()

    if date_list is None:
        # Get recent dates from the database
        try:
            db_processor = BatchDatabaseProcessor(result_db_path)
            date_df = db_processor.batch_fetch(
                """SELECT DISTINCT Date FROM relative_strength 
                   WHERE RelativeStrengthPercentage IS NOT NULL 
                   AND RelativeStrengthIndex IS NULL 
                   ORDER BY Date DESC LIMIT 20""",
                as_dataframe=True,
            )
            if date_df.empty:
                date_df = db_processor.batch_fetch(
                    "SELECT DISTINCT Date FROM relative_strength ORDER BY Date DESC LIMIT 10",
                    as_dataframe=True,
                )
            date_list = date_df["Date"].tolist()
        except sqlite3.Error as e:
            logger.error(f"Error getting date list: {e}")
            return 0

    target_dates = date_list[period:] if period < 0 else date_list
    logger.info(f"Updating relative strength index for {len(target_dates)} dates")

    if not target_dates:
        logger.info("No dates to process")
        return 0

    errors = []
    db_processor = BatchDatabaseProcessor(result_db_path)

    try:
        # Load ALL data for all target dates at once
        date_params = ",".join(["?" for _ in target_dates])
        all_data = db_processor.batch_fetch(
            f"""SELECT Code, Date, RelativeStrengthPercentage 
               FROM relative_strength 
               WHERE Date IN ({date_params})
               AND RelativeStrengthPercentage IS NOT NULL
               ORDER BY Date, RelativeStrengthPercentage DESC""",
            params=target_dates,
            as_dataframe=True,
        )

        if all_data.empty:
            logger.warning("No RSP data found for any target dates")
            return 0

        # Convert to numeric
        all_data["RelativeStrengthPercentage"] = pd.to_numeric(
            all_data["RelativeStrengthPercentage"], errors="coerce"
        )

        # Process all dates at once using pandas groupby
        logger.info("Calculating RSI values using vectorized operations...")

        def calculate_rsi_for_date_group(group):
            """Calculate RSI for a single date's data"""
            # Sort by RSP descending
            group = group.sort_values(
                "RelativeStrengthPercentage", ascending=False, na_position="last"
            )

            # Filter valid entries
            valid_mask = group["RelativeStrengthPercentage"].notna()
            valid_count = valid_mask.sum()

            if valid_count == 0:
                group["RelativeStrengthIndex"] = np.nan
                return group

            # Calculate RSI using vectorized operations
            group["RelativeStrengthIndex"] = np.nan
            if valid_count > 1:
                ranks = np.arange(valid_count)
                group.loc[valid_mask, "RelativeStrengthIndex"] = (
                    99 - (ranks / (valid_count - 1)) * 98
                )
            else:
                group.loc[valid_mask, "RelativeStrengthIndex"] = 50

            return group

        # Apply RSI calculation to all dates simultaneously
        result_data = all_data.groupby("Date", group_keys=False).apply(
            calculate_rsi_for_date_group
        )

        # Filter only records with valid RSI values for batch update
        valid_rsi_data = result_data[
            result_data["RelativeStrengthIndex"].notna()
        ].copy()

        if valid_rsi_data.empty:
            logger.warning("No valid RSI values calculated")
            return 0

        # Prepare batch update data
        update_records = []
        for _, row in valid_rsi_data.iterrows():
            update_records.append(
                {
                    "Date": row["Date"],
                    "Code": row["Code"],
                    "RelativeStrengthIndex": row["RelativeStrengthIndex"],
                }
            )

        # Perform single batch update using REPLACE to update RSI values
        logger.info(f"Performing batch update for {len(update_records)} records...")

        with sqlite3.connect(result_db_path) as conn:
            # Enable optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Bulk update RSI values
            conn.executemany(
                """
                UPDATE relative_strength 
                SET RelativeStrengthIndex = ?
                WHERE Date = ? AND Code = ?
            """,
                [
                    (r["RelativeStrengthIndex"], r["Date"], r["Code"])
                    for r in update_records
                ],
            )

            conn.commit()

        # Log progress by date
        dates_processed = valid_rsi_data["Date"].nunique()
        stocks_per_date = (
            len(update_records) // dates_processed if dates_processed > 0 else 0
        )

        logger.info("RSI update completed successfully:")
        logger.info(f"  Dates processed: {dates_processed}")
        logger.info(f"  Total records updated: {len(update_records)}")
        logger.info(f"  Average stocks per date: {stocks_per_date}")

        # Log sample of processed dates for verification
        sample_dates = sorted(valid_rsi_data["Date"].unique())[:5]
        for date in sample_dates:
            count = len(valid_rsi_data[valid_rsi_data["Date"] == date])
            logger.info(f"  {date}: {count} stocks updated")

    except Exception as e:
        logger.error(f"Error in batch RSI update: {e}")
        errors.append(["ALL", "ALL", str(e)])

    if errors:
        error_file = os.path.join(
            OUTPUT_DIR,
            f"errors_rsi_update_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
        )
        error_df = pd.DataFrame(errors, columns=["Date", "Code", "error"])
        error_df.to_csv(error_file, index=False)
        logger.warning(f"Errors saved to {error_file}")

    return len(errors)


# Maintain backward compatibility
# Removed duplicate function definitions that were causing RecursionError
