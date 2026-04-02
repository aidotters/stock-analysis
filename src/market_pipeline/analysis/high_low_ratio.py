import sqlite3
import datetime
import logging
import os
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import sys

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
        f"high_low_ratio_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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


def init_hl_ratio_db(db_path=RESULTS_DB_PATH):
    """Initialize hl_ratio table in results database with indexes"""
    logger = logging.getLogger(__name__)
    logger.info(f"Initializing hl_ratio results database at {db_path}")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS hl_ratio (
            Date TEXT NOT NULL,
            Code TEXT NOT NULL,
            HlRatio REAL NOT NULL,
            MedianRatio REAL NOT NULL,
            Weeks INTEGER NOT NULL,
            PRIMARY KEY (Date, Code)
        )
        """)
        conn.commit()

    # Create indexes for performance
    db_processor = BatchDatabaseProcessor(db_path)
    db_processor.create_indexes(
        [
            {"name": "idx_hl_ratio_date", "table": "hl_ratio", "columns": ["Date"]},
            {"name": "idx_hl_ratio_code", "table": "hl_ratio", "columns": ["Code"]},
            {
                "name": "idx_hl_ratio_date_code",
                "table": "hl_ratio",
                "columns": ["Date", "Code"],
                "unique": True,
            },
        ]
    )

    logger.info("HL ratio results database initialized successfully")


def calc_ratios_vectorized(df: pd.DataFrame, weeks: int = 52) -> pd.DataFrame:
    """
    Calculate both HL ratio and median ratio for all stocks using vectorized operations.

    Args:
        df: DataFrame with columns Date, Code, High, Low, AdjustmentClose
        weeks: Number of weeks for calculation

    Returns:
        DataFrame with Code, HlRatio, MedianRatio
    """
    days = weeks * 5

    # Ensure numeric columns
    df["High"] = pd.to_numeric(df["High"], errors="coerce")
    df["Low"] = pd.to_numeric(df["Low"], errors="coerce")
    df["AdjustmentClose"] = pd.to_numeric(df["AdjustmentClose"], errors="coerce")

    # Group by stock code
    grouped = df.groupby("Code")

    results = []

    for code, group in grouped:
        # Skip if insufficient data
        if len(group) < days:
            continue

        # Forward fill missing values
        group = group.sort_values("Date").ffill()

        # Get the last 'days' records
        period_data = group.tail(days)

        # Calculate highest and lowest in the period
        highest_price = period_data["High"].max()
        lowest_price = period_data["Low"].min()

        # Get current price (last close)
        current_price = group["AdjustmentClose"].iloc[-1]

        # Calculate median price
        median_price = period_data["AdjustmentClose"].median()

        # Skip if invalid data
        if (
            pd.isna(highest_price)
            or pd.isna(lowest_price)
            or pd.isna(current_price)
            or pd.isna(median_price)
        ):
            continue

        # Handle edge case
        if highest_price == lowest_price:
            hl_ratio = 50.0
            median_ratio = 50.0
        else:
            hl_ratio = (
                (current_price - lowest_price) / (highest_price - lowest_price) * 100
            )
            median_ratio = (
                (median_price - lowest_price) / (highest_price - lowest_price) * 100
            )

        results.append({"Code": code, "HlRatio": hl_ratio, "MedianRatio": median_ratio})

    return pd.DataFrame(results)


def process_stock_batch(
    stock_codes: list, price_data: pd.DataFrame, weeks: int
) -> dict:
    """
    Process a batch of stocks for HL ratio calculation.

    Args:
        stock_codes: List of stock codes to process
        price_data: DataFrame with all price data
        weeks: Number of weeks for calculation

    Returns:
        Dictionary mapping stock codes to their ratios
    """
    # Filter data for this batch of stocks
    batch_data = price_data[price_data["Code"].isin(stock_codes)]

    # Calculate ratios using vectorized operations
    ratios_df = calc_ratios_vectorized(batch_data, weeks)

    # Convert to dictionary format
    results = {}
    for _, row in ratios_df.iterrows():
        results[row["Code"]] = {
            "HlRatio": row["HlRatio"],
            "MedianRatio": row["MedianRatio"],
        }

    return results


@measure_performance
def calc_hl_ratio_for_all(
    db_path=JQUANTS_DB_PATH, end_date=None, weeks=52, n_workers=None
):
    """
    Optimized version of calc_hl_ratio_for_all using parallel processing and batch operations.

    Args:
        db_path: Path to jquants database
        end_date: End date for calculation
        weeks: Number of weeks for ratio calculation
        n_workers: Number of parallel workers (None for auto)

    Returns:
        DataFrame with calculated ratios
    """
    logger = logging.getLogger(__name__)

    if end_date is None:
        end_date = datetime.datetime.today().strftime("%Y-%m-%d")

    if isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    buffers = 30
    start_date = end_date - relativedelta(days=weeks * 7 + buffers)

    logger.info(
        f"Calculating HL ratio for all stocks from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )

    # Load all data at once (batch fetch)
    try:
        db_processor = BatchDatabaseProcessor(db_path)
        price_df = db_processor.batch_fetch(
            """
            SELECT Date, Code, High, Low, AdjustmentClose
            FROM daily_quotes
            WHERE Date BETWEEN ? AND ?
            ORDER BY Code, Date
            """,
            params=[start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")],
            as_dataframe=True,
        )
        price_df["Date"] = pd.to_datetime(price_df["Date"])
    except sqlite3.Error as e:
        logger.error(f"Database error while reading daily_quotes: {e}")
        raise

    price_df = price_df.replace("", np.nan)
    code_list = sorted(price_df["Code"].unique())

    logger.info(
        f"Processing {len(code_list)} stocks for HL Ratio using parallel processing."
    )

    # Process stocks in parallel
    from functools import partial

    process_func = partial(process_stock_batch, price_data=price_df, weeks=weeks)

    # Split codes into batches for parallel processing
    results_dict = {}
    errors = {}

    # Process in batches
    batch_size = 100
    for i in range(0, len(code_list), batch_size):
        batch = code_list[i : i + batch_size]
        try:
            batch_results = process_func(batch)
            results_dict.update(batch_results)
        except Exception as e:
            logger.error(f"Error processing batch {i // batch_size}: {e}")
            for code in batch:
                errors[code] = str(e)

    if not results_dict:
        logger.warning("No HL ratios were calculated.")
        return pd.DataFrame()

    # Convert results to DataFrame
    ratio_data = []
    for code, ratios in results_dict.items():
        ratio_data.append(
            {
                "Code": code,
                "HlRatio": ratios["HlRatio"],
                "MedianRatio": ratios["MedianRatio"],
            }
        )

    ratio_df = pd.DataFrame(ratio_data)
    ratio_df = ratio_df.sort_values("HlRatio", ascending=False).reset_index(drop=True)
    ratio_df["Date"] = end_date.strftime("%Y-%m-%d")
    ratio_df["Weeks"] = weeks

    if errors:
        logger.warning(f"Encountered {len(errors)} errors during HL ratio calculation.")

    logger.info(
        f"HL ratio calculation completed. Calculated for {len(results_dict)} stocks."
    )

    # Save results using batch insert
    if len(ratio_df) > 0:
        save_results_batch(ratio_df, RESULTS_DB_PATH)

    return ratio_df


def save_results_batch(ratio_df: pd.DataFrame, db_path: str):
    """Save results to database using batch insert"""
    logger = logging.getLogger(__name__)

    # Ensure database is initialized
    init_hl_ratio_db(db_path)

    # Convert DataFrame to list of dictionaries
    records = ratio_df.to_dict("records")

    # Use batch insert
    db_processor = BatchDatabaseProcessor(db_path)
    inserted = db_processor.batch_insert("hl_ratio", records, on_conflict="REPLACE")

    logger.info(f"Saved {inserted} HL ratio records to database")


# Maintain backward compatibility
def calc_hl_ratio_for_all_legacy(db_path=JQUANTS_DB_PATH, end_date=None, weeks=52):
    """
    Wrapper function to maintain backward compatibility.
    Calls the optimized version.
    """
    return calc_hl_ratio_for_all(db_path, end_date, weeks)


def calc_hl_ratio_by_code(
    code, db_path=JQUANTS_DB_PATH, end_date=None, weeks=52, save_to_db=True
):
    """Calculate HL ratio for a specific stock code (maintains compatibility)"""
    logger = logging.getLogger(__name__)

    if end_date is None:
        end_date = datetime.datetime.today().strftime("%Y-%m-%d")

    if isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    buffers = 30
    start_date = end_date - relativedelta(days=weeks * 7 + buffers)

    logger.info(
        f"Calculating HL ratio for {code} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )

    try:
        db_processor = BatchDatabaseProcessor(db_path)
        price_df = db_processor.batch_fetch(
            """
            SELECT Date, Code, High, Low, AdjustmentClose
            FROM daily_quotes
            WHERE Date BETWEEN ? AND ? 
            AND Code = ?
            ORDER BY Date
            """,
            params=[
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                code,
            ],
            as_dataframe=True,
        )
        price_df["Date"] = pd.to_datetime(price_df["Date"])
    except sqlite3.Error as e:
        logger.error(f"Database error for {code}: {e}")
        raise

    if len(price_df) < weeks * 5:
        logger.warning(f"Insufficient data for {code}: {len(price_df)} days")
        return None, price_df

    # Calculate ratios using vectorized function
    ratios_df = calc_ratios_vectorized(price_df, weeks)

    if len(ratios_df) == 0:
        logger.warning(f"No valid ratios calculated for {code}")
        return None, price_df

    # Extract results
    hl_ratio = ratios_df.iloc[0]["HlRatio"]
    median_ratio = ratios_df.iloc[0]["MedianRatio"]

    logger.info(
        f"HL ratio for {code}: {hl_ratio:.2f}%, Median ratio: {median_ratio:.2f}%"
    )

    # Save to database if requested
    if save_to_db:
        try:
            record = {
                "Date": end_date.strftime("%Y-%m-%d"),
                "Code": code,
                "HlRatio": hl_ratio,
                "MedianRatio": median_ratio,
                "Weeks": weeks,
            }
            db_processor = BatchDatabaseProcessor(RESULTS_DB_PATH)
            db_processor.batch_insert("hl_ratio", [record], on_conflict="REPLACE")
            logger.debug(f"HL ratio and Median ratio for {code} saved to database")
        except Exception as e:
            logger.error(f"Error saving ratios for {code} to database: {e}")

    return {"HlRatio": hl_ratio, "MedianRatio": median_ratio}, price_df


if __name__ == "__main__":
    setup_logging()
    calc_hl_ratio_for_all()
