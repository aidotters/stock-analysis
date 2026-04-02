from __future__ import annotations

import os
import logging
import sqlite3
import warnings
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict

import numpy as np
import pandas as pd
from functools import partial

# Add project root to sys.path for imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

from market_pipeline.utils.parallel_processor import (
    BatchDatabaseProcessor,
    measure_performance,
)  # noqa: E402

try:
    import talib

    HAS_TALIB = True
except ImportError:
    HAS_TALIB = False
    warnings.warn("talib not available, using simple moving average implementation")


class MinerviniConfig:
    """Configuration settings for Minervini analysis."""

    def __init__(self, base_dir: Optional[Path] = None):
        if base_dir is None:
            base_dir = Path("/Users/tak/Markets/Stocks/Stock-Analysis")

        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.output_dir = self.base_dir / "output"

        self.jquants_db_path = self.data_dir / "jquants.db"
        self.results_db_path = self.data_dir / "analysis_results.db"

        # Create directories if they don't exist
        for directory in [self.data_dir, self.logs_dir, self.output_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    @property
    def error_output_dir(self) -> Path:
        """Get error output directory, creating if necessary."""
        error_dir = self.output_dir / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)
        return error_dir


def setup_logging(config: MinerviniConfig) -> logging.Logger:
    """Setup logging configuration."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = config.logs_dir / f"minervini_{timestamp}.log"

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


def simple_sma_vectorized(data: pd.Series, period: int) -> pd.Series:
    """Vectorized simple moving average implementation."""
    return data.rolling(window=period, min_periods=period).mean()


class MinerviniAnalyzer:
    """Optimized Minervini strategy analyzer using vectorized operations."""

    def __init__(self, config: MinerviniConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def calculate_strategy_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Minervini criteria using vectorized operations.

        Args:
            df: DataFrame with Date index and AdjustmentClose column

        Returns:
            DataFrame with all Minervini criteria calculated
        """
        # Ensure data is clean
        close_prices = pd.to_numeric(df["AdjustmentClose"], errors="coerce").ffill()

        # Calculate moving averages using vectorized operations
        if HAS_TALIB:
            sma50 = pd.Series(
                talib.SMA(close_prices.values, timeperiod=50), index=df.index
            )
            sma150 = pd.Series(
                talib.SMA(close_prices.values, timeperiod=150), index=df.index
            )
            sma200 = pd.Series(
                talib.SMA(close_prices.values, timeperiod=200), index=df.index
            )
        else:
            sma50 = simple_sma_vectorized(close_prices, 50)
            sma150 = simple_sma_vectorized(close_prices, 150)
            sma200 = simple_sma_vectorized(close_prices, 200)

        # Calculate 52-week high/low using rolling windows
        week_52_high = close_prices.rolling(window=260, min_periods=260).max()
        week_52_low = close_prices.rolling(window=260, min_periods=260).min()

        # Calculate all criteria vectorized
        results = df.copy()
        results["Close"] = close_prices
        results["Sma50"] = sma50
        results["Sma150"] = sma150
        results["Sma200"] = sma200

        # Type 1: Current price above 150-day and 200-day MA
        results["Type_1"] = ((close_prices > sma150) & (close_prices > sma200)).astype(
            float
        )

        # Type 2: 150-day MA above 200-day MA
        results["Type_2"] = (sma150 > sma200).astype(float)

        # Type 3: 200-day MA in uptrend for at least 1 month (20 days)
        sma200_trend = sma200 > sma200.shift(20)
        results["Type_3"] = sma200_trend.astype(float)

        # Type 4: 50-day MA above 150-day and 200-day MA
        results["Type_4"] = ((sma50 > sma150) & (sma50 > sma200)).astype(float)

        # Type 5: Current price above 50-day MA
        results["Type_5"] = (close_prices > sma50).astype(float)

        # Type 6: Current price at least 30% above 52-week low
        results["Type_6"] = (close_prices >= week_52_low * 1.3).astype(float)

        # Type 7: Current price within 25% of 52-week high
        results["Type_7"] = (close_prices >= week_52_high * 0.75).astype(float)

        # Type 8: Relative Strength Index >= 70 (calculated separately)
        results["Type_8"] = np.nan

        return results


def process_stock_batch_minervini(
    stock_codes: List[str], price_data: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """
    Process a batch of stocks for Minervini analysis.

    Args:
        stock_codes: List of stock codes to process
        price_data: DataFrame with all price data

    Returns:
        Dictionary mapping stock codes to their analysis results
    """
    config = MinerviniConfig()
    analyzer = MinerviniAnalyzer(config)
    results = {}

    for code in stock_codes:
        stock_data = price_data[price_data["Code"] == code].copy()

        if len(stock_data) < 260:  # Need at least 260 days for 52-week calculations
            continue

        stock_data = stock_data.set_index("Date").sort_index()
        analysis_result = analyzer.calculate_strategy_vectorized(stock_data)

        # Keep only valid results (where calculations are possible)
        valid_mask = analysis_result["Sma200"].notna()
        analysis_result = analysis_result[valid_mask]

        if not analysis_result.empty:
            results[code] = analysis_result

    return results


class MinerviniDatabase:
    """Optimized database operations for Minervini analysis."""

    def __init__(self, config: MinerviniConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def init_minervini_table(self, dest_db_path: str):
        """Initialize Minervini table with indexes."""
        with sqlite3.connect(dest_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS minervini (
                Date TEXT NOT NULL,
                Code TEXT NOT NULL,
                Close REAL,
                Sma50 REAL,
                Sma150 REAL,
                Sma200 REAL,
                Type_1 REAL,
                Type_2 REAL,
                Type_3 REAL,
                Type_4 REAL,
                Type_5 REAL,
                Type_6 REAL,
                Type_7 REAL,
                Type_8 REAL,
                PRIMARY KEY (Date, Code)
            )
            """)
            conn.commit()

        # Create indexes for performance
        db_processor = BatchDatabaseProcessor(dest_db_path)
        db_processor.create_indexes(
            [
                {
                    "name": "idx_minervini_date",
                    "table": "minervini",
                    "columns": ["Date"],
                },
                {
                    "name": "idx_minervini_code",
                    "table": "minervini",
                    "columns": ["Code"],
                },
                {
                    "name": "idx_minervini_date_code",
                    "table": "minervini",
                    "columns": ["Date", "Code"],
                    "unique": True,
                },
            ]
        )

    @measure_performance
    def init_database(
        self,
        source_db_path: str,
        dest_db_path: str,
        code_list: List[str],
        n_workers: Optional[int] = None,
    ) -> None:
        """Initialize Minervini database using parallel processing."""
        self.logger.info(
            f"Initializing Minervini database for {len(code_list)} stocks using parallel processing"
        )

        # Initialize table
        self.init_minervini_table(dest_db_path)

        # Load all price data at once
        db_processor = BatchDatabaseProcessor(source_db_path)
        price_data = db_processor.batch_fetch(
            """
            SELECT Code, Date, AdjustmentClose
            FROM daily_quotes
            ORDER BY Code, Date
            """,
            as_dataframe=True,
        )
        price_data["Date"] = pd.to_datetime(price_data["Date"])
        price_data["AdjustmentClose"] = price_data["AdjustmentClose"].replace(
            "", np.nan
        )

        # Process stocks in parallel
        process_func = partial(process_stock_batch_minervini, price_data=price_data)

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
                                "Date": str(date.date()),
                                "Code": row["Code"],
                                "Close": row["Close"],
                                "Sma50": row["Sma50"],
                                "Sma150": row["Sma150"],
                                "Sma200": row["Sma200"],
                                "Type_1": row["Type_1"],
                                "Type_2": row["Type_2"],
                                "Type_3": row["Type_3"],
                                "Type_4": row["Type_4"],
                                "Type_5": row["Type_5"],
                                "Type_6": row["Type_6"],
                                "Type_7": row["Type_7"],
                                "Type_8": row["Type_8"],
                            }
                        )

                self.logger.info(
                    f"Processed batch {i // batch_size + 1}, found results for {len(batch_results)} stocks"
                )

            except Exception as e:
                self.logger.error(f"Error processing batch {i // batch_size}: {e}")
                for code in batch:
                    errors.append([code, str(e)])

        # Batch insert all results
        if all_results:
            dest_db_processor = BatchDatabaseProcessor(dest_db_path)
            inserted = dest_db_processor.batch_insert("minervini", all_results)
            self.logger.info(f"Inserted {inserted} Minervini records")

        if errors:
            self._save_errors(errors, "errors_minervini_init.csv")

    @measure_performance
    def update_database(
        self,
        source_db_path: str,
        dest_db_path: str,
        code_list: List[str],
        calc_start_date: str,
        calc_end_date: str,
        period: int = 5,
    ) -> None:
        """Update Minervini database using parallel processing."""
        self.logger.info(
            f"Updating Minervini database for {len(code_list)} stocks from {calc_start_date} to {calc_end_date}"
        )

        # Load price data for the date range
        db_processor = BatchDatabaseProcessor(source_db_path)
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
        price_data["AdjustmentClose"] = price_data["AdjustmentClose"].replace(
            "", np.nan
        )

        # Process stocks in parallel
        process_func = partial(process_stock_batch_minervini, price_data=price_data)

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
                    df_filtered = df.tail(period) if period > 0 else df
                    for date, row in df_filtered.iterrows():
                        all_results.append(
                            {
                                "Date": str(date.date()),
                                "Code": row["Code"],
                                "Close": row["Close"],
                                "Sma50": row["Sma50"],
                                "Sma150": row["Sma150"],
                                "Sma200": row["Sma200"],
                                "Type_1": row["Type_1"],
                                "Type_2": row["Type_2"],
                                "Type_3": row["Type_3"],
                                "Type_4": row["Type_4"],
                                "Type_5": row["Type_5"],
                                "Type_6": row["Type_6"],
                                "Type_7": row["Type_7"],
                                "Type_8": row["Type_8"],
                            }
                        )

            except Exception as e:
                self.logger.error(f"Error processing batch {i // batch_size}: {e}")
                for code in batch:
                    errors.append([code, str(e)])

        # Batch insert all results
        if all_results:
            dest_db_processor = BatchDatabaseProcessor(dest_db_path)
            inserted = dest_db_processor.batch_insert(
                "minervini", all_results, on_conflict="REPLACE"
            )
            self.logger.info(f"Updated {inserted} Minervini records")

        if errors:
            self._save_errors(errors, "errors_minervini_update.csv")

    def _save_errors(self, errors: List, filename: str) -> None:
        """Save errors to CSV file."""
        if errors:
            error_df = pd.DataFrame(errors, columns=["Code", "Error"])
            error_path = self.config.error_output_dir / filename
            error_df.to_csv(error_path, index=False)
            self.logger.info(f"Saved {len(errors)} errors to {error_path}")

    @measure_performance
    def update_type8(
        self, dest_db_path: str, date_list: List[str], period: int = -5
    ) -> None:
        """Update type 8 (relative strength) using optimized batch operations."""
        self.logger.info(
            f"Updating Type_8 for {len(date_list)} dates using ultra-fast batch operations"
        )

        dates_to_process = date_list[period:] if period < 0 else date_list[-period:]

        try:
            # Load ALL relative strength data for all target dates at once (OPTIMIZED)
            rs_db_processor = BatchDatabaseProcessor(dest_db_path)
            date_params = ",".join(["?" for _ in dates_to_process])
            all_rs_data = rs_db_processor.batch_fetch(
                f"""
                SELECT Date, Code, RelativeStrengthIndex 
                FROM relative_strength 
                WHERE Date IN ({date_params})
                AND RelativeStrengthIndex IS NOT NULL
                ORDER BY Date, Code
                """,
                params=dates_to_process,
                as_dataframe=True,
            )

            if all_rs_data.empty:
                self.logger.warning(
                    "No relative strength data found for any target dates"
                )
                return

            # Vectorized Type_8 calculation for ALL data at once
            self.logger.info("Calculating Type_8 values using vectorized operations...")
            all_rs_data["RelativeStrengthIndex"] = pd.to_numeric(
                all_rs_data["RelativeStrengthIndex"], errors="coerce"
            )
            all_rs_data["Type_8"] = (all_rs_data["RelativeStrengthIndex"] >= 70).astype(
                float
            )
            all_rs_data.loc[all_rs_data["RelativeStrengthIndex"].isna(), "Type_8"] = (
                np.nan
            )

            # Filter valid Type_8 values
            valid_data = all_rs_data[all_rs_data["Type_8"].notna()].copy()

            if valid_data.empty:
                self.logger.warning("No valid Type_8 values calculated")
                return

            # Prepare ALL update records at once
            update_records = []
            for _, row in valid_data.iterrows():
                update_records.append(
                    {"Date": row["Date"], "Code": row["Code"], "Type_8": row["Type_8"]}
                )

            # Single batch update for ALL records
            self.logger.info(
                f"Performing single batch update for {len(update_records)} records..."
            )

            with sqlite3.connect(dest_db_path) as conn:
                # Enable optimizations
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")

                # Use direct UPDATE with executemany for maximum performance
                conn.executemany(
                    """
                    UPDATE minervini 
                    SET Type_8 = ?
                    WHERE Date = ? AND Code = ?
                """,
                    [(r["Type_8"], r["Date"], r["Code"]) for r in update_records],
                )

                conn.commit()

            # Log progress by date
            dates_processed = valid_data["Date"].nunique()
            records_per_date = (
                len(update_records) // dates_processed if dates_processed > 0 else 0
            )

            self.logger.info("Type_8 update completed successfully:")
            self.logger.info(f"  Dates processed: {dates_processed}")
            self.logger.info(f"  Total records updated: {len(update_records)}")
            self.logger.info(f"  Average records per date: {records_per_date}")

            # Log sample of processed dates for verification
            sample_dates = sorted(valid_data["Date"].unique())[:3]
            for date in sample_dates:
                count = len(valid_data[valid_data["Date"] == date])
                self.logger.info(f"  {date}: {count} records updated")

        except Exception as e:
            self.logger.error(f"Error in batch Type_8 update: {e}")
            raise

        self.logger.info(
            f"Type_8 ultra-fast batch update completed for {len(dates_to_process)} dates"
        )


# Backward compatibility functions
def init_minervini_db(
    source_conn: sqlite3.Connection,
    dest_conn: sqlite3.Connection,
    code_list: List[str],
    n_workers: Optional[int] = None,
) -> None:
    """Initialize Minervini database using optimized parallel processing."""
    config = MinerviniConfig()
    database = MinerviniDatabase(config)

    # Get database paths from connections
    source_db_path = source_conn.execute("PRAGMA database_list").fetchone()[2]
    dest_db_path = dest_conn.execute("PRAGMA database_list").fetchone()[2]

    database.init_database(source_db_path, dest_db_path, code_list, n_workers)


def update_minervini_db(
    source_conn: sqlite3.Connection,
    dest_conn: sqlite3.Connection,
    code_list: List[str],
    calc_start_date: str,
    calc_end_date: str,
    period: int = 5,
) -> None:
    """Update Minervini database using optimized parallel processing."""
    config = MinerviniConfig()
    database = MinerviniDatabase(config)

    # Get database paths from connections
    source_db_path = source_conn.execute("PRAGMA database_list").fetchone()[2]
    dest_db_path = dest_conn.execute("PRAGMA database_list").fetchone()[2]

    database.update_database(
        source_db_path, dest_db_path, code_list, calc_start_date, calc_end_date, period
    )


def update_type8_db(
    conn: sqlite3.Connection, date_list: List[str], period: int = -5
) -> None:
    """Update type 8 using optimized batch operations."""
    config = MinerviniConfig()
    database = MinerviniDatabase(config)

    # Get database path from connection
    dest_db_path = conn.execute("PRAGMA database_list").fetchone()[2]

    database.update_type8(dest_db_path, date_list, period)


# Removed duplicate function definitions that were causing RecursionError
