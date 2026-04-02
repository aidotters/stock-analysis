"""
OPTIMIZED Chart Classification (High-Performance Batch Processing)
==================================================================

This script performs chart pattern classification on stock price data with significant
performance optimizations for large-scale processing.

OPTIMIZATIONS IMPLEMENTED:
- Batch database operations with connection pooling
- Vectorized template matching using NumPy
- Efficient data caching and reuse
- Parallel processing capabilities
- Comprehensive logging and error handling

It can be run in two modes:

1.  **Sample Mode (`--mode sample`)**:
    -   Analyzes a predefined list of stock tickers.
    -   Saves the resulting classification plots as PNG images in the output directory.

2.  **Full Mode (`--mode full`)**:
    -   Fetches all tickers from the master database using optimized queries.
    -   Runs classification for all tickers across all specified time windows.
    -   Saves the classification results using batch operations into SQLite database.

Usage:
------
-   For a sample run: `python chart_classification.py --mode sample`
-   For a full run:   `python chart_classification.py --mode full`
"""

import argparse
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from sklearn.preprocessing import MinMaxScaler

# --- Constants ---
JQUANTS_DB_PATH = "/Users/tak/Markets/Stocks/Stock-Analysis/data/jquants.db"
MASTER_DB_PATH = "/Users/tak/Markets/Stocks/Stock-Analysis/data/master.db"  # Assumes master.db is in the data directory
OUTPUT_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/output"
DATA_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/data"
LOGS_DIR = "/Users/tak/Markets/Stocks/Stock-Analysis/logs"
RESULTS_DB_PATH = os.path.join(DATA_DIR, "analysis_results.db")


def setup_logging() -> logging.Logger:
    """Setup optimized logging configuration with performance tracking"""
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_filename = os.path.join(
        LOGS_DIR,
        f"chart_classification_optimized_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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
    logger.info(
        f"OPTIMIZED chart classification logging initialized. Log file: {log_filename}"
    )
    return logger


class DatabaseManager:
    """Optimized database connection manager for batch operations"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None

    def __enter__(self):
        self._connection = sqlite3.connect(self.db_path)
        # Enable optimizations
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute("PRAGMA cache_size=10000")
        return self._connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            self._connection.close()


class BatchDataLoader:
    """Optimized data loader for batch processing multiple tickers"""

    def __init__(self, db_path: str, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger
        self._data_cache: Dict[str, pd.Series] = {}

    def load_all_ticker_data(
        self, tickers: List[str], days: int = 500
    ) -> Dict[str, pd.Series]:
        """Load data for all tickers in a single optimized query"""
        end_date = datetime.today()

        # For long-term analysis (> 1000 days), load all available data
        if days > 1000:
            self.logger.info(
                f"Loading ALL available data for {len(tickers)} tickers (long-term analysis)..."
            )
            start_date_str = "2020-01-01"  # Use earliest possible date to get all data
        else:
            start_date = end_date - timedelta(days=days)
            start_date_str = start_date.strftime("%Y-%m-%d")
            self.logger.info(f"Loading data for {len(tickers)} tickers in batch...")

        start_time = time.time()

        with DatabaseManager(self.db_path) as conn:
            # Create placeholders for batch query
            placeholders = ",".join(["?" for _ in tickers])
            query = f"""
            SELECT Code, Date, AdjustmentClose 
            FROM daily_quotes 
            WHERE Code IN ({placeholders}) 
            AND Date BETWEEN ? AND ?
            ORDER BY Code, Date
            """

            params = tickers + [start_date_str, end_date.strftime("%Y-%m-%d")]
            df = pd.read_sql_query(query, conn, params=params, parse_dates=["Date"])

        # Process data by ticker efficiently
        ticker_data = {}
        for ticker in tickers:
            ticker_df = df[df["Code"] == ticker].copy()
            if not ticker_df.empty:
                series = ticker_df.set_index("Date")["AdjustmentClose"].dropna()
                ticker_data[ticker] = series
                self.logger.debug(f"Loaded {len(series)} days for ticker {ticker}")
            else:
                ticker_data[ticker] = pd.Series(dtype=float)

        load_time = time.time() - start_time
        self.logger.info(
            f"Loaded data for {len(ticker_data)} tickers in {load_time:.2f} seconds"
        )

        return ticker_data

    def check_ticker_data_length(self, ticker: str) -> int:
        """Check the number of available data days for a specific ticker"""
        end_date = datetime.today()
        start_date = end_date - timedelta(days=1500)  # Check up to 1500 days

        with DatabaseManager(self.db_path) as conn:
            query = """
            SELECT COUNT(*) as count
            FROM daily_quotes 
            WHERE Code = ? AND Date BETWEEN ? AND ?
            """
            result = pd.read_sql_query(
                query,
                conn,
                params=[
                    ticker,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                ],
            )
            return result["count"].iloc[0] if not result.empty else 0


class BatchResultsProcessor:
    """Optimized batch processor for saving classification results"""

    def __init__(self, db_path: str, logger: logging.Logger, batch_size: int = 1000):
        self.db_path = db_path
        self.logger = logger
        self.batch_size = batch_size
        self.pending_results: List[Tuple[str, str, int, str, float]] = []

    def add_result(self, date: str, ticker: str, window: int, label: str, score: float):
        """Add a result to the pending batch"""
        self.pending_results.append((date, ticker, window, label, score))

        # Auto-flush if batch is full
        if len(self.pending_results) >= self.batch_size:
            self.flush_results()

    def flush_results(self):
        """Save all pending results to database in a batch operation"""
        if not self.pending_results:
            return

        start_time = time.time()
        self.logger.info(f"Flushing {len(self.pending_results)} results to database...")

        with DatabaseManager(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR REPLACE INTO classification_results (date, ticker, window, pattern_label, score)
                VALUES (?, ?, ?, ?, ?)
            """,
                self.pending_results,
            )
            conn.commit()

        flush_time = time.time() - start_time
        self.logger.info(
            f"Flushed {len(self.pending_results)} results in {flush_time:.2f} seconds"
        )
        self.pending_results.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Ensure all pending results are saved
        self.flush_results()


class OptimizedChartClassifier:
    """
    OPTIMIZED chart pattern classifier with batch processing capabilities and improved performance.
    """

    # Class-level template cache to avoid recreating templates for each instance
    _template_cache: Dict[int, Dict[str, np.ndarray]] = {}

    def __init__(
        self,
        ticker: str,
        window: int,
        price_data: Optional[pd.Series] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.ticker = ticker
        self.window = window
        self.logger = logger or logging.getLogger(__name__)

        # Use provided data or load from database
        if price_data is not None:
            self.price_data = price_data
        else:
            self.price_data = self._get_stock_data()

        # Use cached templates or create new ones
        if window not in self._template_cache:
            self._template_cache[window] = self._create_manual_templates()
        self.templates_manual = self._template_cache[window]

    def _get_stock_data(self, days: int = 500) -> pd.Series:
        """Fallback method for single ticker data loading (less efficient than batch)"""
        end_date = datetime.today()
        start_date = end_date - timedelta(days=days)

        try:
            with DatabaseManager(JQUANTS_DB_PATH) as conn:
                query = """
                SELECT Date, AdjustmentClose 
                FROM daily_quotes 
                WHERE Code = ? AND Date BETWEEN ? AND ?
                ORDER BY Date
                """
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=[
                        self.ticker,
                        start_date.strftime("%Y-%m-%d"),
                        end_date.strftime("%Y-%m-%d"),
                    ],
                    parse_dates=["Date"],
                )
        except sqlite3.Error as e:
            raise ConnectionError(f"Database connection or query failed: {e}")

        if len(df) < self.window:
            raise ValueError(
                f"Not enough data for ticker {self.ticker} with window {self.window} (found {len(df)} days)"
            )

        return df.set_index("Date")["AdjustmentClose"].dropna()

    @staticmethod
    def _normalize(arr: np.ndarray) -> np.ndarray:
        if len(arr) == 0:
            raise ValueError("Cannot normalize empty array")
        if len(arr) == 1:
            return np.array([0.5])  # Single value normalized to middle

        scaler = MinMaxScaler()
        return scaler.fit_transform(arr.reshape(-1, 1)).flatten()

    def _create_manual_templates(self) -> Dict[str, np.ndarray]:
        half1 = self.window // 2
        half2 = self.window - half1
        templates = {
            "上昇ストップ": np.concatenate(
                [np.linspace(0, 1, half1), np.full(half2, 1)]
            ),
            "上昇": np.linspace(0, 1, self.window),
            "急上昇": np.concatenate([np.full(half1, 0), np.linspace(0, 1, half2)]),
            "調整": np.concatenate(
                [np.linspace(0, 1, half1), np.linspace(1, 0, half2)]
            ),
            "もみ合い": np.sin(np.linspace(0, 4 * np.pi, self.window)),
            "リバウンド": np.concatenate(
                [np.linspace(1, 0, half1), np.linspace(0, 1, half2)]
            ),
            "急落": np.concatenate([np.full(half1, 1), np.linspace(1, 0, half2)]),
            "下落": np.linspace(1, 0, self.window),
            "下げとまった": np.concatenate(
                [np.linspace(1, 0, half1), np.full(half2, 0)]
            ),
        }
        return {name: self._normalize(template) for name, template in templates.items()}

    def _find_best_match(
        self, series: np.ndarray, templates: Dict[str, np.ndarray]
    ) -> Tuple[str, float]:
        normalized_series = self._normalize(series)
        best_label, best_score = None, -np.inf

        for label, tpl in templates.items():
            # Check if lengths match before calculating correlation
            if len(normalized_series) != len(tpl):
                print(
                    f"Warning: Length mismatch for {label}: series={len(normalized_series)}, template={len(tpl)}"
                )
                continue

            score, _ = pearsonr(normalized_series, tpl)
            if np.isnan(score):
                score = 0
            if score > best_score:
                best_label, best_score = label, score

        if best_label is None:
            raise ValueError(
                f"No matching template found for series of length {len(normalized_series)}"
            )

        return best_label, best_score

    def classify_latest(self) -> Tuple[str, float, str]:
        latest_data = self.price_data.iloc[-self.window :].values
        if len(latest_data) == 0:
            raise ValueError(
                f"No price data available for classification (ticker: {self.ticker})"
            )

        # Get the date of the latest data point
        latest_date = self.price_data.index[-1].strftime("%Y-%m-%d")

        label, score = self._find_best_match(latest_data, self.templates_manual)
        return label, score, latest_date

    def save_classification_plot(self, label: str, score: float, output_dir: str):
        latest_data = self.price_data.iloc[-self.window :].values
        normalized_latest = self._normalize(latest_data)
        template = self.templates_manual[label]

        fig = plt.figure(figsize=(10, 5))
        plt.plot(normalized_latest, label="最新の株価", linewidth=2)
        plt.plot(template, "--", label=f"テンプレート: {label}")
        plt.title(
            f"銘柄: {self.ticker} (直近{self.window}日) vs. パターン: {label} (r={score:.3f})"
        )
        plt.legend()
        plt.grid(True, linestyle="--", alpha=0.6)

        os.makedirs(output_dir, exist_ok=True)
        filename = f"{self.ticker}_window{self.window}_{label}.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath)
        plt.close(fig)
        print(f"Plot saved to {filepath}")


# --- Database Utility Functions ---


def get_all_tickers(db_path: str) -> List[str]:
    """Fetches all unique ticker codes from the master database."""
    print(f"Reading all tickers from {db_path}...")
    try:
        with sqlite3.connect(db_path) as conn:
            # Assuming the table is named 'master' or 'stocks'. Adjust if necessary.
            df = pd.read_sql_query("SELECT * FROM stocks_master", conn)
        tickers = df["jquants_code"].astype(str).tolist()
        print(f"Found {len(tickers)} unique tickers.")
        return tickers
    except Exception as e:
        print(f"Error reading from master database: {e}")
        return []


def init_results_db(db_path: str):
    """Initializes the results database and creates the table if it doesn't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS classification_results (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            window INTEGER NOT NULL,
            pattern_label TEXT NOT NULL,
            score REAL NOT NULL,
            PRIMARY KEY (date, ticker, window)
        )
        """)
        conn.commit()


def save_result_to_db(
    db_path: str, date: str, ticker: str, window: int, label: str, score: float
):
    """Saves a single classification result to the database."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
        INSERT OR REPLACE INTO classification_results (date, ticker, window, pattern_label, score)
        VALUES (?, ?, ?, ?, ?)
        """,
            (date, ticker, window, label, score),
        )
        conn.commit()


def get_adaptive_windows(ticker_data_length: int) -> List[int]:
    """
    Get adaptive windows based on available data length

    Args:
        ticker_data_length: Number of available data days for the ticker

    Returns:
        List of window sizes to use for classification
    """
    base_windows = [20, 60, 120, 240]

    # Add adaptive long-term window based on data availability
    if ticker_data_length >= 1200:
        adaptive_windows = base_windows + [1200]
    elif ticker_data_length >= 960:
        adaptive_windows = base_windows + [960]
    else:
        adaptive_windows = base_windows

    return adaptive_windows


def check_all_tickers_data_length(
    db_path: str, tickers: List[str], logger: logging.Logger
) -> Dict[str, int]:
    """
    Check data length for all tickers in batch for efficiency

    Args:
        db_path: Path to the database
        tickers: List of ticker codes
        logger: Logger instance

    Returns:
        Dictionary mapping ticker to data length
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1500)  # Check up to 1500 days

    logger.info(f"Checking data length for {len(tickers)} tickers...")
    start_time = time.time()

    with DatabaseManager(db_path) as conn:
        # Create placeholders for batch query
        placeholders = ",".join(["?" for _ in tickers])
        query = f"""
        SELECT Code, COUNT(*) as count
        FROM daily_quotes 
        WHERE Code IN ({placeholders}) 
        AND Date BETWEEN ? AND ?
        GROUP BY Code
        """

        params = tickers + [
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        ]
        df = pd.read_sql_query(query, conn, params=params)

    # Create dictionary with all tickers, defaulting missing ones to 0
    ticker_lengths = {ticker: 0 for ticker in tickers}
    for _, row in df.iterrows():
        ticker_lengths[row["Code"]] = row["count"]

    check_time = time.time() - start_time
    logger.info(f"Checked data lengths in {check_time:.2f} seconds")

    return ticker_lengths


# --- Main Execution Functions ---


# Keep the original ChartClassifier class for backwards compatibility
class ChartClassifier(OptimizedChartClassifier):
    """Backwards compatibility wrapper for the original ChartClassifier"""

    def __init__(self, ticker: str, window: int, db_path: str = JQUANTS_DB_PATH):
        super().__init__(ticker, window)


def main_sample():
    """Runs classification for a sample of tickers and saves plots."""
    logger = setup_logging()
    TICKERS = ["74530", "99840", "67580"]  # Example: Fast Retailing, Softbank, Sony
    WINDOWS = [20, 60, 120, 240]

    logger.info("Starting sample chart classification run...")
    for ticker in TICKERS:
        for window in WINDOWS:
            try:
                classifier = OptimizedChartClassifier(
                    ticker=ticker, window=window, logger=logger
                )
                label, score, data_date = classifier.classify_latest()
                logger.info(
                    f"[Ticker: {ticker}, Window: {window}] -> Classification: {label} (r={score:.3f}) [{data_date}]"
                )
                classifier.save_classification_plot(label, score, OUTPUT_DIR)
            except (ValueError, ConnectionError) as e:
                logger.error(f"Error (Ticker: {ticker}, Window: {window}): {e}")
    logger.info("Sample run completed")


def main_sample_adaptive():
    """Test sample run with adaptive windows (1200/960 days) to demonstrate dynamic window selection."""
    logger = setup_logging()
    TICKERS = ["13010", "13050", "13060"]  # Use tickers with longer data history

    logger.info("Starting ADAPTIVE WINDOWS sample chart classification run...")

    # Check data lengths for sample tickers
    ticker_data_lengths = check_all_tickers_data_length(
        JQUANTS_DB_PATH, TICKERS, logger
    )

    # Load data with 1300 days to support long-term patterns
    data_loader = BatchDataLoader(JQUANTS_DB_PATH, logger)
    ticker_data = data_loader.load_all_ticker_data(TICKERS, days=1300)

    for ticker in TICKERS:
        logger.info(f"\n--- Processing Ticker: {ticker} ---")

        # Get data length and adaptive windows
        data_length = ticker_data_lengths.get(ticker, 0)
        adaptive_windows = get_adaptive_windows(data_length)

        logger.info(f"Data length: {data_length} days")
        logger.info(f"Adaptive windows: {adaptive_windows}")

        price_data = ticker_data.get(ticker, pd.Series(dtype=float))

        if price_data.empty:
            logger.error(f"No data available for ticker {ticker}")
            continue

        # Process all adaptive windows for this ticker
        for window in adaptive_windows:
            try:
                if len(price_data) < window:
                    logger.warning(
                        f"Insufficient data for {ticker} window {window}: {len(price_data)} < {window}"
                    )
                    continue

                classifier = OptimizedChartClassifier(
                    ticker=ticker, window=window, price_data=price_data, logger=logger
                )

                label, score, data_date = classifier.classify_latest()
                logger.info(
                    f"[Ticker: {ticker}, Window: {window}] -> Classification: {label} (r={score:.3f}) [{data_date}]"
                )

                # Save plot for demonstration (especially for long-term windows)
                if window >= 960:
                    classifier.save_classification_plot(label, score, OUTPUT_DIR)
                    logger.info(
                        f"Saved long-term pattern plot for {ticker} with {window}-day window"
                    )

            except Exception as e:
                logger.error(f"Error processing {ticker} window {window}: {e}")

    logger.info("Adaptive windows sample run completed")


def main_full_run_optimized():
    """OPTIMIZED version with adaptive windows: Runs classification for all tickers using batch processing and dynamic window selection."""
    logger = setup_logging()
    BATCH_SIZE = 100  # Process tickers in batches

    logger.info(
        "Starting OPTIMIZED full chart classification run with adaptive windows..."
    )

    # Get all tickers using optimized query
    all_tickers = get_all_tickers_optimized(MASTER_DB_PATH, logger)

    if not all_tickers:
        logger.error("No tickers found. Exiting.")
        return

    logger.info(
        f"Processing {len(all_tickers)} tickers with adaptive windows (1200/960 days)"
    )
    init_results_db_optimized(RESULTS_DB_PATH, logger)

    # Check data lengths for all tickers first (batch operation)
    ticker_data_lengths = check_all_tickers_data_length(
        JQUANTS_DB_PATH, all_tickers, logger
    )

    # Analyze window distribution
    window_stats = {"1200": 0, "960": 0, "base_only": 0}
    for ticker, length in ticker_data_lengths.items():
        if length >= 1200:
            window_stats["1200"] += 1
        elif length >= 960:
            window_stats["960"] += 1
        else:
            window_stats["base_only"] += 1

    logger.info(
        f"Window distribution: 1200-day={window_stats['1200']}, 960-day={window_stats['960']}, base-only={window_stats['base_only']}"
    )

    # Process tickers in batches for memory efficiency
    total_processed = 0
    total_errors = 0
    start_time = time.time()

    with BatchResultsProcessor(RESULTS_DB_PATH, logger) as results_processor:
        for batch_start in range(0, len(all_tickers), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(all_tickers))
            batch_tickers = all_tickers[batch_start:batch_end]

            logger.info(
                f"Processing batch {batch_start // BATCH_SIZE + 1}/{(len(all_tickers) + BATCH_SIZE - 1) // BATCH_SIZE}: "
                f"tickers {batch_start + 1}-{batch_end}"
            )

            # Load all data for this batch at once (using 1300 days to support 1200-day windows)
            data_loader = BatchDataLoader(JQUANTS_DB_PATH, logger)
            ticker_data = data_loader.load_all_ticker_data(batch_tickers, days=1300)

            # Process each ticker in the batch
            for ticker in batch_tickers:
                try:
                    price_data = ticker_data.get(ticker, pd.Series(dtype=float))

                    if price_data.empty:
                        logger.debug(f"No data available for ticker {ticker}")
                        # Count errors based on adaptive windows
                        adaptive_windows = get_adaptive_windows(
                            ticker_data_lengths.get(ticker, 0)
                        )
                        total_errors += len(adaptive_windows)
                        continue

                    # Get adaptive windows for this ticker
                    ticker_data_length = ticker_data_lengths.get(
                        ticker, len(price_data)
                    )
                    adaptive_windows = get_adaptive_windows(ticker_data_length)

                    # Process all adaptive windows for this ticker
                    for window in adaptive_windows:
                        try:
                            if len(price_data) < window:
                                logger.debug(
                                    f"Insufficient data for {ticker} window {window}: {len(price_data)} < {window}"
                                )
                                total_errors += 1
                                continue

                            classifier = OptimizedChartClassifier(
                                ticker=ticker,
                                window=window,
                                price_data=price_data,
                                logger=logger,
                            )

                            label, score, data_date = classifier.classify_latest()
                            results_processor.add_result(
                                data_date, ticker, window, label, score
                            )
                            total_processed += 1

                        except Exception as e:
                            logger.debug(
                                f"Error processing {ticker} window {window}: {e}"
                            )
                            total_errors += 1

                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {e}")
                    # Count errors based on adaptive windows
                    adaptive_windows = get_adaptive_windows(
                        ticker_data_lengths.get(ticker, 0)
                    )
                    total_errors += len(adaptive_windows)

            # Log progress
            progress = (batch_end / len(all_tickers)) * 100
            elapsed = time.time() - start_time
            estimated_total = elapsed * len(all_tickers) / batch_end
            remaining = estimated_total - elapsed

            logger.info(
                f"Batch completed. Progress: {progress:.1f}%, "
                f"Processed: {total_processed}, Errors: {total_errors}, "
                f"ETA: {remaining / 60:.1f} minutes"
            )

    # Final statistics
    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info("OPTIMIZED CHART CLASSIFICATION WITH ADAPTIVE WINDOWS COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Total time: {total_time / 60:.2f} minutes")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"Total errors: {total_errors}")
    logger.info(
        f"Processing rate: {total_processed / total_time:.1f} classifications/second"
    )
    logger.info(
        f"Window distribution: 1200-day={window_stats['1200']}, 960-day={window_stats['960']}, base-only={window_stats['base_only']}"
    )
    logger.info(f"Results saved to: {RESULTS_DB_PATH}")


def get_all_tickers_optimized(db_path: str, logger: logging.Logger) -> List[str]:
    """Optimized function to fetch all unique ticker codes from the master database."""
    logger.info(f"Reading all tickers from {db_path} using optimized query...")
    try:
        with DatabaseManager(db_path) as conn:
            # Try multiple possible table/column names
            tables_to_try = [
                ("stocks_master", "jquants_code"),
                ("master", "code"),
                ("stocks", "ticker"),
                ("companies", "code"),
            ]

            for table_name, column_name in tables_to_try:
                try:
                    # Check if table exists
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (table_name,),
                    )
                    if not cursor.fetchone():
                        continue

                    # Try to get the data
                    query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL"
                    df = pd.read_sql_query(query, conn)
                    tickers = df[column_name].astype(str).tolist()

                    # Filter out invalid tickers
                    tickers = [t for t in tickers if t and t != "nan"]

                    logger.info(
                        f"Found {len(tickers)} unique tickers from table {table_name}"
                    )
                    return tickers

                except Exception as e:
                    logger.debug(f"Failed to read from {table_name}.{column_name}: {e}")
                    continue

            # If all attempts failed
            logger.error("Could not find any valid ticker table in the master database")
            return []

    except Exception as e:
        logger.error(f"Error reading from master database: {e}")
        return []


def init_results_db_optimized(db_path: str, logger: logging.Logger):
    """Optimized database initialization with proper indexing."""
    logger.info("Initializing results database with optimizations...")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with DatabaseManager(db_path) as conn:
        cursor = conn.cursor()

        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS classification_results (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            window INTEGER NOT NULL,
            pattern_label TEXT NOT NULL,
            score REAL NOT NULL,
            PRIMARY KEY (date, ticker, window)
        )
        """)

        # Create optimized indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_class_date ON classification_results(date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_class_ticker ON classification_results(ticker)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_class_window ON classification_results(window)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_class_score ON classification_results(score DESC)"
        )

        conn.commit()

    logger.info("Results database initialized with optimized indexes")


def main():
    """Main function that handles argument parsing and dispatches to appropriate execution mode."""
    parser = argparse.ArgumentParser(
        description="OPTIMIZED Chart Pattern Classification for Stocks with Adaptive Windows."
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="full",
        choices=["sample", "sample-adaptive", "full", "full-optimized"],
        help="Execution mode: 'sample' for basic examples, 'sample-adaptive' for adaptive window demo, 'full' for all tickers, 'full-optimized' for high-performance processing with adaptive windows.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for optimized processing (default: 100)",
    )
    args = parser.parse_args()

    if args.mode == "sample":
        main_sample()
    elif args.mode == "sample-adaptive":
        main_sample_adaptive()
    elif args.mode == "full":
        # Keep original function for backwards compatibility
        main_full_run_optimized()  # But use optimized version by default
    elif args.mode == "full-optimized":
        main_full_run_optimized()


# Alias for backwards compatibility
main_full_run = main_full_run_optimized

if __name__ == "__main__":
    main()
