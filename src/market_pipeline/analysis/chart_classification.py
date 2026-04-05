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
from typing import Dict, List, Tuple, Optional, Union
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from sklearn.preprocessing import MinMaxScaler

# Window specification: int for cumulative (last N trading days), tuple for slice (start, end) trading days ago
WindowSpec = Union[int, Tuple[int, int]]

MIN_CORRELATION_THRESHOLD = 0.3

# --- Calendar/Trading Day Conversion ---
# Japanese market: ~245 trading days/year
_TRADING_DAYS_PER_YEAR = 245
_CALENDAR_DAYS_PER_YEAR = 365


def trading_to_calendar_days(trading_days: int) -> int:
    """Convert trading days to calendar days with safety margin."""
    return int(trading_days * _CALENDAR_DAYS_PER_YEAR / _TRADING_DAYS_PER_YEAR) + 100


# Maximum slice window end (trading days)
_MAX_SLICE_END = 4800

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
        self, tickers: List[str], trading_days: int = 500
    ) -> Dict[str, pd.Series]:
        """Load data for all tickers in a single optimized query.

        Args:
            tickers: List of ticker codes
            trading_days: Number of trading days to load (converted to calendar days internally)
        """
        end_date = datetime.today()
        calendar_days = trading_to_calendar_days(trading_days)
        start_date = end_date - timedelta(days=calendar_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        self.logger.info(
            f"Loading data for {len(tickers)} tickers "
            f"({trading_days} trading days / {calendar_days} calendar days)..."
        )

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
        """Check the number of available trading days for a specific ticker."""
        end_date = datetime.today()
        start_date = end_date - timedelta(days=trading_to_calendar_days(_MAX_SLICE_END))

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
        """Fallback method for single ticker data loading (less efficient than batch).

        Args:
            days: Number of trading days to load
        """
        end_date = datetime.today()
        start_date = end_date - timedelta(days=trading_to_calendar_days(days))

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

        # Log transform to reduce skew from sharp price spikes
        # np.where evaluates both branches before selecting, so suppress
        # expected warnings from log(0) and log(NaN)
        with np.errstate(divide="ignore", invalid="ignore"):
            arr = np.where(arr > 0, np.log(arr), 0.0)

        scaler = MinMaxScaler()
        return scaler.fit_transform(arr.reshape(-1, 1)).flatten()

    def _create_manual_templates(
        self, length: int | None = None
    ) -> Dict[str, np.ndarray]:
        n = length if length is not None else self.window
        half1 = n // 2
        half2 = n - half1
        templates = {
            "上昇ストップ": np.concatenate(
                [np.linspace(0, 1, half1), np.full(half2, 1)]
            ),
            "上昇": np.linspace(0, 1, n),
            "急上昇": np.concatenate([np.full(half1, 0), np.linspace(0, 1, half2)]),
            "調整": np.concatenate(
                [np.linspace(0, 1, half1), np.linspace(1, 0, half2)]
            ),
            "もみ合い": np.sin(np.linspace(0, 4 * np.pi, n)),
            "リバウンド": np.concatenate(
                [np.linspace(1, 0, half1), np.linspace(0, 1, half2)]
            ),
            "急落": np.concatenate([np.full(half1, 1), np.linspace(1, 0, half2)]),
            "下落": np.linspace(1, 0, n),
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

    def classify_window(self, window_spec: "WindowSpec") -> Tuple[str, float, str]:
        """Classify using cumulative or slice window.

        Args:
            window_spec: int for cumulative (last N days), tuple (start, end) for slice

        Returns:
            (label, score, latest_date)

        Raises:
            ValueError: If insufficient non-NaN data
        """
        if isinstance(window_spec, tuple):
            start, end = window_spec
            data = self.price_data.iloc[-end:-start].values
            expected_len = end - start
            template_len = expected_len
        else:
            data = self.price_data.iloc[-window_spec:].values
            expected_len = window_spec
            template_len = window_spec

        if len(data) == 0:
            raise ValueError(
                f"No price data available for classification (ticker: {self.ticker})"
            )

        # NaN removal
        data = data[~np.isnan(data)]

        # Data length check (require >= 50% of expected)
        if len(data) < expected_len * 0.5:
            raise ValueError(
                f"Insufficient non-NaN data: {len(data)} < {expected_len * 0.5}"
            )

        # Resample to template length if needed
        if len(data) != template_len:
            x_old = np.linspace(0, 1, len(data))
            x_new = np.linspace(0, 1, template_len)
            data = np.interp(x_new, x_old, data)

        # Get or create templates for this length
        if template_len not in self._template_cache:
            self._template_cache[template_len] = self._create_manual_templates(
                length=template_len
            )

        templates = self._template_cache[template_len]
        label, score = self._find_best_match(data, templates)

        latest_date = self.price_data.index[-1].strftime("%Y-%m-%d")
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


CUMULATIVE_WINDOWS = [20, 60, 120, 240]
SLICE_WINDOWS: List[Tuple[int, int]] = [
    (240, 480),
    (480, 1200),
    (1200, 2400),
    (2400, 4800),
]


def get_adaptive_windows(ticker_data_length: int) -> List[WindowSpec]:
    """
    Get adaptive windows based on available data length.

    Short-term windows (20-240) are cumulative (last N days).
    Long-term windows are period slices (start to end days ago).

    Args:
        ticker_data_length: Number of available data days for the ticker

    Returns:
        List of WindowSpec: int for cumulative, tuple(start, end) for slice
    """
    windows: List[WindowSpec] = [
        w for w in CUMULATIVE_WINDOWS if ticker_data_length >= w
    ]

    # Add data_length as max cumulative window when shorter than 240
    if (
        ticker_data_length < CUMULATIVE_WINDOWS[-1]
        and ticker_data_length not in windows
    ):
        windows.append(ticker_data_length)

    # Slice windows: use actual data length when shorter than standard end
    for start, end in SLICE_WINDOWS:
        if ticker_data_length >= end:
            windows.append((start, end))
        elif ticker_data_length > start:
            # Data extends into this slice range but doesn't reach standard end
            # Use actual data length as end, will be mapped to standard window on save
            windows.append((start, ticker_data_length))

    return windows


STANDARD_WINDOWS = [20, 60, 120, 240, 2400480, 4801200, 12002400, 24004800]


def map_to_standard_window(actual_window: int) -> int | None:
    """Map an actual cumulative window size to the next larger standard window.

    Only applies to cumulative windows. For slice windows, use window_spec_to_db_value().
    Returns None if larger than all standards (skip saving).
    """
    cumulative_standards = [20, 60, 120, 240]
    for std in cumulative_standards:
        if actual_window <= std:
            return std
    return None


def map_slice_to_standard(window_spec: Tuple[int, int]) -> Tuple[int, int]:
    """Map a non-standard slice window to its standard slice window.

    E.g., (2400, 3600) -> (2400, 4800) because data falls in the 2400-4800 range.
    """
    start = window_spec[0]
    for s, e in SLICE_WINDOWS:
        if s == start:
            return (s, e)
    return window_spec


def window_spec_to_db_value(window_spec: WindowSpec) -> int:
    """Convert WindowSpec to integer for DB storage.

    Cumulative: stored as-is (e.g., 20, 60, 240)
    Slice: mapped to standard window, then start * 10000 + end (e.g., (2400, 3600) -> 24004800)
    """
    if isinstance(window_spec, tuple):
        standard = map_slice_to_standard(window_spec)
        return standard[0] * 10000 + standard[1]
    return window_spec


def db_value_to_window_spec(db_value: int) -> WindowSpec:
    """Convert DB integer back to WindowSpec.

    Values > 10000 are treated as slice windows.
    """
    if db_value > 10000:
        start = db_value // 10000
        end = db_value % 10000
        return (start, end)
    return db_value


def check_all_tickers_data_length(
    db_path: str, tickers: List[str], logger: logging.Logger
) -> Dict[str, int]:
    """
    Check available trading days for all tickers in batch.

    Returns:
        Dictionary mapping ticker to number of trading days
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=trading_to_calendar_days(_MAX_SLICE_END))

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
    """Test sample run with adaptive windows (480/1200/2400/4800 days) to demonstrate dynamic window selection."""
    logger = setup_logging()
    TICKERS = ["13010", "13050", "13060"]  # Use tickers with longer data history

    logger.info("Starting ADAPTIVE WINDOWS sample chart classification run...")

    data_loader = BatchDataLoader(JQUANTS_DB_PATH, logger)
    ticker_data = data_loader.load_all_ticker_data(TICKERS, trading_days=_MAX_SLICE_END)

    for ticker in TICKERS:
        logger.info(f"\n--- Processing Ticker: {ticker} ---")

        price_data = ticker_data.get(ticker, pd.Series(dtype=float))

        if price_data.empty:
            logger.error(f"No data available for ticker {ticker}")
            continue

        adaptive_windows = get_adaptive_windows(len(price_data))
        logger.info(f"Data length: {len(price_data)} trading days")
        logger.info(f"Adaptive windows: {adaptive_windows}")

        # Create a single classifier for all windows
        classifier = OptimizedChartClassifier(
            ticker=ticker,
            window=CUMULATIVE_WINDOWS[0],
            price_data=price_data,
            logger=logger,
        )

        for window_spec in adaptive_windows:
            try:
                required_len = (
                    window_spec[1] if isinstance(window_spec, tuple) else window_spec
                )

                if len(price_data) < required_len:
                    logger.warning(
                        f"Insufficient data for {ticker} window {window_spec}: {len(price_data)} < {required_len}"
                    )
                    continue

                label, score, data_date = classifier.classify_window(window_spec)
                logger.info(
                    f"[Ticker: {ticker}, Window: {window_spec}] -> Classification: {label} (r={score:.3f}) [{data_date}]"
                )

            except Exception as e:
                logger.error(f"Error processing {ticker} window {window_spec}: {e}")

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
        f"Processing {len(all_tickers)} tickers with adaptive windows (480/1200/2400/4800 days)"
    )
    init_results_db_optimized(RESULTS_DB_PATH, logger)

    # Check data lengths for window distribution statistics
    ticker_data_lengths = check_all_tickers_data_length(
        JQUANTS_DB_PATH, all_tickers, logger
    )

    # Log window distribution (informational only; actual windows determined by loaded data)
    window_stats = {"4800+": 0, "2400+": 0, "1200+": 0, "480+": 0, "base_only": 0}
    for ticker, length in ticker_data_lengths.items():
        if length >= 4800:
            window_stats["4800+"] += 1
        elif length >= 2400:
            window_stats["2400+"] += 1
        elif length >= 1200:
            window_stats["1200+"] += 1
        elif length >= 480:
            window_stats["480+"] += 1
        else:
            window_stats["base_only"] += 1

    logger.info(
        f"Window distribution: 4800+={window_stats['4800+']}, 2400+={window_stats['2400+']}, "
        f"1200+={window_stats['1200+']}, 480+={window_stats['480+']}, base-only={window_stats['base_only']}"
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

            data_loader = BatchDataLoader(JQUANTS_DB_PATH, logger)
            ticker_data = data_loader.load_all_ticker_data(
                batch_tickers, trading_days=_MAX_SLICE_END
            )

            # Process each ticker in the batch
            for ticker in batch_tickers:
                try:
                    price_data = ticker_data.get(ticker, pd.Series(dtype=float))

                    if price_data.empty:
                        logger.debug(f"No data available for ticker {ticker}")
                        total_errors += len(get_adaptive_windows(0))
                        continue

                    # Adaptive windows based on actual loaded trading days
                    adaptive_windows = get_adaptive_windows(len(price_data))

                    # Create a single classifier for all windows (reuse price_data)
                    classifier = OptimizedChartClassifier(
                        ticker=ticker,
                        window=CUMULATIVE_WINDOWS[0],
                        price_data=price_data,
                        logger=logger,
                    )

                    # Process all adaptive windows for this ticker
                    for window_spec in adaptive_windows:
                        try:
                            # Data length check
                            if isinstance(window_spec, tuple):
                                required_len = window_spec[1]
                            else:
                                required_len = window_spec

                            if len(price_data) < required_len:
                                logger.debug(
                                    f"Insufficient data for {ticker} window {window_spec}: {len(price_data)} < {required_len}"
                                )
                                total_errors += 1
                                continue

                            label, score, data_date = classifier.classify_window(
                                window_spec
                            )
                            save_window = window_spec_to_db_value(window_spec)
                            results_processor.add_result(
                                data_date, ticker, save_window, label, score
                            )
                            total_processed += 1

                        except Exception as e:
                            logger.debug(
                                f"Error processing {ticker} window {window_spec}: {e}"
                            )
                            total_errors += 1

                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {e}")
                    total_errors += 1

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
        f"Window distribution: 4800+={window_stats['4800+']}, 2400+={window_stats['2400+']}, "
        f"1200+={window_stats['1200+']}, 480+={window_stats['480+']}, base-only={window_stats['base_only']}"
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
