"""
Pytest configuration and shared fixtures for stock analysis tests.
"""

import pytest
import numpy as np
import pandas as pd
import sqlite3
import tempfile
import os
from datetime import datetime, timedelta


def create_screener_analysis_db(db_path, *, include_classification=False):
    """Create analysis DB tables used by StockScreener tests.

    Args:
        db_path: Path to the SQLite database file.
        include_classification: If True, also create classification_results table.
    """
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE integrated_scores (
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
    conn.execute(
        """
        CREATE TABLE hl_ratio (
            Date TEXT NOT NULL,
            Code TEXT NOT NULL,
            HlRatio REAL,
            MedianRatio REAL,
            Weeks INTEGER,
            PRIMARY KEY (Date, Code)
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE relative_strength (
            Date TEXT NOT NULL,
            Code TEXT NOT NULL,
            RelativeStrengthPercentage REAL,
            RelativeStrengthIndex REAL,
            PRIMARY KEY (Date, Code)
        )
    """
    )
    if include_classification:
        conn.execute(
            """
            CREATE TABLE classification_results (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                window INTEGER NOT NULL,
                pattern_label TEXT NOT NULL,
                score REAL NOT NULL,
                PRIMARY KEY (date, ticker, window)
            )
        """
        )
    conn.commit()
    conn.close()


def create_screener_statements_db(db_path):
    """Create statements DB tables used by StockScreener tests."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE calculated_fundamentals (
            code TEXT PRIMARY KEY,
            company_name TEXT,
            sector_33 TEXT,
            sector_17 TEXT,
            market_segment TEXT,
            market_cap REAL,
            per REAL,
            pbr REAL,
            dividend_yield REAL,
            roe REAL,
            roa REAL,
            equity_ratio REAL,
            eps REAL,
            bps REAL,
            last_updated TEXT
        )
    """
    )
    conn.commit()
    conn.close()


@pytest.fixture(scope="session")
def sample_stock_codes():
    """Standard set of stock codes for testing"""
    return ["1001", "1002", "1003", "7203", "9984"]


@pytest.fixture(scope="session")
def sample_date_range():
    """Standard date range for testing (1 year)"""
    end_date = datetime(2023, 12, 31)
    start_date = end_date - timedelta(days=365)
    return pd.date_range(start_date, end_date, freq="D")


@pytest.fixture
def mock_jquants_database():
    """Create a mock jquants database with realistic data"""
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    conn = sqlite3.connect(temp_db.name)

    # Create daily_quotes table
    conn.execute("""
    CREATE TABLE daily_quotes (
        Date TEXT NOT NULL,
        Code TEXT NOT NULL,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        AdjustmentClose REAL,
        Volume INTEGER,
        PRIMARY KEY (Date, Code)
    )
    """)

    # Generate realistic stock data
    np.random.seed(42)  # For reproducible tests

    codes = ["1001", "1002", "1003", "7203", "9984"]
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    dates = pd.date_range(start_date, end_date, freq="D")

    for code in codes:
        # Generate base parameters for each stock
        base_price = np.random.uniform(50, 500)
        volatility = np.random.uniform(0.01, 0.03)
        trend = np.random.uniform(-0.0005, 0.001)

        current_price = base_price

        for date in dates:
            # Skip weekends for more realistic data
            if date.weekday() >= 5:
                continue

            # Generate daily price movement
            daily_return = np.random.normal(trend, volatility)
            current_price *= 1 + daily_return

            # Generate OHLC data
            high_factor = 1 + abs(np.random.normal(0, 0.01))
            low_factor = 1 - abs(np.random.normal(0, 0.01))

            open_price = current_price * np.random.uniform(0.99, 1.01)
            high_price = max(current_price, open_price) * high_factor
            low_price = min(current_price, open_price) * low_factor
            close_price = current_price
            adjustment_close = close_price  # Simplified
            volume = int(np.random.uniform(100000, 1000000))

            conn.execute(
                """
            INSERT INTO daily_quotes 
            (Date, Code, Open, High, Low, Close, AdjustmentClose, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    date.strftime("%Y-%m-%d"),
                    code,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    adjustment_close,
                    volume,
                ),
            )

    conn.commit()
    conn.close()

    yield temp_db.name

    # Cleanup
    os.unlink(temp_db.name)


@pytest.fixture
def mock_analysis_results_database():
    """Create a mock analysis results database"""
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    conn = sqlite3.connect(temp_db.name)

    # Create tables for analysis results

    # Chart classification results
    conn.execute("""
    CREATE TABLE IF NOT EXISTS classification_results (
        date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        window INTEGER NOT NULL,
        pattern_label TEXT NOT NULL,
        score REAL NOT NULL,
        PRIMARY KEY (date, ticker, window)
    )
    """)

    # Minervini analysis results
    conn.execute("""
    CREATE TABLE IF NOT EXISTS minervini (
        date TEXT NOT NULL,
        code TEXT NOT NULL,
        close REAL,
        sma50 REAL,
        sma150 REAL,
        sma200 REAL,
        type_1 REAL,
        type_2 REAL,
        type_3 REAL,
        type_4 REAL,
        type_5 REAL,
        type_6 REAL,
        type_7 REAL,
        type_8 REAL,
        PRIMARY KEY (date, code)
    )
    """)

    # Relative strength analysis results
    conn.execute("""
    CREATE TABLE IF NOT EXISTS relative_strength (
        date TEXT NOT NULL,
        code TEXT NOT NULL,
        relative_strength_percentage REAL,
        relative_strength_index REAL,
        PRIMARY KEY (date, code)
    )
    """)

    conn.commit()
    conn.close()

    yield temp_db.name

    # Cleanup
    os.unlink(temp_db.name)


@pytest.fixture
def sample_price_series():
    """Generate a sample price series for testing algorithms"""
    np.random.seed(42)

    # Generate 300 days of realistic price data
    days = 300
    base_price = 100.0
    prices = [base_price]

    for i in range(1, days):
        # Random walk with slight upward bias
        change = np.random.normal(0.001, 0.02)  # 0.1% daily return, 2% volatility
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, 1.0))  # Prevent negative prices

    return np.array(prices)


@pytest.fixture
def sample_ohlc_dataframe():
    """Generate sample OHLC DataFrame for testing"""
    np.random.seed(42)

    dates = pd.date_range("2023-01-01", periods=300, freq="D")
    base_price = 100.0

    data = []
    current_price = base_price

    for date in dates:
        # Generate price movement
        daily_return = np.random.normal(0.001, 0.02)
        current_price *= 1 + daily_return

        # Generate OHLC
        open_price = current_price * np.random.uniform(0.99, 1.01)
        high_price = max(current_price, open_price) * (
            1 + abs(np.random.normal(0, 0.01))
        )
        low_price = min(current_price, open_price) * (
            1 - abs(np.random.normal(0, 0.01))
        )
        close_price = current_price

        data.append(
            {
                "Date": date,
                "Open": open_price,
                "High": high_price,
                "Low": low_price,
                "Close": close_price,
                "AdjustmentClose": close_price,
                "Volume": int(np.random.uniform(100000, 1000000)),
            }
        )

    return pd.DataFrame(data).set_index("Date")


@pytest.fixture
def mock_logging():
    """Mock logging for tests that don't need actual log output"""
    import logging
    from unittest.mock import MagicMock

    mock_logger = MagicMock()

    # Mock common logging methods
    mock_logger.info = MagicMock()
    mock_logger.debug = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    with pytest.MonkeyPatch.context() as m:
        m.setattr(logging, "getLogger", lambda name: mock_logger)
        yield mock_logger


@pytest.fixture
def temp_output_directory():
    """Create a temporary output directory for test files"""
    import tempfile
    import shutil

    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_minervini_criteria():
    """Sample data that meets Minervini criteria for testing"""
    np.random.seed(42)

    # Generate data that should meet most Minervini criteria
    days = 300
    prices = []
    base_price = 50.0  # Start low to show growth

    for i in range(days):
        # Strong upward trend for first 200 days, then consolidation
        if i < 200:
            trend = 0.003  # Strong upward trend
        else:
            trend = 0.0005  # Slight upward trend

        volatility = np.random.normal(0, 0.015)
        base_price *= 1 + trend + volatility
        prices.append(base_price)

    return np.array(prices)


@pytest.fixture(scope="session")
def test_constants():
    """Common test constants"""
    return {
        "DEFAULT_WEEKS": 52,
        "DEFAULT_PERIODS": {
            "SMA_SHORT": 50,
            "SMA_MEDIUM": 150,
            "SMA_LONG": 200,
            "RSP_PERIOD": 200,
            "MIN_DATA_POINTS": 260,
        },
        "TEST_CODES": ["1001", "1002", "1003"],
        "TEST_DATE": "2023-12-01",
        "TEST_DATE_RANGE": {"start": "2023-01-01", "end": "2023-12-31"},
    }
