"""
Tests for ValuationFetcher class.
Tests data fetching, priority logic, metric calculation, and batch saving.
"""

import sqlite3
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestValuationFetcher:
    """Tests for ValuationFetcher class."""

    @pytest.fixture
    def temp_master_db(self):
        """Create a temporary master database."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)
        conn.execute(
            """
            CREATE TABLE stocks_master (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                sector TEXT,
                market TEXT,
                yfinance_symbol TEXT,
                jquants_code TEXT,
                is_active BOOLEAN DEFAULT 1
            )
            """
        )
        # Insert test stocks
        stocks = [
            ("1001", "Company A", "電気機器", "プライム", "1001.T", "10010", 1),
            ("1002", "Company B", "情報通信", "プライム", "1002.T", "10020", 1),
            ("1003", "Company C", "機械", "スタンダード", "1003.T", "10030", 1),
            ("1004", "Company D", "化学", "プライム", "1004.T", "10040", 1),
            ("1005", "Company E", "銀行", "プライム", "1005.T", "10050", 0),  # inactive
        ]
        conn.executemany(
            "INSERT INTO stocks_master (code, name, sector, market, yfinance_symbol, jquants_code, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)",
            stocks,
        )
        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def temp_statements_db(self):
        """Create a temporary statements database."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)
        # Create calculated_fundamentals for PER data
        conn.execute(
            """
            CREATE TABLE calculated_fundamentals (
                code TEXT PRIMARY KEY,
                per REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO calculated_fundamentals (code, per) VALUES (?, ?)",
            [("10010", 10.0), ("10020", 5.0), ("10030", None), ("10040", -15.0)],
        )
        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def fetcher(self, temp_master_db, temp_statements_db):
        """Create a ValuationFetcher instance with test databases."""
        from market_pipeline.yfinance.valuation_fetcher import ValuationFetcher

        return ValuationFetcher(
            master_db_path=temp_master_db,
            statements_db_path=temp_statements_db,
            batch_size=10,
            max_workers=1,
            wait_seconds=0.0,
        )

    def test_initialize_table(self, fetcher):
        """Test that initialize_table creates the yfinance_valuation table."""
        fetcher.initialize_table()

        with sqlite3.connect(fetcher.statements_db_path) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='yfinance_valuation'"
            )
            assert cursor.fetchone() is not None

    def test_initialize_table_idempotent(self, fetcher):
        """Test that initialize_table can be called multiple times."""
        fetcher.initialize_table()
        fetcher.initialize_table()  # Should not raise

    @patch("market_pipeline.yfinance.valuation_fetcher.yf.Ticker")
    def test_fetch_single_success(self, mock_ticker_cls, fetcher):
        """Test successful data fetch from yfinance."""
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker

        # Mock balance sheet
        bs_data = pd.DataFrame(
            {"Cash And Cash Equivalents": [1_000_000_000], "Total Debt": [500_000_000]},
            index=pd.DatetimeIndex(["2025-12-31"]),
        ).T
        mock_ticker.balance_sheet = bs_data

        # Mock info
        mock_ticker.info = {"marketCap": 10_000_000_000, "trailingPE": 12.5}

        result = fetcher.fetch_single("1001.T")

        assert result is not None
        assert result["code"] == "1001"
        assert result["cash_and_equivalents"] == 1_000_000_000
        assert result["interest_bearing_debt"] == 500_000_000
        assert result["market_cap"] == 10_000_000_000
        assert result["per"] == 12.5

    @patch("market_pipeline.yfinance.valuation_fetcher.yf.Ticker")
    def test_fetch_single_no_data(self, mock_ticker_cls, fetcher):
        """Test fetch when yfinance returns no balance sheet data."""
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker
        mock_ticker.balance_sheet = pd.DataFrame()
        mock_ticker.info = {}

        result = fetcher.fetch_single("1001.T")

        assert result is not None
        assert result["code"] == "1001"
        assert result["cash_and_equivalents"] is None
        assert result["interest_bearing_debt"] is None
        assert result["market_cap"] is None
        assert result["per"] is None

    @patch("market_pipeline.yfinance.valuation_fetcher.yf.Ticker")
    def test_fetch_single_error(self, mock_ticker_cls, fetcher):
        """Test fetch when yfinance raises an exception."""
        mock_ticker_cls.side_effect = Exception("API error")

        result = fetcher.fetch_single("1001.T")
        assert result is None

    def test_select_target_codes_priority(self, fetcher):
        """Test that select_target_codes returns codes in priority order:
        unregistered (PER asc) > 90-day stale (PER asc) > oldest updated.
        """
        fetcher.initialize_table()

        # Add some existing valuation data
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        old_date = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
        recent_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

        with sqlite3.connect(fetcher.statements_db_path) as conn:
            # 1002: stale (bs_period_end > 90 days ago), PER=5
            conn.execute(
                "INSERT INTO yfinance_valuation (code, bs_period_end, bs_updated_at) VALUES (?, ?, ?)",
                ("1002", old_date, now),
            )
            # 1004: recent, PER=15
            conn.execute(
                "INSERT INTO yfinance_valuation (code, bs_period_end, bs_updated_at) VALUES (?, ?, ?)",
                ("1004", recent_date, now),
            )
            conn.commit()

        codes = fetcher.select_target_codes()

        # Group 1 (unregistered): 1001(PER=10), 1003(PER=NULL) → 1001 first, 1003 last
        # Group 2 (stale): 1002(PER=5)
        # Group 3 (rest): 1004
        # Note: 1005 is inactive, excluded
        assert "1005" not in codes
        assert len(codes) == 4

        # 1001 should be before 1003 (PER=10 vs NULL)
        assert codes.index("1001") < codes.index("1003")
        # Group 1 (1001, 1003) before Group 2 (1002)
        assert codes.index("1001") < codes.index("1002")
        assert codes.index("1003") < codes.index("1002")
        # Group 2 (1002) before Group 3 (1004)
        assert codes.index("1002") < codes.index("1004")

    def test_select_target_codes_per_null_last(self, fetcher):
        """Test that PER<=0 and PER=NULL stocks are placed at the end of each group."""
        fetcher.initialize_table()

        codes = fetcher.select_target_codes()

        # All 4 active stocks are unregistered (Group 1)
        # PER positive asc: 1002=5, 1001=10 → PER<=0/NULL at end: 1004=-15, 1003=NULL
        assert codes[0] == "1002"  # PER=5 (lowest positive)
        assert codes[1] == "1001"  # PER=10
        # 1004 (PER=-15) and 1003 (PER=NULL) are both in the tail group
        assert set(codes[2:4]) == {"1003", "1004"}

    def test_calculate_metrics_normal(self, fetcher):
        """Test normal metric calculation."""
        row = {
            "code": "1001",
            "cash_and_equivalents": 1_000_000_000,
            "interest_bearing_debt": 500_000_000,
            "market_cap": 10_000_000_000,
            "per": 12.5,
        }
        result = fetcher.calculate_metrics(row)

        # net_cash_ratio = (1B - 0.5B) / 10B = 0.05
        assert result["net_cash_ratio"] == pytest.approx(0.05)
        # cash_neutral_per = 12.5 * (1 - 0.05) = 11.875
        assert result["cash_neutral_per"] == pytest.approx(11.875)

    def test_calculate_metrics_null(self, fetcher):
        """Test metric calculation with NULL inputs."""
        row = {
            "code": "1001",
            "cash_and_equivalents": None,
            "interest_bearing_debt": None,
            "market_cap": 10_000_000_000,
            "per": 12.5,
        }
        result = fetcher.calculate_metrics(row)

        assert result["net_cash_ratio"] is None
        assert result["cash_neutral_per"] is None

    def test_calculate_metrics_zero_market_cap(self, fetcher):
        """Test metric calculation with zero market cap (division by zero guard)."""
        row = {
            "code": "1001",
            "cash_and_equivalents": 1_000_000_000,
            "interest_bearing_debt": 500_000_000,
            "market_cap": 0,
            "per": 12.5,
        }
        result = fetcher.calculate_metrics(row)

        assert result["net_cash_ratio"] is None
        assert result["cash_neutral_per"] is None

    def test_calculate_metrics_none_market_cap(self, fetcher):
        """Test metric calculation with None market cap."""
        row = {
            "code": "1001",
            "cash_and_equivalents": 1_000_000_000,
            "interest_bearing_debt": 500_000_000,
            "market_cap": None,
            "per": 12.5,
        }
        result = fetcher.calculate_metrics(row)

        assert result["net_cash_ratio"] is None
        assert result["cash_neutral_per"] is None

    def test_calculate_metrics_negative_net_cash(self, fetcher):
        """Test metric calculation with negative net cash (debt > cash)."""
        row = {
            "code": "1001",
            "cash_and_equivalents": 500_000_000,
            "interest_bearing_debt": 2_000_000_000,
            "market_cap": 10_000_000_000,
            "per": 12.5,
        }
        result = fetcher.calculate_metrics(row)

        # net_cash_ratio = (0.5B - 2B) / 10B = -0.15
        assert result["net_cash_ratio"] == pytest.approx(-0.15)
        # cash_neutral_per = 12.5 * (1 - (-0.15)) = 12.5 * 1.15 = 14.375
        assert result["cash_neutral_per"] == pytest.approx(14.375)

    def test_calculate_metrics_no_debt(self, fetcher):
        """Test metric calculation when debt is None (treated as 0)."""
        row = {
            "code": "1001",
            "cash_and_equivalents": 1_000_000_000,
            "interest_bearing_debt": None,
            "market_cap": 10_000_000_000,
            "per": 10.0,
        }
        result = fetcher.calculate_metrics(row)

        # net_cash_ratio = (1B - 0) / 10B = 0.1
        assert result["net_cash_ratio"] == pytest.approx(0.1)
        # cash_neutral_per = 10 * (1 - 0.1) = 9.0
        assert result["cash_neutral_per"] == pytest.approx(9.0)

    def test_calculate_metrics_no_per(self, fetcher):
        """Test metric calculation when PER is None."""
        row = {
            "code": "1001",
            "cash_and_equivalents": 1_000_000_000,
            "interest_bearing_debt": 500_000_000,
            "market_cap": 10_000_000_000,
            "per": None,
        }
        result = fetcher.calculate_metrics(row)

        assert result["net_cash_ratio"] == pytest.approx(0.05)
        assert result["cash_neutral_per"] is None

    def test_save_batch(self, fetcher):
        """Test batch saving to yfinance_valuation table."""
        fetcher.initialize_table()

        records = [
            {
                "code": "1001",
                "cash_and_equivalents": 1_000_000_000,
                "interest_bearing_debt": 500_000_000,
                "bs_period_end": "2025-12-31",
                "market_cap": 10_000_000_000,
                "per": 12.5,
                "net_cash_ratio": 0.05,
                "cash_neutral_per": 11.875,
            },
            {
                "code": "1002",
                "cash_and_equivalents": 2_000_000_000,
                "interest_bearing_debt": None,
                "bs_period_end": "2025-12-31",
                "market_cap": 5_000_000_000,
                "per": 8.0,
                "net_cash_ratio": 0.4,
                "cash_neutral_per": 4.8,
            },
        ]

        saved = fetcher.save_batch(records)
        assert saved == 2

        # Verify data in DB
        with sqlite3.connect(fetcher.statements_db_path) as conn:
            rows = conn.execute(
                "SELECT code, cash_and_equivalents, net_cash_ratio FROM yfinance_valuation ORDER BY code"
            ).fetchall()

        assert len(rows) == 2
        assert rows[0][0] == "1001"
        assert rows[0][1] == 1_000_000_000
        assert rows[0][2] == pytest.approx(0.05)

    def test_save_batch_replace(self, fetcher):
        """Test that save_batch replaces existing records."""
        fetcher.initialize_table()

        # Save initial record
        fetcher.save_batch(
            [{"code": "1001", "market_cap": 10_000_000_000, "per": 12.5}]
        )

        # Save updated record
        fetcher.save_batch(
            [{"code": "1001", "market_cap": 15_000_000_000, "per": 10.0}]
        )

        with sqlite3.connect(fetcher.statements_db_path) as conn:
            rows = conn.execute(
                "SELECT market_cap, per FROM yfinance_valuation WHERE code = '1001'"
            ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == 15_000_000_000
        assert rows[0][1] == 10.0

    def test_save_batch_empty(self, fetcher):
        """Test save_batch with empty list."""
        fetcher.initialize_table()
        saved = fetcher.save_batch([])
        assert saved == 0

    @patch("market_pipeline.yfinance.valuation_fetcher.yf.Ticker")
    def test_run_integration(self, mock_ticker_cls, fetcher):
        """Test the full run() method with mocked yfinance."""
        mock_ticker = MagicMock()
        mock_ticker_cls.return_value = mock_ticker

        # Mock balance sheet
        bs_data = pd.DataFrame(
            {"Cash And Cash Equivalents": [1_000_000_000], "Total Debt": [500_000_000]},
            index=pd.DatetimeIndex(["2025-12-31"]),
        ).T
        mock_ticker.balance_sheet = bs_data
        mock_ticker.info = {"marketCap": 10_000_000_000, "trailingPE": 12.5}

        result = fetcher.run(limit=2)

        assert result["success"] >= 0
        assert result["failed"] >= 0
        assert result["elapsed"] >= 0

        # Verify data was saved
        with sqlite3.connect(fetcher.statements_db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM yfinance_valuation").fetchone()[
                0
            ]
        assert count > 0
