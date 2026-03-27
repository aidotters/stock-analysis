"""
Tests for StockScreener class.
Tests filtering, rank changes, and history functionality.
"""

import pytest
import pandas as pd
import sqlite3
import tempfile
import os
from datetime import datetime, timedelta


class TestStockScreener:
    """Tests for StockScreener class."""

    @pytest.fixture
    def temp_analysis_db(self):
        """Create a temporary analysis database with integrated_scores table."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)

        # Create integrated_scores table
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

        # Create classification_results table
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

        # Create hl_ratio table for additional data
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

        # Create relative_strength table
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

        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def temp_statements_db(self):
        """Create a temporary statements database with calculated_fundamentals."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)

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
                eps REAL,
                bps REAL,
                last_updated TEXT
            )
        """
        )

        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def populated_databases(self, temp_analysis_db, temp_statements_db):
        """Populate databases with test data."""
        # Populate analysis database
        conn = sqlite3.connect(temp_analysis_db)

        # Insert integrated_scores data for multiple dates
        test_date = "2026-02-01"
        codes = ["1001", "1002", "1003", "1004", "1005"]

        for i, code in enumerate(codes):
            composite_score = 90 - i * 10  # 90, 80, 70, 60, 50
            conn.execute(
                """
                INSERT INTO integrated_scores
                (Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (test_date, code, composite_score, i + 1, i + 1, i + 1),
            )

            # Add hl_ratio data
            conn.execute(
                """
                INSERT INTO hl_ratio (Date, Code, HlRatio, MedianRatio, Weeks)
                VALUES (?, ?, ?, ?, ?)
            """,
                (test_date, code, 95 - i * 5, 50.0, 52),
            )

            # Add relative_strength data
            conn.execute(
                """
                INSERT INTO relative_strength
                (Date, Code, RelativeStrengthPercentage, RelativeStrengthIndex)
                VALUES (?, ?, ?, ?)
            """,
                (test_date, code, 85 - i * 5, 70 - i * 5),
            )

        # Add historical data for rank_changes testing
        for days_back in range(1, 8):
            hist_date = (datetime(2026, 2, 1) - timedelta(days=days_back)).strftime(
                "%Y-%m-%d"
            )
            for i, code in enumerate(codes):
                # Simulate rank changes over time
                if code == "1003":
                    # Code 1003 improves rank significantly
                    rank = max(1, 5 - days_back)
                else:
                    rank = i + 1
                conn.execute(
                    """
                    INSERT INTO integrated_scores
                    (Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (hist_date, code, 80 - rank * 5, rank, rank, rank),
                )

        # Add classification_results
        for code in codes[:3]:
            conn.execute(
                """
                INSERT INTO classification_results (date, ticker, window, pattern_label, score)
                VALUES (?, ?, ?, ?, ?)
            """,
                (test_date, code, 60, "上昇", 0.85),
            )
            conn.execute(
                """
                INSERT INTO classification_results (date, ticker, window, pattern_label, score)
                VALUES (?, ?, ?, ?, ?)
            """,
                (test_date, code, 120, "横ばい", 0.75),
            )

        conn.commit()
        conn.close()

        # Populate statements database
        conn = sqlite3.connect(temp_statements_db)
        for i, code in enumerate(codes):
            market_cap = (5 - i) * 1000000000  # 5B, 4B, 3B, 2B, 1B
            per = 10 + i * 2  # 10, 12, 14, 16, 18
            pbr = 1.0 + i * 0.3  # 1.0, 1.3, 1.6, 1.9, 2.2
            roe = 20 - i * 2  # 20, 18, 16, 14, 12
            div_yield = 3.0 - i * 0.5  # 3.0, 2.5, 2.0, 1.5, 1.0

            conn.execute(
                """
                INSERT INTO calculated_fundamentals
                (code, company_name, sector_33, market_cap, per, pbr, dividend_yield, roe)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    code,
                    f"Company {code}",
                    "電気機器",
                    market_cap,
                    per,
                    pbr,
                    div_yield,
                    roe,
                ),
            )
        conn.commit()
        conn.close()

        return temp_analysis_db, temp_statements_db

    @pytest.fixture
    def screener(self, populated_databases):
        """Create a StockScreener instance with populated databases."""
        from technical_tools.screener import StockScreener

        analysis_db, statements_db = populated_databases
        return StockScreener(
            analysis_db_path=analysis_db, statements_db_path=statements_db
        )

    # Filter tests
    def test_filter_basic(self, screener):
        """Test basic filter without parameters."""
        results = screener.filter()
        assert isinstance(results, pd.DataFrame)
        assert len(results) > 0

    def test_filter_composite_score_min(self, screener):
        """Test filter with composite_score_min."""
        results = screener.filter(composite_score_min=75.0)
        assert all(results["composite_score"] >= 75.0)

    def test_filter_composite_score_max(self, screener):
        """Test filter with composite_score_max."""
        results = screener.filter(composite_score_max=80.0)
        assert all(results["composite_score"] <= 80.0)

    def test_filter_hl_ratio_min(self, screener):
        """Test filter with hl_ratio_min."""
        results = screener.filter(hl_ratio_min=80.0)
        assert all(results["HlRatio"] >= 80.0)

    def test_filter_hl_ratio_max(self, screener):
        """Test filter with hl_ratio_max."""
        results = screener.filter(hl_ratio_max=90.0)
        assert all(results["HlRatio"] <= 90.0)

    def test_filter_rsi_min(self, screener):
        """Test filter with rsi_min."""
        results = screener.filter(rsi_min=60.0)
        assert all(results["RelativeStrengthIndex"] >= 60.0)

    def test_filter_rsi_max(self, screener):
        """Test filter with rsi_max."""
        results = screener.filter(rsi_max=65.0)
        assert all(results["RelativeStrengthIndex"] <= 65.0)

    def test_filter_market_cap_min(self, screener):
        """Test filter with market_cap_min."""
        results = screener.filter(market_cap_min=2000000000)  # 2B
        assert all(results["marketCap"] >= 2000000000)

    def test_filter_market_cap_max(self, screener):
        """Test filter with market_cap_max."""
        results = screener.filter(market_cap_max=3000000000)  # 3B
        assert all(results["marketCap"] <= 3000000000)

    def test_filter_per_min(self, screener):
        """Test filter with per_min."""
        results = screener.filter(per_min=12.0)
        assert all(results["trailingPE"] >= 12.0)

    def test_filter_per_max(self, screener):
        """Test filter with per_max."""
        results = screener.filter(per_max=14.0)
        assert all(results["trailingPE"] <= 14.0)

    def test_filter_pbr_max(self, screener):
        """Test filter with pbr_max."""
        results = screener.filter(pbr_max=1.5)
        assert all(results["priceToBook"] <= 1.5)

    def test_filter_roe_min(self, screener):
        """Test filter with roe_min."""
        results = screener.filter(roe_min=16.0)
        assert all(results["returnOnEquity"] >= 16.0)

    def test_filter_dividend_yield_min(self, screener):
        """Test filter with dividend_yield_min."""
        results = screener.filter(dividend_yield_min=2.0)
        assert all(results["dividendYield"] >= 2.0)

    def test_filter_pattern_window(self, screener):
        """Test filter with pattern_window."""
        results = screener.filter(pattern_window=60)
        assert len(results) > 0
        # Results should only include stocks with pattern data for window 60

    def test_filter_pattern_labels(self, screener):
        """Test filter with pattern_labels."""
        results = screener.filter(pattern_window=60, pattern_labels=["上昇"])
        assert len(results) > 0

    def test_filter_limit(self, screener):
        """Test filter with limit."""
        results = screener.filter(limit=2)
        assert len(results) <= 2

    def test_filter_specific_date(self, screener):
        """Test filter with specific date."""
        results = screener.filter(date="2026-02-01")
        assert all(results["Date"] == "2026-02-01")

    def test_filter_combined_criteria(self, screener):
        """Test filter with multiple criteria."""
        results = screener.filter(
            composite_score_min=70.0, hl_ratio_min=85.0, market_cap_min=1000000000
        )
        assert all(results["composite_score"] >= 70.0)
        assert all(results["HlRatio"] >= 85.0)
        assert all(results["marketCap"] >= 1000000000)

    def test_filter_no_results(self, screener):
        """Test filter with impossible criteria."""
        results = screener.filter(composite_score_min=999.0)
        assert isinstance(results, pd.DataFrame)
        assert len(results) == 0

    # rank_changes tests
    def test_rank_changes_basic(self, screener):
        """Test basic rank_changes."""
        results = screener.rank_changes(days=7)
        assert isinstance(results, pd.DataFrame)
        # Results may be empty if no significant rank changes in test data

    def test_rank_changes_direction_up(self, screener):
        """Test rank_changes with direction='up'."""
        results = screener.rank_changes(days=7, direction="up")
        # All changes should be positive (rank improved = lower number)
        if len(results) > 0:
            assert all(results["rank_change"] > 0)

    def test_rank_changes_direction_down(self, screener):
        """Test rank_changes with direction='down'."""
        results = screener.rank_changes(days=7, direction="down")
        # All changes should be negative (rank worsened = higher number)
        if len(results) > 0:
            assert all(results["rank_change"] < 0)

    def test_rank_changes_min_change(self, screener):
        """Test rank_changes with min_change filter."""
        results = screener.rank_changes(days=7, min_change=2)
        if len(results) > 0:
            assert all(abs(results["rank_change"]) >= 2)

    def test_rank_changes_limit(self, screener):
        """Test rank_changes with limit."""
        results = screener.rank_changes(days=7, limit=2)
        assert len(results) <= 2

    def test_rank_changes_metric(self, screener):
        """Test rank_changes with different metrics."""
        results_composite = screener.rank_changes(metric="composite_score", days=7)
        results_hl = screener.rank_changes(metric="hl_ratio", days=7)
        results_rsp = screener.rank_changes(metric="rsp", days=7)

        # All should return DataFrames
        assert isinstance(results_composite, pd.DataFrame)
        assert isinstance(results_hl, pd.DataFrame)
        assert isinstance(results_rsp, pd.DataFrame)

    def test_rank_changes_invalid_metric(self, screener):
        """Test rank_changes raises ValueError for invalid metric."""
        with pytest.raises(ValueError) as exc_info:
            screener.rank_changes(metric="invalid_metric", days=7)

        assert "Invalid metric" in str(exc_info.value)
        assert "invalid_metric" in str(exc_info.value)

    def test_rank_changes_invalid_metric_empty_string(self, screener):
        """Test rank_changes raises ValueError for empty metric string."""
        with pytest.raises(ValueError) as exc_info:
            screener.rank_changes(metric="", days=7)

        assert "Invalid metric" in str(exc_info.value)

    # history tests
    def test_history_basic(self, screener):
        """Test basic history retrieval."""
        results = screener.history("1001", days=30)
        assert isinstance(results, pd.DataFrame)
        assert len(results) > 0
        assert all(results["Code"] == "1001")

    def test_history_limited_days(self, screener):
        """Test history with days limit."""
        results = screener.history("1001", days=3)
        assert len(results) <= 3

    def test_history_nonexistent_code(self, screener):
        """Test history for non-existent code."""
        results = screener.history("9999", days=30)
        assert isinstance(results, pd.DataFrame)
        assert len(results) == 0

    def test_history_includes_required_columns(self, screener):
        """Test that history includes all required columns."""
        results = screener.history("1001", days=30)
        required_columns = [
            "Date",
            "Code",
            "composite_score",
            "composite_score_rank",
        ]
        for col in required_columns:
            assert col in results.columns


class TestScreenerFilter:
    """Tests for ScreenerFilter dataclass."""

    def test_screener_filter_defaults(self):
        """Test ScreenerFilter default values."""
        from technical_tools.screener import ScreenerFilter

        config = ScreenerFilter()
        assert config.date is None
        assert config.composite_score_min is None
        assert config.limit == 100

    def test_screener_filter_with_values(self):
        """Test ScreenerFilter with specified values."""
        from technical_tools.screener import ScreenerFilter

        config = ScreenerFilter(
            composite_score_min=70.0,
            hl_ratio_min=80.0,
            market_cap_min=100_000_000_000,
            limit=50,
        )
        assert config.composite_score_min == 70.0
        assert config.hl_ratio_min == 80.0
        assert config.market_cap_min == 100_000_000_000
        assert config.limit == 50

    def test_screener_filter_to_dict(self):
        """Test ScreenerFilter.to_dict() method."""
        from technical_tools.screener import ScreenerFilter

        config = ScreenerFilter(
            composite_score_min=70.0,
            per_max=15.0,
        )
        d = config.to_dict()
        assert d["composite_score_min"] == 70.0
        assert d["per_max"] == 15.0
        assert d["limit"] == 100
        # None values should not be in dict
        assert "composite_score_max" not in d

    def test_screener_filter_to_dict_excludes_none(self):
        """Test that to_dict excludes None values."""
        from technical_tools.screener import ScreenerFilter

        config = ScreenerFilter()
        d = config.to_dict()
        # Only limit should be present (has default value of 100)
        assert "limit" in d
        assert "composite_score_min" not in d


class TestStockScreenerWithFilter:
    """Tests for StockScreener.filter() with ScreenerFilter object."""

    @pytest.fixture
    def temp_analysis_db(self):
        """Create a temporary analysis database with integrated_scores table."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)

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

        # Insert test data
        test_date = "2026-02-01"
        for i, code in enumerate(["1001", "1002", "1003"]):
            score = 90 - i * 10
            conn.execute(
                """
                INSERT INTO integrated_scores
                (Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (test_date, code, score, i + 1, i + 1, i + 1),
            )
            conn.execute(
                """
                INSERT INTO hl_ratio (Date, Code, HlRatio, MedianRatio, Weeks)
                VALUES (?, ?, ?, ?, ?)
            """,
                (test_date, code, 95 - i * 5, 50.0, 52),
            )
            conn.execute(
                """
                INSERT INTO relative_strength
                (Date, Code, RelativeStrengthPercentage, RelativeStrengthIndex)
                VALUES (?, ?, ?, ?)
            """,
                (test_date, code, 85 - i * 5, 70 - i * 5),
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
        conn.execute(
            """
            CREATE TABLE calculated_fundamentals (
                code TEXT PRIMARY KEY,
                company_name TEXT,
                sector_33 TEXT,
                market_cap REAL,
                per REAL,
                pbr REAL,
                dividend_yield REAL,
                roe REAL
            )
        """
        )
        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def screener(self, temp_analysis_db, temp_statements_db):
        """Create a StockScreener instance."""
        from technical_tools.screener import StockScreener

        return StockScreener(
            analysis_db_path=temp_analysis_db, statements_db_path=temp_statements_db
        )

    def test_filter_with_screener_filter_object(self, screener):
        """Test filter() accepts ScreenerFilter object."""
        from technical_tools.screener import ScreenerFilter

        config = ScreenerFilter(composite_score_min=75.0)
        results = screener.filter(config)

        assert isinstance(results, pd.DataFrame)
        assert len(results) > 0
        assert all(results["composite_score"] >= 75.0)

    def test_filter_with_screener_filter_multiple_params(self, screener):
        """Test filter() with ScreenerFilter using multiple parameters."""
        from technical_tools.screener import ScreenerFilter

        config = ScreenerFilter(
            composite_score_min=70.0,
            hl_ratio_min=85.0,
            limit=10,
        )
        results = screener.filter(config)

        assert isinstance(results, pd.DataFrame)
        if len(results) > 0:
            assert all(results["composite_score"] >= 70.0)
            assert all(results["HlRatio"] >= 85.0)
        assert len(results) <= 10

    def test_filter_keyword_args_still_work(self, screener):
        """Test that keyword arguments still work (backward compatibility)."""
        results = screener.filter(composite_score_min=75.0)

        assert isinstance(results, pd.DataFrame)
        assert len(results) > 0
        assert all(results["composite_score"] >= 75.0)

    def test_filter_screener_filter_overrides_kwargs(self, screener):
        """Test that ScreenerFilter takes precedence when both are provided."""
        from technical_tools.screener import ScreenerFilter

        # ScreenerFilter sets min to 75, kwarg would set to 60
        config = ScreenerFilter(composite_score_min=75.0)
        results = screener.filter(config, composite_score_min=60.0)

        # ScreenerFilter should take precedence
        assert all(results["composite_score"] >= 75.0)


class TestStockScreenerValuation:
    """Tests for StockScreener yfinance_valuation filters."""

    @pytest.fixture
    def temp_analysis_db(self):
        """Create a temporary analysis database."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)
        conn.execute(
            """
            CREATE TABLE integrated_scores (
                Date TEXT NOT NULL,
                Code TEXT NOT NULL,
                composite_score REAL,
                composite_score_rank INTEGER,
                hl_ratio_rank INTEGER,
                rsp_rank INTEGER,
                PRIMARY KEY (Date, Code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE hl_ratio (
                Date TEXT NOT NULL, Code TEXT NOT NULL,
                HlRatio REAL, MedianRatio REAL, Weeks INTEGER,
                PRIMARY KEY (Date, Code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE relative_strength (
                Date TEXT NOT NULL, Code TEXT NOT NULL,
                RelativeStrengthPercentage REAL, RelativeStrengthIndex REAL,
                PRIMARY KEY (Date, Code)
            )
            """
        )

        test_date = "2026-03-01"
        # Use 5-digit codes to match real integrated_scores format
        for i, code in enumerate(["20010", "20020", "20030", "20040"]):
            score = 90 - i * 10
            conn.execute(
                "INSERT INTO integrated_scores (Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank) VALUES (?, ?, ?, ?, ?, ?)",
                (test_date, code, score, i + 1, i + 1, i + 1),
            )
        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def temp_statements_db_with_valuation(self):
        """Create statements DB with yfinance_valuation table (4-digit codes like real data)."""
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()

        conn = sqlite3.connect(temp_db.name)
        conn.execute(
            """
            CREATE TABLE calculated_fundamentals (
                code TEXT PRIMARY KEY, company_name TEXT, sector_33 TEXT,
                market_cap REAL, per REAL, pbr REAL, dividend_yield REAL, roe REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE yfinance_valuation (
                code TEXT PRIMARY KEY,
                cash_and_equivalents REAL,
                interest_bearing_debt REAL,
                bs_period_end TEXT,
                market_cap REAL,
                per REAL,
                net_cash_ratio REAL,
                cash_neutral_per REAL,
                bs_updated_at TEXT,
                updated_at TEXT
            )
            """
        )
        # Insert valuation data
        valuations = [
            ("2001", 0.5, 5.0),  # High net cash ratio, low CN-PER
            ("2002", 0.3, 8.0),  # Medium
            ("2003", 0.1, 12.0),  # Low net cash ratio
            ("2004", -0.1, 15.0),  # Negative net cash ratio
        ]
        for code, ncr, cnper in valuations:
            conn.execute(
                "INSERT INTO yfinance_valuation (code, net_cash_ratio, cash_neutral_per, market_cap, per) VALUES (?, ?, ?, ?, ?)",
                (code, ncr, cnper, 10_000_000_000, 10.0),
            )
        conn.commit()
        conn.close()

        yield temp_db.name
        os.unlink(temp_db.name)

    @pytest.fixture
    def screener_with_valuation(
        self, temp_analysis_db, temp_statements_db_with_valuation
    ):
        from technical_tools.screener import StockScreener

        return StockScreener(
            analysis_db_path=temp_analysis_db,
            statements_db_path=temp_statements_db_with_valuation,
        )

    def test_filter_net_cash_ratio_min(self, screener_with_valuation):
        """Test filtering by net_cash_ratio_min."""
        results = screener_with_valuation.filter(net_cash_ratio_min=0.3)
        assert len(results) == 2  # 2001 (0.5) and 2002 (0.3)
        assert all(results["net_cash_ratio"] >= 0.3)

    def test_filter_net_cash_ratio_max(self, screener_with_valuation):
        """Test filtering by net_cash_ratio_max."""
        results = screener_with_valuation.filter(net_cash_ratio_max=0.1)
        assert len(results) == 2  # 2003 (0.1) and 2004 (-0.1)
        assert all(results["net_cash_ratio"] <= 0.1)

    def test_filter_cash_neutral_per_max(self, screener_with_valuation):
        """Test filtering by cash_neutral_per_max."""
        results = screener_with_valuation.filter(cash_neutral_per_max=10.0)
        assert len(results) == 2  # 2001 (5.0) and 2002 (8.0)
        assert all(results["cash_neutral_per"] <= 10.0)

    def test_filter_cash_neutral_per_min(self, screener_with_valuation):
        """Test filtering by cash_neutral_per_min."""
        results = screener_with_valuation.filter(cash_neutral_per_min=10.0)
        assert len(results) == 2  # 2003 (12.0) and 2004 (15.0)
        assert all(results["cash_neutral_per"] >= 10.0)

    def test_filter_combined_with_existing(self, screener_with_valuation):
        """Test valuation filters combined with existing filters."""
        results = screener_with_valuation.filter(
            composite_score_min=70.0,
            net_cash_ratio_min=0.3,
        )
        assert len(results) > 0
        assert all(results["composite_score"] >= 70.0)
        assert all(results["net_cash_ratio"] >= 0.3)

    def test_filter_no_valuation_table(self, temp_analysis_db):
        """Test that filter works when yfinance_valuation table does not exist."""
        from technical_tools.screener import StockScreener

        # Create statements DB without valuation table
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()
        conn = sqlite3.connect(temp_db.name)
        conn.execute(
            "CREATE TABLE calculated_fundamentals (code TEXT PRIMARY KEY, per REAL)"
        )
        conn.commit()
        conn.close()

        # Insert some data into analysis DB
        conn = sqlite3.connect(temp_analysis_db)
        conn.execute(
            "INSERT OR IGNORE INTO integrated_scores (Date, Code, composite_score, composite_score_rank, hl_ratio_rank, rsp_rank) VALUES ('2026-03-01', '3001', 80.0, 1, 1, 1)"
        )
        conn.commit()
        conn.close()

        screener = StockScreener(
            analysis_db_path=temp_analysis_db,
            statements_db_path=temp_db.name,
        )

        # Should not raise, just log warning and return results without valuation filters
        results = screener.filter(net_cash_ratio_min=0.3)
        assert isinstance(results, pd.DataFrame)

        os.unlink(temp_db.name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
