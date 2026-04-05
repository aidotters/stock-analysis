"""
Pytest tests for chart_classification.py
"""

import os
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from market_pipeline.analysis.chart_classification import (
    ChartClassifier,
    OptimizedChartClassifier,
    BatchDataLoader,
    BatchResultsProcessor,
    DatabaseManager,
    get_all_tickers,
    get_adaptive_windows,
    map_to_standard_window,
    map_slice_to_standard,
    window_spec_to_db_value,
    db_value_to_window_spec,
    init_results_db,
    save_result_to_db,
    main_sample,
    main,
    MIN_CORRELATION_THRESHOLD,
)

# --- Fixtures ---


@pytest.fixture
def mock_db_connections(mocker):
    """Mocks all database interactions (read and write)."""
    # Mock for sqlite3.connect
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = None  # Ensure execute returns something
    mock_conn.commit.return_value = None  # Mock commit as well

    # Explicitly set __enter__ and __exit__ for the context manager
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False  # Indicate no exception was handled

    mocker.patch("sqlite3.connect", return_value=mock_conn)

    # Mock for pandas.read_sql_query
    mock_df_stock = pd.DataFrame(
        {
            "Date": pd.to_datetime(pd.date_range(start="2024-01-01", periods=300)),
            "AdjustmentClose": np.linspace(100, 150, 300),
        }
    )
    mock_df_master = pd.DataFrame({"jquants_code": ["101", "102", "103"]})

    def mock_read_sql_query(query, conn, params=None, parse_dates=None):
        if "FROM daily_quotes" in query:
            return mock_df_stock
        elif "FROM stocks_master" in query:
            return mock_df_master
        return pd.DataFrame()  # Default empty dataframe

    mocker.patch("pandas.read_sql_query", side_effect=mock_read_sql_query)

    return mock_conn


@pytest.fixture
def classifier_instance(mock_db_connections):
    """Returns a standard ChartClassifier instance for testing."""
    # The mock_db_connections fixture already sets up the necessary mocks
    return ChartClassifier(ticker="99999", window=30)


# --- Test Cases ---


def test_chart_classifier_initialization(classifier_instance):
    """Test if the ChartClassifier initializes correctly."""
    assert classifier_instance.ticker == "99999"
    assert classifier_instance.window == 30
    assert len(classifier_instance.price_data) > 0
    assert len(classifier_instance.templates_manual) == 9
    assert "上昇" in classifier_instance.templates_manual


def test_initialization_not_enough_data(mock_db_connections):
    """Test that initialization raises ValueError if there is not enough data."""
    # Override the mock_df_stock for this specific test
    # We need to patch pandas.read_sql_query specifically for this test
    with patch(
        "pandas.read_sql_query",
        return_value=pd.DataFrame(
            {
                "Date": pd.to_datetime(pd.date_range(start="2024-01-01", periods=10)),
                "AdjustmentClose": np.linspace(100, 150, 10),
            }
        ),
    ):
        with pytest.raises(ValueError, match="Not enough data for ticker"):
            ChartClassifier(ticker="12345", window=20)


def test_normalize():
    """Test the static _normalize method with log transform."""
    arr = np.array([10, 20, 30, 40, 50])
    normalized = ChartClassifier._normalize(arr)
    assert np.isclose(normalized.min(), 0.0)
    assert np.isclose(normalized.max(), 1.0)
    # Log transform: values should be monotonically increasing
    assert all(normalized[i] < normalized[i + 1] for i in range(len(normalized) - 1))


def test_normalize_log_reduces_skew():
    """Test that log normalization reduces distortion from extreme spikes."""
    # Simulate sharp spike: 100→10000→100000
    arr = np.array([100, 200, 300, 400, 500, 10000, 100000])
    normalized = ChartClassifier._normalize(arr)
    # With log transform, mid-range values should NOT be compressed near 0
    # (without log, values 100-500 would all map to ~0.0-0.004)
    assert normalized[2] > 0.1, "Mid-range values should not be compressed near 0"


def test_classify_latest(classifier_instance):
    """Test the classification of the latest window of data."""
    # Create price data that perfectly matches the '上昇' pattern with date index
    perfect_rise = np.linspace(100, 200, 30)
    date_index = pd.date_range(start="2024-01-01", periods=30)
    classifier_instance.price_data = pd.Series(perfect_rise, index=date_index)

    label, score, latest_date = classifier_instance.classify_latest()

    assert label == "上昇"
    assert score > 0.7  # Log transform changes exact correlation
    assert latest_date == "2024-01-30"


def test_save_classification_plot(mocker, classifier_instance):
    """Test that the plot saving function calls the correct file system and plotting methods."""
    mock_makedirs = mocker.patch("os.makedirs")
    mock_savefig = mocker.patch("matplotlib.pyplot.savefig")
    mocker.patch("matplotlib.pyplot.close")  # Don't need to test this, just mock it

    output_dir = "/tmp/test_output"
    classifier_instance.save_classification_plot("上昇", 0.95, output_dir)

    mock_makedirs.assert_called_once_with(output_dir, exist_ok=True)
    expected_path = os.path.join(output_dir, "99999_window30_上昇.png")
    mock_savefig.assert_called_once_with(expected_path)


# --- Test Database and Main Functions ---


def test_get_all_tickers(mock_db_connections):
    """Test fetching all tickers from the master DB."""
    # mock_db_connections already sets up pandas.read_sql_query for master DB
    tickers = get_all_tickers("dummy/master.db")
    assert tickers == ["101", "102", "103"]


def test_init_results_db(mock_db_connections):
    """Test the initialization of the results database."""
    init_results_db("dummy/results.db")
    cursor = mock_db_connections.cursor()
    # Check that the CREATE TABLE query was executed
    cursor.execute.assert_called_once()
    assert "CREATE TABLE IF NOT EXISTS" in cursor.execute.call_args[0][0]


def test_save_result_to_db(mock_db_connections):
    """Test saving a single result to the database."""
    save_result_to_db("dummy/results.db", "2024-07-11", "12345", 60, "調整", 0.88)
    cursor = mock_db_connections.cursor()
    cursor.execute.assert_called_once()
    sql, params = cursor.execute.call_args[0]
    assert "INSERT OR REPLACE INTO" in sql
    assert params == ("2024-07-11", "12345", 60, "調整", 0.88)


@patch("market_pipeline.analysis.chart_classification.OptimizedChartClassifier")
def test_main_sample(MockClassifier, mock_db_connections):
    """Test the main_sample function to ensure it loops and calls correctly."""
    # Mock the instance methods
    mock_instance = MagicMock()
    mock_instance.classify_latest.return_value = ("上昇", 0.99, "2024-01-01")
    MockClassifier.return_value = mock_instance

    main_sample()

    # Check if the classifier was instantiated for all tickers and windows
    tickers = ["74530", "99840", "67580"]
    windows = [20, 60, 120, 240]
    assert MockClassifier.call_count == len(tickers) * len(windows)

    # Check if the plot saving method was called for each
    assert mock_instance.save_classification_plot.call_count == len(tickers) * len(
        windows
    )


@pytest.mark.skip(
    reason="main_full_run has been replaced by main_full_run_optimized with different architecture"
)
def test_main_full_run(mock_db_connections):
    """Test the main_full_run function - SKIPPED due to architecture change."""
    pass


@pytest.mark.parametrize(
    "mode, expected_func",
    [
        ("sample", "main_sample"),
        ("sample-adaptive", "main_sample_adaptive"),
        ("full", "main_full_run_optimized"),
        ("full-optimized", "main_full_run_optimized"),
    ],
)
def test_main_argparse_dispatch(mocker, mode, expected_func):
    """Test that the correct main function is called based on the --mode arg."""
    # Patch the actual main functions that `main()` will call
    mock_main_sample = mocker.patch(
        "market_pipeline.analysis.chart_classification.main_sample"
    )
    mock_main_sample_adaptive = mocker.patch(
        "market_pipeline.analysis.chart_classification.main_sample_adaptive"
    )
    mock_main_full_run_optimized = mocker.patch(
        "market_pipeline.analysis.chart_classification.main_full_run_optimized"
    )

    # Simulate command-line arguments
    mocker.patch("sys.argv", ["script_name", "--mode", mode])

    # Call the top-level main function that parses args and dispatches
    main()

    if expected_func == "main_sample":
        mock_main_sample.assert_called_once()
        mock_main_sample_adaptive.assert_not_called()
        mock_main_full_run_optimized.assert_not_called()
    elif expected_func == "main_sample_adaptive":
        mock_main_sample.assert_not_called()
        mock_main_sample_adaptive.assert_called_once()
        mock_main_full_run_optimized.assert_not_called()
    else:  # main_full_run_optimized (for both 'full' and 'full-optimized')
        mock_main_sample.assert_not_called()
        mock_main_sample_adaptive.assert_not_called()
        mock_main_full_run_optimized.assert_called_once()


# --- Test get_adaptive_windows function ---


class TestGetAdaptiveWindows:
    """Tests for the get_adaptive_windows utility function with slice windows."""

    def test_short_data_returns_filtered_base_with_full_period(self):
        """Data less than 240 days should return filtered base windows plus full-period."""
        windows = get_adaptive_windows(200)
        assert windows == [20, 60, 120, 200]

    def test_base_only_no_slices(self):
        """Data length 240 should return base windows only (no slices)."""
        windows = get_adaptive_windows(240)
        assert windows == [20, 60, 120, 240]

    def test_medium_data_includes_first_slice_and_partial_second(self):
        """Data length 500 should include (240, 480) + partial (480, 500)."""
        windows = get_adaptive_windows(500)
        assert windows == [20, 60, 120, 240, (240, 480), (480, 500)]

    def test_long_data_includes_two_slices(self):
        """Data >= 1200 days should include first two slice windows."""
        windows = get_adaptive_windows(1200)
        assert windows == [20, 60, 120, 240, (240, 480), (480, 1200)]

    def test_very_long_data_includes_three_slices(self):
        """Data >= 2400 days should include three slice windows."""
        windows = get_adaptive_windows(2400)
        assert windows == [20, 60, 120, 240, (240, 480), (480, 1200), (1200, 2400)]

    def test_max_data_includes_all_slices(self):
        """Data >= 4800 days should include all slice windows."""
        windows = get_adaptive_windows(4800)
        assert windows == [
            20,
            60,
            120,
            240,
            (240, 480),
            (480, 1200),
            (1200, 2400),
            (2400, 4800),
        ]

    def test_boundary_480(self):
        """Test exact boundary at 480 days — first slice should appear."""
        windows = get_adaptive_windows(480)
        assert (240, 480) in windows

    def test_boundary_479_partial_slice(self):
        """Test just below 480 boundary — partial slice (240, 479) should appear."""
        windows = get_adaptive_windows(479)
        assert (240, 479) in windows
        assert (240, 480) not in windows

    def test_boundary_241_partial_slice(self):
        """Data length 241 — partial slice (240, 241) should appear."""
        windows = get_adaptive_windows(241)
        assert (240, 241) in windows

    def test_boundary_240_no_slice(self):
        """Data length 240 — no slice (start must be strictly less than data length)."""
        windows = get_adaptive_windows(240)
        assert all(not isinstance(w, tuple) for w in windows)

    def test_partial_slice_1199(self):
        """Data 1199: full (240,480) + partial (480,1199)."""
        windows = get_adaptive_windows(1199)
        assert (240, 480) in windows
        assert (480, 1199) in windows
        assert (480, 1200) not in windows

    def test_partial_slice_3600(self):
        """Data 3600: three full slices + partial (2400, 3600)."""
        windows = get_adaptive_windows(3600)
        assert (240, 480) in windows
        assert (480, 1200) in windows
        assert (1200, 2400) in windows
        assert (2400, 3600) in windows
        assert (2400, 4800) not in windows


# --- Test map_to_standard_window function ---


class TestMapToStandardWindow:
    """Tests for the map_to_standard_window utility function (cumulative only)."""

    def test_exact_standard_window(self):
        assert map_to_standard_window(20) == 20
        assert map_to_standard_window(60) == 60
        assert map_to_standard_window(240) == 240

    def test_custom_window_maps_to_next_larger(self):
        assert map_to_standard_window(200) == 240
        assert map_to_standard_window(25) == 60
        assert map_to_standard_window(156) == 240

    def test_exceeding_cumulative_max_returns_none(self):
        """Values beyond cumulative range return None (use window_spec_to_db_value for slices)."""
        assert map_to_standard_window(300) is None
        assert map_to_standard_window(5000) is None

    def test_smallest_possible(self):
        assert map_to_standard_window(1) == 20


class TestWindowSpecDbConversion:
    """Tests for window_spec_to_db_value and db_value_to_window_spec."""

    def test_cumulative_to_db(self):
        assert window_spec_to_db_value(20) == 20
        assert window_spec_to_db_value(240) == 240

    def test_slice_to_db(self):
        assert window_spec_to_db_value((240, 480)) == 2400480
        assert window_spec_to_db_value((480, 1200)) == 4801200
        assert window_spec_to_db_value((1200, 2400)) == 12002400
        assert window_spec_to_db_value((2400, 4800)) == 24004800

    def test_partial_slice_maps_to_standard_db_value(self):
        """Non-standard slice windows should map to their standard DB value."""
        assert window_spec_to_db_value((240, 479)) == 2400480
        assert window_spec_to_db_value((480, 1199)) == 4801200
        assert window_spec_to_db_value((1200, 2000)) == 12002400
        assert window_spec_to_db_value((2400, 3600)) == 24004800

    def test_db_to_cumulative(self):
        assert db_value_to_window_spec(20) == 20
        assert db_value_to_window_spec(240) == 240

    def test_db_to_slice(self):
        assert db_value_to_window_spec(2400480) == (240, 480)
        assert db_value_to_window_spec(4801200) == (480, 1200)
        assert db_value_to_window_spec(12002400) == (1200, 2400)
        assert db_value_to_window_spec(24004800) == (2400, 4800)

    def test_roundtrip_standard(self):
        """Verify roundtrip conversion for standard window specs."""
        specs = [20, 60, 120, 240, (240, 480), (480, 1200), (1200, 2400), (2400, 4800)]
        for spec in specs:
            assert db_value_to_window_spec(window_spec_to_db_value(spec)) == spec

    def test_roundtrip_partial_maps_to_standard(self):
        """Partial slice windows roundtrip to standard slice windows."""
        assert db_value_to_window_spec(window_spec_to_db_value((2400, 3600))) == (
            2400,
            4800,
        )
        assert db_value_to_window_spec(window_spec_to_db_value((480, 999))) == (
            480,
            1200,
        )


class TestMapSliceToStandard:
    """Tests for map_slice_to_standard function."""

    def test_standard_slices_unchanged(self):
        for s, e in [(240, 480), (480, 1200), (1200, 2400), (2400, 4800)]:
            assert map_slice_to_standard((s, e)) == (s, e)

    def test_partial_slices_map_to_standard(self):
        assert map_slice_to_standard((240, 300)) == (240, 480)
        assert map_slice_to_standard((480, 800)) == (480, 1200)
        assert map_slice_to_standard((1200, 1800)) == (1200, 2400)
        assert map_slice_to_standard((2400, 3600)) == (2400, 4800)


# --- Test normalize edge cases ---


class TestNormalizeEdgeCases:
    """Tests for _normalize method edge cases."""

    def test_normalize_empty_array_raises(self):
        """Empty array should raise ValueError."""
        with pytest.raises(ValueError, match="Cannot normalize empty array"):
            OptimizedChartClassifier._normalize(np.array([]))

    def test_normalize_single_value(self):
        """Single value should normalize to 0.5."""
        result = OptimizedChartClassifier._normalize(np.array([42]))
        assert len(result) == 1
        assert result[0] == 0.5


# --- Test BatchDataLoader ---


class TestBatchDataLoader:
    """Tests for the BatchDataLoader class."""

    def test_load_all_ticker_data(self, mocker, tmp_path):
        """Test batch loading of ticker data."""
        from datetime import datetime, timedelta

        # Create a temporary database
        db_path = tmp_path / "test.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE daily_quotes (
                Code TEXT, Date TEXT, AdjustmentClose REAL
            )
        """)
        # Insert test data with recent dates
        today = datetime.today()
        for i in range(100):
            date = (today - timedelta(days=100 - i)).strftime("%Y-%m-%d")
            cursor.execute(
                "INSERT INTO daily_quotes VALUES (?, ?, ?)", ("1001", date, 100 + i)
            )
            cursor.execute(
                "INSERT INTO daily_quotes VALUES (?, ?, ?)", ("1002", date, 200 + i)
            )
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        loader = BatchDataLoader(str(db_path), logger)
        result = loader.load_all_ticker_data(["1001", "1002"], trading_days=150)

        assert "1001" in result
        assert "1002" in result
        assert len(result["1001"]) > 0
        assert len(result["1002"]) > 0

    def test_load_empty_ticker_list(self, mocker, tmp_path):
        """Test with empty ticker list."""
        db_path = tmp_path / "test.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE daily_quotes (
                Code TEXT, Date TEXT, AdjustmentClose REAL
            )
        """)
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        loader = BatchDataLoader(str(db_path), logger)
        result = loader.load_all_ticker_data([], trading_days=100)

        assert result == {}

    def test_load_missing_ticker(self, mocker, tmp_path):
        """Test loading a ticker that doesn't exist in the database."""
        db_path = tmp_path / "test.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE daily_quotes (
                Code TEXT, Date TEXT, AdjustmentClose REAL
            )
        """)
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        loader = BatchDataLoader(str(db_path), logger)
        result = loader.load_all_ticker_data(["9999"], trading_days=100)

        assert "9999" in result
        assert result["9999"].empty

    def test_long_term_data_loading(self, mocker, tmp_path):
        """Test loading data with days > 1000 (long-term analysis mode)."""
        db_path = tmp_path / "test.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE daily_quotes (
                Code TEXT, Date TEXT, AdjustmentClose REAL
            )
        """)
        # Insert minimal test data
        cursor.execute(
            "INSERT INTO daily_quotes VALUES (?, ?, ?)", ("1001", "2024-01-01", 100)
        )
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        loader = BatchDataLoader(str(db_path), logger)
        loader.load_all_ticker_data(
            ["1001"], trading_days=1500
        )  # Result not needed, testing logging

        # Should log with trading days info
        assert any(
            "1500 trading days" in str(call) for call in logger.info.call_args_list
        )


# --- Test BatchResultsProcessor ---


class TestBatchResultsProcessor:
    """Tests for the BatchResultsProcessor class."""

    def test_add_and_flush_results(self, mocker, tmp_path):
        """Test adding results and flushing to database."""
        db_path = tmp_path / "results.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE classification_results (
                date TEXT, ticker TEXT, window INTEGER,
                pattern_label TEXT, score REAL,
                PRIMARY KEY (date, ticker, window)
            )
        """)
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        processor = BatchResultsProcessor(str(db_path), logger, batch_size=10)

        # Add results
        processor.add_result("2024-01-01", "1001", 20, "上昇", 0.95)
        processor.add_result("2024-01-01", "1002", 20, "下落", 0.88)

        # Manually flush
        processor.flush_results()

        # Verify data was saved
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM classification_results")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 2

    def test_auto_flush_on_batch_full(self, mocker, tmp_path):
        """Test auto-flush when batch size is reached."""
        db_path = tmp_path / "results.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE classification_results (
                date TEXT, ticker TEXT, window INTEGER,
                pattern_label TEXT, score REAL,
                PRIMARY KEY (date, ticker, window)
            )
        """)
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        processor = BatchResultsProcessor(str(db_path), logger, batch_size=3)

        # Add 3 results - should trigger auto-flush
        processor.add_result("2024-01-01", "1001", 20, "上昇", 0.95)
        processor.add_result("2024-01-01", "1002", 20, "下落", 0.88)
        processor.add_result("2024-01-01", "1003", 20, "調整", 0.75)

        # Verify data was saved (auto-flushed)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM classification_results")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 3
        assert len(processor.pending_results) == 0

    def test_context_manager_flushes_on_exit(self, mocker, tmp_path):
        """Test that context manager flushes pending results on exit."""
        db_path = tmp_path / "results.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE classification_results (
                date TEXT, ticker TEXT, window INTEGER,
                pattern_label TEXT, score REAL,
                PRIMARY KEY (date, ticker, window)
            )
        """)
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()

        with BatchResultsProcessor(str(db_path), logger, batch_size=100) as processor:
            processor.add_result("2024-01-01", "1001", 20, "上昇", 0.95)

        # Verify data was flushed on exit
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM classification_results")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 1

    def test_flush_empty_results(self, mocker, tmp_path):
        """Test flushing with no pending results does nothing."""
        db_path = tmp_path / "results.db"

        logger = mocker.MagicMock()
        processor = BatchResultsProcessor(str(db_path), logger)

        # Should not raise error
        processor.flush_results()
        assert len(processor.pending_results) == 0


# --- Test DatabaseManager ---


class TestDatabaseManager:
    """Tests for the DatabaseManager context manager."""

    def test_context_manager_opens_and_closes(self, tmp_path):
        """Test that context manager properly opens and closes connection."""
        db_path = tmp_path / "test.db"

        with DatabaseManager(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE test (id INTEGER)")
            conn.commit()

        # Verify file was created
        assert db_path.exists()

    def test_pragma_settings_applied(self, tmp_path):
        """Test that PRAGMA optimizations are applied."""
        db_path = tmp_path / "test.db"

        with DatabaseManager(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            # WAL mode should be set
            assert journal_mode.lower() == "wal"


# --- Test find_best_match edge cases ---


class TestFindBestMatchEdgeCases:
    """Tests for _find_best_match edge cases."""

    def test_length_mismatch_warning(self, mocker, capsys):
        """Test that length mismatch triggers warning and skips template."""
        # Create classifier with window=30
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mocker.patch("sqlite3.connect", return_value=mock_conn)

        date_index = pd.date_range(start="2024-01-01", periods=30)
        price_data = pd.Series(np.linspace(100, 200, 30), index=date_index)

        classifier = OptimizedChartClassifier(
            ticker="9999", window=30, price_data=price_data
        )

        # Manually add a template with wrong length
        classifier.templates_manual["wrong_length"] = np.array([1, 2, 3])

        # This should print a warning but not crash
        label, score = classifier._find_best_match(
            price_data.values, classifier.templates_manual
        )

        # Should still return a valid match from correct templates
        assert label is not None
        assert score > -np.inf

        captured = capsys.readouterr()
        assert "Warning: Length mismatch" in captured.out


# --- Test check_ticker_data_length ---


class TestCheckTickerDataLength:
    """Tests for BatchDataLoader.check_ticker_data_length method."""

    def test_check_ticker_data_length(self, mocker, tmp_path):
        """Test checking data length for a single ticker."""
        from datetime import datetime, timedelta

        db_path = tmp_path / "test.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE daily_quotes (
                Code TEXT, Date TEXT, AdjustmentClose REAL
            )
        """)
        # Insert 50 days of data
        today = datetime.today()
        for i in range(50):
            date = (today - timedelta(days=50 - i)).strftime("%Y-%m-%d")
            cursor.execute(
                "INSERT INTO daily_quotes VALUES (?, ?, ?)", ("1001", date, 100 + i)
            )
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        loader = BatchDataLoader(str(db_path), logger)
        count = loader.check_ticker_data_length("1001")

        assert count == 50

    def test_check_ticker_data_length_empty(self, mocker, tmp_path):
        """Test checking data length for non-existent ticker."""
        db_path = tmp_path / "test.db"
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE daily_quotes (
                Code TEXT, Date TEXT, AdjustmentClose REAL
            )
        """)
        conn.commit()
        conn.close()

        logger = mocker.MagicMock()
        loader = BatchDataLoader(str(db_path), logger)
        count = loader.check_ticker_data_length("9999")

        assert count == 0


# --- Test get_all_tickers error handling ---


class TestGetAllTickersErrors:
    """Tests for get_all_tickers error handling."""

    def test_get_all_tickers_db_error(self, mocker, tmp_path):
        """Test error handling when database read fails."""
        # Create an invalid database path
        result = get_all_tickers("/nonexistent/path/master.db")
        assert result == []


# --- Test OptimizedChartClassifier additional cases ---


class TestOptimizedChartClassifierAdditional:
    """Additional tests for OptimizedChartClassifier."""

    def test_no_price_data_raises(self, mocker):
        """Test that classify_latest raises when no data available."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mocker.patch("sqlite3.connect", return_value=mock_conn)

        # Create classifier with empty price data
        date_index = pd.DatetimeIndex([])

        classifier = OptimizedChartClassifier(
            ticker="9999",
            window=30,
            price_data=pd.Series([], index=date_index, dtype=float),
        )

        with pytest.raises(ValueError, match="No price data available"):
            classifier.classify_latest()

    def test_nan_score_handled(self, mocker):
        """Test that NaN correlation scores are converted to 0."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mocker.patch("sqlite3.connect", return_value=mock_conn)

        # Create constant data that would produce NaN correlation
        date_index = pd.date_range(start="2024-01-01", periods=30)
        constant_data = pd.Series([100.0] * 30, index=date_index)

        classifier = OptimizedChartClassifier(
            ticker="9999", window=30, price_data=constant_data
        )

        # Should not raise - NaN scores should be handled
        label, score, _ = classifier.classify_latest()
        assert label is not None
        assert not np.isnan(score)


# --- Test classify_window ---


class TestClassifyWindow:
    """Tests for the classify_window method."""

    def _make_classifier(self, prices, window=60):
        date_index = pd.date_range(start="2024-01-01", periods=len(prices))
        price_data = pd.Series(prices, index=date_index)
        return OptimizedChartClassifier(
            ticker="9999", window=window, price_data=price_data
        )

    def test_cumulative_window(self):
        """classify_window with int uses last N days."""
        prices = np.linspace(100, 200, 100)
        classifier = self._make_classifier(prices, window=60)
        label, score, date = classifier.classify_window(60)
        assert label == "上昇"
        assert score > 0.7

    def test_slice_window(self):
        """classify_window with tuple slices the correct range."""
        # 500 data points: rising first half, flat second half
        rising = np.linspace(100, 300, 300)
        flat = np.full(200, 300.0)
        prices = np.concatenate([rising, flat])
        classifier = self._make_classifier(prices, window=20)

        # Slice (200, 500) should capture the rising portion
        label, score, _ = classifier.classify_window((200, 500))
        assert label == "上昇"
        assert score > 0.8

    def test_nan_handling(self):
        """classify_window with NaN values should not crash."""
        prices = np.linspace(100, 200, 100).copy()
        # Insert some NaNs (< 50% of 60)
        prices[10] = np.nan
        prices[20] = np.nan
        prices[30] = np.nan
        classifier = self._make_classifier(prices, window=60)
        label, score, _ = classifier.classify_window(60)
        assert label is not None

    def test_nan_too_many_raises(self):
        """classify_window with > 50% NaN should raise ValueError."""
        prices = np.full(100, np.nan)
        prices[:20] = np.linspace(100, 200, 20)  # Only 20 valid out of 60
        classifier = self._make_classifier(prices, window=60)
        with pytest.raises(ValueError, match="Insufficient non-NaN data"):
            classifier.classify_window(60)

    def test_low_correlation_returns_best_match(self):
        """classify_window should return best match label even for low correlation."""
        # Random noise has low correlation with any template
        np.random.seed(42)
        prices = np.random.uniform(100, 200, 100)
        classifier = self._make_classifier(prices, window=60)
        label, score, _ = classifier.classify_window(60)
        # Label is best match (not "不明"), score reflects low confidence
        assert label != "不明"
        assert isinstance(label, str)

    def test_constant_data_returns_best_match(self):
        """Constant data produces NaN correlation → score 0, but still returns best match."""
        prices = np.full(100, 150.0)
        classifier = self._make_classifier(prices, window=60)
        label, score, _ = classifier.classify_window(60)
        assert label != "不明"
        assert score < MIN_CORRELATION_THRESHOLD
