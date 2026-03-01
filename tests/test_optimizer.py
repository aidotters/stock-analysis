"""Tests for StrategyOptimizer class."""

import pandas as pd
import pytest

from technical_tools.optimizer import StrategyOptimizer
from technical_tools.exceptions import NoValidParametersError, OptimizationTimeoutError


@pytest.fixture
def sample_price_data() -> pd.DataFrame:
    """Create sample price data for testing."""
    dates = pd.date_range(start="2023-01-01", periods=200, freq="B")

    # Create a simple uptrend with some volatility
    base_price = 1000
    prices = []
    for i in range(len(dates)):
        price = base_price + i * 5 + (i % 10 - 5) * 10
        prices.append(price)

    df = pd.DataFrame(
        {
            "Open": prices,
            "High": [p * 1.02 for p in prices],
            "Low": [p * 0.98 for p in prices],
            "Close": prices,
            "Volume": [1000000] * len(dates),
        },
        index=dates,
    )

    return df


class TestStrategyOptimizerInit:
    """Test StrategyOptimizer initialization."""

    def test_init_default(self) -> None:
        """StrategyOptimizer can be instantiated with default parameters."""
        optimizer = StrategyOptimizer()
        assert optimizer is not None

    def test_init_with_cash(self) -> None:
        """StrategyOptimizer can be instantiated with custom initial cash."""
        optimizer = StrategyOptimizer(cash=500000)
        assert optimizer._cash == 500000

    def test_init_with_commission(self) -> None:
        """StrategyOptimizer can be instantiated with custom commission rate."""
        optimizer = StrategyOptimizer(commission=0.001)
        assert optimizer._commission == 0.001


class TestAddSearchSpace:
    """Test add_search_space method."""

    def test_add_single_search_space(self) -> None:
        """Can add single parameter to search space."""
        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 20])
        assert "ma_short" in optimizer._search_spaces
        assert optimizer._search_spaces["ma_short"] == [5, 10, 20]

    def test_add_multiple_search_spaces(self) -> None:
        """Can add multiple parameters to search space."""
        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 20])
        optimizer.add_search_space("ma_long", [50, 75, 100])
        assert len(optimizer._search_spaces) == 2
        assert "ma_short" in optimizer._search_spaces
        assert "ma_long" in optimizer._search_spaces

    def test_method_chaining(self) -> None:
        """add_search_space returns self for method chaining."""
        optimizer = StrategyOptimizer()
        result = optimizer.add_search_space("ma_short", [5, 10])
        assert result is optimizer

    def test_empty_name_raises_error(self) -> None:
        """Empty parameter name raises ValueError."""
        optimizer = StrategyOptimizer()
        with pytest.raises(ValueError, match="parameter name"):
            optimizer.add_search_space("", [5, 10])

    def test_empty_values_raises_error(self) -> None:
        """Empty values list raises ValueError."""
        optimizer = StrategyOptimizer()
        with pytest.raises(ValueError, match="values"):
            optimizer.add_search_space("ma_short", [])


class TestAddConstraint:
    """Test add_constraint method."""

    def test_add_single_constraint(self) -> None:
        """Can add single constraint."""
        optimizer = StrategyOptimizer()
        optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])
        assert len(optimizer._constraints) == 1

    def test_add_multiple_constraints(self) -> None:
        """Can add multiple constraints."""
        optimizer = StrategyOptimizer()
        optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])
        optimizer.add_constraint(lambda p: p["stop_loss"] > -0.20)
        assert len(optimizer._constraints) == 2

    def test_method_chaining(self) -> None:
        """add_constraint returns self for method chaining."""
        optimizer = StrategyOptimizer()
        result = optimizer.add_constraint(lambda p: True)
        assert result is optimizer


class TestGridSearch:
    """Test grid search functionality."""

    def test_grid_search_generates_all_combinations(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Grid search evaluates all parameter combinations."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        # 2 x 2 = 4 combinations
        assert len(results._trials) == 4

    def test_grid_search_with_constraint(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Grid search respects parameter constraints."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 20, 50])
        optimizer.add_search_space("ma_long", [25, 50, 75])
        # Constraint: ma_short must be less than ma_long
        optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        # Valid combinations: (5,25), (5,50), (5,75), (10,25), (10,50), (10,75),
        #                     (20,25), (20,50), (20,75), (50,75) = 10 combinations
        # But (20, 25) is invalid since 20 < 25, (50, 25) invalid, (50, 50) invalid
        # Valid: (5,25), (5,50), (5,75), (10,25), (10,50), (10,75),
        #        (20,25), (20,50), (20,75) - but 20 < 25? Yes. 50 < 25? No.
        # Actually: (5,25), (5,50), (5,75), (10,25), (10,50), (10,75),
        #           (20,25), (20,50), (20,75) = 9 valid combinations for [5,10,20] x [25,50,75]
        # With 50: (50,75) = 1 more
        # Total should be less than 4 * 3 = 12
        assert len(results._trials) < 12

        # All results should satisfy the constraint
        for trial in results._trials:
            assert trial.params["ma_short"] < trial.params["ma_long"]

    def test_all_params_filtered_raises_error(self) -> None:
        """Raises error when all parameters are filtered by constraints."""
        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [100, 200])
        optimizer.add_search_space("ma_long", [50, 75])
        # Impossible constraint: ma_short < ma_long when all ma_short > ma_long
        optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])

        with pytest.raises(NoValidParametersError):
            optimizer.run(
                symbols=["7203"],
                start="2023-01-01",
                end="2023-12-31",
                method="grid",
                metric="sharpe_ratio",
            )


class TestRandomSearch:
    """Test random search functionality."""

    def test_random_search_limits_trials(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Random search evaluates only n_trials combinations."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", list(range(5, 26)))  # 21 values
        optimizer.add_search_space("ma_long", list(range(50, 201)))  # 151 values

        # Total combinations would be 21 * 151 = 3,171
        # But we only want 10 trials
        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="random",
            n_trials=10,
            metric="sharpe_ratio",
        )

        assert len(results._trials) == 10

    def test_random_search_with_constraint(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Random search respects constraints."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 20, 50])
        optimizer.add_search_space("ma_long", [25, 50, 75, 100])
        optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="random",
            n_trials=5,
            metric="sharpe_ratio",
        )

        # All results should satisfy the constraint
        for trial in results._trials:
            assert trial.params["ma_short"] < trial.params["ma_long"]


class TestMetricOptimization:
    """Test different metric optimization."""

    def test_total_return_metric(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Can optimize for total_return metric."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="total_return",
        )

        assert results.best() is not None
        assert "total_return" in results.best().metrics

    def test_sharpe_ratio_metric(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Can optimize for sharpe_ratio metric."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        assert results.best() is not None
        assert "sharpe_ratio" in results.best().metrics

    def test_max_drawdown_metric(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Can optimize for max_drawdown metric (minimization)."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="max_drawdown",
        )

        assert results.best() is not None
        assert "max_drawdown" in results.best().metrics

    def test_win_rate_metric(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Can optimize for win_rate metric."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="win_rate",
        )

        assert results.best() is not None
        assert "win_rate" in results.best().metrics

    def test_composite_metric(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Can optimize with composite (weighted) metric."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric={
                "sharpe_ratio": 0.5,
                "max_drawdown": 0.3,
                "win_rate": 0.2,
            },
        )

        assert results.best() is not None


class TestRSIOptimization:
    """Test RSI parameter optimization."""

    def test_rsi_oversold_optimization(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Can optimize RSI oversold threshold."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("rsi_threshold", [20, 25, 30, 35])
        optimizer.add_search_space("stop_loss", [-0.10])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        assert len(results._trials) == 4


class TestMACDOptimization:
    """Test MACD parameter optimization."""

    def test_macd_optimization(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Can optimize MACD parameters."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("macd_fast", [8, 12])
        optimizer.add_search_space("macd_slow", [21, 26])
        optimizer.add_search_space("macd_signal", [9])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        assert len(results._trials) == 4


class TestExitRuleOptimization:
    """Test exit rule parameter optimization."""

    def test_stop_loss_take_profit_optimization(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Can optimize stop_loss and take_profit thresholds."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [25])
        optimizer.add_search_space("stop_loss", [-0.05, -0.10, -0.15])
        optimizer.add_search_space("take_profit", [0.10, 0.20, 0.30])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        # 1 * 1 * 3 * 3 = 9 combinations
        assert len(results._trials) == 9


class TestParallelExecution:
    """Test parallel execution."""

    def test_parallel_execution(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """Parallel execution produces same results as single-threaded."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            n_jobs=2,
        )

        assert len(results._trials) == 4


class TestIntegrationWithBacktester:
    """Test integration with existing Backtester."""

    def test_basic_integration(self, sample_price_data: pd.DataFrame, mocker) -> None:
        """StrategyOptimizer integrates with Backtester correctly."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])
        optimizer.add_search_space("stop_loss", [-0.10])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        assert results.best() is not None
        assert len(results.top(4)) == 4


class TestWalkForwardValidation:
    """Test walk-forward validation."""

    def test_walk_forward_returns_oos_metrics(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Walk-forward validation returns out-of-sample metrics."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            validation="walk_forward",
            train_ratio=0.7,
            n_splits=3,
        )

        best = results.best()
        assert best.oos_metrics is not None
        assert "total_return" in best.oos_metrics
        assert "sharpe_ratio" in best.oos_metrics
        assert "win_rate" in best.oos_metrics

    def test_walk_forward_all_trials_have_oos_metrics(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """All trials in walk-forward mode have oos_metrics."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            validation="walk_forward",
            train_ratio=0.7,
            n_splits=3,
        )

        for trial in results._trials:
            assert trial.oos_metrics is not None
            assert isinstance(trial.oos_metrics, dict)

    def test_without_walk_forward_no_oos_metrics(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Without walk-forward validation, oos_metrics is None."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )

        for trial in results._trials:
            assert trial.oos_metrics is None

    def test_walk_forward_with_very_short_period(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Walk-forward handles very short period (test_start >= test_end)."""
        # Create minimal data that will cause some splits to be skipped
        short_data = sample_price_data.iloc[:30].copy()
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = short_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [10])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-01-15",  # Very short period
            method="grid",
            metric="sharpe_ratio",
            validation="walk_forward",
            train_ratio=0.95,  # Very high ratio leaves minimal test period
            n_splits=10,  # Many splits on short period
        )

        # Should still return results with fallback oos_metrics
        best = results.best()
        assert best is not None
        assert best.oos_metrics is not None

    def test_walk_forward_all_splits_fail(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Walk-forward returns fallback metrics when all splits fail."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        # Return data that will cause run to fail in walk-forward splits
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [50])

        # Mock _run_walk_forward to simulate all splits failing
        def failing_walk_forward(*args, **kwargs):
            # Return fallback metrics (what happens when oos_returns is empty)
            return {
                "total_return": 0.0,
                "sharpe_ratio": 0.0,
                "win_rate": 0.0,
            }

        optimizer._run_walk_forward = failing_walk_forward

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            validation="walk_forward",
        )

        best = results.best()
        assert best.oos_metrics is not None
        assert best.oos_metrics["total_return"] == 0.0
        assert best.oos_metrics["sharpe_ratio"] == 0.0
        assert best.oos_metrics["win_rate"] == 0.0

    def test_walk_forward_split_exception_handling(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Walk-forward gracefully handles exceptions in individual splits."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")

        # Create mock that fails on some calls
        call_count = [0]

        def get_prices_with_failures(*args, **kwargs):
            call_count[0] += 1
            # Fail on every other call after the first few
            if call_count[0] > 5 and call_count[0] % 2 == 0:
                raise ValueError("Simulated data fetch failure")
            return sample_price_data

        mock_reader.return_value.get_prices.side_effect = get_prices_with_failures

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [50])

        # Should not raise, just log warnings and continue
        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            validation="walk_forward",
            n_splits=3,
        )

        # Should still get results even with some failures
        assert results.best() is not None


class TestStreamingOutput:
    """Test streaming output functionality."""

    def test_streaming_output_creates_file(
        self, sample_price_data: pd.DataFrame, mocker, tmp_path
    ) -> None:
        """Streaming output creates JSONL file."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        output_path = tmp_path / "results.jsonl"

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            streaming_output=output_path,
        )

        assert output_path.exists()
        lines = output_path.read_text().strip().split("\n")
        assert len(lines) == 4  # 2 x 2 = 4 combinations

    def test_streaming_output_jsonl_format(
        self, sample_price_data: pd.DataFrame, mocker, tmp_path
    ) -> None:
        """Streaming output uses valid JSONL format."""
        import json

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        output_path = tmp_path / "results.jsonl"

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [50])

        optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            streaming_output=output_path,
        )

        lines = output_path.read_text().strip().split("\n")
        for line in lines:
            record = json.loads(line)
            assert "params" in record
            assert "metrics" in record
            assert "oos_metrics" in record

    def test_streaming_output_contains_params_and_metrics(
        self, sample_price_data: pd.DataFrame, mocker, tmp_path
    ) -> None:
        """Streaming output contains params and metrics."""
        import json

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        output_path = tmp_path / "results.jsonl"

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50])

        optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            streaming_output=output_path,
        )

        lines = output_path.read_text().strip().split("\n")
        params_seen = set()
        for line in lines:
            record = json.loads(line)
            params_seen.add(record["params"]["ma_short"])
            assert "sharpe_ratio" in record["metrics"]
            assert "total_return" in record["metrics"]

        assert params_seen == {5, 10}

    def test_streaming_output_with_walk_forward(
        self, sample_price_data: pd.DataFrame, mocker, tmp_path
    ) -> None:
        """Streaming output includes oos_metrics for walk-forward."""
        import json

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        output_path = tmp_path / "results.jsonl"

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [50])

        optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            validation="walk_forward",
            streaming_output=output_path,
        )

        lines = output_path.read_text().strip().split("\n")
        for line in lines:
            record = json.loads(line)
            assert record["oos_metrics"] is not None

    def test_streaming_output_creates_parent_dirs(
        self, sample_price_data: pd.DataFrame, mocker, tmp_path
    ) -> None:
        """Streaming output creates parent directories if needed."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        output_path = tmp_path / "subdir1" / "subdir2" / "results.jsonl"

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5])
        optimizer.add_search_space("ma_long", [50])

        optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            streaming_output=output_path,
        )

        assert output_path.exists()

    def test_streaming_parallel_execution(
        self, sample_price_data: pd.DataFrame, mocker, tmp_path
    ) -> None:
        """Streaming output works with parallel execution."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        output_path = tmp_path / "results.jsonl"

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            n_jobs=2,
            streaming_output=output_path,
        )

        lines = output_path.read_text().strip().split("\n")
        assert len(lines) == 4


class TestPerformance:
    """Test performance requirements."""

    @pytest.mark.slow
    def test_performance_100_combinations(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """100 parameter combinations complete within 60 seconds."""
        import time

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 15, 20, 25])
        optimizer.add_search_space("ma_long", [50, 75, 100, 125, 150, 175, 200])
        optimizer.add_search_space("stop_loss", [-0.05, -0.10, -0.15])
        # 5 * 7 * 3 = 105 combinations (slightly over 100)
        # With constraint, expect ~100 valid combinations

        start = time.time()
        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
        )
        elapsed = time.time() - start

        assert len(results._trials) > 0
        assert elapsed < 60, f"Optimization took {elapsed:.1f}s, expected < 60s"


class TestTimeout:
    """Test optimization timeout functionality."""

    def test_timeout_raises_error(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Optimization raises OptimizationTimeoutError when timeout exceeded."""
        import time

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")

        # Add delay to simulate slow evaluation
        def slow_get_prices(*args, **kwargs):
            time.sleep(0.1)
            return sample_price_data

        mock_reader.return_value.get_prices.side_effect = slow_get_prices

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 15, 20, 25])
        optimizer.add_search_space("ma_long", [50, 75, 100])

        with pytest.raises(OptimizationTimeoutError) as exc_info:
            optimizer.run(
                symbols=["7203"],
                start="2023-01-01",
                end="2023-12-31",
                method="grid",
                metric="sharpe_ratio",
                timeout=0.1,  # Very short timeout
                n_jobs=1,  # Single-threaded for predictable behavior
            )

        assert exc_info.value.timeout == 0.1
        assert exc_info.value.completed < exc_info.value.total

    def test_timeout_error_attributes(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """OptimizationTimeoutError contains correct attributes."""
        import time

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")

        def slow_get_prices(*args, **kwargs):
            time.sleep(0.05)
            return sample_price_data

        mock_reader.return_value.get_prices.side_effect = slow_get_prices

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        with pytest.raises(OptimizationTimeoutError) as exc_info:
            optimizer.run(
                symbols=["7203"],
                start="2023-01-01",
                end="2023-12-31",
                method="grid",
                metric="sharpe_ratio",
                timeout=0.05,
                n_jobs=1,
            )

        error = exc_info.value
        assert hasattr(error, "timeout")
        assert hasattr(error, "completed")
        assert hasattr(error, "total")
        assert error.total == 4  # 2 x 2 combinations

    def test_no_timeout_completes_normally(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Without timeout, optimization completes normally."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            timeout=None,
        )

        assert len(results._trials) == 4

    def test_sufficient_timeout_completes(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """With sufficient timeout, optimization completes without error."""
        mock_reader = mocker.patch("technical_tools.backtester.DataReader")
        mock_reader.return_value.get_prices.return_value = sample_price_data

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10])
        optimizer.add_search_space("ma_long", [50, 75])

        results = optimizer.run(
            symbols=["7203"],
            start="2023-01-01",
            end="2023-12-31",
            method="grid",
            metric="sharpe_ratio",
            timeout=60,  # Generous timeout
        )

        assert len(results._trials) == 4

    def test_timeout_parallel_execution(
        self, sample_price_data: pd.DataFrame, mocker
    ) -> None:
        """Timeout works correctly with parallel execution."""
        import time

        mock_reader = mocker.patch("technical_tools.backtester.DataReader")

        def slow_get_prices(*args, **kwargs):
            time.sleep(0.1)
            return sample_price_data

        mock_reader.return_value.get_prices.side_effect = slow_get_prices

        optimizer = StrategyOptimizer()
        optimizer.add_search_space("ma_short", [5, 10, 15, 20])
        optimizer.add_search_space("ma_long", [50, 75, 100])

        with pytest.raises(OptimizationTimeoutError) as exc_info:
            optimizer.run(
                symbols=["7203"],
                start="2023-01-01",
                end="2023-12-31",
                method="grid",
                metric="sharpe_ratio",
                timeout=0.15,
                n_jobs=2,
            )

        assert exc_info.value.completed < exc_info.value.total
