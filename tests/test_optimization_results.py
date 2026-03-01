"""Tests for OptimizationResults class."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from technical_tools.optimization_results import OptimizationResults, TrialResult


@pytest.fixture
def sample_trials() -> list[TrialResult]:
    """Create sample trial results for testing."""
    return [
        TrialResult(
            params={"ma_short": 5, "ma_long": 50},
            metrics={
                "total_return": 0.15,
                "sharpe_ratio": 1.5,
                "max_drawdown": 0.08,
                "win_rate": 0.6,
                "profit_factor": 2.0,
            },
            oos_metrics=None,
            backtest_results=None,
        ),
        TrialResult(
            params={"ma_short": 10, "ma_long": 50},
            metrics={
                "total_return": 0.20,
                "sharpe_ratio": 2.0,
                "max_drawdown": 0.10,
                "win_rate": 0.65,
                "profit_factor": 2.5,
            },
            oos_metrics=None,
            backtest_results=None,
        ),
        TrialResult(
            params={"ma_short": 5, "ma_long": 75},
            metrics={
                "total_return": 0.10,
                "sharpe_ratio": 1.0,
                "max_drawdown": 0.05,
                "win_rate": 0.55,
                "profit_factor": 1.5,
            },
            oos_metrics=None,
            backtest_results=None,
        ),
        TrialResult(
            params={"ma_short": 10, "ma_long": 75},
            metrics={
                "total_return": 0.25,
                "sharpe_ratio": 1.8,
                "max_drawdown": 0.12,
                "win_rate": 0.70,
                "profit_factor": 3.0,
            },
            oos_metrics=None,
            backtest_results=None,
        ),
    ]


@pytest.fixture
def optimization_results(sample_trials: list[TrialResult]) -> OptimizationResults:
    """Create OptimizationResults instance for testing."""
    return OptimizationResults(
        trials=sample_trials,
        metric="sharpe_ratio",
        search_spaces={"ma_short": [5, 10], "ma_long": [50, 75]},
    )


class TestOptimizationResultsInit:
    """Test OptimizationResults initialization."""

    def test_init(self, sample_trials: list[TrialResult]) -> None:
        """OptimizationResults can be instantiated."""
        results = OptimizationResults(
            trials=sample_trials,
            metric="sharpe_ratio",
            search_spaces={"ma_short": [5, 10], "ma_long": [50, 75]},
        )
        assert results is not None
        assert len(results._trials) == 4


class TestBest:
    """Test best() method."""

    def test_best_returns_highest_sharpe(
        self, optimization_results: OptimizationResults
    ) -> None:
        """best() returns trial with highest sharpe_ratio."""
        best = optimization_results.best()
        assert best is not None
        assert best.params == {"ma_short": 10, "ma_long": 50}
        assert best.metrics["sharpe_ratio"] == 2.0

    def test_best_with_max_drawdown_metric(
        self, sample_trials: list[TrialResult]
    ) -> None:
        """best() returns trial with lowest max_drawdown when that's the metric."""
        results = OptimizationResults(
            trials=sample_trials,
            metric="max_drawdown",
            search_spaces={"ma_short": [5, 10], "ma_long": [50, 75]},
        )
        best = results.best()
        assert best is not None
        # max_drawdown is minimized, so lowest is best
        assert best.metrics["max_drawdown"] == 0.05
        assert best.params == {"ma_short": 5, "ma_long": 75}

    def test_best_empty_trials(self) -> None:
        """best() returns None when no trials."""
        results = OptimizationResults(
            trials=[],
            metric="sharpe_ratio",
            search_spaces={},
        )
        assert results.best() is None


class TestTop:
    """Test top() method."""

    def test_top_returns_dataframe(
        self, optimization_results: OptimizationResults
    ) -> None:
        """top() returns a DataFrame."""
        top_df = optimization_results.top(3)
        assert isinstance(top_df, pd.DataFrame)
        assert len(top_df) == 3

    def test_top_sorted_by_metric(
        self, optimization_results: OptimizationResults
    ) -> None:
        """top() results are sorted by metric (descending for sharpe_ratio)."""
        top_df = optimization_results.top(4)
        sharpe_values = top_df["sharpe_ratio"].tolist()
        assert sharpe_values == sorted(sharpe_values, reverse=True)

    def test_top_contains_params_and_metrics(
        self, optimization_results: OptimizationResults
    ) -> None:
        """top() DataFrame contains both params and metrics columns."""
        top_df = optimization_results.top(2)
        assert "ma_short" in top_df.columns
        assert "ma_long" in top_df.columns
        assert "sharpe_ratio" in top_df.columns
        assert "total_return" in top_df.columns

    def test_top_more_than_available(
        self, optimization_results: OptimizationResults
    ) -> None:
        """top(n) with n > available trials returns all trials."""
        top_df = optimization_results.top(10)
        assert len(top_df) == 4


class TestPlotHeatmap:
    """Test plot_heatmap() method."""

    def test_plot_heatmap_returns_figure(
        self, optimization_results: OptimizationResults
    ) -> None:
        """plot_heatmap() returns a plotly Figure."""
        import plotly.graph_objects as go

        fig = optimization_results.plot_heatmap("ma_short", "ma_long")
        assert isinstance(fig, go.Figure)

    def test_plot_heatmap_with_custom_metric(
        self, optimization_results: OptimizationResults
    ) -> None:
        """plot_heatmap() can use custom metric."""
        import plotly.graph_objects as go

        fig = optimization_results.plot_heatmap(
            "ma_short", "ma_long", metric="total_return"
        )
        assert isinstance(fig, go.Figure)

    def test_plot_heatmap_invalid_param(
        self, optimization_results: OptimizationResults
    ) -> None:
        """plot_heatmap() raises error for invalid parameter name."""
        with pytest.raises(ValueError, match="not found"):
            optimization_results.plot_heatmap("invalid_param", "ma_long")


class TestSaveLoad:
    """Test save() and load() methods."""

    def test_save_json(self, optimization_results: OptimizationResults) -> None:
        """Can save results to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.json"
            saved_path = optimization_results.save(path)
            assert saved_path.exists()
            assert saved_path.suffix == ".json"

    def test_save_csv(self, optimization_results: OptimizationResults) -> None:
        """Can save results to CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.csv"
            saved_path = optimization_results.save(path)
            assert saved_path.exists()
            assert saved_path.suffix == ".csv"

    def test_load_json(self, optimization_results: OptimizationResults) -> None:
        """Can load results from JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.json"
            optimization_results.save(path)

            loaded = OptimizationResults.load(path)
            assert loaded is not None
            assert len(loaded._trials) == len(optimization_results._trials)
            assert loaded.best().params == optimization_results.best().params

    def test_load_preserves_metrics(
        self, optimization_results: OptimizationResults
    ) -> None:
        """Loaded results preserve all metrics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.json"
            optimization_results.save(path)

            loaded = OptimizationResults.load(path)
            original_best = optimization_results.best()
            loaded_best = loaded.best()

            for metric_name in original_best.metrics:
                assert (
                    loaded_best.metrics[metric_name]
                    == original_best.metrics[metric_name]
                )


class TestCompositeMetric:
    """Test composite (weighted) metric handling."""

    def test_composite_metric_ranking(self, sample_trials: list[TrialResult]) -> None:
        """Composite metric correctly combines multiple metrics."""
        results = OptimizationResults(
            trials=sample_trials,
            metric={
                "sharpe_ratio": 0.5,
                "max_drawdown": 0.3,
                "win_rate": 0.2,
            },
            search_spaces={"ma_short": [5, 10], "ma_long": [50, 75]},
        )

        # best() should work with composite metric
        best = results.best()
        assert best is not None

    def test_top_with_composite_metric(self, sample_trials: list[TrialResult]) -> None:
        """top() works correctly with composite metric."""
        results = OptimizationResults(
            trials=sample_trials,
            metric={
                "sharpe_ratio": 0.5,
                "max_drawdown": 0.3,
                "win_rate": 0.2,
            },
            search_spaces={"ma_short": [5, 10], "ma_long": [50, 75]},
        )

        top_df = results.top(4)
        assert "composite_score" in top_df.columns
        # Composite scores should be sorted descending
        scores = top_df["composite_score"].tolist()
        assert scores == sorted(scores, reverse=True)


class TestWalkForwardResults:
    """Test walk-forward validation results."""

    def test_oos_metrics_in_trial(self) -> None:
        """TrialResult can store out-of-sample metrics."""
        trial = TrialResult(
            params={"ma_short": 5, "ma_long": 50},
            metrics={
                "total_return": 0.15,
                "sharpe_ratio": 1.5,
            },
            oos_metrics={
                "total_return": 0.10,
                "sharpe_ratio": 1.0,
            },
            backtest_results=None,
        )
        assert trial.oos_metrics is not None
        assert trial.oos_metrics["sharpe_ratio"] == 1.0

    def test_best_with_oos_metrics(self) -> None:
        """best() result includes oos_metrics when available."""
        trials = [
            TrialResult(
                params={"ma_short": 5, "ma_long": 50},
                metrics={"sharpe_ratio": 2.0},
                oos_metrics={"sharpe_ratio": 1.0},
                backtest_results=None,
            ),
            TrialResult(
                params={"ma_short": 10, "ma_long": 50},
                metrics={"sharpe_ratio": 1.5},
                oos_metrics={"sharpe_ratio": 1.2},
                backtest_results=None,
            ),
        ]
        results = OptimizationResults(
            trials=trials,
            metric="sharpe_ratio",
            search_spaces={"ma_short": [5, 10], "ma_long": [50]},
        )

        best = results.best()
        assert best.oos_metrics is not None


class TestLoadStreaming:
    """Test load_streaming() method."""

    def test_load_streaming_basic(self) -> None:
        """Can load results from JSONL file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.jsonl"
            # Create sample JSONL file
            lines = [
                '{"params": {"ma_short": 5, "ma_long": 50}, "metrics": {"sharpe_ratio": 1.5, "total_return": 0.15}, "oos_metrics": null}',
                '{"params": {"ma_short": 10, "ma_long": 50}, "metrics": {"sharpe_ratio": 2.0, "total_return": 0.20}, "oos_metrics": null}',
            ]
            path.write_text("\n".join(lines))

            results = OptimizationResults.load_streaming(path)
            assert len(results._trials) == 2
            assert results.best().metrics["sharpe_ratio"] == 2.0

    def test_load_streaming_with_oos_metrics(self) -> None:
        """Can load streaming results with oos_metrics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.jsonl"
            lines = [
                '{"params": {"ma_short": 5, "ma_long": 50}, "metrics": {"sharpe_ratio": 1.5}, "oos_metrics": {"sharpe_ratio": 1.0}}',
                '{"params": {"ma_short": 10, "ma_long": 50}, "metrics": {"sharpe_ratio": 2.0}, "oos_metrics": {"sharpe_ratio": 1.5}}',
            ]
            path.write_text("\n".join(lines))

            results = OptimizationResults.load_streaming(path)
            best = results.best()
            assert best.oos_metrics is not None
            assert best.oos_metrics["sharpe_ratio"] == 1.5

    def test_load_streaming_reconstructs_search_spaces(self) -> None:
        """load_streaming reconstructs search spaces from observed params."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.jsonl"
            lines = [
                '{"params": {"ma_short": 5, "ma_long": 50}, "metrics": {"sharpe_ratio": 1.5}, "oos_metrics": null}',
                '{"params": {"ma_short": 10, "ma_long": 50}, "metrics": {"sharpe_ratio": 2.0}, "oos_metrics": null}',
                '{"params": {"ma_short": 5, "ma_long": 75}, "metrics": {"sharpe_ratio": 1.8}, "oos_metrics": null}',
                '{"params": {"ma_short": 10, "ma_long": 75}, "metrics": {"sharpe_ratio": 1.9}, "oos_metrics": null}',
            ]
            path.write_text("\n".join(lines))

            results = OptimizationResults.load_streaming(path)
            assert "ma_short" in results._search_spaces
            assert "ma_long" in results._search_spaces
            assert sorted(results._search_spaces["ma_short"]) == [5, 10]
            assert sorted(results._search_spaces["ma_long"]) == [50, 75]

    def test_load_streaming_custom_metric(self) -> None:
        """load_streaming can use custom metric for sorting."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.jsonl"
            lines = [
                '{"params": {"ma_short": 5}, "metrics": {"sharpe_ratio": 1.5, "total_return": 0.30}, "oos_metrics": null}',
                '{"params": {"ma_short": 10}, "metrics": {"sharpe_ratio": 2.0, "total_return": 0.20}, "oos_metrics": null}',
            ]
            path.write_text("\n".join(lines))

            results = OptimizationResults.load_streaming(path, metric="total_return")
            best = results.best()
            # Best by total_return is ma_short=5 with 0.30
            assert best.params["ma_short"] == 5

    def test_load_streaming_empty_lines_ignored(self) -> None:
        """load_streaming ignores empty lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.jsonl"
            lines = [
                '{"params": {"ma_short": 5}, "metrics": {"sharpe_ratio": 1.5}, "oos_metrics": null}',
                "",  # Empty line
                '{"params": {"ma_short": 10}, "metrics": {"sharpe_ratio": 2.0}, "oos_metrics": null}',
                "  ",  # Whitespace line
            ]
            path.write_text("\n".join(lines))

            results = OptimizationResults.load_streaming(path)
            assert len(results._trials) == 2

    def test_load_streaming_composite_metric(self) -> None:
        """load_streaming works with composite metric."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.jsonl"
            lines = [
                '{"params": {"ma_short": 5}, "metrics": {"sharpe_ratio": 1.5, "max_drawdown": 0.1, "win_rate": 0.6}, "oos_metrics": null}',
                '{"params": {"ma_short": 10}, "metrics": {"sharpe_ratio": 2.0, "max_drawdown": 0.2, "win_rate": 0.5}, "oos_metrics": null}',
            ]
            path.write_text("\n".join(lines))

            results = OptimizationResults.load_streaming(
                path,
                metric={"sharpe_ratio": 0.5, "max_drawdown": 0.3, "win_rate": 0.2},
            )
            best = results.best()
            assert best is not None
