"""OptimizationResults class for storing and analyzing optimization results."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go


@dataclass
class TrialResult:
    """Result from a single optimization trial.

    Attributes:
        params: Parameter values used in this trial
        metrics: Performance metrics from backtest
        oos_metrics: Out-of-sample metrics (for walk-forward validation)
        backtest_results: Full BacktestResults object (optional, may be None)
    """

    params: dict[str, Any]
    metrics: dict[str, float]
    oos_metrics: dict[str, float] | None
    backtest_results: Any  # BacktestResults | None


class OptimizationResults:
    """Container for optimization results with analysis methods.

    Provides methods for finding best strategies, ranking results,
    visualization, and persistence.

    Example:
        >>> results = optimizer.run(...)
        >>> best = results.best()
        >>> top_10 = results.top(10)
        >>> results.plot_heatmap("ma_short", "ma_long").show()
        >>> results.save("results.json")
    """

    def __init__(
        self,
        trials: list[TrialResult],
        metric: str | dict[str, float],
        search_spaces: dict[str, list[Any]],
    ) -> None:
        """Initialize OptimizationResults.

        Args:
            trials: List of TrialResult objects
            metric: Optimization metric (string or weighted dict)
            search_spaces: Dict of parameter names to value lists
        """
        self._trials = trials
        self._metric = metric
        self._search_spaces = search_spaces

    def best(self) -> TrialResult | None:
        """Get the best trial result.

        Returns:
            TrialResult with the best metric value, or None if no trials
        """
        if not self._trials:
            return None

        sorted_trials = self._sort_trials()
        return sorted_trials[0]

    def top(self, n: int = 10) -> pd.DataFrame:
        """Get top N results as DataFrame.

        Args:
            n: Number of top results to return

        Returns:
            DataFrame with params and metrics columns
        """
        sorted_trials = self._sort_trials()
        top_trials = sorted_trials[:n]

        rows = []
        for trial in top_trials:
            row = {**trial.params, **trial.metrics}
            if isinstance(self._metric, dict):
                row["composite_score"] = self._calculate_composite_score(trial)
            rows.append(row)

        return pd.DataFrame(rows)

    def plot_heatmap(
        self,
        x_param: str,
        y_param: str,
        metric: str = "sharpe_ratio",
    ) -> go.Figure:
        """Generate heatmap of parameter space.

        Args:
            x_param: Parameter for x-axis
            y_param: Parameter for y-axis
            metric: Metric to visualize

        Returns:
            Plotly Figure object

        Raises:
            ValueError: If parameter not found in search space
        """
        if x_param not in self._search_spaces:
            raise ValueError(f"Parameter '{x_param}' not found in search spaces")
        if y_param not in self._search_spaces:
            raise ValueError(f"Parameter '{y_param}' not found in search spaces")

        x_values = sorted(set(self._search_spaces[x_param]))
        y_values = sorted(set(self._search_spaces[y_param]))

        # Create matrix for heatmap
        z_matrix = np.full((len(y_values), len(x_values)), np.nan)

        for trial in self._trials:
            x_val = trial.params.get(x_param)
            y_val = trial.params.get(y_param)

            if x_val in x_values and y_val in y_values:
                x_idx = x_values.index(x_val)
                y_idx = y_values.index(y_val)
                z_matrix[y_idx, x_idx] = trial.metrics.get(metric, np.nan)

        fig = go.Figure(
            data=go.Heatmap(
                z=z_matrix,
                x=[str(v) for v in x_values],
                y=[str(v) for v in y_values],
                colorscale="RdYlGn" if metric != "max_drawdown" else "RdYlGn_r",
                colorbar=dict(title=metric),
            )
        )

        fig.update_layout(
            title=f"{metric} by {x_param} and {y_param}",
            xaxis_title=x_param,
            yaxis_title=y_param,
        )

        return fig

    def save(self, path: str | Path) -> Path:
        """Save results to file.

        Args:
            path: Output file path (supports .json and .csv)

        Returns:
            Path to saved file
        """
        path = Path(path)
        suffix = path.suffix.lower()

        if suffix == ".csv":
            df = self.top(len(self._trials))
            df.to_csv(path, index=False)
        else:
            # Default to JSON
            if suffix != ".json":
                path = path.with_suffix(".json")

            data = {
                "metric": self._metric,
                "search_spaces": {
                    k: [_convert_to_json_serializable(v) for v in values]
                    for k, values in self._search_spaces.items()
                },
                "trials": [
                    {
                        "params": {
                            k: _convert_to_json_serializable(v)
                            for k, v in trial.params.items()
                        },
                        "metrics": trial.metrics,
                        "oos_metrics": trial.oos_metrics,
                    }
                    for trial in self._trials
                ],
            }
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

        return path

    @classmethod
    def load(cls, path: str | Path) -> "OptimizationResults":
        """Load results from file.

        Args:
            path: Input file path (supports .json)

        Returns:
            OptimizationResults object
        """
        path = Path(path)
        data = json.loads(path.read_text())

        trials = [
            TrialResult(
                params=trial["params"],
                metrics=trial["metrics"],
                oos_metrics=trial.get("oos_metrics"),
                backtest_results=None,
            )
            for trial in data["trials"]
        ]

        return cls(
            trials=trials,
            metric=data["metric"],
            search_spaces=data["search_spaces"],
        )

    @classmethod
    def load_streaming(
        cls,
        path: str | Path,
        metric: str | dict[str, float] = "sharpe_ratio",
    ) -> "OptimizationResults":
        """Load results from streaming JSONL file.

        Args:
            path: Input JSONL file path
            metric: Optimization metric for sorting results

        Returns:
            OptimizationResults object
        """
        path = Path(path)
        trials = []
        search_spaces: dict[str, set] = {}

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                trial = TrialResult(
                    params=record["params"],
                    metrics=record["metrics"],
                    oos_metrics=record.get("oos_metrics"),
                    backtest_results=None,
                )
                trials.append(trial)

                # Reconstruct search spaces from observed params
                for param_name, param_value in record["params"].items():
                    if param_name not in search_spaces:
                        search_spaces[param_name] = set()
                    search_spaces[param_name].add(param_value)

        # Convert sets to sorted lists
        search_spaces_list = {k: sorted(list(v)) for k, v in search_spaces.items()}

        return cls(
            trials=trials,
            metric=metric,
            search_spaces=search_spaces_list,
        )

    def _sort_trials(self) -> list[TrialResult]:
        """Sort trials by metric.

        Returns:
            List of TrialResult sorted by metric
        """
        if not self._trials:
            return []

        if isinstance(self._metric, dict):
            # Composite metric
            return sorted(
                self._trials,
                key=lambda t: self._calculate_composite_score(t),
                reverse=True,
            )
        else:
            # Single metric
            metric_name = self._metric
            # max_drawdown should be minimized
            reverse = metric_name != "max_drawdown"
            return sorted(
                self._trials,
                key=lambda t: t.metrics.get(
                    metric_name, float("-inf") if reverse else float("inf")
                ),
                reverse=reverse,
            )

    def _calculate_composite_score(self, trial: TrialResult) -> float:
        """Calculate composite score for a trial.

        Args:
            trial: TrialResult to score

        Returns:
            Weighted composite score
        """
        if not isinstance(self._metric, dict):
            return 0.0

        score = 0.0
        for metric_name, weight in self._metric.items():
            value = trial.metrics.get(metric_name, 0.0)
            # Normalize max_drawdown (lower is better)
            if metric_name == "max_drawdown":
                value = 1.0 - value  # Convert to "lower is worse"
            score += value * weight

        return score

    def __repr__(self) -> str:
        return f"OptimizationResults(trials={len(self._trials)}, metric={self._metric})"


def _convert_to_json_serializable(value: Any) -> Any:
    """Convert value to JSON-serializable type."""
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value
