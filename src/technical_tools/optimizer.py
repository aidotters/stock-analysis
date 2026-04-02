"""StrategyOptimizer for automated strategy parameter optimization."""

from __future__ import annotations

import itertools
import json
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal

from .backtester import Backtester
from .exceptions import (
    InvalidSearchSpaceError,
    NoValidParametersError,
    OptimizationTimeoutError,
)

if TYPE_CHECKING:
    from .optimization_results import OptimizationResults, TrialResult

logger = logging.getLogger(__name__)


class StrategyOptimizer:
    """Strategy optimization engine.

    Uses Backtester internally to efficiently evaluate multiple parameter
    combinations and find optimal trading strategies.

    Example:
        >>> optimizer = StrategyOptimizer()
        >>> optimizer.add_search_space("ma_short", [5, 10, 20])
        >>> optimizer.add_search_space("ma_long", [50, 75, 100])
        >>> optimizer.add_constraint(lambda p: p["ma_short"] < p["ma_long"])
        >>> results = optimizer.run(
        ...     symbols=["7203"],
        ...     start="2023-01-01",
        ...     end="2023-12-31",
        ...     method="grid",
        ...     metric="sharpe_ratio"
        ... )
        >>> print(results.best())
    """

    def __init__(
        self,
        cash: float = 1_000_000,
        commission: float = 0.0,
    ) -> None:
        """Initialize StrategyOptimizer.

        Args:
            cash: Initial cash amount (default: 1,000,000)
            commission: Commission rate per trade (default: 0)
        """
        self._cash = cash
        self._commission = commission
        self._search_spaces: dict[str, list[Any]] = {}
        self._constraints: list[Callable[[dict[str, Any]], bool]] = []

    def add_search_space(
        self,
        name: str,
        values: list[Any],
    ) -> "StrategyOptimizer":
        """Add parameter to search space.

        Args:
            name: Parameter name
            values: List of values to search

        Returns:
            Self for method chaining

        Raises:
            ValueError: If parameter name is empty or values list is empty
        """
        if not name or not name.strip():
            raise ValueError("parameter name cannot be empty")
        if not values:
            raise ValueError("values list cannot be empty")

        self._search_spaces[name] = values
        return self

    def add_constraint(
        self,
        func: Callable[[dict[str, Any]], bool],
    ) -> "StrategyOptimizer":
        """Add parameter constraint.

        Args:
            func: Function that takes parameter dict and returns True if valid

        Returns:
            Self for method chaining
        """
        self._constraints.append(func)
        return self

    def run(
        self,
        symbols: list[str],
        start: str,
        end: str,
        method: Literal["grid", "random"] = "grid",
        n_trials: int = 100,
        metric: str | dict[str, float] = "sharpe_ratio",
        n_jobs: int = -1,
        validation: Literal["none", "walk_forward"] | None = None,
        train_ratio: float = 0.7,
        n_splits: int = 5,
        timeout: float | None = None,
        streaming_output: str | Path | None = None,
    ) -> "OptimizationResults":
        """Run optimization.

        Args:
            symbols: List of stock symbols to test
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            method: Search method ("grid" or "random")
            n_trials: Number of trials for random search
            metric: Optimization metric (string or weighted dict)
            n_jobs: Number of parallel jobs (-1 for all CPUs)
            validation: Validation method ("walk_forward" or None)
            train_ratio: Training period ratio for walk-forward
            n_splits: Number of splits for walk-forward
            timeout: Maximum time in seconds for optimization (None for no limit)
            streaming_output: Path to JSONL file for streaming results (None to disable)

        Returns:
            OptimizationResults object

        Raises:
            OptimizationTimeoutError: If optimization exceeds timeout
        """
        import time

        from .optimization_results import OptimizationResults, TrialResult  # noqa: F401

        # Generate parameter combinations
        param_sets = self._generate_param_sets(method, n_trials)

        if not param_sets:
            raise NoValidParametersError(
                "No valid parameter combinations after applying constraints"
            )

        # Run evaluations
        n_workers = os.cpu_count() if n_jobs == -1 else max(1, n_jobs)
        trials: list[TrialResult] = []
        start_time = time.time()
        total_param_sets = len(param_sets)

        # Setup streaming output
        stream_file = None
        if streaming_output is not None:
            stream_path = Path(streaming_output)
            stream_path.parent.mkdir(parents=True, exist_ok=True)
            stream_file = open(stream_path, "w", encoding="utf-8")

        def _write_stream(result: TrialResult) -> None:
            """Write trial result to streaming output."""
            if stream_file is not None:
                record = {
                    "params": result.params,
                    "metrics": result.metrics,
                    "oos_metrics": result.oos_metrics,
                }
                stream_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                stream_file.flush()

        try:
            if len(param_sets) == 1 or n_workers == 1:
                # Single-threaded execution
                for params in param_sets:
                    # Check timeout
                    if timeout is not None:
                        elapsed = time.time() - start_time
                        if elapsed >= timeout:
                            raise OptimizationTimeoutError(
                                timeout=timeout,
                                completed=len(trials),
                                total=total_param_sets,
                            )

                    try:
                        result = self._evaluate_params(
                            params,
                            symbols,
                            start,
                            end,
                            validation,
                            train_ratio,
                            n_splits,
                        )
                        trials.append(result)
                        _write_stream(result)
                    except Exception as e:
                        logger.warning(f"Evaluation failed for {params}: {e}")
            else:
                # Parallel execution
                with ThreadPoolExecutor(max_workers=n_workers) as executor:
                    futures = {
                        executor.submit(
                            self._evaluate_params,
                            params,
                            symbols,
                            start,
                            end,
                            validation,
                            train_ratio,
                            n_splits,
                        ): params
                        for params in param_sets
                    }

                    for future in as_completed(futures):
                        # Check timeout
                        if timeout is not None:
                            elapsed = time.time() - start_time
                            if elapsed >= timeout:
                                # Cancel remaining futures
                                for f in futures:
                                    f.cancel()
                                raise OptimizationTimeoutError(
                                    timeout=timeout,
                                    completed=len(trials),
                                    total=total_param_sets,
                                )

                        params = futures[future]
                        try:
                            result = future.result()
                            trials.append(result)
                            _write_stream(result)
                        except Exception as e:
                            logger.warning(f"Evaluation failed for {params}: {e}")
        finally:
            if stream_file is not None:
                stream_file.close()

        return OptimizationResults(
            trials=trials,
            metric=metric,
            search_spaces=self._search_spaces.copy(),
        )

    def _generate_param_sets(
        self,
        method: Literal["grid", "random"],
        n_trials: int,
    ) -> list[dict[str, Any]]:
        """Generate parameter combinations.

        Args:
            method: Search method
            n_trials: Number of trials for random search

        Returns:
            List of parameter dictionaries
        """
        if not self._search_spaces:
            raise InvalidSearchSpaceError("No search spaces defined")

        # Generate all combinations
        param_names = list(self._search_spaces.keys())
        param_values = list(self._search_spaces.values())
        all_combinations = list(itertools.product(*param_values))

        # Create parameter dictionaries
        all_param_sets = [dict(zip(param_names, combo)) for combo in all_combinations]

        # Apply constraints
        valid_param_sets = [
            params
            for params in all_param_sets
            if all(constraint(params) for constraint in self._constraints)
        ]

        if method == "random" and len(valid_param_sets) > n_trials:
            valid_param_sets = random.sample(valid_param_sets, n_trials)

        return valid_param_sets

    def _evaluate_params(
        self,
        params: dict[str, Any],
        symbols: list[str],
        start: str,
        end: str,
        validation: Literal["none", "walk_forward"] | None,
        train_ratio: float,
        n_splits: int,
    ) -> "TrialResult":
        """Evaluate a single parameter set.

        Args:
            params: Parameter dictionary
            symbols: Stock symbols
            start: Start date
            end: End date
            validation: Validation method
            train_ratio: Training ratio
            n_splits: Number of splits

        Returns:
            TrialResult object
        """
        from .optimization_results import TrialResult

        bt = Backtester(cash=self._cash, commission=self._commission)

        # Configure signals based on params
        self._configure_backtester(bt, params)

        # Run backtest
        results = bt.run(symbols=symbols, start=start, end=end)
        summary = results.summary()

        # Extract metrics
        metrics = {
            "total_return": summary.get("avg_return", 0.0),
            "sharpe_ratio": summary.get("sharpe_ratio", 0.0),
            "max_drawdown": summary.get("max_drawdown", 0.0),
            "win_rate": summary.get("win_rate", 0.0),
            "profit_factor": summary.get("profit_factor", 0.0),
        }

        # Handle walk-forward validation
        oos_metrics = None
        if validation == "walk_forward":
            oos_metrics = self._run_walk_forward(
                params, symbols, start, end, train_ratio, n_splits
            )

        return TrialResult(
            params=params,
            metrics=metrics,
            oos_metrics=oos_metrics,
            backtest_results=None,  # Don't store full results to save memory
        )

    def _configure_backtester(
        self,
        bt: Backtester,
        params: dict[str, Any],
    ) -> None:
        """Configure Backtester based on parameters.

        Args:
            bt: Backtester instance
            params: Parameter dictionary
        """
        # Handle MA cross signals
        if "ma_short" in params and "ma_long" in params:
            bt.add_signal(
                "golden_cross",
                short=params["ma_short"],
                long=params["ma_long"],
            )

        # Handle RSI signals
        if "rsi_threshold" in params:
            bt.add_signal("rsi_oversold", threshold=params["rsi_threshold"])

        # Handle MACD signals
        if "macd_fast" in params and "macd_slow" in params:
            bt.add_signal(
                "macd_cross",
                fast=params["macd_fast"],
                slow=params["macd_slow"],
                signal_period=params.get("macd_signal", 9),
            )

        # Handle exit rules
        if "stop_loss" in params:
            bt.add_exit_rule("stop_loss", threshold=params["stop_loss"])
        if "take_profit" in params:
            bt.add_exit_rule("take_profit", threshold=params["take_profit"])

    def _run_walk_forward(
        self,
        params: dict[str, Any],
        symbols: list[str],
        start: str,
        end: str,
        train_ratio: float,
        n_splits: int,
    ) -> dict[str, float]:
        """Run walk-forward validation.

        Args:
            params: Parameter dictionary
            symbols: Stock symbols
            start: Start date
            end: End date
            train_ratio: Training ratio
            n_splits: Number of splits

        Returns:
            Out-of-sample metrics
        """
        import pandas as pd

        start_date = pd.Timestamp(start)
        end_date = pd.Timestamp(end)
        total_days = (end_date - start_date).days
        split_days = total_days // n_splits

        oos_returns = []
        oos_sharpe = []
        oos_win_rate = []

        for i in range(n_splits):
            split_start = start_date + pd.Timedelta(days=i * split_days)
            split_end = split_start + pd.Timedelta(days=split_days)
            train_end = split_start + pd.Timedelta(days=int(split_days * train_ratio))

            # Test period
            test_start = train_end + pd.Timedelta(days=1)
            test_end = split_end

            if test_start >= test_end:
                continue

            try:
                bt = Backtester(cash=self._cash, commission=self._commission)
                self._configure_backtester(bt, params)
                results = bt.run(
                    symbols=symbols,
                    start=test_start.strftime("%Y-%m-%d"),
                    end=test_end.strftime("%Y-%m-%d"),
                )
                summary = results.summary()
                oos_returns.append(summary.get("avg_return", 0.0))
                oos_sharpe.append(summary.get("sharpe_ratio", 0.0))
                oos_win_rate.append(summary.get("win_rate", 0.0))
            except Exception as e:
                logger.debug(f"Walk-forward split {i} failed: {e}")

        if not oos_returns:
            return {
                "total_return": 0.0,
                "sharpe_ratio": 0.0,
                "win_rate": 0.0,
            }

        return {
            "total_return": sum(oos_returns) / len(oos_returns),
            "sharpe_ratio": sum(oos_sharpe) / len(oos_sharpe),
            "win_rate": sum(oos_win_rate) / len(oos_win_rate),
        }

    def __repr__(self) -> str:
        return (
            f"StrategyOptimizer(search_spaces={list(self._search_spaces.keys())}, "
            f"constraints={len(self._constraints)})"
        )
