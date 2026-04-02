"""Backtester class for running backtests on trading strategies."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
from backtesting import Backtest, Strategy

from market_reader import DataReader

from .backtest_results import BacktestResults, Trade
from .backtest_signals import SignalRegistry
from .exceptions import (
    BacktestError,
    BacktestInsufficientDataError,
    InvalidRuleError,
    InvalidSignalError,
)
from .screener import ScreenerFilter, StockScreener

logger = logging.getLogger(__name__)


# Valid entry and exit rules
VALID_ENTRY_RULES = {"next_day_open"}
VALID_EXIT_RULES = {"stop_loss", "take_profit", "max_holding_days", "trailing_stop"}


class SignalStrategy(Strategy):
    """Strategy class for backtesting.py integration.

    This class wraps our signal-based logic to work with
    the backtesting.py framework.
    """

    # Class-level parameters set by Backtester
    signal_series: pd.Series
    stop_loss: float | None = None
    take_profit: float | None = None
    max_holding_days: int | None = None
    trailing_stop: float | None = None

    def init(self) -> None:
        """Initialize strategy indicators."""
        # Signal series is pre-computed and passed via class attribute
        self.signal = self.I(lambda: self.signal_series.values, name="Signal")
        # Track entry bar index and high watermark for trailing stop
        self._entry_bar_index: int | None = None
        self._high_watermark: float = 0.0
        self._entry_price: float = 0.0

    def next(self) -> None:
        """Execute trading logic for each bar."""
        # Check for entry signal
        if self.signal[-1] and not self.position:
            self.buy()
            # Track entry bar index for max_holding_days calculation
            self._entry_bar_index = len(self.data) - 1
            # Reset high watermark for trailing stop
            self._high_watermark = self.data.Close[-1]
            # Track entry price
            self._entry_price = self.data.Close[-1]

        # Check exit conditions
        if self.position:
            entry_price = self._entry_price

            # Stop loss
            if self.stop_loss is not None:
                stop_price = entry_price * (1 + self.stop_loss)
                if self.data.Close[-1] <= stop_price:
                    self._reset_tracking()
                    self.position.close()
                    return

            # Take profit
            if self.take_profit is not None:
                target_price = entry_price * (1 + self.take_profit)
                if self.data.Close[-1] >= target_price:
                    self._reset_tracking()
                    self.position.close()
                    return

            # Max holding days
            if self.max_holding_days is not None and self._entry_bar_index is not None:
                current_bar_index = len(self.data) - 1
                bars_held = current_bar_index - self._entry_bar_index
                if bars_held >= self.max_holding_days:
                    self._reset_tracking()
                    self.position.close()
                    return

            # Trailing stop
            if self.trailing_stop is not None:
                # Update high watermark
                self._high_watermark = max(self._high_watermark, self.data.Close[-1])

                trail_price = self._high_watermark * (1 + self.trailing_stop)
                if self.data.Close[-1] <= trail_price:
                    self._reset_tracking()
                    self.position.close()
                    return

    def _reset_tracking(self) -> None:
        """Reset tracking variables when position is closed."""
        self._entry_bar_index = None
        self._high_watermark = 0.0
        self._entry_price = 0.0


class Backtester:
    """Main backtesting engine.

    Provides a simple API for configuring and running backtests
    on Japanese stocks using various technical signals.

    Example:
        >>> bt = Backtester()
        >>> bt.add_signal("golden_cross", short=5, long=25)
        >>> bt.add_exit_rule("stop_loss", threshold=-0.10)
        >>> results = bt.run(symbols=["7203"], start="2023-01-01", end="2023-12-31")
        >>> print(results.summary())
    """

    def __init__(
        self,
        cash: float = 1_000_000,
        commission: float = 0.0,
    ) -> None:
        """Initialize Backtester.

        Args:
            cash: Initial cash amount (default: 1,000,000)
            commission: Commission rate per trade (default: 0)
        """
        self._cash = cash
        self._commission = commission
        self._signals: list[dict[str, Any]] = []
        self._entry_rules: list[dict[str, Any]] = []
        self._exit_rules: list[dict[str, Any]] = []
        self._reader = DataReader()

    def add_signal(self, signal_name: str, **params: Any) -> "Backtester":
        """Add a trading signal.

        Args:
            signal_name: Name of the signal (e.g., "golden_cross", "rsi_oversold")
            **params: Signal-specific parameters

        Returns:
            Self for method chaining

        Raises:
            InvalidSignalError: If signal name is not recognized
        """
        signal_cls = SignalRegistry.get(signal_name)
        if signal_cls is None:
            available = ", ".join(SignalRegistry.list_signals())
            raise InvalidSignalError(signal_name, f"Available signals: {available}")

        self._signals.append({"name": signal_name, "params": params})
        return self

    def add_entry_rule(self, rule_name: str, **params: Any) -> "Backtester":
        """Add an entry rule.

        Args:
            rule_name: Name of the entry rule (e.g., "next_day_open")
            **params: Rule-specific parameters

        Returns:
            Self for method chaining

        Raises:
            InvalidRuleError: If rule name is not recognized
        """
        if rule_name not in VALID_ENTRY_RULES:
            available = ", ".join(VALID_ENTRY_RULES)
            raise InvalidRuleError(rule_name, f"Available entry rules: {available}")

        self._entry_rules.append({"name": rule_name, "params": params})
        return self

    def add_exit_rule(self, rule_name: str, **params: Any) -> "Backtester":
        """Add an exit rule.

        Args:
            rule_name: Name of the exit rule
                (e.g., "stop_loss", "take_profit", "max_holding_days")
            **params: Rule-specific parameters

        Returns:
            Self for method chaining

        Raises:
            InvalidRuleError: If rule name is not recognized or params invalid
        """
        if rule_name not in VALID_EXIT_RULES:
            available = ", ".join(VALID_EXIT_RULES)
            raise InvalidRuleError(rule_name, f"Available exit rules: {available}")

        # Validate parameters
        if rule_name == "stop_loss":
            threshold = params.get("threshold")
            if threshold is not None and threshold > 0:
                raise InvalidRuleError(
                    rule_name, "stop_loss threshold must be negative (e.g., -0.10)"
                )
        elif rule_name == "take_profit":
            threshold = params.get("threshold")
            if threshold is not None and threshold < 0:
                raise InvalidRuleError(
                    rule_name, "take_profit threshold must be positive (e.g., 0.20)"
                )

        self._exit_rules.append({"name": rule_name, "params": params})
        return self

    def run(
        self,
        symbols: list[str],
        start: str,
        end: str,
        max_workers: int | None = None,
    ) -> BacktestResults:
        """Run backtest for specified symbols and period.

        Args:
            symbols: List of stock symbols to test
            start: Start date (YYYY-MM-DD format)
            end: End date (YYYY-MM-DD format)
            max_workers: Maximum parallel workers (default: min(len(symbols), 4))

        Returns:
            BacktestResults object with performance metrics

        Raises:
            BacktestError: If no signals configured
            BacktestInsufficientDataError: If insufficient data for backtest
        """
        if not self._signals:
            raise BacktestError("No signals configured. Use add_signal() first.")

        if max_workers is None:
            max_workers = min(len(symbols), 4)

        all_trades: list[Trade] = []
        all_equity_curves: list[pd.Series] = []

        if len(symbols) == 1 or max_workers == 1:
            # Single-threaded execution
            for symbol in symbols:
                try:
                    trades, equity = self._run_single(symbol, start, end)
                    all_trades.extend(trades)
                    all_equity_curves.append(equity)
                except BacktestInsufficientDataError:
                    raise
                except Exception as e:
                    logger.warning(f"Error backtesting {symbol}: {e}")
        else:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._run_single, symbol, start, end): symbol
                    for symbol in symbols
                }

                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        trades, equity = future.result()
                        all_trades.extend(trades)
                        all_equity_curves.append(equity)
                    except BacktestInsufficientDataError:
                        raise
                    except Exception as e:
                        logger.warning(f"Error backtesting {symbol}: {e}")

        # Combine equity curves
        if all_equity_curves:
            combined_equity = self._combine_equity_curves(all_equity_curves)
        else:
            combined_equity = pd.Series(dtype=float)

        return BacktestResults(
            trades=all_trades,
            equity_curve=combined_equity,
            initial_cash=self._cash,
        )

    def _run_single(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> tuple[list[Trade], pd.Series]:
        """Run backtest for a single symbol.

        Args:
            symbol: Stock symbol
            start: Start date
            end: End date

        Returns:
            Tuple of (trades list, equity curve)
        """
        # Get price data
        df = self._reader.get_prices(symbol, start=start, end=end)

        if df.empty or len(df) < 30:
            raise BacktestInsufficientDataError(
                symbol=symbol,
                required=30,
                actual=len(df),
            )

        # Prepare data for backtesting.py (requires specific column names)
        bt_data = df[["Open", "High", "Low", "Close", "Volume"]].copy()

        # Remove NaN values (backtesting.py requires clean data)
        bt_data = bt_data.dropna()

        if len(bt_data) < 30:
            raise BacktestInsufficientDataError(
                symbol=symbol,
                required=30,
                actual=len(bt_data),
            )

        # Generate combined signal (use cleaned data)
        df_clean = df.loc[bt_data.index]
        signal_series = self._generate_signals(df_clean)

        # Configure strategy
        strategy_class = self._create_strategy_class(signal_series)

        # Run backtest
        bt = Backtest(
            bt_data,
            strategy_class,
            cash=self._cash,
            commission=self._commission,
            exclusive_orders=True,
            finalize_trades=True,
        )

        stats = bt.run()

        # Extract trades
        trades = self._extract_trades(symbol, stats)

        # Extract equity curve
        equity_curve = pd.Series(stats["_equity_curve"]["Equity"])

        return trades, equity_curve

    def _generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """Generate combined signal series from all configured signals.

        Args:
            df: Price DataFrame

        Returns:
            Boolean Series with combined signals
        """
        combined_signal = pd.Series(False, index=df.index)

        for signal_config in self._signals:
            signal_cls = SignalRegistry.get(signal_config["name"])
            if signal_cls is None:
                continue

            signal = signal_cls(**signal_config["params"])
            signal_series = signal.detect(df)
            combined_signal = combined_signal | signal_series

        return combined_signal

    def _create_strategy_class(self, signal_series: pd.Series) -> type[Strategy]:
        """Create a strategy class with configured parameters.

        Args:
            signal_series: Pre-computed signal series

        Returns:
            Strategy subclass with configured parameters
        """

        class ConfiguredStrategy(SignalStrategy):
            pass

        # Set signal series
        ConfiguredStrategy.signal_series = signal_series

        # Set exit rules
        for rule in self._exit_rules:
            rule_name = rule["name"]
            params = rule["params"]

            if rule_name == "stop_loss":
                ConfiguredStrategy.stop_loss = params.get("threshold", -0.10)
            elif rule_name == "take_profit":
                ConfiguredStrategy.take_profit = params.get("threshold", 0.20)
            elif rule_name == "max_holding_days":
                ConfiguredStrategy.max_holding_days = params.get("days", 30)
            elif rule_name == "trailing_stop":
                ConfiguredStrategy.trailing_stop = params.get("threshold", -0.05)

        return ConfiguredStrategy

    def _extract_trades(self, symbol: str, stats: Any) -> list[Trade]:
        """Extract Trade objects from backtest stats.

        Args:
            symbol: Stock symbol
            stats: Backtest statistics object

        Returns:
            List of Trade objects
        """
        trades: list[Trade] = []

        # backtesting.py provides _trades DataFrame
        trades_df = stats.get("_trades")
        if trades_df is None or trades_df.empty:
            return trades

        for _, row in trades_df.iterrows():
            entry_date = row["EntryTime"]
            exit_date = row["ExitTime"]

            # Convert to datetime if needed
            if isinstance(entry_date, pd.Timestamp):
                entry_date = entry_date.to_pydatetime()
            if isinstance(exit_date, pd.Timestamp):
                exit_date = exit_date.to_pydatetime()

            entry_price = row["EntryPrice"]
            exit_price = row["ExitPrice"]
            size = int(abs(row["Size"]))
            pnl = row["PnL"]
            return_pct = row["ReturnPct"] / 100  # Convert to decimal

            # Calculate holding days
            holding_days = (exit_date - entry_date).days

            # Determine exit reason
            exit_reason = self._determine_exit_reason(
                entry_price, exit_price, holding_days
            )

            trades.append(
                Trade(
                    symbol=symbol,
                    entry_date=entry_date,
                    entry_price=entry_price,
                    exit_date=exit_date,
                    exit_price=exit_price,
                    shares=size,
                    pnl=pnl,
                    return_pct=return_pct,
                    holding_days=holding_days,
                    exit_reason=exit_reason,
                )
            )

        return trades

    def _determine_exit_reason(
        self,
        entry_price: float,
        exit_price: float,
        holding_days: int | None = None,
    ) -> str:
        """Determine the reason for exiting a trade.

        Args:
            entry_price: Entry price
            exit_price: Exit price
            holding_days: Number of days position was held (optional)

        Returns:
            Exit reason string
        """
        return_pct = (exit_price - entry_price) / entry_price

        # Check stop loss
        for rule in self._exit_rules:
            if rule["name"] == "stop_loss":
                threshold = rule["params"].get("threshold", -0.10)
                if return_pct <= threshold:
                    return "stop_loss"

        # Check take profit
        for rule in self._exit_rules:
            if rule["name"] == "take_profit":
                threshold = rule["params"].get("threshold", 0.20)
                if return_pct >= threshold:
                    return "take_profit"

        # Check max holding days
        if holding_days is not None:
            for rule in self._exit_rules:
                if rule["name"] == "max_holding_days":
                    max_days = rule["params"].get("days", 30)
                    if holding_days >= max_days:
                        return "max_holding_days"

        return "signal_exit"

    def _combine_equity_curves(
        self,
        equity_curves: list[pd.Series],
    ) -> pd.Series:
        """Combine multiple equity curves into one.

        Args:
            equity_curves: List of equity curve Series

        Returns:
            Combined equity curve
        """
        if not equity_curves:
            return pd.Series(dtype=float)

        if len(equity_curves) == 1:
            return equity_curves[0]

        # Simple sum of all equity curves (treating as if all run in parallel)
        # Reindex to common date range
        all_dates: set[pd.Timestamp] = set()
        for ec in equity_curves:
            all_dates.update(ec.index)

        date_index = pd.DatetimeIndex(sorted(all_dates))
        combined = pd.Series(0.0, index=date_index)

        for ec in equity_curves:
            reindexed = ec.reindex(date_index).ffill().bfill()
            combined = combined + reindexed

        # Normalize to initial cash
        combined = combined / len(equity_curves)

        return combined

    def run_with_screener(
        self,
        screener_filter: ScreenerFilter | dict,
        start: str,
        end: str,
        entry_rule: str = "next_day_open",
        exit_rules: dict[str, float] | None = None,
        screener: StockScreener | None = None,
        max_workers: int | None = None,
    ) -> BacktestResults:
        """Run backtest with stocks from screener filter.

        This method simulates buying stocks that match the screener filter
        on each trading day during the period, allowing you to test how
        well the screener conditions predict future performance.

        Args:
            screener_filter: ScreenerFilter object or dict with filter params
            start: Start date (YYYY-MM-DD format)
            end: End date (YYYY-MM-DD format)
            entry_rule: Entry rule name (default: "next_day_open")
            exit_rules: Dict of exit rules and thresholds
                (e.g., {"stop_loss": -0.10, "take_profit": 0.20})
            screener: Optional StockScreener instance
            max_workers: Maximum parallel workers

        Returns:
            BacktestResults object with performance metrics

        Example:
            >>> bt = Backtester()
            >>> results = bt.run_with_screener(
            ...     screener_filter={"composite_score_min": 70, "hl_ratio_min": 80},
            ...     start="2023-01-01",
            ...     end="2024-12-31",
            ...     exit_rules={"stop_loss": -0.10, "take_profit": 0.20}
            ... )
        """
        if screener is None:
            screener = StockScreener()

        # Build filter
        if isinstance(screener_filter, dict):
            filter_obj = ScreenerFilter(**screener_filter)
        else:
            filter_obj = screener_filter

        # Get screener results
        results_df = screener.filter(filter_obj)

        if results_df.empty:
            logger.warning("No stocks matched the screener filter")
            return BacktestResults(
                trades=[],
                equity_curve=pd.Series(dtype=float),
                initial_cash=self._cash,
            )

        # Extract symbols
        symbols = results_df["code"].tolist()
        logger.info(f"Running backtest on {len(symbols)} symbols from screener")

        # Clear existing signals and add screener-based signal
        # For screener-based backtest, we use a simple "always buy" approach
        # since the screener already filtered the stocks
        self._signals = []
        self.add_signal(
            "golden_cross", short=1, long=2
        )  # Triggers on any price movement

        # Set entry rule
        self._entry_rules = []
        self.add_entry_rule(entry_rule)

        # Set exit rules
        self._exit_rules = []
        if exit_rules:
            for rule_name, threshold in exit_rules.items():
                if rule_name == "stop_loss":
                    self.add_exit_rule("stop_loss", threshold=threshold)
                elif rule_name == "take_profit":
                    self.add_exit_rule("take_profit", threshold=threshold)
                elif rule_name == "max_holding_days":
                    self.add_exit_rule("max_holding_days", days=int(threshold))
                elif rule_name == "trailing_stop":
                    self.add_exit_rule("trailing_stop", threshold=threshold)

        # Run the backtest
        return self.run(symbols=symbols, start=start, end=end, max_workers=max_workers)

    def __repr__(self) -> str:
        signal_names = [s["name"] for s in self._signals]
        return f"Backtester(signals={signal_names}, cash={self._cash})"
