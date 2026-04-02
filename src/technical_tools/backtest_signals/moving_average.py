"""Moving average cross signals for backtesting."""

import pandas as pd

from .base import BaseSignal, SignalRegistry


@SignalRegistry.register("golden_cross")
class GoldenCrossSignal(BaseSignal):
    """Golden Cross signal - short MA crosses above long MA.

    A bullish signal that occurs when a shorter-term moving average
    crosses above a longer-term moving average.
    """

    def __init__(self, short: int = 5, long: int = 25) -> None:
        """Initialize GoldenCrossSignal.

        Args:
            short: Period for short-term moving average (default: 5)
            long: Period for long-term moving average (default: 25)
        """
        self.short = short
        self.long = long

    @property
    def name(self) -> str:
        return "golden_cross"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect golden cross signals.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where golden cross occurs
        """
        sma_short = df["Close"].rolling(window=self.short).mean()
        sma_long = df["Close"].rolling(window=self.long).mean()

        # Golden cross: short was below or equal, now above
        signal = (sma_short.shift(1) <= sma_long.shift(1)) & (sma_short > sma_long)

        return signal.fillna(False)

    def __repr__(self) -> str:
        return f"GoldenCrossSignal(short={self.short}, long={self.long})"


@SignalRegistry.register("dead_cross")
class DeadCrossSignal(BaseSignal):
    """Dead Cross signal - short MA crosses below long MA.

    A bearish signal that occurs when a shorter-term moving average
    crosses below a longer-term moving average.
    """

    def __init__(self, short: int = 5, long: int = 25) -> None:
        """Initialize DeadCrossSignal.

        Args:
            short: Period for short-term moving average (default: 5)
            long: Period for long-term moving average (default: 25)
        """
        self.short = short
        self.long = long

    @property
    def name(self) -> str:
        return "dead_cross"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect dead cross signals.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where dead cross occurs
        """
        sma_short = df["Close"].rolling(window=self.short).mean()
        sma_long = df["Close"].rolling(window=self.long).mean()

        # Dead cross: short was above or equal, now below
        signal = (sma_short.shift(1) >= sma_long.shift(1)) & (sma_short < sma_long)

        return signal.fillna(False)

    def __repr__(self) -> str:
        return f"DeadCrossSignal(short={self.short}, long={self.long})"
