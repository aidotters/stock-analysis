"""MACD-based signals for backtesting."""

import pandas as pd

from .base import BaseSignal, SignalRegistry


def calculate_macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate MACD (Moving Average Convergence Divergence).

    Args:
        close: Series of closing prices
        fast: Fast EMA period (default: 12)
        slow: Slow EMA period (default: 26)
        signal_period: Signal line EMA period (default: 9)

    Returns:
        Tuple of (MACD line, Signal line, Histogram)
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()

    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


@SignalRegistry.register("macd_cross")
class MACDCrossSignal(BaseSignal):
    """MACD Cross signal - MACD line crosses above signal line.

    A bullish signal that occurs when the MACD line crosses above
    the signal line.
    """

    def __init__(
        self,
        fast: int = 12,
        slow: int = 26,
        signal_period: int = 9,
    ) -> None:
        """Initialize MACDCrossSignal.

        Args:
            fast: Fast EMA period (default: 12)
            slow: Slow EMA period (default: 26)
            signal_period: Signal line EMA period (default: 9)
        """
        self.fast = fast
        self.slow = slow
        self.signal_period = signal_period

    @property
    def name(self) -> str:
        return "macd_cross"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect MACD cross signals.

        Signal triggers when MACD line crosses above signal line.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where MACD cross occurs
        """
        macd_line, signal_line, _ = calculate_macd(
            df["Close"],
            self.fast,
            self.slow,
            self.signal_period,
        )

        # Signal when MACD crosses above signal line
        signal = (macd_line.shift(1) <= signal_line.shift(1)) & (
            macd_line > signal_line
        )

        return signal.fillna(False)

    def __repr__(self) -> str:
        return (
            f"MACDCrossSignal(fast={self.fast}, slow={self.slow}, "
            f"signal_period={self.signal_period})"
        )
