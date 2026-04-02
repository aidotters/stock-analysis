"""RSI-based signals for backtesting."""

import pandas as pd

from .base import BaseSignal, SignalRegistry


def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI (Relative Strength Index).

    Args:
        close: Series of closing prices
        period: RSI period (default: 14)

    Returns:
        RSI values as a Series
    """
    delta = close.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi.fillna(50.0)  # Default to neutral


@SignalRegistry.register("rsi_oversold")
class RSIOversoldSignal(BaseSignal):
    """RSI Oversold signal - RSI crosses below threshold.

    A bullish signal suggesting the asset may be oversold and due for a bounce.
    """

    def __init__(self, threshold: int = 30, period: int = 14) -> None:
        """Initialize RSIOversoldSignal.

        Args:
            threshold: RSI threshold for oversold condition (default: 30)
            period: RSI calculation period (default: 14)
        """
        self.threshold = threshold
        self.period = period

    @property
    def name(self) -> str:
        return "rsi_oversold"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect RSI oversold signals.

        Signal triggers when RSI crosses below the threshold from above.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where oversold signal occurs
        """
        rsi = calculate_rsi(df["Close"], self.period)

        # Signal when RSI crosses below threshold
        signal = (rsi.shift(1) >= self.threshold) & (rsi < self.threshold)

        return signal.fillna(False)

    def __repr__(self) -> str:
        return f"RSIOversoldSignal(threshold={self.threshold}, period={self.period})"


@SignalRegistry.register("rsi_overbought")
class RSIOverboughtSignal(BaseSignal):
    """RSI Overbought signal - RSI crosses above threshold.

    A bearish signal suggesting the asset may be overbought and due for a pullback.
    """

    def __init__(self, threshold: int = 70, period: int = 14) -> None:
        """Initialize RSIOverboughtSignal.

        Args:
            threshold: RSI threshold for overbought condition (default: 70)
            period: RSI calculation period (default: 14)
        """
        self.threshold = threshold
        self.period = period

    @property
    def name(self) -> str:
        return "rsi_overbought"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect RSI overbought signals.

        Signal triggers when RSI crosses above the threshold from below.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where overbought signal occurs
        """
        rsi = calculate_rsi(df["Close"], self.period)

        # Signal when RSI crosses above threshold
        signal = (rsi.shift(1) <= self.threshold) & (rsi > self.threshold)

        return signal.fillna(False)

    def __repr__(self) -> str:
        return f"RSIOverboughtSignal(threshold={self.threshold}, period={self.period})"
