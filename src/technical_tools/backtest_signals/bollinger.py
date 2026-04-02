"""Bollinger Band signals for backtesting."""

import pandas as pd

from .base import BaseSignal, SignalRegistry


def calculate_bollinger_bands(
    close: pd.Series, period: int = 20, std_dev: float = 2.0
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate Bollinger Bands.

    Args:
        close: Series of closing prices
        period: Moving average period (default: 20)
        std_dev: Standard deviation multiplier (default: 2.0)

    Returns:
        Tuple of (middle_band, upper_band, lower_band)
    """
    middle = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()

    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)

    return middle, upper, lower


@SignalRegistry.register("bollinger_breakout")
class BollingerBreakoutSignal(BaseSignal):
    """Bollinger Band Breakout signal - price breaks above upper band.

    A bullish momentum signal that occurs when price closes above
    the upper Bollinger Band, indicating strong upward momentum.
    """

    def __init__(
        self,
        period: int = 20,
        std_dev: float = 2.0,
        direction: str = "up",
    ) -> None:
        """Initialize BollingerBreakoutSignal.

        Args:
            period: Moving average period (default: 20)
            std_dev: Standard deviation multiplier (default: 2.0)
            direction: Breakout direction - "up" for upper band, "down" for lower band
        """
        self.period = period
        self.std_dev = std_dev
        self.direction = direction

    @property
    def name(self) -> str:
        return "bollinger_breakout"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect Bollinger Band breakout signals.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where breakout signal occurs
        """
        _, upper, lower = calculate_bollinger_bands(
            df["Close"], self.period, self.std_dev
        )

        if self.direction == "up":
            # Bullish breakout: price crosses above upper band
            signal = (df["Close"].shift(1) <= upper.shift(1)) & (df["Close"] > upper)
        else:
            # Bearish breakout (breakdown): price crosses below lower band
            signal = (df["Close"].shift(1) >= lower.shift(1)) & (df["Close"] < lower)

        return signal.fillna(False)

    def __repr__(self) -> str:
        return (
            f"BollingerBreakoutSignal(period={self.period}, "
            f"std_dev={self.std_dev}, direction='{self.direction}')"
        )


@SignalRegistry.register("bollinger_squeeze")
class BollingerSqueezeSignal(BaseSignal):
    """Bollinger Band Squeeze signal - bands contract then expand.

    A breakout signal that detects when Bollinger Bands have contracted
    (low volatility) and then start to expand, often preceding
    a significant price move.
    """

    def __init__(
        self,
        period: int = 20,
        std_dev: float = 2.0,
        squeeze_threshold: float = 0.03,
    ) -> None:
        """Initialize BollingerSqueezeSignal.

        Args:
            period: Moving average period (default: 20)
            std_dev: Standard deviation multiplier (default: 2.0)
            squeeze_threshold: Minimum band width ratio to detect squeeze (default: 0.03)
        """
        self.period = period
        self.std_dev = std_dev
        self.squeeze_threshold = squeeze_threshold

    @property
    def name(self) -> str:
        return "bollinger_squeeze"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect Bollinger Band squeeze signals.

        Signal triggers when bands were squeezed and start to expand.

        Args:
            df: DataFrame with 'Close' column

        Returns:
            Boolean Series with True where squeeze breakout signal occurs
        """
        middle, upper, lower = calculate_bollinger_bands(
            df["Close"], self.period, self.std_dev
        )

        # Band width as ratio of middle band
        band_width = (upper - lower) / middle

        # Squeeze condition: band width was below threshold
        was_squeezed = band_width.shift(1) < self.squeeze_threshold

        # Expansion: current band width is greater than previous
        is_expanding = band_width > band_width.shift(1)

        # Price breaking out upward during expansion
        price_up = df["Close"] > df["Close"].shift(1)

        signal = was_squeezed & is_expanding & price_up

        return signal.fillna(False)

    def __repr__(self) -> str:
        return (
            f"BollingerSqueezeSignal(period={self.period}, "
            f"std_dev={self.std_dev}, squeeze_threshold={self.squeeze_threshold})"
        )
