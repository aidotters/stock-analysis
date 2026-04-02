"""Volume-based signals for backtesting."""

import pandas as pd

from .base import BaseSignal, SignalRegistry


@SignalRegistry.register("volume_spike")
class VolumeSpikeSignal(BaseSignal):
    """Volume Spike signal - volume exceeds moving average by threshold.

    A signal that detects unusual volume activity, often indicating
    institutional interest or significant market events.
    """

    def __init__(
        self,
        period: int = 20,
        threshold: float = 2.0,
        price_direction: str | None = None,
    ) -> None:
        """Initialize VolumeSpikeSignal.

        Args:
            period: Period for volume moving average (default: 20)
            threshold: Multiplier for volume spike detection (default: 2.0)
            price_direction: Optional filter - "up" for bullish, "down" for bearish,
                           None for any direction
        """
        self.period = period
        self.threshold = threshold
        self.price_direction = price_direction

    @property
    def name(self) -> str:
        return "volume_spike"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect volume spike signals.

        Args:
            df: DataFrame with 'Close' and 'Volume' columns

        Returns:
            Boolean Series with True where volume spike signal occurs
        """
        if "Volume" not in df.columns:
            return pd.Series(False, index=df.index)

        volume_ma = df["Volume"].rolling(window=self.period).mean()

        # Volume exceeds threshold times the average
        volume_spike = df["Volume"] > (volume_ma * self.threshold)

        if self.price_direction is None:
            signal = volume_spike
        elif self.price_direction == "up":
            # Bullish volume spike: high volume with price up
            price_up = df["Close"] > df["Close"].shift(1)
            signal = volume_spike & price_up
        else:
            # Bearish volume spike: high volume with price down
            price_down = df["Close"] < df["Close"].shift(1)
            signal = volume_spike & price_down

        return signal.fillna(False)

    def __repr__(self) -> str:
        return (
            f"VolumeSpikeSignal(period={self.period}, "
            f"threshold={self.threshold}, price_direction={self.price_direction!r})"
        )


@SignalRegistry.register("volume_breakout")
class VolumeBreakoutSignal(BaseSignal):
    """Volume Breakout signal - price breakout with volume confirmation.

    Detects price breaking above recent highs with above-average volume,
    a classic confirmation of breakout validity.
    """

    def __init__(
        self,
        price_period: int = 20,
        volume_period: int = 20,
        volume_threshold: float = 1.5,
    ) -> None:
        """Initialize VolumeBreakoutSignal.

        Args:
            price_period: Period for price high/low lookback (default: 20)
            volume_period: Period for volume moving average (default: 20)
            volume_threshold: Minimum volume ratio vs average (default: 1.5)
        """
        self.price_period = price_period
        self.volume_period = volume_period
        self.volume_threshold = volume_threshold

    @property
    def name(self) -> str:
        return "volume_breakout"

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect volume-confirmed breakout signals.

        Args:
            df: DataFrame with 'Close', 'High', and 'Volume' columns

        Returns:
            Boolean Series with True where volume breakout signal occurs
        """
        if "Volume" not in df.columns:
            return pd.Series(False, index=df.index)

        # Price breakout: close above recent high
        recent_high = df["High"].rolling(window=self.price_period).max().shift(1)
        price_breakout = df["Close"] > recent_high

        # Volume confirmation: volume above average
        volume_ma = df["Volume"].rolling(window=self.volume_period).mean()
        volume_confirm = df["Volume"] > (volume_ma * self.volume_threshold)

        signal = price_breakout & volume_confirm

        return signal.fillna(False)

    def __repr__(self) -> str:
        return (
            f"VolumeBreakoutSignal(price_period={self.price_period}, "
            f"volume_period={self.volume_period}, "
            f"volume_threshold={self.volume_threshold})"
        )
