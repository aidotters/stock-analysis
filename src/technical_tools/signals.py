"""Signal detection for moving average crosses."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pandas as pd


@dataclass
class Signal:
    """Represents a trading signal.

    Attributes:
        date: Date when the signal occurred
        signal_type: Type of signal (golden_cross or dead_cross)
        price: Closing price at signal date
        short_period: Period of the short-term moving average
        long_period: Period of the long-term moving average
    """

    date: datetime
    signal_type: Literal["golden_cross", "dead_cross"]
    price: float
    short_period: int
    long_period: int


def detect_crosses(
    df: pd.DataFrame,
    short: int = 5,
    long: int = 25,
) -> list[Signal]:
    """Detect golden cross and dead cross signals.

    Golden Cross: Short-term MA crosses above long-term MA
    Dead Cross: Short-term MA crosses below long-term MA

    Args:
        df: DataFrame with SMA columns (SMA_N format)
        short: Period of short-term moving average
        long: Period of long-term moving average

    Returns:
        List of Signal objects sorted by date
    """
    short_col = f"SMA_{short}"
    long_col = f"SMA_{long}"

    # Verify required columns exist
    if short_col not in df.columns or long_col not in df.columns:
        return []

    sma_short = df[short_col]
    sma_long = df[long_col]

    # Detect crossovers
    # Golden cross: short was below or equal, now above
    cross_above = (sma_short.shift(1) <= sma_long.shift(1)) & (sma_short > sma_long)
    # Dead cross: short was above or equal, now below
    cross_below = (sma_short.shift(1) >= sma_long.shift(1)) & (sma_short < sma_long)

    signals: list[Signal] = []

    # Get golden cross signals
    for date_idx in df.index[cross_above]:
        # Convert to datetime if needed
        if isinstance(date_idx, pd.Timestamp):
            signal_date = date_idx.to_pydatetime()
        else:
            signal_date = datetime.fromisoformat(str(date_idx))

        signals.append(
            Signal(
                date=signal_date,
                signal_type="golden_cross",
                price=float(df.loc[date_idx, "Close"]),
                short_period=short,
                long_period=long,
            )
        )

    # Get dead cross signals
    for date_idx in df.index[cross_below]:
        if isinstance(date_idx, pd.Timestamp):
            signal_date = date_idx.to_pydatetime()
        else:
            signal_date = datetime.fromisoformat(str(date_idx))

        signals.append(
            Signal(
                date=signal_date,
                signal_type="dead_cross",
                price=float(df.loc[date_idx, "Close"]),
                short_period=short,
                long_period=long,
            )
        )

    # Sort by date
    return sorted(signals, key=lambda s: s.date)


def detect_crosses_multiple(
    df: pd.DataFrame,
    patterns: list[tuple[int, int]] | None = None,
) -> list[Signal]:
    """Detect crosses for multiple MA patterns.

    Args:
        df: DataFrame with SMA columns
        patterns: List of (short, long) period tuples
                 Default: [(5, 25), (25, 75)]

    Returns:
        List of all Signal objects sorted by date
    """
    if patterns is None:
        patterns = [(5, 25), (25, 75)]

    all_signals: list[Signal] = []
    for short, long in patterns:
        signals = detect_crosses(df, short=short, long=long)
        all_signals.extend(signals)

    return sorted(all_signals, key=lambda s: s.date)
