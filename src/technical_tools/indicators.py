"""Technical indicator calculation functions.

Uses pandas for calculations instead of pandas-ta for simplicity.
All functions modify DataFrame in place and return it for method chaining.
"""

import pandas as pd


def add_sma(df: pd.DataFrame, periods: list[int]) -> pd.DataFrame:
    """Add Simple Moving Average columns.

    Args:
        df: DataFrame with 'Close' column
        periods: List of periods for SMA calculation

    Returns:
        DataFrame with SMA_N columns added
    """
    for period in periods:
        df[f"SMA_{period}"] = df["Close"].rolling(window=period).mean()
    return df


def add_ema(df: pd.DataFrame, periods: list[int]) -> pd.DataFrame:
    """Add Exponential Moving Average columns.

    Args:
        df: DataFrame with 'Close' column
        periods: List of periods for EMA calculation

    Returns:
        DataFrame with EMA_N columns added
    """
    for period in periods:
        df[f"EMA_{period}"] = df["Close"].ewm(span=period, adjust=False).mean()
    return df


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Add Relative Strength Index column.

    Uses the standard RSI calculation:
    RSI = 100 - (100 / (1 + RS))
    where RS = average gain / average loss

    Args:
        df: DataFrame with 'Close' column
        period: RSI period (default: 14)

    Returns:
        DataFrame with RSI_N column added
    """
    delta = df["Close"].diff()

    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    # Use exponential moving average for smoothing
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # Handle division by zero (when avg_loss is 0)
    rsi = rsi.fillna(100.0)

    df[f"RSI_{period}"] = rsi
    return df


def add_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """Add MACD (Moving Average Convergence Divergence) columns.

    MACD = EMA(fast) - EMA(slow)
    Signal = EMA(MACD, signal)
    Histogram = MACD - Signal

    Args:
        df: DataFrame with 'Close' column
        fast: Fast EMA period (default: 12)
        slow: Slow EMA period (default: 26)
        signal: Signal line EMA period (default: 9)

    Returns:
        DataFrame with MACD, MACD_Signal, MACD_Hist columns added
    """
    ema_fast = df["Close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["Close"].ewm(span=slow, adjust=False).mean()

    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_hist = macd - macd_signal

    df["MACD"] = macd
    df["MACD_Signal"] = macd_signal
    df["MACD_Hist"] = macd_hist

    return df


def add_bollinger_bands(
    df: pd.DataFrame,
    period: int = 20,
    std: float = 2.0,
) -> pd.DataFrame:
    """Add Bollinger Bands columns.

    Middle = SMA(period)
    Upper = Middle + std * StdDev(period)
    Lower = Middle - std * StdDev(period)

    Args:
        df: DataFrame with 'Close' column
        period: Moving average period (default: 20)
        std: Standard deviation multiplier (default: 2.0)

    Returns:
        DataFrame with BB_Upper, BB_Middle, BB_Lower columns added
    """
    rolling = df["Close"].rolling(window=period)
    middle = rolling.mean()
    std_dev = rolling.std()

    df["BB_Upper"] = middle + std * std_dev
    df["BB_Middle"] = middle
    df["BB_Lower"] = middle - std * std_dev

    return df


def calculate_indicators(
    df: pd.DataFrame,
    indicators: list[str],
    sma_periods: list[int] | None = None,
    ema_periods: list[int] | None = None,
    rsi_period: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    bb_period: int = 20,
    bb_std: float = 2.0,
) -> pd.DataFrame:
    """Calculate multiple technical indicators.

    Args:
        df: DataFrame with 'Close' column
        indicators: List of indicators to calculate.
                   Valid values: "sma", "ema", "rsi", "macd", "bb"
        sma_periods: Periods for SMA (default: [5, 25, 75])
        ema_periods: Periods for EMA (default: [12, 26])
        rsi_period: RSI period
        macd_fast: MACD fast period
        macd_slow: MACD slow period
        macd_signal: MACD signal period
        bb_period: Bollinger Bands period
        bb_std: Bollinger Bands standard deviation

    Returns:
        DataFrame with requested indicator columns added
    """
    if sma_periods is None:
        sma_periods = [5, 25, 75]
    if ema_periods is None:
        ema_periods = [12, 26]

    result = df.copy()

    for indicator in indicators:
        indicator_lower = indicator.lower()
        if indicator_lower == "sma":
            result = add_sma(result, sma_periods)
        elif indicator_lower == "ema":
            result = add_ema(result, ema_periods)
        elif indicator_lower == "rsi":
            result = add_rsi(result, rsi_period)
        elif indicator_lower == "macd":
            result = add_macd(result, macd_fast, macd_slow, macd_signal)
        elif indicator_lower == "bb":
            result = add_bollinger_bands(result, bb_period, bb_std)

    return result
