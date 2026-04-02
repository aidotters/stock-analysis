"""TechnicalAnalyzer - Facade class for technical analysis."""

from typing import Any, Literal

import pandas as pd
import plotly.graph_objects as go

from .charts import create_chart
from .data_sources.base import DataSource
from .data_sources.jquants import JQuantsSource
from .data_sources.yfinance import YFinanceSource
from .indicators import (
    add_bollinger_bands,
    add_ema,
    add_macd,
    add_rsi,
    add_sma,
    calculate_indicators,
)
from .integration import load_existing_analysis
from .signals import Signal, detect_crosses, detect_crosses_multiple


class TechnicalAnalyzer:
    """Facade class for technical analysis operations.

    Provides a unified interface for:
    - Fetching price data from multiple sources
    - Calculating technical indicators
    - Detecting trading signals
    - Generating interactive charts

    Example:
        >>> analyzer = TechnicalAnalyzer(source="jquants")
        >>> fig = analyzer.plot_chart("7203", show_sma=[25, 75], show_rsi=True)
        >>> fig.show()
    """

    def __init__(self, source: Literal["jquants", "yfinance"] = "jquants") -> None:
        """Initialize TechnicalAnalyzer.

        Args:
            source: Data source to use ("jquants" or "yfinance")
        """
        self._data_source: DataSource
        if source == "jquants":
            self._data_source = JQuantsSource()
        else:
            self._data_source = YFinanceSource()

        self._source_name = source
        self._cache: dict[str, pd.DataFrame] = {}

    def get_prices(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Retrieve stock price data with caching.

        Args:
            ticker: Stock ticker symbol
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            **kwargs: Additional arguments (e.g., period for yfinance)

        Returns:
            DataFrame with OHLCV columns
        """
        # Create cache key
        cache_key = f"{ticker}_{start}_{end}_{kwargs.get('period', '')}"

        if cache_key not in self._cache:
            df = self._data_source.get_prices(ticker, start=start, end=end, **kwargs)
            self._cache[cache_key] = df

        return self._cache[cache_key].copy()

    def calculate_indicators(
        self,
        ticker: str,
        indicators: list[str],
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Calculate multiple technical indicators.

        Args:
            ticker: Stock ticker symbol
            indicators: List of indicators ("sma", "ema", "rsi", "macd", "bb")
            start: Start date
            end: End date
            **kwargs: Additional arguments for get_prices or indicators

        Returns:
            DataFrame with price data and indicator columns
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)
        return calculate_indicators(df, indicators, **kwargs)

    def add_sma(
        self,
        ticker: str,
        periods: list[int],
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Add Simple Moving Average to price data.

        Args:
            ticker: Stock ticker symbol
            periods: List of SMA periods
            start: Start date
            end: End date

        Returns:
            DataFrame with SMA columns added
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)
        return add_sma(df, periods)

    def add_ema(
        self,
        ticker: str,
        periods: list[int],
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Add Exponential Moving Average to price data.

        Args:
            ticker: Stock ticker symbol
            periods: List of EMA periods
            start: Start date
            end: End date

        Returns:
            DataFrame with EMA columns added
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)
        return add_ema(df, periods)

    def add_rsi(
        self,
        ticker: str,
        period: int = 14,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Add RSI to price data.

        Args:
            ticker: Stock ticker symbol
            period: RSI period (default: 14)
            start: Start date
            end: End date

        Returns:
            DataFrame with RSI column added
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)
        return add_rsi(df, period)

    def add_macd(
        self,
        ticker: str,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Add MACD to price data.

        Args:
            ticker: Stock ticker symbol
            fast: Fast EMA period
            slow: Slow EMA period
            signal: Signal line period
            start: Start date
            end: End date

        Returns:
            DataFrame with MACD columns added
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)
        return add_macd(df, fast, slow, signal)

    def add_bollinger_bands(
        self,
        ticker: str,
        period: int = 20,
        std: float = 2.0,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Add Bollinger Bands to price data.

        Args:
            ticker: Stock ticker symbol
            period: Moving average period
            std: Standard deviation multiplier
            start: Start date
            end: End date

        Returns:
            DataFrame with BB columns added
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)
        return add_bollinger_bands(df, period, std)

    def detect_crosses(
        self,
        ticker: str,
        short: int = 5,
        long: int = 25,
        patterns: list[tuple[int, int]] | None = None,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> list[Signal]:
        """Detect golden cross and dead cross signals.

        Args:
            ticker: Stock ticker symbol
            short: Short-term MA period (used if patterns is None)
            long: Long-term MA period (used if patterns is None)
            patterns: List of (short, long) tuples for multiple pattern detection
            start: Start date
            end: End date

        Returns:
            List of Signal objects sorted by date
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)

        # Determine which periods we need
        if patterns:
            all_periods = set()
            for short_p, long_p in patterns:
                all_periods.add(short_p)
                all_periods.add(long_p)
        else:
            all_periods = {short, long}

        # Add SMA for all required periods
        df = add_sma(df, list(all_periods))

        if patterns:
            return detect_crosses_multiple(df, patterns)
        else:
            return detect_crosses(df, short, long)

    def plot_chart(
        self,
        ticker: str,
        show_sma: list[int] | None = None,
        show_bb: bool = False,
        show_rsi: bool = False,
        show_macd: bool = False,
        show_signals: bool = False,
        signal_patterns: list[tuple[int, int]] | None = None,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> go.Figure:
        """Create interactive chart with technical indicators.

        Args:
            ticker: Stock ticker symbol
            show_sma: List of SMA periods to display
            show_bb: Whether to show Bollinger Bands
            show_rsi: Whether to show RSI subplot
            show_macd: Whether to show MACD subplot
            show_signals: Whether to show cross signals
            signal_patterns: Cross detection patterns [(short, long), ...]
            start: Start date
            end: End date

        Returns:
            Plotly Figure object
        """
        df = self.get_prices(ticker, start=start, end=end, **kwargs)

        # Add requested indicators
        if show_sma:
            df = add_sma(df, show_sma)
        if show_bb:
            df = add_bollinger_bands(df)
        if show_rsi:
            df = add_rsi(df)
        if show_macd:
            df = add_macd(df)

        # Detect signals if requested
        signals: list[Signal] | None = None
        if show_signals:
            if signal_patterns is None:
                # Default patterns
                signal_patterns = [(5, 25), (25, 75)]

            # Ensure SMAs exist for signal detection
            all_periods = set()
            for short_p, long_p in signal_patterns:
                all_periods.add(short_p)
                all_periods.add(long_p)
            df = add_sma(df, list(all_periods))

            signals = detect_crosses_multiple(df, signal_patterns)

        return create_chart(
            df,
            ticker=ticker,
            show_sma=show_sma,
            show_bb=show_bb,
            show_rsi=show_rsi,
            show_macd=show_macd,
            signals=signals,
        )

    def load_existing_analysis(self, ticker: str) -> dict[str, Any]:
        """Load existing analysis results from database.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dictionary with minervini and relative_strength results
        """
        return load_existing_analysis(ticker)
