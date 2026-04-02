"""JQuants data source using market_reader package."""

from datetime import datetime, timedelta

import pandas as pd

from market_reader import DataReader
from market_reader.exceptions import StockNotFoundError as MarketReaderNotFoundError

from ..exceptions import TickerNotFoundError
from .base import DataSource

# Period to days mapping
PERIOD_DAYS = {
    "1mo": 30,
    "3mo": 90,
    "6mo": 180,
    "1y": 365,
    "2y": 730,
    "5y": 1825,
}


def _period_to_dates(period: str) -> tuple[str, str]:
    """Convert period string to start/end dates.

    Args:
        period: Period string (e.g., "1y", "6mo")

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    days = PERIOD_DAYS.get(period)
    if days is None:
        raise ValueError(
            f"Invalid period: {period}. Valid values: {list(PERIOD_DAYS.keys())}"
        )

    end = datetime.now()
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


class JQuantsSource(DataSource):
    """Data source using market_reader to access J-Quants database.

    Uses the existing market_reader package to fetch Japanese stock data
    from the local J-Quants SQLite database.
    """

    # Mapping from adjusted columns to standard OHLCV names
    ADJUSTED_COLUMN_MAP = {
        "AdjustmentOpen": "Open",
        "AdjustmentHigh": "High",
        "AdjustmentLow": "Low",
        "AdjustmentClose": "Close",
        "AdjustmentVolume": "Volume",
    }

    def __init__(self) -> None:
        """Initialize JQuantsSource."""
        self._reader = DataReader()

    def get_prices(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Retrieve stock price data from J-Quants database.

        Args:
            ticker: Stock code (4-digit Japanese stock code)
            start: Start date (YYYY-MM-DD format)
            end: End date (YYYY-MM-DD format)
            period: Alternative to start/end (e.g., "1y", "6mo")
                   Supported values: 1mo, 3mo, 6mo, 1y, 2y, 5y

        Returns:
            DataFrame with OHLCV columns and DatetimeIndex

        Raises:
            TickerNotFoundError: If ticker is not found in database
        """
        # Handle period argument
        period = kwargs.get("period")
        if period and start is None and end is None:
            start, end = _period_to_dates(str(period))

        try:
            df = self._reader.get_prices(ticker, start=start, end=end, columns="full")
        except MarketReaderNotFoundError as e:
            raise TickerNotFoundError(ticker, "jquants") from e

        if df.empty:
            raise TickerNotFoundError(ticker, "jquants")

        return self._normalize(df)

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame to standard column format using adjusted prices.

        Args:
            df: Raw DataFrame from market_reader

        Returns:
            DataFrame with standard OHLCV columns (using split-adjusted prices)
        """
        # Use adjusted prices to account for stock splits
        result = pd.DataFrame(index=df.index)

        for adj_col, std_col in self.ADJUSTED_COLUMN_MAP.items():
            if adj_col in df.columns:
                result[std_col] = df[adj_col]

        return result
