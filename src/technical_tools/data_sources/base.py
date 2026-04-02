"""Abstract base class for data sources."""

from abc import ABC, abstractmethod

import pandas as pd


class DataSource(ABC):
    """Abstract base class for stock data sources.

    All data sources should return DataFrame with the following columns:
    - Open: Opening price
    - High: High price
    - Low: Low price
    - Close: Closing price
    - Volume: Trading volume

    DataFrame should have DatetimeIndex.
    """

    @abstractmethod
    def get_prices(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Retrieve stock price data.

        Args:
            ticker: Stock ticker symbol
            start: Start date (YYYY-MM-DD format)
            end: End date (YYYY-MM-DD format)
            **kwargs: Additional source-specific arguments

        Returns:
            DataFrame with OHLCV columns and DatetimeIndex
        """
        pass
