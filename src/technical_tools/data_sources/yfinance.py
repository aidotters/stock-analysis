"""YFinance data source for US and international stocks."""

import pandas as pd
import yfinance as yf

from ..exceptions import TickerNotFoundError
from .base import DataSource


class YFinanceSource(DataSource):
    """Data source using yfinance for US and international stocks.

    Supports:
    - US stocks: AAPL, MSFT, etc.
    - Japanese stocks: 7203.T, 9984.T, etc.
    - Period-based queries: 1mo, 3mo, 6mo, 1y, 2y, 5y
    - Date range queries: start/end dates
    """

    # Standard columns expected in output
    STANDARD_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

    def get_prices(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        **kwargs: object,
    ) -> pd.DataFrame:
        """Retrieve stock price data from yfinance.

        Args:
            ticker: Stock ticker symbol (e.g., "AAPL", "7203.T")
            start: Start date (YYYY-MM-DD format)
            end: End date (YYYY-MM-DD format)
            period: Alternative to start/end, e.g., "1y", "6mo"
                   (passed via kwargs)

        Returns:
            DataFrame with OHLCV columns and DatetimeIndex

        Raises:
            TickerNotFoundError: If ticker is not found
        """
        period = kwargs.get("period")

        try:
            if period:
                df = yf.download(
                    ticker,
                    period=str(period),
                    progress=False,
                    auto_adjust=False,
                )
            else:
                df = yf.download(
                    ticker,
                    start=start,
                    end=end,
                    progress=False,
                    auto_adjust=False,
                )
        except Exception as e:
            raise TickerNotFoundError(ticker, "yfinance") from e

        if df.empty:
            raise TickerNotFoundError(ticker, "yfinance")

        return self._normalize(df)

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame to standard column format.

        yfinance returns MultiIndex columns when downloading single ticker
        with format like ('Close', 'AAPL'). We need to flatten this.

        Args:
            df: Raw DataFrame from yfinance

        Returns:
            DataFrame with standard OHLCV columns
        """
        # Handle MultiIndex columns from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            # Get first level of column names (the actual column names)
            df.columns = df.columns.get_level_values(0)

        # Select only standard columns that exist
        available_cols = [c for c in self.STANDARD_COLUMNS if c in df.columns]
        result = df[available_cols].copy()

        # Ensure index is DatetimeIndex
        if not isinstance(result.index, pd.DatetimeIndex):
            result.index = pd.to_datetime(result.index)

        return result
