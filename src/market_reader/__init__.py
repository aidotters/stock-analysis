"""market_reader - pandas_datareader-like interface for J-Quants data.

A simple, intuitive API for accessing stock price data stored in SQLite
databases collected from J-Quants API.

Example:
    >>> from market_reader import DataReader
    >>> reader = DataReader()
    >>> df = reader.get_prices("7203", start="2024-01-01", end="2024-12-31")
    >>> df.head()
                   Open    High     Low   Close    Volume  AdjustmentClose
    Date
    2024-01-04  2500.0  2550.0  2480.0  2520.0  1000000.0           2520.0
    ...
"""

from .exceptions import (
    DatabaseConnectionError,
    InvalidDateRangeError,
    StockNotFoundError,
    StockReaderError,
)
from .reader import DataReader

__all__ = [
    "DataReader",
    "StockReaderError",
    "StockNotFoundError",
    "DatabaseConnectionError",
    "InvalidDateRangeError",
]

__version__ = "0.1.0"
