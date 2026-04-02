"""Custom exceptions for stock_reader package."""


class StockReaderError(Exception):
    """Base exception for stock_reader package."""

    pass


class StockNotFoundError(StockReaderError):
    """Raised when specified stock code is not found in database."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"Stock code not found: {code}")


class DatabaseConnectionError(StockReaderError):
    """Raised when database connection fails."""

    def __init__(self, path: str, original_error: Exception | None = None) -> None:
        self.path = str(path)
        self.original_error = original_error
        message = f"Failed to connect to database: {path}"
        if original_error:
            message += f" ({original_error})"
        super().__init__(message)


class InvalidDateRangeError(StockReaderError):
    """Raised when invalid date range is specified."""

    def __init__(self, start: str, end: str) -> None:
        self.start = start
        self.end = end
        super().__init__(f"Invalid date range: start ({start}) is after end ({end})")
