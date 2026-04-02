"""Custom exceptions for technical_tools package."""


class TechnicalToolsError(Exception):
    """Base exception for technical_tools package."""

    pass


class DataSourceError(TechnicalToolsError):
    """Error occurred while fetching data."""

    pass


class TickerNotFoundError(DataSourceError):
    """Specified ticker was not found."""

    def __init__(self, ticker: str, source: str) -> None:
        self.ticker = ticker
        self.source = source
        super().__init__(f"Ticker '{ticker}' not found in {source}")


class InsufficientDataError(TechnicalToolsError):
    """Not enough data for calculation."""

    def __init__(self, required: int, actual: int) -> None:
        self.required = required
        self.actual = actual
        super().__init__(f"Insufficient data: required {required} rows, got {actual}")


# Backtest-related exceptions


class BacktestError(TechnicalToolsError):
    """Base exception for backtest-related errors."""

    pass


class BacktestInsufficientDataError(BacktestError):
    """Insufficient data for backtesting."""

    def __init__(self, symbol: str, required: int, actual: int) -> None:
        self.symbol = symbol
        self.required = required
        self.actual = actual
        super().__init__(
            f"Insufficient data for {symbol}: required {required} rows, got {actual}"
        )


class InvalidSignalError(BacktestError):
    """Invalid signal name or parameters."""

    def __init__(self, signal_name: str, message: str | None = None) -> None:
        self.signal_name = signal_name
        msg = f"Invalid signal: '{signal_name}'"
        if message:
            msg += f" - {message}"
        super().__init__(msg)


class InvalidRuleError(BacktestError):
    """Invalid rule name or parameters."""

    def __init__(self, rule_name: str, message: str | None = None) -> None:
        self.rule_name = rule_name
        msg = f"Invalid rule: '{rule_name}'"
        if message:
            msg += f" - {message}"
        super().__init__(msg)


# Portfolio-related exceptions


class PortfolioError(TechnicalToolsError):
    """Base exception for portfolio-related errors."""

    pass


# Optimizer-related exceptions


class OptimizerError(TechnicalToolsError):
    """Base exception for optimizer-related errors."""

    pass


class InvalidSearchSpaceError(OptimizerError):
    """Invalid search space definition."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class NoValidParametersError(OptimizerError):
    """No valid parameter combinations after applying constraints."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class OptimizationTimeoutError(OptimizerError):
    """Optimization exceeded the specified timeout."""

    def __init__(self, timeout: float, completed: int, total: int) -> None:
        self.timeout = timeout
        self.completed = completed
        self.total = total
        super().__init__(
            f"Optimization timed out after {timeout}s: "
            f"completed {completed}/{total} trials"
        )
