"""technical_tools - Technical analysis tools for Jupyter Notebook.

A unified interface for Japanese and US stock technical analysis,
providing:
- Data fetching from J-Quants (via market_reader) and yfinance
- Technical indicator calculation (SMA, EMA, RSI, MACD, Bollinger Bands)
- Signal detection (Golden Cross, Dead Cross)
- Interactive chart generation with plotly
- Stock screening with integrated analysis data
- Backtesting with various trading signals
- Virtual portfolio tracking

Example:
    >>> from technical_tools import TechnicalAnalyzer, StockScreener, ScreenerFilter
    >>> analyzer = TechnicalAnalyzer(source="jquants")
    >>> fig = analyzer.plot_chart("7203", show_sma=[25, 75], show_rsi=True)
    >>> fig.show()

    >>> screener = StockScreener()
    >>> results = screener.filter(composite_score_min=70.0, hl_ratio_min=80)

    # Using ScreenerFilter for structured parameter handling
    >>> config = ScreenerFilter(composite_score_min=70.0, market_cap_min=100_000_000_000)
    >>> results = screener.filter(config)

    # Backtesting
    >>> from technical_tools import Backtester
    >>> bt = Backtester()
    >>> bt.add_signal("golden_cross", short=5, long=25)
    >>> bt.add_exit_rule("stop_loss", threshold=-0.10)
    >>> results = bt.run(symbols=["7203"], start="2023-01-01", end="2023-12-31")
    >>> print(results.summary())

    # Virtual Portfolio
    >>> from technical_tools import VirtualPortfolio
    >>> vp = VirtualPortfolio("my_strategy")
    >>> vp.buy("7203", shares=100, price=2500)
    >>> print(vp.summary())
"""

from .analyzer import TechnicalAnalyzer
from .backtest_results import BacktestResults, Trade
from .backtester import Backtester
from .exceptions import (
    BacktestError,
    BacktestInsufficientDataError,
    DataSourceError,
    InsufficientDataError,
    InvalidRuleError,
    InvalidSearchSpaceError,
    InvalidSignalError,
    NoValidParametersError,
    OptimizationTimeoutError,
    OptimizerError,
    PortfolioError,
    TechnicalToolsError,
    TickerNotFoundError,
)
from .optimization_results import OptimizationResults, TrialResult
from .optimizer import StrategyOptimizer
from .screener import ScreenerFilter, StockScreener
from .signals import Signal
from .virtual_portfolio import VirtualPortfolio

__all__ = [
    # Existing
    "TechnicalAnalyzer",
    "StockScreener",
    "ScreenerFilter",
    "Signal",
    # Backtest
    "Backtester",
    "BacktestResults",
    "Trade",
    "VirtualPortfolio",
    # Optimizer
    "StrategyOptimizer",
    "OptimizationResults",
    "TrialResult",
    # Exceptions
    "TechnicalToolsError",
    "DataSourceError",
    "TickerNotFoundError",
    "InsufficientDataError",
    "BacktestError",
    "BacktestInsufficientDataError",
    "InvalidSignalError",
    "InvalidRuleError",
    "PortfolioError",
    "OptimizerError",
    "InvalidSearchSpaceError",
    "NoValidParametersError",
    "OptimizationTimeoutError",
]

__version__ = "0.2.0"
