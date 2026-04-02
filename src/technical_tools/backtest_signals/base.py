"""Base signal class for backtesting."""

from abc import ABC, abstractmethod

import pandas as pd


class BaseSignal(ABC):
    """Abstract base class for all trading signals.

    All signal classes must implement the detect() method that returns
    a boolean Series indicating when the signal is triggered.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the signal name."""
        pass

    @abstractmethod
    def detect(self, df: pd.DataFrame) -> pd.Series:
        """Detect signal occurrences in the price data.

        Args:
            df: DataFrame with OHLCV data (must have 'Close' column)

        Returns:
            Boolean Series with True where signal is triggered
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class SignalRegistry:
    """Registry for signal classes."""

    _signals: dict[str, type[BaseSignal]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a signal class."""

        def decorator(signal_cls: type[BaseSignal]):
            cls._signals[name] = signal_cls
            return signal_cls

        return decorator

    @classmethod
    def get(cls, name: str) -> type[BaseSignal] | None:
        """Get signal class by name."""
        return cls._signals.get(name)

    @classmethod
    def list_signals(cls) -> list[str]:
        """List all registered signal names."""
        return list(cls._signals.keys())
