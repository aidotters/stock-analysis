"""Backtest signal classes for technical analysis.

This module provides a plugin-style architecture for trading signals
used in backtesting. Each signal class implements the BaseSignal
interface and is registered with the SignalRegistry.

Supported signals:
- golden_cross: Short MA crosses above long MA
- dead_cross: Short MA crosses below long MA
- rsi_oversold: RSI crosses below threshold (e.g., 30)
- rsi_overbought: RSI crosses above threshold (e.g., 70)
- macd_cross: MACD line crosses above signal line
- bollinger_breakout: Price breaks above/below Bollinger Bands
- bollinger_squeeze: Bollinger Bands contract then expand
- volume_spike: Volume exceeds moving average threshold
- volume_breakout: Price breakout with volume confirmation

Example:
    >>> from technical_tools.backtest_signals import SignalRegistry, GoldenCrossSignal
    >>> signal_cls = SignalRegistry.get("golden_cross")
    >>> signal = signal_cls(short=5, long=25)
    >>> signals = signal.detect(price_df)
"""

from .base import BaseSignal, SignalRegistry
from .bollinger import BollingerBreakoutSignal, BollingerSqueezeSignal
from .macd import MACDCrossSignal
from .moving_average import DeadCrossSignal, GoldenCrossSignal
from .rsi import RSIOverboughtSignal, RSIOversoldSignal
from .volume import VolumeBreakoutSignal, VolumeSpikeSignal

__all__ = [
    "BaseSignal",
    "SignalRegistry",
    "GoldenCrossSignal",
    "DeadCrossSignal",
    "RSIOversoldSignal",
    "RSIOverboughtSignal",
    "MACDCrossSignal",
    "BollingerBreakoutSignal",
    "BollingerSqueezeSignal",
    "VolumeSpikeSignal",
    "VolumeBreakoutSignal",
]
