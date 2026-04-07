"""
技术指标模块

提供常用技术指标的计算：
- 均线类：MA, EMA
- 趋势类：MACD
- 动量类：RSI
- 波动类：ATR
- 量能方向类：NDA
"""

from .base import BaseIndicator, IndicatorRegistry
from .flow import NDAIndicator
from .moving_average import MAIndicator, EMAIndicator
from .momentum import MACDIndicator, RSIIndicator
from .volatility import ATRIndicator

__all__ = [
    "BaseIndicator",
    "IndicatorRegistry",
    "NDAIndicator",
    "MAIndicator",
    "EMAIndicator",
    "MACDIndicator",
    "RSIIndicator",
    "ATRIndicator",
]
