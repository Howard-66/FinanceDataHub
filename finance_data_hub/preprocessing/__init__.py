"""
FinanceDataHub 数据预处理模块

提供数据预处理功能：
- 复权处理（前复权存储、后复权实时计算）
- 周期重采样（周/月/季/年）
- 技术指标计算（MA/MACD/RSI/ATR/NDA）
- 基本面指标计算（估值分位/F-Score）
- 宏观周期计算（中国宏观周期、行业快照）
"""

from .adjust import AdjustType, AdjustProcessor
from .macro import MacroCycleCalculator
from .resample import ResampleFreq, ResampleProcessor
from .pipeline import PreprocessPipeline
from .storage import ProcessedDataStorage

__all__ = [
    "AdjustType",
    "AdjustProcessor",
    "MacroCycleCalculator",
    "ResampleFreq",
    "ResampleProcessor",
    "PreprocessPipeline",
    "ProcessedDataStorage",
]
