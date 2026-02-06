"""
移动平均线指标

包含：
- MA: 简单移动平均线
- EMA: 指数移动平均线

使用 TA-Lib 加速计算。
"""

from typing import List
import pandas as pd
import numpy as np

try:
    import talib
    HAS_TALIB = True
except ImportError:
    HAS_TALIB = False

from .base import BaseIndicator, register_indicator


def _compute_by_symbol(df: pd.DataFrame, compute_func, col_name: str) -> pd.DataFrame:
    """
    按 symbol 分组计算指标（优化版本）
    
    使用向量化操作，避免逐行处理。
    """
    result = df.copy()
    
    # 按 symbol 分组处理
    symbols = df["symbol"].unique()
    result[col_name] = np.nan
    
    for symbol in symbols:
        mask = df["symbol"] == symbol
        close = df.loc[mask, "close"].values.astype(np.float64)
        
        if len(close) > 0:
            values = compute_func(close)
            result.loc[mask, col_name] = values
    
    return result


class MAIndicator(BaseIndicator):
    """
    简单移动平均线 (Simple Moving Average)
    
    计算公式：MA(N) = SUM(CLOSE, N) / N
    
    使用场景：
    - 判断趋势方向
    - 作为支撑/阻力位
    - 金叉/死叉信号
    
    常用周期：
    - 短期：5, 10 日
    - 中期：20, 60 日
    - 长期：120, 250 日
    """
    
    def __init__(self, period: int = 20):
        """
        初始化 MA 指标
        
        Args:
            period: 计算周期（天数）
        """
        self.period = period
        
    @property
    def name(self) -> str:
        return f"ma_{self.period}"
        
    @property
    def columns(self) -> List[str]:
        return [self.name]
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算简单移动平均线
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 MA 列的 DataFrame
        """
        if HAS_TALIB:
            # 使用 TA-Lib 加速
            def compute_ma(close: np.ndarray) -> np.ndarray:
                return talib.SMA(close, timeperiod=self.period)
            
            return _compute_by_symbol(df, compute_ma, self.name)
        else:
            # 回退到 pandas 实现
            result = df.copy()
            result[self.name] = (
                df.groupby("symbol")["close"]
                .transform(
                    lambda x: x.rolling(
                        window=self.period, 
                        min_periods=1
                    ).mean()
                )
            )
            return result


class EMAIndicator(BaseIndicator):
    """
    指数移动平均线 (Exponential Moving Average)
    
    计算公式：
    EMA(N) = 2/(N+1) * CLOSE + (N-1)/(N+1) * EMA(N-1)
    
    特点：
    - 对近期价格敏感度更高
    - 能更快反映价格变化
    - 常用于 MACD 计算
    
    常用周期：
    - 12, 26 日（MACD 默认）
    - 5, 10, 20 日
    """
    
    def __init__(self, period: int = 20):
        """
        初始化 EMA 指标
        
        Args:
            period: 计算周期（天数）
        """
        self.period = period
        
    @property
    def name(self) -> str:
        return f"ema_{self.period}"
        
    @property
    def columns(self) -> List[str]:
        return [self.name]
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算指数移动平均线
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 EMA 列的 DataFrame
        """
        if HAS_TALIB:
            # 使用 TA-Lib 加速
            def compute_ema(close: np.ndarray) -> np.ndarray:
                return talib.EMA(close, timeperiod=self.period)
            
            return _compute_by_symbol(df, compute_ema, self.name)
        else:
            # 回退到 pandas 实现
            result = df.copy()
            result[self.name] = (
                df.groupby("symbol")["close"]
                .transform(
                    lambda x: x.ewm(
                        span=self.period, 
                        adjust=False
                    ).mean()
                )
            )
            return result


# 注册常用周期的均线指标
for period in [5, 10, 20, 60, 120, 250]:
    register_indicator(f"ma_{period}", lambda p=period: MAIndicator(p))
    register_indicator(f"ema_{period}", lambda p=period: EMAIndicator(p))
