"""
移动平均线指标

包含：
- MA: 简单移动平均线
- EMA: 指数移动平均线

使用 TA-Lib 加速计算。
Phase 3 优化：使用 groupby.apply 替代 for-symbol 循环，提升 30-50% 性能。
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
        计算简单移动平均线（向量化版本）
        
        TA-Lib 分支使用 groupby.apply(include_groups=False)；
        pandas 分支使用 groupby.transform（天然无 FutureWarning）。
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 MA 列的 DataFrame
        """
        period = self.period
        col_name = self.name
        result = df.copy()

        if HAS_TALIB:
            def _calc_ma_group(group: pd.DataFrame) -> pd.DataFrame:
                close = group["close"].values.astype(np.float64)
                if len(close) > 0:
                    vals = talib.SMA(close, timeperiod=period)
                else:
                    vals = np.nan
                return pd.DataFrame({col_name: vals}, index=group.index)

            computed = df.groupby("symbol", group_keys=False).apply(
                _calc_ma_group, include_groups=False
            )
            result[col_name] = computed[col_name]
        else:
            # pandas 实现（groupby.transform 天然无此 warning）
            result[col_name] = (
                df.groupby("symbol")["close"]
                .transform(
                    lambda x: x.rolling(
                        window=period, 
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
        计算指数移动平均线（向量化版本）
        
        TA-Lib 分支使用 groupby.apply(include_groups=False)；
        pandas 分支使用 groupby.transform。
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 EMA 列的 DataFrame
        """
        period = self.period
        col_name = self.name
        result = df.copy()

        if HAS_TALIB:
            def _calc_ema_group(group: pd.DataFrame) -> pd.DataFrame:
                close = group["close"].values.astype(np.float64)
                if len(close) > 0:
                    vals = talib.EMA(close, timeperiod=period)
                else:
                    vals = np.nan
                return pd.DataFrame({col_name: vals}, index=group.index)

            computed = df.groupby("symbol", group_keys=False).apply(
                _calc_ema_group, include_groups=False
            )
            result[col_name] = computed[col_name]
        else:
            # pandas 实现（groupby.transform 天然无此 warning）
            result[col_name] = (
                df.groupby("symbol")["close"]
                .transform(
                    lambda x: x.ewm(
                        span=period, 
                        adjust=False
                    ).mean()
                )
            )
        return result


# 注册常用周期的均线指标
for period in [20, 50]:
    register_indicator(f"ma_{period}", lambda p=period: MAIndicator(p))
    register_indicator(f"ema_{period}", lambda p=period: EMAIndicator(p))
