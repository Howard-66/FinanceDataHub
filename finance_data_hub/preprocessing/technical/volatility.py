"""
波动率指标

包含：
- ATR: 平均真实波幅
- TR: 真实波幅

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


class ATRIndicator(BaseIndicator):
    """
    ATR 平均真实波幅 (Average True Range)
    
    计算公式：
    TR = MAX(
        HIGH - LOW,
        ABS(HIGH - PREV_CLOSE),
        ABS(LOW - PREV_CLOSE)
    )
    ATR = EMA(TR, N)
    
    特点：
    - 衡量价格波动的剧烈程度
    - 不反映价格方向，只反映波动幅度
    - 常用于设置止损位
    
    使用场景：
    - 动态止损：止损位 = 当前价格 - 2*ATR
    - 仓位管理：波动大时减仓，波动小时加仓
    - 判断突破有效性：突破时 ATR 放大
    
    常用周期：14 日
    """
    
    def __init__(self, period: int = 14):
        """
        初始化 ATR 指标
        
        Args:
            period: 计算周期（天数）
        """
        self.period = period
        
    @property
    def name(self) -> str:
        return f"atr_{self.period}"
        
    @property
    def columns(self) -> List[str]:
        return [self.name]
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 ATR 指标
        
        Args:
            df: 包含 symbol, time, high, low, close 的 DataFrame
            
        Returns:
            添加 ATR 列的 DataFrame
        """
        # 验证必要的列
        required_cols = ["high", "low", "close"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"DataFrame must contain '{col}' column for ATR calculation")
        
        result = df.copy()
        symbols = df["symbol"].unique()
        result[self.name] = np.nan
        
        for symbol in symbols:
            mask = df["symbol"] == symbol
            high = df.loc[mask, "high"].values.astype(np.float64)
            low = df.loc[mask, "low"].values.astype(np.float64)
            close = df.loc[mask, "close"].values.astype(np.float64)
            
            if len(close) > 0:
                if HAS_TALIB:
                    # 使用 TA-Lib 加速
                    atr = talib.ATR(high, low, close, timeperiod=self.period)
                    result.loc[mask, self.name] = atr
                else:
                    # 回退到 pandas 实现
                    prev_close = np.roll(close, 1)
                    prev_close[0] = close[0]
                    
                    tr1 = high - low
                    tr2 = np.abs(high - prev_close)
                    tr3 = np.abs(low - prev_close)
                    tr = np.maximum(np.maximum(tr1, tr2), tr3)
                    
                    # ATR = TR 的 EMA
                    atr = pd.Series(tr).ewm(span=self.period, adjust=False).mean().values
                    result.loc[mask, self.name] = atr
        
        return result


class TRIndicator(BaseIndicator):
    """
    TR 真实波幅 (True Range)
    
    ATR 的中间值，不做平滑处理。
    """
    
    def __init__(self):
        pass
        
    @property
    def name(self) -> str:
        return "tr"
        
    @property
    def columns(self) -> List[str]:
        return [self.name]
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算 TR"""
        # 验证必要的列
        required_cols = ["high", "low", "close"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"DataFrame must contain '{col}' column for TR calculation")
        
        result = df.copy()
        symbols = df["symbol"].unique()
        result[self.name] = np.nan
        
        for symbol in symbols:
            mask = df["symbol"] == symbol
            high = df.loc[mask, "high"].values.astype(np.float64)
            low = df.loc[mask, "low"].values.astype(np.float64)
            close = df.loc[mask, "close"].values.astype(np.float64)
            
            if len(close) > 0:
                if HAS_TALIB:
                    # 使用 TA-Lib 加速
                    tr = talib.TRANGE(high, low, close)
                    result.loc[mask, self.name] = tr
                else:
                    # 回退到 pandas 实现
                    prev_close = np.roll(close, 1)
                    prev_close[0] = close[0]
                    
                    tr1 = high - low
                    tr2 = np.abs(high - prev_close)
                    tr3 = np.abs(low - prev_close)
                    tr = np.maximum(np.maximum(tr1, tr2), tr3)
                    result.loc[mask, self.name] = tr
        
        return result


# 注册常用周期的 ATR
for period in [14]:
    register_indicator(f"atr_{period}", lambda p=period: ATRIndicator(p))

register_indicator("tr", TRIndicator)
