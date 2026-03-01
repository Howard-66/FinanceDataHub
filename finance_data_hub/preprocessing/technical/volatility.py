"""
波动率指标

包含：
- ATR: 平均真实波幅
- TR: 真实波幅

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
        计算 ATR 指标（向量化版本）
        
        使用 groupby.apply(include_groups=False) 替代逐 symbol 循环。
        
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
        
        period = self.period
        col_name = self.name

        def _calc_atr_group(group: pd.DataFrame) -> pd.DataFrame:
            high = group["high"].values.astype(np.float64)
            low = group["low"].values.astype(np.float64)
            close = group["close"].values.astype(np.float64)

            if len(close) == 0:
                return pd.DataFrame({col_name: np.nan}, index=group.index)

            if HAS_TALIB:
                atr = talib.ATR(high, low, close, timeperiod=period)
            else:
                prev_close = np.roll(close, 1)
                prev_close[0] = close[0]

                tr1 = high - low
                tr2 = np.abs(high - prev_close)
                tr3 = np.abs(low - prev_close)
                tr = np.maximum(np.maximum(tr1, tr2), tr3)
                atr = pd.Series(tr).ewm(span=period, adjust=False).mean().values

            return pd.DataFrame({col_name: atr}, index=group.index)

        result = df.copy()
        computed = df.groupby("symbol", group_keys=False).apply(
            _calc_atr_group, include_groups=False
        )
        result[col_name] = computed[col_name]
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
        """
        计算 TR（向量化版本）
        
        使用 groupby.apply(include_groups=False) 替代逐 symbol 循环。
        """
        # 验证必要的列
        required_cols = ["high", "low", "close"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"DataFrame must contain '{col}' column for TR calculation")

        col_name = self.name

        def _calc_tr_group(group: pd.DataFrame) -> pd.DataFrame:
            high = group["high"].values.astype(np.float64)
            low = group["low"].values.astype(np.float64)
            close = group["close"].values.astype(np.float64)

            if len(close) == 0:
                return pd.DataFrame({col_name: np.nan}, index=group.index)

            if HAS_TALIB:
                tr = talib.TRANGE(high, low, close)
            else:
                prev_close = np.roll(close, 1)
                prev_close[0] = close[0]

                tr1 = high - low
                tr2 = np.abs(high - prev_close)
                tr3 = np.abs(low - prev_close)
                tr = np.maximum(np.maximum(tr1, tr2), tr3)

            return pd.DataFrame({col_name: tr}, index=group.index)

        result = df.copy()
        computed = df.groupby("symbol", group_keys=False).apply(
            _calc_tr_group, include_groups=False
        )
        result[col_name] = computed[col_name]
        return result


# 注册常用周期的 ATR
for period in [14]:
    register_indicator(f"atr_{period}", lambda p=period: ATRIndicator(p))

register_indicator("tr", TRIndicator)
