"""
波动率指标

包含：
- ATR: 平均真实波幅
"""

from typing import List
import pandas as pd
import numpy as np

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
        
        def calc_atr(group: pd.DataFrame) -> pd.Series:
            high = group["high"]
            low = group["low"]
            close = group["close"]
            
            # 前一日收盘价
            prev_close = close.shift(1)
            
            # 计算三个真实波幅候选值
            tr1 = high - low  # 当日振幅
            tr2 = (high - prev_close).abs()  # 当日最高与前日收盘的距离
            tr3 = (low - prev_close).abs()  # 当日最低与前日收盘的距离
            
            # 真实波幅 = 三个候选值中的最大值
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            # ATR = TR 的 EMA
            atr = tr.ewm(span=self.period, adjust=False).mean()
            
            return pd.Series(atr.values, index=group.index)
        
        # 按股票分组计算
        atr_values = []
        for _, group in df.groupby("symbol", sort=False):
            atr_values.append(calc_atr(group))
        
        result[self.name] = pd.concat(atr_values)
        
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
        result = df.copy()
        
        def calc_tr(group: pd.DataFrame) -> pd.Series:
            high = group["high"]
            low = group["low"]
            close = group["close"]
            prev_close = close.shift(1)
            
            tr1 = high - low
            tr2 = (high - prev_close).abs()
            tr3 = (low - prev_close).abs()
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            return pd.Series(tr.values, index=group.index)
        
        # 按股票分组计算
        tr_values = []
        for _, group in df.groupby("symbol", sort=False):
            tr_values.append(calc_tr(group))
        
        result[self.name] = pd.concat(tr_values)
        
        return result


# 注册常用周期的 ATR
for period in [14, 20]:
    register_indicator(f"atr_{period}", lambda p=period: ATRIndicator(p))

register_indicator("tr", TRIndicator)
