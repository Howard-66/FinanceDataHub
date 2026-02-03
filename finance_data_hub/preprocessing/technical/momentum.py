"""
动量指标

包含：
- MACD: 指数平滑异同移动平均线
- RSI: 相对强弱指标
"""

from typing import List
import pandas as pd
import numpy as np

from .base import BaseIndicator, register_indicator


class MACDIndicator(BaseIndicator):
    """
    MACD 指标 (Moving Average Convergence Divergence)
    
    计算公式：
    - DIF = EMA(12) - EMA(26)
    - DEA = EMA(DIF, 9)
    - MACD = (DIF - DEA) * 2
    
    输出列：
    - macd_dif: 快慢线差值
    - macd_dea: DIF 的 9 日 EMA（信号线）
    - macd_hist: MACD 柱状图（红绿柱）
    
    使用场景：
    - 判断趋势强度
    - 金叉（DIF 上穿 DEA）买入信号
    - 死叉（DIF 下穿 DEA）卖出信号
    - 背离信号
    """
    
    def __init__(
        self, 
        fast: int = 12, 
        slow: int = 26, 
        signal: int = 9
    ):
        """
        初始化 MACD 指标
        
        Args:
            fast: 快速 EMA 周期
            slow: 慢速 EMA 周期
            signal: 信号线周期
        """
        self.fast = fast
        self.slow = slow
        self.signal = signal
        
    @property
    def name(self) -> str:
        return "macd"
        
    @property
    def columns(self) -> List[str]:
        return ["macd_dif", "macd_dea", "macd_hist"]
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 MACD 指标
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 MACD 相关列的 DataFrame
        """
        result = df.copy()
        
        def calc_macd(group: pd.DataFrame) -> pd.DataFrame:
            close = group["close"]
            
            # 计算快速和慢速 EMA
            ema_fast = close.ewm(span=self.fast, adjust=False).mean()
            ema_slow = close.ewm(span=self.slow, adjust=False).mean()
            
            # DIF = 快线 - 慢线
            dif = ema_fast - ema_slow
            
            # DEA = DIF 的 EMA
            dea = dif.ewm(span=self.signal, adjust=False).mean()
            
            # MACD 柱状图（乘以 2 是为了放大显示）
            hist = (dif - dea) * 2
            
            return pd.DataFrame({
                "macd_dif": dif,
                "macd_dea": dea,
                "macd_hist": hist
            }, index=group.index)
        
        # 按股票分组计算
        macd_df = (
            df.groupby("symbol", group_keys=False)
            .apply(calc_macd)
        )
        
        # 合并结果
        result = result.join(macd_df)
        
        return result


class RSIIndicator(BaseIndicator):
    """
    RSI 相对强弱指标 (Relative Strength Index)
    
    计算公式：
    RS = AVG(上涨幅度, N) / AVG(下跌幅度, N)
    RSI = 100 - 100 / (1 + RS)
    
    取值范围：0 - 100
    
    使用场景：
    - RSI > 70: 超买区域，可能存在回调风险
    - RSI < 30: 超卖区域，可能存在反弹机会
    - RSI = 50: 多空平衡
    
    常用周期：
    - 6 日（短期）
    - 14 日（标准）
    - 24 日（长期）
    """
    
    def __init__(self, period: int = 14):
        """
        初始化 RSI 指标
        
        Args:
            period: 计算周期（天数）
        """
        self.period = period
        
    @property
    def name(self) -> str:
        return f"rsi_{self.period}"
        
    @property
    def columns(self) -> List[str]:
        return [self.name]
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 RSI 指标
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 RSI 列的 DataFrame
        """
        result = df.copy()
        
        def calc_rsi(group: pd.DataFrame) -> pd.Series:
            close = group["close"]
            
            # 计算价格变化
            delta = close.diff()
            
            # 分离上涨和下跌
            gain = delta.where(delta > 0, 0)
            loss = (-delta).where(delta < 0, 0)
            
            # 计算平均上涨和下跌
            avg_gain = gain.rolling(window=self.period, min_periods=1).mean()
            avg_loss = loss.rolling(window=self.period, min_periods=1).mean()
            
            # 计算 RS 和 RSI
            # 避免除以零
            rs = avg_gain / avg_loss.replace(0, np.inf)
            rsi = 100 - (100 / (1 + rs))
            
            # 处理极端情况
            rsi = rsi.replace([np.inf, -np.inf], np.nan)
            
            return rsi
        
        result[self.name] = (
            df.groupby("symbol", group_keys=False)
            .apply(calc_rsi)
        )
        
        return result


# 注册默认 MACD
register_indicator("macd", MACDIndicator)

# 注册常用周期的 RSI
for period in [6, 14, 24]:
    register_indicator(f"rsi_{period}", lambda p=period: RSIIndicator(p))
