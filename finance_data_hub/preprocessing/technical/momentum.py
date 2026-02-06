"""
动量指标

包含：
- MACD: 指数平滑异同移动平均线
- RSI: 相对强弱指标

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
        symbols = df["symbol"].unique()
        
        result["macd_dif"] = np.nan
        result["macd_dea"] = np.nan
        result["macd_hist"] = np.nan
        
        for symbol in symbols:
            mask = df["symbol"] == symbol
            close = df.loc[mask, "close"].values.astype(np.float64)
            
            if len(close) > 0:
                if HAS_TALIB:
                    # TA-Lib 的 MACD 返回 (dif, dea, hist)
                    dif, dea, hist = talib.MACD(
                        close, 
                        fastperiod=self.fast, 
                        slowperiod=self.slow, 
                        signalperiod=self.signal
                    )
                    # TA-Lib 的 hist 没有乘以 2，这里保持一致
                    result.loc[mask, "macd_dif"] = dif
                    result.loc[mask, "macd_dea"] = dea
                    result.loc[mask, "macd_hist"] = hist * 2  # 乘以 2 以匹配国内习惯
                else:
                    # 回退到 pandas 实现
                    close_series = pd.Series(close)
                    ema_fast = close_series.ewm(span=self.fast, adjust=False).mean()
                    ema_slow = close_series.ewm(span=self.slow, adjust=False).mean()
                    dif = ema_fast - ema_slow
                    dea = dif.ewm(span=self.signal, adjust=False).mean()
                    hist = (dif - dea) * 2
                    
                    result.loc[mask, "macd_dif"] = dif.values
                    result.loc[mask, "macd_dea"] = dea.values
                    result.loc[mask, "macd_hist"] = hist.values
        
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
        symbols = df["symbol"].unique()
        result[self.name] = np.nan
        
        for symbol in symbols:
            mask = df["symbol"] == symbol
            close = df.loc[mask, "close"].values.astype(np.float64)
            
            if len(close) > 0:
                if HAS_TALIB:
                    # 使用 TA-Lib 加速
                    rsi = talib.RSI(close, timeperiod=self.period)
                    result.loc[mask, self.name] = rsi
                else:
                    # 回退到 pandas 实现
                    close_series = pd.Series(close)
                    delta = close_series.diff()
                    gain = delta.where(delta > 0, 0)
                    loss = (-delta).where(delta < 0, 0)
                    avg_gain = gain.rolling(window=self.period, min_periods=1).mean()
                    avg_loss = loss.rolling(window=self.period, min_periods=1).mean()
                    rs = avg_gain / avg_loss.replace(0, np.inf)
                    rsi = 100 - (100 / (1 + rs))
                    rsi = rsi.replace([np.inf, -np.inf], np.nan)
                    result.loc[mask, self.name] = rsi.values
        
        return result


# 注册默认 MACD
register_indicator("macd", MACDIndicator)

# 注册常用周期的 RSI
for period in [6, 14, 24]:
    register_indicator(f"rsi_{period}", lambda p=period: RSIIndicator(p))
