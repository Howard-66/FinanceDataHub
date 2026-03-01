"""
动量指标

包含：
- MACD: 指数平滑异同移动平均线
- RSI: 相对强弱指标

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
        计算 MACD 指标（向量化版本）
        
        使用 groupby.apply(include_groups=False) 替代逐 symbol 循环。
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 MACD 相关列的 DataFrame
        """
        fast, slow, signal = self.fast, self.slow, self.signal

        def _calc_macd_group(group: pd.DataFrame) -> pd.DataFrame:
            close = group["close"].values.astype(np.float64)
            if len(close) == 0:
                return pd.DataFrame(
                    {"macd_dif": np.nan, "macd_dea": np.nan, "macd_hist": np.nan},
                    index=group.index
                )

            if HAS_TALIB:
                dif, dea, hist = talib.MACD(
                    close,
                    fastperiod=fast,
                    slowperiod=slow,
                    signalperiod=signal
                )
                return pd.DataFrame({
                    "macd_dif": dif,
                    "macd_dea": dea,
                    "macd_hist": hist * 2,  # 乘以 2 以匹配国内习惯
                }, index=group.index)
            else:
                close_series = pd.Series(close)
                ema_fast = close_series.ewm(span=fast, adjust=False).mean()
                ema_slow = close_series.ewm(span=slow, adjust=False).mean()
                dif = ema_fast - ema_slow
                dea = dif.ewm(span=signal, adjust=False).mean()
                hist = (dif - dea) * 2
                return pd.DataFrame({
                    "macd_dif": dif.values,
                    "macd_dea": dea.values,
                    "macd_hist": hist.values,
                }, index=group.index)

        result = df.copy()
        macd_cols = df.groupby("symbol", group_keys=False).apply(
            _calc_macd_group, include_groups=False
        )
        result[["macd_dif", "macd_dea", "macd_hist"]] = macd_cols
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
        计算 RSI 指标（向量化版本）
        
        使用 groupby.apply(include_groups=False) 替代逐 symbol 循环。
        
        Args:
            df: 包含 symbol, time, close 的 DataFrame
            
        Returns:
            添加 RSI 列的 DataFrame
        """
        period = self.period
        col_name = self.name

        def _calc_rsi_group(group: pd.DataFrame) -> pd.DataFrame:
            close = group["close"].values.astype(np.float64)
            if len(close) == 0:
                return pd.DataFrame({col_name: np.nan}, index=group.index)

            if HAS_TALIB:
                rsi = talib.RSI(close, timeperiod=period)
            else:
                close_series = pd.Series(close)
                delta = close_series.diff()
                gain = delta.where(delta > 0, 0)
                loss = (-delta).where(delta < 0, 0)
                avg_gain = gain.rolling(window=period, min_periods=1).mean()
                avg_loss = loss.rolling(window=period, min_periods=1).mean()
                rs = avg_gain / avg_loss.replace(0, np.inf)
                rsi = 100 - (100 / (1 + rs))
                rsi = rsi.replace([np.inf, -np.inf], np.nan).values

            return pd.DataFrame({col_name: rsi}, index=group.index)

        result = df.copy()
        rsi_col = df.groupby("symbol", group_keys=False).apply(
            _calc_rsi_group, include_groups=False
        )
        result[col_name] = rsi_col[col_name]
        return result


# 注册默认 MACD
register_indicator("macd", MACDIndicator)

# 注册常用周期的 RSI
for period in [14]:
    register_indicator(f"rsi_{period}", lambda p=period: RSIIndicator(p))
