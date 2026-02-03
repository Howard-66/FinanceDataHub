"""
估值指标分位计算

计算 PE/PB/PS 在历史数据中的分位数，用于判断当前估值水平。
使用滚动窗口计算，支持多个时间跨度（1年/2年/3年/5年）。

使用场景：
- 判断当前估值在历史中的位置
- 高分位（>80%）可能存在高估风险
- 低分位（<20%）可能存在低估机会
- 结合行业特性和成长性综合判断
"""

from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger


class ValuationPercentile:
    """
    估值分位计算器
    
    计算 PE_TTM、PB、PS_TTM 等估值指标的历史分位数。
    
    示例:
        >>> calculator = ValuationPercentile(
        ...     metrics=["pe_ttm", "pb"],
        ...     windows=[250, 500]
        ... )
        >>> result = calculator.calculate(df)
    """
    
    # 默认估值指标
    DEFAULT_METRICS = ["pe_ttm", "pb", "ps_ttm"]
    
    # 默认滚动窗口（交易日）
    # 250 ≈ 1年, 500 ≈ 2年, 750 ≈ 3年, 1250 ≈ 5年
    DEFAULT_WINDOWS = [250, 500, 750, 1250]
    
    def __init__(
        self,
        metrics: Optional[List[str]] = None,
        windows: Optional[List[int]] = None
    ):
        """
        初始化估值分位计算器
        
        Args:
            metrics: 估值指标列表
            windows: 滚动窗口列表（交易日）
        """
        self.metrics = metrics or self.DEFAULT_METRICS
        self.windows = windows or self.DEFAULT_WINDOWS
        
    @property
    def columns(self) -> List[str]:
        """输出列名"""
        cols = []
        for metric in self.metrics:
            for window in self.windows:
                cols.append(f"{metric}_pct_{window}d")
        return cols
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算估值分位
        
        Args:
            df: 包含 symbol, time, 估值指标的 DataFrame
            
        Returns:
            添加分位列后的 DataFrame
            
        说明:
            分位数范围 0-100，表示当前值在历史数据中的位置：
            - 0: 历史最低
            - 50: 历史中位数
            - 100: 历史最高
        """
        result = df.copy()
        
        for metric in self.metrics:
            if metric not in df.columns:
                logger.warning(f"Metric '{metric}' not found in DataFrame, skipping")
                continue
                
            for window in self.windows:
                col_name = f"{metric}_pct_{window}d"
                
                result[col_name] = (
                    df.groupby("symbol")[metric]
                    .transform(
                        lambda x: self._rolling_percentile(x, window)
                    )
                )
                
        logger.debug(f"Calculated valuation percentiles for {len(result)} records")
        return result
    
    def _rolling_percentile(
        self, 
        series: pd.Series, 
        window: int
    ) -> pd.Series:
        """
        计算滚动分位数
        
        使用滚动窗口计算当前值在历史数据中的分位。
        
        Args:
            series: 指标值序列
            window: 窗口大小
            
        Returns:
            分位数序列 (0-100)
        """
        def calc_percentile(x):
            """计算当前值在窗口中的分位"""
            # 排除 NaN 和 <= 0 的值（PE/PB/PS 为负或零无意义）
            valid = x[(~np.isnan(x)) & (x > 0)]
            
            if len(valid) < 2:
                return np.nan
            
            current = x.iloc[-1]
            
            if np.isnan(current) or current <= 0:
                return np.nan
            
            # 计算当前值在窗口中的分位
            # 使用 < 而非 <= 来排除当前值本身
            rank = (valid < current).sum()
            percentile = rank / len(valid) * 100
            
            return percentile
        
        return series.rolling(
            window=window, 
            min_periods=min(20, window)  # 最少需要 20 个有效数据点
        ).apply(calc_percentile, raw=False)
    
    def get_percentile_level(self, percentile: float) -> str:
        """
        获取分位水平描述
        
        Args:
            percentile: 分位值 (0-100)
            
        Returns:
            水平描述
        """
        if percentile >= 80:
            return "极高"
        elif percentile >= 60:
            return "偏高"
        elif percentile >= 40:
            return "适中"
        elif percentile >= 20:
            return "偏低"
        else:
            return "极低"
    
    def calculate_current_percentile(
        self, 
        df: pd.DataFrame, 
        metric: str,
        window: int = 250
    ) -> pd.DataFrame:
        """
        计算最新分位数
        
        只计算每只股票的最新分位数，用于实时监控。
        
        Args:
            df: 包含历史数据的 DataFrame
            metric: 估值指标
            window: 滚动窗口
            
        Returns:
            每只股票最新分位数的 DataFrame
        """
        result_list = []
        
        for symbol, group in df.groupby("symbol"):
            if len(group) < window:
                # 数据不足
                continue
            
            # 取最近 window 天的数据
            recent = group.sort_values("time").tail(window)
            
            # 获取最新值
            latest = recent.iloc[-1]
            current_value = latest[metric]
            
            if pd.isna(current_value) or current_value <= 0:
                continue
            
            # 计算分位
            valid = recent[metric][(~recent[metric].isna()) & (recent[metric] > 0)]
            rank = (valid < current_value).sum()
            percentile = rank / len(valid) * 100
            
            result_list.append({
                "symbol": symbol,
                "time": latest["time"],
                metric: current_value,
                f"{metric}_pct": percentile,
                f"{metric}_level": self.get_percentile_level(percentile)
            })
        
        return pd.DataFrame(result_list)
