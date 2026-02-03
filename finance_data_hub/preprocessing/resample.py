"""
周期重采样模块

支持将日线数据重采样为：
- W: 周线
- M: 月线
- Q: 季线
- Y: 年线

重要说明：
1. 重采样前必须先进行复权处理，否则价格不连续会导致错误的 OHLC
2. 对于大批量数据，建议使用 TimescaleDB 的 Continuous Aggregates
3. 本模块适用于 SDK 内存处理或小批量数据
"""

from enum import Enum
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger


class ResampleFreq(str, Enum):
    """重采样周期枚举"""
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    YEARLY = "Y"


# Pandas 周期映射
FREQ_MAP = {
    ResampleFreq.WEEKLY: "W-FRI",      # 周五收盘
    ResampleFreq.MONTHLY: "ME",        # 月末
    ResampleFreq.QUARTERLY: "QE",      # 季末
    ResampleFreq.YEARLY: "YE",         # 年末
}

# 周期中文名称
FREQ_NAMES = {
    ResampleFreq.WEEKLY: "周线",
    ResampleFreq.MONTHLY: "月线",
    ResampleFreq.QUARTERLY: "季线",
    ResampleFreq.YEARLY: "年线",
}


class ResampleProcessor:
    """
    周期重采样处理器
    
    将日线数据重采样为更低频率的数据（周/月/季/年）。
    
    示例:
        >>> processor = ResampleProcessor()
        >>> df_weekly = processor.resample(df, ResampleFreq.WEEKLY)
        >>> df_monthly = processor.resample(df, ResampleFreq.MONTHLY)
    """
    
    # 默认聚合规则
    DEFAULT_AGG_RULES = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "amount": "sum",
        "adj_factor": "last",
    }
    
    def __init__(self, db_operations=None):
        """
        初始化重采样处理器
        
        Args:
            db_operations: 数据库操作对象（可选）
        """
        self.db_operations = db_operations
        
    def resample(
        self,
        df: pd.DataFrame,
        freq: ResampleFreq,
        agg_rules: Optional[Dict[str, Any]] = None,
        check_adjusted: bool = True
    ) -> pd.DataFrame:
        """
        对 OHLCV 数据进行周期重采样
        
        Args:
            df: 日线数据 DataFrame，需包含 symbol, time, OHLCV 列
            freq: 目标周期
            agg_rules: 自定义聚合规则，默认使用 DEFAULT_AGG_RULES
            check_adjusted: 是否检查数据已复权
            
        Returns:
            重采样后的 DataFrame
            
        Raises:
            ValueError: 如果数据未复权且 check_adjusted=True
        """
        if check_adjusted and "adjust_type" in df.columns:
            if (df["adjust_type"] == "none").any():
                raise ValueError(
                    "Resample requires adjusted data. "
                    "Please apply adjustment first using AdjustProcessor."
                )
        
        if df.empty:
            logger.warning("Empty DataFrame, returning as-is")
            return df
        
        # 确定聚合规则
        agg = agg_rules or self.DEFAULT_AGG_RULES
        
        # 只保留 DataFrame 中存在的列
        agg = {k: v for k, v in agg.items() if k in df.columns}
        
        if not agg:
            raise ValueError("No aggregatable columns found in DataFrame")
        
        pandas_freq = FREQ_MAP[freq]
        
        # 确保 time 列是 datetime 类型
        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"])
        
        # 按股票分组重采样
        result_list = []
        
        for symbol, group in df.groupby("symbol"):
            # 设置时间索引
            group = group.set_index("time").sort_index()
            
            # 重采样
            resampled = group.resample(pandas_freq).agg(agg)
            
            # 移除全为 NaN 的行（非交易周期）
            resampled = resampled.dropna(subset=["open"])
            
            # 重置索引
            resampled = resampled.reset_index()
            resampled["symbol"] = symbol
            
            result_list.append(resampled)
        
        if not result_list:
            logger.warning("No data after resampling")
            return pd.DataFrame()
        
        result = pd.concat(result_list, ignore_index=True)
        
        # 添加频率标记
        result["freq"] = freq.value
        
        # 如果原数据有 adjust_type，保留
        if "adjust_type" in df.columns:
            result["adjust_type"] = df["adjust_type"].iloc[0]
        
        logger.debug(
            f"Resampled {len(df)} daily records to {len(result)} "
            f"{FREQ_NAMES[freq]} records"
        )
        
        return result
    
    def resample_multi(
        self,
        df: pd.DataFrame,
        freqs: List[ResampleFreq],
        agg_rules: Optional[Dict[str, Any]] = None
    ) -> Dict[ResampleFreq, pd.DataFrame]:
        """
        对数据进行多周期重采样
        
        Args:
            df: 日线数据 DataFrame
            freqs: 目标周期列表
            agg_rules: 自定义聚合规则
            
        Returns:
            周期 -> DataFrame 的字典
        """
        result = {}
        
        for freq in freqs:
            result[freq] = self.resample(df, freq, agg_rules)
        
        return result
    
    def get_week_start_end(self, date: pd.Timestamp) -> tuple:
        """
        获取给定日期所在周的起止日期
        
        Args:
            date: 日期
            
        Returns:
            (周一日期, 周五日期)
        """
        # 找到本周一
        week_start = date - pd.Timedelta(days=date.weekday())
        # 找到本周五
        week_end = week_start + pd.Timedelta(days=4)
        
        return week_start, week_end
    
    def get_month_start_end(self, date: pd.Timestamp) -> tuple:
        """
        获取给定日期所在月的起止日期
        
        Args:
            date: 日期
            
        Returns:
            (月初日期, 月末日期)
        """
        month_start = date.replace(day=1)
        # 下月第一天 - 1天
        if date.month == 12:
            month_end = date.replace(year=date.year + 1, month=1, day=1) - pd.Timedelta(days=1)
        else:
            month_end = date.replace(month=date.month + 1, day=1) - pd.Timedelta(days=1)
        
        return month_start, month_end
