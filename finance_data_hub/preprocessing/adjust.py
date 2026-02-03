"""
复权处理模块

复权类型：
- qfq: 前复权（以最新价格为基准向前调整）- 存储到数据库
- hfq: 后复权（以上市价格为基准向后调整）- 实时计算
- none: 不复权（原始价格）

复权公式：
- 前复权: 调整价格 = 原价格 × (当日复权因子 / 最新复权因子)
- 后复权: 调整价格 = 原价格 × (当日复权因子 / 上市首日复权因子)

注意事项：
- 前复权价格会随时间变化（新的除权除息会影响历史价格）
- 后复权价格相对稳定，历史价格不会改变
- 技术分析通常使用前复权数据
- 收益率计算通常使用后复权数据
"""

from enum import Enum
from typing import Optional, List, Union
import pandas as pd
import numpy as np
from loguru import logger


class AdjustType(str, Enum):
    """复权类型枚举"""
    QFQ = "qfq"    # 前复权（存储）
    HFQ = "hfq"    # 后复权（实时计算）
    NONE = "none"  # 不复权


class AdjustProcessor:
    """
    复权处理器
    
    支持对 OHLCV 数据进行前复权和后复权处理。
    前复权数据将存储到数据库，后复权数据实时计算。
    
    示例:
        >>> processor = AdjustProcessor()
        >>> df_qfq = processor.adjust_qfq(df)
        >>> df_hfq = processor.adjust_hfq(df)
    """
    
    PRICE_COLUMNS = ["open", "high", "low", "close"]
    
    def __init__(self, db_operations=None):
        """
        初始化复权处理器
        
        Args:
            db_operations: 数据库操作对象（可选，用于获取复权因子）
        """
        self.db_operations = db_operations
        
    def adjust_qfq(
        self, 
        df: pd.DataFrame,
        price_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        前复权处理（以最新价格为基准）
        
        前复权使价格曲线平滑，便于技术分析。
        最新价格不变，历史价格向下调整。
        
        Args:
            df: 包含 symbol, time, open, high, low, close, adj_factor 的 DataFrame
            price_columns: 需要复权的价格列，默认为 OHLC
            
        Returns:
            前复权后的 DataFrame
            
        Raises:
            ValueError: 如果 DataFrame 缺少必要的列
        """
        if "adj_factor" not in df.columns:
            raise ValueError("DataFrame must contain 'adj_factor' column for adjustment")
        
        if price_columns is None:
            price_columns = self.PRICE_COLUMNS
            
        result = df.copy()
        
        # 按股票分组处理
        for symbol, group in result.groupby("symbol"):
            if group.empty:
                continue
                
            # 获取最新复权因子（时间最大的记录）
            latest_idx = group["time"].idxmax()
            latest_factor = group.loc[latest_idx, "adj_factor"]
            
            if latest_factor == 0 or pd.isna(latest_factor):
                logger.warning(f"Invalid adj_factor for {symbol}, skipping adjustment")
                continue
            
            # 计算调整比例
            adjust_ratio = group["adj_factor"] / latest_factor
            
            # 应用复权
            for col in price_columns:
                if col in result.columns:
                    result.loc[group.index, col] = group[col] * adjust_ratio
        
        # 标记复权类型
        result["adjust_type"] = "qfq"
        
        logger.debug(f"Applied QFQ adjustment to {len(result)} records")
        return result
    
    def adjust_hfq(
        self, 
        df: pd.DataFrame,
        price_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        后复权处理（以上市首日为基准）- 实时计算
        
        后复权使历史价格不变，便于计算真实收益率。
        首日价格不变，后续价格向上调整。
        
        Args:
            df: 包含 symbol, time, open, high, low, close, adj_factor 的 DataFrame
            price_columns: 需要复权的价格列，默认为 OHLC
            
        Returns:
            后复权后的 DataFrame
            
        Raises:
            ValueError: 如果 DataFrame 缺少必要的列
        """
        if "adj_factor" not in df.columns:
            raise ValueError("DataFrame must contain 'adj_factor' column for adjustment")
        
        if price_columns is None:
            price_columns = self.PRICE_COLUMNS
            
        result = df.copy()
        
        # 按股票分组处理
        for symbol, group in result.groupby("symbol"):
            if group.empty:
                continue
                
            # 获取首日复权因子（时间最小的记录）
            first_idx = group["time"].idxmin()
            first_factor = group.loc[first_idx, "adj_factor"]
            
            if first_factor == 0 or pd.isna(first_factor):
                logger.warning(f"Invalid adj_factor for {symbol}, skipping adjustment")
                continue
            
            # 计算调整比例
            adjust_ratio = group["adj_factor"] / first_factor
            
            # 应用复权
            for col in price_columns:
                if col in result.columns:
                    result.loc[group.index, col] = group[col] * adjust_ratio
        
        # 标记复权类型
        result["adjust_type"] = "hfq"
        
        logger.debug(f"Applied HFQ adjustment to {len(result)} records")
        return result
        
    def adjust(
        self, 
        df: pd.DataFrame, 
        adjust_type: Union[AdjustType, str] = AdjustType.QFQ,
        price_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        统一复权入口
        
        Args:
            df: 原始 OHLCV 数据
            adjust_type: 复权类型
            price_columns: 需要复权的价格列
            
        Returns:
            复权后的 DataFrame
        """
        if isinstance(adjust_type, str):
            adjust_type = AdjustType(adjust_type)
            
        if adjust_type == AdjustType.NONE:
            result = df.copy()
            result["adjust_type"] = "none"
            return result
        elif adjust_type == AdjustType.QFQ:
            return self.adjust_qfq(df, price_columns)
        else:
            return self.adjust_hfq(df, price_columns)
    
    def reverse_qfq(
        self, 
        df: pd.DataFrame,
        price_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        反向前复权（从前复权恢复到原始价格）
        
        用于需要从前复权数据恢复原始数据的场景。
        
        Args:
            df: 前复权后的数据
            price_columns: 需要恢复的价格列
            
        Returns:
            原始价格的 DataFrame
        """
        if "adj_factor" not in df.columns:
            raise ValueError("DataFrame must contain 'adj_factor' column for reverse adjustment")
        
        if price_columns is None:
            price_columns = self.PRICE_COLUMNS
            
        result = df.copy()
        
        for symbol, group in result.groupby("symbol"):
            if group.empty:
                continue
                
            latest_idx = group["time"].idxmax()
            latest_factor = group.loc[latest_idx, "adj_factor"]
            
            if latest_factor == 0 or pd.isna(latest_factor):
                continue
            
            # 反向调整比例
            adjust_ratio = latest_factor / group["adj_factor"]
            
            for col in price_columns:
                if col in result.columns:
                    result.loc[group.index, col] = group[col] * adjust_ratio
        
        result["adjust_type"] = "none"
        return result
