"""
向量化技术指标批量计算入口

Phase 3 优化：提供批量计算接口，减少多次 copy / groupby 开销。

使用方式：
    from finance_data_hub.preprocessing.technical.vectorized import compute_indicators_batch
    
    result = compute_indicators_batch(df, ["ma_20", "macd", "rsi_14", "atr_14"])
"""

from typing import List
import pandas as pd
from loguru import logger

from .base import create_indicator


def compute_indicators_batch(
    df: pd.DataFrame,
    indicator_names: List[str]
) -> pd.DataFrame:
    """
    批量计算多个技术指标
    
    依序应用各指标计算，每个指标内部已使用 groupby.apply 向量化。
    相比逐个创建 indicator 再单独调用，此函数统一管理异常处理和日志。
    
    Args:
        df: 包含 symbol, time, close (以及可能的 high, low) 的 DataFrame
        indicator_names: 指标名称列表，如 ["ma_20", "macd", "rsi_14", "atr_14"]
        
    Returns:
        添加所有指标列后的 DataFrame
    """
    result = df.copy()
    
    for name in indicator_names:
        try:
            indicator = create_indicator(name)
            result = indicator.calculate(result)
        except KeyError:
            logger.warning(f"Unknown indicator: {name}, skipping")
        except Exception as e:
            logger.warning(f"Failed to calculate indicator {name}: {e}, skipping")
    
    return result
