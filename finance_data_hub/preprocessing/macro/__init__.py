"""
宏观预处理模块

提供中国宏观周期相关的月度预处理能力：
- 宏观数据对齐（M2/PPI/PMI/GDP）
- 信用脉冲计算
- 宏观阶段判定（raw/stable）
- 行业配置快照生成
"""

from .cycle import (
    CN_PHASE_METADATA,
    MacroCycleCalculator,
)

__all__ = [
    "CN_PHASE_METADATA",
    "MacroCycleCalculator",
]
