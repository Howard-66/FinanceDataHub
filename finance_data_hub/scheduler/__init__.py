"""
FinanceDataHub 调度模块

提供定时任务调度能力，支持：
- 数据下载任务（daily, daily_basic, adj_factor 等）
- 预处理任务（技术指标、基本面指标计算）
- 任务依赖管理
- 失败重试机制
"""

from .models import JobConfig, ScheduleConfig
from .engine import SchedulerEngine
from .executor import TaskExecutor
from .manager import ScheduleManager

__all__ = [
    "JobConfig",
    "ScheduleConfig",
    "SchedulerEngine",
    "TaskExecutor",
    "ScheduleManager",
]
