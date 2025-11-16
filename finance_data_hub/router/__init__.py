"""
智能路由模块

提供数据源的智能路由和故障转移功能。
"""

from finance_data_hub.router.smart_router import (
    SmartRouter,
    RoutingConfig,
    CircuitBreaker,
)

__all__ = [
    "SmartRouter",
    "RoutingConfig",
    "CircuitBreaker",
]
