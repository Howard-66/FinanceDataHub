"""
数据提供者模块

提供统一的数据源接口抽象和实现。
"""

from finance_data_hub.providers.base import (
    BaseDataProvider,
    ProviderError,
    ProviderConnectionError,
    ProviderAuthError,
    ProviderRateLimitError,
    ProviderDataError,
)
from finance_data_hub.providers.registry import ProviderRegistry, register_provider
from finance_data_hub.providers.schema import (
    DailyDataSchema,
    MinuteDataSchema,
    StockBasicSchema,
    DailyBasicSchema,
    validate_dataframe,
    standardize_symbol,
    convert_to_standard_columns,
)

# 导入所有Provider类以触发注册装饰器
# 注意：这些导入只是为了触发@register_provider装饰器的执行
from finance_data_hub.providers import tushare  # noqa: F401
from finance_data_hub.providers import xtquant  # noqa: F401

__all__ = [
    # Base classes and errors
    "BaseDataProvider",
    "ProviderError",
    "ProviderConnectionError",
    "ProviderAuthError",
    "ProviderRateLimitError",
    "ProviderDataError",
    # Registry
    "ProviderRegistry",
    "register_provider",
    # Schema validation
    "DailyDataSchema",
    "MinuteDataSchema",
    "StockBasicSchema",
    "DailyBasicSchema",
    "validate_dataframe",
    "standardize_symbol",
    "convert_to_standard_columns",
]
