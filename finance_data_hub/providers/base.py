"""
数据提供者基类和错误定义

提供所有数据提供者的抽象基类和统一的错误处理机制。
"""

import time
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Callable
import pandas as pd
from loguru import logger


# ===========================
# 错误类定义
# ===========================


class ProviderError(Exception):
    """数据提供者基础错误类"""

    def __init__(self, message: str, provider_name: str = "Unknown", **kwargs):
        super().__init__(message)
        self.provider_name = provider_name
        self.extra_info = kwargs


class ProviderConnectionError(ProviderError):
    """连接错误：网络问题、超时等"""

    pass


class ProviderAuthError(ProviderError):
    """认证错误：Token无效、权限不足等"""

    pass


class ProviderRateLimitError(ProviderError):
    """限频错误：请求过于频繁"""

    def __init__(
        self,
        message: str,
        provider_name: str = "Unknown",
        retry_after: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(message, provider_name, **kwargs)
        self.retry_after = retry_after  # 秒


class ProviderDataError(ProviderError):
    """数据错误：格式不正确、验证失败等"""

    pass


# ===========================
# Provider 基类
# ===========================


class BaseDataProvider(ABC):
    """
    数据提供者抽象基类

    所有具体的数据提供者（TushareProvider、XTQuantProvider等）都必须继承此类。
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        初始化数据提供者

        Args:
            name: 提供者名称（例如 "tushare", "xtquant"）
            config: 提供者配置字典
        """
        self.name = name
        self.config = config or {}
        self._is_initialized = False
        logger.info(f"Initializing provider: {self.name}")

    @abstractmethod
    def initialize(self) -> None:
        """
        初始化提供者（建立连接、验证凭证等）

        Raises:
            ProviderError: 初始化失败时抛出
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        健康检查

        Returns:
            bool: 提供者是否可用
        """
        pass

    @abstractmethod
    def get_stock_basic(
        self,
        market: Optional[str] = None,
        list_status: Optional[str] = "L",
    ) -> pd.DataFrame:
        """
        获取股票基本信息

        Args:
            market: 市场代码（例如 "SH", "SZ"），None表示全部
            list_status: 上市状态（L=上市 D=退市 P=暂停），None表示全部

        Returns:
            pd.DataFrame: 标准格式的股票基本信息
                必需列: symbol, name, market, industry, area, list_status, list_date

        Raises:
            ProviderError: 获取数据失败时抛出
        """
        pass

    @abstractmethod
    def get_daily_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adj: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取日线行情数据

        Args:
            symbol: 股票代码（例如 "600519.SH"）
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            adj: 复权类型（None=不复权, "qfq"=前复权, "hfq"=后复权）

        Returns:
            pd.DataFrame: 标准格式的日线数据
                必需列: time, symbol, open, high, low, close, volume, amount

        Raises:
            ProviderError: 获取数据失败时抛出
        """
        pass

    @abstractmethod
    def get_minute_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        freq: str = "1m",
    ) -> pd.DataFrame:
        """
        获取分钟级行情数据

        Args:
            symbol: 股票代码（例如 "600519.SH"）
            start_date: 开始日期时间（YYYY-MM-DD HH:MM:SS）
            end_date: 结束日期时间（YYYY-MM-DD HH:MM:SS）
            freq: 频率（"1m", "5m", "15m", "30m", "60m"）

        Returns:
            pd.DataFrame: 标准格式的分钟数据
                必需列: time, symbol, open, high, low, close, volume, amount

        Raises:
            ProviderError: 获取数据失败时抛出
        """
        pass

    @abstractmethod
    def get_daily_basic(
        self,
        symbol: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取每日指标数据

        Args:
            symbol: 股票代码（例如 "600519.SH"），None表示全部
            trade_date: 交易日期（YYYY-MM-DD），与start_date/end_date互斥
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）

        Returns:
            pd.DataFrame: 标准格式的每日指标数据
                必需列: time, symbol, turnover_rate, pe, pe_ttm, pb, total_mv, circ_mv

        Raises:
            ProviderError: 获取数据失败时抛出
        """
        pass

    def retry_on_failure(
        self,
        func: Callable,
        *args,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        **kwargs,
    ) -> Any:
        """
        带指数退避的重试机制

        Args:
            func: 要重试的函数
            *args: 函数位置参数
            max_retries: 最大重试次数
            base_delay: 基础延迟时间（秒）
            max_delay: 最大延迟时间（秒）
            exponential_base: 指数基数
            **kwargs: 函数关键字参数

        Returns:
            函数执行结果

        Raises:
            ProviderError: 重试耗尽后仍失败时抛出
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"Retry succeeded on attempt {attempt + 1}")
                return result

            except ProviderRateLimitError as e:
                last_exception = e
                if e.retry_after:
                    wait_time = min(e.retry_after, max_delay)
                else:
                    wait_time = min(
                        base_delay * (exponential_base**attempt), max_delay
                    )

                if attempt < max_retries:
                    logger.warning(
                        f"Rate limit hit, retrying in {wait_time:.2f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Rate limit retry exhausted after {max_retries} attempts")
                    raise

            except (ProviderConnectionError, ProviderDataError) as e:
                last_exception = e
                wait_time = min(
                    base_delay * (exponential_base**attempt), max_delay
                )

                if attempt < max_retries:
                    logger.warning(
                        f"Provider error: {str(e)}, retrying in {wait_time:.2f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Retry exhausted after {max_retries} attempts")
                    raise

            except ProviderAuthError:
                # 认证错误不重试，直接抛出
                logger.error("Authentication error - no retry")
                raise

            except Exception as e:
                # 未知错误，包装后抛出
                logger.exception(f"Unexpected error in provider {self.name}")
                raise ProviderError(
                    f"Unexpected error: {str(e)}", provider_name=self.name
                ) from e

        # 如果所有重试都失败，抛出最后一个异常
        if last_exception:
            raise last_exception

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name='{self.name}')>"
