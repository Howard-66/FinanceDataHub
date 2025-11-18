"""
数据提供者基类和错误定义

提供所有数据提供者的抽象基类和统一的错误处理机制。
"""

import time
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Callable, Tuple
from datetime import datetime
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

    @abstractmethod
    async def get_latest_record(
        self, symbol: str, data_type: str, table_name: str
    ) -> Optional[pd.DataFrame]:
        """
        获取数据库中指定symbol和数据类型的最新记录

        Args:
            symbol: 股票代码（如 "600519.SH"）
            data_type: 数据类型（如 "daily", "minute", "daily_basic" 等）
            table_name: 数据库表名（如 "symbol_daily", "symbol_minute" 等）

        Returns:
            Optional[pd.DataFrame]: 最新记录，包含所有列。如果不存在返回None

        Raises:
            ProviderError: 查询失败时抛出
        """
        pass

    @abstractmethod
    def should_overwrite_latest_record(
        self,
        latest_record_time: datetime,
        current_time: datetime,
        data_type: str,
    ) -> bool:
        """
        判断是否应该覆盖最新的记录

        Args:
            latest_record_time: 数据库中最新记录的时间
            current_time: 当前时间
            data_type: 数据类型（如 "daily", "minute" 等）

        Returns:
            bool: 如果应该覆盖返回True，否则返回False

        Raises:
            ProviderError: 判断失败时抛出
        """
        pass

    @abstractmethod
    async def get_incremental_data(
        self,
        symbol: Optional[str],
        data_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取增量数据

        Args:
            symbol: 股票代码，为None时表示批量更新所有股票
            data_type: 数据类型（如 "daily", "minute_1", "daily_basic" 等）
            start_date: 开始日期，为None时表示使用智能计算的日期范围
            end_date: 结束日期，为None时表示使用智能计算的日期范围
            **kwargs: 其他参数（如 adj, freq 等）

        Returns:
            pd.DataFrame: 增量数据，标准格式

        Raises:
            ProviderError: 获取数据失败时抛出
        """
        pass

    def calculate_date_range(
        self,
        latest_record: Optional[pd.DataFrame],
        data_type: str,
        current_time: datetime,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        智能计算日期范围

        Args:
            latest_record: 最新记录DataFrame，为None表示新symbol
            data_type: 数据类型（如 "daily", "minute_1" 等）
            current_time: 当前时间

        Returns:
            Tuple[Optional[str], Optional[str]]: (start_date, end_date)
                如果返回 (None, None)，表示需要全量更新
                如果返回 (start_date, end_date)，表示增量更新

        Raises:
            ProviderError: 计算失败时抛出
        """
        if not latest_record or latest_record.empty:
            # 新symbol，返回None表示全量更新
            logger.info(f"New symbol detected - will fetch full historical data")
            return None, None

        # 获取最新记录时间
        if "time" not in latest_record.columns:
            raise ProviderError(
                "Latest record must contain 'time' column",
                provider_name=self.name
            )

        latest_time = latest_record["time"].iloc[0]
        if isinstance(latest_time, str):
            latest_time = pd.to_datetime(latest_time)

        # 根据数据类型计算日期范围
        if data_type == "daily":
            return self._calculate_daily_range(latest_time, current_time)
        elif data_type.startswith("minute"):
            return self._calculate_minute_range(latest_time, current_time)
        elif data_type in ["daily_basic", "adj_factor"]:
            # 非时间序列数据，处理方式类似日线
            return self._calculate_daily_range(latest_time, current_time)
        else:
            raise ProviderError(
                f"Unsupported data type for incremental update: {data_type}",
                provider_name=self.name
            )

    def _calculate_daily_range(
        self,
        latest_time: datetime,
        current_time: datetime,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        计算日线数据的日期范围

        Args:
            latest_time: 最新记录时间
            current_time: 当前时间

        Returns:
            Tuple[str, str]: (start_date, end_date)
        """
        from datetime import timedelta

        # 检查是否跨天了
        latest_date = latest_time.date()
        current_date = current_time.date()

        if latest_date < current_date:
            # 计算下一个交易日（这里简化处理，实际应该考虑交易日历）
            next_trading_day = latest_date + timedelta(days=1)
            start_date = next_trading_day.strftime("%Y-%m-%d")
            end_date = current_date.strftime("%Y-%m-%d")
            logger.debug(
                f"Calculated daily range: {start_date} to {end_date} "
                f"(latest: {latest_date}, current: {current_date})"
            )
            return start_date, end_date
        else:
            # 同一天，可能需要覆盖盘中数据
            current_date_str = current_date.strftime("%Y-%m-%d")
            logger.debug(
                f"Same day data detected - checking if overwrite is needed "
                f"(latest: {latest_time}, current: {current_time})"
            )
            return None, current_date_str

    def _calculate_minute_range(
        self,
        latest_time: datetime,
        current_time: datetime,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        计算分钟级数据的日期范围

        Args:
            latest_time: 最新记录时间
            current_time: 当前时间

        Returns:
            Tuple[str, str]: (start_time, end_time)
        """
        from datetime import timedelta

        # 计算下一分钟
        next_minute = latest_time + timedelta(minutes=1)
        start_time = next_minute.strftime("%Y-%m-%d %H:%M:%S")
        end_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

        logger.debug(
            f"Calculated minute range: {start_time} to {end_time}"
        )
        return start_time, end_time

    def is_trading_hours(self, current_time: Optional[datetime] = None) -> bool:
        """
        判断当前时间是否在交易时间内

        Args:
            current_time: 要检查的时间，为None时使用当前时间

        Returns:
            bool: 是否在交易时间内
        """
        if current_time is None:
            current_time = datetime.now()

        # 简化的中国股市交易时间判断（不考虑节假日）
        # 上午: 9:30-11:30
        # 下午: 13:00-15:00
        # 周一到周五

        weekday = current_time.weekday()  # 0=周一, 6=周日
        if weekday >= 5:  # 周六周日
            return False

        hour = current_time.hour
        minute = current_time.minute

        # 上午交易时间
        if hour == 9 and minute >= 30:
            return True
        elif 10 <= hour <= 11:
            return True
        elif hour == 11 and minute <= 30:
            return True

        # 下午交易时间
        if 13 <= hour < 15:
            return True

        return False
