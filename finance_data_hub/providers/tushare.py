"""
Tushare数据提供者

提供从Tushare Pro API获取金融数据的功能。
"""

import time
from typing import Optional, Dict, Any
from datetime import datetime
import pandas as pd
import tushare as ts
from loguru import logger

from finance_data_hub.providers.base import (
    BaseDataProvider,
    ProviderError,
    ProviderAuthError,
    ProviderConnectionError,
    ProviderRateLimitError,
    ProviderDataError,
)
from finance_data_hub.providers.registry import register_provider
from finance_data_hub.providers.schema import (
    StockBasicSchema,
    DailyDataSchema,
    MinuteDataSchema,
    DailyBasicSchema,
    CNGDPSchema,
    CNPPISchema,
    CNMSchema,
    CNPMISchema,
    IndexDailybasicSchema,
    FinaIndicatorSchema,
    CashflowSchema,
    validate_dataframe,
    convert_to_standard_columns,
)


@register_provider("tushare")
class TushareProvider(BaseDataProvider):
    """
    Tushare Pro数据提供者

    使用Tushare Pro API获取中国A股市场数据。

    配置参数:
        token (str): Tushare Pro API Token
        timeout (int): 请求超时时间（秒），默认30
        max_retry (int): 最大重试次数，默认3
        retry_delay (float): 重试延迟（秒），默认1.0
    """

    def __init__(self, name: str = "tushare", config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.token: Optional[str] = None
        self.pro_api: Optional[Any] = None
        self.timeout: int = config.get("timeout", 30) if config else 30
        self.max_retry: int = config.get("max_retry", 3) if config else 3
        self.retry_delay: float = config.get("retry_delay", 1.0) if config else 1.0

        # Tushare API调用统计
        self._call_count = 0
        self._last_call_time = 0.0

    def initialize(self) -> None:
        """
        初始化Tushare Provider

        从配置中获取Token并创建Pro API实例。

        Raises:
            ProviderAuthError: Token无效或缺失
            ProviderError: 初始化失败
        """
        if self._is_initialized:
            logger.debug("TushareProvider already initialized")
            return

        # 获取Token
        self.token = self.config.get("token")
        if not self.token:
            raise ProviderAuthError(
                "Tushare token is required. Please set TUSHARE_TOKEN in config.",
                provider_name=self.name,
            )

        try:
            # 设置Token并获取Pro API
            ts.set_token(self.token)
            self.pro_api = ts.pro_api()

            # 验证Token有效性（调用一个简单的接口）
            try:
                self.pro_api.trade_cal(
                    exchange="SSE", start_date="20240101", end_date="20240101"
                )
                logger.info("TushareProvider initialized successfully")
            except Exception as e:
                raise ProviderAuthError(
                    f"Invalid Tushare token or API connection failed: {str(e)}",
                    provider_name=self.name,
                ) from e

            self._is_initialized = True

        except ProviderAuthError:
            raise
        except Exception as e:
            raise ProviderError(
                f"Failed to initialize TushareProvider: {str(e)}",
                provider_name=self.name,
            ) from e

    def health_check(self) -> bool:
        """
        健康检查

        验证Tushare API是否可用。

        Returns:
            bool: API是否可用
        """
        if not self._is_initialized:
            return False

        try:
            # 简单调用trade_cal接口检查连通性
            self.pro_api.trade_cal(
                exchange="SSE", start_date="20240101", end_date="20240101"
            )
            return True
        except Exception as e:
            logger.warning(f"Tushare health check failed: {str(e)}")
            return False

    def _rate_limit_check(self) -> None:
        """
        限频检查

        Tushare Pro API有调用频率限制，确保不超过限制。
        免费用户：每分钟最多200次调用
        """
        current_time = time.time()

        # 如果距离上次调用小于0.3秒（每分钟200次 = 每次间隔0.3秒），则等待
        time_since_last_call = current_time - self._last_call_time
        if time_since_last_call < 0.3:
            wait_time = 0.3 - time_since_last_call
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)

        self._last_call_time = time.time()
        self._call_count += 1

    def _call_api(
        self, api_name: str, fields: Optional[str] = None, **kwargs
    ) -> pd.DataFrame:
        """
        调用Tushare API的通用方法

        Args:
            api_name: API接口名称（例如 "stock_basic", "daily"）
            fields: 返回字段列表（逗号分隔字符串）
            **kwargs: API参数

        Returns:
            pd.DataFrame: API返回的数据

        Raises:
            ProviderError: API调用失败
        """
        if not self._is_initialized:
            raise ProviderError(
                "TushareProvider not initialized", provider_name=self.name
            )

        # 限频检查
        self._rate_limit_check()

        # 获取API方法
        api_method = getattr(self.pro_api, api_name, None)
        if not api_method:
            raise ProviderError(
                f"Tushare API method '{api_name}' not found",
                provider_name=self.name,
            )

        # 调用API（使用重试机制）
        def _call():
            try:
                if fields:
                    return api_method(fields=fields, **kwargs)
                else:
                    return api_method(**kwargs)
            except Exception as e:
                error_msg = str(e)
                if "抱歉，您每分钟最多访问" in error_msg:
                    raise ProviderRateLimitError(
                        "Tushare rate limit exceeded",
                        provider_name=self.name,
                        retry_after=60,
                    )
                elif "权限" in error_msg or "积分" in error_msg:
                    raise ProviderAuthError(
                        f"Insufficient permissions: {error_msg}",
                        provider_name=self.name,
                    )
                else:
                    raise ProviderConnectionError(
                        f"Tushare API call failed: {error_msg}",
                        provider_name=self.name,
                    )

        result = self.retry_on_failure(_call, max_retries=self.max_retry)

        # 确保result是DataFrame
        if not isinstance(result, pd.DataFrame):
            logger.error(f"API {api_name} returned non-DataFrame type: {type(result)}")
            return pd.DataFrame()

        if result.empty:
            logger.warning(f"Empty result from Tushare API: {api_name}")
            return pd.DataFrame()

        return result

    def get_stock_basic(
        self,
        market: Optional[str] = None,
        list_status: Optional[str] = "L",
    ) -> pd.DataFrame:
        """
        获取股票基本信息

        Args:
            market: 市场代码（SH/SZ），None表示全部
            list_status: 上市状态（L=上市 D=退市 P=暂停），None表示全部

        Returns:
            pd.DataFrame: 标准格式的股票基本信息
        """
        logger.info(
            f"Fetching stock basic info (market={market}, list_status={list_status})"
        )

        # 调用Tushare API
        df = self._call_api(
            "stock_basic",
            fields="ts_code,name,area,industry,market,list_date,delist_date,is_hs",
            list_status=list_status,
            exchange=market,
        )

        if df.empty:
            return pd.DataFrame(columns=StockBasicSchema.get_required_columns())

        # 列名映射
        column_mapping = {
            "ts_code": "symbol",  # Tushare的ts_code就是我们需要的symbol格式
            "name": "name",
            "market": "market",
            "industry": "industry",
            "area": "area",
            "list_date": "list_date",
            "delist_date": "delist_date",
            "is_hs": "is_hs",
        }

        df = convert_to_standard_columns(df, column_mapping)

        # 添加list_status列
        df["list_status"] = list_status if list_status else "L"

        # 验证数据格式
        df = validate_dataframe(df, StockBasicSchema, provider_name=self.name)

        logger.info(f"Fetched {len(df)} stocks from Tushare")
        return df

    def get_daily_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adj: Optional[str] = None,
        trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取日线行情数据（自动处理Tushare 6000条记录限制）

        当返回记录数等于6000时，自动继续获取更早的数据，确保获取完整历史数据。
        当提供trade_date参数时，批量获取指定交易日所有股票的数据。

        Args:
            symbol: 股票代码（例如 "600519.SH"），为None且提供trade_date时获取所有股票
            start_date: 开始日期（YYYY-MM-DD 或 YYYYMMDD），为None时获取全部历史数据
            end_date: 结束日期（YYYY-MM-DD 或 YYYYMMDD），为None时获取到最新数据
            adj: 复权类型（None=不复权, "qfq"=前复权, "hfq"=后复权）
            trade_date: 交易日（YYYY-MM-DD 或 YYYYMMDD），批量获取当日所有股票数据

        Returns:
            pd.DataFrame: 标准格式的日线数据
        """
        logger.info(
            f"Fetching daily data for {symbol or 'all stocks'} from {start_date or 'beginning'} to {end_date or 'latest'} (adj={adj}, trade_date={trade_date})"
        )

        # 处理 trade_date 批量模式
        if trade_date:
            logger.info(f"Using trade_date batch mode for {trade_date}")
            # 转换日期格式
            trade_date_clean = trade_date.replace("-", "")

            # 验证复权类型 - trade_date 模式通常不复权
            if adj:
                logger.warning(f"trade_date mode does not support adj parameter, ignoring adj={adj}")
                adj = None

            # 使用 trade_date 参数批量获取当日所有股票数据
            api_name = "daily"
            kwargs = {
                "trade_date": trade_date_clean,
            }
            logger.debug(f"Using daily API with trade_date={trade_date_clean}")

            df = self._call_api(api_name, **kwargs)

            if df.empty:
                logger.warning(f"No data returned for trade_date={trade_date}")
                return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

            # 列名映射
            column_mapping = {
                "trade_date": "time",
                "ts_code": "symbol",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "amount": "amount",
                "pct_chg": "change_pct",
                "change": "change_amount",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

            # 验证数据格式
            df = validate_dataframe(df, DailyDataSchema, provider_name=self.name)

            # 按时间排序
            df = df.sort_values("time").reset_index(drop=True)

            logger.info(f"Fetched {len(df)} daily records for trade_date={trade_date}")
            return df

        # 常规单股票模式（处理 6000 条限制）
        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 6000

        # 记录所有批次的数据
        all_dataframes = []
        current_end_date = end_date
        batch_count = 0
        total_records = 0

        while True:
            batch_count += 1
            logger.info(f"Batch {batch_count}: fetching data up to {current_end_date or 'latest'}")

            # 处理日期格式（去掉横杠）
            start_date_clean = None  # 始终从最早开始，让API返回完整的6000条
            end_date_clean = current_end_date.replace("-", "") if current_end_date else None

            # 根据复权类型选择API
            if adj:
                # 需要复权数据，使用 pro_bar API
                api_name = "pro_bar"
                kwargs = {
                    "ts_code": symbol,
                    "start_date": start_date_clean,
                    "end_date": end_date_clean,
                    "freq": "D",
                    "adj": adj,
                }
                logger.debug(f"Using pro_bar API for adjusted data (adj={adj})")
            else:
                # 不需要复权，使用 daily API
                api_name = "daily"
                kwargs = {
                    "ts_code": symbol,
                    "start_date": start_date_clean,
                    "end_date": end_date_clean,
                }
                logger.debug(f"Using daily API")

            df = self._call_api(api_name, **kwargs)

            # 调试信息
            logger.debug(f"API {api_name} returned type: {type(df)}")
            if isinstance(df, pd.DataFrame):
                logger.debug(f"Batch {batch_count} DataFrame shape: {df.shape}")
                logger.debug(f"DataFrame columns: {df.columns.tolist()}")
            else:
                logger.error(f"Unexpected return type: {type(df)}, value: {df}")
                break

            if df.empty:
                logger.info(f"Batch {batch_count}: No data returned, stopping")
                break

            # 列名映射
            column_mapping = {
                "trade_date": "time",
                "ts_code": "symbol",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "amount": "amount",
                "pct_chg": "change_pct",
                "change": "change_amount",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

            # 验证数据格式
            df = validate_dataframe(df, DailyDataSchema, provider_name=self.name)

            # 按时间排序
            df = df.sort_values("time").reset_index(drop=True)

            batch_records = len(df)
            total_records += batch_records
            all_dataframes.append(df)

            logger.info(f"Batch {batch_count}: Fetched {batch_records} records (total so far: {total_records})")

            # 检查是否需要继续获取更早的数据
            if batch_records == TUSHARE_MAX_RECORDS:
                # 获取当前批次中最早的日期
                earliest_date = df["time"].min().date()
                logger.info(f"Batch {batch_count}: Got {batch_records} records (max limit), fetching earlier data...")

                # 计算新的结束日期（向前推1天）
                from datetime import timedelta
                new_end_date = earliest_date - timedelta(days=1)
                current_end_date = new_end_date.strftime("%Y-%m-%d")

                logger.info(f"Next batch will fetch up to {current_end_date}")
                continue
            else:
                # 记录数少于6000，说明已经获取完所有数据
                logger.info(f"Batch {batch_count}: Got {batch_records} records (< {TUSHARE_MAX_RECORDS}), all data fetched")
                break

        # 合并所有批次的数据
        if not all_dataframes:
            logger.warning(f"No data fetched for {symbol}")
            return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)

        # 去重（如果批次间有重叠）
        final_df = final_df.drop_duplicates(subset=["time", "symbol"]).sort_values("time").reset_index(drop=True)

        # 最终验证
        final_df = validate_dataframe(final_df, DailyDataSchema, provider_name=self.name)

        logger.info(f"Total fetched {len(final_df)} daily records for {symbol} in {batch_count} batch(es)")
        return final_df

    def _get_adj_factor(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        获取复权因子

        Args:
            symbol: 股票代码
            start_date: 开始日期（YYYYMMDD）
            end_date: 结束日期（YYYYMMDD）

        Returns:
            pd.DataFrame: 包含复权因子的DataFrame
        """
        try:
            df = self._call_api(
                "adj_factor",
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date,
            )
            if not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
            return df
        except Exception as e:
            logger.warning(f"Failed to fetch adj_factor for {symbol}: {str(e)}")
            return pd.DataFrame()

    def get_minute_data(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = "1m",
    ) -> pd.DataFrame:
        """
        获取分钟级行情数据（自动处理Tushare 6000条记录限制）

        当返回记录数等于6000时，自动继续获取更早的数据，确保获取完整历史数据。

        注意：Tushare Pro的分钟数据需要较高权限，免费用户可能无法访问。

        Args:
            symbol: 股票代码
            start_date: 开始日期时间，为None时获取全部数据
            end_date: 结束日期时间，为None时获取到最新数据
            freq: 频率（1min, 5min, 15min, 30min, 60min）

        Returns:
            pd.DataFrame: 标准格式的分钟数据
        """
        logger.info(
            f"Fetching {freq} data for {symbol} from {start_date or 'beginning'} to {end_date or 'latest'}"
        )

        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 6000

        # Tushare的频率映射和对应的timedelta
        freq_mapping = {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "60m": "60min",
        }

        # 计算向前推的时间间隔
        freq_timedelta_mapping = {
            "1m": 1,    # 1分钟
            "5m": 5,    # 5分钟
            "15m": 15,  # 15分钟
            "30m": 30,  # 30分钟
            "60m": 60,  # 60分钟
        }

        tushare_freq = freq_mapping.get(freq, "1min")
        step_minutes = freq_timedelta_mapping.get(freq, 1)

        # 记录所有批次的数据
        all_dataframes = []
        current_end_date = end_date
        batch_count = 0
        total_records = 0

        while True:
            batch_count += 1
            logger.info(f"Batch {batch_count}: fetching {freq} data up to {current_end_date or 'latest'}")

            # 转换日期格式
            start_date_clean = None  # 始终从最早开始
            end_date_clean = current_end_date.replace("-", "").replace(" ", "").replace(":", "")[:8] if current_end_date else None

            try:
                df = self._call_api(
                    "pro_bar",
                    ts_code=symbol,
                    start_date=start_date_clean,
                    end_date=end_date_clean,
                    freq=tushare_freq,
                )
            except ProviderAuthError:
                logger.warning(
                    "Insufficient permissions for minute data. "
                    "Upgrade your Tushare account for access."
                )
                return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

            if df.empty:
                logger.info(f"Batch {batch_count}: No data returned, stopping")
                break

            # 列名映射
            column_mapping = {
                "trade_time": "time",
                "ts_code": "symbol",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "amount": "amount",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["time"] = pd.to_datetime(df["time"])

            # 验证数据格式
            df = validate_dataframe(df, MinuteDataSchema, provider_name=self.name)

            # 按时间排序
            df = df.sort_values("time").reset_index(drop=True)

            batch_records = len(df)
            total_records += batch_records
            all_dataframes.append(df)

            logger.info(f"Batch {batch_count}: Fetched {batch_records} records (total so far: {total_records})")

            # 检查是否需要继续获取更早的数据
            if batch_records == TUSHARE_MAX_RECORDS:
                # 获取当前批次中最早的时间
                earliest_time = df["time"].min()
                logger.info(f"Batch {batch_count}: Got {batch_records} records (max limit), fetching earlier data...")

                # 计算新的结束时间（向前推对应的分钟数）
                from datetime import timedelta
                new_end_time = earliest_time - timedelta(minutes=step_minutes)
                current_end_date = new_end_time.strftime("%Y-%m-%d %H:%M:%S")

                logger.info(f"Next batch will fetch up to {current_end_date}")
                continue
            else:
                # 记录数少于6000，说明已经获取完所有数据
                logger.info(f"Batch {batch_count}: Got {batch_records} records (< {TUSHARE_MAX_RECORDS}), all data fetched")
                break

        # 合并所有批次的数据
        if not all_dataframes:
            logger.warning(f"No minute data fetched for {symbol}")
            return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)

        # 去重（如果批次间有重叠）
        final_df = final_df.drop_duplicates(subset=["time", "symbol"]).sort_values("time").reset_index(drop=True)

        # 最终验证
        final_df = validate_dataframe(final_df, MinuteDataSchema, provider_name=self.name)

        logger.info(f"Total fetched {len(final_df)} {freq} records for {symbol} in {batch_count} batch(es)")
        return final_df

    def get_daily_basic(
        self,
        symbol: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取每日指标数据（自动处理Tushare 6000条记录限制）

        当返回记录数等于6000时，自动继续获取更早的数据，确保获取完整历史数据。
        当提供trade_date参数时，批量获取指定交易日所有股票的数据。

        Args:
            symbol: 股票代码，None表示全部
            trade_date: 交易日期（YYYY-MM-DD），与start_date/end_date互斥
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pd.DataFrame: 标准格式的每日指标数据
        """
        logger.info(
            f"Fetching daily basic for symbol={symbol}, "
            f"trade_date={trade_date}, start_date={start_date}, end_date={end_date}"
        )

        # 处理 trade_date 批量模式
        if trade_date:
            logger.info(f"Using trade_date batch mode for {trade_date}")
            # 转换日期格式
            trade_date_clean = trade_date.replace("-", "")

            # 使用 trade_date 参数批量获取当日所有股票数据
            api_name = "daily_basic"
            kwargs = {
                "trade_date": trade_date_clean,
            }
            logger.debug(f"Using daily_basic API with trade_date={trade_date_clean}")

            df = self._call_api(api_name, **kwargs)

            if df.empty:
                logger.warning(f"No data returned for trade_date={trade_date}")
                return pd.DataFrame(columns=DailyBasicSchema.get_required_columns())

            # 列名映射
            column_mapping = {
                "trade_date": "time",
                "ts_code": "symbol",
                "turnover_rate": "turnover_rate",
                "turnover_rate_f": "volume_ratio",  # 注意：Tushare没有直接的量比字段
                "pe": "pe",
                "pe_ttm": "pe_ttm",
                "pb": "pb",
                "ps": "ps",
                "ps_ttm": "ps_ttm",
                "dv_ratio": "dv_ratio",
                "dv_ttm": "dv_ttm",
                "total_share": "total_share",
                "float_share": "float_share",
                "free_share": "free_share",
                "total_mv": "total_mv",
                "circ_mv": "circ_mv",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

            # 验证数据格式
            df = validate_dataframe(df, DailyBasicSchema, provider_name=self.name)

            # 按时间排序
            df = df.sort_values("time").reset_index(drop=True)

            logger.info(f"Fetched {len(df)} daily basic records for trade_date={trade_date}")
            return df

        # 常规模式（处理 6000 条限制）
        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 6000

        # 转换日期格式
        if start_date:
            start_date = start_date.replace("-", "")
        if end_date:
            end_date = end_date.replace("-", "")

        # 构建参数
        kwargs = {}
        if symbol:
            kwargs["ts_code"] = symbol
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        # 记录所有批次的数据
        all_dataframes = []
        current_end_date = end_date
        batch_count = 0
        total_records = 0

        while True:
            batch_count += 1
            logger.info(f"Batch {batch_count}: fetching data up to {current_end_date or 'latest'}")

            # 处理日期格式（去掉横杠）
            start_date_clean = start_date  # 使用原始start_date（可能为None）
            end_date_clean = current_end_date.replace("-", "") if current_end_date else None

            # 构建当前批次的参数
            batch_kwargs = kwargs.copy()
            if start_date_clean:
                batch_kwargs["start_date"] = start_date_clean
            if end_date_clean:
                batch_kwargs["end_date"] = end_date_clean

            df = self._call_api("daily_basic", **batch_kwargs)

            if df.empty:
                logger.info(f"Batch {batch_count}: No data returned, stopping")
                break

            # 列名映射
            column_mapping = {
                "trade_date": "time",
                "ts_code": "symbol",
                "turnover_rate": "turnover_rate",
                "turnover_rate_f": "volume_ratio",  # 注意：Tushare没有直接的量比字段
                "pe": "pe",
                "pe_ttm": "pe_ttm",
                "pb": "pb",
                "ps": "ps",
                "ps_ttm": "ps_ttm",
                "dv_ratio": "dv_ratio",
                "dv_ttm": "dv_ttm",
                "total_share": "total_share",
                "float_share": "float_share",
                "free_share": "free_share",
                "total_mv": "total_mv",
                "circ_mv": "circ_mv",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

            # 验证数据格式
            df = validate_dataframe(df, DailyBasicSchema, provider_name=self.name)

            # 按时间排序
            df = df.sort_values("time").reset_index(drop=True)

            batch_records = len(df)
            total_records += batch_records
            all_dataframes.append(df)

            logger.info(f"Batch {batch_count}: Fetched {batch_records} records (total so far: {total_records})")

            # 检查是否需要继续获取更早的数据
            if batch_records == TUSHARE_MAX_RECORDS:
                # 获取当前批次中最早的日期
                earliest_date = df["time"].min().date()
                logger.info(f"Batch {batch_count}: Got {batch_records} records (max limit), fetching earlier data...")

                # 计算新的结束日期（向前推1天）
                from datetime import timedelta
                new_end_date = earliest_date - timedelta(days=1)
                current_end_date = new_end_date.strftime("%Y-%m-%d")

                logger.info(f"Next batch will fetch up to {current_end_date}")
                continue
            else:
                # 记录数少于6000，说明已经获取完所有数据
                logger.info(f"Batch {batch_count}: Got {batch_records} records (< {TUSHARE_MAX_RECORDS}), all data fetched")
                break

        # 合并所有批次的数据
        if not all_dataframes:
            logger.warning(f"No daily basic data fetched for {symbol}")
            return pd.DataFrame(columns=DailyBasicSchema.get_required_columns())

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)

        # 去重（如果批次间有重叠）
        final_df = final_df.drop_duplicates(subset=["time", "symbol"]).sort_values("time").reset_index(drop=True)

        # 最终验证
        final_df = validate_dataframe(final_df, DailyBasicSchema, provider_name=self.name)

        logger.info(f"Total fetched {len(final_df)} daily basic records for {symbol} in {batch_count} batch(es)")
        return final_df

    def get_adj_factor(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取复权因子（自动处理Tushare 6000条记录限制）

        当返回记录数等于6000时，自动继续获取更早的数据，确保获取完整历史数据。

        Args:
            symbol: 股票代码（例如 "600519.SH"），None表示全部
            start_date: 开始日期（YYYY-MM-DD 或 YYYYMMDD）
            end_date: 结束日期（YYYY-MM-DD 或 YYYYMMDD）

        Returns:
            pd.DataFrame: 包含symbol, time, adj_factor的DataFrame
        """
        logger.info(
            f"Fetching adj factor for symbol={symbol}, "
            f"start_date={start_date}, end_date={end_date}"
        )

        # 转换日期格式
        if start_date:
            start_date = start_date.replace("-", "")
        if end_date:
            end_date = end_date.replace("-", "")

        # 构建参数
        kwargs = {}
        if symbol:
            kwargs["ts_code"] = symbol
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 6000

        # 记录所有批次的数据
        all_dataframes = []
        current_end_date = end_date
        batch_count = 0
        total_records = 0

        while True:
            batch_count += 1
            logger.info(f"Batch {batch_count}: fetching data up to {current_end_date or 'latest'}")

            # 处理日期格式（去掉横杠）
            start_date_clean = start_date  # 使用原始start_date（可能为None）
            end_date_clean = current_end_date.replace("-", "") if current_end_date else None

            # 构建当前批次的参数
            batch_kwargs = kwargs.copy()
            if start_date_clean:
                batch_kwargs["start_date"] = start_date_clean
            if end_date_clean:
                batch_kwargs["end_date"] = end_date_clean

            df = self._call_api("adj_factor", **batch_kwargs)

            if df.empty:
                logger.info(f"Batch {batch_count}: No data returned, stopping")
                break

            # 列名映射
            column_mapping = {
                "ts_code": "symbol",
                "trade_date": "time",
                "adj_factor": "adj_factor",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

            # 按时间排序
            df = df.sort_values("time").reset_index(drop=True)

            batch_records = len(df)
            total_records += batch_records
            all_dataframes.append(df)

            logger.info(f"Batch {batch_count}: Fetched {batch_records} records (total so far: {total_records})")

            # 检查是否需要继续获取更早的数据
            if batch_records == TUSHARE_MAX_RECORDS:
                # 获取当前批次中最早的日期
                earliest_date = df["time"].min().date()
                logger.info(f"Batch {batch_count}: Got {batch_records} records (max limit), fetching earlier data...")

                # 计算新的结束日期（向前推1天）
                from datetime import timedelta
                new_end_date = earliest_date - timedelta(days=1)
                current_end_date = new_end_date.strftime("%Y-%m-%d")

                logger.info(f"Next batch will fetch up to {current_end_date}")
                continue
            else:
                # 记录数少于6000，说明已经获取完所有数据
                logger.info(f"Batch {batch_count}: Got {batch_records} records (< {TUSHARE_MAX_RECORDS}), all data fetched")
                break

        # 合并所有批次的数据
        if not all_dataframes:
            logger.warning(f"Empty adj_factor result for {symbol}")
            return pd.DataFrame(
                columns=["symbol", "time", "adj_factor"]
            )

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)

        # 去重（如果批次间有重叠）
        final_df = final_df.drop_duplicates(subset=["time", "symbol"]).sort_values("time").reset_index(drop=True)

        logger.info(f"Total fetched {len(final_df)} adj_factor records for {symbol} in {batch_count} batch(es)")
        return final_df

    async def get_latest_record(
        self, symbol: str, data_type: str, table_name: str
    ) -> Optional[pd.DataFrame]:
        """
        获取数据库中指定symbol和数据类型的最新记录

        注意：TushareProvider本身不直接访问数据库。
        这个方法需要外部传入数据库操作对象来执行查询。

        Args:
            symbol: 股票代码（如 "600519.SH"）
            data_type: 数据类型（如 "daily", "minute", "daily_basic" 等）
            table_name: 数据库表名（如 "symbol_daily", "symbol_minute" 等）

        Returns:
            Optional[pd.DataFrame]: 最新记录，包含所有列。如果不存在返回None

        Raises:
            ProviderError: 查询失败时抛出
        """
        # 注意：这个方法需要外部传入数据库操作
        # 实际实现会在DataUpdater中调用DataOperations的方法
        # 然后将结果传递给其他需要的方法
        logger.warning(
            "get_latest_record() requires external database access. "
            "This method should be called through DataUpdater with DataOperations."
        )
        raise ProviderError(
            "get_latest_record() requires external database operations",
            provider_name=self.name
        )

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
        """
        logger.debug(
            f"Checking if should overwrite: latest={latest_record_time}, "
            f"current={current_time}, type={data_type}"
        )

        # 根据数据类型判断
        if data_type == "daily":
            return self._should_overwrite_daily(latest_record_time, current_time)
        elif data_type.startswith("minute"):
            return self._should_overwrite_minute(latest_record_time, current_time)
        else:
            # 其他类型，默认不覆盖
            return False

    def _should_overwrite_daily(
        self, latest_record_time: datetime, current_time: datetime
    ) -> bool:
        """
        判断日线数据是否应该覆盖

        策略：
        1. 如果最新记录不是今天的数据，不覆盖
        2. 如果最新记录是今天的数据：
           - 当前在交易时间内，覆盖（盘中数据会更新）
           - 当前不在交易时间内，不覆盖（今天的数据已经收盘）
        """
        latest_date = latest_record_time.date()
        current_date = current_time.date()

        # 如果不是同一天，不需要覆盖
        if latest_date != current_date:
            logger.debug("Not same day - no overwrite needed")
            return False

        # 同一天，检查是否在交易时间内
        if self.is_trading_hours(current_time):
            logger.debug("Same day and during trading hours - will overwrite")
            return True
        else:
            logger.debug("Same day but after hours - no overwrite needed")
            return False

    def _should_overwrite_minute(
        self, latest_record_time: datetime, current_time: datetime
    ) -> bool:
        """
        判断分钟数据是否应该覆盖

        分钟数据通常在交易时间内会持续更新，所以总是覆盖
        """
        # 分钟数据在交易时间内持续更新，总是覆盖
        if self.is_trading_hours(current_time):
            return True
        else:
            # 非交易时间，检查是否跨天了
            latest_date = latest_record_time.date()
            current_date = current_time.date()
            return latest_date == current_date

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
        """
        logger.info(
            f"Getting incremental data: symbol={symbol}, data_type={data_type}, "
            f"start_date={start_date}, end_date={end_date}"
        )

        # 处理批量更新（无symbol）
        if symbol is None:
            return self._get_incremental_data_bulk(data_type, start_date, end_date, **kwargs)

        # 处理单个symbol的增量更新
        if data_type == "daily":
            return self._get_incremental_daily(symbol, start_date, end_date, **kwargs)
        elif data_type.startswith("minute"):
            freq = kwargs.get("freq", "1m")
            return self._get_incremental_minute(symbol, start_date, end_date, freq)
        elif data_type == "daily_basic":
            return self._get_incremental_daily_basic(symbol, start_date, end_date)
        elif data_type == "adj_factor":
            return self._get_incremental_adj_factor(symbol, start_date, end_date)
        else:
            raise ProviderError(
                f"Unsupported data type for incremental update: {data_type}",
                provider_name=self.name
            )

    def _get_incremental_data_bulk(
        self,
        data_type: str,
        start_date: Optional[str],
        end_date: Optional[str],
        **kwargs,
    ) -> pd.DataFrame:
        """
        批量获取增量数据（不指定symbol）

        Args:
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数

        Returns:
            pd.DataFrame: 所有股票的增量数据
        """
        logger.debug(f"Getting bulk incremental data for type: {data_type}")

        if data_type == "daily":
            # Tushare支持使用trade_date批量获取所有股票数据
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if not start_date:
                start_date = end_date  # 默认只获取今天的

            # 转换日期格式
            trade_date = end_date.replace("-", "")

            df = self._call_api(
                "daily",
                trade_date=trade_date,
            )

            if df.empty:
                return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

            # 转换格式
            column_mapping = {
                "trade_date": "time",
                "ts_code": "symbol",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "vol": "volume",
                "amount": "amount",
                "pct_chg": "change_pct",
                "change": "change_amount",
            }
            df = convert_to_standard_columns(df, column_mapping)
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d")

            return df

        else:
            raise ProviderError(
                f"Bulk update not supported for data type: {data_type}",
                provider_name=self.name
            )

    def _get_incremental_daily(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取日线增量数据

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数（如 adj）

        Returns:
            pd.DataFrame: 日线数据
        """
        adj = kwargs.get("adj")

        if not start_date or not end_date:
            raise ProviderError(
                "start_date and end_date are required for incremental daily data",
                provider_name=self.name
            )

        return self.get_daily_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adj=adj,
        )

    def _get_incremental_minute(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
        freq: str,
    ) -> pd.DataFrame:
        """
        获取分钟增量数据

        Args:
            symbol: 股票代码
            start_date: 开始日期时间
            end_date: 结束日期时间
            freq: 频率

        Returns:
            pd.DataFrame: 分钟数据
        """
        if not start_date or not end_date:
            raise ProviderError(
                "start_date and end_date are required for incremental minute data",
                provider_name=self.name
            )

        return self.get_minute_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            freq=freq,
        )

    def _get_incremental_daily_basic(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """
        获取每日指标增量数据

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pd.DataFrame: 每日指标数据
        """
        if not start_date or not end_date:
            raise ProviderError(
                "start_date and end_date are required for incremental daily_basic data",
                provider_name=self.name
            )

        return self.get_daily_basic(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def _get_incremental_adj_factor(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """
        获取复权因子增量数据

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pd.DataFrame: 复权因子数据
        """
        if not start_date or not end_date:
            raise ProviderError(
                "start_date and end_date are required for incremental adj_factor data",
                provider_name=self.name
            )

        return self.get_adj_factor(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def _convert_quarter_to_date(self, quarter: str) -> pd.Timestamp:
        """
        将季度格式转换为季度末日期

        Args:
            quarter: 季度字符串，如 "2025Q1", "2024Q4"

        Returns:
            pd.Timestamp: 季度末日期
        """
        # 解析季度格式 (YYYYQX)
        year = int(quarter[:4])
        q = int(quarter[5])

        # 计算季度末月份和日期
        if q == 1:
            month, day = 3, 31  # Q1 -> 3月31日
        elif q == 2:
            month, day = 6, 30  # Q2 -> 6月30日
        elif q == 3:
            month, day = 9, 30  # Q3 -> 9月30日
        else:  # q == 4
            month, day = 12, 31  # Q4 -> 12月31日

        return pd.Timestamp(year, month, day)

    def get_gdp_data(
        self,
        q: Optional[str] = None,
        start_q: Optional[str] = None,
        end_q: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取中国国民经济GDP数据

        Args:
            q: 季度（2019Q1表示2019年第一季度）
            start_q: 开始季度
            end_q: 结束季度

        Returns:
            pd.DataFrame: 标准格式的GDP数据，包含time（季度末日期）和quarter字段
        """
        logger.info(
            f"Fetching GDP data (q={q}, start_q={start_q}, end_q={end_q})"
        )

        # 构建参数
        kwargs = {}
        if q:
            kwargs["q"] = q
        if start_q:
            kwargs["start_q"] = start_q
        if end_q:
            kwargs["end_q"] = end_q

        # 调用Tushare API
        df = self._call_api(
            "cn_gdp",
            fields="quarter,gdp,gdp_yoy,pi,pi_yoy,si,si_yoy,ti,ti_yoy",
            **kwargs
        )

        if df.empty:
            logger.warning("No GDP data returned from Tushare")
            return pd.DataFrame(columns=CNGDPSchema.get_required_columns())

        # 将quarter转换为季度末日期（time字段）
        df["time"] = df["quarter"].apply(self._convert_quarter_to_date)

        # 列名映射
        column_mapping = {
            "quarter": "quarter",
            "gdp": "gdp",
            "gdp_yoy": "gdp_yoy",
            "pi": "pi",
            "pi_yoy": "pi_yoy",
            "si": "si",
            "si_yoy": "si_yoy",
            "ti": "ti",
            "ti_yoy": "ti_yoy",
        }

        df = convert_to_standard_columns(df, column_mapping)

        # 验证数据格式
        df = validate_dataframe(df, CNGDPSchema, provider_name=self.name)

        # 按时间排序
        df = df.sort_values("time").reset_index(drop=True)

        logger.info(f"Fetched {len(df)} GDP records")
        return df

    def _convert_month_to_date(self, month: str) -> pd.Timestamp:
        """
        将YYYYMM格式转换为月份末日期

        Args:
            month: 月份字符串，如 "202401", "202412"

        Returns:
            pd.Timestamp: 月份末日期
        """
        # 解析月份格式 (YYYYMM)
        year = int(month[:4])
        m = int(month[4:6])

        # 计算该月最后一天
        if m == 12:
            return pd.Timestamp(year, 12, 31)
        else:
            return pd.Timestamp(year, m + 1, 1) - pd.Timedelta(days=1)

    def get_ppi_data(
        self,
        m: Optional[str] = None,
        start_m: Optional[str] = None,
        end_m: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取中国PPI工业生产者出厂价格指数数据

        Args:
            m: 月份（YYYYMM格式），支持多个月份逗号分隔
            start_m: 开始月份
            end_m: 结束月份

        Returns:
            pd.DataFrame: 标准格式的PPI数据，包含time（月份末日期）和month字段
        """
        logger.info(
            f"Fetching PPI data (m={m}, start_m={start_m}, end_m={end_m})"
        )

        # 构建参数
        kwargs = {}
        if m:
            kwargs["m"] = m
        if start_m:
            kwargs["start_m"] = start_m
        if end_m:
            kwargs["end_m"] = end_m

        # 调用Tushare API
        df = self._call_api(
            "cn_ppi",
            fields="month,ppi_yoy,ppi_mp_yoy,ppi_mp_qm_yoy,ppi_mp_rm_yoy,ppi_mp_p_yoy,"
                   "ppi_cg_yoy,ppi_cg_f_yoy,ppi_cg_c_yoy,ppi_cg_adu_yoy,ppi_cg_dcg_yoy,"
                   "ppi_mom,ppi_mp_mom,ppi_mp_qm_mom,ppi_mp_rm_mom,ppi_mp_p_mom,"
                   "ppi_cg_mom,ppi_cg_f_mom,ppi_cg_c_mom,ppi_cg_adu_mom,ppi_cg_dcg_mom,"
                   "ppi_accu,ppi_mp_accu,ppi_mp_qm_accu,ppi_mp_rm_accu,ppi_mp_p_accu,"
                   "ppi_cg_accu,ppi_cg_f_accu,ppi_cg_c_accu,ppi_cg_adu_accu,ppi_cg_dcg_accu",
            **kwargs
        )

        if df.empty:
            logger.warning("No PPI data returned from Tushare")
            return pd.DataFrame(columns=CNPPISchema.get_required_columns())

        # 将month转换为月份末日期（time字段）
        df["time"] = df["month"].apply(self._convert_month_to_date)

        # 列名映射
        column_mapping = {
            "month": "month",
            "ppi_yoy": "ppi_yoy",
            "ppi_mp_yoy": "ppi_mp_yoy",
            "ppi_mp_qm_yoy": "ppi_mp_qm_yoy",
            "ppi_mp_rm_yoy": "ppi_mp_rm_yoy",
            "ppi_mp_p_yoy": "ppi_mp_p_yoy",
            "ppi_cg_yoy": "ppi_cg_yoy",
            "ppi_cg_f_yoy": "ppi_cg_f_yoy",
            "ppi_cg_c_yoy": "ppi_cg_c_yoy",
            "ppi_cg_adu_yoy": "ppi_cg_adu_yoy",
            "ppi_cg_dcg_yoy": "ppi_cg_dcg_yoy",
            "ppi_mom": "ppi_mom",
            "ppi_mp_mom": "ppi_mp_mom",
            "ppi_mp_qm_mom": "ppi_mp_qm_mom",
            "ppi_mp_rm_mom": "ppi_mp_rm_mom",
            "ppi_mp_p_mom": "ppi_mp_p_mom",
            "ppi_cg_mom": "ppi_cg_mom",
            "ppi_cg_f_mom": "ppi_cg_f_mom",
            "ppi_cg_c_mom": "ppi_cg_c_mom",
            "ppi_cg_adu_mom": "ppi_cg_adu_mom",
            "ppi_cg_dcg_mom": "ppi_cg_dcg_mom",
            "ppi_accu": "ppi_accu",
            "ppi_mp_accu": "ppi_mp_accu",
            "ppi_mp_qm_accu": "ppi_mp_qm_accu",
            "ppi_mp_rm_accu": "ppi_mp_rm_accu",
            "ppi_mp_p_accu": "ppi_mp_p_accu",
            "ppi_cg_accu": "ppi_cg_accu",
            "ppi_cg_f_accu": "ppi_cg_f_accu",
            "ppi_cg_c_accu": "ppi_cg_c_accu",
            "ppi_cg_adu_accu": "ppi_cg_adu_accu",
            "ppi_cg_dcg_accu": "ppi_cg_dcg_accu",
        }

        df = convert_to_standard_columns(df, column_mapping)

        # 验证数据格式
        df = validate_dataframe(df, CNPPISchema, provider_name=self.name)

        # 按时间排序
        df = df.sort_values("time").reset_index(drop=True)

        logger.info(f"Fetched {len(df)} PPI records")
        return df

    def get_m_data(
        self,
        m: Optional[str] = None,
        start_m: Optional[str] = None,
        end_m: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取中国货币供应量数据（M0、M1、M2）

        Args:
            m: 月份（YYYYMM格式），支持多个月份逗号分隔
            start_m: 开始月份
            end_m: 结束月份

        Returns:
            pd.DataFrame: 标准格式的货币供应量数据，包含time（月份末日期）和month字段
        """
        logger.info(
            f"Fetching M data (m={m}, start_m={start_m}, end_m={end_m})"
        )

        # 构建参数
        kwargs = {}
        if m:
            kwargs["m"] = m
        if start_m:
            kwargs["start_m"] = start_m
        if end_m:
            kwargs["end_m"] = end_m

        # 调用Tushare API
        df = self._call_api(
            "cn_m",
            fields="month,m0,m0_yoy,m0_mom,m1,m1_yoy,m1_mom,m2,m2_yoy,m2_mom",
            **kwargs
        )

        if df.empty:
            logger.warning("No M data returned from Tushare")
            return pd.DataFrame(columns=CNMSchema.get_required_columns())

        # 将month转换为月份末日期（time字段）- 复用PPI的转换方法
        df["time"] = df["month"].apply(self._convert_month_to_date)

        # 列名映射
        column_mapping = {
            "month": "month",
            "m0": "m0",
            "m0_yoy": "m0_yoy",
            "m0_mom": "m0_mom",
            "m1": "m1",
            "m1_yoy": "m1_yoy",
            "m1_mom": "m1_mom",
            "m2": "m2",
            "m2_yoy": "m2_yoy",
            "m2_mom": "m2_mom",
        }

        df = convert_to_standard_columns(df, column_mapping)

        # 验证数据格式
        df = validate_dataframe(df, CNMSchema, provider_name=self.name)

        # 按时间排序
        df = df.sort_values("time").reset_index(drop=True)

        logger.info(f"Fetched {len(df)} M records")
        return df

    def get_pmi_data(
        self,
        m: Optional[str] = None,
        start_m: Optional[str] = None,
        end_m: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取中国PMI采购经理人指数数据

        Args:
            m: 月份（YYYYMM格式），支持多个月份逗号分隔
            start_m: 开始月份
            end_m: 结束月份

        Returns:
            pd.DataFrame: 标准格式的PMI数据，包含time（月份末日期）和month字段
        """
        logger.info(
            f"Fetching PMI data (m={m}, start_m={start_m}, end_m={end_m})"
        )

        # 构建参数
        kwargs = {}
        if m:
            kwargs["m"] = m
        if start_m:
            kwargs["start_m"] = start_m
        if end_m:
            kwargs["end_m"] = end_m

        # 调用Tushare API
        df = self._call_api(
            "cn_pmi",
            fields="month,pmi010000,pmi010100,pmi010200,pmi010300,pmi010400,pmi010500,"
                   "pmi010600,pmi010700,pmi010800,pmi010900,pmi011000,pmi011100,"
                   "pmi011200,pmi011300,pmi011400,pmi011500,pmi011600,pmi011700,"
                   "pmi011800,pmi011900,pmi012000,pmi020100,pmi020200,pmi020300,"
                   "pmi020400,pmi020500,pmi020600,pmi020700,pmi020800,pmi020900,"
                   "pmi021000,pmi030000",
            **kwargs
        )

        if df.empty:
            logger.warning("No PMI data returned from Tushare")
            return pd.DataFrame(columns=CNPMISchema.get_required_columns())

        # 将month转换为月份末日期（time字段）- 复用PPI的转换方法
        df["time"] = df["month"].apply(self._convert_month_to_date)

        # 列名映射（全部字段）
        column_mapping = {
            "month": "month",
            "pmi010000": "pmi010000",
            "pmi010100": "pmi010100",
            "pmi010200": "pmi010200",
            "pmi010300": "pmi010300",
            "pmi010400": "pmi010400",
            "pmi010500": "pmi010500",
            "pmi010600": "pmi010600",
            "pmi010700": "pmi010700",
            "pmi010800": "pmi010800",
            "pmi010900": "pmi010900",
            "pmi011000": "pmi011000",
            "pmi011100": "pmi011100",
            "pmi011200": "pmi011200",
            "pmi011300": "pmi011300",
            "pmi011400": "pmi011400",
            "pmi011500": "pmi011500",
            "pmi011600": "pmi011600",
            "pmi011700": "pmi011700",
            "pmi011800": "pmi011800",
            "pmi011900": "pmi011900",
            "pmi012000": "pmi012000",
            "pmi020100": "pmi020100",
            "pmi020200": "pmi020200",
            "pmi020300": "pmi020300",
            "pmi020400": "pmi020400",
            "pmi020500": "pmi020500",
            "pmi020600": "pmi020600",
            "pmi020700": "pmi020700",
            "pmi020800": "pmi020800",
            "pmi020900": "pmi020900",
            "pmi021000": "pmi021000",
            "pmi030000": "pmi030000",
        }

        df = convert_to_standard_columns(df, column_mapping)

        # 验证数据格式
        df = validate_dataframe(df, CNPMISchema, provider_name=self.name)

        # 按时间排序
        df = df.sort_values("time").reset_index(drop=True)

        logger.info(f"Fetched {len(df)} PMI records")
        return df

    # 支持的指数代码列表
    SUPPORTED_INDEX_CODES = [
        "000001.SH",  # 上证综指
        "000016.SH",  # 上证50
        "000300.SH",  # 沪深300
        "000905.SH",  # 中证500
        "399001.SZ",  # 深证成指
        "399005.SZ",  # 中小板指
        "399006.SZ",  # 创业板指
        "399300.SZ",  # 沪深300
        "399905.SZ",  # 中证500
    ]

    def get_index_dailybasic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取大盘指数每日指标数据（自动处理Tushare 3000条记录限制）

        当返回记录数等于3000时，自动继续获取更早的数据，确保获取完整历史数据。

        目前只提供上证综指，深证成指，上证50，中证500，中小板指，创业板指的每日指标数据。

        Args:
            ts_code: 指数代码（如000001.SH上证综指），支持多值，None表示所有支持的指数
            trade_date: 交易日期（YYYY-MM-DD 或 YYYYMMDD格式），与start_date/end_date互斥
            start_date: 开始日期（YYYY-MM-DD 或 YYYYMMDD格式）
            end_date: 结束日期（YYYY-MM-DD 或 YYYYMMDD格式）

        Returns:
            pd.DataFrame: 标准格式的指数每日指标数据
        """
        logger.info(
            f"Fetching index dailybasic data (ts_code={ts_code}, trade_date={trade_date}, "
            f"start_date={start_date}, end_date={end_date})"
        )

        # 处理 trade_date 批量模式
        if trade_date:
            logger.info(f"Using trade_date batch mode for {trade_date}")
            trade_date_clean = trade_date.replace("-", "")

            df = self._call_api(
                "index_dailybasic",
                fields="ts_code,trade_date,total_mv,float_mv,total_share,"
                       "float_share,free_share,turnover_rate,turnover_rate_f,"
                       "pe,pe_ttm,pb",
                trade_date=trade_date_clean,
            )

            if df.empty:
                logger.warning(f"No data returned for trade_date={trade_date}")
                return pd.DataFrame(columns=IndexDailybasicSchema.get_required_columns())

            # 列名映射
            column_mapping = {
                "ts_code": "ts_code",
                "trade_date": "trade_date",
                "total_mv": "total_mv",
                "float_mv": "float_mv",
                "total_share": "total_share",
                "float_share": "float_share",
                "free_share": "free_share",
                "turnover_rate": "turnover_rate",
                "turnover_rate_f": "turnover_rate_f",
                "pe": "pe",
                "pe_ttm": "pe_ttm",
                "pb": "pb",
            }

            df = convert_to_standard_columns(df, column_mapping)

            # 转换时间格式
            df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

            # 验证数据格式
            df = validate_dataframe(df, IndexDailybasicSchema, provider_name=self.name)

            # 按时间和代码排序
            df = df.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

            logger.info(f"Fetched {len(df)} index dailybasic records for trade_date={trade_date}")
            return df

        # 日期范围模式
        # 转换日期格式
        if start_date:
            start_date = start_date.replace("-", "")
        if end_date:
            end_date = end_date.replace("-", "")

        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 3000

        # 记录所有批次的数据
        all_dataframes = []
        total_records = 0

        # 确定要查询的指数代码列表
        if ts_code:
            index_codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        else:
            # 没有指定ts_code时，使用所有支持的指数
            index_codes = self.SUPPORTED_INDEX_CODES
            logger.info(f"No ts_code specified, using supported indexes: {index_codes}")

        # 对每个指数代码查询数据
        for code in index_codes:
            logger.info(f"Fetching index_dailybasic for {code}")
            code_dataframes = []
            current_end_date = end_date
            batch_count = 0

            while True:
                batch_count += 1
                # 构建API参数
                api_params = {
                    "ts_code": code,
                    "fields": "ts_code,trade_date,total_mv,float_mv,total_share,"
                              "float_share,free_share,turnover_rate,turnover_rate_f,"
                              "pe,pe_ttm,pb",
                }
                if start_date:
                    api_params["start_date"] = start_date
                if current_end_date:
                    api_params["end_date"] = current_end_date.replace("-", "") if isinstance(current_end_date, str) else current_end_date

                df = self._call_api("index_dailybasic", **api_params)

                if df.empty:
                    logger.info(f"  Batch {batch_count}: No data returned, stopping")
                    break

                # 列名映射
                column_mapping = {
                    "ts_code": "ts_code",
                    "trade_date": "trade_date",
                    "total_mv": "total_mv",
                    "float_mv": "float_mv",
                    "total_share": "total_share",
                    "float_share": "float_share",
                    "free_share": "free_share",
                    "turnover_rate": "turnover_rate",
                    "turnover_rate_f": "turnover_rate_f",
                    "pe": "pe",
                    "pe_ttm": "pe_ttm",
                    "pb": "pb",
                }

                df = convert_to_standard_columns(df, column_mapping)

                # 转换时间格式
                df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

                # 按时间排序
                df = df.sort_values("trade_date").reset_index(drop=True)

                batch_records = len(df)
                code_dataframes.append(df)

                logger.info(f"  Batch {batch_count}: Fetched {batch_records} records")

                # 检查是否需要继续获取更早的数据
                if batch_records == TUSHARE_MAX_RECORDS and start_date is None:
                    # 获取当前批次中最早的日期
                    earliest_date = df["trade_date"].min().date()
                    # 计算新的结束日期（向前推1天）
                    from datetime import timedelta
                    new_end_date = earliest_date - timedelta(days=1)
                    current_end_date = new_end_date.strftime("%Y-%m-%d")
                    logger.info(f"  Batch {batch_count}: Max limit reached, fetching earlier data up to {current_end_date}")
                    continue
                else:
                    break

            # 合并该指数的数据
            if code_dataframes:
                code_df = pd.concat(code_df for code_df in code_dataframes)
                all_dataframes.append(code_df)
                total_records += len(code_df)
                logger.info(f"  {code}: Total {len(code_df)} records")

        # 合并所有指数的数据
        if not all_dataframes:
            logger.warning("No index dailybasic data returned from Tushare")
            return pd.DataFrame(columns=IndexDailybasicSchema.get_required_columns())

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)

        # 去重（如果批次间有重叠）
        final_df = final_df.drop_duplicates(subset=["trade_date", "ts_code"]).sort_values(
            ["trade_date", "ts_code"]
        ).reset_index(drop=True)

        # 验证数据格式
        final_df = validate_dataframe(final_df, IndexDailybasicSchema, provider_name=self.name)

        logger.info(f"Total fetched {len(final_df)} index dailybasic records")
        return final_df

    def get_fina_indicator(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取上市公司财务指标数据（自动处理Tushare 100条记录限制）

        当返回记录数等于100时，自动继续获取更早的数据，确保获取完整历史数据。

        Args:
            ts_code: TS股票代码（如600001.SH/000001.SZ），支持多值（逗号分隔）
            start_date: 报告期开始日期（YYYYMMDD格式）
            end_date: 报告期结束日期（YYYYMMDD格式）

        Returns:
            pd.DataFrame: 标准格式的财务指标数据
        """
        logger.info(
            f"Fetching fina_indicator data (ts_code={ts_code}, start_date={start_date}, end_date={end_date})"
        )

        # 转换日期格式
        if start_date:
            start_date = start_date.replace("-", "")
        if end_date:
            end_date = end_date.replace("-", "")

        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 100

        # 解析 ts_code 列表
        ts_code_list = [c.strip() for c in ts_code.split(",") if c.strip()]

        # 记录所有股票的数据
        all_dataframes = []
        total_records = 0

        for code in ts_code_list:
            logger.info(f"Fetching fina_indicator for {code}")
            code_dataframes = []
            current_end_date = end_date
            batch_count = 0

            while True:
                batch_count += 1
                # 构建API参数
                api_params = {
                    "ts_code": code,
                    "fields": "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,"
                              "capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,"
                              "gross_margin,current_ratio,quick_ratio,cash_ratio,ar_turn,ca_turn,"
                              "fa_turn,assets_turn,op_income,ebit,ebitda,fcff,fcfe,current_exint,"
                              "noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,"
                              "networking_capital,invest_capital,retained_earnings,diluted2_eps,"
                              "bps,ocfps,cfps,ebit_ps,netprofit_margin,grossprofit_margin,profit_to_gr,"
                              "roe,roe_waa,roe_dt,roa,roic,debt_to_assets,assets_to_eqt,ca_to_assets,"
                              "nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,"
                              "currentdebt_to_debt,longdeb_to_debt,debt_to_eqt,eqt_to_debt,"
                              "eqt_to_interestdebt,tangibleasset_to_debt,ocf_to_debt,turn_days,"
                              "fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,q_opincome,"
                              "q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_profit_to_gr,"
                              "q_salescash_to_or,q_ocf_to_sales,basic_eps_yoy,dt_eps_yoy,cfps_yoy,"
                              "op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,"
                              "assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_sales_yoy,q_op_yoy,q_op_qoq,"
                              "q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy",
                }
                if start_date:
                    api_params["start_date"] = start_date
                if current_end_date:
                    api_params["end_date"] = current_end_date.replace("-", "") if isinstance(current_end_date, str) else current_end_date

                df = self._call_api("fina_indicator", **api_params)

                if df.empty:
                    logger.info(f"  Batch {batch_count}: No data returned, stopping")
                    break

                # 记录原始数据条数
                batch_records = len(df)
                logger.info(f"  Batch {batch_count}: Fetched {batch_records} records")

                # 将 ann_date 和 end_date 转换为标准格式
                # end_date 转换为时间序列格式 end_date_time
                df["end_date_time"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

                # 列名映射（财务指标字段保持原名，只需处理时间字段）
                column_mapping = {
                    "ts_code": "ts_code",
                    "ann_date": "ann_date",
                    "end_date": "end_date",
                    "end_date_time": "end_date_time",
                }

                df = convert_to_standard_columns(df, column_mapping)

                # 保留所有原始财务指标字段
                for col in df.columns:
                    if col not in ["ts_code", "ann_date", "end_date", "end_date_time"]:
                        if col not in column_mapping:
                            pass  # 保持原列名

                code_dataframes.append(df)

                # 检查是否需要继续获取更早的数据
                if batch_records == TUSHARE_MAX_RECORDS and start_date is None:
                    # 获取当前批次中最早的报告期
                    earliest_date = df["end_date"].min()
                    logger.info(f"  Batch {batch_count}: Max limit reached, fetching earlier data before {earliest_date}")
                    current_end_date = earliest_date
                    continue
                else:
                    break

            # 合并该股票的数据
            if code_dataframes:
                # 过滤掉空DataFrame和全NA的DataFrame，并删除每个DataFrame中的全NA列
                valid_dataframes = []
                for df in code_dataframes:
                    if df.empty:
                        continue
                    # 检查是否有至少一个非NA值
                    if df.dropna(how='all').empty:
                        continue
                    # 删除全NA的列
                    df_clean = df.dropna(axis=1, how='all')
                    if not df_clean.empty:
                        valid_dataframes.append(df_clean)

                if valid_dataframes:
                    code_df = pd.concat(valid_dataframes, ignore_index=True, sort=False)
                    all_dataframes.append(code_df)
                    total_records += len(code_df)
                    logger.info(f"  {code}: Total {len(code_df)} records")

        # 合并所有股票的数据
        if not all_dataframes:
            logger.warning("No fina_indicator data returned from Tushare")
            return pd.DataFrame(columns=FinaIndicatorSchema.get_required_columns())

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        # 清理：删除全NA的列
        final_df = final_df.dropna(axis=1, how='all')

        # 去重（按ts_code和end_date_time）
        final_df = final_df.drop_duplicates(subset=["ts_code", "end_date"]).sort_values(
            ["ts_code", "end_date"]
        ).reset_index(drop=True)

        # 验证数据格式
        final_df = validate_dataframe(final_df, FinaIndicatorSchema, provider_name=self.name)

        logger.info(f"Total fetched {len(final_df)} fina_indicator records")
        return final_df

    def get_cashflow(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取上市公司现金流量表数据（自动处理Tushare 100条记录限制）

        当返回记录数等于100时，自动继续获取更早的数据，确保获取完整历史数据。

        Args:
            ts_code: TS股票代码（如600001.SH/000001.SZ），支持多值（逗号分隔）
            start_date: 报告期开始日期（YYYYMMDD格式）
            end_date: 报告期结束日期（YYYYMMDD格式）

        Returns:
            pd.DataFrame: 标准格式的现金流量表数据
        """
        logger.info(
            f"Fetching cashflow data (ts_code={ts_code}, start_date={start_date}, end_date={end_date})"
        )

        # 转换日期格式
        if start_date:
            start_date = start_date.replace("-", "")
        if end_date:
            end_date = end_date.replace("-", "")

        # Tushare API 记录限制常量
        TUSHARE_MAX_RECORDS = 100

        # 解析 ts_code 列表
        ts_code_list = [c.strip() for c in ts_code.split(",") if c.strip()]

        # 记录所有股票的数据
        all_dataframes = []
        total_records = 0

        for code in ts_code_list:
            logger.info(f"Fetching cashflow for {code}")
            code_dataframes = []
            current_end_date = end_date
            batch_count = 0

            while True:
                batch_count += 1
                # 构建API参数
                api_params = {
                    "ts_code": code,
                    "fields": "ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,end_type,"
                              "net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,"
                              "n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,"
                              "n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,"
                              "n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,"
                              "c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,"
                              "n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,"
                              "pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,"
                              "st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,"
                              "c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,"
                              "n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,"
                              "c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,"
                              "n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,"
                              "c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,"
                              "stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,"
                              "c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,"
                              "oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,"
                              "eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,"
                              "c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,"
                              "uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,"
                              "amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,"
                              "incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,"
                              "decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,"
                              "decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,"
                              "conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,"
                              "im_n_incr_cash_equ,net_dism_capital_add,net_cash_rece_sec,"
                              "credit_impa_loss,use_right_asset_dep,oth_loss_asset,"
                              "end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,update_flag",
                }
                if start_date:
                    api_params["start_date"] = start_date
                if current_end_date:
                    api_params["end_date"] = current_end_date.replace("-", "") if isinstance(current_end_date, str) else current_end_date

                df = self._call_api("cashflow", **api_params)

                if df.empty:
                    logger.info(f"  Batch {batch_count}: No data returned, stopping")
                    break

                # 记录原始数据条数
                batch_records = len(df)
                logger.info(f"  Batch {batch_count}: Fetched {batch_records} records")

                # 将 end_date 转换为时间序列格式 end_date_time
                df["end_date_time"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

                # 列名映射（现金流量表字段保持原名，只需处理时间字段）
                column_mapping = {
                    "ts_code": "ts_code",
                    "ann_date": "ann_date",
                    "f_ann_date": "f_ann_date",
                    "end_date": "end_date",
                    "end_date_time": "end_date_time",
                }

                df = convert_to_standard_columns(df, column_mapping)

                code_dataframes.append(df)

                # 检查是否需要继续获取更早的数据
                if batch_records == TUSHARE_MAX_RECORDS and start_date is None:
                    # 获取当前批次中最早的报告期
                    earliest_date = df["end_date"].min()
                    logger.info(f"  Batch {batch_count}: Max limit reached, fetching earlier data before {earliest_date}")
                    current_end_date = earliest_date
                    continue
                else:
                    break

            # 合并该股票的数据
            if code_dataframes:
                # 过滤掉空DataFrame和全NA的DataFrame，并删除每个DataFrame中的全NA列
                valid_dataframes = []
                for df in code_dataframes:
                    if df.empty:
                        continue
                    # 检查是否有至少一个非NA值
                    if df.dropna(how='all').empty:
                        continue
                    # 删除全NA的列
                    df_clean = df.dropna(axis=1, how='all')
                    if not df_clean.empty:
                        valid_dataframes.append(df_clean)

                if valid_dataframes:
                    code_df = pd.concat(valid_dataframes, ignore_index=True, sort=False)
                    all_dataframes.append(code_df)
                    total_records += len(code_df)
                    logger.info(f"  {code}: Total {len(code_df)} records")

        # 合并所有股票的数据
        if not all_dataframes:
            logger.warning("No cashflow data returned from Tushare")
            return pd.DataFrame(columns=CashflowSchema.get_required_columns())

        # 合并DataFrame
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        # 清理：删除全NA的列
        final_df = final_df.dropna(axis=1, how='all')

        # 去重（按ts_code和end_date_time）
        final_df = final_df.drop_duplicates(subset=["ts_code", "end_date"]).sort_values(
            ["ts_code", "end_date"]
        ).reset_index(drop=True)

        # 验证数据格式
        final_df = validate_dataframe(final_df, CashflowSchema, provider_name=self.name)

        logger.info(f"Total fetched {len(final_df)} cashflow records")
        return final_df
