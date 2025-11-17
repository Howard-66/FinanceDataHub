"""
Tushare数据提供者

提供从Tushare Pro API获取金融数据的功能。
"""

import time
from typing import Optional, Dict, Any
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
        symbol: str,
        start_date: str,
        end_date: str,
        adj: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取日线行情数据

        Args:
            symbol: 股票代码（例如 "600519.SH"）
            start_date: 开始日期（YYYY-MM-DD 或 YYYYMMDD）
            end_date: 结束日期（YYYY-MM-DD 或 YYYYMMDD）
            adj: 复权类型（None=不复权, "qfq"=前复权, "hfq"=后复权）

        Returns:
            pd.DataFrame: 标准格式的日线数据
        """
        logger.info(
            f"Fetching daily data for {symbol} from {start_date} to {end_date} (adj={adj})"
        )

        # 转换日期格式（去掉横杠）
        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "")

        # 根据复权类型选择API
        if adj == "qfq":
            api_name = "pro_bar"
            kwargs = {
                "ts_code": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "adj": "qfq",
                "freq": "D",
            }
        elif adj == "hfq":
            api_name = "pro_bar"
            kwargs = {
                "ts_code": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "adj": "hfq",
                "freq": "D",
            }
        else:
            # 对于不复权数据，使用daily API，它返回标准的DataFrame
            api_name = "daily"
            kwargs = {
                "ts_code": symbol,
                "start_date": start_date,
                "end_date": end_date,
            }

        df = self._call_api(api_name, **kwargs)

        # 调试信息
        logger.debug(f"API {api_name} returned type: {type(df)}")
        if isinstance(df, pd.DataFrame):
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"DataFrame columns: {df.columns.tolist()}")
        else:
            logger.error(f"Unexpected return type: {type(df)}, value: {df}")
            return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

        if df.empty:
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

        # 获取复权因子（如果需要）
        if adj:
            adj_factors = self._get_adj_factor(symbol, start_date, end_date)
            if not adj_factors.empty:
                df = df.merge(
                    adj_factors[["time", "adj_factor"]],
                    left_on="time",
                    right_on="time",
                    how="left",
                )
                df.drop(columns=["time_y"], inplace=True, errors="ignore")
                df.rename(columns={"time_x": "time"}, inplace=True)

        # 验证数据格式
        df = validate_dataframe(df, DailyDataSchema, provider_name=self.name)

        # 按时间排序
        df = df.sort_values("time").reset_index(drop=True)

        logger.info(f"Fetched {len(df)} daily records for {symbol}")
        return df

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
        start_date: str,
        end_date: str,
        freq: str = "1m",
    ) -> pd.DataFrame:
        """
        获取分钟级行情数据

        注意：Tushare Pro的分钟数据需要较高权限，免费用户可能无法访问。

        Args:
            symbol: 股票代码
            start_date: 开始日期时间
            end_date: 结束日期时间
            freq: 频率（1min, 5min, 15min, 30min, 60min）

        Returns:
            pd.DataFrame: 标准格式的分钟数据
        """
        logger.info(
            f"Fetching {freq} data for {symbol} from {start_date} to {end_date}"
        )

        # Tushare的频率映射
        freq_mapping = {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "60m": "60min",
        }
        tushare_freq = freq_mapping.get(freq, "1min")

        # 转换日期格式
        start_date = start_date.replace("-", "").replace(" ", "").replace(":", "")
        end_date = end_date.replace("-", "").replace(" ", "").replace(":", "")

        try:
            df = self._call_api(
                "pro_bar",
                ts_code=symbol,
                start_date=start_date[:8],  # 只取日期部分
                end_date=end_date[:8],
                freq=tushare_freq,
            )
        except ProviderAuthError:
            logger.warning(
                "Insufficient permissions for minute data. "
                "Upgrade your Tushare account for access."
            )
            return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

        if df.empty:
            return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

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

        logger.info(f"Fetched {len(df)} minute records for {symbol}")
        return df

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

        # 转换日期格式
        if trade_date:
            trade_date = trade_date.replace("-", "")
        if start_date:
            start_date = start_date.replace("-", "")
        if end_date:
            end_date = end_date.replace("-", "")

        # 构建参数
        kwargs = {}
        if symbol:
            kwargs["ts_code"] = symbol
        if trade_date:
            kwargs["trade_date"] = trade_date
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        df = self._call_api("daily_basic", **kwargs)

        if df.empty:
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

        logger.info(f"Fetched {len(df)} daily basic records")
        return df

    def get_adj_factor(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取复权因子

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

        df = self._call_api("adj_factor", **kwargs)

        if df.empty:
            logger.warning(f"Empty adj_factor result for {symbol}")
            return pd.DataFrame(
                columns=["symbol", "time", "adj_factor"]
            )

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

        logger.info(f"Fetched {len(df)} adj_factor records")
        return df
