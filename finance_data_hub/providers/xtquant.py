"""
XTQuant数据提供者

通过HTTP API客户端连接xtquant_helper微服务获取金融数据。
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
import pandas as pd
import httpx
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
    standardize_symbol,
)


@register_provider("xtquant")
class XTQuantProvider(BaseDataProvider):
    """
    XTQuant数据提供者（HTTP API客户端模式）

    通过HTTP API连接xtquant_helper微服务获取中国A股市场数据。

    配置参数:
        api_url (str): xtquant_helper微服务地址，默认 http://localhost:8100
        timeout (int): 请求超时时间（秒），默认60
        max_retry (int): 最大重试次数，默认3
    """

    def __init__(
        self, name: str = "xtquant", config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, config)
        self.api_url: str = (
            config.get("api_url", "http://localhost:8100") if config else "http://localhost:8100"
        )
        self.timeout: int = config.get("timeout", 60) if config else 60
        self.max_retry: int = config.get("max_retry", 3) if config else 3

        # HTTP client
        self.client: Optional[httpx.Client] = None

    def initialize(self) -> None:
        """
        初始化XTQuant Provider

        创建HTTP客户端并验证微服务连接。

        Raises:
            ProviderConnectionError: 无法连接到微服务
            ProviderError: 初始化失败
        """
        if self._is_initialized:
            logger.debug("XTQuantProvider already initialized")
            return

        try:
            # 创建HTTP客户端
            self.client = httpx.Client(
                base_url=self.api_url,
                timeout=self.timeout,
                follow_redirects=True,
            )

            # 健康检查
            response = self.client.get("/")
            if response.status_code != 200:
                raise ProviderConnectionError(
                    f"xtquant_helper health check failed: {response.status_code}",
                    provider_name=self.name,
                )

            data = response.json()
            if data.get("status") != "ok":
                raise ProviderConnectionError(
                    "xtquant_helper is not running properly",
                    provider_name=self.name,
                )

            logger.info(
                f"XTQuantProvider initialized successfully (api_url={self.api_url})"
            )
            self._is_initialized = True

        except httpx.ConnectError as e:
            raise ProviderConnectionError(
                f"Failed to connect to xtquant_helper at {self.api_url}: {str(e)}",
                provider_name=self.name,
            ) from e
        except Exception as e:
            raise ProviderError(
                f"Failed to initialize XTQuantProvider: {str(e)}",
                provider_name=self.name,
            ) from e

    def health_check(self) -> bool:
        """
        健康检查

        验证xtquant_helper微服务是否可用。

        Returns:
            bool: 微服务是否可用
        """
        if not self._is_initialized or not self.client:
            return False

        try:
            response = self.client.get("/", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"XTQuant health check failed: {str(e)}")
            return False

    def _call_api(
        self, endpoint: str, payload: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        调用xtquant_helper API的通用方法

        Args:
            endpoint: API端点（例如 "/get_market_data"）
            payload: 请求参数

        Returns:
            API返回的数据

        Raises:
            ProviderError: API调用失败
        """
        if not self._is_initialized or not self.client:
            raise ProviderError(
                "XTQuantProvider not initialized", provider_name=self.name
            )

        def _call():
            try:
                response = self.client.post(endpoint, json=payload or {})

                # 检查HTTP状态码
                if response.status_code >= 500:
                    raise ProviderConnectionError(
                        f"xtquant_helper server error: {response.status_code}",
                        provider_name=self.name,
                    )
                elif response.status_code >= 400:
                    raise ProviderDataError(
                        f"xtquant_helper request error: {response.status_code}",
                        provider_name=self.name,
                    )

                # 解析响应
                data = response.json()

                # 检查是否有错误
                if isinstance(data, dict) and "error" in data:
                    error_msg = data["error"]
                    if "xtquant.xtdata not available" in error_msg:
                        raise ProviderConnectionError(
                            "xtquant is not available or not connected",
                            provider_name=self.name,
                        )
                    else:
                        raise ProviderDataError(
                            f"xtquant API error: {error_msg}",
                            provider_name=self.name,
                        )

                return data

            except httpx.ConnectError as e:
                raise ProviderConnectionError(
                    f"Failed to connect to xtquant_helper: {str(e)}",
                    provider_name=self.name,
                )
            except httpx.TimeoutException:
                raise ProviderConnectionError(
                    f"Request to xtquant_helper timed out (timeout={self.timeout}s)",
                    provider_name=self.name,
                )
            except (ProviderConnectionError, ProviderDataError):
                raise
            except Exception as e:
                raise ProviderError(
                    f"Unexpected error calling xtquant API: {str(e)}",
                    provider_name=self.name,
                ) from e

        # 使用重试机制
        return self.retry_on_failure(_call, max_retries=self.max_retry)

    def _convert_dict_to_dataframe(
        self, data_dict: Dict[str, Any], symbol: str
    ) -> pd.DataFrame:
        """
        将XTQuant API返回的字典转换为DataFrame

        XTQuant返回的数据格式：
        {'symbol': {'time': {'key': timestamp, ...}, 'open': {...}, 'close': {...}, ...}}

        Args:
            data_dict: API返回的数据字典
            symbol: 股票代码

        Returns:
            pd.DataFrame: 转换后的DataFrame
        """
        if not data_dict:
            return pd.DataFrame()

        logger.debug(f"Converting data dict, keys: {list(data_dict.keys())}")

        # XTQuant返回的数据格式是嵌套的：{'symbol': {field: {key: value}}}
        # 如果data_dict直接包含symbol数据，提取它
        if symbol in data_dict:
            data_dict = data_dict[symbol]
            logger.debug(f"Extracted data for symbol {symbol}")
        elif len(data_dict) == 1 and isinstance(list(data_dict.values())[0], dict):
            # 如果只有一个symbol的数据，提取它
            data_dict = list(data_dict.values())[0]
            logger.debug(f"Extracted single symbol data")

        logger.debug(f"Processing fields: {list(data_dict.keys())}")

        # 直接使用pandas的DataFrame构造器，它会自动处理字典格式的嵌套结构
        # 这会创建：{field: [values_by_key_order]}
        df = pd.DataFrame(data_dict)

        if df.empty:
            logger.warning("Empty DataFrame after conversion")
            return df

        # 转换时间戳（毫秒 -> datetime）
        if "time" in df.columns:
            time_values = df["time"]
            sample_ts = time_values.iloc[0]
            logger.debug(f"Converting time column, sample: {repr(sample_ts)}, type: {type(sample_ts).__name__}")

            try:
                # xtquant返回的是UTC毫秒时间戳，需要转换为中国时区
                # 首先转换为UTC时间的Timestamp
                df["time"] = pd.to_datetime(time_values, unit="ms", utc=True)
                # 然后转换为中国时区
                df["time"] = df["time"].dt.tz_convert('Asia/Shanghai')
                logger.debug(f"时间戳转换成功，时间范围: {df['time'].min()} 到 {df['time'].max()}")
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to convert timestamps: {e}")
                logger.error(f"  时间戳样例: {sample_ts}")
                # 保留原始时间值，不转换
                df["time"] = time_values

        # 添加symbol列
        df["symbol"] = symbol

        logger.debug(f"DataFrame created with shape: {df.shape}, columns: {list(df.columns)}")

        return df

    def get_stock_basic(
        self,
        market: Optional[str] = None,
        list_status: Optional[str] = "L",
    ) -> pd.DataFrame:
        """
        获取股票基本信息

        注意：XTQuant没有直接获取股票列表的接口，需要通过板块接口间接获取。

        Args:
            market: 市场代码（SH/SZ），None表示全部
            list_status: 上市状态（暂不支持）

        Returns:
            pd.DataFrame: 标准格式的股票基本信息
        """
        logger.info(f"Fetching stock basic info from XTQuant (market={market})")

        # XTQuant没有直接的股票列表接口，暂返回空
        # 实际使用中可能需要维护一个股票列表文件或者从其他接口获取
        logger.warning(
            "XTQuant does not provide direct stock list API. "
            "Consider using Tushare or maintaining a stock list file."
        )

        return pd.DataFrame(columns=StockBasicSchema.get_required_columns())

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

        # 转换symbol格式（Tushare格式 -> XTQuant格式: 600519.SH -> SH.600519）
        # if "." in symbol:
        #     code, exchange = symbol.split(".")
        #     xtquant_symbol = f"{exchange}.{code}"
        # else:
        #     xtquant_symbol = symbol

        # 转换日期格式
        # None表示获取全量历史数据，使用默认起始日期
        if start_date is None:
            start_date = "20000101"  # 默认从2000年开始
        else:
            start_date = start_date.replace("-", "")

        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        else:
            end_date = end_date.replace("-", "")

        # 确定复权类型
        dividend_type = "none"
        if adj == "qfq":
            dividend_type = "front"
        elif adj == "hfq":
            dividend_type = "back"
        
        # XTQuant需要两步：
        # 1. 先下载数据到本地（使用 download_history_data）
        # 2. 然后从本地获取（使用 get_local_data）
        download_payload = {
            "stock_code": symbol,
            "period": "1d",
            "start_time": start_date,
            "end_time": end_date,
            "incrementally": None,  # 增量下载
        }

        # 第一步：下载数据到本地
        logger.debug(f"Downloading daily data for {symbol} from {start_date} to {end_date}")
        self._call_api("/download_history_data", download_payload)

        # 第二步：从本地获取数据
        payload = {
            "field_list": [],  # 空列表表示返回所有字段
            "stock_list": [symbol],
            "period": "1d",
            "start_time": start_date,
            "end_time": end_date,
            "dividend_type": dividend_type,
            "fill_data": True,
            "use_client_data": False,
        }

        data = self._call_api("/get_local_data", payload)
        print('data from call_api:\n', data)

        # 解析返回数据
        if not data:
            print("Invalid daily data from xtquant")
            return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

        # 转换为DataFrame
        df = self._convert_dict_to_dataframe(data, symbol)
        print(df.tail(5))

        if df.empty:
            return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

        # 列名映射（XTQuant字段名 -> 标准字段名）
        # XTQuant的字段名通常是小写的
        df.columns = [col.lower() for col in df.columns]

        # 计算缺失的字段
        # xtquant没有直接提供change_pct和change_amount，但提供了preClose
        if "close" in df.columns and "preclose" in df.columns:
            # 计算涨跌额和涨跌幅
            df["change_amount"] = df["close"] - df["preclose"]
            df["change_pct"] = (df["close"] - df["preclose"]) / df["preclose"] * 100
            logger.debug(f"Calculated change_pct and change_amount for {len(df)} records")
        else:
            logger.warning(f"Missing preclose field, cannot calculate change_pct and change_amount")
            # 设为None
            df["change_amount"] = None
            df["change_pct"] = None

        # 验证并转换数据
        df = validate_dataframe(df, DailyDataSchema, provider_name=self.name)

        # 按时间排序
        df = df.sort_values("time").reset_index(drop=True)

        logger.info(f"Fetched {len(df)} daily records for {symbol}")
        return df

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
            symbol: 股票代码
            start_date: 开始日期时间
            end_date: 结束日期时间
            freq: 频率（1m, 5m, 15m, 30m, 60m）

        Returns:
            pd.DataFrame: 标准格式的分钟数据
        """
        logger.info(
            f"Fetching {freq} data for {symbol} from {start_date} to {end_date}"
        )

        # 转换symbol格式
        # if "." in symbol:
        #     code, exchange = symbol.split(".")
        #     xtquant_symbol = f"{exchange}.{code}"
        # else:
        #     xtquant_symbol = symbol

        # 转换日期格式
        # None表示获取全量历史数据，使用默认起始日期
        if start_date is None:
            start_date = ""  # 默认从2000年开始
        else:
            start_date = start_date.replace("-", "").replace(" ", "").replace(":", "")

        if end_date is None:
            end_date = ""
        else:
            end_date = end_date.replace("-", "").replace(" ", "").replace(":", "")

        # 转换频率格式
        freq_mapping = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "60m": "1h",
        }
        xtquant_freq = freq_mapping.get(freq, "1m")
        logger.info(f"Frequency mapping: {freq} -> {xtquant_freq}")

        # XTQuant需要两步：
        # 1. 先下载数据到本地（使用 download_history_data）
        # 2. 然后从本地获取（使用 get_local_data）
        download_payload = {
            "stock_code": symbol,
            "period": xtquant_freq,
            "start_time": start_date[:8],  # 只取日期部分
            "end_time": end_date[:8],
            "incrementally": None,  # 增量下载
        }

        # 第一步：下载数据到本地
        logger.debug(f"Downloading {freq} data for {symbol} from {start_date[:8]} to {end_date[:8]}")
        logger.debug(f"Download payload: period={xtquant_freq}, start={start_date[:8]}, end={end_date[:8]}")
        self._call_api("/download_history_data", download_payload)

        # 第二步：从本地获取数据
        payload = {
            "field_list": [],
            "stock_list": [symbol],
            "period": xtquant_freq,
            "start_time": start_date[:8],  # 只取日期部分
            "end_time": end_date[:8],
            "dividend_type": "front",
            "fill_data": True,
            "use_client_data": False,
        }

        logger.debug(f"Getting local data with payload: period={xtquant_freq}, start={start_date[:8]}, end={end_date[:8]}")
        data = self._call_api("/get_local_data", payload)
        # print('data from call_api:\n', data)

        if not data:
            print('Invalid data...')
            return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

        # 转换为DataFrame
        df = self._convert_dict_to_dataframe(data, symbol)
        print(df.tail(5))

        if df.empty:
            return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

        # 列名标准化
        df.columns = [col.lower() for col in df.columns]

        # 验证数据
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

        注意：XTQuant的基础数据字段较少，可能不包含所有指标。

        Args:
            symbol: 股票代码，None表示全部
            trade_date: 交易日期，与start_date/end_date互斥
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            pd.DataFrame: 标准格式的每日指标数据
        """
        logger.info(
            f"Fetching daily basic for symbol={symbol}, "
            f"trade_date={trade_date}, start_date={start_date}, end_date={end_date}"
        )

        # XTQuant的市场数据中不包含完整的每日指标
        # 可以通过财务数据接口获取部分指标，但不完全匹配
        logger.warning(
            "XTQuant does not provide comprehensive daily basic indicators. "
            "Consider using Tushare for this data."
        )

        return pd.DataFrame(columns=DailyBasicSchema.get_required_columns())

    def __del__(self):
        """清理资源"""
        if self.client:
            self.client.close()

    async def get_latest_record(
        self, symbol: str, data_type: str, table_name: str
    ) -> Optional[pd.DataFrame]:
        """
        获取数据库中指定symbol和数据类型的最新记录

        注意：XTQuantProvider本身不直接访问数据库。
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

        # XTQuant不支持批量更新，需要指定symbol
        if symbol is None:
            raise ProviderError(
                "XTQuant requires explicit symbol list. Bulk update not supported.",
                provider_name=self.name
            )

        # 处理单个symbol的增量更新
        if data_type == "daily":
            return self._get_incremental_daily(symbol, start_date, end_date, **kwargs)
        elif data_type.startswith("minute"):
            # 从data_type中提取频率 (e.g., "minute_5" -> "5m")
            if "_" in data_type:
                minute_freq = data_type.split("_")[1]  # "minute_5" -> "5"
                freq = kwargs.get("freq", f"{minute_freq}m")  # Default to "5m"
                logger.debug(f"Extracted frequency from data_type '{data_type}': {freq}")
            else:
                freq = kwargs.get("freq", "1m")  # Default for "minute"
                logger.debug(f"Using default frequency for data_type '{data_type}': {freq}")
            return self._get_incremental_minute(symbol, start_date, end_date, freq)
        elif data_type == "daily_basic":
            logger.warning("XTQuant does not support daily_basic incremental update")
            return pd.DataFrame(columns=DailyBasicSchema.get_required_columns())
        elif data_type == "adj_factor":
            logger.warning("XTQuant does not support adj_factor incremental update")
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])
        else:
            raise ProviderError(
                f"Unsupported data type for incremental update: {data_type}",
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
