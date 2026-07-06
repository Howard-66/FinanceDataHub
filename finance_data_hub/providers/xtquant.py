"""
XTQuant数据提供者

通过HTTP API客户端连接xtquant_helper微服务获取金融数据。
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
import httpx
import numpy as np
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
    FuturesDailySchema,
    FuturesMinuteSchema,
    validate_dataframe,
    convert_to_standard_columns,
    standardize_symbol,
)
from finance_data_hub.utils.market import infer_market_from_symbol, normalize_market
from finance_data_hub.utils.futures import (
    extract_futures_product_code,
    get_futures_exchange_from_symbol,
    is_xtquant_downloadable_futures_symbol,
    normalize_tushare_futures_symbol,
    to_xtquant_futures_symbol,
)


HK_STOCK_SECTOR_NAME = "香港联交所股票"
SUPPORTED_XTQUANT_MINUTE_FREQS = {
    "1m": "1m",
    "5m": "5m",
    "60m": "1h",
}
SUPPORTED_XTQUANT_FUTURES_MINUTE_FREQS = {
    "1m": "1m",
    "5m": "5m",
}


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
        self,
        name: str = "xtquant",
        config: Optional[Dict[str, Any]] = None,
        market: str = "CN",
    ):
        super().__init__(name, config, market=market)
        self.api_url: str = (
            config.get("api_url", "http://localhost:8100") if config else "http://localhost:8100"
        )
        self.timeout: int = config.get("timeout", 60) if config else 60
        self.health_timeout: int = (
            config.get("health_timeout", min(5, self.timeout)) if config else 5
        )
        self.max_retry: int = config.get("max_retry", 3) if config else 3
        self.retry_delay: float = config.get("retry_delay", 1.0) if config else 1.0

        # HTTP client
        self.client: Optional[httpx.Client] = None

    def _parse_retry_after(self, response: httpx.Response) -> Optional[int]:
        """Parse Retry-After header seconds for retryable helper responses."""
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return None
        try:
            return max(0, int(float(retry_after)))
        except (TypeError, ValueError):
            return None

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

            self._probe_helper_connectivity()

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

    def _is_ok_health_response(self, response: httpx.Response) -> bool:
        if response.status_code != 200:
            return False
        try:
            data = response.json()
        except ValueError:
            return True
        return not data or data.get("status") == "ok"

    def _probe_helper_connectivity(self) -> None:
        """Check helper reachability with a lightweight health endpoint first."""
        if not self.client:
            raise ProviderConnectionError(
                "XTQuant HTTP client is not initialized",
                provider_name=self.name,
            )

        for endpoint in ("/health", "/"):
            try:
                response = self.client.get(endpoint, timeout=self.health_timeout)
                if self._is_ok_health_response(response):
                    return
                logger.debug(
                    f"XTQuant health check {endpoint} returned {response.status_code}; "
                    "trying next probe"
                )
            except httpx.TimeoutException:
                logger.warning(
                    f"XTQuant health check {endpoint} timed out after "
                    f"{self.health_timeout}s; trying next probe"
                )
            except httpx.ConnectError:
                raise
            except Exception as exc:
                logger.debug(
                    f"XTQuant health check {endpoint} failed ({exc}); "
                    "trying next probe"
                )

        try:
            response = self.client.get(
                "/download_history_data",
                timeout=self.health_timeout,
            )
            if response.status_code in {200, 405, 422}:
                return
            raise ProviderConnectionError(
                f"xtquant_helper endpoint probe failed: {response.status_code}",
                provider_name=self.name,
            )
        except httpx.ConnectError:
            raise
        except httpx.TimeoutException as exc:
            raise ProviderConnectionError(
                f"xtquant_helper endpoint probe timed out after {self.health_timeout}s",
                provider_name=self.name,
            ) from exc

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
            self._probe_helper_connectivity()
            return True
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
                if response.status_code == 503:
                    raise ProviderRateLimitError(
                        "xtquant_helper temporarily unavailable: 503",
                        provider_name=self.name,
                        retry_after=self._parse_retry_after(response),
                    )
                elif response.status_code >= 500:
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
            except (ProviderConnectionError, ProviderRateLimitError, ProviderDataError):
                raise
            except Exception as e:
                raise ProviderError(
                    f"Unexpected error calling xtquant API: {str(e)}",
                    provider_name=self.name,
                ) from e

        # 使用重试机制
        return self.retry_on_failure(
            _call,
            max_retries=self.max_retry,
            base_delay=self.retry_delay,
        )

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

        raw_symbol = str(symbol).strip()
        raw_symbol_upper = raw_symbol.upper()
        symbol = standardize_symbol(raw_symbol_upper, provider_format="xtquant")
        candidate_symbols = list(dict.fromkeys([symbol, raw_symbol, raw_symbol_upper]))
        logger.debug(f"Converting data dict, keys: {list(data_dict.keys())}")

        matched_symbol = next(
            (candidate for candidate in candidate_symbols if candidate in data_dict),
            None,
        )
        if matched_symbol:
            # Legacy/internal shape: {symbol: {field: {row_key: value}}}
            data_dict = data_dict[matched_symbol]
            df = pd.DataFrame(data_dict)
            if "time" not in df.columns:
                df = df.reset_index().rename(columns={"index": "time"})
        else:
            # xtquant_helper serializes XTQuant's {field: DataFrame} shape with
            # DataFrame.to_dict(): {field: {time_key: {symbol: value}}}.
            rows: Dict[Any, Dict[str, Any]] = {}
            for field, field_data in data_dict.items():
                if not isinstance(field_data, dict):
                    continue

                field_symbol = next(
                    (
                        candidate
                        for candidate in candidate_symbols
                        if candidate in field_data
                        and isinstance(field_data[candidate], dict)
                    ),
                    None,
                )
                if field_symbol:
                    iterable = field_data[field_symbol].items()
                    for row_key, value in iterable:
                        rows.setdefault(row_key, {})[field] = value
                    continue

                for row_key, value_by_symbol in field_data.items():
                    value = None
                    if isinstance(value_by_symbol, dict):
                        for candidate in candidate_symbols:
                            value = value_by_symbol.get(candidate)
                            if value is not None:
                                break
                        if value is None and len(value_by_symbol) == 1:
                            value = next(iter(value_by_symbol.values()))
                    else:
                        value = value_by_symbol
                    rows.setdefault(row_key, {})[field] = value

            if not rows:
                # Fallback for already row-oriented dicts.
                df = pd.DataFrame(data_dict)
            else:
                df = pd.DataFrame.from_dict(rows, orient="index").reset_index()
                if "time" not in df.columns:
                    df = df.rename(columns={"index": "time"})
                else:
                    df = df.drop(columns=["index"], errors="ignore")

        if df.empty:
            logger.warning("Empty DataFrame after conversion")
            return df

        # 转换时间戳（毫秒 -> datetime）
        if "time" in df.columns:
            time_values = df["time"]
            sample_ts = time_values.iloc[0]
            logger.debug(f"Converting time column, sample: {repr(sample_ts)}, type: {type(sample_ts).__name__}")

            try:
                numeric_time = pd.to_numeric(time_values, errors="coerce")
                if numeric_time.notna().any():
                    numeric_non_null = numeric_time.dropna()
                    numeric_strings = numeric_non_null.astype("int64").astype(str)
                    unique_lengths = set(numeric_strings.str.len())
                    max_abs = numeric_non_null.abs().max()
                    if unique_lengths == {14}:
                        df["time"] = pd.to_datetime(
                            numeric_time.astype("Int64").astype(str),
                            format="%Y%m%d%H%M%S",
                            errors="coerce",
                        )
                    elif unique_lengths == {8}:
                        df["time"] = pd.to_datetime(
                            numeric_time.astype("Int64").astype(str),
                            format="%Y%m%d",
                            errors="coerce",
                        )
                    elif max_abs >= 10**11:
                        # XTQuant commonly returns UTC millisecond timestamps.
                        df["time"] = pd.to_datetime(numeric_time, unit="ms", utc=True)
                        df["time"] = df["time"].dt.tz_convert("Asia/Shanghai")
                    elif max_abs >= 10**9:
                        df["time"] = pd.to_datetime(numeric_time, unit="s", utc=True)
                        df["time"] = df["time"].dt.tz_convert("Asia/Shanghai")
                    else:
                        df["time"] = pd.to_datetime(time_values, errors="coerce")
                else:
                    df["time"] = pd.to_datetime(time_values, errors="coerce")
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

        market_code = normalize_market(market, default=self.market)

        if market_code != "HK":
            logger.warning(
                "XTQuant stock basic currently only supports HK via sector list. "
                "Use Tushare for CN stock basic data."
            )
            return pd.DataFrame(columns=StockBasicSchema.get_required_columns())

        # Refresh sector metadata first; failures here should not hide a usable
        # cached sector list, so continue to read the sector if refresh fails.
        try:
            self._call_api("/download_sector_data")
        except ProviderError as e:
            logger.warning(f"Failed to refresh XTQuant sector data: {e}")

        data = self._call_api(
            "/get_stock_list_in_sector",
            {"sector_name": HK_STOCK_SECTOR_NAME},
        )
        symbols = data.get("result", data) if isinstance(data, dict) else data
        if not symbols:
            logger.warning(f"No symbols returned for sector: {HK_STOCK_SECTOR_NAME}")
            return pd.DataFrame(columns=StockBasicSchema.get_required_columns())

        standardized_symbols = [
            standardize_symbol(str(symbol), provider_format="xtquant")
            for symbol in symbols
            if symbol
        ]
        df = pd.DataFrame(
            {
                "symbol": standardized_symbols,
                "name": standardized_symbols,
                "market": ["HK"] * len(standardized_symbols),
                "exchange": ["HK"] * len(standardized_symbols),
                "industry": [None] * len(standardized_symbols),
                "area": [None] * len(standardized_symbols),
                "list_status": [list_status or "L"] * len(standardized_symbols),
                "list_date": [pd.NaT] * len(standardized_symbols),
                "delist_date": [pd.NaT] * len(standardized_symbols),
                "is_hs": [None] * len(standardized_symbols),
            }
        )

        df = validate_dataframe(df, StockBasicSchema, provider_name=self.name)
        logger.info(f"Fetched {len(df)} HK stocks from XTQuant sector list")
        return df

    def _format_daily_date(self, date_value: Optional[str], default: Optional[str]) -> str:
        """Format a daily date for XTQuant endpoints."""
        if date_value is None:
            return default or ""
        return str(date_value).replace("-", "")[:8]

    def _format_minute_date(self, date_value: Optional[str]) -> str:
        """Format a minute datetime for XTQuant endpoints."""
        if date_value is None:
            return ""
        text = str(date_value).strip()
        if not text:
            return ""
        if text.isdigit() and len(text) in {8, 14}:
            return text

        parsed = pd.to_datetime(text, errors="coerce")
        if not pd.isna(parsed):
            if isinstance(parsed, pd.Timestamp) and parsed.tz is not None:
                parsed = parsed.tz_convert("Asia/Shanghai").tz_localize(None)
            if (
                parsed.hour == 0
                and parsed.minute == 0
                and parsed.second == 0
                and parsed.microsecond == 0
                and len(text) <= 10
            ):
                return parsed.strftime("%Y%m%d")
            return parsed.strftime("%Y%m%d%H%M%S")

        return (
            text.replace("-", "")
            .replace(" ", "")
            .replace("T", "")
            .replace(":", "")[:14]
        )

    def _format_minute_boundary(
        self,
        date_value: Optional[str],
        *,
        is_end: bool,
    ) -> str:
        """Format a minute boundary, expanding date-only values to full-day bounds."""
        formatted = self._format_minute_date(date_value)
        if not formatted:
            return ""
        if len(formatted) == 8:
            return f"{formatted}{'235959' if is_end else '000000'}"
        return formatted[:14]

    def _expand_futures_minute_fetch_end(self, end_time: str, freq: str) -> str:
        """Request one extra bar because XTQuant treats minute end_time as exclusive."""
        if not end_time:
            return ""
        step_minutes = 5 if freq == "5m" else 1
        end_dt = datetime.strptime(end_time[:14], "%Y%m%d%H%M%S")
        return (end_dt + timedelta(minutes=step_minutes)).strftime("%Y%m%d%H%M%S")

    def _filter_frame_by_minute_window(
        self,
        df: pd.DataFrame,
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        """Filter a normalized minute frame by full datetime boundaries."""
        if df.empty or "time" not in df.columns:
            return df

        time_values = pd.to_datetime(df["time"], errors="coerce")
        if isinstance(time_values.dtype, pd.DatetimeTZDtype):
            time_values = (
                time_values.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
            )

        mask = time_values.notna()
        if start_time:
            start_bound = pd.to_datetime(start_time, format="%Y%m%d%H%M%S")
            mask &= time_values >= start_bound
        if end_time:
            end_bound = pd.to_datetime(end_time, format="%Y%m%d%H%M%S")
            mask &= time_values <= end_bound

        return df.loc[mask].sort_values("time").reset_index(drop=True)

    def _empty_daily_frame(self) -> pd.DataFrame:
        return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

    def _normalize_ohlcv_frame(
        self,
        data: Any,
        symbol: str,
        schema,
    ) -> pd.DataFrame:
        """Convert XTQuant kline payloads into the standard OHLCV schema."""
        if not data:
            logger.warning("Invalid or empty kline data from XTQuant")
            return pd.DataFrame(columns=schema.get_required_columns())

        df = self._convert_dict_to_dataframe(data, symbol)
        if df.empty:
            return pd.DataFrame(columns=schema.get_required_columns())

        df.columns = [col.lower() for col in df.columns]

        if "preclose" in df.columns and "close" in df.columns:
            close = pd.to_numeric(df["close"], errors="coerce")
            preclose = pd.to_numeric(df["preclose"], errors="coerce")
            valid_preclose = preclose.notna() & (preclose != 0)

            df["change_amount"] = (close - preclose).where(valid_preclose)
            df["change_pct"] = ((close - preclose) / preclose * 100).where(
                valid_preclose
            )
            df["change_amount"] = df["change_amount"].replace(
                [np.inf, -np.inf], pd.NA
            )
            df["change_pct"] = df["change_pct"].replace([np.inf, -np.inf], pd.NA)
        else:
            logger.warning("Missing preclose field, cannot calculate change_pct/change_amount")
            df["change_amount"] = None
            df["change_pct"] = None

        df = validate_dataframe(df, schema, provider_name=self.name)
        return df.sort_values("time").reset_index(drop=True)

    def _fetch_daily_by_dividend(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
        dividend_type: str,
    ) -> pd.DataFrame:
        """Download and read daily data with a specific XTQuant dividend_type."""
        start_time = self._format_daily_date(start_date, "20000101")
        end_time = self._format_daily_date(
            end_date,
            datetime.now().strftime("%Y%m%d"),
        )

        download_payload = {
            "stock_code": symbol,
            "period": "1d",
            "start_time": start_time,
            "end_time": end_time,
            "incrementally": None,
        }

        logger.debug(
            f"Downloading daily data for {symbol}: {start_time} to {end_time}, "
            f"dividend_type={dividend_type}"
        )
        self._call_api("/download_history_data", download_payload)

        payload = {
            "field_list": [],
            "stock_list": [symbol],
            "period": "1d",
            "start_time": start_time,
            "end_time": end_time,
            "dividend_type": dividend_type,
            "fill_data": True,
            "use_client_data": False,
        }

        data = self._call_api("/get_local_data", payload)
        return self._normalize_ohlcv_frame(data, symbol, DailyDataSchema)

    def _derive_adj_factor_from_adjusted_close(
        self,
        raw_df: pd.DataFrame,
        adjusted_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Derive cumulative adj_factor from adjusted/raw close ratios."""
        if raw_df.empty or adjusted_df.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        merged = raw_df[["time", "symbol", "close"]].merge(
            adjusted_df[["time", "symbol", "close"]],
            on=["time", "symbol"],
            how="inner",
            suffixes=("_raw", "_adjusted"),
        )
        if merged.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        raw_close = pd.to_numeric(merged["close_raw"], errors="coerce")
        adjusted_close = pd.to_numeric(merged["close_adjusted"], errors="coerce")
        valid = raw_close.notna() & adjusted_close.notna() & (raw_close != 0)
        result = merged.loc[valid, ["symbol", "time"]].copy()
        result["adj_factor"] = (
            adjusted_close[valid].to_numpy() / raw_close[valid].to_numpy()
        )
        return result.sort_values("time").reset_index(drop=True)

    def _derive_adj_factor_from_preclose_chain(
        self,
        raw_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Derive cumulative adj_factor from raw daily bars using preclose jumps.

        For HK, XTQuant currently returns identical bars for different
        dividend_type values. We therefore infer ex-right / ex-dividend jumps
        from the relationship between today's preclose and the previous
        trading day's close, while filtering out clearly incompatible scale
        changes (for example rows where preclose is still on a pre-split basis
        but OHLC has already been normalized).
        """
        if raw_df.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        if "preclose" not in raw_df.columns:
            logger.warning("Raw daily data has no preclose column; cannot derive adj_factor")
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        df = raw_df.sort_values("time").reset_index(drop=True).copy()
        close = pd.to_numeric(df["close"], errors="coerce")
        preclose = pd.to_numeric(df["preclose"], errors="coerce")
        prev_close = close.shift(1)

        ratio = preclose / prev_close
        finite_ratio = ratio.replace([np.inf, -np.inf], pd.NA)

        # Keep only same-scale event markers. This captures normal
        # ex-dividend / rights adjustments while ignoring obviously
        # incompatible values such as pre-split reference prices.
        valid_jump = (
            prev_close.notna()
            & (prev_close > 0)
            & preclose.notna()
            & (preclose > 0)
            & finite_ratio.notna()
            & (finite_ratio > 0.5)
            & (finite_ratio < 1.5)
        )

        multipliers = pd.Series(1.0, index=df.index, dtype="float64")
        multipliers.loc[valid_jump] = (
            prev_close.loc[valid_jump].to_numpy()
            / preclose.loc[valid_jump].to_numpy()
        )
        multipliers = multipliers.replace([np.inf, -np.inf], 1.0).fillna(1.0)

        result = df[["symbol", "time"]].copy()
        result["adj_factor"] = multipliers.cumprod()
        return result.sort_values("time").reset_index(drop=True)

    def get_daily_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adj: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取日线行情数据

        Args:
            symbol: 股票代码（例如 "600519.SH"）
            start_date: 开始日期（YYYY-MM-DD 或 YYYYMMDD）
            end_date: 结束日期（YYYY-MM-DD 或 YYYYMMDD）
            adj: 复权类型（None=不复权, "qfq"=前复权, "hfq"=后复权）
            market: 宽市场代码（CN/HK）

        Returns:
            pd.DataFrame: 标准格式的日线数据
        """
        market_code = normalize_market(
            market,
            default=infer_market_from_symbol(symbol, default=self.market),
        )

        dividend_type = "none"
        if market_code == "HK" and adj:
            logger.warning(
                "HK daily updates store raw unadjusted bars. "
                "Use adj_factor + SDK adjustment for qfq/hfq queries."
            )
        elif adj == "qfq":
            dividend_type = "front"
        elif adj == "hfq":
            dividend_type = "back"

        logger.info(
            f"Fetching daily data for {symbol} from {start_date} to {end_date} "
            f"(adj={adj}, market={market_code}, dividend_type={dividend_type})"
        )
        df = self._fetch_daily_by_dividend(symbol, start_date, end_date, dividend_type)
        logger.info(f"Fetched {len(df)} daily records for {symbol}")
        return df

    def get_minute_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        freq: str = "1m",
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取分钟级行情数据

        Args:
            symbol: 股票代码
            start_date: 开始日期时间
            end_date: 结束日期时间
            freq: 频率（1m, 5m, 60m）
            market: 宽市场代码（CN/HK）

        Returns:
            pd.DataFrame: 标准格式的分钟数据
        """
        market_code = normalize_market(
            market,
            default=infer_market_from_symbol(symbol, default=self.market),
        )
        start_time = self._format_minute_date(start_date)
        end_time = self._format_minute_date(end_date)

        # 转换频率格式
        xtquant_freq = SUPPORTED_XTQUANT_MINUTE_FREQS.get(freq)
        if not xtquant_freq:
            raise ProviderDataError(
                f"XTQuant minute only supports {', '.join(SUPPORTED_XTQUANT_MINUTE_FREQS)}; got {freq}",
                provider_name=self.name,
            )
        dividend_type = "none" if market_code == "HK" else "front"
        logger.info(
            f"Fetching {freq} data for {symbol} from {start_date} to {end_date} "
            f"(market={market_code}, dividend_type={dividend_type}, xtquant_freq={xtquant_freq})"
        )

        # XTQuant需要两步：
        # 1. 先下载数据到本地（使用 download_history_data）
        # 2. 然后从本地获取（使用 get_local_data）
        download_payload = {
            "stock_code": symbol,
            "period": xtquant_freq,
            "start_time": start_time[:8],
            "end_time": end_time[:8],
            "incrementally": None,  # 增量下载
        }

        # 第一步：下载数据到本地
        logger.debug(
            f"Downloading {freq} data for {symbol} from {start_time[:8]} to {end_time[:8]}"
        )
        logger.debug(
            f"Download payload: period={xtquant_freq}, start={start_time[:8]}, end={end_time[:8]}"
        )
        self._call_api("/download_history_data", download_payload)

        # 第二步：从本地获取数据
        payload = {
            "field_list": [],
            "stock_list": [symbol],
            "period": xtquant_freq,
            "start_time": start_time[:8],
            "end_time": end_time[:8],
            "dividend_type": dividend_type,
            "fill_data": True,
            "use_client_data": False,
        }

        logger.debug(
            f"Getting local data with payload: period={xtquant_freq}, "
            f"start={start_time[:8]}, end={end_time[:8]}"
        )
        data = self._call_api("/get_local_data", payload)

        df = self._normalize_ohlcv_frame(data, symbol, MinuteDataSchema)

        logger.info(f"Fetched {len(df)} minute records for {symbol}")
        return df

    def _normalize_futures_ohlcv_frame(
        self,
        data: Any,
        xt_symbol: str,
        canonical_symbol: str,
        schema,
        frequency: Optional[str] = None,
    ) -> pd.DataFrame:
        """Convert XTQuant futures kline payloads to FinanceDataHub futures schema."""
        if not data:
            return pd.DataFrame(columns=schema.get_required_columns())

        df = self._convert_dict_to_dataframe(data, xt_symbol)
        if df.empty:
            return pd.DataFrame(columns=schema.get_required_columns())

        df.columns = [col.lower() for col in df.columns]
        df = df.rename(
            columns={
                "preclose": "pre_close",
                "pre_close": "pre_close",
                "presettle": "pre_settle",
                "pre_settle": "pre_settle",
                "settlement": "settle",
                "openinterest": "open_interest",
                "open_interest": "open_interest",
                "oi": "open_interest",
            }
        )
        df["symbol"] = canonical_symbol
        df["product_code"] = extract_futures_product_code(canonical_symbol)
        df["exchange"] = get_futures_exchange_from_symbol(canonical_symbol)
        df["source"] = "xtquant"
        if frequency:
            df["frequency"] = frequency

        if "amount" not in df.columns:
            df["amount"] = None
        if "volume" not in df.columns and "vol" in df.columns:
            df["volume"] = df["vol"]

        return validate_dataframe(df, schema, provider_name=self.name).sort_values(
            "time"
        ).reset_index(drop=True)

    def get_futures_daily(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **_: Any,
    ) -> pd.DataFrame:
        """获取期货日线行情。"""
        if symbol and not symbols:
            symbols = [symbol]
        if not symbols:
            raise ProviderDataError(
                "XTQuant futures daily requires explicit symbols",
                provider_name=self.name,
            )

        start_time = self._format_daily_date(start_date, "20000101")
        end_time = self._format_daily_date(
            end_date,
            datetime.now().strftime("%Y%m%d"),
        )
        all_data = []
        for raw_symbol in symbols:
            canonical_symbol = normalize_tushare_futures_symbol(raw_symbol)
            if not is_xtquant_downloadable_futures_symbol(canonical_symbol):
                raise ProviderDataError(
                    f"Unsupported XTQuant futures symbol: {raw_symbol}",
                    provider_name=self.name,
                )
            xt_symbol = to_xtquant_futures_symbol(canonical_symbol)
            if not xt_symbol:
                raise ProviderDataError(
                    f"Invalid XTQuant futures symbol: {raw_symbol}",
                    provider_name=self.name,
                )
            self._call_api(
                "/download_history_data",
                {
                    "stock_code": xt_symbol,
                    "period": "1d",
                    "start_time": start_time,
                    "end_time": end_time,
                    "incrementally": None,
                },
            )
            data = self._call_api(
                "/get_local_data",
                {
                    "field_list": [],
                    "stock_list": [xt_symbol],
                    "period": "1d",
                    "start_time": start_time,
                    "end_time": end_time,
                    "dividend_type": "none",
                    "fill_data": True,
                    "use_client_data": False,
                },
            )
            df = self._normalize_futures_ohlcv_frame(
                data,
                xt_symbol,
                canonical_symbol,
                FuturesDailySchema,
            )
            if not df.empty:
                all_data.append(df)

        if not all_data:
            return pd.DataFrame(columns=FuturesDailySchema.get_required_columns())
        return pd.concat(all_data, ignore_index=True, sort=False)

    def get_futures_minute(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = "1m",
        **_: Any,
    ) -> pd.DataFrame:
        """获取期货分钟行情。"""
        if symbol and not symbols:
            symbols = [symbol]
        if not symbols:
            raise ProviderDataError(
                "XTQuant futures minute requires explicit symbols",
                provider_name=self.name,
            )

        xt_freq = SUPPORTED_XTQUANT_FUTURES_MINUTE_FREQS.get(freq)
        if not xt_freq:
            raise ProviderDataError(
                f"XTQuant futures minute only supports {', '.join(SUPPORTED_XTQUANT_FUTURES_MINUTE_FREQS)}; got {freq}",
                provider_name=self.name,
            )
        start_time = self._format_minute_boundary(start_date, is_end=False)
        end_time = self._format_minute_boundary(end_date, is_end=True)
        fetch_end_time = self._expand_futures_minute_fetch_end(end_time, freq)
        start_day = start_time[:8]
        end_day = fetch_end_time[:8]

        all_data = []
        for raw_symbol in symbols:
            canonical_symbol = normalize_tushare_futures_symbol(raw_symbol)
            if not is_xtquant_downloadable_futures_symbol(canonical_symbol):
                raise ProviderDataError(
                    f"Unsupported XTQuant futures symbol: {raw_symbol}",
                    provider_name=self.name,
                )
            xt_symbol = to_xtquant_futures_symbol(canonical_symbol)
            if not xt_symbol:
                raise ProviderDataError(
                    f"Invalid XTQuant futures symbol: {raw_symbol}",
                    provider_name=self.name,
                )
            self._call_api(
                "/download_history_data",
                {
                    "stock_code": xt_symbol,
                    "period": xt_freq,
                    "start_time": start_time or start_day,
                    "end_time": fetch_end_time or end_day,
                    "incrementally": None,
                },
            )
            data = self._call_api(
                "/get_local_data",
                {
                    "field_list": [],
                    "stock_list": [xt_symbol],
                    "period": xt_freq,
                    "start_time": start_time or start_day,
                    "end_time": fetch_end_time or end_day,
                    "dividend_type": "none",
                    "fill_data": True,
                    "use_client_data": False,
                },
            )
            df = self._normalize_futures_ohlcv_frame(
                data,
                xt_symbol,
                canonical_symbol,
                FuturesMinuteSchema,
                frequency=freq,
            )
            df = self._filter_frame_by_minute_window(df, start_time, end_time)
            if not df.empty:
                all_data.append(df)

        if not all_data:
            return pd.DataFrame(columns=FuturesMinuteSchema.get_required_columns())
        return pd.concat(all_data, ignore_index=True, sort=False)

    def get_adj_factor(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_date: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        通过 XTQuant 原始日线的 preclose 链反推复权因子。

        对港股，XTQuant 当前无论 dividend_type 如何设置，返回的都是未复权
        日线，因此这里不再尝试 back_ratio/back，而是直接基于原始日线中的
        preclose 与前一交易日 close 的跳变关系推导累计复权因子。
        """
        if symbol is None:
            raise ProviderError(
                "XTQuant adj_factor derivation requires an explicit symbol",
                provider_name=self.name,
            )

        if trade_date:
            start_date = trade_date
            end_date = trade_date

        market_code = normalize_market(
            market,
            default=infer_market_from_symbol(symbol, default=self.market),
        )
        logger.info(
            f"Deriving adj_factor for {symbol} from {start_date} to {end_date} "
            f"(market={market_code})"
        )

        raw_df = self._fetch_daily_by_dividend(symbol, start_date, end_date, "none")
        if raw_df.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        if market_code != "HK":
            adjusted_result = pd.DataFrame(columns=["symbol", "time", "adj_factor"])
            try:
                adjusted_df = self._fetch_daily_by_dividend(
                    symbol,
                    start_date,
                    end_date,
                    "back_ratio",
                )
            except ProviderError as e:
                logger.warning(f"XTQuant back_ratio data unavailable for {symbol}: {e}")
                adjusted_df = pd.DataFrame()

            if adjusted_df.empty:
                logger.warning(f"Falling back to dividend_type=back for {symbol}")
                adjusted_df = self._fetch_daily_by_dividend(
                    symbol,
                    start_date,
                    end_date,
                    "back",
                )

            if not adjusted_df.empty:
                adjusted_result = self._derive_adj_factor_from_adjusted_close(
                    raw_df, adjusted_df
                )

            if not adjusted_result.empty:
                logger.info(
                    f"Derived {len(adjusted_result)} adj_factor records for {symbol} "
                    f"via adjusted/raw close ratios"
                )
                return adjusted_result

        result = self._derive_adj_factor_from_preclose_chain(raw_df)
        logger.info(
            f"Derived {len(result)} adj_factor records for {symbol} via raw preclose chain"
        )
        return result

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
            return self._get_incremental_minute(
                symbol,
                start_date,
                end_date,
                freq,
                market=kwargs.get("market"),
            )
        elif data_type == "daily_basic":
            logger.warning("XTQuant does not support daily_basic incremental update")
            return pd.DataFrame(columns=DailyBasicSchema.get_required_columns())
        elif data_type == "adj_factor":
            return self.get_adj_factor(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                market=kwargs.get("market"),
            )
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
            market=kwargs.get("market"),
        )

    def _get_incremental_minute(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
        freq: str,
        market: Optional[str] = None,
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
            market=market,
        )
