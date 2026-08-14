"""
数据更新器

集成Provider、Router和数据库操作，实现完整的数据更新流程。
"""

import asyncio
import calendar
import re
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any, Union, Iterator, Tuple, Callable
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
import json
import time
import pandas as pd
from loguru import logger

from finance_data_hub.router.smart_router import SmartRouter
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import (
    DataOperations,
    _normalize_futures_minute_frequency,
)
from finance_data_hub.config import Settings
from finance_data_hub.providers.tushare import (
    SUPPORTED_INDEX_CODES,
    SUPPORTED_EXCHANGES,
    SUPPORTED_FUTURES_EXCHANGES,
    SUPPORTED_FUTURES_INDEX_CODES,
    TUSHARE_INDEX_MARKETS,
    TUSHARE_FUND_MARKETS,
)
from finance_data_hub.utils.market import infer_market_from_symbol, normalize_market
from finance_data_hub.utils.futures import is_xtquant_downloadable_futures_symbol

FUTURES_SPOT_BASIS_HISTORY_START = "2011-01-04"
FUTURES_SPOT_BASIS_CHUNK_DAYS = 31
FUTURES_INVENTORY_SUPPORTED_EXCHANGES = {"DCE", "CZCE", "SHFE", "GFEX"}
FUTURES_TERM_INSERT_BATCH_SIZE = 1000
FUTURES_TERM_STRUCTURE_RATE_EPS = 1e-4
FUTURES_PERIOD_LOOKBACK_DAYS = {
    "weekly": 7,
    "monthly": 31,
}
FUTURES_MINUTE_TRADING_DAY_START = datetime_time(21, 0, 0)
FUTURES_MINUTE_TRADING_DAY_END = datetime_time(15, 0, 0)
INDEX_DAILY_EXCLUDED_CATALOG_MARKETS = {"SW"}
FUND_PORTFOLIO_HISTORY_START = "1998-01-01"
IDX_ANNS_HISTORY_START = "1990-01-01"


def _is_all_symbol_request(symbols: Optional[List[str]]) -> bool:
    if not symbols:
        return False
    lowered = {symbol.lower() for symbol in symbols}
    return lowered == {"all"}


def _validate_no_mixed_all(symbols: Optional[List[str]], scope_name: str) -> None:
    if not symbols:
        return
    lowered = {symbol.lower() for symbol in symbols}
    if "all" in lowered and len(lowered) > 1:
        raise ValueError(f"{scope_name} --symbols all 不能与其他代码混用")


def _as_positive_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number) or number <= 0:
        return None
    return number


def _futures_curve_expiry(row: pd.Series) -> Optional[pd.Timestamp]:
    for column in ("last_ddate", "delivery_month_start"):
        value = row.get(column)
        if pd.notna(value):
            return pd.Timestamp(value)
    return None


def _calculate_futures_term_structure_flag(curve: pd.DataFrame) -> float:
    """Return a signed term-structure signal for a maturity-sorted futures curve."""
    if len(curve) < 2:
        return 0.0

    signed_rates: List[Tuple[float, int]] = []
    for position in range(1, len(curve)):
        near = curve.iloc[position - 1]
        far = curve.iloc[position]
        near_price = _as_positive_float(near.get("price"))
        far_price = _as_positive_float(far.get("price"))
        if near_price is None or far_price is None:
            continue

        signed_return = (near_price - far_price) / near_price
        near_expiry = _futures_curve_expiry(near)
        far_expiry = _futures_curve_expiry(far)
        if near_expiry is not None and far_expiry is not None:
            days_between = (far_expiry.date() - near_expiry.date()).days
        else:
            days_between = 0

        if days_between > 0:
            signed_rates.append((signed_return * (365 / days_between), days_between))
        else:
            signed_rates.append((signed_return, 1))

    if not signed_rates:
        return 0.0

    if all(rate > FUTURES_TERM_STRUCTURE_RATE_EPS for rate, _ in signed_rates):
        return 1.0
    if all(rate < -FUTURES_TERM_STRUCTURE_RATE_EPS for rate, _ in signed_rates):
        return -1.0

    total_weight = sum(weight for _, weight in signed_rates)
    weighted_rate = sum(rate * weight for rate, weight in signed_rates) / total_weight
    if weighted_rate > FUTURES_TERM_STRUCTURE_RATE_EPS:
        return 0.5
    if weighted_rate < -FUTURES_TERM_STRUCTURE_RATE_EPS:
        return -0.5
    return 0.0


def _convert_to_month_format(date_str: Optional[str]) -> Optional[str]:
    """
    将 YYYY-MM-DD 格式转换为 YYYYMM 格式

    Args:
        date_str: YYYY-MM-DD 格式日期，如 "2024-01-31"

    Returns:
        YYYYMM 格式月份字符串，如 "202401"，或 None
    """
    if not date_str:
        return None
    # 提取 YYYY-MM 部分并去掉连字符
    return date_str.replace("-", "")[:6]


def _convert_to_quarter_format(date_str: Optional[str]) -> Optional[str]:
    """
    将 YYYY-MM-DD 格式转换为 YYYYQn 格式

    Args:
        date_str: YYYY-MM-DD 格式日期，如 "2024-03-31" 表示 Q1

    Returns:
        YYYYQn 格式季度字符串，如 "2024Q1"，或 None
    """
    if not date_str:
        return None
    # 解析日期，提取月份确定季度
    month = int(date_str[5:7])
    year = date_str[:4]

    # 根据月份确定季度
    if month <= 3:
        return f"{year}Q1"
    elif month <= 6:
        return f"{year}Q2"
    elif month <= 9:
        return f"{year}Q3"
    else:
        return f"{year}Q4"


def _split_futures_symbols_and_products(
    values: Optional[List[str]],
) -> tuple[List[str], List[str]]:
    """Split CLI-style futures inputs into contract symbols and product codes."""
    symbols: List[str] = []
    product_codes: List[str] = []
    for value in values or []:
        raw = str(value).strip()
        if not raw:
            continue
        if "." not in raw and not any(char.isdigit() for char in raw):
            product_codes.append(raw.upper())
        else:
            symbols.append(raw)
    return symbols, product_codes


def _is_all_futures_selector(values: Optional[List[str]]) -> bool:
    """Return True when the CLI explicitly requests the full futures universe."""
    cleaned = [str(value).strip().lower() for value in values or [] if str(value).strip()]
    return len(cleaned) == 1 and cleaned[0] == "all"


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _iter_date_chunks(
    start_date: str,
    end_date: str,
    max_days: int,
) -> Iterator[Tuple[str, str]]:
    """Yield inclusive date chunks from start to end in ascending order."""
    if max_days < 1:
        raise ValueError("max_days must be >= 1")
    current = datetime.strptime(start_date, "%Y-%m-%d")
    final = datetime.strptime(end_date, "%Y-%m-%d")
    while current <= final:
        chunk_end = min(current + timedelta(days=max_days - 1), final)
        yield current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        current = chunk_end + timedelta(days=1)


def _normalize_futures_window(
    trade_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Normalize futures request dates to an overlap-filter window."""
    if trade_date:
        if start_date or end_date:
            raise ValueError("trade_date cannot be used with start_date or end_date")
        return trade_date, trade_date

    if start_date and end_date:
        return start_date, end_date
    if start_date:
        return start_date, start_date
    if end_date:
        return end_date, end_date
    return None, None


def _normalize_futures_query_dates(
    trade_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Normalize futures download query dates without changing incremental semantics."""
    if trade_date:
        if start_date or end_date:
            raise ValueError("trade_date cannot be used with start_date or end_date")
        return trade_date, trade_date
    return start_date, end_date


def _futures_period_query_end(
    end_date: Optional[str],
    period: str,
) -> Optional[str]:
    """Expand a business-date cutoff to the API's period-end label."""
    if not end_date:
        return None
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if period == "weekly":
        end += timedelta(days=(4 - end.weekday()) % 7)
    elif period == "monthly":
        end = end.replace(day=calendar.monthrange(end.year, end.month)[1])
    else:
        raise ValueError(f"Unsupported futures period: {period}")
    return end.strftime("%Y-%m-%d")


def _coerce_datetime(value: Union[str, datetime]) -> datetime:
    """Parse common scheduler/CLI date or datetime strings."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    text = str(value).strip()
    if re.fullmatch(r"\d{14}", text):
        return datetime.strptime(text, "%Y%m%d%H%M%S")
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d")

    parsed = pd.to_datetime(text)
    if isinstance(parsed, pd.Timestamp):
        if parsed.tz is not None:
            parsed = parsed.tz_convert("Asia/Shanghai").tz_localize(None)
        return parsed.to_pydatetime()
    raise ValueError(f"Invalid datetime value: {value}")


def _has_explicit_time(value: Optional[Union[str, datetime]]) -> bool:
    """Return whether the user provided an intraday time component."""
    if value is None:
        return False
    if isinstance(value, datetime):
        return value.time() != datetime_time.min

    text = str(value).strip()
    if re.fullmatch(r"\d{14}", text):
        return True
    return bool(re.search(r"(?:\s|T)\d{1,2}:\d{2}", text))


def _format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_futures_minute_window(
    trade_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Normalize futures minute inputs to the mainland futures trading-day window."""
    if trade_date:
        if start_date or end_date:
            raise ValueError("trade_date cannot be used with start_date or end_date")
        trade_day = _coerce_datetime(trade_date).date()
        start_dt = datetime.combine(
            trade_day - timedelta(days=1),
            FUTURES_MINUTE_TRADING_DAY_START,
        )
        end_dt = datetime.combine(trade_day, FUTURES_MINUTE_TRADING_DAY_END)
        return _format_datetime(start_dt), _format_datetime(end_dt)

    normalized_start = None
    if start_date:
        start_dt = _coerce_datetime(start_date)
        if not _has_explicit_time(start_date):
            start_dt = datetime.combine(
                start_dt.date() - timedelta(days=1),
                FUTURES_MINUTE_TRADING_DAY_START,
            )
        normalized_start = _format_datetime(start_dt)

    if end_date:
        end_dt = _coerce_datetime(end_date)
        if not _has_explicit_time(end_date):
            end_dt = datetime.combine(end_dt.date(), FUTURES_MINUTE_TRADING_DAY_END)
    else:
        end_dt = datetime.now()

    return normalized_start, _format_datetime(end_dt)


class DataUpdater:
    """数据更新器"""

    def __init__(
        self,
        settings: Settings,
        config_path: Optional[str] = None,
    ):
        """
        初始化数据更新器

        Args:
            settings: 应用配置
            config_path: 路由配置文件路径
        """
        self.settings = settings
        self.config_path = config_path or "sources.yml"

        # 初始化组件
        self.router: Optional[SmartRouter] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.data_ops: Optional[DataOperations] = None

    def _get_futures_minute_max_workers(self, symbol_count: int) -> int:
        """Return bounded worker count for futures minute downloads."""
        data_source_config = getattr(self.settings, "data_source", None)
        configured_workers = getattr(
            data_source_config, "futures_minute_max_workers", 1
        )
        try:
            workers = int(configured_workers)
        except (TypeError, ValueError):
            workers = 1
        workers = max(1, workers)
        if symbol_count > 0:
            workers = min(workers, symbol_count)
        return workers

    async def initialize(self) -> None:
        """初始化所有组件"""
        logger.info("Initializing DataUpdater...")

        # 初始化路由器
        try:
            self.router = SmartRouter(self.config_path)
            logger.info("SmartRouter initialized")
        except Exception as e:
            logger.error(f"Failed to initialize SmartRouter: {str(e)}")
            raise

        # 初始化数据库管理器
        try:
            self.db_manager = DatabaseManager(self.settings)
            await self.db_manager.initialize()
            self.data_ops = DataOperations(self.db_manager)
            logger.info("DatabaseManager initialized")
        except Exception as e:
            logger.error(f"Failed to initialize DatabaseManager: {str(e)}")
            raise

        logger.info("DataUpdater initialized successfully")

    def _normalize_update_markets(self, market: Optional[str]) -> List[str]:
        """Return concrete broad markets for an update request."""
        market_code = normalize_market(market, default="CN")
        if market_code == "ALL":
            return ["CN", "HK"]
        return [market_code]

    def _symbols_for_market(
        self,
        symbols: Optional[List[str]],
        market: str,
    ) -> Optional[List[str]]:
        """Filter an explicit symbol list for a concrete market in ALL mode."""
        if symbols is None:
            return None
        return [
            symbol
            for symbol in symbols
            if infer_market_from_symbol(symbol, default="CN") == market
        ]

    async def _resolve_index_catalog_codes(
        self,
        data_type: str,
        exclude_markets: Optional[set[str]] = None,
        active_date: Optional[str] = None,
    ) -> List[str]:
        """Resolve all index codes from the local index_basic table."""
        if not self.data_ops:
            raise RuntimeError("DataUpdater is not initialized")

        codes = await self.data_ops.get_index_basic_codes(
            exclude_markets=sorted(exclude_markets or []),
            active_date=active_date,
        )
        if codes:
            logger.info(
                f"Resolved {len(codes)} {data_type} index codes from local index_basic"
            )
            return list(codes)

        active_hint = f"（active_date={active_date}）" if active_date else ""
        raise ValueError(
            f"本地 index_basic 未返回任何可更新指数代码{active_hint}，"
            "请先执行 fdh-cli update --dataset index_basic"
        )

    async def resolve_index_daily_codes(
        self,
        active_date: Optional[str] = None,
    ) -> List[str]:
        """Resolve all index codes suitable for index_daily updates."""
        return await self._resolve_index_catalog_codes(
            data_type="daily",
            exclude_markets=INDEX_DAILY_EXCLUDED_CATALOG_MARKETS,
            active_date=active_date,
        )

    async def resolve_index_weight_codes(
        self,
        active_date: Optional[str] = None,
    ) -> List[str]:
        """Resolve all index codes suitable for index_weight updates."""
        return await self._resolve_index_catalog_codes(
            data_type="index_weight",
            active_date=active_date,
        )

    @staticmethod
    def _index_catalog_active_date(
        start_date: Optional[str],
        end_date: Optional[str],
        force_update: bool,
        trade_date: Optional[str] = None,
    ) -> Optional[str]:
        """Return the date used to exclude expired indexes from the local catalog."""
        if trade_date:
            return trade_date
        if start_date:
            return start_date
        if force_update:
            return None
        return end_date

    async def update_index_basic(
        self,
        markets: Optional[List[str]] = None,
    ) -> int:
        """刷新 Tushare 指数基本信息目录。

        该数据集是静态元数据，不使用日期增量。每次调用均会对所选市场
        的完整目录执行 upsert，使新发布和更新后的指数信息可被及时覆盖。

        Args:
            markets: Tushare 指数市场代码列表；None 表示全部支持市场。

        Returns:
            插入或更新的记录数。
        """
        normalized_markets = None
        if markets:
            normalized_markets = [str(item).upper() for item in markets]
            invalid_markets = sorted(
                set(normalized_markets) - set(TUSHARE_INDEX_MARKETS)
            )
            if invalid_markets:
                raise ValueError(
                    "不支持的指数市场代码: "
                    f"{', '.join(invalid_markets)}。支持: {', '.join(TUSHARE_INDEX_MARKETS)}"
                )

        try:
            data = self.router.route(
                asset_class="index",
                data_type="basic",
                method_name="get_index_basic",
                markets=normalized_markets,
            )
            if data is None or data.empty:
                logger.warning("No index_basic data received")
                return 0

            inserted_count = await self.data_ops.insert_index_basic_batch(data)
            logger.info(f"Updated {inserted_count} index_basic records")
            return inserted_count
        except Exception:
            logger.exception("Failed to update index_basic")
            raise

    async def update_fund_basic(
        self,
        markets: Optional[List[str]] = None,
        status: Optional[str] = None,
    ) -> int:
        """刷新 Tushare 公募基金基础信息目录。

        基金目录是非时间序列数据。每次同步会获取指定场内/场外市场的完整
        列表并执行 upsert；分页由 Provider 在接口返回 15,000 条时处理。
        """
        normalized_markets = None
        if markets:
            normalized_markets = [str(item).upper() for item in markets]
            invalid_markets = sorted(
                set(normalized_markets) - set(TUSHARE_FUND_MARKETS)
            )
            if invalid_markets:
                raise ValueError(
                    "不支持的基金市场代码: "
                    f"{', '.join(invalid_markets)}。支持: {', '.join(TUSHARE_FUND_MARKETS)}"
                )

        try:
            data = self.router.route(
                asset_class="fund",
                data_type="basic",
                method_name="get_fund_basic",
                markets=normalized_markets,
                status=status,
            )
            if data is None or data.empty:
                logger.warning("No fund_basic data received")
                return 0

            inserted_count = await self.data_ops.insert_fund_basic_batch(data)
            logger.info(f"Updated {inserted_count} fund_basic records")
            return inserted_count
        except Exception:
            logger.exception("Failed to update fund_basic")
            raise

    async def update_etf_basic(
        self,
        ts_code: Optional[str] = None,
        index_code: Optional[str] = None,
        list_date: Optional[str] = None,
        list_status: Optional[str] = None,
        exchange: Optional[str] = None,
        mgr: Optional[str] = None,
    ) -> int:
        """刷新 Tushare ETF 基础信息目录。"""
        try:
            data = self.router.route(
                asset_class="fund",
                data_type="etf_basic",
                method_name="get_etf_basic",
                ts_code=ts_code,
                index_code=index_code,
                list_date=list_date,
                list_status=list_status,
                exchange=exchange,
                mgr=mgr,
            )
            if data is None or data.empty:
                logger.warning("No etf_basic data received")
                return 0
            inserted_count = await self.data_ops.insert_etf_basic_batch(data)
            logger.info("Updated {} etf_basic records", inserted_count)
            return inserted_count
        except Exception:
            logger.exception("Failed to update etf_basic")
            raise

    async def update_etf_index(
        self, ts_code: Optional[str] = None, pub_date: Optional[str] = None,
        base_date: Optional[str] = None,
    ) -> int:
        """刷新 ETF 基准指数目录；无可靠变更时间时使用低成本全表 upsert。"""
        data = self.router.route(
            asset_class="fund", data_type="etf_index", method_name="get_etf_index",
            ts_code=ts_code, pub_date=pub_date, base_date=base_date,
        )
        return await self.data_ops.insert_etf_index_batch(data) if data is not None else 0

    async def _resolve_etf_history_start(self) -> str:
        start = await self.data_ops.get_earliest_etf_basic_date()
        if not start:
            raise ValueError(
                "全量 ETF 数据下载需要本地 etf_basic 目录；"
                "请先执行 `fdh-cli update --dataset etf_basic --symbols all`"
            )
        return start

    async def _update_etf_series(
        self, *, data_type: str, method_name: str, insert_method_name: str,
        latest_method_name: str, fund_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None, start_date: Optional[str] = None,
        end_date: Optional[str] = None, all_funds: bool = False,
        smart_incremental: bool = False, full_by_codes: bool = False,
        catalog_exchange: Optional[str] = None, extra_params: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        incremental_lookback_days: int = 0,
        always_by_codes: bool = False,
    ) -> int:
        """Shared full/incremental workflow for ETF date-series endpoints."""
        if all_funds and smart_incremental:
            raise ValueError(f"{data_type} 不能同时使用全量和智能增量模式")
        params = dict(extra_params or {})
        insert_method = getattr(self.data_ops, insert_method_name)
        latest = None

        if fund_codes and not always_by_codes:
            total = 0
            if progress_callback:
                progress_callback(0, len(fund_codes))
            for index, code in enumerate(fund_codes, start=1):
                data = self.router.route(
                    asset_class="fund", data_type=data_type, method_name=method_name,
                    ts_code=code, trade_date=trade_date, start_date=start_date,
                    end_date=end_date, **params,
                )
                if data is not None and not data.empty:
                    total += await insert_method(data)
                if progress_callback:
                    progress_callback(index, len(fund_codes))
            return total

        resolved_end = pd.Timestamp(end_date or datetime.now()).strftime("%Y-%m-%d")
        if all_funds:
            resolved_start = pd.Timestamp(
                start_date or await self._resolve_etf_history_start()
            ).strftime("%Y-%m-%d")
        elif smart_incremental:
            latest = await getattr(self.data_ops, latest_method_name)()
            if latest and incremental_lookback_days:
                latest = (pd.Timestamp(latest) - pd.Timedelta(days=incremental_lookback_days)).strftime(
                    "%Y-%m-%d"
                )
            resolved_start = pd.Timestamp(
                start_date or latest or await self._resolve_etf_history_start()
            ).strftime("%Y-%m-%d")
        elif trade_date:
            resolved_start = resolved_end = pd.Timestamp(trade_date).strftime("%Y-%m-%d")
        elif start_date or end_date:
            resolved_start = pd.Timestamp(
                start_date or await self._resolve_etf_history_start()
            ).strftime("%Y-%m-%d")
        else:
            raise ValueError(f"{data_type} 需要基金代码、交易日、日期范围或智能增量模式")

        if resolved_start > resolved_end:
            return 0

        if always_by_codes or (
            full_by_codes and (all_funds or (smart_incremental and not latest))
        ):
            catalog = await self.data_ops.get_etf_basic()
            if catalog is None or catalog.empty:
                await self._resolve_etf_history_start()
            if catalog_exchange:
                exchange_aliases = {
                    "SH": {"SH", "SSE"},
                    "SZ": {"SZ", "SZSE"},
                }.get(catalog_exchange, {catalog_exchange})
                code_suffix = f".{catalog_exchange}"
                catalog = catalog[
                    catalog["exchange"].astype(str).str.upper().isin(exchange_aliases)
                    | catalog["ts_code"].astype(str).str.upper().str.endswith(code_suffix)
                ]
            if fund_codes:
                requested_codes = {str(code).strip() for code in fund_codes}
                catalog = catalog[catalog["ts_code"].astype(str).isin(requested_codes)]
            catalog = catalog.sort_values("ts_code").reset_index(drop=True)
            if catalog.empty:
                raise ValueError(
                    f"{data_type} 未在本地 etf_basic 中找到匹配的 ETF；"
                    "请先刷新 etf_basic"
                )
            total = 0
            total_codes = len(catalog)
            if progress_callback:
                progress_callback(0, total_codes)
            for index, row in catalog.iterrows():
                code_start = resolved_start
                for date_column in ("setup_date", "list_date"):
                    value = row.get(date_column)
                    if pd.notna(value):
                        code_start = max(code_start, pd.Timestamp(value).strftime("%Y-%m-%d"))
                        break
                data = self.router.route(
                    asset_class="fund", data_type=data_type, method_name=method_name,
                    ts_code=row["ts_code"], start_date=code_start, end_date=resolved_end,
                    **params,
                )
                if data is not None and not data.empty:
                    total += await insert_method(data)
                if progress_callback:
                    progress_callback(index + 1, total_codes)
            return total

        dates = await self.data_ops.get_trade_dates(
            exchange="SSE", start_date=resolved_start, end_date=resolved_end
        )
        if not dates:
            logger.warning("SSE trade_cal unavailable; falling back to weekdays")
            dates = pd.bdate_range(resolved_start, resolved_end).strftime("%Y-%m-%d").tolist()
        total = 0
        if progress_callback:
            progress_callback(0, len(dates))
        for index, current_date in enumerate(dates, start=1):
            data = self.router.route(
                asset_class="fund", data_type=data_type, method_name=method_name,
                trade_date=current_date, **params,
            )
            if data is not None and not data.empty:
                total += await insert_method(data)
            if progress_callback:
                progress_callback(index, len(dates))
        return total

    async def update_fund_daily(self, fund_codes=None, trade_date=None, start_date=None,
                                end_date=None, all_funds=False, smart_incremental=False,
                                progress_callback=None) -> int:
        return await self._update_etf_series(
            data_type="fund_daily", method_name="get_fund_daily",
            insert_method_name="insert_fund_daily_batch",
            latest_method_name="get_latest_fund_daily_trade_date", fund_codes=fund_codes,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
            all_funds=all_funds, smart_incremental=smart_incremental,
            full_by_codes=True, progress_callback=progress_callback,
        )

    async def update_fund_adj(self, fund_codes=None, trade_date=None, start_date=None,
                              end_date=None, all_funds=False, smart_incremental=False,
                              progress_callback=None) -> int:
        return await self._update_etf_series(
            data_type="fund_adj", method_name="get_fund_adj",
            insert_method_name="insert_fund_adj_batch",
            latest_method_name="get_latest_fund_adj_trade_date", fund_codes=fund_codes,
            trade_date=trade_date, start_date=start_date, end_date=end_date,
            all_funds=all_funds, smart_incremental=smart_incremental,
            progress_callback=progress_callback,
        )

    async def update_etf_share_size(self, fund_codes=None, trade_date=None, start_date=None,
                                    end_date=None, exchange=None, all_funds=False,
                                    smart_incremental=False, progress_callback=None) -> int:
        exchange_map = {"SH": "SSE", "SZ": "SZSE"}
        return await self._update_etf_series(
            data_type="etf_share_size", method_name="get_etf_share_size",
            insert_method_name="insert_etf_share_size_batch",
            latest_method_name="get_latest_etf_share_size_trade_date",
            fund_codes=fund_codes, trade_date=trade_date, start_date=start_date,
            end_date=end_date, all_funds=all_funds, smart_incremental=smart_incremental,
            full_by_codes=True, catalog_exchange=exchange,
            extra_params={"exchange": exchange_map.get(exchange)},
            progress_callback=progress_callback, incremental_lookback_days=7,
        )

    async def update_etf_sh_cons(self, fund_codes=None, trade_date=None, start_date=None,
                                 end_date=None, con_code=None, all_funds=False,
                                 smart_incremental=False, progress_callback=None) -> int:
        return await self._update_etf_series(
            data_type="etf_sh_cons", method_name="get_etf_sh_cons",
            insert_method_name="insert_etf_sh_cons_batch",
            latest_method_name="get_latest_etf_sh_cons_trade_date",
            fund_codes=fund_codes, trade_date=trade_date, start_date=start_date,
            end_date=end_date, all_funds=all_funds, smart_incremental=smart_incremental,
            full_by_codes=True, always_by_codes=True, catalog_exchange="SH",
            extra_params={"con_code": con_code}, progress_callback=progress_callback,
        )

    async def update_etf_sz_cons(self, fund_codes=None, trade_date=None, start_date=None,
                                 end_date=None, con_code=None, all_funds=False,
                                 smart_incremental=False, progress_callback=None) -> int:
        return await self._update_etf_series(
            data_type="etf_sz_cons", method_name="get_etf_sz_cons",
            insert_method_name="insert_etf_sz_cons_batch",
            latest_method_name="get_latest_etf_sz_cons_trade_date",
            fund_codes=fund_codes, trade_date=trade_date, start_date=start_date,
            end_date=end_date, all_funds=all_funds, smart_incremental=smart_incremental,
            full_by_codes=True, always_by_codes=True, catalog_exchange="SZ",
            extra_params={"con_code": con_code}, progress_callback=progress_callback,
        )

    async def update_idx_anns(self, ann_date=None, start_date=None, end_date=None,
                              src=None, all_data=False, smart_incremental=False,
                              progress_callback=None) -> int:
        """按自然月窗口同步指数公告，并从最新公告日回看七天。"""
        if ann_date:
            data = self.router.route(
                asset_class="fund", data_type="idx_anns", method_name="get_idx_anns",
                ann_date=ann_date, src=src,
            )
            return await self.data_ops.insert_idx_anns_batch(data) if data is not None else 0
        resolved_end = pd.Timestamp(end_date or datetime.now()).normalize()
        if start_date:
            resolved_start = pd.Timestamp(start_date).normalize()
        elif smart_incremental:
            latest = await self.data_ops.get_latest_idx_anns_ann_date()
            resolved_start = (
                pd.Timestamp(latest) - pd.Timedelta(days=7)
                if latest else pd.Timestamp(await self._resolve_idx_anns_history_start())
            )
        else:
            resolved_start = pd.Timestamp(await self._resolve_idx_anns_history_start())
        if resolved_start > resolved_end:
            return 0
        windows = []
        cursor = resolved_start
        while cursor <= resolved_end:
            window_end = min(cursor + pd.offsets.MonthEnd(0), resolved_end)
            windows.append((cursor, window_end))
            cursor = window_end + pd.Timedelta(days=1)
        total = 0
        if progress_callback:
            progress_callback(0, len(windows))
        for index, (window_start, window_end) in enumerate(windows, start=1):
            data = self.router.route(
                asset_class="fund", data_type="idx_anns", method_name="get_idx_anns",
                start_date=window_start.strftime("%Y-%m-%d"),
                end_date=window_end.strftime("%Y-%m-%d"), src=src,
            )
            if data is not None and not data.empty:
                total += await self.data_ops.insert_idx_anns_batch(data)
            if progress_callback:
                progress_callback(index, len(windows))
        return total

    async def _resolve_idx_anns_history_start(self) -> str:
        return (
            await self.data_ops.get_earliest_trade_cal_date(
                exchange="SSE", start_date=IDX_ANNS_HISTORY_START
            ) or IDX_ANNS_HISTORY_START
        )

    async def update_fund_share(
        self, ts_code: Optional[str] = None, trade_date: Optional[str] = None,
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        market: Optional[str] = None,
        all_funds: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """同步 Tushare 基金规模数据。

        ``all_funds=True`` 时按交易日请求全市场基金规模，避免逐基金代码
        下载。Provider 会在单个交易日返回满 2,000 条记录时自动按 offset
        继续分页。
        """
        if all_funds:
            if ts_code:
                raise ValueError("fund_share 全量模式不能同时指定 ts_code")
            if trade_date and (start_date or end_date):
                raise ValueError(
                    "fund_share 全量模式中 --trade-date 不能与 --start-date/--end-date 同时使用"
                )

            if trade_date:
                resolved_start_date = trade_date
                resolved_end_date = trade_date
            else:
                resolved_start_date = (
                    start_date or await self.data_ops.get_earliest_fund_basic_date()
                )
                resolved_end_date = end_date or datetime.now().strftime("%Y-%m-%d")

            if not resolved_start_date:
                raise ValueError(
                    "fund_share 全量下载需要本地 fund_basic 基金目录；"
                    "请先执行 `fdh-cli update --dataset fund_basic --symbols all`"
                )

            resolved_start_date = pd.Timestamp(resolved_start_date).strftime("%Y-%m-%d")
            resolved_end_date = pd.Timestamp(resolved_end_date).strftime("%Y-%m-%d")
            if resolved_start_date > resolved_end_date:
                raise ValueError("fund_share 的开始日期不能晚于结束日期")

            return await self._update_fund_share_by_dates(
                start_date=resolved_start_date,
                end_date=resolved_end_date,
                market=market,
                progress_callback=progress_callback,
            )

        data = self.router.route(
            asset_class="fund", data_type="share", method_name="get_fund_share",
            ts_code=ts_code, trade_date=trade_date, start_date=start_date,
            end_date=end_date, market=market,
        )
        return await self.data_ops.insert_fund_share_batch(data) if data is not None else 0

    async def _resolve_fund_share_dates(
        self,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """返回完整可用的基金规模交易日，优先使用本地 SSE 交易日历。"""
        trade_cal = await self.data_ops.get_trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
        )
        if trade_cal is not None and not trade_cal.empty and {
            "cal_date", "is_open"
        }.issubset(trade_cal.columns):
            calendar_dates = pd.to_datetime(trade_cal["cal_date"], errors="coerce").dropna()
            if not calendar_dates.empty:
                calendar_start = calendar_dates.min().strftime("%Y-%m-%d")
                calendar_end = calendar_dates.max().strftime("%Y-%m-%d")
                if calendar_start <= start_date and calendar_end >= end_date:
                    open_dates = pd.to_datetime(
                        trade_cal.loc[trade_cal["is_open"] == 1, "cal_date"],
                        errors="coerce",
                    ).dropna()
                    resolved_dates = (
                        open_dates.dt.strftime("%Y-%m-%d").drop_duplicates().tolist()
                    )
                    if resolved_dates:
                        return resolved_dates

        logger.warning(
            "SSE trade_cal does not fully cover fund_share range {} to {}; "
            "falling back to weekdays",
            start_date,
            end_date,
        )
        return pd.bdate_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()

    async def _update_fund_share_by_dates(
        self,
        start_date: str,
        end_date: str,
        market: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """按交易日从历史到最新拉取基金规模全量，并逐日写入本地库。"""
        trade_dates = await self._resolve_fund_share_dates(start_date, end_date)
        if not trade_dates:
            logger.info("No fund_share dates found between {} and {}", start_date, end_date)
            return 0

        total = 0
        total_dates = len(trade_dates)
        logger.info(
            "Updating fund_share for {} trade dates (start={}, end={}, market={})",
            total_dates,
            start_date,
            end_date,
            market or "all",
        )
        if progress_callback:
            progress_callback(0, total_dates)

        for index, current_trade_date in enumerate(trade_dates, start=1):
            try:
                data = self.router.route(
                    asset_class="fund",
                    data_type="share",
                    method_name="get_fund_share",
                    trade_date=current_trade_date,
                    market=market,
                )
                if data is not None and not data.empty:
                    total += await self.data_ops.insert_fund_share_batch(data)
                else:
                    logger.debug("No fund_share data for {}", current_trade_date)
            except Exception:
                logger.exception("Failed to update fund_share for {}", current_trade_date)
                raise
            finally:
                if progress_callback:
                    progress_callback(index, total_dates)

        logger.info("Updated {} fund_share records across {} dates", total, total_dates)
        return total

    async def update_fund_nav(
        self, ts_code: Optional[str] = None, nav_date: Optional[str] = None,
        market: Optional[str] = None, start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        all_funds: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """同步 Tushare 公募基金净值数据。

        ``all_funds=True`` 时，按净值日而不是逐基金代码请求数据。起始日期
        默认取本地 ``fund_basic`` 目录中最早基金日期，显著减少全量下载的
        请求次数；每个净值日的 10,500 行分页由 Provider 处理。
        """
        if all_funds:
            if ts_code:
                raise ValueError("fund_nav 全量模式不能同时指定 ts_code")
            if nav_date and (start_date or end_date):
                raise ValueError(
                    "fund_nav 全量模式中 --trade-date 不能与 --start-date/--end-date 同时使用"
                )

            if nav_date:
                resolved_start_date = nav_date
                resolved_end_date = nav_date
            else:
                resolved_start_date = (
                    start_date or await self.data_ops.get_earliest_fund_basic_date()
                )
                resolved_end_date = end_date or datetime.now().strftime("%Y-%m-%d")

            if not resolved_start_date:
                raise ValueError(
                    "fund_nav 全量下载需要本地 fund_basic 基金目录；"
                    "请先执行 `fdh-cli update --dataset fund_basic --symbols all`"
                )

            resolved_start_date = pd.Timestamp(resolved_start_date).strftime("%Y-%m-%d")
            resolved_end_date = pd.Timestamp(resolved_end_date).strftime("%Y-%m-%d")
            if resolved_start_date > resolved_end_date:
                raise ValueError("fund_nav 的开始日期不能晚于结束日期")

            return await self._update_fund_nav_by_dates(
                start_date=resolved_start_date,
                end_date=resolved_end_date,
                market=market,
                progress_callback=progress_callback,
            )

        data = self.router.route(
            asset_class="fund", data_type="nav", method_name="get_fund_nav",
            ts_code=ts_code, nav_date=nav_date, market=market,
            start_date=start_date, end_date=end_date,
        )
        return await self.data_ops.insert_fund_nav_batch(data) if data is not None else 0

    async def _resolve_fund_nav_dates(
        self,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """返回完整可用的净值日期列表，优先使用本地 SSE 交易日历。"""
        trade_cal = await self.data_ops.get_trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
        )
        if trade_cal is not None and not trade_cal.empty and {
            "cal_date", "is_open"
        }.issubset(trade_cal.columns):
            calendar_dates = pd.to_datetime(trade_cal["cal_date"], errors="coerce").dropna()
            if not calendar_dates.empty:
                calendar_start = calendar_dates.min().strftime("%Y-%m-%d")
                calendar_end = calendar_dates.max().strftime("%Y-%m-%d")
                if calendar_start <= start_date and calendar_end >= end_date:
                    open_dates = pd.to_datetime(
                        trade_cal.loc[trade_cal["is_open"] == 1, "cal_date"],
                        errors="coerce",
                    ).dropna()
                    resolved_dates = (
                        open_dates.dt.strftime("%Y-%m-%d").drop_duplicates().tolist()
                    )
                    if resolved_dates:
                        return resolved_dates

        logger.warning(
            "SSE trade_cal does not fully cover fund_nav range {} to {}; "
            "falling back to weekdays",
            start_date,
            end_date,
        )
        return pd.bdate_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()

    async def _update_fund_nav_by_dates(
        self,
        start_date: str,
        end_date: str,
        market: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """按净值日拉取基金净值全量，并逐日写入本地库。"""
        nav_dates = await self._resolve_fund_nav_dates(start_date, end_date)
        if not nav_dates:
            logger.info("No fund_nav dates found between {} and {}", start_date, end_date)
            return 0

        total = 0
        total_dates = len(nav_dates)
        logger.info(
            "Updating fund_nav for {} net-value dates (start={}, end={}, market={})",
            total_dates,
            start_date,
            end_date,
            market or "all",
        )
        if progress_callback:
            progress_callback(0, total_dates)

        for index, current_nav_date in enumerate(nav_dates, start=1):
            try:
                data = self.router.route(
                    asset_class="fund",
                    data_type="nav",
                    method_name="get_fund_nav",
                    nav_date=current_nav_date,
                    market=market,
                )
                if data is not None and not data.empty:
                    total += await self.data_ops.insert_fund_nav_batch(data)
                else:
                    logger.debug("No fund_nav data for {}", current_nav_date)
            except Exception:
                logger.exception("Failed to update fund_nav for {}", current_nav_date)
                raise
            finally:
                if progress_callback:
                    progress_callback(index, total_dates)

        logger.info("Updated {} fund_nav records across {} dates", total, total_dates)
        return total

    async def update_fund_div(
        self, ts_code: Optional[str] = None, ann_date: Optional[str] = None,
        ex_date: Optional[str] = None, pay_date: Optional[str] = None,
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        all_funds: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """同步 Tushare 公募基金分红数据。

        ``all_funds=True`` 时按公告日逐日下载全市场分红数据。公告可能发生在
        非交易日，故全量范围覆盖每一个自然日；每个公告日的 1,000 行分页由
        Provider 处理。
        """
        if all_funds:
            if ts_code or ex_date or pay_date:
                raise ValueError(
                    "fund_div 全量模式不能同时指定 ts_code、ex_date 或 pay_date"
                )
            if ann_date and (start_date or end_date):
                raise ValueError(
                    "fund_div 全量模式中 ann_date 不能与 start_date/end_date 同时使用"
                )

            if ann_date:
                resolved_start_date = ann_date
                resolved_end_date = ann_date
            else:
                resolved_start_date = (
                    start_date or await self.data_ops.get_earliest_fund_basic_date()
                )
                resolved_end_date = end_date or datetime.now().strftime("%Y-%m-%d")

            if not resolved_start_date:
                raise ValueError(
                    "fund_div 全量下载需要本地 fund_basic 基金目录；"
                    "请先执行 `fdh-cli update --dataset fund_basic --symbols all`"
                )

            resolved_start_date = pd.Timestamp(resolved_start_date).strftime("%Y-%m-%d")
            resolved_end_date = pd.Timestamp(resolved_end_date).strftime("%Y-%m-%d")
            if resolved_start_date > resolved_end_date:
                raise ValueError("fund_div 的开始日期不能晚于结束日期")

            return await self._update_fund_div_by_dates(
                start_date=resolved_start_date,
                end_date=resolved_end_date,
                progress_callback=progress_callback,
            )

        data = self.router.route(
            asset_class="fund", data_type="div", method_name="get_fund_div",
            ts_code=ts_code, ann_date=ann_date, ex_date=ex_date, pay_date=pay_date,
        )
        return await self.data_ops.insert_fund_div_batch(data) if data is not None else 0

    async def _update_fund_div_by_dates(
        self,
        start_date: str,
        end_date: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """按公告日拉取全市场基金分红，并逐日写入本地库。"""
        ann_dates = pd.date_range(
            start=start_date,
            end=end_date,
            freq="D",
        ).strftime("%Y-%m-%d").tolist()
        if not ann_dates:
            logger.info("No fund_div dates found between {} and {}", start_date, end_date)
            return 0

        total = 0
        total_dates = len(ann_dates)
        logger.info(
            "Updating fund_div for {} announcement dates (start={}, end={})",
            total_dates,
            start_date,
            end_date,
        )
        if progress_callback:
            progress_callback(0, total_dates)

        for index, current_ann_date in enumerate(ann_dates, start=1):
            try:
                data = self.router.route(
                    asset_class="fund",
                    data_type="div",
                    method_name="get_fund_div",
                    ann_date=current_ann_date,
                )
                if data is not None and not data.empty:
                    total += await self.data_ops.insert_fund_div_batch(data)
                else:
                    logger.debug("No fund_div data for {}", current_ann_date)
            except Exception:
                logger.exception("Failed to update fund_div for {}", current_ann_date)
                raise
            finally:
                if progress_callback:
                    progress_callback(index, total_dates)

        logger.info("Updated {} fund_div records across {} dates", total, total_dates)
        return total

    async def update_fund_company(self) -> int:
        """刷新 Tushare 公募基金管理人目录（全量、非时间序列）。"""
        try:
            data = self.router.route(
                asset_class="fund",
                data_type="company",
                method_name="get_fund_company",
            )
            if data is None or data.empty:
                logger.warning("No fund_company data received")
                return 0
            inserted_count = await self.data_ops.insert_fund_company_batch(data)
            logger.info(f"Updated {inserted_count} fund_company records")
            return inserted_count
        except Exception:
            logger.exception("Failed to update fund_company")
            raise

    async def update_fund_manager(
        self,
        fund_codes: Optional[List[str]] = None,
        ann_date: Optional[str] = None,
        name: Optional[str] = None,
    ) -> int:
        """刷新基金经理任职与简历数据；未指定基金代码时全量分页同步。"""
        ts_code = ",".join(fund_codes) if fund_codes else None
        try:
            data = self.router.route(
                asset_class="fund",
                data_type="manager",
                method_name="get_fund_manager",
                ts_code=ts_code,
                ann_date=ann_date,
                name=name,
            )
            if data is None or data.empty:
                logger.warning("No fund_manager data received")
                return 0
            inserted_count = await self.data_ops.insert_fund_manager_batch(data)
            logger.info(f"Updated {inserted_count} fund_manager records")
            return inserted_count
        except Exception:
            logger.exception("Failed to update fund_manager")
            raise

    async def update_mkt_idx_bmk(
        self,
        ts_code: Optional[str] = None,
        bmk_type: Optional[str] = None,
        bmk_level: Optional[str] = None,
    ) -> int:
        """刷新 Tushare ETF 业绩比较基准库。"""
        try:
            data = self.router.route(
                asset_class="fund",
                data_type="mkt_idx_bmk",
                method_name="get_mkt_idx_bmk",
                ts_code=ts_code,
                bmk_type=bmk_type,
                bmk_level=bmk_level,
            )
            if data is None or data.empty:
                logger.warning("No mkt_idx_bmk data received")
                return 0
            return await self.data_ops.insert_mkt_idx_bmk_batch(data)
        except Exception:
            logger.exception("Failed to update mkt_idx_bmk")
            raise

    async def update_fund_portfolio(
        self,
        fund_codes: Optional[List[str]] = None,
        symbol: Optional[str] = None,
        ann_date: Optional[str] = None,
        period: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        all_funds: bool = False,
        smart_incremental: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """同步公募基金持仓。

        全量及智能增量模式按 ``ann_date`` 的自然日请求全市场数据。不能按
        ``period`` 的季度末分批，因为早期数据存在非季末报告截止日。每次
        请求的 8,000 行分页由 Provider 负责处理。
        """
        if all_funds or smart_incremental:
            if fund_codes or symbol or ann_date or period:
                raise ValueError(
                    "fund_portfolio 全市场按公告日模式不能同时指定基金代码、股票代码、公告日或报告期"
                )
            if all_funds and smart_incremental:
                raise ValueError("fund_portfolio 不能同时使用全量和智能增量模式")

            resolved_end_date = pd.Timestamp(
                end_date or datetime.now().strftime("%Y-%m-%d")
            ).strftime("%Y-%m-%d")
            if start_date:
                resolved_start_date = pd.Timestamp(start_date).strftime("%Y-%m-%d")
            elif smart_incremental:
                # The checkpoint is inclusive: re-fetching the latest ann_date
                # is cheap and also absorbs a same-day Tushare correction.
                resolved_start_date = (
                    await self.data_ops.get_latest_fund_portfolio_ann_date()
                )
                if not resolved_start_date:
                    resolved_start_date = await self._resolve_fund_portfolio_history_start()
            else:
                resolved_start_date = await self._resolve_fund_portfolio_history_start()

            resolved_start_date = pd.Timestamp(resolved_start_date).strftime("%Y-%m-%d")
            if resolved_start_date > resolved_end_date:
                logger.info(
                    "fund_portfolio is already current (start={} > end={})",
                    resolved_start_date,
                    resolved_end_date,
                )
                return 0
            return await self._update_fund_portfolio_by_ann_dates(
                start_date=resolved_start_date,
                end_date=resolved_end_date,
                progress_callback=progress_callback,
            )

        if not any((fund_codes, ann_date, period)):
            raise ValueError("fund_portfolio 需要 --symbols、--trade-date 或报告期参数")
        total = 0
        codes = fund_codes or [None]
        for ts_code in codes:
            try:
                data = self.router.route(
                    asset_class="fund",
                    data_type="portfolio",
                    method_name="get_fund_portfolio",
                    ts_code=ts_code,
                    symbol=symbol,
                    ann_date=ann_date,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                )
                if data is not None and not data.empty:
                    total += await self.data_ops.insert_fund_portfolio_batch(data)
            except Exception:
                logger.exception("Failed to update fund_portfolio for {}", ts_code or period)
                raise
        logger.info("Updated {} fund_portfolio records", total)
        return total

    async def _resolve_fund_portfolio_history_start(self) -> str:
        """Resolve the first full-download date from the local SSE calendar."""
        earliest_calendar_date = await self.data_ops.get_earliest_trade_cal_date(
            exchange="SSE",
            start_date=FUND_PORTFOLIO_HISTORY_START,
        )
        if earliest_calendar_date:
            return earliest_calendar_date
        logger.warning(
            "SSE trade_cal is unavailable for fund_portfolio; falling back to {}",
            FUND_PORTFOLIO_HISTORY_START,
        )
        return FUND_PORTFOLIO_HISTORY_START

    async def _update_fund_portfolio_by_ann_dates(
        self,
        start_date: str,
        end_date: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Download all holdings by natural announcement date and upsert daily."""
        ann_dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime(
            "%Y-%m-%d"
        ).tolist()
        if not ann_dates:
            return 0

        total = 0
        total_dates = len(ann_dates)
        logger.info(
            "Updating fund_portfolio for {} announcement dates (start={}, end={})",
            total_dates,
            start_date,
            end_date,
        )
        if progress_callback:
            progress_callback(0, total_dates)

        for index, current_ann_date in enumerate(ann_dates, start=1):
            try:
                data = self.router.route(
                    asset_class="fund",
                    data_type="portfolio",
                    method_name="get_fund_portfolio",
                    ann_date=current_ann_date,
                )
                if data is not None and not data.empty:
                    total += await self.data_ops.insert_fund_portfolio_batch(data)
                else:
                    logger.debug("No fund_portfolio data for {}", current_ann_date)
            except Exception:
                logger.exception("Failed to update fund_portfolio for {}", current_ann_date)
                raise
            finally:
                if progress_callback:
                    progress_callback(index, total_dates)

        logger.info("Updated {} fund_portfolio records across {} dates", total, total_dates)
        return total

    @staticmethod
    def _filter_index_daily_to_catalog(
        data: pd.DataFrame,
        ts_code_list: List[str],
    ) -> pd.DataFrame:
        """Keep only index_daily rows that belong to the selected local catalog."""
        if data is None or data.empty:
            return data
        selected_codes = {str(code).strip() for code in ts_code_list if str(code).strip()}
        if not selected_codes or "ts_code" not in data.columns:
            return data.iloc[0:0].copy()
        return data[data["ts_code"].astype(str).isin(selected_codes)].reset_index(drop=True)

    async def _resolve_index_daily_batch_start_date(
        self,
        start_date: Optional[str],
        end_date: str,
        force_update: bool,
    ) -> Optional[str]:
        """Resolve the date-batch start for full-catalog index_daily updates."""
        if start_date:
            return start_date

        if force_update:
            return await self.data_ops.get_earliest_index_basic_list_date(
                exclude_markets=sorted(INDEX_DAILY_EXCLUDED_CATALOG_MARKETS),
            )

        latest_date = await self.data_ops.get_latest_index_daily_date()
        if latest_date:
            next_day = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
            return next_day.strftime("%Y-%m-%d")

        return await self.data_ops.get_earliest_index_basic_list_date(
            exclude_markets=sorted(INDEX_DAILY_EXCLUDED_CATALOG_MARKETS),
            active_date=end_date,
        )

    async def _update_index_daily_for_trade_dates(
        self,
        ts_code_list: List[str],
        trade_dates: List[str],
        progress_callback: Optional[callable] = None,
    ) -> int:
        """Fetch index_daily by trade_date and filter to the local index catalog."""
        total_records = 0
        total_dates = len(trade_dates)

        for idx, trade_date in enumerate(trade_dates):
            try:
                data = self.router.route(
                    asset_class="index",
                    data_type="daily",
                    method_name="get_index_daily",
                    trade_date=trade_date,
                )
                data = self._filter_index_daily_to_catalog(data, ts_code_list)

                if data is not None and not data.empty:
                    inserted = await self.data_ops.insert_index_daily_batch(data)
                    total_records += inserted
                    logger.info(f"Inserted {inserted} index_daily records for {trade_date}")
                else:
                    logger.debug(f"No index_daily data for {trade_date}")

                if progress_callback:
                    progress_callback(idx + 1, total_dates)
            except Exception as e:
                logger.error(f"Failed to fetch index_daily for {trade_date}: {str(e)}")
                if progress_callback:
                    progress_callback(idx + 1, total_dates)
                continue

        return total_records

    async def _update_index_daily_by_trade_date_range(
        self,
        ts_code_list: List[str],
        start_date: str,
        end_date: str,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """Fetch index_daily by each local SSE trading day in a date range."""
        if start_date > end_date:
            logger.info("index_daily is already up to date")
            return 0

        trade_dates = await self.data_ops.get_trade_dates(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
        )
        if not trade_dates:
            logger.info(
                f"No open SSE trading days found for index_daily range {start_date} to {end_date}"
            )
            return 0

        logger.info(
            f"Updating index_daily by {len(trade_dates)} trade dates "
            f"(start={start_date}, end={end_date})"
        )
        return await self._update_index_daily_for_trade_dates(
            ts_code_list=ts_code_list,
            trade_dates=trade_dates,
            progress_callback=progress_callback,
        )

    @staticmethod
    def _expand_hk_adj_factor_to_daily(
        daily_df: pd.DataFrame,
        sparse_adj_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Expand sparse HK factor-change rows to daily rows using local trading dates."""
        if daily_df is None or daily_df.empty or sparse_adj_df is None or sparse_adj_df.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        if "time" not in daily_df.columns or "time" not in sparse_adj_df.columns:
            logger.warning("HK adj_factor expansion requires time columns on both inputs")
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        calendar = daily_df[["symbol", "time"]].copy().sort_values("time").reset_index(
            drop=True
        )
        factors = sparse_adj_df[["time", "adj_factor"]].copy().sort_values(
            "time"
        ).reset_index(drop=True)

        calendar["trade_date"] = pd.to_datetime(calendar["time"], errors="coerce")
        factors["trade_date"] = pd.to_datetime(factors["time"], errors="coerce")

        for df in (calendar, factors):
            if isinstance(df["trade_date"].dtype, pd.DatetimeTZDtype):
                df["trade_date"] = (
                    df["trade_date"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
                )

        calendar["trade_date"] = calendar["trade_date"].dt.normalize()
        factors["trade_date"] = factors["trade_date"].dt.normalize()
        calendar["trade_date"] = pd.to_datetime(
            calendar["trade_date"], errors="coerce"
        ).astype("datetime64[ns]")
        factors["trade_date"] = pd.to_datetime(
            factors["trade_date"], errors="coerce"
        ).astype("datetime64[ns]")
        factors["adj_factor"] = pd.to_numeric(factors["adj_factor"], errors="coerce")
        factors = factors.dropna(subset=["trade_date", "adj_factor"])

        if factors.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        merged = pd.merge_asof(
            calendar.sort_values("trade_date"),
            factors[["trade_date", "adj_factor"]].sort_values("trade_date"),
            on="trade_date",
            direction="backward",
        )
        merged["adj_factor"] = (
            pd.to_numeric(merged["adj_factor"], errors="coerce").ffill().fillna(1.0)
        )

        return (
            merged[["symbol", "time", "adj_factor"]]
            .drop_duplicates(subset=["symbol", "time"])
            .sort_values("time")
            .reset_index(drop=True)
        )

    async def _get_hk_adj_factor_from_akshare(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """Fetch HK sparse factors from AKShare and align them to local daily dates."""
        daily_df = await self.data_ops.get_symbol_daily_for_adj_factor(
            symbol,
            start_date=start_date,
            end_date=end_date,
            market="HK",
        )
        if daily_df.empty:
            logger.warning(f"No local HK daily rows available for adj_factor expansion: {symbol}")
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        sparse_adj_df = self.router.route(
            asset_class="stock",
            data_type="adj_factor",
            method_name="get_adj_factor",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            market="HK",
        )
        if sparse_adj_df is None or sparse_adj_df.empty:
            return pd.DataFrame(columns=["symbol", "time", "adj_factor"])

        return self._expand_hk_adj_factor_to_daily(daily_df, sparse_adj_df)

    async def update_stock_basic(self, market: Optional[str] = None) -> int:
        """
        更新股票基本信息

        Args:
            market: 宽市场代码（CN/HK/ALL），默认 CN

        Returns:
            int: 更新的记录数
        """
        markets = self._normalize_update_markets(market)
        if len(markets) > 1:
            total = 0
            for market_code in markets:
                total += await self.update_stock_basic(market=market_code)
            return total

        market_code = markets[0]
        logger.info(f"Updating stock basic info (market={market_code})")

        try:
            # 从路由器获取数据
            data = self.router.route(
                asset_class="stock",
                data_type="basic",
                method_name="get_stock_basic",
                market=market_code,
                list_status="L",
            )

            if data is None or data.empty:
                logger.warning("No stock basic data received")
                return 0

            # 插入数据库
            inserted_count = await self.data_ops.insert_asset_basic_batch(data)

            logger.info(f"Updated {inserted_count} stock basic records")
            return inserted_count

        except Exception as e:
            logger.exception("Failed to update stock basic")
            raise

    async def update_daily_data(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adj: Optional[str] = None,
        trade_date: Optional[str] = None,
        force_update: bool = False,
        market: Optional[str] = None,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新日线数据

        支持的调用模式：
        1. 指定 trade_date：批量获取指定交易日所有股票数据
        2. 指定 symbols/日期范围：逐个股票获取历史数据
        3. 智能下载模式（默认）：自动增量更新

        Args:
            symbols: 股票代码列表，None表示全部
            start_date: 开始日期，None表示智能下载（查询数据库自动计算）
            end_date: 结束日期，为None时使用今天
            adj: 复权类型
            trade_date: 交易日期（YYYY-MM-DD格式），批量获取当日所有股票数据
            force_update: 是否强制更新（忽略数据库状态）
            market: 宽市场代码（CN/HK/ALL），默认 CN
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数

        Raises:
            ValueError: 当 trade_date 与 start_date 或 end_date 同时指定时
        """
        markets = self._normalize_update_markets(market)
        if len(markets) > 1:
            total = 0
            for market_code in markets:
                market_symbols = self._symbols_for_market(symbols, market_code)
                if symbols is not None and not market_symbols:
                    continue
                total += await self.update_daily_data(
                    symbols=market_symbols,
                    start_date=start_date,
                    end_date=end_date,
                    adj=adj,
                    trade_date=trade_date,
                    force_update=force_update,
                    market=market_code,
                    progress_callback=progress_callback,
                )
            return total

        market_code = markets[0]

        # 参数互斥检查
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date cannot be used with start_date or end_date")

        if trade_date and market_code == "HK":
            logger.info(
                f"HK trade_date mode: fetching per-symbol daily data for {trade_date}"
            )
            return await self.update_daily_data(
                symbols=symbols,
                start_date=trade_date,
                end_date=trade_date,
                adj=adj,
                force_update=True,
                market=market_code,
                progress_callback=progress_callback,
            )

        # 交易日模式：批量获取当日所有股票数据
        if trade_date:
            logger.info(f"Trade date mode: fetching all {market_code} stocks for {trade_date}")

            # 转换日期格式（CLI传入 YYYY-MM-DD，API需要 YYYYMMDD）
            trade_date_api = trade_date.replace("-", "")

            data = self.router.route(
                asset_class="stock",
                data_type="daily",
                method_name="get_daily_data",
                symbol=None,  # 获取所有股票
                trade_date=trade_date_api,
                adj=adj,
                market=market_code,
            )

            if data is None or data.empty:
                logger.warning(f"No daily data received for trade_date={trade_date}")
                return 0

            inserted_count = await self.data_ops.insert_symbol_daily_batch(data, batch_size=1000)
            logger.info(f"Updated {inserted_count} daily records for trade_date={trade_date}")
            return inserted_count

        # 原有逻辑：按股票逐个获取
        if not symbols:
            # 如果没有指定股票，获取所有股票
            symbols = await self.data_ops.get_symbol_list(market=market_code)

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating daily data for {len(symbols)} symbols "
            f"from {start_date} to {end_date} "
            f"(adj={adj}, market={market_code}, force={force_update})"
        )

        total_records = 0
        single_symbol_request = len(symbols) == 1

        for idx, symbol in enumerate(symbols):
            try:
                # 智能下载逻辑：确定该symbol的实际起始日期
                symbol_start_date = start_date

                if not force_update and not start_date:
                    # 查询数据库最新记录
                    latest_date = await self.data_ops.get_latest_data_date(
                        symbol, "symbol_daily"
                    )

                    if latest_date:
                        # 有记录，计算下一个交易日
                        next_day = latest_date + timedelta(days=1)
                        symbol_start_date = next_day.strftime("%Y-%m-%d")
                        if symbol_start_date > end_date:
                            logger.debug(f"Skipping {symbol} - already up to date")
                            continue
                        logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                    else:
                        # 新symbol，智能下载模式：传None让API获取全量数据
                        symbol_start_date = None
                        logger.info(f"Smart download: {symbol} - fetching full history")
                elif force_update:
                    # 强制更新模式：使用提供的日期范围或全量
                    logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                # 从路由器获取数据
                data = self.router.route(
                    asset_class="stock",
                    data_type="daily",
                    method_name="get_daily_data",
                    symbol=symbol,
                    start_date=symbol_start_date,
                    end_date=end_date,
                    adj=adj,
                    market=market_code,
                )

                if data is not None and not data.empty:
                    # 插入数据库
                    inserted = await self.data_ops.insert_symbol_daily_batch(
                        data, batch_size=1000
                    )
                    total_records += inserted
                else:
                    logger.debug(f"No data for {symbol}")

            except Exception as e:
                logger.error(f"Failed to update {symbol}: {type(e).__name__}: {str(e)}")
                logger.exception("Traceback:")
                if single_symbol_request:
                    raise
                continue
            finally:
                if progress_callback:
                    progress_callback(idx + 1, len(symbols))

        logger.info(f"Updated total {total_records} daily records")
        return total_records

    async def update_minute_data(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = "1m",
        force_update: bool = False,
        market: Optional[str] = None,
    ) -> int:
        """
        更新分钟数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期，None表示智能下载
            end_date: 结束日期
            freq: 频率
            force_update: 是否强制更新（忽略数据库状态）
            market: 宽市场代码（CN/HK/ALL），默认 CN

        Returns:
            int: 更新的记录数
        """
        markets = self._normalize_update_markets(market)
        if len(markets) > 1:
            total = 0
            for market_code in markets:
                market_symbols = self._symbols_for_market(symbols, market_code)
                if symbols is not None and not market_symbols:
                    continue
                total += await self.update_minute_data(
                    symbols=market_symbols,
                    start_date=start_date,
                    end_date=end_date,
                    freq=freq,
                    force_update=force_update,
                    market=market_code,
                )
            return total

        market_code = markets[0]

        if not symbols:
            # 限制股票数量（分钟数据量很大）
            symbols = await self.data_ops.get_symbol_list(market=market_code, limit=10)

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating {freq} data for {len(symbols)} symbols "
            f"from {start_date or 'smart'} to {end_date} "
            f"(market={market_code}, force={force_update})"
        )

        total_records = 0

        for symbol in symbols:
            try:
                # 智能下载逻辑：确定该symbol的实际起始日期
                symbol_start_date = start_date

                if not force_update and not start_date:
                    # 查询数据库最新记录（分钟级别）
                    table_name = f"symbol_minute_{freq}"
                    latest_date = await self.data_ops.get_latest_data_date(
                        symbol, table_name
                    )

                    if latest_date:
                        # 有记录，计算下一分钟
                        next_minute = latest_date + timedelta(minutes=1)
                        # 转换为日期字符串（分钟数据使用datetime）
                        symbol_start_date = next_minute.strftime("%Y-%m-%d %H:%M:%S")
                        logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                    else:
                        # 新symbol，智能下载模式：传None让API获取全量数据
                        symbol_start_date = None
                        logger.info(f"Smart download: {symbol} - fetching full history")
                elif force_update:
                    # 强制更新模式：使用提供的日期范围或全量
                    logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                # 从路由器获取数据
                data = self.router.route(
                    asset_class="stock",
                    data_type="minute",
                    freq=freq,
                    method_name="get_minute_data",
                    symbol=symbol,
                    start_date=symbol_start_date,
                    end_date=end_date,
                    market=market_code,
                )

                if data is not None and not data.empty:
                    inserted = await self.data_ops.insert_symbol_minute_batch(
                        data, batch_size=1000, freq=freq
                    )
                    total_records += inserted
                else:
                    logger.debug(f"No {freq} data for {symbol}")

            except Exception as e:
                logger.error(f"Failed to update {freq} data for {symbol}: {str(e)}")
                continue

        logger.info(f"Updated total {total_records} {freq} records")
        return total_records

    async def update_daily_basic(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_date: Optional[str] = None,
        force_update: bool = False,
        market: Optional[str] = None,
    ) -> int:
        """
        更新每日指标数据

        支持的调用模式：
        1. 指定 trade_date：批量获取指定交易日所有股票数据
        2. 指定 symbols/日期范围：逐个股票获取历史数据
        3. 智能下载模式（默认）：自动增量更新

        Args:
            symbols: 股票代码列表
            start_date: 开始日期，None表示智能下载
            end_date: 结束日期
            trade_date: 交易日期（YYYY-MM-DD格式），批量获取当日所有股票数据
            force_update: 是否强制更新（忽略数据库状态）
            market: 宽市场代码（CN/HK/ALL），默认 CN

        Returns:
            int: 更新的记录数

        Raises:
            ValueError: 当 trade_date 与 start_date 或 end_date 同时指定时
        """
        market_code = self._normalize_update_markets(market)[0]
        if market_code != "CN":
            logger.warning(f"daily_basic is only supported for CN in v1 (market={market_code})")
            return 0

        # 参数互斥检查
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date cannot be used with start_date or end_date")

        # 交易日模式：批量获取当日所有股票数据
        if trade_date:
            logger.info(f"Trade date mode: fetching all stocks for {trade_date}")

            # 转换日期格式（CLI传入 YYYY-MM-DD，API需要 YYYYMMDD）
            trade_date_api = trade_date.replace("-", "")

            data = self.router.route(
                asset_class="stock",
                data_type="daily_basic",
                method_name="get_daily_basic",
                symbol=None,  # 获取所有股票
                trade_date=trade_date_api,
                market=market_code,
            )

            if data is None or data.empty:
                logger.warning(f"No daily_basic data received for trade_date={trade_date}")
                return 0

            inserted_count = await self.data_ops.insert_daily_basic_batch(data, batch_size=1000)
            logger.info(f"Updated {inserted_count} daily_basic records for trade_date={trade_date}")
            return inserted_count

        # 原有逻辑：按股票逐个获取
        if not symbols:
            symbols = await self.data_ops.get_symbol_list(market=market_code)

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating daily basic for {len(symbols)} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        total_records = 0

        for symbol in symbols:
            try:
                # 智能下载逻辑：确定该symbol的实际起始日期
                symbol_start_date = start_date

                if not force_update and not start_date:
                    # 查询数据库最新记录
                    latest_date = await self.data_ops.get_latest_data_date(
                        symbol, "daily_basic"
                    )

                    if latest_date:
                        # 有记录，计算下一个交易日
                        next_day = latest_date + timedelta(days=1)
                        symbol_start_date = next_day.strftime("%Y-%m-%d")
                        logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                    else:
                        # 新symbol，智能下载模式：传None让API获取全量数据
                        symbol_start_date = None
                        logger.info(f"Smart download: {symbol} - fetching full history")
                elif force_update:
                    # 强制更新模式：使用提供的日期范围或全量
                    logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                # 批量获取数据（Tushare支持批量）
                symbols_chunk = [symbol]

                data = self.router.route(
                    asset_class="stock",
                    data_type="daily_basic",
                    method_name="get_daily_basic",
                    symbol=symbol,
                    start_date=symbol_start_date,
                    end_date=end_date,
                    market=market_code,
                )

                if data is not None and not data.empty:
                    inserted = await self.data_ops.insert_daily_basic_batch(
                        data, batch_size=1000
                    )
                    total_records += inserted
                else:
                    logger.debug(f"No daily basic data for {symbol}")

            except Exception as e:
                logger.error(f"Failed to update daily basic for {symbol}: {str(e)}")
                continue

        logger.info(f"Updated total {total_records} daily basic records")
        return total_records

    async def update_adj_factor(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_date: Optional[str] = None,
        force_update: bool = False,
        market: Optional[str] = None,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新复权因子数据

        支持的调用模式：
        1. 指定 trade_date：批量获取指定交易日所有股票复权因子
        2. 指定 symbols/日期范围：逐个股票获取历史数据
        3. 智能下载模式（默认）：自动增量更新

        Args:
            symbols: 股票代码列表，None表示全部
            start_date: 开始日期，None表示智能下载
            end_date: 结束日期
            trade_date: 交易日期（YYYY-MM-DD格式），批量获取当日所有股票复权因子
            force_update: 是否强制更新（忽略数据库状态）
            market: 宽市场代码（CN/HK/ALL），默认 CN
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数

        Raises:
            ValueError: 当 trade_date 与 start_date 或 end_date 同时指定时
        """
        markets = self._normalize_update_markets(market)
        if len(markets) > 1:
            total = 0
            for market_code in markets:
                market_symbols = self._symbols_for_market(symbols, market_code)
                if symbols is not None and not market_symbols:
                    continue
                total += await self.update_adj_factor(
                    symbols=market_symbols,
                    start_date=start_date,
                    end_date=end_date,
                    trade_date=trade_date,
                    force_update=force_update,
                    market=market_code,
                    progress_callback=progress_callback,
                )
            return total

        market_code = markets[0]

        # 参数互斥检查
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date cannot be used with start_date or end_date")

        if trade_date and market_code == "HK":
            logger.info(
                f"HK trade_date mode: deriving per-symbol adj_factor for {trade_date}"
            )
            return await self.update_adj_factor(
                symbols=symbols,
                start_date=trade_date,
                end_date=trade_date,
                force_update=True,
                market=market_code,
                progress_callback=progress_callback,
            )

        # 交易日模式：批量获取当日所有股票复权因子
        if trade_date:
            logger.info(f"Trade date mode: fetching {market_code} adj_factor for {trade_date}")

            data = self.router.route(
                asset_class="stock",
                data_type="adj_factor",
                method_name="get_adj_factor",
                symbol=None,  # 获取所有股票
                trade_date=trade_date,
                market=market_code,
            )

            if data is None or data.empty:
                logger.warning(f"No adj_factor data received for trade_date={trade_date}")
                return 0

            inserted_count = await self.data_ops.insert_adj_factor_batch(data, batch_size=1000)
            logger.info(f"Updated {inserted_count} adj_factor records for trade_date={trade_date}")
            return inserted_count

        # 原有逻辑：按股票逐个获取
        if not symbols:
            # 如果没有指定股票，获取所有股票
            symbols = await self.data_ops.get_symbol_list(market=market_code)

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating adj_factor for {len(symbols)} symbols "
            f"from {start_date or 'smart'} to {end_date} "
            f"(market={market_code}, force={force_update})"
        )

        total_records = 0
        skipped_count = 0
        single_symbol_request = len(symbols) == 1

        for idx, symbol in enumerate(symbols):
            try:
                # 智能下载逻辑：确定该symbol的实际起始日期
                symbol_start_date = start_date

                if not force_update and not start_date:
                    # 获取该股票最新的复权因子日期
                    latest_date = await self.data_ops.get_latest_data_date(
                        symbol, "adj_factor"
                    )

                    if latest_date:
                        # 有记录，计算下一个交易日
                        next_day = latest_date + timedelta(days=1)
                        symbol_start_date = next_day.strftime("%Y-%m-%d")
                        if symbol_start_date > end_date:
                            logger.debug(f"Skipping {symbol} - already up to date")
                            skipped_count += 1
                            continue
                        logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                    else:
                        # 新symbol，智能下载模式：传None让API获取全量数据
                        symbol_start_date = None
                        logger.info(f"Smart download: {symbol} - fetching full history")
                elif force_update:
                    # 强制更新模式：使用提供的日期范围或全量
                    logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                # 从路由器获取数据
                if market_code == "HK":
                    data = await self._get_hk_adj_factor_from_akshare(
                        symbol,
                        start_date=symbol_start_date,
                        end_date=end_date,
                    )
                else:
                    data = self.router.route(
                        asset_class="stock",
                        data_type="adj_factor",
                        method_name="get_adj_factor",
                        symbol=symbol,
                        start_date=symbol_start_date,
                        end_date=end_date,
                        market=market_code,
                    )

                if data is not None and not data.empty:
                    # 插入数据库
                    inserted = await self.data_ops.insert_adj_factor_batch(
                        data, batch_size=1000
                    )
                    total_records += inserted
                    logger.info(f"Updated {inserted} adj_factor records for {symbol}")
                else:
                    logger.debug(f"No adj_factor data for {symbol}")

            except Exception as e:
                logger.error(f"Failed to update adj_factor for {symbol}: {str(e)}")
                logger.exception("Traceback:")
                if single_symbol_request:
                    raise
                continue
            finally:
                if progress_callback:
                    progress_callback(idx + 1, len(symbols))

        if skipped_count > 0:
            logger.info(
                f"Skipped {skipped_count} symbols - already up to date"
            )

        logger.info(f"Updated total {total_records} adj_factor records")
        return total_records

    async def update_gdp(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """
        更新中国GDP数据

        Args:
            start_date: 开始日期（季度末日期格式，如2020-03-31表示2020Q1），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating GDP data from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        try:
            # 智能下载逻辑：确定实际起始日期
            actual_start_date = start_date

            if not force_update and not start_date:
                # 查询数据库最新记录（cn_gdp表没有symbol列，使用专用方法）
                latest_date = await self.data_ops.get_latest_data_date_no_symbol("cn_gdp")

                if latest_date:
                    # 有记录，计算下一个季度第一天
                    next_quarter = latest_date + timedelta(days=1)
                    actual_start_date = next_quarter.strftime("%Y-%m-%d")
                    if actual_start_date > end_date:
                        logger.info("GDP data is already up to date")
                        return 0
                    logger.debug(f"Smart incremental: GDP from {actual_start_date}")
                else:
                    # 没有记录，智能下载模式：传None让API获取全量数据
                    actual_start_date = None
                    logger.info("Smart download: fetching full GDP history")

            # 从路由器获取GDP数据
            data = self.router.route(
                asset_class="macro",  # GDP属于宏观经济数据
                data_type="gdp",
                method_name="get_gdp_data",
                start_q=_convert_to_quarter_format(actual_start_date),
                end_q=_convert_to_quarter_format(end_date),
            )

            if data is None or data.empty:
                logger.warning("No GDP data received")
                return 0

            # 插入数据库
            inserted_count = await self.data_ops.insert_cn_gdp_batch(data)

            logger.info(f"Updated {inserted_count} GDP records")
            return inserted_count

        except Exception as e:
            logger.exception("Failed to update GDP data")
            raise

    async def update_ppi(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """
        更新中国PPI数据

        Args:
            start_date: 开始日期（月份末日期格式，如2020-01-31表示2020年1月），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating PPI data from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        try:
            # 智能下载逻辑：确定实际起始日期
            actual_start_date = start_date

            if not force_update and not start_date:
                # 查询数据库最新记录（cn_ppi表没有symbol列，使用专用方法）
                latest_date = await self.data_ops.get_latest_data_date_no_symbol("cn_ppi")

                if latest_date:
                    # 有记录，计算下一个月第一天
                    next_month = latest_date + timedelta(days=1)
                    actual_start_date = next_month.strftime("%Y-%m-%d")
                    if actual_start_date > end_date:
                        logger.info("PPI data is already up to date")
                        return 0
                    logger.debug(f"Smart incremental: PPI from {actual_start_date}")
                else:
                    # 没有记录，智能下载模式：传None让API获取全量数据
                    actual_start_date = None
                    logger.info("Smart download: fetching full PPI history")

            # 从路由器获取PPI数据
            data = self.router.route(
                asset_class="macro",  # PPI属于宏观经济数据
                data_type="ppi",
                method_name="get_ppi_data",
                start_m=_convert_to_month_format(actual_start_date),
                end_m=_convert_to_month_format(end_date),
            )

            if data is None or data.empty:
                logger.warning("No PPI data received")
                return 0

            # 插入数据库
            inserted_count = await self.data_ops.insert_cn_ppi_batch(data)

            logger.info(f"Updated {inserted_count} PPI records")
            return inserted_count

        except Exception as e:
            logger.exception("Failed to update PPI data")
            raise

    async def update_m(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """
        更新中国货币供应量数据

        Args:
            start_date: 开始日期（月份末日期格式，如2020-01-31表示2020年1月），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating M data from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        try:
            # 智能下载逻辑：确定实际起始日期
            actual_start_date = start_date

            if not force_update and not start_date:
                # 查询数据库最新记录（cn_m表没有symbol列，使用专用方法）
                latest_date = await self.data_ops.get_latest_data_date_no_symbol("cn_m")

                if latest_date:
                    # 有记录，计算下一个月第一天
                    next_month = latest_date + timedelta(days=1)
                    actual_start_date = next_month.strftime("%Y-%m-%d")
                    if actual_start_date > end_date:
                        logger.info("M data is already up to date")
                        return 0
                    logger.debug(f"Smart incremental: M from {actual_start_date}")
                else:
                    # 没有记录，智能下载模式：传None让API获取全量数据
                    actual_start_date = None
                    logger.info("Smart download: fetching full M history")

            # 从路由器获取M数据
            data = self.router.route(
                asset_class="macro",  # M属于宏观经济数据
                data_type="m",
                method_name="get_m_data",
                start_m=_convert_to_month_format(actual_start_date),
                end_m=_convert_to_month_format(end_date),
            )

            if data is None or data.empty:
                logger.warning("No M data received")
                return 0

            # 插入数据库
            inserted_count = await self.data_ops.insert_cn_m_batch(data)

            logger.info(f"Updated {inserted_count} M records")
            return inserted_count

        except Exception as e:
            logger.exception("Failed to update M data")
            raise

    async def update_pmi(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """
        更新中国PMI数据

        Args:
            start_date: 开始日期（月份末日期格式，如2020-01-31表示2020年1月），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating PMI data from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        try:
            # 智能下载逻辑：确定实际起始日期
            actual_start_date = start_date

            if not force_update and not start_date:
                # 查询数据库最新记录（cn_pmi表没有symbol列，使用专用方法）
                latest_date = await self.data_ops.get_latest_data_date_no_symbol("cn_pmi")

                if latest_date:
                    # 有记录，计算下一个月第一天
                    next_month = latest_date + timedelta(days=1)
                    actual_start_date = next_month.strftime("%Y-%m-%d")
                    if actual_start_date > end_date:
                        logger.info("PMI data is already up to date")
                        return 0
                    logger.debug(f"Smart incremental: PMI from {actual_start_date}")
                else:
                    # 没有记录，智能下载模式：传None让API获取全量数据
                    actual_start_date = None
                    logger.info("Smart download: fetching full PMI history")

            # 从路由器获取PMI数据
            data = self.router.route(
                asset_class="macro",  # PMI属于宏观经济数据
                data_type="pmi",
                method_name="get_pmi_data",
                start_m=_convert_to_month_format(actual_start_date),
                end_m=_convert_to_month_format(end_date),
            )

            if data is None or data.empty:
                logger.warning("No PMI data received")
                return 0

            # 插入数据库
            inserted_count = await self.data_ops.insert_cn_pmi_batch(data)

            logger.info(f"Updated {inserted_count} PMI records")
            return inserted_count

        except Exception as e:
            logger.exception("Failed to update PMI data")
            raise

    async def update_index_dailybasic(
        self,
        ts_code_list: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新大盘指数每日指标数据

        支持的指数：上证综指（000001.SH）、上证50（000016.SH）、沪深300（000300.SH）、
        中证500（000905.SH）、深证成指（399001.SZ）、中小板指（399005.SZ）、
        创业板指（399006.SZ）、沪深300（399300.SZ）、中证500（399905.SZ）

        支持的调用模式：
        1. 指定 trade_date：获取指定交易日所有指数数据
        2. 指定 ts_code_list：获取指定指数的历史数据
        3. 智能下载模式：增量更新（不指定ts_code_list和trade_date时）

        Args:
            ts_code_list: 指数代码列表，None表示所有支持的指数
            trade_date: 交易日期（YYYYMMDD格式），优先级高于ts_code_list
            start_date: 开始日期（YYYY-MM-DD格式），None表示全量历史数据
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数

        Raises:
            ValueError: 当 trade_date 与 start_date 或 end_date 同时指定时
        """
        # 参数互斥检查
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date cannot be used with start_date or end_date")
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # 默认支持的指数列表（使用TushareProvider中定义的权威列表）
        # 合并单个ts_code和列表
        if ts_code_list is None:
            ts_code_list = SUPPORTED_INDEX_CODES

        logger.info(
            f"Updating index_dailybasic for {len(ts_code_list)} indexes (trade_date={trade_date}, start={start_date or 'full'}, end={end_date}, force={force_update})"
        )

        try:
            total_records = 0
            total_indexes = len(ts_code_list)

            # 判断使用哪种模式
            # 1. 交易日模式（优先级最高）
            if trade_date:
                logger.info(f"Trade date mode: fetching all indexes for {trade_date}")
                data = self.router.route(
                    asset_class="index",
                    data_type="dailybasic",
                    method_name="get_index_dailybasic",
                    trade_date=trade_date,
                    ts_code=None,  # 获取所有指数
                )

                if data is None or data.empty:
                    logger.warning(f"No index_dailybasic data received for trade_date={trade_date}")
                    return 0

                inserted_count = await self.data_ops.insert_index_dailybasic_batch(data)
                logger.info(f"Updated {inserted_count} index_dailybasic records for trade_date={trade_date}")
                return inserted_count

            # 2. 指定指数代码列表模式或智能下载模式
            # 遍历每个指数
            for idx, ts_code in enumerate(ts_code_list):
                try:
                    # 智能下载逻辑：确定该指数的实际起始日期
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库中该指数的最新记录
                        latest_date = await self.data_ops.get_latest_index_dailybasic_date(ts_code)

                        if latest_date:
                            # 有记录，计算下一个交易日
                            next_day = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
                            symbol_start_date = next_day.strftime("%Y-%m-%d")
                            if symbol_start_date > end_date:
                                logger.debug(f"Skipping {ts_code} - already up to date")
                                if progress_callback:
                                    progress_callback(idx + 1, total_indexes)
                                continue
                            logger.debug(f"Smart incremental: {ts_code} from {symbol_start_date}")
                        else:
                            # 新指数，智能下载模式：传None让API获取全量数据
                            symbol_start_date = None
                            logger.debug(f"Smart download: {ts_code} - fetching full history")
                    elif force_update:
                        # 强制更新模式
                        logger.debug(f"Force update: {ts_code} from {symbol_start_date or 'beginning'}")

                    api_start = symbol_start_date.replace("-", "") if symbol_start_date else None
                    api_end = end_date.replace("-", "") if end_date else None

                    logger.info(f"Fetching {ts_code} ({idx + 1}/{total_indexes})")

                    data = self.router.route(
                        asset_class="index",
                        data_type="dailybasic",
                        method_name="get_index_dailybasic",
                        ts_code=ts_code,
                        start_date=api_start,
                        end_date=api_end,
                    )

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_index_dailybasic_batch(data)
                        total_records += inserted
                        logger.info(f"Inserted {inserted} records for {ts_code}")
                    else:
                        logger.debug(f"No data for {ts_code}")

                    # 调用进度回调
                    if progress_callback:
                        progress_callback(idx + 1, total_indexes)

                except Exception as e:
                    logger.error(f"Failed to fetch data for {ts_code}: {str(e)}")
                    continue

            logger.info(f"Updated total {total_records} index_dailybasic records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update index_dailybasic data")
            raise

    async def update_index_daily(
        self,
        ts_code_list: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新指数日线行情数据

        默认或指定 all 时优先使用 trade_date 单日批量接口；
        指定具体指数列表时采用逐指数模式。

        Args:
            ts_code_list: 指数代码列表，None 表示项目支持的全部指数
            trade_date: 交易日期（YYYY-MM-DD 格式），与 start_date/end_date 互斥
            start_date: 开始日期（YYYY-MM-DD 格式）
            end_date: 结束日期（YYYY-MM-DD 格式），None 表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date cannot be used with start_date or end_date")

        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        _validate_no_mixed_all(ts_code_list, "指数日线")
        is_full_catalog_request = ts_code_list is None or _is_all_symbol_request(ts_code_list)
        if is_full_catalog_request:
            active_date = self._index_catalog_active_date(
                start_date=start_date,
                end_date=end_date,
                force_update=force_update,
                trade_date=trade_date,
            )
            ts_code_list = await self.resolve_index_daily_codes(
                active_date=active_date,
            )

        logger.info(
            f"Updating index_daily for {len(ts_code_list)} indexes "
            f"(trade_date={trade_date}, start={start_date or 'smart/full'}, end={end_date}, force={force_update})"
        )

        try:
            if trade_date:
                return await self._update_index_daily_for_trade_dates(
                    ts_code_list=ts_code_list,
                    trade_dates=[trade_date],
                    progress_callback=progress_callback,
                )

            if is_full_catalog_request:
                batch_start_date = await self._resolve_index_daily_batch_start_date(
                    start_date=start_date,
                    end_date=end_date,
                    force_update=force_update,
                )
                if not batch_start_date:
                    raise ValueError(
                        "本地 index_basic 缺少可用于 index_daily 全量更新的 list_date，"
                        "请先刷新 index_basic"
                    )
                return await self._update_index_daily_by_trade_date_range(
                    ts_code_list=ts_code_list,
                    start_date=batch_start_date,
                    end_date=end_date,
                    progress_callback=progress_callback,
                )

            total_records = 0
            total_indexes = len(ts_code_list)

            for idx, ts_code in enumerate(ts_code_list):
                try:
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        latest_date = await self.data_ops.get_latest_index_daily_date(ts_code)

                        if latest_date:
                            next_day = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
                            symbol_start_date = next_day.strftime("%Y-%m-%d")
                            if symbol_start_date > end_date:
                                logger.debug(f"Skipping {ts_code} - already up to date")
                                if progress_callback:
                                    progress_callback(idx + 1, total_indexes)
                                continue
                            logger.debug(f"Smart incremental: {ts_code} from {symbol_start_date}")
                        else:
                            symbol_start_date = None
                            logger.debug(f"Smart download: {ts_code} - fetching full history")
                    elif force_update:
                        logger.debug(f"Force update: {ts_code} from {symbol_start_date or 'beginning'}")

                    api_start = symbol_start_date.replace("-", "") if symbol_start_date else None
                    api_end = end_date.replace("-", "") if end_date else None

                    logger.info(f"Fetching {ts_code} ({idx + 1}/{total_indexes})")

                    data = self.router.route(
                        asset_class="index",
                        data_type="daily",
                        method_name="get_index_daily",
                        ts_code=ts_code,
                        start_date=api_start,
                        end_date=api_end,
                    )

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_index_daily_batch(data)
                        total_records += inserted
                        logger.info(f"Inserted {inserted} records for {ts_code}")
                    else:
                        logger.debug(f"No data for {ts_code}")

                    if progress_callback:
                        progress_callback(idx + 1, total_indexes)

                except Exception as e:
                    logger.error(f"Failed to fetch data for {ts_code}: {str(e)}")
                    continue

            logger.info(f"Updated total {total_records} index_daily records")
            return total_records

        except Exception:
            logger.exception("Failed to update index_daily data")
            raise

    async def update_sw_daily(
        self,
        ts_code: Optional[str] = None,
        ts_code_list: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新申万行业日线行情数据

        支持的调用模式：
        1. 指定 ts_code/tss_code_list：获取指定行业的历史数据
        2. 指定 trade_date：获取指定日期的所有行业数据
        3. 智能下载模式：增量更新（遍历每个行业，根据数据库记录确定起始日期）

        Args:
            ts_code: 行业代码，如 '801780.SI'
            ts_code_list: 行业代码列表，如 ['801780.SI', '801790.SI']
            trade_date: 交易日期（YYYYMMDD格式）
            start_date: 开始日期（YYYY-MM-DD格式），None表示智能下载
            end_date: 结束日期（YYYY-MM-DD格式）
            force_update: 是否强制更新
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # 合并单个ts_code和列表
        if ts_code and not ts_code_list:
            ts_code_list = [ts_code]
        elif ts_code_list is None:
            ts_code_list = []

        _validate_no_mixed_all(ts_code_list, "申万行业日线模式下")
        if _is_all_symbol_request(ts_code_list):
            ts_code_list = []

        logger.info(
            f"Updating sw_daily for {ts_code_list or 'all industries'} "
            f"(trade_date={trade_date}, start={start_date or 'smart'}, end={end_date}, force={force_update})"
        )

        try:
            total_records = 0
            failed_requests: List[Tuple[str, str]] = []

            # 交易日模式
            if trade_date:
                api_trade_date = trade_date.replace("-", "")
                logger.info(
                    f"Trade date mode: fetching all industries for {api_trade_date}"
                )
                data = self.router.route(
                    asset_class="index",
                    data_type="sw_daily",
                    method_name="get_sw_daily",
                    trade_date=api_trade_date,
                )

                if data is not None and not data.empty:
                    inserted = await self.data_ops.insert_sw_daily_batch(data)
                    total_records += inserted
                    logger.info(
                        f"Inserted {inserted} sw_daily records "
                        f"for trade_date={api_trade_date}"
                    )
                return total_records

            # 获取行业代码列表
            if not ts_code_list:
                industry_classify = await self.data_ops.get_sw_industry_classify(level=None)
                if industry_classify is not None and not industry_classify.empty:
                    ts_code_list = industry_classify['index_code'].tolist()
                else:
                    logger.warning("No industry list found")
                    return 0

            total_industries = len(ts_code_list)
            logger.info(f"Processing {total_industries} industries")

            # 遍历每个行业
            for idx, code in enumerate(ts_code_list):
                # 调用进度回调
                if progress_callback:
                    progress_callback(idx + 1, total_industries)

                try:
                    # 智能下载逻辑：确定该行业的实际起始日期
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库中该行业的最新记录
                        latest_date = await self.data_ops.get_latest_sw_daily_date(code)

                        if latest_date:
                            # 有记录，计算下一个交易日
                            next_day = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
                            symbol_start_date = next_day.strftime("%Y%m%d")
                            if symbol_start_date > end_date.replace("-", ""):
                                logger.debug(f"Skipping {code} - already up to date")
                                continue
                            logger.debug(f"Smart incremental: {code} from {symbol_start_date}")
                        else:
                            # 新行业，智能下载模式：传None让API获取全量数据
                            symbol_start_date = None
                            logger.debug(f"Smart download: {code} - fetching full history")

                    api_start = (
                        symbol_start_date.replace("-", "")
                        if symbol_start_date
                        else None
                    )
                    api_end = end_date.replace("-", "") if end_date else None

                    logger.info(f"Industry mode: fetching {code} ({idx + 1}/{total_industries})")

                    data = self.router.route(
                        asset_class="index",
                        data_type="sw_daily",
                        method_name="get_sw_daily",
                        ts_code=code,
                        start_date=api_start,
                        end_date=api_end,
                    )

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_sw_daily_batch(data)
                        total_records += inserted
                        logger.info(f"Inserted {inserted} records for {code}")
                    else:
                        logger.debug(f"No data for {code}")

                except Exception as e:
                    logger.error(f"Failed to fetch data for {code}: {str(e)}")
                    failed_requests.append((code, str(e)))
                    continue

            if failed_requests and len(failed_requests) == total_industries:
                first_code, first_error = failed_requests[0]
                raise RuntimeError(
                    f"All {total_industries} sw_daily requests failed; "
                    f"first failure: {first_code}: {first_error}"
                )
            if failed_requests:
                logger.warning(
                    f"Failed to fetch {len(failed_requests)}/{total_industries} "
                    "sw_daily industries"
                )

            # 最终进度回调
            if progress_callback:
                progress_callback(total_industries, total_industries)

            logger.info(f"Updated total {total_records} sw_daily records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update sw_daily data")
            raise

    async def update_fina_indicator(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新财务指标数据

        Args:
            symbols: 股票代码列表，None表示从数据库获取所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating fina_indicator for {len(symbols) if symbols else 'all symbols'} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        # 如果没有指定股票，从数据库获取所有股票
        if not symbols:
            symbols = await self.data_ops.get_symbol_list()

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        total_symbols = len(symbols)

        try:
            total_records = 0

            for idx, symbol in enumerate(symbols):
                # 调用进度回调
                if progress_callback:
                    progress_callback(idx + 1, total_symbols)

                try:
                    # 智能下载逻辑：确定该symbol的实际起始日期
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库最新记录
                        latest_date = await self.data_ops.get_latest_fina_indicator_date(symbol)

                        if latest_date:
                            # 有记录，计算下一个报告期
                            # 财务数据通常是季度数据，报告期为季度末（3/31, 6/30, 9/30, 12/31）
                            # 获取下一个季度末
                            next_quarter = self._get_next_quarter_end(latest_date)
                            symbol_start_date = next_quarter
                            if symbol_start_date > end_date:
                                logger.debug(f"Skipping {symbol} - already up to date")
                                continue
                            logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                        else:
                            # 新股票，智能下载模式：传None让API获取全量数据
                            symbol_start_date = None
                            logger.info(f"Smart download: {symbol} - fetching full history")
                    elif force_update:
                        # 强制更新模式
                        logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                    # 从路由器获取数据
                    data = self.router.route(
                        asset_class="stock",
                        data_type="fina_indicator",
                        method_name="get_fina_indicator",
                        ts_code=symbol,
                        start_date=symbol_start_date,
                        end_date=end_date,
                    )

                    if data is not None and not data.empty:
                        # 插入数据库
                        inserted = await self.data_ops.insert_fina_indicator_batch(
                            data, batch_size=1000
                        )
                        total_records += inserted
                        logger.info(f"Updated {inserted} fina_indicator records for {symbol}")
                    else:
                        logger.debug(f"No fina_indicator data for {symbol}")

                except Exception as e:
                    logger.error(f"Failed to update fina_indicator for {symbol}: {str(e)}")
                    continue

            # 调用最终进度回调
            if progress_callback:
                progress_callback(total_symbols, total_symbols)

            logger.info(f"Updated total {total_records} fina_indicator records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update fina_indicator data")
            raise

    async def update_cashflow(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新现金流量表数据

        Args:
            symbols: 股票代码列表，None表示从数据库获取所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating cashflow for {len(symbols) if symbols else 'all symbols'} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        # 如果没有指定股票，从数据库获取所有股票
        if not symbols:
            symbols = await self.data_ops.get_symbol_list()

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        total_symbols = len(symbols)

        try:
            total_records = 0

            for idx, symbol in enumerate(symbols):
                # 调用进度回调
                if progress_callback:
                    progress_callback(idx + 1, total_symbols)

                try:
                    # 智能下载逻辑：确定该symbol的实际起始日期
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库最新记录
                        latest_date = await self.data_ops.get_latest_cashflow_date(symbol)

                        if latest_date:
                            # 有记录，计算下一个报告期
                            # 财务数据通常是季度数据，报告期为季度末（3/31, 6/30, 9/30, 12/31）
                            # 获取下一个季度末
                            next_quarter = self._get_next_quarter_end(latest_date)
                            symbol_start_date = next_quarter
                            if symbol_start_date > end_date:
                                logger.debug(f"Skipping {symbol} - already up to date")
                                continue
                            logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                        else:
                            # 新股票，智能下载模式：传None让API获取全量数据
                            symbol_start_date = None
                            logger.info(f"Smart download: {symbol} - fetching full history")
                    elif force_update:
                        # 强制更新模式
                        logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                    # 从路由器获取数据
                    data = self.router.route(
                        asset_class="stock",
                        data_type="cashflow",
                        method_name="get_cashflow",
                        ts_code=symbol,
                        start_date=symbol_start_date,
                        end_date=end_date,
                    )

                    if data is not None and not data.empty:
                        # 插入数据库
                        inserted = await self.data_ops.insert_cashflow_batch(
                            data, batch_size=1000
                        )
                        total_records += inserted
                        logger.info(f"Updated {inserted} cashflow records for {symbol}")
                    else:
                        logger.debug(f"No cashflow data for {symbol}")

                except Exception as e:
                    logger.error(f"Failed to update cashflow for {symbol}: {str(e)}")
                    continue

            # 调用最终进度回调
            if progress_callback:
                progress_callback(total_symbols, total_symbols)

            logger.info(f"Updated total {total_records} cashflow records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update cashflow data")
            raise

    async def update_balancesheet(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新资产负债表数据

        Args:
            symbols: 股票代码列表，None表示从数据库获取所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating balancesheet for {len(symbols) if symbols else 'all symbols'} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        # 如果没有指定股票，从数据库获取所有股票
        if not symbols:
            symbols = await self.data_ops.get_symbol_list()

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        total_symbols = len(symbols)

        try:
            total_records = 0

            for idx, symbol in enumerate(symbols):
                # 调用进度回调
                if progress_callback:
                    progress_callback(idx + 1, total_symbols)

                try:
                    # 智能下载逻辑：确定该symbol的实际起始日期
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库最新记录
                        latest_date = await self.data_ops.get_latest_balancesheet_date(symbol)

                        if latest_date:
                            # 有记录，计算下一个报告期
                            # 财务数据通常是季度数据，报告期为季度末（3/31, 6/30, 9/30, 12/31）
                            # 获取下一个季度末
                            next_quarter = self._get_next_quarter_end(latest_date)
                            symbol_start_date = next_quarter
                            if symbol_start_date > end_date:
                                logger.debug(f"Skipping {symbol} - already up to date")
                                continue
                            logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                        else:
                            # 新股票，智能下载模式：传None让API获取全量数据
                            symbol_start_date = None
                            logger.info(f"Smart download: {symbol} - fetching full history")
                    elif force_update:
                        # 强制更新模式
                        logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                    # 从路由器获取数据
                    data = self.router.route(
                        asset_class="stock",
                        data_type="balancesheet",
                        method_name="get_balancesheet",
                        ts_code=symbol,
                        start_date=symbol_start_date,
                        end_date=end_date,
                    )

                    if data is not None and not data.empty:
                        # 统计各股票的记录数
                        symbol_records = len(data[data['ts_code'] == symbol]) if 'ts_code' in data.columns else len(data)
                        logger.info(f"Fetched {len(data)} total records for {symbol} (matching: {symbol_records})")

                        # 插入数据库
                        inserted = await self.data_ops.insert_balancesheet_batch(
                            data, batch_size=1000
                        )
                        total_records += inserted
                        logger.info(f"Inserted {inserted} balancesheet records for {symbol}")
                    else:
                        logger.warning(f"No balancesheet data returned for {symbol}")

                except Exception as e:
                    logger.error(f"Failed to update balancesheet for {symbol}: {str(e)}")
                    continue

            # 调用最终进度回调
            if progress_callback:
                progress_callback(total_symbols, total_symbols)

            logger.info(f"Updated total {total_records} balancesheet records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update balancesheet data")
            raise

    async def update_income(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新利润表数据

        Args:
            symbols: 股票代码列表，None表示从数据库获取所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示智能下载
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating income for {len(symbols) if symbols else 'all symbols'} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        # 如果没有指定股票，从数据库获取所有股票
        if not symbols:
            symbols = await self.data_ops.get_symbol_list()

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        total_symbols = len(symbols)

        try:
            total_records = 0

            for idx, symbol in enumerate(symbols):
                # 调用进度回调
                if progress_callback:
                    progress_callback(idx + 1, total_symbols)

                try:
                    # 智能下载逻辑：确定该symbol的实际起始日期
                    symbol_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库最新记录
                        latest_date = await self.data_ops.get_latest_income_date(symbol)

                        if latest_date:
                            # 有记录，计算下一个报告期
                            # 财务数据通常是季度数据，报告期为季度末（3/31, 6/30, 9/30, 12/31）
                            # 获取下一个季度末
                            next_quarter = self._get_next_quarter_end(latest_date)
                            symbol_start_date = next_quarter
                            if symbol_start_date > end_date:
                                logger.debug(f"Skipping {symbol} - already up to date")
                                continue
                            logger.debug(f"Smart incremental: {symbol} from {symbol_start_date}")
                        else:
                            # 新股票，智能下载模式：传None让API获取全量数据
                            symbol_start_date = None
                            logger.info(f"Smart download: {symbol} - fetching full history")
                    elif force_update:
                        # 强制更新模式
                        logger.debug(f"Force update: {symbol} from {symbol_start_date or 'beginning'}")

                    # 从路由器获取数据
                    data = self.router.route(
                        asset_class="stock",
                        data_type="income",
                        method_name="get_income",
                        ts_code=symbol,
                        start_date=symbol_start_date,
                        end_date=end_date,
                    )

                    if data is not None and not data.empty:
                        # 插入数据库
                        inserted = await self.data_ops.insert_income_batch(
                            data, batch_size=1000
                        )
                        total_records += inserted
                        logger.info(f"Updated {inserted} income records for {symbol}")
                    else:
                        logger.debug(f"No income data for {symbol}")

                except Exception as e:
                    logger.error(f"Failed to update income for {symbol}: {str(e)}")
                    continue

            # 调用最终进度回调
            if progress_callback:
                progress_callback(total_symbols, total_symbols)

            logger.info(f"Updated total {total_records} income records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update income data")
            raise

    def _get_next_quarter_end(self, current_date: str) -> str:
        """
        获取下一个季度末日期

        Args:
            current_date: 当前日期（YYYY-MM-DD格式）

        Returns:
            str: 下一个季度末日期（YYYY-MM-DD格式）
        """
        from datetime import datetime
        import calendar

        current = datetime.strptime(current_date, "%Y-%m-%d")
        year = current.year
        month = current.month

        # 当前季度：Q1(1-3), Q2(4-6), Q3(7-9), Q4(10-12)
        # 找到下一个季度末
        if month <= 3:
            # Q1结束，下一个季度是Q2
            next_month = 6
            next_year = year
        elif month <= 6:
            # Q2结束，下一个季度是Q3
            next_month = 9
            next_year = year
        elif month <= 9:
            # Q3结束，下一个季度是Q4
            next_month = 12
            next_year = year
        else:
            # Q4结束，下一个季度是次年Q1
            next_month = 3
            next_year = year + 1

        # 获取该月最后一天
        last_day = calendar.monthrange(next_year, next_month)[1]
        return f"{next_year:04d}-{next_month:02d}-{last_day:02d}"

    # ============================================================================
    # 申万行业数据更新方法
    # ============================================================================

    async def update_sw_industry_classify(
        self,
        level: str = "L1",
        src: str = "SW2021",
        force_update: bool = False,
    ) -> int:
        """
        更新申万行业分类数据

        Args:
            level: 行业层级 (L1/L2/L3)
            src: 行业分类来源 (SW2014/SW2021)
            force_update: 是否强制更新

        Returns:
            int: 更新的记录数
        """
        logger.info(
            f"Updating Shenwan industry classify (level={level}, src={src}, force={force_update})"
        )

        try:
            total_inserted = 0

            # 获取并存储所有级别的行业分类数据（L1, L2, L3）
            for lvl in ["L1", "L2", "L3"]:
                data = self.router.route(
                    asset_class="index",
                    data_type="sw_classify",
                    method_name="get_sw_industry_classify",
                    level=lvl,
                    src=src,
                )

                if data is None or data.empty:
                    logger.warning(f"No {lvl} industry classify data received")
                    continue

                # 插入数据库
                inserted_count = await self.data_ops.insert_sw_industry_classify_batch(data)
                total_inserted += inserted_count
                logger.info(f"Inserted {inserted_count} {lvl} industry classify records")

            logger.info(f"Updated total {total_inserted} industry classify records")
            return total_inserted

        except Exception as e:
            logger.exception("Failed to update industry classify")
            raise

    async def update_sw_industry_members(
        self,
        l1_code: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新申万行业成分股数据（按一级行业逐个下载）

        Args:
            l1_code: 一级行业代码，None表示更新所有行业
            force_update: 是否强制更新
            progress_callback: 进度回调函数 (current, total)

        Returns:
            int: 更新的记录数
        """
        logger.info(
            f"Updating Shenwan industry members (l1={l1_code}, force={force_update})"
        )

        try:
            # 获取所有一级行业列表
            classify_data = self.router.route(
                asset_class="index",
                data_type="sw_classify",
                method_name="get_sw_industry_classify",
                level="L1",
                src="SW2021",
            )

            if classify_data is None or classify_data.empty:
                logger.warning("No industry classify data, please update classify first")
                return 0

            # 使用 index_code（完整的指数代码，如 801780.SI）而不是 industry_code（如 801780）
            l1_codes = classify_data["index_code"].tolist()
            logger.info(f"Found {len(l1_codes)} level-1 industries")

            total_industries = len(l1_codes)
            total_records = 0
            new_records = 0
            skipped_records = 0
            completed = 0

            # 如果不是强制更新，获取数据库中已有的L1行业列表
            existing_l1_codes = set()
            if not force_update:
                try:
                    all_members = await self.data_ops.get_sw_industry_members()
                    if all_members is not None and not all_members.empty:
                        existing_l1_codes = set(all_members["l1_code"].unique())
                        logger.info(f"Found {len(existing_l1_codes)} L1 industries with existing member data")
                except Exception as e:
                    logger.warning(f"Could not query existing data: {str(e)}, will re-download all")
                    existing_l1_codes = set()

            for l1_code_item in l1_codes:
                try:
                    # 跳过已存在的L1行业
                    if not force_update and l1_code_item in existing_l1_codes:
                        skipped_records += 1
                        logger.info(f"Skipping L1 {l1_code_item} - already exists")
                        completed += 1
                        if progress_callback:
                            progress_callback(completed, total_industries)
                        continue

                    # 直接按L1行业代码获取所有成分股
                    logger.info(f"Fetching members for L1 {l1_code_item}...")
                    data = self.router.route(
                        asset_class="index",
                        data_type="sw_member",
                        method_name="get_sw_industry_members",
                        l1_code=l1_code_item,
                    )

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_sw_industry_member_batch(data)
                        total_records += inserted
                        new_records += len(data)
                        logger.info(f"L1 {l1_code_item}: inserted {inserted} member records")
                    else:
                        logger.warning(f"No data for L1 {l1_code_item}")

                    # API限流
                    time.sleep(0.3)

                except Exception as e:
                    logger.error(f"Failed to update members for industry {l1_code_item}: {str(e)}")
                    continue

                completed += 1
                if progress_callback:
                    progress_callback(completed, total_industries)

            logger.info(f"Updated total {total_records} industry member records (new: {new_records}, skipped: {skipped_records}, force: {force_update})")
            return total_records

        except Exception as e:
            logger.exception("Failed to update industry members")
            raise

    async def update_trade_cal(
        self,
        exchange_list: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新交易日历数据

        支持的交易所：SSE(上交所)、SZSE(深交所)、CFFEX(中金所)、SHFE(上期所)、
        CZCE(郑商所)、DCE(大商所)、INE(上能源)

        支持的调用模式：
        1. 智能下载模式：增量更新（不指定exchange_list和date参数时）
        2. 指定交易所列表：获取指定交易所的日历数据
        3. 强制更新模式：强制重新获取所有数据

        Args:
            exchange_list: 交易所代码列表，None表示所有支持的交易所
            start_date: 开始日期（YYYY-MM-DD格式），None表示全量历史数据
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 注意：交易日历数据不设置默认 end_date，传递 None 让 API 返回全量历史数据
        # Tushare API 支持 start_date/end_date 为 None，返回所有数据

        # 默认使用所有支持的交易所
        if exchange_list is None:
            exchange_list = SUPPORTED_EXCHANGES

        logger.info(
            f"Updating trade_cal for {len(exchange_list)} exchanges "
            f"(start={start_date or 'full'}, end={end_date or 'latest available'}, force={force_update})"
        )

        try:
            total_records = 0
            total_exchanges = len(exchange_list)

            for idx, exchange in enumerate(exchange_list):
                try:
                    # 智能下载逻辑：确定该交易所的实际起始日期
                    exch_start_date = start_date

                    if not force_update and not start_date:
                        # 查询数据库中该交易所的最新记录
                        latest_date = await self.data_ops.get_latest_trade_cal_date(exchange)

                        if latest_date:
                            # 有记录，计算下一天
                            next_day = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
                            exch_start_date = next_day.strftime("%Y-%m-%d")
                            # end_date 为 None 时表示获取全量数据，不需要跳过
                            if end_date and exch_start_date > end_date:
                                logger.debug(f"Skipping {exchange} - already up to date")
                                if progress_callback:
                                    progress_callback(idx + 1, total_exchanges)
                                continue
                            logger.debug(f"Smart incremental: {exchange} from {exch_start_date}")
                        else:
                            # 新交易所，获取全量数据
                            exch_start_date = None
                            logger.debug(f"Smart download: {exchange} - fetching full history")
                    elif force_update:
                        logger.debug(f"Force update: {exchange} from {exch_start_date or 'beginning'}")

                    logger.info(f"Fetching {exchange} ({idx + 1}/{total_exchanges})")

                    # 调用API获取数据
                    data = self.router.route(
                        asset_class="market",
                        data_type="trade_cal",
                        method_name="get_trade_cal",
                        exchange=exchange,
                        start_date=exch_start_date,
                        end_date=end_date,
                    )

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_trade_cal_batch(data)
                        total_records += inserted
                        logger.info(f"Inserted {inserted} records for {exchange}")
                    else:
                        logger.debug(f"No data for {exchange}")

                    # 调用进度回调
                    if progress_callback:
                        progress_callback(idx + 1, total_exchanges)

                except Exception as e:
                    logger.error(f"Failed to fetch data for {exchange}: {str(e)}")
                    continue

            logger.info(f"Updated total {total_records} trade_cal records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update trade_cal data")
            raise

    async def update_index_weight(
        self,
        index_list: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新指数成分权重数据（月度数据）

        建议输入参数里开始日期和结束日分别输入当月第一天和最后一天的日期。

        支持的调用模式：
        1. 智能下载模式：增量更新（不指定index_list和date参数时）
        2. 指定指数列表：获取指定指数的权重数据
        3. 强制更新模式：强制重新获取所有数据
        4. 单日模式：指定trade_date获取特定日期的数据

        Args:
            index_list: 指数代码列表，None表示所有支持的指数
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）
            trade_date: 交易日期（YYYY-MM-DD格式），与start_date/end_date互斥
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        # 参数约束检查
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date 不能与 start_date/end_date 同时指定")

        # 默认使用所有支持的指数
        _validate_no_mixed_all(index_list, "指数成分权重")
        if index_list is None or _is_all_symbol_request(index_list):
            active_date = self._index_catalog_active_date(
                start_date=start_date,
                end_date=end_date,
                force_update=force_update,
                trade_date=trade_date,
            )
            index_list = await self.resolve_index_weight_codes(
                active_date=active_date,
            )

        logger.info(
            f"Updating index_weight for {len(index_list)} indexes "
            f"(start={start_date or 'auto'}, end={end_date or 'auto'}, trade_date={trade_date}, force={force_update})"
        )

        try:
            total_records = 0
            total_indexes = len(index_list)

            for idx, index_code in enumerate(index_list):
                try:
                    # 智能下载逻辑：确定该指数的实际起始日期
                    # 注意：index_weight 是月度数据，需要处理月份边界
                    exch_start_date = start_date
                    exch_end_date = end_date

                    if not force_update and not trade_date and not start_date:
                        # 查询数据库中该指数的最新记录
                        latest_date = await self.data_ops.get_latest_index_weight_date(index_code)

                        if latest_date:
                            # 有记录，计算下一个月
                            # 将日期转换为下个月的第一天
                            latest = datetime.strptime(latest_date, "%Y-%m-%d")
                            # 计算下个月的第一天
                            if latest.month == 12:
                                next_month = datetime(latest.year + 1, 1, 1)
                            else:
                                next_month = datetime(latest.year, latest.month + 1, 1)
                            exch_start_date = next_month.strftime("%Y-%m-%d")
                            logger.debug(f"Smart incremental: {index_code} from {exch_start_date}")
                        else:
                            # 新指数，获取全量数据
                            exch_start_date = None
                            logger.debug(f"Smart download: {index_code} - fetching full history")
                    elif force_update:
                        logger.debug(f"Force update: {index_code} from {exch_start_date or 'beginning'}")

                    logger.info(f"Fetching {index_code} ({idx + 1}/{total_indexes})")

                    # 调用API获取数据
                    data = self.router.route(
                        asset_class="index",
                        data_type="index_weight",
                        method_name="get_index_weight",
                        index_code=index_code,
                        trade_date=trade_date,
                        start_date=exch_start_date,
                        end_date=exch_end_date,
                    )

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_index_weight_batch(data)
                        total_records += inserted
                        logger.info(f"Inserted {inserted} records for {index_code}")
                    else:
                        logger.debug(f"No data for {index_code}")

                    # 调用进度回调
                    if progress_callback:
                        progress_callback(idx + 1, total_indexes)

                except Exception as e:
                    logger.error(f"Failed to fetch index_weight for {index_code}: {str(e)}")
                    continue

            logger.info(f"Updated total {total_records} index_weight records")
            return total_records

        except Exception as e:
            logger.exception("Failed to update index_weight data")
            raise

    def _incremental_start(
        self,
        latest_date: Optional[str],
        start_date: Optional[str],
        force_update: bool,
    ) -> Optional[str]:
        """Return the actual start date for smart incremental futures updates."""
        if force_update or start_date or not latest_date:
            return start_date
        return (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

    def _futures_minute_incremental_start(
        self,
        latest_date: Optional[str],
        start_date: Optional[str],
        force_update: bool,
        freq: str,
    ) -> Optional[str]:
        """Return the next minute-bar timestamp for smart futures minute updates."""
        if force_update or start_date or not latest_date:
            return start_date

        step_minutes = 5 if _normalize_futures_minute_frequency(freq) == "5m" else 1
        return _format_datetime(
            _coerce_datetime(latest_date) + timedelta(minutes=step_minutes)
        )

    def _futures_period_incremental_start(
        self,
        latest_date: Optional[str],
        start_date: Optional[str],
        force_update: bool,
        period: str,
    ) -> Optional[str]:
        """Return a start date that re-downloads the still-moving last period bar."""
        if force_update or start_date or not latest_date:
            return start_date
        lookback_days = FUTURES_PERIOD_LOOKBACK_DAYS[period]
        return (
            datetime.strptime(latest_date, "%Y-%m-%d")
            - timedelta(days=lookback_days)
        ).strftime("%Y-%m-%d")

    async def _resolve_futures_symbol_universe(
        self,
        symbols: Optional[List[str]],
        product_codes: Optional[List[str]],
        contract_types: List[str],
        trade_date: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        force_update: bool,
        default_active_only: bool,
    ) -> List[str]:
        """Resolve futures contract symbols from contract_basic with date-overlap filtering."""
        explicit_all = _is_all_futures_selector(symbols) or _is_all_futures_selector(
            product_codes
        )
        if not explicit_all and symbols:
            return _dedupe_preserve_order(symbols)

        normalized_products = None
        if not explicit_all and product_codes:
            normalized_products = _dedupe_preserve_order(
                [str(code).strip().upper() for code in product_codes if str(code).strip()]
            )

        overlap_start, overlap_end = _normalize_futures_window(
            trade_date,
            start_date,
            end_date,
        )
        active_only = (
            default_active_only
            and not force_update
            and overlap_start is None
            and overlap_end is None
        )

        resolved: List[str] = []
        if normalized_products:
            for product_code in normalized_products:
                resolved.extend(
                    await self.data_ops.get_futures_contract_symbols(
                        product_code=product_code,
                        contract_types=contract_types,
                        active_only=active_only,
                        overlap_start=overlap_start,
                        overlap_end=overlap_end,
                    )
                )
        else:
            resolved = await self.data_ops.get_futures_contract_symbols(
                contract_types=contract_types,
                active_only=active_only,
                overlap_start=overlap_start,
                overlap_end=overlap_end,
            )
        return _dedupe_preserve_order(resolved)

    async def update_futures_basic(self, exchange: Optional[str] = None) -> int:
        """更新期货合约基础信息。"""
        data = self.router.route(
            asset_class="future",
            data_type="basic",
            method_name="get_futures_basic",
            exchange=exchange,
        )
        if data is None or data.empty:
            logger.warning("No futures contract basic data received")
            return 0
        return await self.data_ops.insert_futures_contract_basic_batch(data)

    async def update_futures_mapping(
        self,
        symbols: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """更新期货主力/连续合约映射。"""
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        explicit_all = _is_all_futures_selector(symbols)
        inferred_products: List[str] = []
        if not explicit_all:
            explicit_symbols, inferred_products = _split_futures_symbols_and_products(
                symbols
            )
            if symbols is not None:
                symbols = explicit_symbols

        if trade_date:
            data = self.router.route(
                asset_class="future",
                data_type="mapping",
                method_name="get_futures_mapping",
                trade_date=trade_date,
            )
            return await self.data_ops.insert_futures_contract_mapping_batch(data)

        symbols = await self._resolve_futures_symbol_universe(
            symbols=symbols if not explicit_all else ["all"],
            product_codes=inferred_products,
            contract_types=["main", "continuous"],
            trade_date=None,
            start_date=start_date,
            end_date=end_date,
            force_update=force_update,
            default_active_only=False,
        )

        total = 0
        for idx, symbol in enumerate(symbols or []):
            latest = await self.data_ops.get_latest_futures_date(
                "contract_mapping", symbol=symbol
            )
            actual_start = self._incremental_start(latest, start_date, force_update)
            if actual_start and end_date and actual_start > end_date:
                if progress_callback:
                    progress_callback(idx + 1, len(symbols))
                continue
            data = self.router.route(
                asset_class="future",
                data_type="mapping",
                method_name="get_futures_mapping",
                symbol=symbol,
                start_date=actual_start,
                end_date=end_date,
            )
            if data is not None and not data.empty:
                total += await self.data_ops.insert_futures_contract_mapping_batch(data)
            if progress_callback:
                progress_callback(idx + 1, len(symbols))
        return total

    async def update_futures_daily(
        self,
        symbols: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """更新期货日线行情。"""
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        explicit_all = _is_all_futures_selector(symbols)
        inferred_products: List[str] = []
        if not explicit_all:
            explicit_symbols, inferred_products = _split_futures_symbols_and_products(
                symbols
            )
            if symbols is not None:
                symbols = explicit_symbols
            if inferred_products:
                product_codes = (product_codes or []) + inferred_products

        if trade_date:
            data = self.router.route(
                asset_class="future",
                data_type="daily",
                method_name="get_futures_daily",
                trade_date=trade_date,
            )
            return await self.data_ops.insert_futures_daily_batch(data)

        symbols = await self._resolve_futures_symbol_universe(
            symbols=symbols if not explicit_all else ["all"],
            product_codes=product_codes,
            contract_types=["normal", "main", "continuous"],
            trade_date=None,
            start_date=start_date,
            end_date=end_date,
            force_update=force_update,
            default_active_only=True,
        )
        if not symbols:
            logger.warning("No futures contracts found; run future basic first")
            return 0

        total = 0
        for idx, symbol in enumerate(symbols):
            latest = await self.data_ops.get_latest_futures_date("daily", symbol=symbol)
            actual_start = self._incremental_start(latest, start_date, force_update)
            if actual_start and end_date and actual_start > end_date:
                if progress_callback:
                    progress_callback(idx + 1, len(symbols))
                continue
            data = self.router.route(
                asset_class="future",
                data_type="daily",
                method_name="get_futures_daily",
                symbol=symbol,
                start_date=actual_start,
                end_date=end_date,
            )
            if data is not None and not data.empty:
                total += await self.data_ops.insert_futures_daily_batch(data)
            if progress_callback:
                progress_callback(idx + 1, len(symbols))
        return total

    async def update_futures_weekly_monthly(
        self,
        period: str,
        symbols: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """更新期货周线/月线行情，并每日覆盖最后一根未完成 K 线。"""
        self.last_futures_period_summary: Dict[str, Any] = {
            "total_symbols": 0,
            "inserted_symbols": 0,
            "empty_symbols": 0,
            "up_to_date_symbols": 0,
            "failed_symbols": [],
            "inserted_records": 0,
        }
        period = str(period).strip().lower()
        if period not in {"weekly", "monthly"}:
            raise ValueError(f"Unsupported futures period: {period}")
        if not trade_date and not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        explicit_all = _is_all_futures_selector(symbols)
        inferred_products: List[str] = []
        if not explicit_all:
            explicit_symbols, inferred_products = _split_futures_symbols_and_products(
                symbols
            )
            if symbols is not None:
                symbols = explicit_symbols
            if inferred_products:
                product_codes = (product_codes or []) + inferred_products

        request_start_date, request_end_date = _normalize_futures_query_dates(
            trade_date,
            start_date,
            end_date,
        )
        api_end_date = (
            request_end_date
            if trade_date
            else _futures_period_query_end(request_end_date, period)
        )

        symbols = await self._resolve_futures_symbol_universe(
            symbols=symbols if not explicit_all else ["all"],
            product_codes=product_codes,
            contract_types=["normal", "main", "continuous"],
            trade_date=trade_date,
            start_date=start_date,
            end_date=None if trade_date else end_date,
            force_update=force_update,
            default_active_only=True,
        )
        if not symbols:
            logger.warning("No futures contracts found; run future basic first")
            return 0
        self.last_futures_period_summary["total_symbols"] = len(symbols)

        method_name = (
            "get_futures_weekly" if period == "weekly" else "get_futures_monthly"
        )
        insert_batch = (
            self.data_ops.insert_futures_weekly_batch
            if period == "weekly"
            else self.data_ops.insert_futures_monthly_batch
        )

        total = 0
        inserted_symbols = 0
        empty_symbols = 0
        up_to_date_symbols = 0
        failed_symbols = []
        for idx, symbol in enumerate(symbols):
            try:
                latest = None
                actual_start = request_start_date
                if not trade_date:
                    latest = await self.data_ops.get_latest_futures_date(
                        period,
                        symbol=symbol,
                    )
                    actual_start = self._futures_period_incremental_start(
                        latest,
                        request_start_date,
                        force_update,
                        period,
                    )
                    if actual_start and api_end_date and actual_start > api_end_date:
                        up_to_date_symbols += 1
                        continue

                data = self.router.route(
                    asset_class="future",
                    data_type=period,
                    method_name=method_name,
                    symbol=symbol,
                    trade_date=trade_date,
                    start_date=None if trade_date else actual_start,
                    end_date=None if trade_date else api_end_date,
                )
                if data is not None and not data.empty:
                    total += await insert_batch(data)
                    inserted_symbols += 1
                else:
                    empty_symbols += 1
            except Exception as exc:
                logger.warning(
                    f"Skipping futures {period} symbol {symbol} after failure: {exc}"
                )
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
            finally:
                if progress_callback:
                    progress_callback(idx + 1, len(symbols))

        self.last_futures_period_summary = {
            "total_symbols": len(symbols),
            "inserted_symbols": inserted_symbols,
            "empty_symbols": empty_symbols,
            "up_to_date_symbols": up_to_date_symbols,
            "failed_symbols": failed_symbols,
            "inserted_records": total,
        }
        return total

    async def update_futures_weekly(
        self,
        symbols: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        return await self.update_futures_weekly_monthly(
            "weekly",
            symbols=symbols,
            product_codes=product_codes,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            force_update=force_update,
            progress_callback=progress_callback,
        )

    async def update_futures_monthly(
        self,
        symbols: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        return await self.update_futures_weekly_monthly(
            "monthly",
            symbols=symbols,
            product_codes=product_codes,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            force_update=force_update,
            progress_callback=progress_callback,
        )

    async def update_futures_minute(
        self,
        symbols: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = "1m",
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """更新期货分钟行情。"""
        self.last_futures_minute_summary: Dict[str, Any] = {
            "total_symbols": 0,
            "attempted_symbols": 0,
            "inserted_symbols": 0,
            "empty_symbols": 0,
            "up_to_date_symbols": 0,
            "failed_symbols": [],
            "inserted_records": 0,
        }
        freq = _normalize_futures_minute_frequency(freq)
        if freq not in {"1m", "5m"}:
            logger.info(
                f"Futures minute {freq} is derived from 5m continuous aggregates; "
                "skip provider download"
            )
            return 0
        request_start_date, request_end_date = _normalize_futures_minute_window(
            trade_date,
            start_date,
            end_date,
        )

        explicit_all = _is_all_futures_selector(symbols)
        symbols = await self._resolve_futures_symbol_universe(
            symbols=symbols if not explicit_all else ["all"],
            product_codes=None,
            contract_types=["normal", "main", "continuous"]
            if explicit_all
            else ["main"],
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            force_update=force_update,
            default_active_only=True,
        )
        if not symbols:
            logger.warning("No futures symbols found for minute update")
            return 0
        unsupported_symbols = [
            symbol
            for symbol in symbols
            if not is_xtquant_downloadable_futures_symbol(symbol)
        ]
        if unsupported_symbols:
            sample = ", ".join(unsupported_symbols[:10])
            suffix = "..." if len(unsupported_symbols) > 10 else ""
            logger.warning(
                f"Skipping {len(unsupported_symbols)} XTQuant-unsupported futures "
                f"symbols before minute download: {sample}{suffix}"
            )
            symbols = [
                symbol
                for symbol in symbols
                if is_xtquant_downloadable_futures_symbol(symbol)
            ]
        if not symbols:
            logger.warning(
                "No XTQuant-downloadable futures symbols found for minute update"
            )
            return 0
        self.last_futures_minute_summary["total_symbols"] = len(symbols)

        max_workers = self._get_futures_minute_max_workers(len(symbols))
        logger.info(
            f"Updating futures minute data for {len(symbols)} symbols "
            f"(freq={freq}, start={request_start_date or 'smart'}, "
            f"end={request_end_date}, max_workers={max_workers})"
        )

        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(max_workers)
        progress_lock = asyncio.Lock()
        completed = 0

        async def report_progress() -> None:
            nonlocal completed
            if not progress_callback:
                return
            async with progress_lock:
                completed += 1
                progress_callback(completed, len(symbols))

        async def update_one_symbol(
            symbol: str, executor: ThreadPoolExecutor
        ) -> Dict[str, Any]:
            try:
                async with semaphore:
                    latest = await self.data_ops.get_latest_futures_date(
                        "minute", symbol=symbol, frequency=freq
                    )
                    actual_start = self._futures_minute_incremental_start(
                        latest, request_start_date, force_update, freq
                    )
                    if (
                        actual_start
                        and request_end_date
                        and actual_start > request_end_date
                    ):
                        return {
                            "symbol": symbol,
                            "status": "up_to_date",
                            "records": 0,
                        }

                    try:
                        data = await loop.run_in_executor(
                            executor,
                            lambda: self.router.route(
                                asset_class="future",
                                data_type="minute",
                                freq=freq,
                                method_name="get_futures_minute",
                                symbol=symbol,
                                start_date=actual_start,
                                end_date=request_end_date,
                                wait_for_circuit_breaker=True,
                            ),
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Skipping futures minute symbol {symbol} "
                            f"(freq={freq}) after download failure: {exc}"
                        )
                        return {
                            "symbol": symbol,
                            "status": "failed",
                            "records": 0,
                            "error": str(exc),
                        }

                    if data is not None and not data.empty:
                        inserted = await self.data_ops.insert_futures_minute_batch(data)
                        return {
                            "symbol": symbol,
                            "status": "inserted",
                            "records": inserted,
                        }
                    return {"symbol": symbol, "status": "empty", "records": 0}
            finally:
                await report_progress()

        total = 0
        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="futures-minute"
        ) as executor:
            tasks = [
                asyncio.create_task(update_one_symbol(symbol, executor))
                for symbol in symbols
            ]
            results = await asyncio.gather(*tasks)
        total = sum(int(result.get("records", 0)) for result in results)
        failed_symbols = [
            {
                "symbol": result["symbol"],
                "error": result.get("error", "unknown error"),
            }
            for result in results
            if result.get("status") == "failed"
        ]
        self.last_futures_minute_summary = {
            "total_symbols": len(symbols),
            "attempted_symbols": sum(
                1 for result in results if result.get("status") != "up_to_date"
            ),
            "inserted_symbols": sum(
                1 for result in results if result.get("status") == "inserted"
            ),
            "empty_symbols": sum(
                1 for result in results if result.get("status") == "empty"
            ),
            "up_to_date_symbols": sum(
                1 for result in results if result.get("status") == "up_to_date"
            ),
            "failed_symbols": failed_symbols,
            "inserted_records": total,
        }
        if failed_symbols:
            sample = ", ".join(item["symbol"] for item in failed_symbols[:10])
            suffix = "..." if len(failed_symbols) > 10 else ""
            error_samples: List[str] = []
            seen_errors = set()
            for item in failed_symbols:
                error = str(item.get("error") or "").strip()
                if not error or error in seen_errors:
                    continue
                seen_errors.add(error)
                error_samples.append(error[:500])
                if len(error_samples) >= 3:
                    break
            error_text = (
                f"; error samples: {' | '.join(error_samples)}"
                if error_samples
                else ""
            )
            logger.warning(
                f"Futures minute update completed with {len(failed_symbols)} "
                f"failed symbols out of {len(symbols)}: {sample}{suffix}{error_text}"
            )
        return total

    async def update_futures_settle(
        self,
        symbols: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """更新期货结算参数。"""
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        explicit_all = _is_all_futures_selector(symbols)
        inferred_products: List[str] = []
        if not explicit_all:
            explicit_symbols, inferred_products = _split_futures_symbols_and_products(
                symbols
            )
            if symbols is not None:
                symbols = explicit_symbols
        if trade_date:
            data = self.router.route(
                asset_class="future",
                data_type="settle",
                method_name="get_futures_settle",
                trade_date=trade_date,
            )
            return await self.data_ops.insert_futures_settle_batch(data)
        symbols = await self._resolve_futures_symbol_universe(
            symbols=symbols if not explicit_all else ["all"],
            product_codes=inferred_products,
            contract_types=["normal"],
            trade_date=None,
            start_date=start_date,
            end_date=end_date,
            force_update=force_update,
            default_active_only=True,
        )

        total = 0
        for idx, symbol in enumerate(symbols or []):
            latest = await self.data_ops.get_latest_futures_date("settle", symbol=symbol)
            actual_start = self._incremental_start(latest, start_date, force_update)
            if actual_start and end_date and actual_start > end_date:
                if progress_callback:
                    progress_callback(idx + 1, len(symbols))
                continue
            data = self.router.route(
                asset_class="future",
                data_type="settle",
                method_name="get_futures_settle",
                symbol=symbol,
                start_date=actual_start,
                end_date=end_date,
            )
            if data is not None and not data.empty:
                total += await self.data_ops.insert_futures_settle_batch(data)
            if progress_callback:
                progress_callback(idx + 1, len(symbols))
        return total

    async def update_futures_index_daily(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """更新期货指数日线行情。"""
        if _is_all_futures_selector(symbols):
            symbols = None
        symbols = symbols or SUPPORTED_FUTURES_INDEX_CODES
        total = 0
        for symbol in symbols:
            latest = await self.data_ops.get_latest_futures_date(
                "index_daily", symbol=symbol
            )
            actual_start = self._incremental_start(latest, start_date, force_update)
            data = self.router.route(
                asset_class="future",
                data_type="index_daily",
                method_name="get_futures_index_daily",
                symbols=[symbol],
                start_date=actual_start,
                end_date=end_date,
            )
            if data is not None and not data.empty:
                total += await self.data_ops.insert_futures_index_daily_batch(data)
        return total

    async def update_futures_spot_basis(
        self,
        product_codes: Optional[List[str]] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """更新期货现货价格与基差。"""
        if _is_all_futures_selector(product_codes):
            product_codes = None
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if trade_date:
            data = self.router.route(
                asset_class="future",
                data_type="spot_basis",
                method_name="get_futures_spot_basis",
                trade_date=trade_date,
                products=product_codes,
            )
            return await self.data_ops.insert_futures_spot_basis_batch(data)

        if start_date:
            actual_start = start_date
        elif force_update:
            actual_start = FUTURES_SPOT_BASIS_HISTORY_START
        else:
            latest = await self.data_ops.get_latest_futures_date("spot_basis")
            actual_start = (
                (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                )
                if latest
                else FUTURES_SPOT_BASIS_HISTORY_START
            )

        if actual_start and end_date and actual_start > end_date:
            return 0

        chunks = list(
            _iter_date_chunks(
                actual_start,
                end_date,
                FUTURES_SPOT_BASIS_CHUNK_DAYS,
            )
        )
        if len(chunks) > 1:
            logger.info(
                f"Updating futures spot basis in {len(chunks)} chunks "
                f"from {actual_start} to {end_date}"
            )

        total = 0
        for chunk_start, chunk_end in chunks:
            data = self.router.route(
                asset_class="future",
                data_type="spot_basis",
                method_name="get_futures_spot_basis",
                products=product_codes,
                start_date=chunk_start,
                end_date=chunk_end,
            )
            if data is not None and not data.empty:
                total += await self.data_ops.insert_futures_spot_basis_batch(data)
        return total

    async def update_futures_inventory(
        self,
        product_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """更新期货注册仓单。"""
        if _is_all_futures_selector(product_codes):
            product_codes = None
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        latest = await self.data_ops.get_latest_futures_date("inventory_receipt")
        actual_start = self._incremental_start(latest, start_date, force_update)

        use_history = force_update and not actual_start
        product_names = None
        if use_history:
            contracts = await self.data_ops.get_futures_contracts(product_codes=product_codes)
            if contracts is not None and not contracts.empty:
                supported_contracts = contracts[
                    contracts["exchange"].isin(FUTURES_INVENTORY_SUPPORTED_EXCHANGES)
                ]
                skipped_products = sorted(
                    set(contracts["product_code"].dropna().astype(str).str.upper())
                    - set(
                        supported_contracts["product_code"]
                        .dropna()
                        .astype(str)
                        .str.upper()
                    )
                )
                if skipped_products:
                    logger.info(
                        "Skipping futures inventory products on unsupported "
                        f"exchanges: {skipped_products}"
                    )
                product_names = (
                    supported_contracts.dropna(subset=["product_code"])
                    .drop_duplicates("product_code")
                    .assign(inventory_symbol=lambda df: df["product_code"])
                    .set_index("product_code")["inventory_symbol"]
                    .to_dict()
                )
                if not product_names:
                    logger.warning(
                        "No futures inventory-supported products found "
                        f"(supported exchanges: {sorted(FUTURES_INVENTORY_SUPPORTED_EXCHANGES)})"
                    )

        if not actual_start and not use_history:
            actual_start = end_date
        data = self.router.route(
            asset_class="future",
            data_type="inventory",
            method_name="get_futures_inventory",
            products=product_codes,
            product_names=product_names,
            start_date=actual_start,
            end_date=end_date,
            use_history=use_history,
        )
        return await self.data_ops.insert_futures_inventory_batch(data)

    def _load_futures_dominant_months(self) -> Dict[str, List[int]]:
        """Load dominant month config from bundled or adjacent futures_nexus config."""
        candidates = [
            Path(__file__).parent.parent / "preprocessing" / "futures" / "variety.json",
            Path(__file__).parent.parent.parent.parent / "futures_nexus" / "setting" / "variety.json",
        ]
        for path in candidates:
            if not path.exists():
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                return {
                    str(code).upper(): [int(month) for month in value.get("DominantMonths", [])]
                    for code, value in raw.items()
                    if isinstance(value, dict) and value.get("DominantMonths")
                }
            except Exception as exc:
                logger.warning(f"Failed to load futures dominant months from {path}: {exc}")
        return {}

    async def preprocess_futures_term_metrics(
        self,
        product_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """合成期货期限结构快照与派生指标。"""
        if _is_all_futures_selector(product_codes):
            product_codes = None
        raw = await self.data_ops.get_futures_daily_for_preprocess(
            product_codes=product_codes,
            start_date=start_date,
            end_date=end_date,
        )
        if raw.empty:
            if progress_callback:
                progress_callback(1, 1)
            return 0

        dominant_months = self._load_futures_dominant_months()
        metrics_rows = []
        total_inserted = 0

        async def flush_metrics_rows() -> None:
            nonlocal metrics_rows, total_inserted
            if not metrics_rows:
                return
            total_inserted += await self.data_ops.insert_futures_term_metrics_batch(
                pd.DataFrame(metrics_rows)
            )
            metrics_rows = []

        raw["price"] = pd.to_numeric(raw["settle"], errors="coerce").fillna(
            pd.to_numeric(raw["close"], errors="coerce")
        )
        raw["open_interest"] = pd.to_numeric(raw["open_interest"], errors="coerce")

        grouped = raw.groupby(["product_code", "time"])
        total_groups = grouped.ngroups
        if progress_callback:
            progress_callback(0, total_groups)
        progress_interval = max(total_groups // 1000, 1)
        completed_groups = 0

        for completed_groups, ((product, trade_time), group) in enumerate(
            grouped,
            start=1,
        ):
            months = dominant_months.get(str(product).upper()) or list(range(1, 13))
            eligible = group[group["delivery_month"].isin(months)].copy()
            eligible = eligible.dropna(subset=["price"])
            if eligible.empty:
                if progress_callback and (
                    completed_groups == 1
                    or completed_groups % progress_interval == 0
                    or completed_groups == total_groups
                ):
                    progress_callback(completed_groups, total_groups)
                continue

            curve = eligible.sort_values(["delivery_month_start", "symbol"]).reset_index(
                drop=True
            )
            if curve["open_interest"].notna().any():
                primary_pos = curve["open_interest"].idxmax()
                curve = curve.iloc[int(primary_pos) :].reset_index(drop=True)
            if curve.empty:
                if progress_callback and (
                    completed_groups == 1
                    or completed_groups % progress_interval == 0
                    or completed_groups == total_groups
                ):
                    progress_callback(completed_groups, total_groups)
                continue

            flag = _calculate_futures_term_structure_flag(curve)

            primary = curve.iloc[0]
            secondary = curve.iloc[1] if len(curve) > 1 else None
            primary_price = float(primary["price"])

            secondary_price = None
            spread = None
            days_to_primary = None
            days_between = None
            annualized = None
            if secondary is not None:
                secondary_price = float(secondary["price"])
                spread = primary_price - secondary_price
                primary_expiry = _futures_curve_expiry(primary)
                secondary_expiry = _futures_curve_expiry(secondary)
            else:
                primary_expiry = None
                secondary_expiry = None

            if (
                primary_expiry is not None
                and secondary_expiry is not None
                and primary_price
            ):
                trade_ts = pd.Timestamp(trade_time)
                days_to_primary = max((primary_expiry.date() - trade_ts.date()).days, 0)
                days_between = (secondary_expiry.date() - primary_expiry.date()).days
                if days_between > 0:
                    annualized = ((secondary_price - primary_price) / primary_price) * (
                        365 / days_between
                    )

            metrics_rows.append(
                {
                    "time": trade_time,
                    "product_code": product,
                    "exchange": primary.get("exchange"),
                    "flag": flag,
                    "primary_contract": primary.get("symbol"),
                    "primary_contract_close": primary_price,
                    "secondary_contract": secondary.get("symbol")
                    if secondary is not None
                    else None,
                    "secondary_contract_close": secondary_price,
                    "spread": spread,
                    "days_to_primary_expiry": days_to_primary,
                    "days_between_expiry": days_between,
                    "annualized_roll_yield": annualized,
                    "candidate_count": len(curve),
                    "source": "preprocess",
                }
            )

            if len(metrics_rows) >= FUTURES_TERM_INSERT_BATCH_SIZE:
                await flush_metrics_rows()

            if progress_callback and (
                completed_groups == 1
                or completed_groups % progress_interval == 0
                or completed_groups == total_groups
            ):
                progress_callback(completed_groups, total_groups)

        await flush_metrics_rows()
        if progress_callback:
            progress_callback(total_groups, total_groups)
        return total_inserted

    async def close(self) -> None:
        """关闭资源"""
        if self.db_manager:
            await self.db_manager.close()
            logger.info("DataUpdater closed")

    async def __aenter__(self) -> "DataUpdater":
        """异步上下文管理器入口"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """异步上下文管理器出口"""
        await self.close()
