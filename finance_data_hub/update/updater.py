"""
数据更新器

集成Provider、Router和数据库操作，实现完整的数据更新流程。
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any, Union, Iterator, Tuple, Callable
from datetime import datetime, timedelta
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
    SUPPORTED_INDEX_WEIGHT_CODES,
    SUPPORTED_FUTURES_EXCHANGES,
    SUPPORTED_FUTURES_INDEX_CODES,
)
from finance_data_hub.utils.market import infer_market_from_symbol, normalize_market
from finance_data_hub.utils.futures import is_xtquant_downloadable_futures_symbol

FUTURES_SPOT_BASIS_HISTORY_START = "2011-01-04"
FUTURES_SPOT_BASIS_CHUNK_DAYS = 31
FUTURES_INVENTORY_SUPPORTED_EXCHANGES = {"DCE", "CZCE", "SHFE", "GFEX"}
FUTURES_TERM_INSERT_BATCH_SIZE = 1000


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
            data_source_config, "futures_minute_max_workers", 4
        )
        try:
            workers = int(configured_workers)
        except (TypeError, ValueError):
            workers = 4
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

        for symbol in symbols:
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

        for symbol in symbols:
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
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
        progress_callback: Optional[callable] = None,
    ) -> int:
        """
        更新指数日线行情数据

        index_daily 接口不支持仅通过 trade_date 获取全部指数当日数据，
        因此该方法采用“逐指数”模式进行智能增量或全量更新。

        Args:
            ts_code_list: 指数代码列表，None 表示项目支持的全部指数
            start_date: 开始日期（YYYY-MM-DD 格式）
            end_date: 结束日期（YYYY-MM-DD 格式），None 表示到最新
            force_update: 是否强制更新（忽略数据库状态）
            progress_callback: 进度回调函数，接收 (current, total) 参数

        Returns:
            int: 更新的记录数
        """
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        if ts_code_list is None:
            ts_code_list = SUPPORTED_INDEX_CODES

        logger.info(
            f"Updating index_daily for {len(ts_code_list)} indexes "
            f"(start={start_date or 'smart/full'}, end={end_date}, force={force_update})"
        )

        try:
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

        logger.info(
            f"Updating sw_daily for {ts_code_list or 'all industries'} "
            f"(trade_date={trade_date}, start={start_date or 'smart'}, end={end_date}, force={force_update})"
        )

        try:
            total_records = 0

            # 交易日模式
            if trade_date:
                logger.info(f"Trade date mode: fetching all industries for {trade_date}")
                data = self.router.route(
                    asset_class="index",
                    data_type="sw_daily",
                    method_name="get_sw_daily",
                    trade_date=trade_date,
                )

                if data is not None and not data.empty:
                    inserted = await self.data_ops.insert_sw_daily_batch(data)
                    total_records += inserted
                    logger.info(f"Inserted {inserted} sw_daily records for trade_date={trade_date}")
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

                    api_start = symbol_start_date
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
                    continue

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
        if index_list is None:
            index_list = SUPPORTED_INDEX_WEIGHT_CODES

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
        if trade_date and (start_date or end_date):
            raise ValueError("trade_date cannot be used with start_date or end_date")

        request_start_date, request_end_date = _normalize_futures_query_dates(
            trade_date,
            start_date,
            end_date,
        )
        if request_end_date is None:
            request_end_date = datetime.now().strftime("%Y-%m-%d")

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
            f"(freq={freq}, max_workers={max_workers})"
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
                    actual_start = self._incremental_start(
                        latest, request_start_date, force_update
                    )
                    if actual_start and request_end_date and actual_start > request_end_date:
                        return {"symbol": symbol, "status": "up_to_date", "records": 0}

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
            logger.warning(
                f"Futures minute update completed with {len(failed_symbols)} "
                f"failed symbols out of {len(symbols)}: {sample}{suffix}"
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

            prices = curve["price"].astype(float)
            diffs = prices.diff().dropna()
            trend = prices.iloc[-1] - prices.iloc[0] if len(prices) >= 2 else 0
            if len(prices) < 2:
                flag = 0
            elif (diffs > 0).all():
                flag = -1
            elif (diffs < 0).all():
                flag = 1
            elif trend > 0:
                flag = -0.5
            elif trend < 0:
                flag = 0.5
            else:
                flag = 0

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
                primary_expiry = primary.get("last_ddate") or primary.get(
                    "delivery_month_start"
                )
                secondary_expiry = secondary.get("last_ddate") or secondary.get(
                    "delivery_month_start"
                )
            else:
                primary_expiry = None
                secondary_expiry = None

            if pd.notna(primary_expiry) and pd.notna(secondary_expiry) and primary_price:
                primary_expiry = pd.Timestamp(primary_expiry)
                secondary_expiry = pd.Timestamp(secondary_expiry)
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
