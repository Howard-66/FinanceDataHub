"""
数据更新器

集成Provider、Router和数据库操作，实现完整的数据更新流程。
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from loguru import logger

from finance_data_hub.router.smart_router import SmartRouter
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.config import Settings


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

    async def update_stock_basic(self, market: Optional[str] = None) -> int:
        """
        更新股票基本信息

        Args:
            market: 市场代码（SH/SZ）

        Returns:
            int: 更新的记录数
        """
        logger.info(f"Updating stock basic info (market={market})")

        try:
            # 从路由器获取数据
            data = self.router.route(
                asset_class="stock",
                data_type="basic",
                method_name="get_stock_basic",
                market=market,
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
        force_update: bool = False,
    ) -> int:
        """
        更新日线数据

        Args:
            symbols: 股票代码列表，None表示全部
            start_date: 开始日期，None表示智能下载（查询数据库自动计算）
            end_date: 结束日期，为None时使用今天
            adj: 复权类型
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        if not symbols:
            # 如果没有指定股票，获取所有股票
            symbols = await self.data_ops.get_symbol_list()

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating daily data for {len(symbols)} symbols "
            f"from {start_date} to {end_date} (adj={adj}, force={force_update})"
        )

        total_records = 0

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
    ) -> int:
        """
        更新分钟数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期，None表示智能下载
            end_date: 结束日期
            freq: 频率
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        if not symbols:
            # 限制股票数量（分钟数据量很大）
            symbols = await self.data_ops.get_symbol_list(limit=10)

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating {freq} data for {len(symbols)} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
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
        force_update: bool = False,
    ) -> int:
        """
        更新每日指标数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期，None表示智能下载
            end_date: 结束日期
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        if not symbols:
            symbols = await self.data_ops.get_symbol_list()

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
        force_update: bool = False,
    ) -> int:
        """
        更新复权因子数据

        Args:
            symbols: 股票代码列表，None表示全部
            start_date: 开始日期，None表示智能下载
            end_date: 结束日期
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        if not symbols:
            # 如果没有指定股票，获取所有股票
            symbols = await self.data_ops.get_symbol_list()

        if not symbols:
            logger.warning("No symbols to update")
            return 0

        # 确定日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating adj_factor for {len(symbols)} symbols "
            f"from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        total_records = 0
        skipped_count = 0

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
                data = self.router.route(
                    asset_class="stock",
                    data_type="adj_factor",
                    method_name="get_adj_factor",
                    symbol=symbol,
                    start_date=symbol_start_date,
                    end_date=end_date,
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
                start_q=actual_start_date,
                end_q=end_date,
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
