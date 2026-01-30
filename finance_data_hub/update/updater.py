"""
数据更新器

集成Provider、Router和数据库操作，实现完整的数据更新流程。
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path
import time
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
                start_m=actual_start_date,
                end_m=end_date,
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
                start_m=actual_start_date,
                end_m=end_date,
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
                start_m=actual_start_date,
                end_m=end_date,
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
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_update: bool = False,
    ) -> int:
        """
        更新大盘指数每日指标数据

        支持的指数：上证综指（000001.SH）、深证成指（399001.SZ）、上证50（000016.SH）、
        中证500（000905.SH）、中小板指（399005.SZ）、创业板指（399006.SZ）

        Args:
            ts_code: 指数代码，None表示所有支持的指数
            start_date: 开始日期（YYYY-MM-DD格式），仅当指定ts_code时有效
            end_date: 结束日期，None表示到最新
            force_update: 是否强制更新（忽略数据库状态）

        Returns:
            int: 更新的记录数
        """
        # 确定日期
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(
            f"Updating index_dailybasic for {ts_code or 'all indexes'} from {start_date or 'smart'} to {end_date} (force={force_update})"
        )

        try:
            # 判断使用哪种模式
            if ts_code:
                # 指定了指数代码，使用历史数据模式
                if not start_date:
                    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
                    logger.info(f"No start_date specified, defaulting to {start_date}")

                logger.info(f"Historical mode: fetching {ts_code} data from {start_date} to {end_date}")

                api_start = start_date.replace("-", "") if start_date else None
                api_end = end_date.replace("-", "") if end_date else None

                data = self.router.route(
                    asset_class="index",
                    data_type="dailybasic",
                    method_name="get_index_dailybasic",
                    ts_code=ts_code,
                    start_date=api_start,
                    end_date=api_end,
                )
            else:
                # 未指定指数代码，智能下载模式
                if force_update:
                    # 强制更新：获取全量历史数据（从最早到最新）
                    logger.info("Force update: fetching full history")
                    api_start = start_date.replace("-", "") if start_date else None
                    api_end = end_date.replace("-", "") if end_date else None
                    data = self.router.route(
                        asset_class="index",
                        data_type="dailybasic",
                        method_name="get_index_dailybasic",
                        start_date=api_start,
                        end_date=api_end,
                    )
                else:
                    # 智能下载：检查数据库状态
                    latest_date = await self.data_ops.get_latest_index_dailybasic_date(None)

                    if latest_date:
                        # 有记录，增量更新：从最新日期的下一天到最新
                        next_day = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
                        if next_day.strftime("%Y-%m-%d") > end_date:
                            logger.info("Index dailybasic data is already up to date")
                            return 0
                        api_start = next_day.strftime("%Y%m%d")
                        api_end = end_date.replace("-", "")
                        logger.info(f"Smart incremental: fetching from {next_day} to {end_date}")
                        data = self.router.route(
                            asset_class="index",
                            data_type="dailybasic",
                            method_name="get_index_dailybasic",
                            start_date=api_start,
                            end_date=api_end,
                        )
                    else:
                        # 数据库为空，获取全量历史数据
                        logger.info("Smart download: database empty, fetching full history")
                        api_end = end_date.replace("-", "") if end_date else None
                        data = self.router.route(
                            asset_class="index",
                            data_type="dailybasic",
                            method_name="get_index_dailybasic",
                            end_date=api_end,
                        )

            if data is None or data.empty:
                logger.warning("No index_dailybasic data received")
                return 0

            # 插入数据库
            inserted_count = await self.data_ops.insert_index_dailybasic_batch(data)

            logger.info(f"Updated {inserted_count} index_dailybasic records")
            return inserted_count

        except Exception as e:
            logger.exception("Failed to update index_dailybasic data")
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
