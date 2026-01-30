"""
数据操作类

提供对TimescaleDB表的CRUD操作。
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from loguru import logger

from finance_data_hub.database.manager import DatabaseManager


def _normalize_datetime_for_db(value, data_type="daily"):
    """
    将pandas Timestamp或字符串转换为带时区的Python datetime

    Args:
        value: pandas Timestamp、datetime对象或字符串
        data_type: 数据类型 ('daily', 'minute', 'daily_basic', 'adj_factor')

    Returns:
        datetime: 带Asia/Shanghai时区的datetime对象
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        # Python < 3.9, 使用pytz
        import pytz
        ZoneInfo = lambda tz: pytz.timezone(tz)

    # 如果是字符串，先转换为 pd.Timestamp
    if isinstance(value, str):
        value = pd.to_datetime(value)

    if isinstance(value, pd.Timestamp):
        # 中国股市使用Asia/Shanghai时区
        china_tz = ZoneInfo('Asia/Shanghai')

        if value.tz is None:
            # 无时区信息，根据数据类型处理
            if data_type == "daily" or data_type == "daily_basic" or data_type == "adj_factor":
                # 日线数据：设置为收盘时间 15:00:00
                value = value.replace(hour=15, minute=0, second=0, microsecond=0)
            # else: minute数据保持原时间

            # 本地化为中国时间
            return value.tz_localize(china_tz).to_pydatetime()
        else:
            # 已有时区，转换为中国时间
            # xtquant数据已经转换为Asia/Shanghai时区，直接使用
            if str(value.tz) == 'Asia/Shanghai':
                return value.to_pydatetime()
            else:
                return value.tz_convert(china_tz).to_pydatetime()
    return value


class DataOperations:
    """数据操作类"""

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化数据操作类

        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager

    async def insert_symbol_daily_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入日线数据

        Args:
            data: 包含日线数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数

        Raises:
            Exception: 插入失败时抛出
        """
        if data.empty:
            return 0

        # 准备插入语句
        insert_sql = """
            INSERT INTO symbol_daily (
                time, symbol, open, high, low, close,
                volume, amount, adj_factor, open_interest,
                settle, change_pct, change_amount
            )
            VALUES (
                :time, :symbol, :open, :high, :low, :close,
                :volume, :amount, :adj_factor, :open_interest,
                :settle, :change_pct, :change_amount
            )
            ON CONFLICT (symbol, time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                adj_factor = EXCLUDED.adj_factor,
                open_interest = EXCLUDED.open_interest,
                settle = EXCLUDED.settle,
                change_pct = EXCLUDED.change_pct,
                change_amount = EXCLUDED.change_amount,
                updated_at = NOW()
        """

        total_inserted = 0

        # 按批插入
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]

            # 转换为记录列表
            records = batch.to_dict("records")

            # 处理NaT值，转换为None；处理时间戳为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")

            # 确保所有必需字段都存在，缺失的字段设置为None
            required_fields = {
                "adj_factor": None,
                "open_interest": None,
                "settle": None,
                "change_pct": None,
                "change_amount": None,
            }
            for record in records:
                for field, default_value in required_fields.items():
                    if field not in record:
                        record[field] = default_value

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} records to symbol_daily")
        return total_inserted

    async def insert_symbol_minute_batch(
        self, data: pd.DataFrame, batch_size: int = 1000, freq: str = "1m"
    ) -> int:
        """
        批量插入分钟数据

        Args:
            data: 包含分钟数据的DataFrame
            batch_size: 批处理大小
            freq: 数据频率（1m, 5m, 15m, 30m, 60m）

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO symbol_minute (
                time, symbol, frequency, open, high, low, close,
                volume, amount
            )
            VALUES (
                :time, :symbol, :frequency, :open, :high, :low, :close,
                :volume, :amount
            )
            ON CONFLICT (symbol, time, frequency) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                updated_at = NOW()
        """

        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaT值，转换为None；处理时间戳为带时区的datetime
            for record in records:
                # 添加 frequency 字段（如果不存在）
                if "frequency" not in record:
                    record["frequency"] = freq

                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="minute")

            # 清理DataFrame中不属于symbol_minute表的字段
            valid_fields = {"time", "symbol", "frequency", "open", "high", "low", "close", "volume", "amount"}
            for record in records:
                # 删除不属于表结构的字段
                fields_to_remove = [key for key in record.keys() if key not in valid_fields and key != "time"]
                for field in fields_to_remove:
                    del record[field]

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} records to symbol_minute (freq={freq})")
        return total_inserted

    async def insert_asset_basic_batch(self, data: pd.DataFrame) -> int:
        """
        批量插入资产基本信息

        Args:
            data: 包含资产基本信息的DataFrame

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO asset_basic (
                symbol, name, market, industry, area,
                list_status, list_date, delist_date, is_hs
            )
            VALUES (
                :symbol, :name, :market, :industry, :area,
                :list_status, :list_date, :delist_date, :is_hs
            )
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                market = EXCLUDED.market,
                industry = EXCLUDED.industry,
                area = EXCLUDED.area,
                list_status = EXCLUDED.list_status,
                list_date = EXCLUDED.list_date,
                delist_date = EXCLUDED.delist_date,
                is_hs = EXCLUDED.is_hs,
                updated_at = NOW()
        """

        records = data.to_dict("records")

        # 处理NaT值，转换为None（SQLAlchemy会将None转换为NULL）
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        async with self.db_manager._engine.begin() as conn:
            await conn.execute(text(insert_sql), records)

        # PostgreSQL ON CONFLICT 可能返回 -1，使用实际的记录数更可靠
        actual_count = len(records)
        logger.info(f"Inserted/updated {actual_count} asset_basic records (batch: {len(records)})")
        return actual_count

    async def insert_daily_basic_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入每日指标数据

        Args:
            data: 包含每日指标数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO daily_basic (
                time, symbol, turnover_rate, volume_ratio, pe, pe_ttm,
                pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share,
                float_share, free_share, total_mv, circ_mv
            )
            VALUES (
                :time, :symbol, :turnover_rate, :volume_ratio, :pe, :pe_ttm,
                :pb, :ps, :ps_ttm, :dv_ratio, :dv_ttm, :total_share,
                :float_share, :free_share, :total_mv, :circ_mv
            )
            ON CONFLICT (time, symbol) DO UPDATE SET
                turnover_rate = EXCLUDED.turnover_rate,
                volume_ratio = EXCLUDED.volume_ratio,
                pe = EXCLUDED.pe,
                pe_ttm = EXCLUDED.pe_ttm,
                pb = EXCLUDED.pb,
                ps = EXCLUDED.ps,
                ps_ttm = EXCLUDED.ps_ttm,
                dv_ratio = EXCLUDED.dv_ratio,
                dv_ttm = EXCLUDED.dv_ttm,
                total_share = EXCLUDED.total_share,
                float_share = EXCLUDED.float_share,
                free_share = EXCLUDED.free_share,
                total_mv = EXCLUDED.total_mv,
                circ_mv = EXCLUDED.circ_mv,
                updated_at = NOW()
        """

        total_inserted = 0

        # 移除重复列名，避免 to_dict 警告
        data = data.loc[:, ~data.columns.duplicated()]

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaT值，转换为None；处理时间戳为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="daily_basic")

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} daily_basic records")
        return total_inserted

    async def get_latest_data_date(
        self, symbol: str, table: str = "symbol_daily"
    ) -> Optional[datetime]:
        """
        获取指定股票的最新数据日期

        Args:
            symbol: 股票代码
            table: 表名

        Returns:
            Optional[datetime]: 最新数据日期，不存在返回None
        """
        query = text(
            f"SELECT MAX(time) as latest_time FROM {table} WHERE symbol = :symbol"
        )

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(query, {"symbol": symbol})
            row = result.fetchone()

        return row.latest_time if row and row.latest_time else None

    async def get_latest_data_date_no_symbol(self, table: str) -> Optional[datetime]:
        """
        获取指定表的最新数据日期（适用于没有symbol列的表，如cn_gdp）

        Args:
            table: 表名

        Returns:
            Optional[datetime]: 最新数据日期，不存在返回None
        """
        query = text(f"SELECT MAX(time) as latest_time FROM {table}")

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(query)
            row = result.fetchone()

        return row.latest_time if row and row.latest_time else None

    async def get_symbol_list(
        self, market: Optional[str] = None, limit: Optional[int] = None
    ) -> List[str]:
        """
        获取股票代码列表

        Args:
            market: 市场代码（SH/SZ）
            limit: 限制返回数量

        Returns:
            List[str]: 股票代码列表
        """
        query = "SELECT symbol FROM asset_basic WHERE list_status = 'L'"
        params = {}

        if market:
            query += " AND market = :market"
            params["market"] = market

        if limit:
            query += " LIMIT :limit"
            params["limit"] = limit

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        return [row.symbol for row in rows]


    async def get_symbol_daily(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取日线 OHLCV 数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 日线数据，包含 time, symbol, open, high, low, close, volume, amount, adj_factor 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        start_dt = _normalize_datetime_for_db(start_date, "daily")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily")

        query = text("""
            SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
            FROM symbol_daily
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_symbol_minute(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        frequency: str = "minute_1"
    ) -> Optional[pd.DataFrame]:
        """
        获取分钟级 OHLCV 数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率，支持 minute_1, minute_5, minute_15, minute_30, minute_60

        Returns:
            Optional[pd.DataFrame]: 分钟数据，包含 time, symbol, open, high, low, close, volume, amount, frequency 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        start_dt = _normalize_datetime_for_db(start_date, "minute")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "minute")

        query = text("""
            SELECT time, symbol, open, high, low, close, volume, amount, frequency
            FROM symbol_minute
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            AND frequency = :frequency
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                    "frequency": frequency,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_daily_basic(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取每日基本面指标数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 每日基本面数据，包含 time, symbol, turnover_rate, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        start_dt = _normalize_datetime_for_db(start_date, "daily_basic")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily_basic")

        query = text("""
            SELECT time, symbol, turnover_rate, volume_ratio, pe, pe_ttm,
                   pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share,
                   float_share, free_share, total_mv, circ_mv
            FROM daily_basic
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_asset_basic(
        self,
        symbols: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取股票基本信息

        Args:
            symbols: 股票代码列表，如果为 None 则获取所有股票

        Returns:
            Optional[pd.DataFrame]: 股票基本信息，包含 symbol, name, area, industry, market, exchange, list_status, list_date, delist_date, is_hs 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        params = {}
        query = "SELECT symbol, name, area, industry, market, list_status, list_date, delist_date, is_hs FROM asset_basic WHERE 1=1"

        if symbols:
            query += " AND symbol = ANY(:symbols)"
            params["symbols"] = symbols

        query += " ORDER BY symbol"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data


    async def check_table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            bool: 表是否存在
        """
        query = text(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table_name
            ) as exists
        """
        )

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(query, {"table_name": table_name})
            row = result.fetchone()

        return row.exists if row else False

    async def insert_adj_factor_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入复权因子数据

        Args:
            data: 包含复权因子数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO adj_factor (
                symbol, time, adj_factor
            )
            VALUES (
                :symbol, :time, :adj_factor
            )
            ON CONFLICT (symbol, time) DO UPDATE SET
                adj_factor = EXCLUDED.adj_factor,
                updated_at = NOW()
        """

        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaT值，转换为None；处理时间戳为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="adj_factor")

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} adj_factor records")
        return total_inserted

    async def get_adj_factor(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取指定时间范围内的复权因子

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 复权因子数据，包含 time, symbol, adj_factor 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        start_dt = _normalize_datetime_for_db(start_date, "adj_factor")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "adj_factor")

        query = text("""
            SELECT time, symbol, adj_factor
            FROM adj_factor
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_weekly_data(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的 OHLCV 数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线数据，包含 time, symbol, open, high, low, close, volume, amount, adj_factor 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily")

        query = text("""
            SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
            FROM symbol_weekly
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_monthly_data(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的 OHLCV 数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线数据，包含 time, symbol, open, high, low, close, volume, amount, adj_factor 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily")

        query = text("""
            SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
            FROM symbol_monthly
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_daily_basic_weekly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的每日基础指标

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线基础指标数据，包含聚合后的各种指标列
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily_basic")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily_basic")

        query = text("""
            SELECT time, symbol, avg_turnover_rate, avg_volume_ratio, avg_pe, avg_pe_ttm,
                   avg_pb, avg_ps, avg_ps_ttm, avg_dv_ratio, avg_dv_ttm,
                   total_share, float_share, free_share, total_mv, circ_mv
            FROM daily_basic_weekly
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_daily_basic_monthly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的每日基础指标

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线基础指标数据，包含聚合后的各种指标列
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily_basic")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily_basic")

        query = text("""
            SELECT time, symbol, avg_turnover_rate, avg_volume_ratio, avg_pe, avg_pe_ttm,
                   avg_pb, avg_ps, avg_ps_ttm, avg_dv_ratio, avg_dv_ttm,
                   total_share, float_share, free_share, total_mv, circ_mv
            FROM daily_basic_monthly
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_adj_factor_weekly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的复权因子数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线复权因子数据，包含 time, symbol, adj_factor 列
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "adj_factor")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "adj_factor")

        query = text("""
            SELECT time, symbol, adj_factor
            FROM adj_factor_weekly
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_adj_factor_monthly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的复权因子数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线复权因子数据，包含 time, symbol, adj_factor 列
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "adj_factor")
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "adj_factor")

        query = text("""
            SELECT time, symbol, adj_factor
            FROM adj_factor_monthly
            WHERE symbol = ANY(:symbols)
            AND time BETWEEN :start_date AND :end_date
            ORDER BY symbol, time
        """)

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def insert_cn_gdp_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入中国GDP数据

        Args:
            data: 包含GDP数据的DataFrame，包含time（季度末日期）和quarter字段
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO cn_gdp (
                time, quarter, gdp, gdp_yoy, pi, pi_yoy, si, si_yoy, ti, ti_yoy
            )
            VALUES (
                :time, :quarter, :gdp, :gdp_yoy, :pi, :pi_yoy, :si, :si_yoy, :ti, :ti_yoy
            )
            ON CONFLICT (time) DO UPDATE SET
                quarter = EXCLUDED.quarter,
                gdp = EXCLUDED.gdp,
                gdp_yoy = EXCLUDED.gdp_yoy,
                pi = EXCLUDED.pi,
                pi_yoy = EXCLUDED.pi_yoy,
                si = EXCLUDED.si,
                si_yoy = EXCLUDED.si_yoy,
                ti = EXCLUDED.ti,
                ti_yoy = EXCLUDED.ti_yoy,
                updated_at = NOW()
        """

        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaN值，转换为None；处理time字段为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} cn_gdp records")
        return total_inserted

    async def get_cn_gdp(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国GDP数据

        Args:
            start_date: 开始日期（季度末日期格式，如2020-03-31表示2020Q1）
            end_date: 结束日期（季度末日期格式，如2024-12-31表示2024Q4）

        Returns:
            Optional[pd.DataFrame]: GDP数据，包含 time, quarter, gdp, gdp_yoy, pi, pi_yoy, si, si_yoy, ti, ti_yoy 列
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = """
            SELECT time, quarter, gdp, gdp_yoy, pi, pi_yoy, si, si_yoy, ti, ti_yoy
            FROM cn_gdp
            WHERE 1=1
        """
        params = {}

        if start_dt:
            query += " AND time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def insert_cn_ppi_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入中国PPI数据

        Args:
            data: 包含PPI数据的DataFrame，包含time（月份末日期）和month字段
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO cn_ppi (
                time, month, ppi_yoy, ppi_mp_yoy, ppi_mp_qm_yoy, ppi_mp_rm_yoy, ppi_mp_p_yoy,
                ppi_cg_yoy, ppi_cg_f_yoy, ppi_cg_c_yoy, ppi_cg_adu_yoy, ppi_cg_dcg_yoy,
                ppi_mom, ppi_mp_mom, ppi_mp_qm_mom, ppi_mp_rm_mom, ppi_mp_p_mom,
                ppi_cg_mom, ppi_cg_f_mom, ppi_cg_c_mom, ppi_cg_adu_mom, ppi_cg_dcg_mom,
                ppi_accu, ppi_mp_accu, ppi_mp_qm_accu, ppi_mp_rm_accu, ppi_mp_p_accu,
                ppi_cg_accu, ppi_cg_f_accu, ppi_cg_c_accu, ppi_cg_adu_accu, ppi_cg_dcg_accu
            )
            VALUES (
                :time, :month, :ppi_yoy, :ppi_mp_yoy, :ppi_mp_qm_yoy, :ppi_mp_rm_yoy, :ppi_mp_p_yoy,
                :ppi_cg_yoy, :ppi_cg_f_yoy, :ppi_cg_c_yoy, :ppi_cg_adu_yoy, :ppi_cg_dcg_yoy,
                :ppi_mom, :ppi_mp_mom, :ppi_mp_qm_mom, :ppi_mp_rm_mom, :ppi_mp_p_mom,
                :ppi_cg_mom, :ppi_cg_f_mom, :ppi_cg_c_mom, :ppi_cg_adu_mom, :ppi_cg_dcg_mom,
                :ppi_accu, :ppi_mp_accu, :ppi_mp_qm_accu, :ppi_mp_rm_accu, :ppi_mp_p_accu,
                :ppi_cg_accu, :ppi_cg_f_accu, :ppi_cg_c_accu, :ppi_cg_adu_accu, :ppi_cg_dcg_accu
            )
            ON CONFLICT (time) DO UPDATE SET
                month = EXCLUDED.month,
                ppi_yoy = EXCLUDED.ppi_yoy,
                ppi_mp_yoy = EXCLUDED.ppi_mp_yoy,
                ppi_mp_qm_yoy = EXCLUDED.ppi_mp_qm_yoy,
                ppi_mp_rm_yoy = EXCLUDED.ppi_mp_rm_yoy,
                ppi_mp_p_yoy = EXCLUDED.ppi_mp_p_yoy,
                ppi_cg_yoy = EXCLUDED.ppi_cg_yoy,
                ppi_cg_f_yoy = EXCLUDED.ppi_cg_f_yoy,
                ppi_cg_c_yoy = EXCLUDED.ppi_cg_c_yoy,
                ppi_cg_adu_yoy = EXCLUDED.ppi_cg_adu_yoy,
                ppi_cg_dcg_yoy = EXCLUDED.ppi_cg_dcg_yoy,
                ppi_mom = EXCLUDED.ppi_mom,
                ppi_mp_mom = EXCLUDED.ppi_mp_mom,
                ppi_mp_qm_mom = EXCLUDED.ppi_mp_qm_mom,
                ppi_mp_rm_mom = EXCLUDED.ppi_mp_rm_mom,
                ppi_mp_p_mom = EXCLUDED.ppi_mp_p_mom,
                ppi_cg_mom = EXCLUDED.ppi_cg_mom,
                ppi_cg_f_mom = EXCLUDED.ppi_cg_f_mom,
                ppi_cg_c_mom = EXCLUDED.ppi_cg_c_mom,
                ppi_cg_adu_mom = EXCLUDED.ppi_cg_adu_mom,
                ppi_cg_dcg_mom = EXCLUDED.ppi_cg_dcg_mom,
                ppi_accu = EXCLUDED.ppi_accu,
                ppi_mp_accu = EXCLUDED.ppi_mp_accu,
                ppi_mp_qm_accu = EXCLUDED.ppi_mp_qm_accu,
                ppi_mp_rm_accu = EXCLUDED.ppi_mp_rm_accu,
                ppi_mp_p_accu = EXCLUDED.ppi_mp_p_accu,
                ppi_cg_accu = EXCLUDED.ppi_cg_accu,
                ppi_cg_f_accu = EXCLUDED.ppi_cg_f_accu,
                ppi_cg_c_accu = EXCLUDED.ppi_cg_c_accu,
                ppi_cg_adu_accu = EXCLUDED.ppi_cg_adu_accu,
                ppi_cg_dcg_accu = EXCLUDED.ppi_cg_dcg_accu,
                updated_at = NOW()
        """

        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaN值，转换为None；处理time字段为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} cn_ppi records")
        return total_inserted

    async def get_cn_ppi(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国PPI数据

        Args:
            start_date: 开始日期（月份末日期格式，如2020-01-31表示2020年1月）
            end_date: 结束日期（月份末日期格式，如2024-12-31表示2024年12月）

        Returns:
            Optional[pd.DataFrame]: PPI数据，包含time, month及所有PPI指标字段
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = """
            SELECT time, month, ppi_yoy, ppi_mp_yoy, ppi_mp_qm_yoy, ppi_mp_rm_yoy, ppi_mp_p_yoy,
                   ppi_cg_yoy, ppi_cg_f_yoy, ppi_cg_c_yoy, ppi_cg_adu_yoy, ppi_cg_dcg_yoy,
                   ppi_mom, ppi_mp_mom, ppi_mp_qm_mom, ppi_mp_rm_mom, ppi_mp_p_mom,
                   ppi_cg_mom, ppi_cg_f_mom, ppi_cg_c_mom, ppi_cg_adu_mom, ppi_cg_dcg_mom,
                   ppi_accu, ppi_mp_accu, ppi_mp_qm_accu, ppi_mp_rm_accu, ppi_mp_p_accu,
                   ppi_cg_accu, ppi_cg_f_accu, ppi_cg_c_accu, ppi_cg_adu_accu, ppi_cg_dcg_accu
            FROM cn_ppi
            WHERE 1=1
        """
        params = {}

        if start_dt:
            query += " AND time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def insert_cn_m_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入中国货币供应量数据

        Args:
            data: 包含货币供应量数据的DataFrame，包含time（月份末日期）和month字段
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO cn_m (
                time, month, m0, m0_yoy, m0_mom, m1, m1_yoy, m1_mom, m2, m2_yoy, m2_mom
            )
            VALUES (
                :time, :month, :m0, :m0_yoy, :m0_mom, :m1, :m1_yoy, :m1_mom, :m2, :m2_yoy, :m2_mom
            )
            ON CONFLICT (time) DO UPDATE SET
                month = EXCLUDED.month,
                m0 = EXCLUDED.m0,
                m0_yoy = EXCLUDED.m0_yoy,
                m0_mom = EXCLUDED.m0_mom,
                m1 = EXCLUDED.m1,
                m1_yoy = EXCLUDED.m1_yoy,
                m1_mom = EXCLUDED.m1_mom,
                m2 = EXCLUDED.m2,
                m2_yoy = EXCLUDED.m2_yoy,
                m2_mom = EXCLUDED.m2_mom,
                updated_at = NOW()
        """

        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaN值，转换为None；处理time字段为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} cn_m records")
        return total_inserted

    async def get_cn_m(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国货币供应量数据

        Args:
            start_date: 开始日期（月份末日期格式，如2020-01-31表示2020年1月）
            end_date: 结束日期（月份末日期格式，如2024-12-31表示2024年12月）

        Returns:
            Optional[pd.DataFrame]: 货币供应量数据，包含time, month及所有指标字段
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = """
            SELECT time, month, m0, m0_yoy, m0_mom, m1, m1_yoy, m1_mom, m2, m2_yoy, m2_mom
            FROM cn_m
            WHERE 1=1
        """
        params = {}

        if start_dt:
            query += " AND time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def insert_cn_pmi_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入中国PMI数据

        Args:
            data: 包含PMI数据的DataFrame，包含time（月份末日期）和month字段
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO cn_pmi (
                time, month, pmi010000, pmi010100, pmi010200, pmi010300, pmi010400,
                pmi010500, pmi010600, pmi010700, pmi010800, pmi010900, pmi011000,
                pmi011100, pmi011200, pmi011300, pmi011400, pmi011500, pmi011600,
                pmi011700, pmi011800, pmi011900, pmi012000, pmi020100, pmi020200,
                pmi020300, pmi020400, pmi020500, pmi020600, pmi020700, pmi020800,
                pmi020900, pmi021000, pmi030000
            )
            VALUES (
                :time, :month, :pmi010000, :pmi010100, :pmi010200, :pmi010300, :pmi010400,
                :pmi010500, :pmi010600, :pmi010700, :pmi010800, :pmi010900, :pmi011000,
                :pmi011100, :pmi011200, :pmi011300, :pmi011400, :pmi011500, :pmi011600,
                :pmi011700, :pmi011800, :pmi011900, :pmi012000, :pmi020100, :pmi020200,
                :pmi020300, :pmi020400, :pmi020500, :pmi020600, :pmi020700, :pmi020800,
                :pmi020900, :pmi021000, :pmi030000
            )
            ON CONFLICT (time) DO UPDATE SET
                month = EXCLUDED.month,
                pmi010000 = EXCLUDED.pmi010000,
                pmi010100 = EXCLUDED.pmi010100,
                pmi010200 = EXCLUDED.pmi010200,
                pmi010300 = EXCLUDED.pmi010300,
                pmi010400 = EXCLUDED.pmi010400,
                pmi010500 = EXCLUDED.pmi010500,
                pmi010600 = EXCLUDED.pmi010600,
                pmi010700 = EXCLUDED.pmi010700,
                pmi010800 = EXCLUDED.pmi010800,
                pmi010900 = EXCLUDED.pmi010900,
                pmi011000 = EXCLUDED.pmi011000,
                pmi011100 = EXCLUDED.pmi011100,
                pmi011200 = EXCLUDED.pmi011200,
                pmi011300 = EXCLUDED.pmi011300,
                pmi011400 = EXCLUDED.pmi011400,
                pmi011500 = EXCLUDED.pmi011500,
                pmi011600 = EXCLUDED.pmi011600,
                pmi011700 = EXCLUDED.pmi011700,
                pmi011800 = EXCLUDED.pmi011800,
                pmi011900 = EXCLUDED.pmi011900,
                pmi012000 = EXCLUDED.pmi012000,
                pmi020100 = EXCLUDED.pmi020100,
                pmi020200 = EXCLUDED.pmi020200,
                pmi020300 = EXCLUDED.pmi020300,
                pmi020400 = EXCLUDED.pmi020400,
                pmi020500 = EXCLUDED.pmi020500,
                pmi020600 = EXCLUDED.pmi020600,
                pmi020700 = EXCLUDED.pmi020700,
                pmi020800 = EXCLUDED.pmi020800,
                pmi020900 = EXCLUDED.pmi020900,
                pmi021000 = EXCLUDED.pmi021000,
                pmi030000 = EXCLUDED.pmi030000,
                updated_at = NOW()
        """

        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]
            records = batch.to_dict("records")

            # 处理NaN值，转换为None；处理time字段为带时区的datetime
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "time" or isinstance(value, pd.Timestamp):
                        # 转换时间戳为带时区的Python datetime
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} cn_pmi records")
        return total_inserted

    async def get_cn_pmi(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国PMI数据

        Args:
            start_date: 开始日期（月份末日期格式，如2020-01-31表示2020年1月）
            end_date: 结束日期（月份末日期格式，如2024-12-31表示2024年12月）

        Returns:
            Optional[pd.DataFrame]: PMI数据，包含time, month及所有指标字段
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = """
            SELECT time, month, pmi010000, pmi010100, pmi010200, pmi010300, pmi010400,
                   pmi010500, pmi010600, pmi010700, pmi010800, pmi010900, pmi011000,
                   pmi011100, pmi011200, pmi011300, pmi011400, pmi011500, pmi011600,
                   pmi011700, pmi011800, pmi011900, pmi012000, pmi020100, pmi020200,
                   pmi020300, pmi020400, pmi020500, pmi020600, pmi020700, pmi020800,
                   pmi020900, pmi021000, pmi030000
            FROM cn_pmi
            WHERE 1=1
        """
        params = {}

        if start_dt:
            query += " AND time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def insert_index_dailybasic_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入大盘指数每日指标数据

        Args:
            data: 包含指数每日指标数据的DataFrame
            batch_size: 每批插入的记录数

        Returns:
            int: 成功插入的记录数
        """
        if data.empty:
            return 0

        # 确保数据按日期排序
        data = data.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

        # 准备列名和占位符
        columns = [
            "ts_code",
            "trade_date",
            "total_mv",
            "float_mv",
            "total_share",
            "float_share",
            "free_share",
            "turnover_rate",
            "turnover_rate_f",
            "pe",
            "pe_ttm",
            "pb",
        ]

        total_inserted = 0
        n_batches = (len(data) + batch_size - 1) // batch_size

        for i in range(n_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            batch_df = data.iloc[start_idx:end_idx]

            # 构建插入语句（使用ON CONFLICT处理重复数据）
            placeholders = ", ".join([f":{col}" for col in columns])
            insert_sql = f"""
                INSERT INTO index_dailybasic (
                    {", ".join(columns)},
                    updated_at,
                    created_at
                ) VALUES (
                    {placeholders},
                    NOW(),
                    NOW()
                )
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    total_mv = EXCLUDED.total_mv,
                    float_mv = EXCLUDED.float_mv,
                    total_share = EXCLUDED.total_share,
                    float_share = EXCLUDED.float_share,
                    free_share = EXCLUDED.free_share,
                    turnover_rate = EXCLUDED.turnover_rate,
                    turnover_rate_f = EXCLUDED.turnover_rate_f,
                    pe = EXCLUDED.pe,
                    pe_ttm = EXCLUDED.pe_ttm,
                    pb = EXCLUDED.pb,
                    updated_at = NOW()
            """

            # 转换为字典列表
            records = batch_df[columns].to_dict(orient="records")

            # 转换日期为datetime对象（PostgreSQL TIMESTAMPTZ格式）
            # 使用统一的日期规范化函数，确保添加Asia/Shanghai时区
            for record in records:
                record["trade_date"] = _normalize_datetime_for_db(record["trade_date"], "daily")

            async with self.db_manager._engine.begin() as conn:
                await conn.execute(text(insert_sql), records)

            batch_inserted = len(batch_df)
            total_inserted += batch_inserted

        logger.info(f"Total inserted {total_inserted} index_dailybasic records")
        return total_inserted

    async def get_index_dailybasic(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取大盘指数每日指标数据

        Args:
            ts_code: 指数代码（如000001.SH），None表示所有指数
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）

        Returns:
            Optional[pd.DataFrame]: 指数每日指标数据
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = """
            SELECT ts_code, trade_date, total_mv, float_mv, total_share,
                   float_share, free_share, turnover_rate, turnover_rate_f,
                   pe, pe_ttm, pb
            FROM index_dailybasic
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_dt:
            query += " AND trade_date >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND trade_date <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY trade_date, ts_code"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])

        # 处理 trade_date 时区：直接转换为 Asia/Shanghai
        if "trade_date" in data.columns and not data.empty:
            trade_dates = pd.to_datetime(data["trade_date"])
            if trade_dates.dt.tz is not None:
                # 已经是 tz-aware，直接转换时区
                data["trade_date"] = trade_dates.dt.tz_convert("Asia/Shanghai")
            else:
                # 无时区信息，本地化
                data["trade_date"] = trade_dates.dt.tz_localize("Asia/Shanghai")

        return data

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    async def insert_sw_daily_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入申万行业日线行情数据

        Args:
            data: 包含申万行业日线行情数据的DataFrame
            batch_size: 每批插入的记录数

        Returns:
            int: 成功插入的记录数
        """
        if data.empty:
            return 0

        # 确保数据按日期排序
        data = data.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

        # 准备列名和占位符
        columns = [
            "ts_code",
            "trade_date",
            "name",
            "open",
            "low",
            "high",
            "close",
            "change",
            "pct_change",
            "vol",
            "amount",
            "pe",
            "pb",
            "float_mv",
            "total_mv",
        ]

        total_inserted = 0
        n_batches = (len(data) + batch_size - 1) // batch_size

        for i in range(n_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            batch_df = data.iloc[start_idx:end_idx]

            # 构建插入语句（使用ON CONFLICT处理重复数据）
            placeholders = ", ".join([f":{col}" for col in columns])
            insert_sql = f"""
                INSERT INTO sw_daily (
                    {", ".join(columns)},
                    updated_at,
                    created_at
                ) VALUES (
                    {placeholders},
                    NOW(),
                    NOW()
                )
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    name = EXCLUDED.name,
                    open = EXCLUDED.open,
                    low = EXCLUDED.low,
                    high = EXCLUDED.high,
                    close = EXCLUDED.close,
                    change = EXCLUDED.change,
                    pct_change = EXCLUDED.pct_change,
                    vol = EXCLUDED.vol,
                    amount = EXCLUDED.amount,
                    pe = EXCLUDED.pe,
                    pb = EXCLUDED.pb,
                    float_mv = EXCLUDED.float_mv,
                    total_mv = EXCLUDED.total_mv,
                    updated_at = NOW()
            """

            # 转换为字典列表
            records = batch_df[columns].to_dict(orient="records")

            # 转换日期为datetime对象（PostgreSQL TIMESTAMPTZ格式）
            # 使用统一的日期规范化函数，确保添加Asia/Shanghai时区
            for record in records:
                record["trade_date"] = _normalize_datetime_for_db(record["trade_date"], "daily")

            async with self.db_manager._engine.begin() as conn:
                await conn.execute(text(insert_sql), records)

            batch_inserted = len(batch_df)
            total_inserted += batch_inserted

        logger.info(f"Total inserted {total_inserted} sw_daily records")
        return total_inserted

    async def get_sw_daily(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业日线行情数据

        Args:
            ts_code: 行业代码（如801780.SI），None表示所有行业
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）

        Returns:
            Optional[pd.DataFrame]: 申万行业日线行情数据
        """
        # 自动检查并初始化数据库连接（如果需要）
        if self.db_manager._engine is None:
            await self.db_manager.initialize()

        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = """
            SELECT ts_code, trade_date, name, open, low, high, close,
                   change, pct_change, vol, amount, pe, pb, float_mv, total_mv
            FROM sw_daily
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_dt:
            query += " AND trade_date >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND trade_date <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY trade_date, ts_code"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])

        # 处理 trade_date 时区：直接转换为 Asia/Shanghai
        if "trade_date" in data.columns and not data.empty:
            trade_dates = pd.to_datetime(data["trade_date"])
            if trade_dates.dt.tz is not None:
                # 已经是 tz-aware，直接转换时区
                data["trade_date"] = trade_dates.dt.tz_convert("Asia/Shanghai")
            else:
                # 无时区信息，本地化
                data["trade_date"] = trade_dates.dt.tz_localize("Asia/Shanghai")

        return data

    async def get_latest_sw_daily_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的申万行业日线行情日期

        Args:
            ts_code: 行业代码（如801780.SI），None表示任意行业

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(trade_date) as latest_date
            FROM sw_daily
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    async def insert_income_batch(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        批量插入利润表数据

        Args:
            df: 利润表数据DataFrame
            batch_size: 批量插入大小，默认1000

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 确保 end_date_time 列存在且格式正确
        if "end_date_time" not in df.columns:
            df["end_date_time"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

        # 验证时间列有有效值
        if df["end_date_time"].isna().all():
            logger.warning("All end_date_time values are NaT, skipping insert")
            return 0

        total_inserted = 0
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]
            try:
                inserted = await self._insert_income_with_retry(batch_df)
                total_inserted += inserted
            except Exception as e:
                logger.error(f"Failed to insert income batch {i // batch_size}: {e}")
                continue

        return total_inserted

    async def _insert_income_with_retry(self, df: pd.DataFrame, max_retries: int = 3) -> int:
        """
        带重试机制的插入利润表数据

        Args:
            df: 利润表数据DataFrame
            max_retries: 最大重试次数

        Returns:
            int: 插入的记录数
        """
        for attempt in range(max_retries):
            try:
                return await self.__insert_income_batch(df)
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Insert income batch failed (attempt {attempt + 1}/{max_retries}): {e}")
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    logger.error(f"Insert income batch failed after {max_retries} attempts: {e}")
                    raise

        return 0

    async def __insert_income_batch(self, df: pd.DataFrame) -> int:
        """
        内部方法：批量插入利润表数据到数据库

        Args:
            df: 利润表数据DataFrame

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 准备插入数据
        rows_to_insert = []
        for _, row in df.iterrows():
            row_data = {}
            for col in df.columns:
                val = row.get(col)
                if col == "end_date_time":
                    # 时间字段需要时区感知的datetime
                    if pd.notna(val):
                        try:
                            if hasattr(val, 'tzinfo') and val.tzinfo is None:
                                row_data[col] = val.tz_localize('UTC')
                            else:
                                row_data[col] = val
                        except (ValueError, TypeError):
                            row_data[col] = None
                    else:
                        row_data[col] = None
                elif col == "update_flag":
                    row_data[col] = str(val) if pd.notna(val) else None
                elif hasattr(val, 'item'):
                    # numpy类型转换为Python原生类型
                    try:
                        row_data[col] = val.item()
                    except (ValueError, TypeError):
                        row_data[col] = None
                else:
                    row_data[col] = val

            rows_to_insert.append(row_data)

        if not rows_to_insert:
            return 0

        # 构建插入语句
        columns = list(rows_to_insert[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_sql = f"""
            INSERT INTO income ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET updated_at = NOW()
        """

        async with self.db_manager._engine.begin() as conn:
            for row in rows_to_insert:
                try:
                    await conn.execute(text(insert_sql), row)
                except Exception as e:
                    logger.debug(f"Skipping duplicate/invalid income record: {e}")
                    continue

        return len(rows_to_insert)

    async def get_income(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取利润表数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）

        Returns:
            pd.DataFrame: 利润表数据
        """
        query = """
            SELECT * FROM income WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_date:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_date

        query += " ORDER BY ts_code, end_date_time DESC"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                return data

        return None

    async def get_latest_income_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的利润表数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM income
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    async def get_index_dailybasic_ts_codes(self) -> list:
        """
        获取所有已存储的指数代码列表

        Returns:
            list: 指数代码列表
        """
        query = "SELECT DISTINCT ts_code FROM index_dailybasic ORDER BY ts_code"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query))
            rows = result.fetchall()

        return [row.ts_code for row in rows]

    # ============================================================================
    # 财务指标数据操作
    # ============================================================================

    async def insert_fina_indicator_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入财务指标数据

        Args:
            data: 包含财务指标数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        # 财务指标字段列表（除了ts_code, ann_date, end_date, end_date_time）
        indicator_columns = [
            "eps", "dt_eps", "total_revenue_ps", "revenue_ps", "capital_rese_ps",
            "surplus_rese_ps", "undist_profit_ps", "extra_item", "profit_dedt", "gross_margin",
            "current_ratio", "quick_ratio", "cash_ratio", "ar_turn", "ca_turn", "fa_turn",
            "assets_turn", "op_income", "ebit", "ebitda", "fcff", "fcfe", "current_exint",
            "noncurrent_exint", "interestdebt", "netdebt", "tangible_asset", "working_capital",
            "networking_capital", "invest_capital", "retained_earnings", "diluted2_eps",
            "bps", "ocfps", "cfps", "ebit_ps", "netprofit_margin", "grossprofit_margin",
            "profit_to_gr", "roe", "roe_waa", "roe_dt", "roa", "roic", "debt_to_assets",
            "assets_to_eqt", "ca_to_assets", "nca_to_assets", "tbassets_to_totalassets",
            "int_to_talcap", "eqt_to_talcapital", "currentdebt_to_debt", "longdeb_to_debt",
            "debt_to_eqt", "eqt_to_debt", "eqt_to_interestdebt", "tangibleasset_to_debt",
            "ocf_to_debt", "turn_days", "fixed_assets", "profit_prefin_exp", "non_op_profit",
            "op_to_ebt", "q_opincome", "q_dtprofit", "q_eps", "q_netprofit_margin",
            "q_gsprofit_margin", "q_profit_to_gr", "q_salescash_to_or", "q_ocf_to_sales",
            "basic_eps_yoy", "dt_eps_yoy", "cfps_yoy", "op_yoy", "ebt_yoy", "netprofit_yoy",
            "dt_netprofit_yoy", "ocf_yoy", "roe_yoy", "bps_yoy", "assets_yoy", "eqt_yoy",
            "tr_yoy", "or_yoy", "q_gr_yoy", "q_sales_yoy", "q_op_yoy", "q_op_qoq",
            "q_profit_yoy", "q_profit_qoq", "q_netprofit_yoy", "q_netprofit_qoq", "equity_yoy"
        ]

        # 构建INSERT语句
        columns_str = ", ".join([
            "ts_code", "ann_date", "end_date", "end_date_time"
        ] + indicator_columns)
        values_str = ", ".join([":" + col for col in ["ts_code", "ann_date", "end_date", "end_date_time"] + indicator_columns])

        # ON CONFLICT部分
        update_parts = []
        for col in ["ann_date", "end_date"] + indicator_columns:
            update_parts.append(f"{col} = EXCLUDED.{col}")
        update_str = ", ".join(update_parts)

        insert_sql = f"""
            INSERT INTO fina_indicator ({columns_str})
            VALUES ({values_str})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET
                {update_str},
                updated_at = NOW()
        """

        total_inserted = 0

        # 所有需要插入的列名
        all_columns = ["ts_code", "ann_date", "end_date", "end_date_time"] + indicator_columns

        # 可能包含大数值的字段（需要clip到安全范围）
        large_value_columns = {
            "gross_margin", "extra_item", "profit_dedt", "op_income", "ebit", "ebitda",
            "fcff", "fcfe", "current_exint", "noncurrent_exint", "interestdebt", "netdebt",
            "tangible_asset", "working_capital", "networking_capital", "invest_capital",
            "retained_earnings", "fixed_assets", "profit_prefin_exp", "non_op_profit",
            "q_opincome", "q_dtprofit", "total_revenue_ps", "revenue_ps"
        }

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size].copy()
            records = batch.to_dict("records")

            # 处理NaN值和datetime，并确保所有列都存在
            for record in records:
                # 确保所有必需列都存在
                for col in all_columns:
                    if col not in record:
                        record[col] = None

                # 处理值，clip大数值字段到安全范围
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "end_date_time" or isinstance(value, pd.Timestamp):
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")
                    elif key in large_value_columns and isinstance(value, (int, float)):
                        # Clip大数值到DECIMAL(20,4)的安全范围
                        # max = 10^16 - 1 = 9999999999999999
                        max_val = 9_999_999_999_999_999
                        min_val = -9_999_999_999_999_999
                        if value > max_val:
                            logger.warning(f"Clipping {key} value {value} to max {max_val}")
                            record[key] = max_val
                        elif value < min_val:
                            logger.warning(f"Clipping {key} value {value} to min {min_val}")
                            record[key] = min_val

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} fina_indicator records")
        return total_inserted

    async def get_fina_indicator(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取财务指标数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期）
            end_date: 结束日期（YYYY-MM-DD格式，报告期）

        Returns:
            Optional[pd.DataFrame]: 财务指标数据
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = "SELECT * FROM fina_indicator WHERE 1=1"
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_dt:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY ts_code, end_date_time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_latest_fina_indicator_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的财务指标数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM fina_indicator
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    async def insert_income_batch(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        批量插入利润表数据

        Args:
            df: 利润表数据DataFrame
            batch_size: 批量插入大小，默认1000

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 确保 end_date_time 列存在且格式正确
        if "end_date_time" not in df.columns:
            df["end_date_time"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

        # 验证时间列有有效值
        if df["end_date_time"].isna().all():
            logger.warning("All end_date_time values are NaT, skipping insert")
            return 0

        total_inserted = 0
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]
            try:
                inserted = await self._insert_income_with_retry(batch_df)
                total_inserted += inserted
            except Exception as e:
                logger.error(f"Failed to insert income batch {i // batch_size}: {e}")
                continue

        return total_inserted

    async def _insert_income_with_retry(self, df: pd.DataFrame, max_retries: int = 3) -> int:
        """
        带重试机制的插入利润表数据

        Args:
            df: 利润表数据DataFrame
            max_retries: 最大重试次数

        Returns:
            int: 插入的记录数
        """
        for attempt in range(max_retries):
            try:
                return await self.__insert_income_batch(df)
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Insert income batch failed (attempt {attempt + 1}/{max_retries}): {e}")
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    logger.error(f"Insert income batch failed after {max_retries} attempts: {e}")
                    raise

        return 0

    async def __insert_income_batch(self, df: pd.DataFrame) -> int:
        """
        内部方法：批量插入利润表数据到数据库

        Args:
            df: 利润表数据DataFrame

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 准备插入数据
        rows_to_insert = []
        for _, row in df.iterrows():
            row_data = {}
            for col in df.columns:
                val = row.get(col)
                if col == "end_date_time":
                    # 时间字段需要时区感知的datetime
                    if pd.notna(val):
                        try:
                            if hasattr(val, 'tzinfo') and val.tzinfo is None:
                                row_data[col] = val.tz_localize('UTC')
                            else:
                                row_data[col] = val
                        except (ValueError, TypeError):
                            row_data[col] = None
                    else:
                        row_data[col] = None
                elif col == "update_flag":
                    row_data[col] = str(val) if pd.notna(val) else None
                elif hasattr(val, 'item'):
                    # numpy类型转换为Python原生类型
                    try:
                        row_data[col] = val.item()
                    except (ValueError, TypeError):
                        row_data[col] = None
                else:
                    row_data[col] = val

            rows_to_insert.append(row_data)

        if not rows_to_insert:
            return 0

        # 构建插入语句
        columns = list(rows_to_insert[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_sql = f"""
            INSERT INTO income ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET updated_at = NOW()
        """

        async with self.db_manager._engine.begin() as conn:
            for row in rows_to_insert:
                try:
                    await conn.execute(text(insert_sql), row)
                except Exception as e:
                    logger.debug(f"Skipping duplicate/invalid income record: {e}")
                    continue

        return len(rows_to_insert)

    async def get_income(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取利润表数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）

        Returns:
            pd.DataFrame: 利润表数据
        """
        query = """
            SELECT * FROM income WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_date:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_date

        query += " ORDER BY ts_code, end_date_time DESC"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                return data

        return None

    async def get_latest_income_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的利润表数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM income
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    # ============================================================================
    # 现金流量表数据操作
    # ============================================================================

    async def insert_cashflow_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入现金流量表数据

        Args:
            data: 包含现金流量表数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        # 现金流量表字段列表（除了ts_code, ann_date, f_ann_date, end_date, end_date_time）
        cashflow_columns = [
            "comp_type", "report_type", "end_type", "net_profit", "finan_exp",
            "c_fr_sale_sg", "recp_tax_rends", "n_depos_incr_fi", "n_incr_loans_cb",
            "n_inc_borr_oth_fi", "prem_fr_orig_contr", "n_incr_insured_dep", "n_reinsur_prem",
            "n_incr_disp_tfa", "ifc_cash_incr", "n_incr_disp_faas", "n_incr_loans_oth_bank",
            "n_cap_incr_repur", "c_fr_oth_operate_a", "c_inf_fr_operate_a", "c_paid_goods_s",
            "c_paid_to_for_empl", "c_paid_for_taxes", "n_incr_clt_loan_adv", "n_incr_dep_cbob",
            "c_pay_claims_orig_inco", "pay_handling_chrg", "pay_comm_insur_plcy",
            "oth_cash_pay_oper_act", "st_cash_out_act", "n_cashflow_act", "oth_recp_ral_inv_act",
            "c_disp_withdrwl_invest", "c_recp_return_invest", "n_recp_disp_fiolta",
            "n_recp_disp_sobu", "stot_inflows_inv_act", "c_pay_acq_const_fiolta",
            "c_paid_invest", "n_disp_subs_oth_biz", "oth_pay_ral_inv_act", "n_incr_pledge_loan",
            "stot_out_inv_act", "n_cashflow_inv_act", "c_recp_borrow", "proc_issue_bonds",
            "oth_cash_recp_ral_fnc_act", "stot_cash_in_fnc_act", "free_cashflow",
            "c_prepay_amt_borr", "c_pay_dist_dpcp_int_exp", "incl_dvd_profit_paid_sc_ms",
            "oth_cashpay_ral_fnc_act", "stot_cashout_fnc_act", "n_cash_flows_fnc_act",
            "eff_fx_flu_cash", "n_incr_cash_cash_equ", "c_cash_equ_beg_period",
            "c_cash_equ_end_period", "c_recp_cap_contrib", "incl_cash_rec_saims",
            "uncon_invest_loss", "prov_depr_assets", "depr_fa_coga_dpba", "amort_intang_assets",
            "lt_amort_deferred_exp", "decr_deferred_exp", "incr_acc_exp", "loss_disp_fiolta",
            "loss_scr_fa", "loss_fv_chg", "invest_loss", "decr_def_inc_tax_assets",
            "incr_def_inc_tax_liab", "decr_inventories", "decr_oper_payable", "incr_oper_payable",
            "others", "im_net_cashflow_oper_act", "conv_debt_into_cap", "conv_copbonds_due_within_1y",
            "fa_fnc_leases", "im_n_incr_cash_equ", "net_dism_capital_add", "net_cash_rece_sec",
            "credit_impa_loss", "use_right_asset_dep", "oth_loss_asset", "end_bal_cash",
            "beg_bal_cash", "end_bal_cash_equ", "beg_bal_cash_equ", "update_flag"
        ]

        # 构建INSERT语句
        columns_str = ", ".join([
            "ts_code", "ann_date", "f_ann_date", "end_date", "end_date_time"
        ] + cashflow_columns)
        values_str = ", ".join([":" + col for col in ["ts_code", "ann_date", "f_ann_date", "end_date", "end_date_time"] + cashflow_columns])

        # ON CONFLICT部分
        update_parts = []
        for col in ["ann_date", "f_ann_date", "end_date"] + cashflow_columns:
            update_parts.append(f"{col} = EXCLUDED.{col}")
        update_str = ", ".join(update_parts)

        insert_sql = f"""
            INSERT INTO cashflow ({columns_str})
            VALUES ({values_str})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET
                {update_str},
                updated_at = NOW()
        """

        total_inserted = 0

        # 所有需要插入的列名
        all_columns = ["ts_code", "ann_date", "f_ann_date", "end_date", "end_date_time"] + cashflow_columns

        # 可能包含大数值的字段（需要clip到安全范围）
        large_value_columns = {
            "net_profit", "finan_exp", "c_fr_sale_sg", "recp_tax_rends", "c_inf_fr_operate_a",
            "c_paid_goods_s", "c_paid_to_for_empl", "c_paid_for_taxes", "st_cash_out_act",
            "n_cashflow_act", "c_disp_withdrwl_invest", "c_recp_return_invest",
            "stot_inflows_inv_act", "c_pay_acq_const_fiolta", "c_paid_invest",
            "stot_out_inv_act", "n_cashflow_inv_act", "c_recp_borrow", "proc_issue_bonds",
            "stot_cash_in_fnc_act", "free_cashflow", "c_prepay_amt_borr",
            "c_pay_dist_dpcp_int_exp", "stot_cashout_fnc_act", "n_cash_flows_fnc_act",
            "n_incr_cash_cash_equ", "c_cash_equ_beg_period", "c_cash_equ_end_period",
            "c_recp_cap_contrib", "prov_depr_assets", "depr_fa_coga_dpba", "amort_intang_assets",
            "end_bal_cash", "beg_bal_cash", "end_bal_cash_equ", "beg_bal_cash_equ"
        }

        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size].copy()
            records = batch.to_dict("records")

            # 处理NaN值和datetime，并确保所有列都存在
            for record in records:
                # 确保所有必需列都存在
                for col in all_columns:
                    if col not in record:
                        record[col] = None

                # 处理值，clip大数值字段到安全范围
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key == "end_date_time" or isinstance(value, pd.Timestamp):
                        record[key] = _normalize_datetime_for_db(value, data_type="daily")
                    elif key in large_value_columns and isinstance(value, (int, float)):
                        # Clip大数值到DECIMAL(20,4)的安全范围
                        max_val = 9_999_999_999_999_999
                        min_val = -9_999_999_999_999_999
                        if value > max_val:
                            logger.warning(f"Clipping {key} value {value} to max {max_val}")
                            record[key] = max_val
                        elif value < min_val:
                            logger.warning(f"Clipping {key} value {value} to min {min_val}")
                            record[key] = min_val

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} cashflow records")
        return total_inserted

    async def get_cashflow(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取现金流量表数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期）
            end_date: 结束日期（YYYY-MM-DD格式，报告期）

        Returns:
            Optional[pd.DataFrame]: 现金流量表数据
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = "SELECT * FROM cashflow WHERE 1=1"
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_dt:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY ts_code, end_date_time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_latest_cashflow_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的现金流量表数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM cashflow
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    async def insert_income_batch(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        批量插入利润表数据

        Args:
            df: 利润表数据DataFrame
            batch_size: 批量插入大小，默认1000

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 确保 end_date_time 列存在且格式正确
        if "end_date_time" not in df.columns:
            df["end_date_time"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

        # 验证时间列有有效值
        if df["end_date_time"].isna().all():
            logger.warning("All end_date_time values are NaT, skipping insert")
            return 0

        total_inserted = 0
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]
            try:
                inserted = await self._insert_income_with_retry(batch_df)
                total_inserted += inserted
            except Exception as e:
                logger.error(f"Failed to insert income batch {i // batch_size}: {e}")
                continue

        return total_inserted

    async def _insert_income_with_retry(self, df: pd.DataFrame, max_retries: int = 3) -> int:
        """
        带重试机制的插入利润表数据

        Args:
            df: 利润表数据DataFrame
            max_retries: 最大重试次数

        Returns:
            int: 插入的记录数
        """
        for attempt in range(max_retries):
            try:
                return await self.__insert_income_batch(df)
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Insert income batch failed (attempt {attempt + 1}/{max_retries}): {e}")
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    logger.error(f"Insert income batch failed after {max_retries} attempts: {e}")
                    raise

        return 0

    async def __insert_income_batch(self, df: pd.DataFrame) -> int:
        """
        内部方法：批量插入利润表数据到数据库

        Args:
            df: 利润表数据DataFrame

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 准备插入数据
        rows_to_insert = []
        for _, row in df.iterrows():
            row_data = {}
            for col in df.columns:
                val = row.get(col)
                if col == "end_date_time":
                    # 时间字段需要时区感知的datetime
                    if pd.notna(val):
                        try:
                            if hasattr(val, 'tzinfo') and val.tzinfo is None:
                                row_data[col] = val.tz_localize('UTC')
                            else:
                                row_data[col] = val
                        except (ValueError, TypeError):
                            row_data[col] = None
                    else:
                        row_data[col] = None
                elif col == "update_flag":
                    row_data[col] = str(val) if pd.notna(val) else None
                elif hasattr(val, 'item'):
                    # numpy类型转换为Python原生类型
                    try:
                        row_data[col] = val.item()
                    except (ValueError, TypeError):
                        row_data[col] = None
                else:
                    row_data[col] = val

            rows_to_insert.append(row_data)

        if not rows_to_insert:
            return 0

        # 构建插入语句
        columns = list(rows_to_insert[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_sql = f"""
            INSERT INTO income ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET updated_at = NOW()
        """

        async with self.db_manager._engine.begin() as conn:
            for row in rows_to_insert:
                try:
                    await conn.execute(text(insert_sql), row)
                except Exception as e:
                    logger.debug(f"Skipping duplicate/invalid income record: {e}")
                    continue

        return len(rows_to_insert)

    async def get_income(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取利润表数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）

        Returns:
            pd.DataFrame: 利润表数据
        """
        query = """
            SELECT * FROM income WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_date:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_date

        query += " ORDER BY ts_code, end_date_time DESC"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                return data

        return None

    async def get_latest_income_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的利润表数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM income
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    # ============================================================================
    # 资产负债表操作
    # ============================================================================

    async def insert_balancesheet_batch(
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入资产负债表数据

        Args:
            data: 包含资产负债表数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        # 资产负债表字段列表（除了ts_code, ann_date, f_ann_date, end_date, end_date_time）
        balancesheet_columns = [
            "comp_type", "report_type", "end_type", "total_share", "cap_rese",
            "undistr_porfit", "surplus_rese", "special_rese", "money_cap", "trad_asset",
            "notes_receiv", "accounts_receiv", "oth_receiv", "prepayment", "div_receiv",
            "int_receiv", "inventories", "amor_exp", "nca_within_1y", "sett_rsrv",
            "loanto_oth_bank_fi", "premium_receiv", "reinsur_receiv", "reinsur_res_receiv",
            "pur_resale_fa", "oth_cur_assets", "total_cur_assets", "fa_avail_for_sale",
            "htm_invest", "lt_eqt_invest", "invest_real_estate", "time_deposits", "oth_assets",
            "lt_rec", "fix_assets", "cip", "const_materials", "fixed_assets_disp",
            "produc_bio_assets", "oil_and_gas_assets", "intan_assets", "r_and_d", "goodwill",
            "lt_amor_exp", "defer_tax_assets", "decr_in_disbur", "oth_nca", "total_nca",
            "cash_reser_cb", "depos_in_oth_bfi", "prec_metals", "deriv_assets",
            "rr_reins_une_prem", "rr_reins_outstd_cla", "rr_reins_lins_liab", "rr_reins_lthins_liab",
            "refund_depos", "ph_pledge_loans", "refund_cap_depos", "indept_acct_assets",
            "client_depos", "client_prov", "transac_seat_fee", "invest_as_receiv", "total_assets",
            "lt_borr", "st_borr", "cb_borr", "depos_ib_deposits", "loan_oth_bank", "trading_fl",
            "notes_payable", "acct_payable", "adv_receipts", "sold_for_repur_fa", "comm_payable",
            "payroll_payable", "taxes_payable", "int_payable", "div_payable", "oth_payable",
            "acc_exp", "deferred_inc", "st_bonds_payable", "payable_to_reinsurer", "rsrv_insur_cont",
            "acting_trading_sec", "acting_uw_sec", "non_cur_liab_due_1y", "oth_cur_liab",
            "total_cur_liab", "bond_payable", "lt_payable", "specific_payables", "estimated_liab",
            "defer_tax_liab", "defer_inc_non_cur_liab", "oth_ncl", "total_ncl", "depos_oth_bfi",
            "deriv_liab", "depos", "agency_bus_liab", "oth_liab", "prem_receiv_adva",
            "depos_received", "ph_invest", "reser_une_prem", "reser_outstd_claims", "reser_lins_liab",
            "reser_lthins_liab", "indept_acc_liab", "pledge_borr", "indem_payable", "policy_div_payable",
            "total_liab", "treasury_share", "ordin_risk_reser", "forex_differ", "invest_loss_unconf",
            "minority_int", "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int",
            "total_liab_hldr_eqy", "lt_payroll_payable", "oth_comp_income", "oth_eqt_tools",
            "oth_eqt_tools_p_shr", "lending_funds", "acc_receivable", "st_fin_payable", "payables",
            "hfs_assets", "hfs_sales", "cost_fin_assets", "fair_value_fin_assets", "cip_total",
            "oth_pay_total", "long_pay_total", "debt_invest", "oth_debt_invest", "oth_eq_invest",
            "oth_illiq_fin_assets", "oth_eq_ppbond", "receiv_financing", "use_right_assets",
            "lease_liab", "contract_assets", "contract_liab", "accounts_receiv_bill", "accounts_pay",
            "oth_rcv_total", "fix_assets_total", "update_flag"
        ]

        # 构建INSERT语句
        columns_str = ", ".join([
            "ts_code", "ann_date", "f_ann_date", "end_date", "end_date_time"
        ] + balancesheet_columns)
        values_str = ", ".join([":" + col for col in ["ts_code", "ann_date", "f_ann_date", "end_date", "end_date_time"] + balancesheet_columns])

        # ON CONFLICT部分
        update_parts = []
        for col in ["ann_date", "f_ann_date", "end_date"] + balancesheet_columns:
            update_parts.append(f"{col} = EXCLUDED.{col}")
        update_str = ", ".join(update_parts)

        insert_sql = f"""
            INSERT INTO balancesheet ({columns_str})
            VALUES ({values_str})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET
                {update_str},
                updated_at = NOW()
        """

        total_inserted = 0

        # 所有需要插入的列名
        all_columns = ["ts_code", "ann_date", "f_ann_date", "end_date", "end_date_time"] + balancesheet_columns

        # 可能包含大数值的字段（需要clip到安全范围）
        large_value_columns = {
            "total_share", "cap_rese", "undistr_porfit", "surplus_rese", "money_cap",
            "total_cur_assets", "total_nca", "total_assets", "total_cur_liab", "total_ncl",
            "total_liab", "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int",
            "total_liab_hldr_eqy", "fix_assets", "fix_assets_total", "lending_funds",
            "acc_receivable", "payables", "hfs_assets", "cost_fin_assets", "fair_value_fin_assets",
            "cip", "cip_total", "oth_cur_assets", "oth_nca", "oth_assets", "oth_liab",
            "oth_payable", "oth_pay_total", "lt_payable", "long_pay_total", "debt_invest",
            "oth_debt_invest", "oth_eq_invest", "oth_illiq_fin_assets", "receiv_financing",
            "use_right_assets", "lease_liab", "contract_assets", "contract_liab",
            "accounts_receiv", "accounts_receiv_bill", "oth_rcv_total"
        }

        try:
            # 分批处理
            for i in range(0, len(data), batch_size):
                batch = data.iloc[i : i + batch_size].copy()

                # 准备批量数据
                batch_data = []
                for _, row in batch.iterrows():
                    row_data = {}
                    for col in all_columns:
                        if col in large_value_columns:
                            # 大数值字段，clip到安全范围
                            val = row.get(col)
                            if pd.notna(val):
                                try:
                                    row_data[col] = float(val)
                                    if abs(row_data[col]) > 1e15:
                                        row_data[col] = None
                                except (ValueError, TypeError):
                                    row_data[col] = None
                            else:
                                row_data[col] = None
                        elif col == "end_date_time":
                            # 时间字段需要转换为带时区的datetime
                            val = row.get(col)
                            if pd.notna(val):
                                try:
                                    # 如果是tz-naive的Timestamp，添加UTC时区
                                    if hasattr(val, 'tzinfo') and val.tzinfo is None:
                                        row_data[col] = val.tz_localize('UTC')
                                    else:
                                        row_data[col] = val
                                except (ValueError, TypeError):
                                    row_data[col] = None
                            else:
                                row_data[col] = None
                        else:
                            # 普通字段
                            val = row.get(col)
                            if pd.isna(val):
                                row_data[col] = None
                            else:
                                row_data[col] = val
                    batch_data.append(row_data)

                # 执行批量插入
                async with self.db_manager._engine.begin() as conn:
                    for row_data in batch_data:
                        await conn.execute(text(insert_sql), row_data)
                    await conn.commit()

                total_inserted += len(batch_data)

            logger.info(f"Inserted {total_inserted} balancesheet records")
            return total_inserted

        except Exception as e:
            logger.exception(f"Failed to insert balancesheet batch: {str(e)}")
            raise

    async def get_balancesheet(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取资产负债表数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期）
            end_date: 结束日期（YYYY-MM-DD格式，报告期）

        Returns:
            Optional[pd.DataFrame]: 资产负债表数据
        """
        # 将字符串日期转换为带时区的datetime对象
        start_dt = _normalize_datetime_for_db(start_date, "daily") if start_date else None
        end_dt = _normalize_datetime_for_db(end_date + " 23:59:59", "daily") if end_date else None

        # 构建查询条件
        query = "SELECT * FROM balancesheet WHERE 1=1"
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_dt:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_dt

        if end_dt:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_dt

        query += " ORDER BY ts_code, end_date_time"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        if not rows:
            return None

        data = pd.DataFrame([row._asdict() for row in rows])
        return data

    async def get_latest_balancesheet_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的资产负债表数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM balancesheet
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    async def insert_income_batch(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        批量插入利润表数据

        Args:
            df: 利润表数据DataFrame
            batch_size: 批量插入大小，默认1000

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 确保 end_date_time 列存在且格式正确
        if "end_date_time" not in df.columns:
            df["end_date_time"] = pd.to_datetime(df["end_date"], format="%Y%m%d", errors="coerce")

        # 验证时间列有有效值
        if df["end_date_time"].isna().all():
            logger.warning("All end_date_time values are NaT, skipping insert")
            return 0

        total_inserted = 0
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i : i + batch_size]
            try:
                inserted = await self._insert_income_with_retry(batch_df)
                total_inserted += inserted
            except Exception as e:
                logger.error(f"Failed to insert income batch {i // batch_size}: {e}")
                continue

        return total_inserted

    async def _insert_income_with_retry(self, df: pd.DataFrame, max_retries: int = 3) -> int:
        """
        带重试机制的插入利润表数据

        Args:
            df: 利润表数据DataFrame
            max_retries: 最大重试次数

        Returns:
            int: 插入的记录数
        """
        for attempt in range(max_retries):
            try:
                return await self.__insert_income_batch(df)
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Insert income batch failed (attempt {attempt + 1}/{max_retries}): {e}")
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    logger.error(f"Insert income batch failed after {max_retries} attempts: {e}")
                    raise

        return 0

    async def __insert_income_batch(self, df: pd.DataFrame) -> int:
        """
        内部方法：批量插入利润表数据到数据库

        Args:
            df: 利润表数据DataFrame

        Returns:
            int: 插入的记录数
        """
        if df.empty:
            return 0

        # 准备插入数据
        rows_to_insert = []
        for _, row in df.iterrows():
            row_data = {}
            for col in df.columns:
                val = row.get(col)
                if col == "end_date_time":
                    # 时间字段需要时区感知的datetime
                    if pd.notna(val):
                        try:
                            if hasattr(val, 'tzinfo') and val.tzinfo is None:
                                row_data[col] = val.tz_localize('UTC')
                            else:
                                row_data[col] = val
                        except (ValueError, TypeError):
                            row_data[col] = None
                    else:
                        row_data[col] = None
                elif col == "update_flag":
                    row_data[col] = str(val) if pd.notna(val) else None
                elif hasattr(val, 'item'):
                    # numpy类型转换为Python原生类型
                    try:
                        row_data[col] = val.item()
                    except (ValueError, TypeError):
                        row_data[col] = None
                else:
                    row_data[col] = val

            rows_to_insert.append(row_data)

        if not rows_to_insert:
            return 0

        # 构建插入语句
        columns = list(rows_to_insert[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_sql = f"""
            INSERT INTO income ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (ts_code, end_date_time) DO UPDATE SET updated_at = NOW()
        """

        async with self.db_manager._engine.begin() as conn:
            for row in rows_to_insert:
                try:
                    await conn.execute(text(insert_sql), row)
                except Exception as e:
                    logger.debug(f"Skipping duplicate/invalid income record: {e}")
                    continue

        return len(rows_to_insert)

    async def get_income(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取利润表数据

        Args:
            ts_code: 股票代码（如600519.SH），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）

        Returns:
            pd.DataFrame: 利润表数据
        """
        query = """
            SELECT * FROM income WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        if start_date:
            query += " AND end_date_time >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND end_date_time <= :end_date"
            params["end_date"] = end_date

        query += " ORDER BY ts_code, end_date_time DESC"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                return data

        return None

    async def get_latest_income_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的利润表数据日期

        Args:
            ts_code: 股票代码（如600519.SH），None表示任意股票

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(end_date_time) as latest_date
            FROM income
            WHERE 1=1
        """
        params = {}

        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            row = result.fetchone()

        if row and row.latest_date:
            return row.latest_date.strftime("%Y-%m-%d")

        return None

    # ===========================
    # 申万行业分类数据操作
    # ===========================

    async def insert_sw_industry_classify_batch(self, data: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        批量插入申万行业分类数据

        Args:
            data: 包含申万行业分类数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        # 准备插入语句
        insert_sql = """
            INSERT INTO sw_industry_classify (
                index_code, industry_name, parent_code, level,
                industry_code, is_pub, src
            )
            VALUES (
                :index_code, :industry_name, :parent_code, :level,
                :industry_code, :is_pub, :src
            )
            ON CONFLICT (index_code, industry_code) DO UPDATE SET
                industry_name = EXCLUDED.industry_name,
                parent_code = EXCLUDED.parent_code,
                level = EXCLUDED.level,
                is_pub = EXCLUDED.is_pub,
                src = EXCLUDED.src,
                updated_at = NOW()
        """

        total_inserted = 0

        # 按批插入
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]

            # 转换为记录列表
            records = batch.to_dict("records")

            # 处理NaT值和NaN值
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} records to sw_industry_classify")
        return total_inserted

    async def get_sw_industry_classify(self, level: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        查询申万行业分类

        Args:
            level: 行业层级 (L1/L2/L3)，None表示所有层级

        Returns:
            Optional[pd.DataFrame]: 申万行业分类数据
        """
        query = """
            SELECT index_code, industry_name, parent_code, level,
                   industry_code, is_pub, src, update_time
            FROM sw_industry_classify
            WHERE 1=1
        """
        params = {}

        if level:
            query += " AND level = :level"
            params["level"] = level

        query += " ORDER BY industry_code"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                return data

        return None

    async def get_sw_l1_industry_codes(self) -> List[str]:
        """
        获取所有一级行业代码列表

        Returns:
            List[str]: 一级行业代码列表
        """
        query = """
            SELECT DISTINCT industry_code
            FROM sw_industry_classify
            WHERE level = 'L1'
            ORDER BY industry_code
        """

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query))
            rows = result.fetchall()

        return [row.industry_code for row in rows if row.industry_code]

    async def get_sw_industry_list(self) -> List[str]:
        """
        获取 sw_daily 表中所有不重复的行业代码列表

        Returns:
            List[str]: 行业代码列表（如 ['801010.SI', '801020.SI', ...]）
        """
        query = """
            SELECT DISTINCT ts_code
            FROM sw_daily
            ORDER BY ts_code
        """

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query))
            rows = result.fetchall()

        return [row.ts_code for row in rows if row.ts_code]

    # ===========================
    # 申万行业成分股数据操作
    # ===========================

    async def insert_sw_industry_member_batch(self, data: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        批量插入申万行业成分股数据

        Args:
            data: 包含申万行业成分股数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        # 准备插入语句
        insert_sql = """
            INSERT INTO sw_industry_member (
                l1_code, l1_name, l2_code, l2_name, l3_code, l3_name,
                ts_code, name, in_date, out_date, is_new
            )
            VALUES (
                :l1_code, :l1_name, :l2_code, :l2_name, :l3_code, :l3_name,
                :ts_code, :name, :in_date, :out_date, :is_new
            )
            ON CONFLICT (l3_code, ts_code) DO UPDATE SET
                l1_code = EXCLUDED.l1_code,
                l1_name = EXCLUDED.l1_name,
                l2_code = EXCLUDED.l2_code,
                l2_name = EXCLUDED.l2_name,
                l3_name = EXCLUDED.l3_name,
                name = EXCLUDED.name,
                in_date = EXCLUDED.in_date,
                out_date = EXCLUDED.out_date,
                is_new = EXCLUDED.is_new,
                updated_at = NOW()
        """

        total_inserted = 0

        # 按批插入
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i : i + batch_size]

            # 转换为记录列表
            records = batch.to_dict("records")

            # 处理NaT值和NaN值
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif key in ("in_date", "out_date") and isinstance(value, pd.Timestamp):
                        # 转换时间戳为Python datetime
                        record[key] = value.to_pydatetime()

            async with self.db_manager._engine.begin() as conn:
                result = await conn.execute(text(insert_sql), records)
                total_inserted += result.rowcount

            logger.info(
                f"Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} records (total: {total_inserted})"
            )

        logger.info(f"Total inserted {total_inserted} records to sw_industry_member")
        return total_inserted

    async def get_sw_industry_members(
        self,
        l1_code: Optional[str] = None,
        l2_code: Optional[str] = None,
        l3_code: Optional[str] = None,
        ts_code: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        查询申万行业成分股

        Args:
            l1_code: 一级行业代码
            l2_code: 二级行业代码
            l3_code: 三级行业代码
            ts_code: 股票代码，如 '600519.SH'，用于查询股票所属的行业

        Returns:
            Optional[pd.DataFrame]: 申万行业成分股数据
        """
        query = """
            SELECT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name,
                   ts_code, name, in_date, out_date, is_new, update_time
            FROM sw_industry_member
            WHERE 1=1
        """
        params = {}

        if l1_code:
            query += " AND l1_code = :l1_code"
            params["l1_code"] = l1_code
        if l2_code:
            query += " AND l2_code = :l2_code"
            params["l2_code"] = l2_code
        if l3_code:
            query += " AND l3_code = :l3_code"
            params["l3_code"] = l3_code
        if ts_code:
            query += " AND ts_code = :ts_code"
            params["ts_code"] = ts_code

        query += " ORDER BY l1_code, l2_code, l3_code, ts_code"

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                return data

        return None

    async def get_sw_industry_member_count(self, l3_code: str) -> int:
        """
        获取指定三级行业的成分股数量

        Args:
            l3_code: 三级行业代码

        Returns:
            int: 成分股数量
        """
        query = """
            SELECT COUNT(*) as cnt
            FROM sw_industry_member
            WHERE l3_code = :l3_code
        """

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(text(query), {"l3_code": l3_code})
            row = result.fetchone()

        return row.cnt if row else 0
