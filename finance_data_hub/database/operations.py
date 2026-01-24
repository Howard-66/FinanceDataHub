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
            for record in records:
                if isinstance(record["trade_date"], pd.Timestamp):
                    record["trade_date"] = record["trade_date"].to_pydatetime()
                elif isinstance(record["trade_date"], str):
                    record["trade_date"] = pd.to_datetime(record["trade_date"]).to_pydatetime()

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
        return data

    async def get_latest_index_dailybasic_date(self, ts_code: Optional[str] = None) -> Optional[str]:
        """
        获取最新的大盘指数每日指标数据日期

        Args:
            ts_code: 指数代码（如000001.SH），None表示任意指数

        Returns:
            Optional[str]: 最新日期（YYYY-MM-DD格式），如果无数据则返回None
        """
        query = """
            SELECT MAX(trade_date) as latest_date
            FROM index_dailybasic
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
