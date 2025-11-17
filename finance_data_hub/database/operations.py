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


def _normalize_datetime_for_db(value):
    """
    将pandas Timestamp转换为带时区的Python datetime

    Args:
        value: pandas Timestamp或datetime对象

    Returns:
        datetime: 带UTC时区的datetime对象
    """
    if isinstance(value, pd.Timestamp):
        if value.tz is None:
            # 如果没有时区信息，假设是UTC
            return value.tz_localize('UTC').to_pydatetime()
        else:
            return value.tz_convert('UTC').to_pydatetime()
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
                change_amount = EXCLUDED.change_amount
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
                        record[key] = _normalize_datetime_for_db(value)

            # 确保所有必需字段都存在，缺失的字段设置为None
            required_fields = {
                "adj_factor": None,
                "open_interest": None,
                "settle": None,
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
        self, data: pd.DataFrame, batch_size: int = 1000
    ) -> int:
        """
        批量插入分钟数据

        Args:
            data: 包含分钟数据的DataFrame
            batch_size: 批处理大小

        Returns:
            int: 插入的记录数
        """
        if data.empty:
            return 0

        insert_sql = """
            INSERT INTO symbol_minute (
                time, symbol, open, high, low, close,
                volume, amount, open_interest, settle,
                change_pct, change_amount
            )
            VALUES (
                :time, :symbol, :open, :high, :low, :close,
                :volume, :amount, :open_interest, :settle,
                :change_pct, :change_amount
            )
            ON CONFLICT (symbol, time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                open_interest = EXCLUDED.open_interest,
                settle = EXCLUDED.settle,
                change_pct = EXCLUDED.change_pct,
                change_amount = EXCLUDED.change_amount
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
                        record[key] = _normalize_datetime_for_db(value)

            # 确保所有必需字段都存在，缺失的字段设置为None
            required_fields = {
                "open_interest": None,
                "settle": None,
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

        logger.info(f"Total inserted {total_inserted} records to symbol_minute")
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
            result = await conn.execute(text(insert_sql), records)

        logger.info(f"Inserted/updated {result.rowcount} asset_basic records")
        return result.rowcount

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
                circ_mv = EXCLUDED.circ_mv
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
                        record[key] = _normalize_datetime_for_db(value)

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
                        record[key] = _normalize_datetime_for_db(value)

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
        self, symbol: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取指定时间范围内的复权因子

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Optional[pd.DataFrame]: 复权因子数据
        """
        query = text(
            """
            SELECT symbol, trade_date, adj_factor
            FROM adj_factor
            WHERE symbol = :symbol
            AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
        """
        )

        async with self.db_manager._engine.begin() as conn:
            result = await conn.execute(
                query,
                {
                    "symbol": symbol,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            rows = result.fetchall()

        if not rows:
            return None

        # 转换为DataFrame
        data = pd.DataFrame([row._asdict() for row in rows])
        return data
