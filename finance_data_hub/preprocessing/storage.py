"""
预处理数据存储管理器

负责：
1. 预处理数据的批量写入
2. 增量更新策略
3. 数据版本管理
"""

from typing import Optional, List, Dict, Any, TYPE_CHECKING
from datetime import datetime
import pandas as pd
from loguru import logger

if TYPE_CHECKING:
    from finance_data_hub.database.manager import DatabaseManager


class ProcessedDataStorage:
    """
    预处理数据存储管理器
    
    管理预处理后数据的持久化存储。
    
    示例:
        >>> storage = ProcessedDataStorage(db_manager)
        >>> count = await storage.upsert(df, freq="daily", adjust_type="qfq")
    """
    
    # 表名映射
    TABLE_MAP = {
        ("daily", "qfq"): "processed_daily_qfq",
        ("weekly", "qfq"): "processed_weekly_qfq",
        ("monthly", "qfq"): "processed_monthly_qfq",
    }
    
    # 技术指标列
    TECHNICAL_COLUMNS = [
        "ma_20", "ma_50",
        "macd_dif", "macd_dea", "macd_hist",
        "rsi_14",
        "atr_14"
    ]
    
    # 基础 OHLCV 列
    BASE_COLUMNS = ["time", "symbol", "open", "high", "low", "close", "volume", "amount"]
    
    def __init__(self, db_manager: Optional["DatabaseManager"] = None):
        """
        初始化存储管理器
        
        Args:
            db_manager: 数据库管理器
        """
        self.db_manager = db_manager
        
    async def upsert(
        self,
        df: pd.DataFrame,
        freq: str,
        adjust_type: str,
        batch_size: int = 5000
    ) -> int:
        """
        批量插入/更新预处理数据
        
        使用 UPSERT 语义，存在则更新，不存在则插入。
        
        Args:
            df: 预处理后的 DataFrame
            freq: 频率 (daily/weekly/monthly)
            adjust_type: 复权类型 (qfq)
            batch_size: 批处理大小
            
        Returns:
            影响的记录数
        """
        table_name = self.TABLE_MAP.get((freq.lower(), adjust_type.lower()))
        
        if not table_name:
            raise ValueError(
                f"Unsupported freq/adjust combination: {freq}/{adjust_type}. "
                f"Supported: {list(self.TABLE_MAP.keys())}"
            )
        
        # 准备数据
        df = self._prepare_data(df, freq)
        
        if df.empty:
            logger.warning("No data to upsert")
            return 0
        
        # 添加处理时间戳
        df["processed_at"] = datetime.now()
        
        # 批量 upsert
        total = 0
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            count = await self._upsert_batch(batch, table_name)
            total += count
            logger.debug(f"Upserted {count} records to {table_name}")
        
        logger.info(f"Total upserted: {total} records to {table_name}")
        return total
    
    def _prepare_data(self, df: pd.DataFrame, freq: str) -> pd.DataFrame:
        """
        准备要存储的数据
        
        只保留需要存储的列，处理缺失值。
        """
        result = df.copy()
        
        # 确定需要的列
        needed_cols = self.BASE_COLUMNS.copy()
        
        # 添加存在的技术指标列
        for col in self.TECHNICAL_COLUMNS:
            if col in result.columns:
                needed_cols.append(col)
        
        # 只保留需要的列
        available_cols = [c for c in needed_cols if c in result.columns]
        result = result[available_cols]
        
        # 处理缺失值
        # 对于技术指标，NaN 保留（表示数据不足）
        # 对于 OHLCV，NaN 行需要处理
        result = result.dropna(subset=["open", "high", "low", "close"])
        
        # 转换 time 列为 Python datetime（解决 numpy datetime64 兼容性问题）
        if "time" in result.columns:
            # 兼容处理
            if pd.api.types.is_datetime64_any_dtype(result["time"]):
                result["time"] = pd.to_datetime(result["time"]).dt.to_pydatetime()
        
        return result
    
    async def _upsert_batch(self, batch: pd.DataFrame, table_name: str) -> int:
        """
        执行批量 upsert
        
        使用 PostgreSQL 的 ON CONFLICT DO UPDATE 语法。
        """
        if self.db_manager is None:
            logger.warning("No db_manager configured, skipping upsert")
            return 0
        
        # 获取列名
        columns = list(batch.columns)
        
        # 构建列名和占位符
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        
        # 构建更新语句（排除主键）
        update_cols = [c for c in columns if c not in ["symbol", "time"]]
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        
        sql = f"""
            INSERT INTO {table_name} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (symbol, time) 
            DO UPDATE SET {update_str}
        """
        
        # 转换数据为记录列表（确保使用 Python 原生类型）
        values_list = []
        for _, row in batch.iterrows():
            values = []
            for col in columns:
                val = row[col]
                # 转换 numpy/pandas 类型为 Python 原生类型
                if isinstance(val, (pd.Timestamp, datetime)):
                    val = val  # asyncpg accepts datetime
                    if hasattr(val, 'to_pydatetime'):
                         val = val.to_pydatetime()
                elif pd.isna(val):
                    val = None
                elif hasattr(val, 'item'):  # numpy scalar
                    val = val.item()
                values.append(val)
            values_list.append(tuple(values))
        
        try:
            engine = self.db_manager.get_engine()
            async with engine.begin() as conn:
                # 获取底层 asyncpg 连接
                raw_conn = await conn.get_raw_connection()
                asyncpg_conn = raw_conn.driver_connection
                
                # 批量执行
                await asyncpg_conn.executemany(sql, values_list)
                
            return len(batch)
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            raise
    
    async def get_latest_processed_date(
        self,
        symbol: str,
        freq: str,
        adjust_type: str
    ) -> Optional[datetime]:
        """
        获取指定股票的最新预处理数据日期
        
        Args:
            symbol: 股票代码
            freq: 频率
            adjust_type: 复权类型
            
        Returns:
            最新数据日期，不存在返回 None
        """
        table_name = self.TABLE_MAP.get((freq.lower(), adjust_type.lower()))
        
        if not table_name or self.db_manager is None:
            return None
        
        sql = f"""
            SELECT MAX(time) as latest_time
            FROM {table_name}
            WHERE symbol = :symbol
        """
        
        try:
            result = await self.db_manager.execute_raw_sql(sql, {"symbol": symbol})
            row = result.fetchone()
            if row and row[0]:
                return row[0]
        except Exception as e:
            logger.error(f"Query failed: {e}")
        
        return None
    
    async def query(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = "daily",
        adjust_type: str = "qfq",
        indicators: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        查询预处理数据
        
        Args:
            symbols: 股票代码列表，None 表示全部
            start_date: 开始日期
            end_date: 结束日期
            freq: 频率
            adjust_type: 复权类型
            indicators: 需要的指标列表，None 表示全部
            
        Returns:
            预处理数据 DataFrame
        """
        table_name = self.TABLE_MAP.get((freq.lower(), adjust_type.lower()))
        
        if not table_name:
            raise ValueError(f"Unsupported freq/adjust: {freq}/{adjust_type}")
        
        if self.db_manager is None:
            logger.warning("No db_manager configured")
            return pd.DataFrame()
        
        # 构建查询列
        columns = self.BASE_COLUMNS.copy()
        if indicators:
            columns.extend([c for c in indicators if c in self.TECHNICAL_COLUMNS])
        else:
            columns.extend(self.TECHNICAL_COLUMNS)
        
        cols_str = ", ".join(columns)
        
        # 构建 WHERE 子句
        conditions = []
        params = {}
        
        if symbols:
            # 使用 IN 子句替代 ANY
            placeholders = ", ".join([f":sym_{i}" for i in range(len(symbols))])
            conditions.append(f"symbol IN ({placeholders})")
            for i, sym in enumerate(symbols):
                params[f"sym_{i}"] = sym
        
        if start_date:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date).to_pydatetime()
            conditions.append("time >= :start_date")
            params["start_date"] = start_date
        
        if end_date:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date).to_pydatetime()
            conditions.append("time <= :end_date")
            params["end_date"] = end_date
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        sql = f"""
            SELECT {cols_str}
            FROM {table_name}
            {where_clause}
            ORDER BY symbol, time
        """
        
        try:
            result = await self.db_manager.execute_raw_sql(sql, params)
            rows = result.fetchall()
            
            if not rows:
                return pd.DataFrame(columns=columns)
            
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    async def delete_old_data(
        self,
        before_date: str,
        freq: str,
        adjust_type: str
    ) -> int:
        """
        删除指定日期之前的数据
        
        用于数据清理和空间管理。
        
        Args:
            before_date: 日期阈值
            freq: 频率
            adjust_type: 复权类型
            
        Returns:
            删除的记录数
        """
        table_name = self.TABLE_MAP.get((freq.lower(), adjust_type.lower()))
        
        if not table_name or self.db_manager is None:
            return 0
        
        # 转换日期
        if isinstance(before_date, str):
            before_date = pd.to_datetime(before_date).to_pydatetime()
            
        sql = f"""
            DELETE FROM {table_name}
            WHERE time < :before_date
        """
        
        try:
            result = await self.db_manager.execute_raw_sql(
                sql, 
                {"before_date": before_date}
            )
            return result.rowcount
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return 0


class FundamentalDataStorage:
    """
    基本面指标存储管理器
    
    管理估值分位和 F-Score 等基本面指标的存储。
    """
    
    TABLE_NAME = "fundamental_indicators"
    
    # 估值分位列
    VALUATION_COLUMNS = [
        "pe_ttm_pct_250d", "pb_pct_250d", "ps_ttm_pct_250d",
        "pe_ttm_pct_500d", "pb_pct_500d", "ps_ttm_pct_500d",
        "pe_ttm_pct_750d", "pb_pct_750d", "ps_ttm_pct_750d",
        "pe_ttm_pct_1250d", "pb_pct_1250d", "ps_ttm_pct_1250d",
    ]
    
    # F-Score 列
    FSCORE_COLUMNS = [
        "f_score",
        "f_roa", "f_cfo", "f_delta_roa", "f_accrual",
        "f_delta_lever", "f_delta_liquid", "f_eq_offer",
        "f_delta_margin", "f_delta_turn"
    ]
    
    def __init__(self, db_manager: Optional["DatabaseManager"] = None):
        self.db_manager = db_manager
    
    async def upsert(
        self,
        df: pd.DataFrame,
        batch_size: int = 5000
    ) -> int:
        """
        批量插入/更新基本面指标数据
        """
        if self.db_manager is None:
            logger.warning("No db_manager configured")
            return 0
        
        if df.empty:
            return 0
        
        # 准备数据
        result = df.copy()
        result["processed_at"] = datetime.now()
        
        # 转换日期
        if "time" in result.columns and pd.api.types.is_datetime64_any_dtype(result["time"]):
            result["time"] = pd.to_datetime(result["time"]).dt.to_pydatetime()
        
        # 获取列名
        columns = list(result.columns)
        
        # 构建列名和占位符
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        
        # 构建更新语句
        update_cols = [c for c in columns if c not in ["symbol", "time"]]
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        
        sql = f"""
            INSERT INTO {self.TABLE_NAME} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (symbol, time) 
            DO UPDATE SET {update_str}
        """
        
        # 转换数据为记录列表（使用 Python 原生类型）
        values_list = []
        for _, row in result.iterrows():
            values = []
            for col in columns:
                val = row[col]
                # 转换 numpy/pandas 类型为 Python 原生类型
                if isinstance(val, (pd.Timestamp, datetime)):
                    val = val  # asyncpg accepts datetime
                    if hasattr(val, 'to_pydatetime'):
                         val = val.to_pydatetime()
                elif pd.isna(val):
                    val = None
                elif hasattr(val, 'item'):  # numpy scalar
                    val = val.item()
                values.append(val)
            values_list.append(tuple(values))
        
        total = 0
        try:
            engine = self.db_manager.get_engine()
            async with engine.begin() as conn:
                raw_conn = await conn.get_raw_connection()
                asyncpg_conn = raw_conn.driver_connection
                
                # 分批执行
                for i in range(0, len(values_list), batch_size):
                    batch = values_list[i:i+batch_size]
                    await asyncpg_conn.executemany(sql, batch)
                    total += len(batch)
                    
            logger.info(f"Upserted {total} records to {self.TABLE_NAME}")
            return total
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            raise
    
    async def query(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        indicators: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        查询基本面指标
        """
        if self.db_manager is None:
            logger.warning("No db_manager configured")
            return pd.DataFrame()
        
        # 构建查询列
        columns = ["time", "symbol"]
        if indicators:
            for ind in indicators:
                if ind in self.VALUATION_COLUMNS:
                    columns.append(ind)
                elif ind in self.FSCORE_COLUMNS:
                    columns.append(ind)
                elif ind == "valuation":
                    columns.extend(self.VALUATION_COLUMNS)
                elif ind == "fscore":
                    columns.extend(self.FSCORE_COLUMNS)
        else:
            columns.extend(self.VALUATION_COLUMNS)
            columns.extend(self.FSCORE_COLUMNS)
        
        # 去重
        columns = list(dict.fromkeys(columns))
        cols_str = ", ".join(columns)
        
        # 构建 WHERE 子句
        conditions = []
        params = {}
        
        if symbols:
            # 使用 IN 子句替代 ANY
            placeholders = ", ".join([f":sym_{i}" for i in range(len(symbols))])
            conditions.append(f"symbol IN ({placeholders})")
            for i, sym in enumerate(symbols):
                params[f"sym_{i}"] = sym
        
        if start_date:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date).to_pydatetime()
            conditions.append("time >= :start_date")
            params["start_date"] = start_date
        
        if end_date:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date).to_pydatetime()
            conditions.append("time <= :end_date")
            params["end_date"] = end_date
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        sql = f"""
            SELECT {cols_str}
            FROM {self.TABLE_NAME}
            {where_clause}
            ORDER BY symbol, time
        """
        
        try:
            result = await self.db_manager.execute_raw_sql(sql, params)
            rows = result.fetchall()
            
            if not rows:
                return pd.DataFrame(columns=columns)
            
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()
