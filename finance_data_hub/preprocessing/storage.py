"""
预处理数据存储管理器

负责：
1. 预处理数据的批量写入
2. 增量更新策略
3. 数据版本管理
"""

from typing import Optional, List, Dict, Any, TYPE_CHECKING
from datetime import datetime
import numpy as np
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

    # 元数据列
    META_COLUMNS = ["last_adj_factor", "processed_at"]

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

        # 添加元数据列（如果存在）
        for col in self.META_COLUMNS:
            if col in result.columns:
                needed_cols.append(col)

        # 只保留需要的列
        available_cols = [c for c in needed_cols if c in result.columns]
        result = result[available_cols]

        # 处理缺失值
        # 对于技术指标，NaN 保留（表示数据不足）
        # 对于 OHLCV，NaN 行需要处理
        result = result.dropna(subset=["open", "high", "low", "close"])

        # 转换 time 列为带时区的 Python datetime（数据库是 TIMESTAMPTZ）
        if "time" in result.columns:
            if pd.api.types.is_datetime64_any_dtype(result["time"]):
                if pd.api.types.is_datetime64tz_dtype(result["time"]):
                    # 已经是带时区的，转换为 UTC
                    result["time"] = pd.to_datetime(result["time"]).dt.tz_convert('UTC')
                else:
                    # 无时区的，先本地化再转换为 UTC
                    result["time"] = pd.to_datetime(result["time"]).dt.tz_localize('UTC')
                # 转换为 Python datetime（保留时区）
                result["time"] = result["time"].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

        return result

    @staticmethod
    def _prepare_values_list(df: pd.DataFrame, columns: list) -> list:
        """
        向量化转换 DataFrame 为 asyncpg 可接受的参数列表

        相比 iterrows() 逐行处理，性能提升 5-10 倍。

        Args:
            df: 要转换的 DataFrame
            columns: 需要转换的列名列表

        Returns:
            元组列表，可直接用于 asyncpg.executemany()
        """
        if df.empty:
            return []

        # 只选择需要的列
        prepared = df[columns].copy()

        # 向量化类型转换
        for col in columns:
            if col not in prepared.columns:
                continue

            dtype = prepared[col].dtype

            # 日期时间列转换 - 保持时区信息（数据库是 TIMESTAMPTZ）
            if pd.api.types.is_datetime64_any_dtype(dtype):
                # 转换为带时区的 Python datetime（UTC）
                if pd.api.types.is_datetime64tz_dtype(dtype):
                    # 已经是带时区的，转换为 UTC
                    prepared[col] = pd.to_datetime(prepared[col]).dt.tz_convert('UTC')
                else:
                    # 无时区的，先本地化再转换为 UTC
                    prepared[col] = pd.to_datetime(prepared[col]).dt.tz_localize('UTC')
                # 转换为 Python datetime（保留时区）
                prepared[col] = prepared[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
            # 处理 object 类型但包含 datetime/Timestamp 的情况
            elif dtype == object:
                # 检查是否包含 datetime 或 Timestamp 对象
                sample = prepared[col].dropna().iloc[0] if len(prepared[col].dropna()) > 0 else None
                if isinstance(sample, (pd.Timestamp, datetime)):
                    def convert_datetime(x):
                        if pd.isnull(x):
                            return None
                        if isinstance(x, pd.Timestamp):
                            # 确保带时区
                            if x.tzinfo is None:
                                x = x.tz_localize('UTC')
                            else:
                                x = x.tz_convert('UTC')
                            return x.to_pydatetime()
                        elif isinstance(x, datetime):
                            # 确保带时区
                            if x.tzinfo is None:
                                return x.replace(tzinfo=datetime.timezone.utc)
                            return x
                        return x
                    prepared[col] = prepared[col].apply(convert_datetime)
            # 浮点列：NaN 转为 None
            elif pd.api.types.is_float_dtype(dtype):
                prepared[col] = prepared[col].where(prepared[col].notna(), None)
            # 整数列：NaN 转为 None（需要转为 object 类型）
            elif pd.api.types.is_integer_dtype(dtype):
                prepared[col] = prepared[col].where(prepared[col].notna(), None)
                prepared[col] = prepared[col].astype(object)

        # 使用 numpy 快速转换为元组列表
        records = prepared.to_numpy()

        # 转换为 Python 原生类型的元组列表
        def convert_value(v):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return None
            elif hasattr(v, 'item'):  # numpy scalar
                return v.item()
            return v

        return [tuple(convert_value(v) for v in row) for row in records]
    
    async def _upsert_batch(self, batch: pd.DataFrame, table_name: str) -> int:
        """
        执行批量 upsert

        使用 PostgreSQL 的 ON CONFLICT DO UPDATE 语法。
        使用向量化数据转换替代 iterrows()，性能提升 5-10 倍。
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

        # 使用向量化方法转换数据（性能优化）
        values_list = self._prepare_values_list(batch, columns)

        if not values_list:
            return 0

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
    
    TABLE_NAME = "processed_valuation_pct"
    
    # 估值分位列
    VALUATION_COLUMNS = [
        "pe_ttm", "pb", "ps_ttm", "dv_ttm",  # 原始值
        "pe_ttm_pct_1250d", "pb_pct_1250d", "ps_ttm_pct_1250d",
        "peg",
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
        
        # 准备数据，过滤多余列
        allowed_columns = ["time", "symbol"] + self.VALUATION_COLUMNS
        available_columns = [c for c in df.columns if c in allowed_columns]
        
        if len(available_columns) <= 2:  # 只有 time 和 symbol
            logger.warning("No valid fundamental metric columns found in DataFrame")
            return 0
            
        result = df[available_columns].copy()
        result["processed_at"] = datetime.now()
        
        # 转换日期，避开 FutureWarning
        if "time" in result.columns:
             result["time"] = pd.to_datetime(result["time"]).apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
        
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

        # 使用向量化方法转换数据（性能优化）
        values_list = ProcessedDataStorage._prepare_values_list(result, columns)

        if not values_list:
            return 0

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
                elif ind == "valuation":
                    columns.extend(self.VALUATION_COLUMNS)
        else:
            columns.extend(self.VALUATION_COLUMNS)
        
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


class QuarterlyFundamentalDataStorage:
    """
    季度基本面指标存储管理器
    
    管理 F-Score 和补充指标的季度数据存储。
    """
    
    TABLE_NAME = "processed_fundamental_quality"
    
    # F-Score 列
    FSCORE_COLUMNS = [
        "f_score",
        "f_roa", "f_cfo", "f_delta_roa", "f_accrual",
        "f_delta_lever", "f_delta_liquid", "f_eq_offer",
        "f_delta_margin", "f_delta_turn"
    ]
    
    # 补充指标列
    EXTRA_COLUMNS = [
        "roa_ttm", "roe_5y_avg", "ni_cfo_corr_3y", "debt_ratio", "current_ratio",
        "cfo_ttm", "ni_ttm", "gpm_ttm", "at_ttm"
    ]
    
    # 日期列
    DATE_COLUMNS = ["end_date_time", "ann_date_time", "f_ann_date_time"]
    
    def __init__(self, db_manager: Optional["DatabaseManager"] = None):
        self.db_manager = db_manager
    
    async def upsert(
        self,
        df: pd.DataFrame,
        batch_size: int = 1000
    ) -> int:
        """
        批量插入/更新季度基本面指标数据
        
        Args:
            df: 包含 F-Score 及补充指标的 DataFrame
                必须包含: ts_code, end_date_time
                可选包含: ann_date_time, f_ann_date_time, F-Score列, 补充指标列
            batch_size: 批处理大小
            
        Returns:
            影响的记录数
        """
        if self.db_manager is None:
            logger.warning("No db_manager configured")
            return 0
        
        if df.empty:
            return 0
        
        # 准备数据
        allowed_columns = (
            ["ts_code"] + self.DATE_COLUMNS + 
            self.FSCORE_COLUMNS + self.EXTRA_COLUMNS + 
            ["exemptions"]
        )
        available_columns = [c for c in df.columns if c in allowed_columns]
        
        if "ts_code" not in available_columns or "end_date_time" not in available_columns:
            logger.error("DataFrame must contain 'ts_code' and 'end_date_time' columns")
            return 0
        
        result = df[available_columns].copy()
        result["processed_at"] = datetime.now()
        
        # 转换日期列
        for date_col in self.DATE_COLUMNS:
            if date_col in result.columns:
                result[date_col] = pd.to_datetime(result[date_col]).apply(
                    lambda x: x.to_pydatetime() if pd.notnull(x) else None
                )
        
        # 获取列名
        columns = list(result.columns)
        
        # 构建 SQL
        cols_str = ", ".join(columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        
        # 构建更新语句（排除主键）
        update_cols = [c for c in columns if c not in ["ts_code", "end_date_time"]]
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        
        sql = f"""
            INSERT INTO {self.TABLE_NAME} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (ts_code, end_date_time)
            DO UPDATE SET {update_str}
        """

        # 处理 exemptions 列（JSON 序列化）
        if "exemptions" in result.columns:
            import json
            result["exemptions"] = result["exemptions"].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else (x if pd.notna(x) else None)
            )

        # 使用向量化方法转换数据（性能优化）
        values_list = ProcessedDataStorage._prepare_values_list(result, columns)

        if not values_list:
            return 0

        total = 0
        try:
            engine = self.db_manager.get_engine()
            async with engine.begin() as conn:
                raw_conn = await conn.get_raw_connection()
                asyncpg_conn = raw_conn.driver_connection

                for i in range(0, len(values_list), batch_size):
                    batch = values_list[i:i+batch_size]
                    await asyncpg_conn.executemany(sql, batch)
                    total += len(batch)

            logger.info(f"Upserted {total} quarterly records to {self.TABLE_NAME}")
            return total
        except Exception as e:
            logger.error(f"Quarterly upsert failed: {e}")
            raise
    
    async def query(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        查询季度基本面指标
        
        Args:
            symbols: 股票代码列表（ts_code 格式）
            start_date: 开始日期（报告期）
            end_date: 结束日期（报告期）
            
        Returns:
            季度基本面指标 DataFrame
        """
        if self.db_manager is None:
            return pd.DataFrame()
        
        conditions = []
        params = {}
        
        if symbols:
            placeholders = ", ".join([f":sym_{i}" for i in range(len(symbols))])
            conditions.append(f"ts_code IN ({placeholders})")
            for i, sym in enumerate(symbols):
                params[f"sym_{i}"] = sym
        
        if start_date:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date).to_pydatetime()
            conditions.append("end_date_time >= :start_date")
            params["start_date"] = start_date
        
        if end_date:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date).to_pydatetime()
            conditions.append("end_date_time <= :end_date")
            params["end_date"] = end_date
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            {where_clause}
            ORDER BY ts_code, end_date_time
        """
        
        try:
            result = await self.db_manager.execute_raw_sql(sql, params)
            rows = result.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            # 获取列名
            columns = result.keys()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Quarterly query failed: {e}")
            return pd.DataFrame()
