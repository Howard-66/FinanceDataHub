"""
数据库管理器

管理数据库连接池和会话。
"""

import asyncio
from dataclasses import dataclass
from time import perf_counter
from typing import Optional, Dict, Any, ClassVar, Tuple, List
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncEngine,
)
from loguru import logger

from finance_data_hub.config import Settings


@dataclass(frozen=True)
class ReadQueryOptions:
    """读查询执行选项。"""

    query_type: str = "raw_sql"
    heavy: bool = False
    symbols_count: int = 0
    fallback_used: bool = False


class ReadQueryExecutionError(RuntimeError):
    """读查询执行失败。"""

    def __init__(self, query_type: str, original_error: Exception):
        self.query_type = query_type
        self.original_error = original_error
        super().__init__(f"Read query '{query_type}' failed: {original_error}")


class DatabaseManager:
    """数据库管理器"""

    _read_query_limiters: ClassVar[
        Dict[Tuple[int, int, int], Dict[str, asyncio.Semaphore]]
    ] = {}
    HEAVY_QUERY_LOG_WAIT_MS: ClassVar[float] = 10.0
    HEAVY_QUERY_LOG_EXEC_MS: ClassVar[float] = 500.0
    NORMAL_QUERY_LOG_WAIT_MS: ClassVar[float] = 50.0
    NORMAL_QUERY_LOG_EXEC_MS: ClassVar[float] = 1000.0

    def __init__(self, settings: Settings):
        """
        初始化数据库管理器

        Args:
            settings: 应用配置
        """
        self.settings = settings
        self._engine: Optional[AsyncEngine] = None
        self._session_maker: Optional[async_sessionmaker] = None

    async def initialize(self) -> None:
        """初始化数据库连接"""
        if self._engine is not None:
            return

        # 构建异步数据库URL
        database_url = self.settings.database.url

        # 将 postgresql:// 转换为 postgresql+asyncpg://
        if database_url.startswith("postgresql://"):
            async_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
        elif database_url.startswith("postgresql+asyncpg://"):
            async_url = database_url
        else:
            raise ValueError(f"Unsupported database URL: {database_url}")

        try:
            # 创建异步引擎
            self._engine = create_async_engine(
                async_url,
                echo=False,  # 设置为True可以看到SQL日志
                pool_size=self.settings.database.pool_size,
                max_overflow=self.settings.database.max_overflow,
                pool_pre_ping=True,  # 启用连接预检
                pool_recycle=3600,  # 1小时后回收连接
            )

            # 创建会话工厂
            self._session_maker = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            # 测试异步连接
            await self.test_connection()

            logger.info("Database manager initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise

    async def test_connection(self) -> bool:
        """
        测试数据库连接

        Returns:
            bool: 连接是否成功

        Raises:
            Exception: 连接失败时抛出
        """
        if not self._engine:
            raise RuntimeError("Database engine not initialized")

        async with self._engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

        return True

    async def get_session(self) -> AsyncSession:
        """
        获取数据库会话

        Returns:
            AsyncSession: 异步数据库会话

        Note:
            如果数据库连接未初始化，会自动进行初始化（惰性初始化）
        """
        if not self._session_maker:
            await self.initialize()

        return self._session_maker()

    def get_engine(self) -> AsyncEngine:
        """
        获取数据库引擎（确保已初始化）

        Returns:
            AsyncEngine: 异步数据库引擎

        Note:
            如果数据库连接未初始化，会自动进行初始化（惰性初始化）
        """
        if self._engine is None:
            # 构建异步数据库URL
            database_url = self.settings.database.url

            # 将 postgresql:// 转换为 postgresql+asyncpg://
            if database_url.startswith("postgresql://"):
                async_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
            elif database_url.startswith("postgresql+asyncpg://"):
                async_url = database_url
            else:
                raise ValueError(f"Unsupported database URL: {database_url}")

            # 创建异步引擎
            self._engine = create_async_engine(
                async_url,
                echo=False,
                pool_size=self.settings.database.pool_size,
                max_overflow=self.settings.database.max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,
            )

            # 创建会话工厂
            self._session_maker = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            logger.info("Database engine initialized synchronously")

        return self._engine

    @classmethod
    def _get_read_query_limiters(
        cls,
        query_limit: int,
        heavy_limit: int,
    ) -> Dict[str, asyncio.Semaphore]:
        """按事件循环和配置复用读查询限流器。"""
        loop = asyncio.get_running_loop()
        key = (id(loop), query_limit, heavy_limit)

        if key not in cls._read_query_limiters:
            cls._read_query_limiters[key] = {
                "query": asyncio.Semaphore(query_limit),
                "heavy": asyncio.Semaphore(heavy_limit),
            }

        return cls._read_query_limiters[key]

    async def _acquire_read_query_slots(
        self,
        options: ReadQueryOptions,
    ) -> tuple[List[asyncio.Semaphore], float]:
        """获取读查询执行槽位。"""
        limiters = self._get_read_query_limiters(
            self.settings.database.query_max_concurrency,
            self.settings.database.heavy_query_max_concurrency,
        )

        semaphores = [limiters["query"]]
        if options.heavy:
            semaphores.append(limiters["heavy"])

        acquired: List[asyncio.Semaphore] = []
        wait_start = perf_counter()

        try:
            for semaphore in semaphores:
                await semaphore.acquire()
                acquired.append(semaphore)
        except Exception:
            for semaphore in reversed(acquired):
                semaphore.release()
            raise

        waited_ms = (perf_counter() - wait_start) * 1000
        return acquired, waited_ms

    def _log_read_query_metrics(
        self,
        options: ReadQueryOptions,
        waited_ms: float,
        execution_ms: float,
        exception: Optional[Exception] = None,
    ) -> None:
        """记录读查询执行指标。"""
        if exception is None and not options.fallback_used:
            if options.heavy:
                should_log = (
                    waited_ms >= self.HEAVY_QUERY_LOG_WAIT_MS
                    or execution_ms >= self.HEAVY_QUERY_LOG_EXEC_MS
                )
            else:
                should_log = (
                    waited_ms >= self.NORMAL_QUERY_LOG_WAIT_MS
                    or execution_ms >= self.NORMAL_QUERY_LOG_EXEC_MS
                )

            if not should_log:
                return

        log_message = (
            "Read query completed | query_type={} | heavy={} | "
            "waited_for_semaphore_ms={:.2f} | execution_ms={:.2f} | "
            "symbols_count={} | fallback_happened={} | exception_happened={}"
        )
        log_args = (
            options.query_type,
            options.heavy,
            waited_ms,
            execution_ms,
            options.symbols_count,
            options.fallback_used,
            exception is not None,
        )

        if exception is not None:
            logger.opt(exception=exception).error(log_message, *log_args)
        elif options.fallback_used:
            logger.warning(log_message, *log_args)
        elif options.heavy:
            logger.info(log_message, *log_args)
        else:
            logger.debug(log_message, *log_args)

    async def execute_raw_sql(
        self,
        sql: str,
        params: Optional[Dict] = None,
        *,
        options: Optional[ReadQueryOptions] = None,
    ) -> Any:
        """
        执行原生SQL

        Args:
            sql: SQL语句
            params: 参数
            options: 读查询执行选项

        Returns:
            SQL执行结果
        """
        if not self._engine:
            await self.initialize()

        query_options = options or ReadQueryOptions()
        acquired_semaphores, waited_ms = await self._acquire_read_query_slots(
            query_options
        )
        execution_start = perf_counter()

        try:
            async with self._engine.begin() as conn:
                result = await conn.execute(text(sql), params or {})

            execution_ms = (perf_counter() - execution_start) * 1000
            self._log_read_query_metrics(query_options, waited_ms, execution_ms)
            return result
        except Exception as exc:
            execution_ms = (perf_counter() - execution_start) * 1000
            self._log_read_query_metrics(
                query_options,
                waited_ms,
                execution_ms,
                exception=exc,
            )
            raise ReadQueryExecutionError(query_options.query_type, exc) from exc
        finally:
            for semaphore in reversed(acquired_semaphores):
                semaphore.release()

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._engine:
            try:
                await self._engine.dispose()
                logger.info("Database connections closed")
            except Exception as e:
                logger.warning(f"Error closing database connections: {e}")

    async def __aenter__(self) -> "DatabaseManager":
        """异步上下文管理器入口"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """异步上下文管理器出口"""
        await self.close()
