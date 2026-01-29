"""
数据库管理器

管理数据库连接池和会话。
"""

import asyncio
from typing import Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncEngine,
)
from loguru import logger

from finance_data_hub.config import Settings


class DatabaseManager:
    """数据库管理器"""

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

        Raises:
            RuntimeError: 如果管理器未初始化
        """
        if not self._session_maker:
            raise RuntimeError(
                "Database not initialized. Call await initialize() first."
            )

        return self._session_maker()

    async def execute_raw_sql(self, sql: str, params: Optional[Dict] = None) -> Any:
        """
        执行原生SQL

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            SQL执行结果
        """
        if not self._engine:
            raise RuntimeError("Database engine not initialized")

        async with self._engine.begin() as conn:
            result = await conn.execute(text(sql), params or {})
            return result

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
