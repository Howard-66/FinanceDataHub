"""
数据库清理模块

提供数据库模式清理、删除表、视图、函数等功能。
用于重置数据库环境或删除特定数据对象。
"""

import asyncio
from typing import List, Optional, Set
from sqlalchemy import text
from loguru import logger

from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.config import Settings


class DatabaseCleanup:
    """数据库清理器"""

    def __init__(self, settings: Settings, db_manager: Optional[DatabaseManager] = None):
        """
        初始化数据库清理器

        Args:
            settings: 应用配置
            db_manager: 数据库管理器，如果为None则创建新的
        """
        self.settings = settings
        self.db_manager = db_manager or DatabaseManager(settings)

    async def initialize(self) -> None:
        """确保数据库管理器已初始化"""
        if not self.db_manager._engine:
            await self.db_manager.initialize()

    async def cleanup_all(self, verbose: bool = False) -> dict:
        """
        执行完整的数据库清理流程（删除所有数据对象）

        Args:
            verbose: 是否显示详细信息

        Returns:
            dict: 清理结果
        """
        logger.info("开始清理数据库...")

        results = {
            "continuous_aggregates": [],
            "functions": [],
            "views": [],
            "hypertables": [],
            "tables": [],
            "errors": []
        }

        # 按依赖顺序清理（反向初始化顺序）

        # 1. 删除连续聚合
        if verbose:
            logger.info("删除连续聚合...")
        results["continuous_aggregates"] = await self._drop_continuous_aggregates()

        # 2. 删除函数
        if verbose:
            logger.info("删除函数...")
        results["functions"] = await self._drop_functions()

        # 3. 删除视图
        if verbose:
            logger.info("删除视图...")
        results["views"] = await self._drop_views()

        # 4. 删除超表数据（保留表结构）
        if verbose:
            logger.info("清空超表数据...")
        results["hypertables"] = await self._truncate_hypertables()

        # 5. 删除表
        if verbose:
            logger.info("删除表...")
        results["tables"] = await self._drop_tables()

        # 报告结果
        total_dropped = (
            len(results["continuous_aggregates"]) +
            len(results["functions"]) +
            len(results["views"]) +
            len(results["hypertables"]) +
            len(results["tables"])
        )

        logger.info(f"[OK] 数据库清理完成，共删除 {total_dropped} 个对象")

        if results["errors"]:
            logger.warning(f"有 {len(results['errors'])} 个错误")
            for error in results["errors"]:
                logger.error(f"  - {error}")

        return results

    async def cleanup_continuous_aggregates(self, verbose: bool = False) -> List[str]:
        """
        只清理连续聚合（保留表和函数）

        Args:
            verbose: 是否显示详细信息

        Returns:
            List[str]: 被删除的连续聚合名称列表
        """
        logger.info("清理连续聚合...")
        return await self._drop_continuous_aggregates(verbose)

    async def cleanup_data_only(self, tables: Optional[List[str]] = None, verbose: bool = False) -> dict:
        """
        只清空数据，保留表结构和函数

        Args:
            tables: 要清空数据的表列表，默认清空所有时序表
            verbose: 是否显示详细信息

        Returns:
            dict: 清理结果
        """
        logger.info("清空数据库数据（保留表结构）...")

        results = {
            "truncated": [],
            "errors": []
        }

        if tables is None:
            tables = [
                "symbol_daily",
                "symbol_minute",
                "symbol_tick",
                "daily_basic",
                "adj_factor",
                "asset_basic",
                "cn_gdp"
            ]

        for table in tables:
            try:
                async with self.db_manager._engine.begin() as conn:
                    await conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                    results["truncated"].append(table)
                    if verbose:
                        logger.info(f"  已清空: {table}")
            except Exception as e:
                error_msg = f"清空表 {table} 失败: {str(e)}"
                results["errors"].append(error_msg)
                logger.error(error_msg)

        logger.info(f"[OK] 数据清空完成，共清空 {len(results['truncated'])} 个表")
        return results

    async def _drop_continuous_aggregates(self, verbose: bool = False) -> List[str]:
        """删除所有连续聚合"""
        dropped = []

        continuous_aggregates = [
            "symbol_weekly",
            "symbol_monthly",
            "daily_basic_weekly",
            "daily_basic_monthly",
            "adj_factor_weekly",
            "adj_factor_monthly"
        ]

        async with self.db_manager._engine.begin() as conn:
            for agg_name in continuous_aggregates:
                try:
                    # 先删除刷新策略
                    try:
                        await conn.execute(text(f"DROP POLICY IF EXISTS _timescaledb_internal.refresh_job_policy ON {agg_name}"))
                    except Exception:
                        pass  # 策略可能不存在

                    # 删除连续聚合
                    await conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {agg_name} CASCADE"))
                    dropped.append(agg_name)
                    if verbose:
                        logger.info(f"  已删除连续聚合: {agg_name}")
                except Exception as e:
                    logger.warning(f"删除连续聚合 {agg_name} 失败: {str(e)}")

        return dropped

    async def _drop_functions(self, verbose: bool = False) -> List[str]:
        """删除所有自定义函数"""
        dropped = []

        functions = [
            "get_latest_trading_date",
            "upsert_asset_basic",
            "check_data_integrity"
        ]

        async with self.db_manager._engine.begin() as conn:
            for func_name in functions:
                try:
                    await conn.execute(text(f"DROP FUNCTION IF EXISTS {func_name}() CASCADE"))
                    dropped.append(func_name)
                    if verbose:
                        logger.info(f"  已删除函数: {func_name}")
                except Exception as e:
                    logger.warning(f"删除函数 {func_name} 失败: {str(e)}")

        return dropped

    async def _drop_views(self, verbose: bool = False) -> List[str]:
        """删除所有视图"""
        dropped = []

        views = [
            "v_asset_basic_active"
        ]

        async with self.db_manager._engine.begin() as conn:
            for view_name in views:
                try:
                    await conn.execute(text(f"DROP VIEW IF EXISTS {view_name} CASCADE"))
                    dropped.append(view_name)
                    if verbose:
                        logger.info(f"  已删除视图: {view_name}")
                except Exception as e:
                    logger.warning(f"删除视图 {view_name} 失败: {str(e)}")

        return dropped

    async def _truncate_hypertables(self, verbose: bool = False) -> List[str]:
        """清空超表数据（保留表结构）"""
        truncated = []

        hypertables = [
            "symbol_daily",
            "symbol_minute",
            "symbol_tick",
            "daily_basic",
            "adj_factor"
        ]

        async with self.db_manager._engine.begin() as conn:
            for table in hypertables:
                try:
                    await conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                    truncated.append(table)
                    if verbose:
                        logger.info(f"  已清空超表: {table}")
                except Exception as e:
                    logger.warning(f"清空超表 {table} 失败: {str(e)}")

        return truncated

    async def _drop_tables(self, verbose: bool = False) -> List[str]:
        """删除所有表"""
        dropped = []

        tables = [
            "asset_basic",
            "daily_basic",
            "cn_gdp",
            "symbol_daily",
            "symbol_minute",
            "symbol_tick",
            "adj_factor"
        ]

        async with self.db_manager._engine.begin() as conn:
            for table in tables:
                try:
                    await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    dropped.append(table)
                    if verbose:
                        logger.info(f"  已删除表: {table}")
                except Exception as e:
                    logger.warning(f"删除表 {table} 失败: {str(e)}")

        return dropped

    async def drop_specific_objects(
        self,
        tables: Optional[List[str]] = None,
        views: Optional[List[str]] = None,
        functions: Optional[List[str]] = None,
        continuous_aggregates: Optional[List[str]] = None,
        verbose: bool = False
    ) -> dict:
        """
        删除指定的对象

        Args:
            tables: 要删除的表列表
            views: 要删除的视图列表
            functions: 要删除的函数列表
            continuous_aggregates: 要删除的连续聚合列表
            verbose: 是否显示详细信息

        Returns:
            dict: 删除结果
        """
        results = {
            "tables": [],
            "views": [],
            "functions": [],
            "continuous_aggregates": [],
            "errors": []
        }

        async with self.db_manager._engine.begin() as conn:
            # 删除连续聚合
            if continuous_aggregates:
                for name in continuous_aggregates:
                    try:
                        await conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE"))
                        results["continuous_aggregates"].append(name)
                        if verbose:
                            logger.info(f"  已删除连续聚合: {name}")
                    except Exception as e:
                        results["errors"].append(f"删除连续聚合 {name} 失败: {str(e)}")

            # 删除函数
            if functions:
                for name in functions:
                    try:
                        await conn.execute(text(f"DROP FUNCTION IF EXISTS {name}() CASCADE"))
                        results["functions"].append(name)
                        if verbose:
                            logger.info(f"  已删除函数: {name}")
                    except Exception as e:
                        results["errors"].append(f"删除函数 {name} 失败: {str(e)}")

            # 删除视图
            if views:
                for name in views:
                    try:
                        await conn.execute(text(f"DROP VIEW IF EXISTS {name} CASCADE"))
                        results["views"].append(name)
                        if verbose:
                            logger.info(f"  已删除视图: {name}")
                    except Exception as e:
                        results["errors"].append(f"删除视图 {name} 失败: {str(e)}")

            # 删除表
            if tables:
                for name in tables:
                    try:
                        await conn.execute(text(f"DROP TABLE IF EXISTS {name} CASCADE"))
                        results["tables"].append(name)
                        if verbose:
                            logger.info(f"  已删除表: {name}")
                    except Exception as e:
                        results["errors"].append(f"删除表 {name} 失败: {str(e)}")

        return results

    async def close(self) -> None:
        """关闭数据库连接"""
        if self.db_manager:
            await self.db_manager.close()


async def cleanup_database(settings: Settings, mode: str = "all", verbose: bool = False) -> dict:
    """
    清理数据库的便捷函数

    Args:
        settings: 应用配置
        mode: 清理模式
            - "all": 删除所有数据对象（表、视图、函数、连续聚合）
            - "data_only": 只清空数据，保留表结构
            - "aggregates": 只删除连续聚合
        verbose: 是否显示详细信息

    Returns:
        dict: 清理结果
    """
    cleanup = DatabaseCleanup(settings)

    try:
        await cleanup.initialize()

        if mode == "all":
            result = await cleanup.cleanup_all(verbose=verbose)
        elif mode == "data_only":
            result = await cleanup.cleanup_data_only(verbose=verbose)
        elif mode == "aggregates":
            result = {"continuous_aggregates": await cleanup.cleanup_continuous_aggregates(verbose=verbose)}
        else:
            raise ValueError(f"不支持的清理模式: {mode}")

        return result

    finally:
        await cleanup.close()
