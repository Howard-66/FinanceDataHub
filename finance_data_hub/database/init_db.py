"""
数据库初始化模块

提供数据库模式创建、表初始化等功能。
"""

import os
import asyncio
from pathlib import Path
from typing import List, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from loguru import logger

from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.config import Settings


class DatabaseInitializer:
    """数据库初始化器"""

    def __init__(self, settings: Settings, db_manager: Optional[DatabaseManager] = None):
        """
        初始化数据库初始化器

        Args:
            settings: 应用配置
            db_manager: 数据库管理器，如果为None则创建新的
        """
        self.settings = settings
        self.db_manager = db_manager or DatabaseManager(settings)

    async def initialize(self) -> None:
        """执行完整的数据库初始化流程"""
        logger.info("开始初始化数据库...")

        # 确保数据库管理器已初始化
        if not self.db_manager._engine:
            await self.db_manager.initialize()

        # 获取SQL脚本目录
        sql_dir = Path(__file__).parent.parent.parent / "sql" / "init"
        if not sql_dir.exists():
            raise FileNotFoundError(f"SQL初始化脚本目录不存在: {sql_dir}")

        # 获取所有SQL文件并排序
        sql_files = sorted([
            f for f in sql_dir.glob("*.sql")
            if f.name.startswith(('001_', '002_', '003_', '004_', '005_', '006_', '007_'))
        ])

        if not sql_files:
            raise FileNotFoundError(f"在 {sql_dir} 中未找到SQL初始化脚本")

        # 逐个执行SQL文件
        for sql_file in sql_files:
            await self._execute_sql_file(sql_file)

        logger.info("[OK] 数据库初始化完成")

    async def _execute_sql_file(self, sql_file: Path) -> None:
        """
        执行单个SQL文件

        Args:
            sql_file: SQL文件路径
        """
        logger.info(f"执行SQL脚本: {sql_file.name}")

        try:
            # 读取SQL内容
            sql_content = sql_file.read_text(encoding='utf-8')

            # 更智能的SQL分割 - 支持dollar-quoted字符串
            statements = []
            current_statement = []
            in_dollar_quote = False
            dollar_tag = None

            lines = sql_content.split('\n')
            i = 0
            while i < len(lines):
                line = lines[i]
                stripped = line.strip()

                # 跳过空行和注释（如果不在函数定义中）
                if not stripped:
                    if current_statement:
                        current_statement.append(line)
                    i += 1
                    continue

                if stripped.startswith('--'):
                    if current_statement:
                        current_statement.append(line)
                    i += 1
                    continue

                # 处理dollar-quoted字符串状态 - 改进的逻辑
                if not in_dollar_quote:
                    # 检查是否进入dollar quote - 精确匹配AS关键字后的$
                    # 匹配形如 "RETURNS ... AS $$" 或 "RETURNS ... AS $tag$" 的模式
                    if ' AS ' in line and '$' in line:
                        # 提取AS后的内容
                        as_part = line.split(' AS ', 1)[1] if ' AS ' in line else ''
                        if as_part.startswith('$$'):
                            in_dollar_quote = True
                            dollar_tag = '$$'
                        elif as_part.startswith('$') and '$' in as_part[1:]:
                            # 提取tag，例如 $function$
                            end_dollar = as_part.find('$', 1)
                            if end_dollar > 0:
                                tag = as_part[:end_dollar+1]
                                in_dollar_quote = True
                                dollar_tag = tag
                else:
                    # 在dollar quote中，检查是否退出
                    # 使用更精确的匹配 - 检查整行是否只包含dollar tag和其他字符
                    if dollar_tag and dollar_tag in line:
                        # 检查这是否是退出的地方
                        # 如果当前行包含dollar tag，且后面跟空白和可选的语言信息
                        line_after_tag = line.split(dollar_tag, 1)[1] if dollar_tag in line else ''
                        if not line_after_tag or line_after_tag.strip().startswith('LANGUAGE') or line_after_tag.strip().startswith(';'):
                            in_dollar_quote = False
                            dollar_tag = None

                current_statement.append(line)

                # 当遇到分号且不在dollar quote中时，结束语句
                if ';' in stripped and not in_dollar_quote:
                    statement = '\n'.join(current_statement)
                    if statement.strip():
                        statements.append(statement)
                    current_statement = []

                i += 1

            # 执行每个SQL语句（每条语句独立事务，失败后继续执行后续语句）
            for idx, stmt in enumerate(statements):
                # 清理SQL语句
                clean_stmt = stmt.strip()
                if clean_stmt and not clean_stmt.startswith('--'):
                    logger.debug(f"执行语句 {idx+1}/{len(statements)}: {clean_stmt[:100]}...")
                    try:
                        async with self.db_manager._engine.begin() as conn:
                            await conn.execute(text(clean_stmt))
                    except Exception as e:
                        error_msg = str(e).lower()
                        # 忽略已存在的对象错误
                        if 'already exists' in error_msg or 'already installed' in error_msg:
                            logger.warning(f"对象已存在，跳过: {str(e)[:100]}")
                        # 忽略不存在的对象错误（用于索引等依赖对象）
                        elif 'relation does not exist' in error_msg or 'does not exist' in error_msg:
                            logger.warning(f"依赖对象不存在，跳过: {str(e)[:100]}")
                        # 忽略某些视图/物化视图错误
                        elif 'materialized view' in error_msg and 'does not exist' in error_msg:
                            logger.warning(f"视图不存在，跳过: {str(e)[:100]}")
                        else:
                            logger.error(f"执行SQL失败: {str(e)}")
                            logger.error(f"失败的SQL: {clean_stmt[:300]}")
                            raise

            logger.info(f"[OK] 完成: {sql_file.name}")

        except Exception as e:
            logger.error(f"执行SQL文件失败 {sql_file.name}: {str(e)}")
            raise

    async def check_tables_exist(self) -> dict:
        """
        检查数据库表是否存在

        Returns:
            dict: 表存在性检查结果
        """
        tables_to_check = [
            'asset_basic',
            'symbol_daily',
            'symbol_minute',
            'daily_basic',
            'adj_factor',
            'index_daily',
            'symbol_weekly',       # 连续聚合视图
            'symbol_monthly',      # 连续聚合视图
            'daily_basic_weekly',   # 连续聚合视图
            'daily_basic_monthly',  # 连续聚合视图
            'adj_factor_weekly',    # 复权因子周线聚合
            'adj_factor_monthly'    # 复权因子月线聚合
        ]

        results = {}

        async with self.db_manager._engine.begin() as conn:
            for table in tables_to_check:
                result = await conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = '{table}'
                    ) as exists
                """))
                row = result.fetchone()
                results[table] = bool(row[0]) if row else False

        return results

    async def close(self) -> None:
        """关闭数据库连接"""
        if self.db_manager:
            await self.db_manager.close()


async def init_database(settings: Settings, verbose: bool = False) -> None:
    """
    初始化数据库的便捷函数

    Args:
        settings: 应用配置
        verbose: 是否显示详细信息
    """
    initializer = DatabaseInitializer(settings)

    try:
        await initializer.initialize()

        # 检查表是否存在
        table_status = await initializer.check_tables_exist()

        if verbose:
            print("\n[INFO] 数据库表状态:")
            for table, exists in table_status.items():
                status = "[OK] 存在" if exists else "[FAIL] 不存在"
                print(f"  {table}: {status}")

        print("\n[OK] 数据库初始化成功！")

    except Exception as e:
        print(f"\n[ERROR] 数据库初始化失败: {str(e)}")
        raise
    finally:
        await initializer.close()
