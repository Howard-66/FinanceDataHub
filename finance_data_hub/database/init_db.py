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
            if f.name.startswith(('001_', '002_', '003_', '004_'))
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

            # 简单的SQL分割 - 按分号分割并过滤空语句和注释
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

                # 处理dollar-quoted字符串状态
                if not in_dollar_quote:
                    # 检查是否进入dollar quote
                    if ' AS $$' in line or ' AS $' in line:
                        in_dollar_quote = True
                        # 提取tag
                        if ' AS $$' in line:
                            dollar_tag = '$$'
                        else:
                            tag_part = line.split(' AS $')[1].split()[0] if ' AS $' in line else '$function$'
                            if '$' in tag_part:
                                dollar_tag = '$' + tag_part.split('$')[1] + '$'
                            else:
                                dollar_tag = '$' + tag_part + '$'

                # 检查是否退出dollar quote
                if in_dollar_quote and dollar_tag and dollar_tag in line:
                    in_dollar_quote = False
                    dollar_tag = None

                current_statement.append(line)

                # 当遇到分号且不在dollar quote中时，结束语句
                if ';' in line and not in_dollar_quote:
                    statement = '\n'.join(current_statement)
                    if statement.strip():
                        statements.append(statement)
                    current_statement = []

                i += 1

            # 执行每个SQL语句
            async with self.db_manager._engine.begin() as conn:
                for idx, stmt in enumerate(statements):
                    # 清理SQL语句
                    clean_stmt = stmt.strip()
                    if clean_stmt and not clean_stmt.startswith('--'):
                        logger.debug(f"执行语句 {idx+1}/{len(statements)}: {clean_stmt[:100]}...")
                        try:
                            await conn.execute(text(clean_stmt))
                        except Exception as e:
                            # 忽略已存在的对象错误
                            if 'already exists' in str(e).lower() or 'already installed' in str(e).lower():
                                logger.warning(f"对象已存在，跳过: {str(e)[:100]}")
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
            'adj_factor'
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
