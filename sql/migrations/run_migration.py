#!/usr/bin/env python3
"""
数据库迁移工具
执行 symbol_minute 表的 frequency 字段迁移
"""

import asyncio
import sys
from pathlib import Path
from sqlalchemy import text
from loguru import logger

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from finance_data_hub.config import get_settings
from finance_data_hub.database.manager import DatabaseManager


async def run_migration():
    """执行数据库迁移"""
    logger.info("开始执行 symbol_minute 表 frequency 字段迁移...")

    # 初始化数据库连接
    settings = get_settings()
    db_manager = DatabaseManager(settings)
    await db_manager.initialize()

    try:
        # 读取迁移脚本
        migration_file = Path(__file__).parent / "001_add_frequency_to_symbol_minute.sql"

        if not migration_file.exists():
            raise FileNotFoundError(f"迁移脚本不存在: {migration_file}")

        logger.info(f"读取迁移脚本: {migration_file}")
        migration_sql = migration_file.read_text(encoding='utf-8')

        # 执行迁移
        logger.info("执行迁移脚本...")
        async with db_manager._engine.begin() as conn:
            # PostgreSQL 的 NOTICE 和 RAISE 消息会自动打印到控制台
            await conn.execute(text(migration_sql))

        logger.info("✓ 迁移成功完成！")

        # 验证迁移
        logger.info("验证迁移结果...")
        await verify_migration(db_manager)

    except Exception as e:
        logger.error(f"❌ 迁移失败: {str(e)}")
        logger.error("请检查错误日志，并考虑执行回滚脚本")
        raise

    finally:
        await db_manager.close()


async def verify_migration(db_manager: DatabaseManager):
    """验证迁移是否成功"""

    async with db_manager._engine.begin() as conn:
        # 1. 检查 frequency 字段是否存在
        result = await conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'symbol_minute'
                AND column_name = 'frequency'
            )
        """))
        has_frequency = result.scalar()

        if not has_frequency:
            raise Exception("frequency 字段不存在！")

        logger.info("✓ frequency 字段已创建")

        # 2. 检查主键
        result = await conn.execute(text("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'symbol_minute'
            AND constraint_type = 'PRIMARY KEY'
        """))
        pk_name = result.scalar()

        if not pk_name:
            raise Exception("主键不存在！")

        logger.info(f"✓ 主键约束存在: {pk_name}")

        # 3. 检查频率索引
        result = await conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM pg_indexes
                WHERE tablename = 'symbol_minute'
                AND indexname = 'idx_symbol_minute_freq'
            )
        """))
        has_index = result.scalar()

        if not has_index:
            logger.warning("⚠ 频率索引不存在")
        else:
            logger.info("✓ 频率索引已创建")

        # 4. 检查数据
        result = await conn.execute(text("""
            SELECT frequency, COUNT(*) as count
            FROM symbol_minute
            GROUP BY frequency
        """))
        freq_counts = result.fetchall()

        if freq_counts:
            logger.info("数据频率分布:")
            for freq, count in freq_counts:
                logger.info(f"  {freq}: {count} 条记录")
        else:
            logger.info("表中无数据")

        # 5. 检查 hypertable 配置
        result = await conn.execute(text("""
            SELECT num_dimensions
            FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'symbol_minute'
        """))
        num_dimensions = result.scalar()

        if num_dimensions == 2:
            logger.info("✓ TimescaleDB 复合分区已启用（时间 + 频率）")
        elif num_dimensions == 1:
            logger.warning("⚠ TimescaleDB 仅时间分区（建议检查配置）")
        else:
            logger.warning(f"⚠ TimescaleDB 分区维度异常: {num_dimensions}")


async def run_rollback():
    """执行回滚"""
    logger.warning("⚠️  警告：准备执行回滚操作")
    logger.warning("⚠️  这将删除所有非 1m 频率的数据！")

    # 确认回滚
    response = input("确认要回滚吗？输入 'yes' 继续，其他任何输入将取消: ")

    if response.lower() != 'yes':
        logger.info("已取消回滚操作")
        return

    logger.info("开始执行回滚...")

    settings = get_settings()
    db_manager = DatabaseManager(settings)
    await db_manager.initialize()

    try:
        # 读取回滚脚本
        rollback_file = Path(__file__).parent / "001_rollback_frequency.sql"

        if not rollback_file.exists():
            raise FileNotFoundError(f"回滚脚本不存在: {rollback_file}")

        logger.info(f"读取回滚脚本: {rollback_file}")
        rollback_sql = rollback_file.read_text(encoding='utf-8')

        # 执行回滚
        logger.info("执行回滚脚本...")
        async with db_manager._engine.begin() as conn:
            await conn.execute(text(rollback_sql))

        logger.info("✓ 回滚成功完成！")

    except Exception as e:
        logger.error(f"❌ 回滚失败: {str(e)}")
        raise

    finally:
        await db_manager.close()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="symbol_minute 表迁移工具")
    parser.add_argument(
        "action",
        choices=["migrate", "rollback", "verify"],
        help="操作类型：migrate（迁移）、rollback（回滚）、verify（仅验证）"
    )

    args = parser.parse_args()

    if args.action == "migrate":
        asyncio.run(run_migration())
    elif args.action == "rollback":
        asyncio.run(run_rollback())
    elif args.action == "verify":
        settings = get_settings()
        db_manager = DatabaseManager(settings)

        async def verify():
            await db_manager.initialize()
            try:
                await verify_migration(db_manager)
            finally:
                await db_manager.close()

        asyncio.run(verify())


if __name__ == "__main__":
    main()
