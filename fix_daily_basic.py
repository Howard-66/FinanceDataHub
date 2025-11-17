#!/usr/bin/env python3
"""
使用项目自身的数据库管理器修复 daily_basic 表
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, '/Volumes/Repository/Projects/TradingNexus/FinanceDataHub')

from finance_data_hub.database.manager import DatabaseManager
import asyncio


async def fix_daily_basic_table():
    """修复 daily_basic 表"""
    print("正在初始化数据库管理器...")
    db_manager = DatabaseManager()
    await db_manager.initialize()

    try:
        conn = await db_manager._engine.connect()

        # 检查表结构
        print("\n正在检查 daily_basic 表结构...")
        columns = await conn.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'daily_basic'
            ORDER BY ordinal_position
        """)

        print("\n当前表结构:")
        print("-" * 60)
        async for row in columns:
            print(f"{row[0]:<25} {row[1]:<20} {'NULL' if row[2] == 'YES' else 'NOT NULL'}")
        print("-" * 60)

        # 检查唯一约束
        print("\n正在检查唯一约束...")
        constraints = await conn.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'daily_basic'
            AND constraint_type = 'UNIQUE'
        """)

        print("\n当前唯一约束:")
        async for row in constraints:
            print(f"  - {row[0]}: {row[1]}")

        # 检查是否已经存在 (symbol, time) 唯一约束
        print("\n⚠️  正在检查是否需要添加唯一约束 (symbol, time)...")
        try:
            await conn.execute("""
                ALTER TABLE daily_basic
                ADD CONSTRAINT daily_basic_symbol_time_key
                UNIQUE (symbol, time)
            """)
            print("✅ 成功添加唯一约束 daily_basic_symbol_time_key")
        except Exception as e:
            if "already exists" in str(e):
                print("✅ 唯一约束 daily_basic_symbol_time_key 已存在")
            else:
                print(f"❌ 添加唯一约束失败: {e}")
                print("\n⚠️  尝试删除重复数据后重试...")

                # 检查重复数据
                duplicates = await conn.execute("""
                    SELECT symbol, time, COUNT(*) as cnt
                    FROM daily_basic
                    GROUP BY symbol, time
                    HAVING COUNT(*) > 1
                """)

                dup_count = 0
                async for row in duplicates:
                    dup_count += row[2]
                    if dup_count <= 5:  # 只显示前5个
                        print(f"  - {row[0]} @ {row[1]}: {row[2]} 条记录")

                if dup_count > 0:
                    print(f"\n发现总计 {dup_count} 条重复数据")
                    print("正在删除重复数据...")

                    # 删除重复数据，保留最新的一条
                    await conn.execute("""
                        DELETE FROM daily_basic
                        WHERE id NOT IN (
                            SELECT DISTINCT ON (symbol, time) id
                            FROM daily_basic
                            ORDER BY symbol, time, created_at DESC
                        )
                    """)
                    print("✅ 重复数据已删除")

                    # 再次尝试添加唯一约束
                    await conn.execute("""
                        ALTER TABLE daily_basic
                        ADD CONSTRAINT daily_basic_symbol_time_key
                        UNIQUE (symbol, time)
                    """)
                    print("✅ 成功添加唯一约束 daily_basic_symbol_time_key")
                else:
                    print("⚠️  未发现重复数据，但仍然无法添加唯一约束")
                    raise

        print("\n🎉 修复完成！")
        await conn.close()

    finally:
        await db_manager.close()


if __name__ == "__main__":
    asyncio.run(fix_daily_basic_table())
