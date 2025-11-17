#!/usr/bin/env python3
"""
检查并修复 daily_basic 表结构
"""

import asyncio
import asyncpg
import sys


async def check_and_fix_table():
    """检查并修复 daily_basic 表"""
    # 连接数据库
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='trading_nexus',
        password='trading.nexus.data',
        database='trading_nexus_db'
    )

    try:
        # 检查表结构
        print("正在检查 daily_basic 表结构...")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'daily_basic'
            ORDER BY ordinal_position
        """)

        print("\n当前表结构:")
        print("-" * 60)
        for col in columns:
            print(f"{col['column_name']:<25} {col['data_type']:<20} {'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'}")
        print("-" * 60)

        # 检查唯一约束
        print("\n正在检查唯一约束...")
        constraints = await conn.fetch("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'daily_basic'
            AND constraint_type = 'UNIQUE'
        """)

        print("\n当前唯一约束:")
        for c in constraints:
            print(f"  - {c['constraint_name']}: {c['constraint_type']}")

        # 检查是否存在 (symbol, time) 的唯一约束
        unique_exists = await conn.fetchval("""
            SELECT 1
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_name = 'daily_basic'
                AND tc.constraint_type = 'UNIQUE'
                AND ccu.column_name = 'time'
            HAVING COUNT(DISTINCT ccu.column_name) = 2
                AND MAX(ccu.column_name) = 'time'
                AND MIN(ccu.column_name) = 'symbol'
        """)

        if unique_exists:
            print("\n✅ 唯一约束 (symbol, time) 已存在")
        else:
            print("\n⚠️  唯一约束 (symbol, time) 不存在，正在添加...")
            try:
                # 尝试添加唯一约束
                await conn.execute("""
                    ALTER TABLE daily_basic
                    ADD CONSTRAINT daily_basic_symbol_time_key
                    UNIQUE (symbol, time)
                """)
                print("✅ 成功添加唯一约束")
            except Exception as e:
                print(f"❌ 添加唯一约束失败: {e}")
                print("\n尝试删除重复数据后重试...")

                # 检查重复数据
                duplicates = await conn.fetch("""
                    SELECT symbol, time, COUNT(*) as cnt
                    FROM daily_basic
                    GROUP BY symbol, time
                    HAVING COUNT(*) > 1
                """)

                if duplicates:
                    print(f"\n发现 {len(duplicates)} 组重复数据:")
                    for dup in duplicates[:5]:  # 只显示前5个
                        print(f"  - {dup['symbol']} @ {dup['time']}: {dup['cnt']} 条记录")

                    # 删除重复数据，保留最新的一条
                    print("\n正在删除重复数据...")
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
                    print("✅ 成功添加唯一约束")
                else:
                    raise

        print("\n🎉 修复完成！")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(check_and_fix_table())
