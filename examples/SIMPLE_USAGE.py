"""
FinanceDataHub SDK 简单使用示例（无需配置 SmartRouter）

此示例展示如何在不配置 SmartRouter 的情况下使用 SDK，
直接从 PostgreSQL 查询数据。

注意：只需要设置数据库连接，不需要配置 sources.yml 文件。
"""

import asyncio
from finance_data_hub.config import get_settings
from finance_data_hub import FinanceDataHub


async def main():
    """
    主函数 - 异步模式
    在 Jupyter Notebook 中直接运行此函数
    """
    print("=" * 60)
    print("FinanceDataHub SDK 简单使用示例")
    print("=" * 60)

    # 1. 初始化设置（从环境变量读取数据库配置）
    settings = get_settings()
    print(f"\n✓ 设置加载成功")
    print(f"  数据库URL: {settings.database.url[:50]}...")

    # 2. 创建 FinanceDataHub 实例（不指定 router_config_path）
    fdh = FinanceDataHub(settings, backend="postgresql")
    print(f"\n✓ SDK 初始化成功")
    print(f"  后端: {fdh.backend}")
    print(f"  SmartRouter: {'已启用' if fdh.router else '未启用（将使用 PostgreSQL 直接查询）'}")

    try:
        # 3. 查询日线数据
        print("\n" + "=" * 60)
        print("1. 查询日线数据")
        print("=" * 60)
        daily = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
        if daily is not None:
            print(f"✓ 获取数据成功: {len(daily)} 条记录")
            print(f"  数据列: {', '.join(daily.columns.tolist())}")
            print(f"  数据样例:")
            print(daily.head(3).to_string(index=False))
        else:
            print("✗ 未找到数据（可能需要先运行 fdh-cli update 更新数据）")

        # 4. 查询分钟数据
        print("\n" + "=" * 60)
        print("2. 查询分钟数据（5分钟线）")
        print("=" * 60)
        minute = await fdh.get_minute_async(
            ['600519.SH'],
            '2024-11-01',
            '2024-11-30',
            'minute_5'
        )
        if minute is not None:
            print(f"✓ 获取数据成功: {len(minute)} 条记录")
            print(f"  频率: {minute['frequency'].iloc[0]}")
            print(f"  数据样例:")
            print(minute.head(3)[['time', 'symbol', 'close', 'volume', 'frequency']].to_string(index=False))
        else:
            print("✗ 未找到数据（可能需要先运行 fdh-cli update 更新数据）")

        # 5. 查询每日基本面
        print("\n" + "=" * 60)
        print("3. 查询每日基本面")
        print("=" * 60)
        basic = await fdh.get_daily_basic_async(['600519.SH'], '2024-01-01', '2024-12-31')
        if basic is not None:
            print(f"✓ 获取数据成功: {len(basic)} 条记录")
            print(f"  主要指标:")
            print(basic.head(3)[['time', 'symbol', 'pe', 'pb', 'turnover_rate']].to_string(index=False))
        else:
            print("✗ 未找到数据")

        # 6. 查询复权因子
        print("\n" + "=" * 60)
        print("4. 查询复权因子")
        print("=" * 60)
        adj = await fdh.get_adj_factor_async(['600519.SH'], '2020-01-01', '2024-12-31')
        if adj is not None:
            print(f"✓ 获取数据成功: {len(adj)} 条记录")
            print(f"  数据样例:")
            print(adj.head(5)[['time', 'symbol', 'adj_factor']].to_string(index=False))
        else:
            print("✗ 未找到数据")

        # 7. 查询股票基本信息
        print("\n" + "=" * 60)
        print("5. 查询股票基本信息")
        print("=" * 60)
        info = await fdh.get_basic_async(['600519.SH', '000858.SZ'])
        if info is not None:
            print(f"✓ 获取数据成功: {len(info)} 条记录")
            print(f"  数据样例:")
            print(info[['symbol', 'name', 'industry', 'market', 'list_date']].to_string(index=False))
        else:
            print("✗ 未找到数据")

        # 8. 高周期聚合查询
        print("\n" + "=" * 60)
        print("6. 查询周线数据（自动聚合）")
        print("=" * 60)
        weekly = await fdh.get_weekly_async(['600519.SH'], '2024-01-01', '2024-12-31')
        if weekly is not None:
            print(f"✓ 获取数据成功: {len(weekly)} 条记录")
            print(f"  数据样例:")
            print(weekly.head(3)[['time', 'symbol', 'open', 'high', 'low', 'close']].to_string(index=False))
        else:
            print("✗ 未找到数据")

        print("\n" + "=" * 60)
        print("✓ 所有查询完成！")
        print("=" * 60)

    finally:
        # 9. 关闭连接
        await fdh.close()
        print("\n✓ 数据库连接已关闭")


# 同步版本（在普通 Python 脚本中使用）
def main_sync():
    """
    同步主函数 - 在普通 Python 脚本中使用
    """
    print("检测到普通 Python 环境，使用同步模式...")

    # 在 Jupyter 中使用同步方法需要 nest_asyncio
    try:
        import nest_asyncio
        nest_asyncio.apply()
        print("✓ 已启用 nest_asyncio（允许在 Jupyter 中运行异步代码）")
    except ImportError:
        print("⚠ 未安装 nest_asyncio，建议安装: pip install nest_asyncio")

    asyncio.run(main())


# 自动检测运行环境
if __name__ == "__main__":
    try:
        get_ipython()  # 如果能获取到 IPython 实例，说明在 Jupyter 中
        print("检测到 Jupyter Notebook 环境，使用异步模式...")
        print("提示：也可以在 Notebook 中直接使用 await 语法调用异步方法\n")
        asyncio.run(main())
    except NameError:
        # 普通 Python 脚本
        main_sync()


# 在 Jupyter Notebook 中的使用方式
"""
在 Jupyter Notebook 中，您也可以这样使用：

# 方法1：直接在 Notebook cell 中运行
from finance_data_hub.config import get_settings
from finance_data_hub import FinanceDataHub

settings = get_settings()
fdh = FinanceDataHub(settings, backend="postgresql")

# 直接使用 await（推荐）
daily = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
print(daily.head())

# 方法2：使用 nest_asyncio（如果需要同步方法）
import nest_asyncio
nest_asyncio.apply()

daily = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')  # 同步方法
print(daily.head())

# 方法3：调用此文件
exec(open('examples/SIMPLE_USAGE.py').read())
"""
