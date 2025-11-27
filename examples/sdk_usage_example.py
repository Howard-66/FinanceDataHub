"""
FinanceDataHub SDK 使用示例

演示如何使用 FinanceDataHub SDK 查询各种金融数据。

此示例支持两种运行方式：
1. 在 Jupyter Notebook 中使用 async/await
2. 在普通 Python 脚本中使用 asyncio.run()
"""

import asyncio
import sys
from finance_data_hub.config import get_settings
from finance_data_hub.sdk import FinanceDataHub

async def main_async():
    """异步主函数 - 适用于 Jupyter Notebook"""
    # 初始化设置
    settings = get_settings()

    # 创建 FinanceDataHub 实例（包含 SmartRouter 集成）
    fdh = FinanceDataHub(
        settings=settings,
        backend="postgresql",
        router_config_path="sources.yml"
    )

    print("=" * 60)
    print("FinanceDataHub SDK 使用示例 (异步版本)")
    print("=" * 60)

    # 示例 1: 查询日线数据
    print("\n1. 查询日线数据")
    print("-" * 60)
    try:
        daily_data = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
        if daily_data is not None:
            print(f"✓ 获取日线数据成功: {len(daily_data)} 条记录")
            print(f"  列: {', '.join(daily_data.columns)}")
        else:
            print("✗ 未找到数据")
    except Exception as e:
        print(f"✗ 查询失败: {e}")

    # 示例 2: 查询分钟数据
    print("\n2. 查询分钟数据")
    print("-" * 60)
    try:
        minute_data = await fdh.get_minute_async(['600519.SH'], '2024-11-01', '2024-11-30', 'minute_5')
        if minute_data is not None:
            print(f"✓ 获取5分钟数据成功: {len(minute_data)} 条记录")
            print(f"  列: {', '.join(minute_data.columns)}")
        else:
            print("✗ 未找到数据")
    except Exception as e:
        print(f"✗ 查询失败: {e}")

    # 示例 3: 查询每日基本面
    print("\n3. 查询每日基本面数据")
    print("-" * 60)
    try:
        basic_data = await fdh.get_daily_basic_async(['600519.SH'], '2024-01-01', '2024-12-31')
        if basic_data is not None:
            print(f"✓ 获取基本面数据成功: {len(basic_data)} 条记录")
            print(f"  列: {', '.join(basic_data.columns[:10])}...")
        else:
            print("✗ 未找到数据")
    except Exception as e:
        print(f"✗ 查询失败: {e}")

    # 示例 4: 查询复权因子
    print("\n4. 查询复权因子")
    print("-" * 60)
    try:
        adj_factor = await fdh.get_adj_factor_async(['600519.SH'], '2020-01-01', '2024-12-31')
        if adj_factor is not None:
            print(f"✓ 获取复权因子成功: {len(adj_factor)} 条记录")
            print(f"  列: {', '.join(adj_factor.columns)}")
        else:
            print("✗ 未找到数据")
    except Exception as e:
        print(f"✗ 查询失败: {e}")

    # 示例 5: 查询股票基本信息
    print("\n5. 查询股票基本信息")
    print("-" * 60)
    try:
        basic_info = await fdh.get_basic_async(['600519.SH', '000858.SZ'])
        if basic_info is not None:
            print(f"✓ 获取基本信息成功: {len(basic_info)} 条记录")
            print(f"  列: {', '.join(basic_info.columns)}")
        else:
            print("✗ 未找到数据")
    except Exception as e:
        print(f"✗ 查询失败: {e}")

    # 示例 6: 检查数据新鲜度
    print("\n6. 检查数据新鲜度")
    print("-" * 60)
    try:
        freshness = await fdh.check_data_freshness(['600519.SH'], 'daily')
        print(f"✓ 数据新鲜度检查:")
        print(f"  可用提供商: {freshness.get('available_providers', [])}")
        print(f"  建议: {freshness.get('recommendation', 'N/A')}")
    except Exception as e:
        print(f"✗ 检查失败: {e}")

    print("\n" + "=" * 60)
    print("示例运行完成")
    print("=" * 60)

def main_sync():
    """同步主函数 - 适用于普通 Python 脚本"""
    try:
        import nest_asyncio
        nest_asyncio.apply()  # 允许在 Jupyter 中运行异步代码
    except ImportError:
        print("注意: 建议安装 nest_asyncio 以支持在 Jupyter 中运行异步代码")
        print("  pip install nest_asyncio")

    asyncio.run(main_async())

# 根据运行环境自动选择合适的运行方式
if __name__ == "__main__":
    # 检查是否在 Jupyter 环境中
    try:
        get_ipython()  # 如果能获取到 IPython 实例，说明在 Jupyter 中
        print("检测到 Jupyter 环境，使用异步模式...")
        asyncio.run(main_async())
    except NameError:
        # 普通 Python 脚本
        print("检测到普通 Python 环境，使用同步模式...")
        main_sync()
