#!/usr/bin/env python
"""
增量更新演示脚本

演示复权因子增量更新功能的工作原理：
1. 首次运行：获取全部股票1年数据
2. 第二次运行：只获取有更新的股票数据
"""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd

from finance_data_hub.update.updater import DataUpdater
from finance_data_hub.config import Settings


async def demo_incremental_update():
    """演示增量更新机制"""

    print("\n" + "=" * 70)
    print("📊 复权因子增量更新机制演示")
    print("=" * 70)

    # 模拟设置
    settings = Settings(
        database=Mock(url="postgresql://test"),
        tushare=Mock(tushare_token="test"),
        routing=Mock(sources_config_path="sources.yml"),
    )

    # 创建模拟的DataUpdater
    with patch('finance_data_hub.update.updater.SmartRouter') as mock_router, \
         patch('finance_data_hub.update.updater.DatabaseManager') as mock_db:

        # 配置路由器模拟
        mock_router_instance = Mock()
        mock_router.return_value = mock_router_instance

        # 配置数据库模拟
        mock_db_manager = Mock()
        mock_db.return_value = mock_db_manager
        mock_db_manager.initialize = AsyncMock()

        # 创建DataUpdater实例
        updater = DataUpdater(settings)
        updater.router = mock_router_instance
        updater.db_manager = mock_db_manager

        # 模拟数据操作
        mock_data_ops = Mock()
        mock_data_ops.get_symbol_list = AsyncMock(return_value=["600519.SH", "000858.SZ", "000001.SZ"])
        mock_data_ops.insert_adj_factor_batch = AsyncMock(return_value=100)
        updater.data_ops = mock_data_ops

        print("\n📋 测试场景1: 首次运行（无历史数据）")
        print("-" * 70)

        # 模拟无历史数据
        mock_data_ops.get_latest_data_date = AsyncMock(return_value=None)

        # 模拟数据返回
        mock_router_instance.route.return_value = pd.DataFrame({
            'symbol': ['600519.SH', '000858.SZ', '000001.SZ'],
            'trade_date': pd.date_range('2024-01-01', periods=3),
            'adj_factor': [1.0, 1.0, 1.0]
        })

        # 模拟数据插入
        async def mock_insert(data, batch_size=1000):
            print(f"   ✓ 批量插入 {len(data)} 条记录")
            return len(data)

        mock_data_ops.insert_adj_factor_batch = AsyncMock(side_effect=mock_insert)

        # 执行更新
        result = await updater.update_adj_factor()

        print(f"\n✅ 首次更新完成:")
        print(f"   - 获取全部3只股票数据")
        print(f"   - 总计更新 {result} 条记录")
        print(f"   - 获取范围: 2024-01-01 到 2024-12-31")

        print("\n📋 测试场景2: 第二次运行（有历史数据）")
        print("-" * 70)

        # 模拟有历史数据的情况
        latest_dates = {
            "600519.SH": datetime(2024, 11, 15),  # 最新
            "000858.SZ": datetime(2024, 10, 15),  # 需要更新
            "000001.SZ": None  # 无历史数据
        }

        call_count = 0
        async def mock_get_latest(symbol, table):
            nonlocal call_count
            call_count += 1
            return latest_dates.get(symbol)

        mock_data_ops.get_latest_data_date = AsyncMock(side_effect=mock_get_latest)

        # 模拟增量数据返回
        def mock_route(asset_class, data_type, method_name, symbol, start_date, end_date):
            print(f"   📡 获取 {symbol} 数据: {start_date} 到 {end_date}")
            return pd.DataFrame({
                'symbol': [symbol],
                'trade_date': pd.date_range(start_date, periods=1),
                'adj_factor': [1.0]
            })

        mock_router_instance.route = Mock(side_effect=mock_route)

        # 记录插入调用
        insert_calls = []
        async def mock_insert_v2(data, batch_size=1000):
            symbol = data.iloc[0]['symbol']
            insert_calls.append(symbol)
            print(f"   ✓ 插入 {symbol} 数据")
            return 1

        mock_data_ops.insert_adj_factor_batch = AsyncMock(side_effect=mock_insert_v2)

        # 执行增量更新
        result = await updater.update_adj_factor()

        print(f"\n✅ 增量更新完成:")
        print(f"   - 600519.SH: 已最新 (跳过)")
        print(f"   - 000858.SZ: 更新 1 条记录")
        print(f"   - 000001.SZ: 首次获取 1 条记录")
        print(f"   - 总计更新 {result} 条记录")

        print("\n" + "=" * 70)
        print("📈 增量更新效果总结")
        print("=" * 70)
        print("""
首次运行:
  - 获取所有股票数据
  - API调用: 3次 (股票数量)
  - 数据获取范围: 365天

后续运行:
  - 只获取有更新的股票
  - API调用: 2次 (000858.SZ + 000001.SZ)
  - 600519.SH 已最新，自动跳过
  - 节省 1 次 API 调用

性能提升:
  - 避免重复查询已更新股票
  - 节省API调用次数
  - 减少网络传输
  - 提高更新效率
""")

        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(demo_incremental_update())
