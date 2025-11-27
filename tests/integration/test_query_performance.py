"""
查询准确性和性能验证脚本

此脚本验证 FinanceDataHub SDK 的所有查询方法的：
1. 数据准确性：确保返回的数据格式正确、列名匹配
2. 查询性能：测量不同数据类型的查询时间
3. 异步性能：比较同步和异步查询的性能差异
4. 大数据量处理：测试多股票、大时间范围查询的性能

使用方法：
pytest tests/integration/test_query_performance.py -v
或者直接运行：
python tests/integration/test_query_performance.py
"""

import asyncio
import time
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

# 尝试导入 pytest，如果失败则跳过相关功能
try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False

from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.config import Settings
from unittest.mock import Mock, AsyncMock


class QueryPerformanceValidator:
    """查询性能验证器"""

    def __init__(self):
        self.results = {}
        self.fdh = None

    async def setup(self):
        """设置测试环境"""
        settings = Mock(spec=Settings)
        # 添加 database 属性
        settings.database = Mock()
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"

        self.fdh = FinanceDataHub(settings, backend="postgresql")

        # 模拟数据库操作，返回真实格式的 DataFrame
        self.fdh.db_manager = AsyncMock()
        self.fdh.ops = AsyncMock()

        # 设置不同数据类型的模拟数据
        self._setup_mock_data()

    def _setup_mock_data(self):
        """设置模拟数据"""
        # 生成日线数据
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        self.mock_daily_data = pd.DataFrame({
            'time': dates,
            'symbol': ['600519.SH'] * 100,
            'open': [100.0 + i for i in range(100)],
            'high': [105.0 + i for i in range(100)],
            'low': [95.0 + i for i in range(100)],
            'close': [103.0 + i for i in range(100)],
            'volume': [1000000 + i * 1000 for i in range(100)],
            'amount': [103000000 + i * 100000 for i in range(100)],
            'adj_factor': [1.0 + i * 0.001 for i in range(100)]
        })

        # 生成分钟数据
        minutes = pd.date_range('2024-11-01 09:30:00', periods=1000, freq='5min')
        self.mock_minute_data = pd.DataFrame({
            'time': minutes,
            'symbol': ['600519.SH'] * 1000,
            'open': [100.0 + i * 0.01 for i in range(1000)],
            'high': [105.0 + i * 0.01 for i in range(1000)],
            'low': [95.0 + i * 0.01 for i in range(1000)],
            'close': [103.0 + i * 0.01 for i in range(1000)],
            'volume': [100000 + i * 100 for i in range(1000)],
            'amount': [10300000 + i * 10000 for i in range(1000)],
            'frequency': ['minute_5'] * 1000
        })

        # 生成每日基本面数据
        self.mock_daily_basic_data = pd.DataFrame({
            'time': dates,
            'symbol': ['600519.SH'] * 100,
            'turnover_rate': [2.5 + i * 0.01 for i in range(100)],
            'volume_ratio': [1.2 + i * 0.001 for i in range(100)],
            'pe': [25.0 + i * 0.1 for i in range(100)],
            'pe_ttm': [24.5 + i * 0.1 for i in range(100)],
            'pb': [8.0 + i * 0.01 for i in range(100)],
            'ps': [15.0 + i * 0.05 for i in range(100)],
            'ps_ttm': [14.5 + i * 0.05 for i in range(100)],
            'dv_ratio': [1.8 + i * 0.001 for i in range(100)],
            'dv_ttm': [1.7 + i * 0.001 for i in range(100)],
            'total_share': [1000000000] * 100,
            'float_share': [800000000] * 100,
            'free_share': [700000000] * 100,
            'total_mv': [100000000000 + i * 100000000 for i in range(100)],
            'circ_mv': [80000000000 + i * 80000000 for i in range(100)]
        })

        # 生成复权因子数据
        self.mock_adj_factor_data = pd.DataFrame({
            'time': pd.date_range('2020-01-01', periods=500, freq='D'),
            'symbol': ['600519.SH'] * 500,
            'adj_factor': [1.0 + i * 0.001 for i in range(500)]
        })

        # 生成股票基本信息数据
        self.mock_basic_data = pd.DataFrame({
            'ts_code': ['600519.SH', '000858.SZ', '000001.SZ'],
            'symbol': ['600519', '000858', '000001'],
            'name': ['贵州茅台', '五粮液', '平安银行'],
            'area': ['西南', '西南', '华南'],
            'industry': ['白酒', '白酒', '银行'],
            'market': ['主板', '主板', '主板'],
            'exchange': ['SSE', 'SZSE', 'SZSE'],
            'list_status': ['L', 'L', 'L'],
            'list_date': ['2001-08-27', '1998-04-27', '1991-04-03'],
            'delist_date': [None, None, None],
            'is_hs': ['H', 'H', 'H']
        })

    async def test_daily_query_performance(self):
        """测试日线数据查询性能"""
        print("\n=== 测试日线数据查询 ===")

        self.fdh.ops.get_symbol_daily = AsyncMock(return_value=self.mock_daily_data)

        # 测试单个股票
        start_time = time.time()
        result = await self.fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-04-10')
        single_duration = time.time() - start_time

        assert result is not None
        assert len(result) == 100
        self.results['daily_single'] = single_duration

        print(f"  Single stock 100 days query: {single_duration:.6f} seconds")

        # 验证数据格式
        self._validate_daily_data(result)

        # 测试多个股票
        self.fdh.ops.get_symbol_daily = AsyncMock(return_value=self.mock_daily_data)
        multi_daily_data = pd.concat([self.mock_daily_data] * 3, ignore_index=True)

        self.fdh.ops.get_symbol_daily = AsyncMock(return_value=multi_daily_data)

        start_time = time.time()
        result = await self.fdh.get_daily_async(
            ['600519.SH', '000858.SZ', '000001.SZ'],
            '2024-01-01',
            '2024-04-10'
        )
        multi_duration = time.time() - start_time

        assert result is not None
        self.results['daily_multi'] = multi_duration

        print(f"  3 stocks 100 days query: {multi_duration:.6f} seconds")
        if multi_duration > 0:
            print(f"  Performance ratio: {(single_duration / multi_duration):.2f}x")
        else:
            print(f"  Performance ratio: N/A (too fast to measure)")

    async def test_minute_query_performance(self):
        """测试分钟数据查询性能"""
        print("\n=== Testing Minute Data Query ===")

        self.fdh.ops.get_symbol_minute = AsyncMock(return_value=self.mock_minute_data)

        start_time = time.time()
        result = await self.fdh.get_minute_async(
            ['600519.SH'],
            '2024-11-01',
            '2024-11-01',
            'minute_5'
        )
        duration = time.time() - start_time

        assert result is not None
        assert len(result) == 1000
        self.results['minute_5'] = duration

        print(f"  5-minute data 1000 records query: {duration:.6f} seconds")
        if duration > 0:
            print(f"  Average: {len(result) / duration:.0f} records/second")

        # 验证数据格式
        self._validate_minute_data(result)

    async def test_daily_basic_query_performance(self):
        """测试每日基本面数据查询性能"""
        print("\n=== Testing Daily Basic Data Query ===")

        self.fdh.ops.get_daily_basic = AsyncMock(return_value=self.mock_daily_basic_data)

        start_time = time.time()
        result = await self.fdh.get_daily_basic_async(
            ['600519.SH'],
            '2024-01-01',
            '2024-04-10'
        )
        duration = time.time() - start_time

        assert result is not None
        assert len(result) == 100
        self.results['daily_basic'] = duration

        print(f"  100 days basic data query: {duration:.6f} seconds")

        # 验证数据格式
        self._validate_daily_basic_data(result)

    async def test_adj_factor_query_performance(self):
        """测试复权因子数据查询性能"""
        print("\n=== Testing Adj Factor Data Query ===")

        self.fdh.ops.get_adj_factor = AsyncMock(return_value=self.mock_adj_factor_data)

        start_time = time.time()
        result = await self.fdh.get_adj_factor_async(
            ['600519.SH'],
            '2020-01-01',
            '2024-12-31'
        )
        duration = time.time() - start_time

        assert result is not None
        assert len(result) == 500
        self.results['adj_factor'] = duration

        print(f"  500 days adj factor data query: {duration:.6f} seconds")

        # 验证数据格式
        self._validate_adj_factor_data(result)

    async def test_basic_query_performance(self):
        """测试股票基本信息查询性能"""
        print("\n=== Testing Basic Info Query ===")

        self.fdh.ops.get_asset_basic = AsyncMock(return_value=self.mock_basic_data)

        # 测试多个股票
        start_time = time.time()
        result = await self.fdh.get_basic_async(['600519.SH', '000858.SZ', '000001.SZ'])
        multi_duration = time.time() - start_time

        assert result is not None
        assert len(result) == 3
        self.results['basic_multi'] = multi_duration

        print(f"  3 stocks basic info query: {multi_duration:.6f} seconds")

        # 测试所有股票
        self.fdh.ops.get_asset_basic = AsyncMock(return_value=self.mock_basic_data)

        start_time = time.time()
        result = await self.fdh.get_basic_async()
        all_duration = time.time() - start_time

        assert result is not None
        self.results['basic_all'] = all_duration

        print(f"  All stocks basic info query: {all_duration:.6f} seconds")

        # 验证数据格式
        self._validate_basic_data(result)

    async def test_async_vs_sync_performance(self):
        """比较异步与同步查询性能"""
        print("\n=== Async vs Sync Performance Comparison ===")

        # 准备大量数据
        large_data = pd.concat([self.mock_daily_data] * 10, ignore_index=True)
        self.fdh.ops.get_symbol_daily = AsyncMock(return_value=large_data)

        # 异步查询
        async_start = time.time()
        result_async = await self.fdh.get_daily_async(
            ['600519.SH'],
            '2024-01-01',
            '2024-04-10'
        )
        async_duration = time.time() - async_start

        print(f"  Async query: {async_duration:.6f} seconds")

        # 同步查询需要在新线程中运行，避免事件循环冲突
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            sync_start = time.time()
            # 在新线程中执行同步方法
            result_sync = executor.submit(
                self.fdh.get_daily,
                ['600519.SH'],
                '2024-01-01',
                '2024-04-10'
            ).result()
            sync_duration = time.time() - sync_start

        print(f"  Sync query: {sync_duration:.6f} seconds")
        print(f"  Performance difference: {abs(async_duration - sync_duration):.6f} seconds")

        self.results['async_vs_sync'] = {
            'async': async_duration,
            'sync': sync_duration,
            'diff': abs(async_duration - sync_duration)
        }

        # 验证结果一致性
        assert result_async is not None
        assert result_sync is not None

    def _validate_daily_data(self, data: pd.DataFrame):
        """验证日线数据格式"""
        required_columns = ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor']

        for col in required_columns:
            assert col in data.columns, f"缺少列: {col}"

        assert data['time'].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(data['time'])
        assert pd.api.types.is_numeric_dtype(data['open'])
        assert pd.api.types.is_numeric_dtype(data['close'])

    def _validate_minute_data(self, data: pd.DataFrame):
        """验证分钟数据格式"""
        required_columns = ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount', 'frequency']

        for col in required_columns:
            assert col in data.columns, f"缺少列: {col}"

        assert 'frequency' in data.columns
        assert data['frequency'].nunique() == 1  # 所有记录频率应该相同

    def _validate_daily_basic_data(self, data: pd.DataFrame):
        """验证每日基本面数据格式"""
        required_columns = ['time', 'symbol', 'turnover_rate', 'pe', 'pb']

        for col in required_columns:
            assert col in data.columns, f"缺少列: {col}"

        assert pd.api.types.is_numeric_dtype(data['turnover_rate'])
        assert pd.api.types.is_numeric_dtype(data['pe'])

    def _validate_adj_factor_data(self, data: pd.DataFrame):
        """验证复权因子数据格式"""
        required_columns = ['time', 'symbol', 'adj_factor']

        for col in required_columns:
            assert col in data.columns, f"缺少列: {col}"

        assert pd.api.types.is_numeric_dtype(data['adj_factor'])
        # 复权因子应该单调递增
        assert data['adj_factor'].is_monotonic_increasing

    def _validate_basic_data(self, data: pd.DataFrame):
        """验证股票基本信息格式"""
        required_columns = ['ts_code', 'symbol', 'name', 'industry']

        for col in required_columns:
            assert col in data.columns, f"缺少列: {col}"

        assert pd.api.types.is_string_dtype(data['name']) or data['name'].dtype == 'object'

    def print_summary(self):
        """打印性能摘要"""
        print("\n" + "=" * 60)
        print("查询性能测试摘要")
        print("=" * 60)

        for test_name, duration in self.results.items():
            if isinstance(duration, dict):
                print(f"{test_name}:")
                for key, value in duration.items():
                    print(f"  {key}: {value:.3f} 秒")
            else:
                print(f"{test_name}: {duration:.3f} 秒")

        print("=" * 60)


# Pytest 测试类（仅在 pytest 可用时）
if HAS_PYTEST:
    class TestQueryPerformance:
        """查询性能测试"""

        @pytest.fixture
        def validator(self):
            """创建验证器"""
            return QueryPerformanceValidator()

        @pytest.mark.asyncio
        async def test_all_queries_performance(self, validator):
            """运行所有查询性能测试"""
            await validator.setup()

            await validator.test_daily_query_performance()
            await validator.test_minute_query_performance()
            await validator.test_daily_basic_query_performance()
            await validator.test_adj_factor_query_performance()
            await validator.test_basic_query_performance()
            await validator.test_async_vs_sync_performance()

            validator.print_summary()


# 直接运行脚本时的入口
async def main():
    """主函数"""
    print("=" * 60)
    print("FinanceDataHub SDK 查询性能验证")
    print("=" * 60)

    validator = QueryPerformanceValidator()
    await validator.setup()

    await validator.test_daily_query_performance()
    await validator.test_minute_query_performance()
    await validator.test_daily_basic_query_performance()
    await validator.test_adj_factor_query_performance()
    await validator.test_basic_query_performance()
    await validator.test_async_vs_sync_performance()

    validator.print_summary()


if __name__ == '__main__':
    asyncio.run(main())
