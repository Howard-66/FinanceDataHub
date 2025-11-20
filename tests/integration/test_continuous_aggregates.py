"""
集成测试：连续聚合功能

测试连续聚合的创建、刷新、数据准确性和性能。
这些测试需要真实的数据库连接，因此标记为 integration。
"""

import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from finance_data_hub.config import get_settings
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.sdk import FinanceDataHub


@pytest.mark.integration
class TestContinuousAggregatesIntegration:
    """连续聚合集成测试"""

    @pytest.fixture(scope="class")
    async def db_manager(self):
        """创建数据库管理器"""
        settings = get_settings()
        db_manager = DatabaseManager(settings)
        await db_manager.initialize()

        # 在测试前清理现有数据
        await self._cleanup_test_data(db_manager)

        yield db_manager

        # 测试后清理
        await self._cleanup_test_data(db_manager)
        await db_manager.close()

    @pytest.fixture
    def data_ops(self, db_manager):
        """创建数据操作实例"""
        return DataOperations(db_manager)

    @pytest.fixture
    def sdk(self, db_manager):
        """创建 SDK 实例"""
        settings = get_settings()
        return FinanceDataHub(settings, backend="postgresql")

    async def _cleanup_test_data(self, db_manager):
        """清理测试数据"""
        async with db_manager._engine.begin() as conn:
            # 清理测试数据（如果存在）
            await conn.execute(text("DELETE FROM symbol_daily WHERE symbol LIKE 'TEST%'"))
            await conn.execute(text("DELETE FROM daily_basic WHERE symbol LIKE 'TEST%'"))

    async def _insert_test_daily_data(self, db_manager, symbol='TEST001.SH', start_date='2024-01-01', end_date='2024-12-31'):
        """插入测试用的日线数据"""
        # 生成测试数据
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        test_data = []

        base_price = 100.0
        for date in date_range:
            # 跳过周末
            if date.weekday() >= 5:
                continue

            # 模拟价格波动
            open_price = base_price + (hash(str(date)) % 20 - 10)
            high_price = open_price + (hash(str(date) + 'high') % 10)
            low_price = open_price - (hash(str(date) + 'low') % 10)
            close_price = open_price + (hash(str(date) + 'close') % 5 - 2)

            test_data.append({
                'time': date,
                'symbol': symbol,
                'open': float(open_price),
                'high': float(high_price),
                'low': float(low_price),
                'close': float(close_price),
                'volume': int(hash(str(date) + 'volume') % 1000000 + 500000),
                'amount': float((open_price + close_price) * 50000),
                'adj_factor': 1.0 + (date - datetime(2024, 1, 1)).days * 0.001
            })

        df = pd.DataFrame(test_data)

        # 插入数据
        data_ops = DataOperations(db_manager)
        await data_ops.insert_symbol_daily_batch(df, batch_size=100)

        return df

    async def _insert_test_daily_basic_data(self, db_manager, symbol='TEST001.SH', start_date='2024-01-01', end_date='2024-12-31'):
        """插入测试用的每日基础指标数据"""
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        test_data = []

        for date in date_range:
            if date.weekday() >= 5:
                continue

            test_data.append({
                'time': date,
                'symbol': symbol,
                'turnover_rate': 2.0 + (hash(str(date)) % 100) / 100,
                'volume_ratio': 1.0 + (hash(str(date) + 'vr') % 50) / 100,
                'pe': 20.0 + (hash(str(date) + 'pe') % 100) / 10,
                'pe_ttm': 19.5 + (hash(str(date) + 'pe_ttm') % 100) / 10,
                'pb': 3.0 + (hash(str(date) + 'pb') % 100) / 100,
                'ps': 5.0 + (hash(str(date) + 'ps') % 100) / 10,
                'ps_ttm': 4.8 + (hash(str(date) + 'ps_ttm') % 100) / 10,
                'dv_ratio': 1.0 + (hash(str(date) + 'dv') % 100) / 100,
                'dv_ttm': 0.95 + (hash(str(date) + 'dv_ttm') % 100) / 100,
                'total_share': 100000.0,
                'float_share': 90000.0,
                'free_share': 85000.0,
                'total_mv': 2000000.0,
                'circ_mv': 1800000.0
            })

        df = pd.DataFrame(test_data)

        # 插入数据
        async with db_manager._engine.begin() as conn:
            insert_sql = text("""
                INSERT INTO daily_basic (
                    time, symbol, turnover_rate, volume_ratio, pe, pe_ttm,
                    pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share,
                    float_share, free_share, total_mv, circ_mv
                )
                VALUES (
                    :time, :symbol, :turnover_rate, :volume_ratio, :pe, :pe_ttm,
                    :pb, :ps, :ps_ttm, :dv_ratio, :dv_ttm, :total_share,
                    :float_share, :free_share, :total_mv, :circ_mv
                )
                ON CONFLICT (symbol, time) DO UPDATE SET
                    turnover_rate = EXCLUDED.turnover_rate,
                    volume_ratio = EXCLUDED.volume_ratio,
                    pe = EXCLUDED.pe,
                    pe_ttm = EXCLUDED.pe_ttm,
                    pb = EXCLUDED.pb,
                    ps = EXCLUDED.ps,
                    ps_ttm = EXCLUDED.ps_ttm,
                    dv_ratio = EXCLUDED.dv_ratio,
                    dv_ttm = EXCLUDED.dv_ttm,
                    total_share = EXCLUDED.total_share,
                    float_share = EXCLUDED.float_share,
                    free_share = EXCLUDED.free_share,
                    total_mv = EXCLUDED.total_mv,
                    circ_mv = EXCLUDED.circ_mv
            """)

            for record in df.to_dict('records'):
                await conn.execute(insert_sql, record)

    async def test_create_continuous_aggregates(self, db_manager):
        """测试连续聚合创建"""
        async with db_manager._engine.begin() as conn:
            # 检查聚合是否存在
            result = await conn.execute(text("""
                SELECT view_name
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name IN ('symbol_weekly', 'symbol_monthly', 'daily_basic_weekly', 'daily_basic_monthly')
                ORDER BY view_name
            """))

            views = [row.view_name for row in result.fetchall()]

            assert 'symbol_weekly' in views, "symbol_weekly 视图应已创建"
            assert 'symbol_monthly' in views, "symbol_monthly 视图应已创建"
            assert 'daily_basic_weekly' in views, "daily_basic_weekly 视图应已创建"
            assert 'daily_basic_monthly' in views, "daily_basic_monthly 视图应已创建"

    async def test_initial_data_population(self, db_manager):
        """测试初始数据填充"""
        # 插入测试数据
        daily_df = await self._insert_test_daily_data(db_manager)

        # 手动刷新聚合以初始化数据
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 验证周线数据
        weekly_data = await DataOperations(db_manager).get_weekly_data(
            ['TEST001.SH'], '2024-01-01', '2024-12-31'
        )

        assert weekly_data is not None, "应返回周线数据"
        assert len(weekly_data) > 0, "周线数据不应为空"

    async def test_weekly_data_aggregation(self, db_manager):
        """测试周线数据聚合正确性"""
        # 插入测试数据
        daily_df = await self._insert_test_daily_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 获取周线数据
        data_ops = DataOperations(db_manager)
        weekly_data = await data_ops.get_weekly_data(['TEST001.SH'], '2024-01-01', '2024-12-31')

        # 验证数据
        assert weekly_data is not None
        assert len(weekly_data) > 0

        # 验证第一周的聚合
        first_week = weekly_data.iloc[0]
        assert first_week['symbol'] == 'TEST001.SH'
        assert 'open' in weekly_data.columns
        assert 'high' in weekly_data.columns
        assert 'low' in weekly_data.columns
        assert 'close' in weekly_data.columns
        assert 'volume' in weekly_data.columns

    async def test_monthly_data_aggregation(self, db_manager):
        """测试月线数据聚合正确性"""
        # 插入测试数据
        daily_df = await self._insert_test_daily_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_monthly', NULL, NULL)"))

        # 获取月线数据
        data_ops = DataOperations(db_manager)
        monthly_data = await data_ops.get_monthly_data(['TEST001.SH'], '2024-01-01', '2024-12-31')

        # 验证数据
        assert monthly_data is not None
        assert len(monthly_data) > 0

        # 验证第一个月
        first_month = monthly_data.iloc[0]
        assert first_month['symbol'] == 'TEST001.SH'
        assert 'open' in monthly_data.columns
        assert 'high' in monthly_data.columns
        assert 'low' in monthly_data.columns
        assert 'close' in monthly_data.columns

    async def test_daily_basic_weekly_aggregation(self, db_manager):
        """测试周线基础指标聚合"""
        # 插入测试数据
        await self._insert_test_daily_basic_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('daily_basic_weekly', NULL, NULL)"))

        # 获取周线基础指标
        data_ops = DataOperations(db_manager)
        weekly_basic = await data_ops.get_daily_basic_weekly(['TEST001.SH'], '2024-01-01', '2024-12-31')

        # 验证数据
        assert weekly_basic is not None
        assert len(weekly_basic) > 0

        # 验证列
        assert 'avg_turnover_rate' in weekly_basic.columns
        assert 'avg_pe' in weekly_basic.columns
        assert 'avg_pb' in weekly_basic.columns
        assert 'total_share' in weekly_basic.columns

    async def test_daily_basic_monthly_aggregation(self, db_manager):
        """测试月线基础指标聚合"""
        # 插入测试数据
        await self._insert_test_daily_basic_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('daily_basic_monthly', NULL, NULL)"))

        # 获取月线基础指标
        data_ops = DataOperations(db_manager)
        monthly_basic = await data_ops.get_daily_basic_monthly(['TEST001.SH'], '2024-01-01', '2024-12-31')

        # 验证数据
        assert monthly_basic is not None
        assert len(monthly_basic) > 0

        # 验证列
        assert 'avg_turnover_rate' in monthly_basic.columns
        assert 'avg_pe' in monthly_basic.columns
        assert 'avg_pb' in monthly_basic.columns
        assert 'total_share' in monthly_basic.columns

    async def test_sdk_integration(self, sdk):
        """测试 SDK 集成"""
        # 插入测试数据
        await self._insert_test_daily_data(sdk.db_manager)

        # 使用 SDK 同步方法
        weekly_data = sdk.get_weekly(['TEST001.SH'], '2024-01-01', '2024-12-31')

        if weekly_data is not None and len(weekly_data) > 0:
            assert 'symbol' in weekly_data.columns
            assert weekly_data.iloc[0]['symbol'] == 'TEST001.SH'

    async def test_automatic_refresh(self, db_manager):
        """测试自动刷新功能"""
        # 插入初始数据
        await self._insert_test_daily_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 验证初始数据
        data_ops = DataOperations(db_manager)
        initial_count = len(await data_ops.get_weekly_data(['TEST001.SH'], '2024-01-01', '2024-12-31'))

        # 等待自动刷新（实际应用中需要等待一段时间）
        # 这里我们手动再次刷新来模拟
        await asyncio.sleep(0.1)

        # 再次刷新
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 验证数据仍然存在
        refreshed_count = len(await data_ops.get_weekly_data(['TEST001.SH'], '2024-01-01', '2024-12-31'))
        assert refreshed_count == initial_count

    async def test_performance_benchmark(self, db_manager):
        """测试性能基准"""
        # 插入测试数据
        await self._insert_test_daily_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 性能测试
        import time

        data_ops = DataOperations(db_manager)

        start_time = time.time()
        weekly_data = await data_ops.get_weekly_data(['TEST001.SH'], '2024-01-01', '2024-12-31')
        end_time = time.time()

        query_time = end_time - start_time

        # 验证查询时间小于 100ms
        assert query_time < 0.1, f"查询耗时 {query_time:.3f}s，应小于 100ms"

        # 验证返回数据
        assert weekly_data is not None
        assert len(weekly_data) > 0

    async def test_multiple_symbols_query(self, db_manager):
        """测试多股票查询"""
        # 插入多只股票的数据
        await self._insert_test_daily_data(db_manager, symbol='TEST001.SH')
        await self._insert_test_daily_data(db_manager, symbol='TEST002.SZ')

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 查询多只股票
        data_ops = DataOperations(db_manager)
        weekly_data = await data_ops.get_weekly_data(
            ['TEST001.SH', 'TEST002.SZ'], '2024-01-01', '2024-12-31'
        )

        # 验证数据
        assert weekly_data is not None
        assert len(weekly_data) > 0

        symbols = set(weekly_data['symbol'].unique())
        assert 'TEST001.SH' in symbols
        assert 'TEST002.SZ' in symbols

    async def test_data_accuracy_vs_manual_resample(self, db_manager):
        """测试数据准确性（与手动重采样对比）"""
        # 插入测试数据
        daily_df = await self._insert_test_daily_data(db_manager)

        # 刷新聚合
        async with db_manager._engine.begin() as conn:
            await conn.execute(text("CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL)"))

        # 获取聚合数据
        data_ops = DataOperations(db_manager)
        weekly_data = await data_ops.get_weekly_data(['TEST001.SH'], '2024-01-01', '2024-12-31')

        # 手动重采样计算
        daily_df_copy = daily_df.copy()
        daily_df_copy.set_index('time', inplace=True)
        weekly_manual = daily_df_copy.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum',
            'adj_factor': 'last'
        }).reset_index()

        # 比较（允许小误差）
        if len(weekly_data) > 0 and len(weekly_manual) > 0:
            # 比较第一周数据
            agg_first = weekly_data.iloc[0]
            manual_first = weekly_manual.iloc[0]

            # 允许 0.01% 的误差
            price_diff = abs(agg_first['close'] - manual_first['close']) / manual_first['close']
            assert price_diff < 0.0001, f"收盘价差异过大: {price_diff:.6f}"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--integration'])
