"""
单元测试：高周期数据聚合查询

测试连续聚合的查询方法，包括周线、月线数据和基础指标查询。
"""

import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from finance_data_hub.database.operations import DataOperations
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.config import Settings


class TestDataOperationsHigherPeriod:
    """测试 DataOperations 的高周期数据查询方法"""

    @pytest.fixture
    def mock_db_manager(self):
        """创建模拟数据库管理器"""
        manager = MagicMock(spec=DatabaseManager)
        manager._engine = AsyncMock()
        return manager

    @pytest.fixture
    def data_ops(self, mock_db_manager):
        """创建 DataOperations 实例"""
        return DataOperations(mock_db_manager)

    @pytest.mark.asyncio
    async def test_get_weekly_data_success(self, data_ops, mock_db_manager):
        """测试成功获取周线数据"""
        # 准备模拟数据
        expected_data = [
            {
                'time': datetime(2024, 1, 1),
                'symbol': '600519.SH',
                'open': 1800.0,
                'high': 1850.0,
                'low': 1780.0,
                'close': 1820.0,
                'volume': 1000000,
                'amount': 1800000000.0,
                'adj_factor': 1.0
            },
            {
                'time': datetime(2024, 1, 8),
                'symbol': '600519.SH',
                'open': 1820.0,
                'high': 1870.0,
                'low': 1800.0,
                'close': 1840.0,
                'volume': 1200000,
                'amount': 2200000000.0,
                'adj_factor': 1.0
            }
        ]

        # 模拟数据库响应
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(**row) for row in expected_data]
        mock_db_manager._engine.begin.return_value.__aenter__.return_value.execute.return_value = mock_result

        # 执行测试
        result = await data_ops.get_weekly_data(['600519.SH'], '2024-01-01', '2024-12-31')

        # 验证结果
        assert result is not None
        assert len(result) == 2
        assert list(result.columns) == ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor']
        assert result.iloc[0]['symbol'] == '600519.SH'

    @pytest.mark.asyncio
    async def test_get_weekly_data_empty(self, data_ops, mock_db_manager):
        """测试获取空周线数据"""
        # 模拟空结果
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = []
        mock_db_manager._engine.begin.return_value.__aenter__.return_value.execute.return_value = mock_result

        # 执行测试
        result = await data_ops.get_weekly_data(['600519.SH'], '2024-01-01', '2024-12-31')

        # 验证结果
        assert result is None

    @pytest.mark.asyncio
    async def test_get_monthly_data_success(self, data_ops, mock_db_manager):
        """测试成功获取月线数据"""
        # 准备模拟数据
        expected_data = [
            {
                'time': datetime(2024, 1, 31),
                'symbol': '600519.SH',
                'open': 1800.0,
                'high': 1900.0,
                'low': 1750.0,
                'close': 1850.0,
                'volume': 5000000,
                'amount': 9000000000.0,
                'adj_factor': 1.0
            }
        ]

        # 模拟数据库响应
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(**row) for row in expected_data]
        mock_db_manager._engine.begin.return_value.__aenter__.return_value.execute.return_value = mock_result

        # 执行测试
        result = await data_ops.get_monthly_data(['600519.SH'], '2024-01-01', '2024-12-31')

        # 验证结果
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]['close'] == 1850.0

    @pytest.mark.asyncio
    async def test_get_daily_basic_weekly_success(self, data_ops, mock_db_manager):
        """测试成功获取周线基础指标数据"""
        # 准备模拟数据
        expected_data = [
            {
                'time': datetime(2024, 1, 1),
                'symbol': '600519.SH',
                'avg_turnover_rate': 2.5,
                'avg_volume_ratio': 1.2,
                'avg_pe': 25.0,
                'avg_pe_ttm': 24.5,
                'avg_pb': 8.0,
                'avg_ps': 15.0,
                'avg_ps_ttm': 14.5,
                'avg_dv_ratio': 1.5,
                'avg_dv_ttm': 1.4,
                'total_share': 250000.0,
                'float_share': 240000.0,
                'free_share': 230000.0,
                'total_mv': 4600000.0,
                'circ_mv': 4400000.0
            }
        ]

        # 模拟数据库响应
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(**row) for row in expected_data]
        mock_db_manager._engine.begin.return_value.__aenter__.return_value.execute.return_value = mock_result

        # 执行测试
        result = await data_ops.get_daily_basic_weekly(['600519.SH'], '2024-01-01', '2024-12-31')

        # 验证结果
        assert result is not None
        assert len(result) == 1
        assert 'avg_pe' in result.columns
        assert result.iloc[0]['avg_turnover_rate'] == 2.5

    @pytest.mark.asyncio
    async def test_get_daily_basic_monthly_success(self, data_ops, mock_db_manager):
        """测试成功获取月线基础指标数据"""
        # 准备模拟数据
        expected_data = [
            {
                'time': datetime(2024, 1, 31),
                'symbol': '600519.SH',
                'avg_turnover_rate': 2.8,
                'avg_volume_ratio': 1.3,
                'avg_pe': 26.0,
                'avg_pe_ttm': 25.0,
                'avg_pb': 8.2,
                'avg_ps': 15.5,
                'avg_ps_ttm': 15.0,
                'avg_dv_ratio': 1.6,
                'avg_dv_ttm': 1.5,
                'total_share': 250000.0,
                'float_share': 240000.0,
                'free_share': 230000.0,
                'total_mv': 4800000.0,
                'circ_mv': 4600000.0
            }
        ]

        # 模拟数据库响应
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(**row) for row in expected_data]
        mock_db_manager._engine.begin.return_value.__aenter__.return_value.execute.return_value = mock_result

        # 执行测试
        result = await data_ops.get_daily_basic_monthly(['600519.SH'], '2024-01-01', '2024-12-31')

        # 验证结果
        assert result is not None
        assert len(result) == 1
        assert 'avg_pb' in result.columns
        assert result.iloc[0]['avg_pb'] == 8.2

    @pytest.mark.asyncio
    async def test_get_weekly_data_multiple_symbols(self, data_ops, mock_db_manager):
        """测试获取多只股票的周线数据"""
        # 准备多只股票数据
        expected_data = [
            {
                'time': datetime(2024, 1, 1),
                'symbol': '600519.SH',
                'open': 1800.0,
                'high': 1850.0,
                'low': 1780.0,
                'close': 1820.0,
                'volume': 1000000,
                'amount': 1800000000.0,
                'adj_factor': 1.0
            },
            {
                'time': datetime(2024, 1, 1),
                'symbol': '000858.SZ',
                'open': 200.0,
                'high': 210.0,
                'low': 195.0,
                'close': 205.0,
                'volume': 2000000,
                'amount': 400000000.0,
                'adj_factor': 1.0
            }
        ]

        # 模拟数据库响应
        mock_result = AsyncMock()
        mock_result.fetchall.return_value = [MagicMock(**row) for row in expected_data]
        mock_db_manager._engine.begin.return_value.__aenter__.return_value.execute.return_value = mock_result

        # 执行测试
        result = await data_ops.get_weekly_data(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')

        # 验证结果
        assert result is not None
        assert len(result) == 2
        symbols = set(result['symbol'].tolist())
        assert symbols == {'600519.SH', '000858.SZ'}


class TestFinanceDataHubSDK:
    """测试 FinanceDataHub SDK 方法"""

    @pytest.fixture
    def mock_settings(self):
        """创建模拟设置"""
        settings = MagicMock(spec=Settings)
        settings.database.url = "postgresql://user:pass@localhost:5432/test_db"
        return settings

    @pytest.fixture
    def mock_db_manager(self):
        """创建模拟数据库管理器"""
        manager = MagicMock(spec=DatabaseManager)
        manager._engine = AsyncMock()
        return manager

    def test_get_weekly_sync(self, mock_settings, mock_db_manager):
        """测试同步获取周线数据"""
        # 模拟异步方法
        expected_df = pd.DataFrame({
            'time': [datetime(2024, 1, 1)],
            'symbol': ['600519.SH'],
            'open': [1800.0],
            'high': [1850.0],
            'low': [1780.0],
            'close': [1820.0],
            'volume': [1000000],
            'amount': [1800000000.0],
            'adj_factor': [1.0]
        })

        with patch('finance_data_hub.sdk.DatabaseManager', return_value=mock_db_manager):
            with patch.object(DataOperations, 'get_weekly_data', return_value=expected_df):
                sdk = FinanceDataHub(mock_settings)
                result = sdk.get_weekly(['600519.SH'], '2024-01-01', '2024-12-31')

                assert result is not None
                assert len(result) == 1
                assert result.iloc[0]['symbol'] == '600519.SH'

    @pytest.mark.asyncio
    async def test_get_weekly_async(self, mock_settings, mock_db_manager):
        """测试异步获取周线数据"""
        expected_df = pd.DataFrame({
            'time': [datetime(2024, 1, 1)],
            'symbol': ['600519.SH'],
            'open': [1800.0],
            'high': [1850.0],
            'low': [1780.0],
            'close': [1820.0],
            'volume': [1000000],
            'amount': [1800000000.0],
            'adj_factor': [1.0]
        })

        with patch('finance_data_hub.sdk.DatabaseManager', return_value=mock_db_manager):
            mock_db_manager.initialize = AsyncMock()
            mock_db_manager.close = AsyncMock()

            sdk = FinanceDataHub(mock_settings)

            # 模拟 DataOperations
            sdk.ops.get_weekly_data = AsyncMock(return_value=expected_df)

            result = await sdk.get_weekly_async(['600519.SH'], '2024-01-01', '2024-12-31')

            assert result is not None
            assert len(result) == 1

    def test_get_monthly_sync(self, mock_settings, mock_db_manager):
        """测试同步获取月线数据"""
        expected_df = pd.DataFrame({
            'time': [datetime(2024, 1, 31)],
            'symbol': ['600519.SH'],
            'open': [1800.0],
            'high': [1900.0],
            'low': [1750.0],
            'close': [1850.0],
            'volume': [5000000],
            'amount': [9000000000.0],
            'adj_factor': [1.0]
        })

        with patch('finance_data_hub.sdk.DatabaseManager', return_value=mock_db_manager):
            with patch.object(DataOperations, 'get_monthly_data', return_value=expected_df):
                sdk = FinanceDataHub(mock_settings)
                result = sdk.get_monthly(['600519.SH'], '2024-01-01', '2024-12-31')

                assert result is not None
                assert len(result) == 1
                assert result.iloc[0]['close'] == 1850.0

    def test_get_daily_basic_weekly_sync(self, mock_settings, mock_db_manager):
        """测试同步获取周线基础指标"""
        expected_df = pd.DataFrame({
            'time': [datetime(2024, 1, 1)],
            'symbol': ['600519.SH'],
            'avg_turnover_rate': [2.5],
            'avg_volume_ratio': [1.2],
            'avg_pe': [25.0],
            'avg_pe_ttm': [24.5],
            'avg_pb': [8.0],
            'avg_ps': [15.0],
            'avg_ps_ttm': [14.5],
            'avg_dv_ratio': [1.5],
            'avg_dv_ttm': [1.4],
            'total_share': [250000.0],
            'float_share': [240000.0],
            'free_share': [230000.0],
            'total_mv': [4600000.0],
            'circ_mv': [4400000.0]
        })

        with patch('finance_data_hub.sdk.DatabaseManager', return_value=mock_db_manager):
            with patch.object(DataOperations, 'get_daily_basic_weekly', return_value=expected_df):
                sdk = FinanceDataHub(mock_settings)
                result = sdk.get_daily_basic_weekly(['600519.SH'], '2024-01-01', '2024-12-31')

                assert result is not None
                assert 'avg_pe' in result.columns
                assert result.iloc[0]['avg_turnover_rate'] == 2.5

    def test_get_daily_basic_monthly_sync(self, mock_settings, mock_db_manager):
        """测试同步获取月线基础指标"""
        expected_df = pd.DataFrame({
            'time': [datetime(2024, 1, 31)],
            'symbol': ['600519.SH'],
            'avg_turnover_rate': [2.8],
            'avg_volume_ratio': [1.3],
            'avg_pe': [26.0],
            'avg_pe_ttm': [25.0],
            'avg_pb': [8.2],
            'avg_ps': [15.5],
            'avg_ps_ttm': [15.0],
            'avg_dv_ratio': [1.6],
            'avg_dv_ttm': [1.5],
            'total_share': [250000.0],
            'float_share': [240000.0],
            'free_share': [230000.0],
            'total_mv': [4800000.0],
            'circ_mv': [4600000.0]
        })

        with patch('finance_data_hub.sdk.DatabaseManager', return_value=mock_db_manager):
            with patch.object(DataOperations, 'get_daily_basic_monthly', return_value=expected_df):
                sdk = FinanceDataHub(mock_settings)
                result = sdk.get_daily_basic_monthly(['600519.SH'], '2024-01-01', '2024-12-31')

                assert result is not None
                assert 'avg_pb' in result.columns
                assert result.iloc[0]['avg_pb'] == 8.2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
