"""
SDK 查询方法集成测试

测试 FinanceDataHub SDK 的所有查询接口，包括：
- 日线数据查询
- 分钟数据查询
- 每日基本面查询
- 复权因子查询
- 股票基本信息查询

以及高周期数据聚合查询：
- 周线数据查询
- 月线数据查询
- 周线基本面数据查询
- 月线基本面数据查询
- 周线复权因子查询
- 月线复权因子查询
"""

import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
import pandas as pd
from datetime import datetime, date

from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.config import Settings


class TestSDKInitialization:
    """测试 SDK 初始化"""

    @pytest.fixture
    def mock_settings(self):
        """模拟设置对象"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        return settings

    def test_init_with_default_params(self, mock_settings):
        """测试默认参数初始化"""
        fdh = FinanceDataHub(mock_settings)
        assert fdh.settings == mock_settings
        assert fdh.backend == "auto"
        assert fdh.router is None  # 没有配置文件时应为 None

    def test_init_with_postgresql_backend(self, mock_settings):
        """测试指定 PostgreSQL 后端初始化"""
        fdh = FinanceDataHub(mock_settings, backend="postgresql")
        assert fdh.backend == "postgresql"

    def test_init_with_router_config(self, mock_settings):
        """测试带路由配置的初始化"""
        with patch('finance_data_hub.router.smart_router.SmartRouter') as mock_router:
            mock_router_instance = Mock()
            mock_router.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings, router_config_path="sources.yml")

            # 验证 SmartRouter 被调用
            mock_router.assert_called_once_with("sources.yml")
            assert fdh.router == mock_router_instance

    def test_init_with_missing_router_config(self, mock_settings):
        """测试路由配置文件不存在时的初始化"""
        with patch(
            'finance_data_hub.router.smart_router.SmartRouter',
            side_effect=FileNotFoundError
        ):
            fdh = FinanceDataHub(mock_settings, router_config_path="nonexistent.yml")

            # 应该优雅降级，router 为 None
            assert fdh.router is None


class TestSDKAsyncMethods:
    """测试 SDK 异步方法"""

    @pytest.fixture
    def fdh(self):
        """创建 SDK 实例用于测试"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        fdh = FinanceDataHub(settings, backend="postgresql")
        # 模拟数据库连接
        fdh.db_manager = AsyncMock()
        fdh.ops = AsyncMock()
        return fdh

    @pytest.mark.asyncio
    async def test_initialize(self, fdh):
        """测试数据库初始化"""
        await fdh.initialize()
        fdh.db_manager.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_data_freshness_no_router(self, fdh):
        """测试无路由器时的数据新鲜度检查"""
        fdh.router = None

        result = await fdh.check_data_freshness(['600519.SH'], 'daily')

        assert result['is_stale'] is False
        assert result['latest_date'] is None
        assert result['recommendation'] == "SmartRouter not available"
        assert result['available_providers'] == []

    @pytest.mark.asyncio
    async def test_check_data_freshness_with_router(self, fdh):
        """测试带路由器的数据新鲜度检查"""
        # 模拟路由器
        fdh.router = Mock()
        fdh.router.get_providers_for_route = Mock(return_value=['tushare', 'xtquant'])

        result = await fdh.check_data_freshness(['600519.SH'], 'daily')

        assert result['available_providers'] == ['tushare', 'xtquant']
        assert 'Available providers' in result['recommendation']

    @pytest.mark.asyncio
    async def test_get_daily_async(self, fdh):
        """测试获取日线数据"""
        # 模拟数据返回
        mock_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=5),
            'symbol': ['600519.SH'] * 5,
            'open': [100.0] * 5,
            'high': [105.0] * 5,
            'low': [95.0] * 5,
            'close': [103.0] * 5,
            'volume': [1000000] * 5,
            'amount': [103000000] * 5,
            'adj_factor': [1.0] * 5
        })

        fdh.ops.get_symbol_daily = AsyncMock(return_value=mock_data)

        result = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-01-05')

        assert result is not None
        assert len(result) == 5
        assert 'time' in result.columns
        assert 'symbol' in result.columns
        assert 'open' in result.columns
        assert 'close' in result.columns

        # 验证调用参数
        fdh.ops.get_symbol_daily.assert_called_once_with(
            ['600519.SH'], '2024-01-01', '2024-01-05'
        )

    @pytest.mark.asyncio
    async def test_get_minute_async(self, fdh):
        """测试获取分钟数据"""
        mock_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01 09:30:00', periods=10, freq='5min'),
            'symbol': ['600519.SH'] * 10,
            'open': [100.0] * 10,
            'high': [105.0] * 10,
            'low': [95.0] * 10,
            'close': [103.0] * 10,
            'volume': [100000] * 10,
            'amount': [10300000] * 10,
            'frequency': ['minute_5'] * 10
        })

        fdh.ops.get_symbol_minute = AsyncMock(return_value=mock_data)

        result = await fdh.get_minute_async(
            ['600519.SH'],
            '2024-01-01',
            '2024-01-01',
            'minute_5'
        )

        assert result is not None
        assert len(result) == 10
        assert 'frequency' in result.columns

        fdh.ops.get_symbol_minute.assert_called_once_with(
            ['600519.SH'], '2024-01-01', '2024-01-01', 'minute_5'
        )

    @pytest.mark.asyncio
    async def test_get_daily_basic_async(self, fdh):
        """测试获取每日基本面数据"""
        mock_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=5),
            'symbol': ['600519.SH'] * 5,
            'turnover_rate': [2.5] * 5,
            'volume_ratio': [1.2] * 5,
            'pe': [25.0] * 5,
            'pe_ttm': [24.5] * 5,
            'pb': [8.0] * 5,
            'ps': [15.0] * 5,
            'ps_ttm': [14.5] * 5,
            'dv_ratio': [1.8] * 5,
            'dv_ttm': [1.7] * 5,
            'total_share': [1000000000] * 5,
            'float_share': [800000000] * 5,
            'free_share': [700000000] * 5,
            'total_mv': [100000000000] * 5,
            'circ_mv': [80000000000] * 5
        })

        fdh.ops.get_daily_basic = AsyncMock(return_value=mock_data)

        result = await fdh.get_daily_basic_async(
            ['600519.SH'],
            '2024-01-01',
            '2024-01-05'
        )

        assert result is not None
        assert len(result) == 5
        assert 'turnover_rate' in result.columns
        assert 'pe' in result.columns
        assert 'pb' in result.columns

    @pytest.mark.asyncio
    async def test_get_adj_factor_async(self, fdh):
        """测试获取复权因子数据"""
        mock_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=5),
            'symbol': ['600519.SH'] * 5,
            'adj_factor': [1.0, 1.01, 1.02, 1.03, 1.04]
        })

        fdh.ops.get_adj_factor = AsyncMock(return_value=mock_data)

        result = await fdh.get_adj_factor_async(
            ['600519.SH'],
            '2024-01-01',
            '2024-01-05'
        )

        assert result is not None
        assert len(result) == 5
        assert 'adj_factor' in result.columns

    @pytest.mark.asyncio
    async def test_get_basic_async(self, fdh):
        """测试获取股票基本信息"""
        mock_data = pd.DataFrame({
            'ts_code': ['600519.SH', '000858.SZ'],
            'symbol': ['600519', '000858'],
            'name': ['贵州茅台', '五粮液'],
            'area': ['西南', '西南'],
            'industry': ['白酒', '白酒'],
            'market': ['主板', '主板'],
            'exchange': ['SSE', 'SZSE'],
            'list_status': ['L', 'L'],
            'list_date': ['2001-08-27', '1998-04-27'],
            'delist_date': [None, None],
            'is_hs': ['H', 'H']
        })

        fdh.ops.get_asset_basic = AsyncMock(return_value=mock_data)

        result = await fdh.get_basic_async(['600519.SH', '000858.SZ'])

        assert result is not None
        assert len(result) == 2
        assert 'name' in result.columns
        assert 'industry' in result.columns

    @pytest.mark.asyncio
    async def test_get_basic_async_all_stocks(self, fdh):
        """测试获取所有股票基本信息"""
        mock_data = pd.DataFrame({
            'ts_code': ['600519.SH'],
            'symbol': ['600519'],
            'name': ['贵州茅台'],
            'area': ['西南'],
            'industry': ['白酒']
        })

        fdh.ops.get_asset_basic = AsyncMock(return_value=mock_data)

        result = await fdh.get_basic_async()

        assert result is not None
        # 验证传入空列表
        fdh.ops.get_asset_basic.assert_called_once_with([])

    @pytest.mark.asyncio
    async def test_get_weekly_async(self, fdh):
        """测试获取周线数据"""
        mock_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=2, freq='W'),
            'symbol': ['600519.SH', '600519.SH'],
            'open': [100.0, 105.0],
            'high': [105.0, 110.0],
            'low': [95.0, 100.0],
            'close': [103.0, 108.0],
            'volume': [5000000, 5500000],
            'amount': [515000000, 594000000],
            'adj_factor': [1.0, 1.01]
        })

        fdh.ops.get_weekly_data = AsyncMock(return_value=mock_data)

        result = await fdh.get_weekly_async(['600519.SH'], '2024-01-01', '2024-01-31')

        assert result is not None
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_monthly_async(self, fdh):
        """测试获取月线数据"""
        mock_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=2, freq='MS'),
            'symbol': ['600519.SH', '600519.SH'],
            'open': [100.0, 105.0],
            'high': [105.0, 110.0],
            'low': [95.0, 100.0],
            'close': [103.0, 108.0],
            'volume': [20000000, 22000000],
            'amount': [2060000000, 2376000000],
            'adj_factor': [1.0, 1.01]
        })

        fdh.ops.get_monthly_data = AsyncMock(return_value=mock_data)

        result = await fdh.get_monthly_async(['600519.SH'], '2024-01-01', '2024-12-31')

        assert result is not None
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_close(self, fdh):
        """测试关闭连接"""
        await fdh.close()
        fdh.db_manager.close.assert_called_once()


class TestSDKSyncMethods:
    """测试 SDK 同步方法"""

    @pytest.fixture
    def fdh(self):
        """创建 SDK 实例用于测试"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        fdh = FinanceDataHub(settings, backend="postgresql")
        fdh.db_manager = AsyncMock()
        fdh.ops = AsyncMock()
        return fdh

    def test_get_daily_sync(self, fdh):
        """测试同步获取日线数据"""
        mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
        fdh.ops.get_symbol_daily = AsyncMock(return_value=mock_data)

        result = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-01-05')

        assert result is not None
        fdh.ops.get_symbol_daily.assert_called_once()

    def test_get_minute_sync(self, fdh):
        """测试同步获取分钟数据"""
        mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
        fdh.ops.get_symbol_minute = AsyncMock(return_value=mock_data)

        result = fdh.get_minute(['600519.SH'], '2024-01-01', '2024-01-01', 'minute_5')

        assert result is not None

    def test_get_daily_basic_sync(self, fdh):
        """测试同步获取每日基本面数据"""
        mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
        fdh.ops.get_daily_basic = AsyncMock(return_value=mock_data)

        result = fdh.get_daily_basic(['600519.SH'], '2024-01-01', '2024-01-05')

        assert result is not None

    def test_get_adj_factor_sync(self, fdh):
        """测试同步获取复权因子数据"""
        mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
        fdh.ops.get_adj_factor = AsyncMock(return_value=mock_data)

        result = fdh.get_adj_factor(['600519.SH'], '2024-01-01', '2024-01-05')

        assert result is not None

    def test_get_basic_sync(self, fdh):
        """测试同步获取股票基本信息"""
        mock_data = pd.DataFrame({'ts_code': ['600519.SH'], 'name': ['贵州茅台']})
        fdh.ops.get_asset_basic = AsyncMock(return_value=mock_data)

        result = fdh.get_basic(['600519.SH'])

        assert result is not None

    def test_get_weekly_sync(self, fdh):
        """测试同步获取周线数据"""
        mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
        fdh.ops.get_weekly_data = AsyncMock(return_value=mock_data)

        result = fdh.get_weekly(['600519.SH'], '2024-01-01', '2024-01-31')

        assert result is not None

    def test_get_monthly_sync(self, fdh):
        """测试同步获取月线数据"""
        mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
        fdh.ops.get_monthly_data = AsyncMock(return_value=mock_data)

        result = fdh.get_monthly(['600519.SH'], '2024-01-01', '2024-12-31')

        assert result is not None


class TestSDKRoutingLogging:
    """测试 SDK 路由日志记录功能"""

    @pytest.fixture
    def fdh(self):
        """创建带路由的 SDK 实例"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        fdh = FinanceDataHub(settings, backend="postgresql")
        fdh.db_manager = AsyncMock()
        fdh.ops = AsyncMock()

        # 模拟路由器
        fdh.router = Mock()
        fdh.router.get_providers_for_route = Mock(return_value=['tushare'])
        return fdh

    @pytest.mark.asyncio
    async def test_routing_decision_logging(self, fdh):
        """测试路由决策日志记录"""
        with patch('finance_data_hub.sdk.logger') as mock_logger:
            mock_data = pd.DataFrame({'time': [datetime.now()], 'symbol': ['600519.SH']})
            fdh.ops.get_symbol_daily = AsyncMock(return_value=mock_data)

            await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-01-05')

            # 验证日志记录被调用
            assert mock_logger.info.called
            # 检查日志消息包含路由决策信息
            log_calls = [call for call in mock_logger.info.call_args_list]
            # 至少有一条日志记录了路由决策
            assert len(log_calls) > 0


class TestSDKContextManager:
    """测试 SDK 上下文管理器"""

    @pytest.fixture
    def fdh(self):
        """创建 SDK 实例"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        return FinanceDataHub(settings, backend="postgresql")

    @pytest.mark.asyncio
    async def test_async_context_manager(self, fdh):
        """测试异步上下文管理器"""
        fdh.initialize = AsyncMock()
        fdh.close = AsyncMock()

        async with fdh as ctx:
            assert ctx == fdh
            fdh.initialize.assert_called_once()

        fdh.close.assert_called_once()

    def test_sync_context_manager(self, fdh):
        """测试同步上下文管理器（不推荐但应可用）"""
        with patch('asyncio.run') as mock_run:
            fdh.initialize = AsyncMock()
            fdh.close = AsyncMock()

            with fdh as ctx:
                assert ctx == fdh

            # 验证调用了 asyncio.run
            assert mock_run.called
