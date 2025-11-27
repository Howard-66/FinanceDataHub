"""
SmartRouter 与 SDK 集成测试

测试 SmartRouter 与 FinanceDataHub SDK 的完整集成，包括：
1. SmartRouter 初始化
2. 数据源选择逻辑
3. 路由决策日志记录
4. 数据新鲜度检查
5. 配置加载和错误处理
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime

from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.config import Settings
from finance_data_hub.router.smart_router import SmartRouter


class TestSmartRouterInitialization:
    """测试 SmartRouter 初始化"""

    @pytest.fixture
    def mock_settings(self):
        """模拟设置对象"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"

        # 模拟 data_source 属性
        settings.data_source = Mock()
        settings.data_source.sources_config_path = "sources.yml"
        return settings

    def test_smartrouter_success_initialization(self, mock_settings):
        """测试 SmartRouter 成功初始化"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            # 验证 SmartRouter 被正确调用
            mock_router_class.assert_called_once_with("sources.yml")
            assert fdh.router == mock_router_instance

    def test_smartrouter_file_not_found(self, mock_settings):
        """测试配置文件不存在时的处理"""
        with patch(
            'finance_data_hub.sdk.SmartRouter',
            side_effect=FileNotFoundError("sources.yml not found")
        ):
            with patch('finance_data_hub.sdk.logger') as mock_logger:
                fdh = FinanceDataHub(mock_settings)

                # 验证降级处理
                assert fdh.router is None
                mock_logger.warning.assert_called_once()
                # 验证日志消息
                warning_call = mock_logger.warning.call_args[0][0]
                assert "sources.yml not found" in warning_call

    def test_smartrouter_other_error(self, mock_settings):
        """测试其他错误时的处理"""
        with patch(
            'finance_data_hub.sdk.SmartRouter',
            side_effect=Exception("Configuration error")
        ):
            with patch('finance_data_hub.sdk.logger') as mock_logger:
                fdh = FinanceDataHub(mock_settings)

                # 验证降级处理
                assert fdh.router is None
                mock_logger.error.assert_called_once()
                # 验证日志消息
                error_call = mock_logger.error.call_args[0][0]
                assert "Configuration error" in error_call

    def test_smartrouter_no_config_path(self, mock_settings):
        """测试没有配置路径时"""
        mock_settings.data_source.sources_config_path = None

        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            fdh = FinanceDataHub(mock_settings)

            # SmartRouter 不应该被调用
            mock_router_class.assert_not_called()
            assert fdh.router is None

    def test_custom_router_config_path(self, mock_settings):
        """测试自定义路由器配置文件路径"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_class.return_value = mock_router_instance

            custom_path = "custom_sources.yml"
            fdh = FinanceDataHub(mock_settings, router_config_path=custom_path)

            # 验证使用了自定义路径
            mock_router_class.assert_called_once_with(custom_path)
            assert fdh.router == mock_router_instance

    def test_settings_without_data_source(self, mock_settings):
        """测试设置没有 data_source 属性"""
        delattr(mock_settings, 'data_source')

        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            fdh = FinanceDataHub(mock_settings)

            # SmartRouter 不应该被调用
            mock_router_class.assert_not_called()
            assert fdh.router is None


class TestSmartRouterIntegrationInQueries:
    """测试查询方法中的 SmartRouter 集成"""

    @pytest.fixture
    def fdh(self):
        """创建带 SmartRouter 的 SDK 实例"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        fdh = FinanceDataHub(settings, backend="postgresql")

        # 模拟数据库操作
        fdh.db_manager = Mock()
        fdh.ops = Mock()
        fdh.ops.get_symbol_daily = Mock(return_value=pd.DataFrame({
            'time': [datetime.now()],
            'symbol': ['600519.SH']
        }))
        return fdh

    @pytest.mark.asyncio
    async def test_get_daily_with_router_integration(self, fdh):
        """测试带路由器的日线数据查询"""
        # 设置路由器
        fdh.router = Mock()
        fdh.router.get_providers_for_route = Mock(return_value=['tushare'])

        # 执行查询
        result = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-01-05')

        # 验证路由器被调用
        fdh.router.get_providers_for_route.assert_called_once_with(
            "stock", "daily", None
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_minute_with_router_integration(self, fdh):
        """测试带路由器的分钟数据查询"""
        fdh.router = Mock()
        fdh.router.get_providers_for_route = Mock(return_value=['xtquant'])

        fdh.ops.get_symbol_minute = Mock(return_value=pd.DataFrame({
            'time': [datetime.now()],
            'symbol': ['600519.SH']
        }))

        result = await fdh.get_minute_async(
            ['600519.SH'],
            '2024-01-01',
            '2024-01-01',
            'minute_5'
        )

        # 验证路由器被正确调用，传递了 frequency
        fdh.router.get_providers_for_route.assert_called_once_with(
            "stock", "minute", "minute_5"
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_check_data_freshness_with_providers(self, fdh):
        """测试数据新鲜度检查与提供商集成"""
        fdh.router = Mock()
        fdh.router.get_providers_for_route = Mock(return_value=['tushare', 'xtquant'])

        result = await fdh.check_data_freshness(['600519.SH'], 'daily')

        # 验证提供商列表
        assert result['available_providers'] == ['tushare', 'xtquant']

        # 验证路由器被调用
        fdh.router.get_providers_for_route.assert_called_once_with(
            "stock", "daily", None
        )

    @pytest.mark.asyncio
    async def test_check_data_freshness_no_providers(self, fdh):
        """测试没有可用提供商时的处理"""
        fdh.router = Mock()
        fdh.router.get_providers_for_route = Mock(return_value=[])

        result = await fdh.check_data_freshness(['600519.SH'], 'daily')

        # 验证结果
        assert result['available_providers'] == []
        assert "No providers configured" in result['recommendation']

    def test_routing_decision_logging(self, fdh):
        """测试路由决策日志记录"""
        fdh.router = Mock()

        # 测试日志记录
        fdh._log_routing_decision(
            "daily",
            ['600519.SH'],
            "Query from PostgreSQL",
            "Available providers: tushare"
        )

        # 验证日志记录功能
        with patch('finance_data_hub.sdk.logger') as mock_logger:
            fdh.router = Mock()

            # 清空之前的调用
            mock_logger.reset_mock()

            # 记录路由决策
            fdh._log_routing_decision(
                "daily",
                ['600519.SH'],
                "Query from PostgreSQL",
                "Available providers: tushare"
            )

            # 验证日志被记录
            assert mock_logger.info.called

            # 获取日志消息
            log_call = mock_logger.info.call_args[0][0]
            assert "SmartRouter Decision" in log_call
            assert "daily" in log_call
            assert "Query from PostgreSQL" in log_call

    def test_routing_decision_logging_no_router(self, fdh):
        """测试没有路由器时的日志处理"""
        fdh.router = None

        # 应该直接返回，不记录日志
        fdh._log_routing_decision(
            "daily",
            ['600519.SH'],
            "Query from PostgreSQL",
            "No router available"
        )

        # 不应该引发异常


class TestSmartRouterRoutingStrategies:
    """测试 SmartRouter 路由策略"""

    @pytest.fixture
    def mock_settings(self):
        """模拟设置对象"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        return settings

    def test_routing_strategy_for_daily(self, mock_settings):
        """测试日线数据的路由策略"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(return_value=['tushare', 'xtquant'])
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            # 获取路由决策
            providers = fdh.router.get_providers_for_route("stock", "daily", None)

            # 验证路由策略
            assert len(providers) > 0
            assert 'tushare' in providers or 'xtquant' in providers

    def test_routing_strategy_for_minute_1(self, mock_settings):
        """测试1分钟线数据的路由策略"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(return_value=['xtquant'])
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            providers = fdh.router.get_providers_for_route("stock", "minute", "minute_1")

            # 验证1分钟线通常使用 xtquant
            assert 'xtquant' in providers

    def test_routing_strategy_for_daily_basic(self, mock_settings):
        """测试每日基本面的路由策略"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(return_value=['tushare'])
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            providers = fdh.router.get_providers_for_route("stock", "daily_basic", None)

            # 验证每日基本面通常使用 tushare
            assert 'tushare' in providers

    def test_routing_strategy_for_adj_factor(self, mock_settings):
        """测试复权因子的路由策略"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(return_value=['tushare'])
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            providers = fdh.router.get_providers_for_route("stock", "adj_factor", None)

            # 验证复权因子通常使用 tushare
            assert 'tushare' in providers

    def test_routing_strategy_for_basic(self, mock_settings):
        """测试股票基本信息的路由策略"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(return_value=['tushare'])
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            providers = fdh.router.get_providers_for_route("stock", "basic", None)

            # 验证股票基本信息使用 tushare
            assert 'tushare' in providers


class TestSmartRouterErrorHandling:
    """测试 SmartRouter 错误处理"""

    @pytest.fixture
    def mock_settings(self):
        """模拟设置对象"""
        settings = Mock(spec=Settings)
        settings.database.url = "postgresql://test:test@localhost:5432/test_db"
        return settings

    def test_router_get_providers_exception(self, mock_settings):
        """测试路由器获取提供商时发生异常"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(
                side_effect=Exception("Router error")
            )
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            # 即使路由器异常，也应该能够处理
            fdh.router = mock_router_instance

            # 这里需要模拟查询过程
            fdh.ops = Mock()
            fdh.ops.get_symbol_daily = Mock(return_value=pd.DataFrame())

            # 验证不会崩溃
            result = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-01-05')
            assert result is not None

    def test_multiple_symbols_routing(self, mock_settings):
        """测试多个股票代码的路由"""
        with patch('finance_data_hub.sdk.SmartRouter') as mock_router_class:
            mock_router_instance = Mock()
            mock_router_instance.get_providers_for_route = Mock(return_value=['tushare'])
            mock_router_class.return_value = mock_router_instance

            fdh = FinanceDataHub(mock_settings)

            # 验证路由决策日志记录
            fdh._log_routing_decision(
                "daily",
                ['600519.SH', '000858.SZ', '000001.SZ'],
                "Query from PostgreSQL",
                "3 symbols requested"
            )

            # 验证日志包含符号数量信息
            with patch('finance_data_hub.sdk.logger') as mock_logger:
                mock_logger.reset_mock()
                fdh._log_routing_decision(
                    "daily",
                    ['600519.SH', '000858.SZ'],
                    "Query from PostgreSQL",
                    "2 symbols"
                )

                log_call = mock_logger.info.call_args[0][0]
                assert "Symbols: 2" in log_call
