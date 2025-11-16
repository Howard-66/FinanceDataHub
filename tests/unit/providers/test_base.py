"""
Provider基类和注册表测试
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from finance_data_hub.providers.base import (
    BaseDataProvider,
    ProviderError,
    ProviderConnectionError,
    ProviderAuthError,
    ProviderRateLimitError,
    ProviderDataError,
)
from finance_data_hub.providers.registry import ProviderRegistry, register_provider
from finance_data_hub.providers.schema import (
    StockBasicSchema,
    DailyDataSchema,
    validate_dataframe,
    standardize_symbol,
    convert_to_standard_columns,
)


class TestProviderErrors:
    """测试Provider错误类"""

    def test_provider_error(self):
        error = ProviderError("Test error", provider_name="test")
        assert str(error) == "Test error"
        assert error.provider_name == "test"

    def test_provider_rate_limit_error(self):
        error = ProviderRateLimitError(
            "Rate limit", provider_name="test", retry_after=60
        )
        assert error.retry_after == 60


class MockProvider(BaseDataProvider):
    """测试用的Mock Provider"""

    def initialize(self):
        self._is_initialized = True

    def health_check(self):
        return self._is_initialized

    def get_stock_basic(self, market=None, list_status="L"):
        return pd.DataFrame(
            {
                "symbol": ["600519.SH"],
                "name": ["贵州茅台"],
                "market": ["SH"],
                "industry": ["白酒"],
                "area": ["贵州"],
                "list_status": ["L"],
                "list_date": [pd.Timestamp("2001-08-27")],
                "delist_date": [pd.NaT],
                "is_hs": ["N"],
            }
        )

    def get_daily_data(self, symbol, start_date, end_date, adj=None):
        return pd.DataFrame(
            {
                "time": [pd.Timestamp("2024-01-01")],
                "symbol": [symbol],
                "open": [1800.0],
                "high": [1850.0],
                "low": [1780.0],
                "close": [1820.0],
                "volume": [1000000],
                "amount": [1800000000.0],
            }
        )

    def get_minute_data(self, symbol, start_date, end_date, freq="1m"):
        return pd.DataFrame()

    def get_daily_basic(
        self, symbol=None, trade_date=None, start_date=None, end_date=None
    ):
        return pd.DataFrame()


class TestBaseDataProvider:
    """测试BaseDataProvider基类"""

    def test_provider_initialization(self):
        provider = MockProvider("mock", {"test": "config"})
        assert provider.name == "mock"
        assert provider.config["test"] == "config"
        assert not provider._is_initialized

        provider.initialize()
        assert provider._is_initialized

    def test_health_check(self):
        provider = MockProvider("mock")
        assert not provider.health_check()

        provider.initialize()
        assert provider.health_check()

    def test_retry_on_failure_success(self):
        provider = MockProvider("mock")
        provider.initialize()

        call_count = [0]

        def flaky_function():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ProviderConnectionError("Temporary error", provider_name="mock")
            return "success"

        result = provider.retry_on_failure(flaky_function, max_retries=3)
        assert result == "success"
        assert call_count[0] == 3

    def test_retry_on_failure_auth_error(self):
        provider = MockProvider("mock")
        provider.initialize()

        def auth_error_function():
            raise ProviderAuthError("Auth failed", provider_name="mock")

        with pytest.raises(ProviderAuthError):
            provider.retry_on_failure(auth_error_function, max_retries=3)


class TestProviderRegistry:
    """测试ProviderRegistry"""

    def setup_method(self):
        """每个测试前清空注册表"""
        ProviderRegistry.clear()

    def test_register_provider(self):
        ProviderRegistry.register("mock", MockProvider)
        assert ProviderRegistry.has_provider("mock")
        assert "mock" in ProviderRegistry.list_providers()

    def test_register_decorator(self):
        @register_provider("decorated_mock")
        class DecoratedMockProvider(BaseDataProvider):
            def initialize(self):
                pass

            def health_check(self):
                return True

            def get_stock_basic(self, market=None, list_status="L"):
                return pd.DataFrame()

            def get_daily_data(self, symbol, start_date, end_date, adj=None):
                return pd.DataFrame()

            def get_minute_data(self, symbol, start_date, end_date, freq="1m"):
                return pd.DataFrame()

            def get_daily_basic(
                self, symbol=None, trade_date=None, start_date=None, end_date=None
            ):
                return pd.DataFrame()

        assert ProviderRegistry.has_provider("decorated_mock")

    def test_create_provider(self):
        ProviderRegistry.register("mock", MockProvider)
        provider = ProviderRegistry.create_provider("mock", {"test": "value"})

        assert isinstance(provider, MockProvider)
        assert provider._is_initialized
        assert provider.config["test"] == "value"

    def test_create_provider_with_cache(self):
        ProviderRegistry.register("mock", MockProvider)

        provider1 = ProviderRegistry.create_provider("mock", cache=True)
        provider2 = ProviderRegistry.create_provider("mock", cache=True)

        assert provider1 is provider2  # 应该是同一个实例

    def test_create_provider_without_cache(self):
        ProviderRegistry.register("mock", MockProvider)

        provider1 = ProviderRegistry.create_provider("mock", cache=False)
        provider2 = ProviderRegistry.create_provider("mock", cache=False)

        assert provider1 is not provider2  # 应该是不同实例

    def test_get_nonexistent_provider(self):
        with pytest.raises(ProviderError) as exc_info:
            ProviderRegistry.get_provider_class("nonexistent")
        assert "not registered" in str(exc_info.value)


class TestDataFrameValidation:
    """测试DataFrame验证功能"""

    def test_validate_stock_basic(self):
        df = pd.DataFrame(
            {
                "symbol": ["600519.SH"],
                "name": ["贵州茅台"],
                "market": ["SH"],
                "industry": ["白酒"],
                "area": ["贵州"],
                "list_status": ["L"],
                "list_date": ["2001-08-27"],
            }
        )

        validated = validate_dataframe(df, StockBasicSchema, provider_name="test")
        assert len(validated) == 1
        assert validated["symbol"].iloc[0] == "600519.SH"

    def test_validate_missing_columns(self):
        df = pd.DataFrame({"symbol": ["600519.SH"]})  # 缺少必需列

        with pytest.raises(ProviderDataError) as exc_info:
            validate_dataframe(df, StockBasicSchema, strict=True, provider_name="test")
        assert "Missing required columns" in str(exc_info.value)

    def test_validate_empty_dataframe(self):
        df = pd.DataFrame()
        validated = validate_dataframe(df, StockBasicSchema, provider_name="test")
        assert validated.empty
        assert list(validated.columns) == StockBasicSchema.get_required_columns()


class TestSymbolStandardization:
    """测试股票代码标准化"""

    def test_standardize_tushare_format(self):
        symbol = standardize_symbol("600519.SH", provider_format="tushare")
        assert symbol == "600519.SH"

    def test_standardize_xtquant_format(self):
        symbol = standardize_symbol("SH.600519", provider_format="xtquant")
        assert symbol == "600519.SH"


class TestColumnConversion:
    """测试列名转换"""

    def test_convert_columns(self):
        df = pd.DataFrame({"old_name": [1, 2, 3], "other": [4, 5, 6]})

        mapping = {"old_name": "new_name"}
        converted = convert_to_standard_columns(df, mapping)

        assert "new_name" in converted.columns
        assert "old_name" not in converted.columns
        assert "other" in converted.columns
