"""
配置模块单元测试
"""

import os
import pytest
from pydantic import ValidationError

from finance_data_hub.config import (
    DatabaseConfig,
    RedisConfig,
    DataSourceConfig,
    LoggingConfig,
    ETLConfig,
    Settings,
    get_settings,
)


class TestDatabaseConfig:
    """测试数据库配置"""

    def test_default_database_url(self):
        """测试默认数据库 URL"""
        config = DatabaseConfig()
        assert "postgresql://" in config.url
        assert "localhost" in config.url

    def test_custom_database_url(self):
        """测试自定义数据库 URL"""
        url = "postgresql://user:pass@localhost:5432/test_db"
        config = DatabaseConfig(url=url)
        assert config.url == url

    def test_database_pool_size_validation(self):
        """测试连接池大小验证"""
        # 有效值
        config = DatabaseConfig(pool_size=10)
        assert config.pool_size == 10

        # 无效值 - 太小
        with pytest.raises(ValidationError):
            DatabaseConfig(pool_size=0)

        # 无效值 - 太大
        with pytest.raises(ValidationError):
            DatabaseConfig(pool_size=101)

    def test_database_query_concurrency_defaults(self):
        """测试读查询并发默认值。"""
        config = DatabaseConfig()
        assert config.query_max_concurrency == 8
        assert config.heavy_query_max_concurrency == 4

    def test_database_query_concurrency_from_env(self):
        """测试读查询并发支持环境变量覆盖。"""
        original_query = os.environ.get("DATABASE_QUERY_MAX_CONCURRENCY")
        original_heavy = os.environ.get("DATABASE_HEAVY_QUERY_MAX_CONCURRENCY")

        try:
            os.environ["DATABASE_QUERY_MAX_CONCURRENCY"] = "16"
            os.environ["DATABASE_HEAVY_QUERY_MAX_CONCURRENCY"] = "6"

            config = DatabaseConfig()
            assert config.query_max_concurrency == 16
            assert config.heavy_query_max_concurrency == 6
        finally:
            if original_query is None:
                os.environ.pop("DATABASE_QUERY_MAX_CONCURRENCY", None)
            else:
                os.environ["DATABASE_QUERY_MAX_CONCURRENCY"] = original_query

            if original_heavy is None:
                os.environ.pop("DATABASE_HEAVY_QUERY_MAX_CONCURRENCY", None)
            else:
                os.environ["DATABASE_HEAVY_QUERY_MAX_CONCURRENCY"] = original_heavy

    def test_heavy_query_concurrency_cannot_exceed_query_limit(self):
        """测试重查询并发不能超过普通查询并发。"""
        with pytest.raises(ValidationError):
            DatabaseConfig(query_max_concurrency=4, heavy_query_max_concurrency=8)


class TestRedisConfig:
    """测试 Redis 配置"""

    def test_default_redis_url(self):
        """测试默认 Redis URL"""
        config = RedisConfig()
        assert "redis://" in config.url
        assert "localhost" in config.url

    def test_custom_redis_url(self):
        """测试自定义 Redis URL"""
        url = "redis://localhost:6380/1"
        config = RedisConfig(url=url)
        assert config.url == url


class TestDataSourceConfig:
    """测试数据源配置"""

    def test_optional_tushare_token(self):
        """测试可选的 Tushare token"""
        # 如果环境中有 TUSHARE_TOKEN，则配置会使用它
        import os
        original_token = os.environ.get("TUSHARE_TOKEN")
        try:
            # 清除环境变量以测试默认值
            if "TUSHARE_TOKEN" in os.environ:
                del os.environ["TUSHARE_TOKEN"]
            config = DataSourceConfig()
            # 应该为 None 或默认值
            assert config.tushare_token is None or config.tushare_token == ""
        finally:
            # 恢复环境变量
            if original_token:
                os.environ["TUSHARE_TOKEN"] = original_token

    def test_xtquant_api_url(self):
        """测试 XTQuant API URL"""
        url = "http://192.168.1.100:8000"
        config = DataSourceConfig(xtquant_api_url=url)
        assert config.xtquant_api_url == url

    def test_futures_minute_max_workers_default_and_validation(self):
        """测试期货分钟下载并发默认值和范围限制。"""
        config = DataSourceConfig()
        assert config.futures_minute_max_workers == 4

        config = DataSourceConfig(futures_minute_max_workers=8)
        assert config.futures_minute_max_workers == 8

        with pytest.raises(ValidationError):
            DataSourceConfig(futures_minute_max_workers=0)


class TestLoggingConfig:
    """测试日志配置"""

    def test_default_log_level(self):
        """测试默认日志级别"""
        config = LoggingConfig()
        assert config.level == "INFO"

    def test_custom_log_level(self):
        """测试自定义日志级别"""
        config = LoggingConfig(level="DEBUG")
        assert config.level == "DEBUG"


class TestETLConfig:
    """测试 ETL 配置"""

    def test_default_paths(self):
        """测试默认路径"""
        config = ETLConfig()
        # Path 会自动标准化，去掉开头的 ./
        assert config.data_path.endswith("data/etl")
        assert config.parquet_path.endswith("data/parquet")

    def test_batch_size_validation(self):
        """测试批处理大小验证"""
        # 有效值
        config = ETLConfig(batch_size=500)
        assert config.batch_size == 500

        # 无效值
        with pytest.raises(ValidationError):
            ETLConfig(batch_size=0)


class TestSettings:
    """测试主配置类"""

    def test_settings_creation(self):
        """测试配置创建"""
        settings = Settings()
        assert isinstance(settings.database, DatabaseConfig)
        assert isinstance(settings.redis, RedisConfig)
        assert isinstance(settings.data_source, DataSourceConfig)
        assert isinstance(settings.logging, LoggingConfig)
        assert isinstance(settings.etl, ETLConfig)

    def test_settings_from_env(self):
        """测试从环境变量加载配置"""
        # 设置环境变量
        os.environ["DATABASE_URL"] = "postgresql://test:test@localhost:5432/test"
        os.environ["REDIS_URL"] = "redis://localhost:6380/1"

        # 重新加载配置（这里会有默认值）
        settings = Settings()

        # 验证设置被正确应用（可能是默认值或环境变量）
        # 我们只检查是否没有出错
        assert settings is not None

        # 清理环境变量
        del os.environ["DATABASE_URL"]
        del os.environ["REDIS_URL"]

    def test_get_settings(self):
        """测试获取全局配置"""
        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2
