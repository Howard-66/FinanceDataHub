"""
配置管理模块

使用 Pydantic 管理应用程序配置，支持环境变量和 .env 文件加载。
"""

from typing import Optional
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

import os


# 获取项目根目录路径
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
ENV_FILE_PATH = PROJECT_ROOT / ".env"

# 加载 .env 文件
if ENV_FILE_PATH.exists():
    load_dotenv(ENV_FILE_PATH)


class DatabaseConfig(BaseSettings):
    """数据库配置"""

    url: str = Field(
        default="postgresql://trading_nexus:trading.nexus.data@localhost:5432/trading_nexus_db",
        description="PostgreSQL 连接字符串",
        env="DATABASE_URL"
    )

    pool_size: int = Field(
        default=20,
        ge=1,
        le=100,
        description="连接池大小"
    )

    max_overflow: int = Field(
        default=30,
        ge=0,
        le=100,
        description="最大溢出连接数"
    )

    pool_timeout: int = Field(
        default=30,
        ge=1,
        description="连接池超时时间（秒）"
    )


class RedisConfig(BaseSettings):
    """Redis 配置"""

    url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis 连接 URL",
        env="REDIS_URL"
    )

    max_connections: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="最大连接数"
    )

    retry_on_timeout: bool = Field(
        default=True,
        description="超时重试"
    )


class DataSourceConfig(BaseSettings):
    """数据源配置"""

    tushare_token: Optional[str] = Field(
        default=None,
        description="Tushare API 令牌",
        env="TUSHARE_TOKEN"
    )

    xtquant_api_url: str = Field(
        default="http://192.168.1.100:8000",
        description="XTQuant API URL",
        env="XTQUANT_API_URL"
    )

    sources_config_path: str = Field(
        default="./sources.yml",
        description="数据源配置文件路径",
        env="SOURCES_CONFIG_PATH"
    )


class LoggingConfig(BaseSettings):
    """日志配置"""

    level: str = Field(
        default="INFO",
        description="日志级别",
        env="LOG_LEVEL"
    )

    format: str = Field(
        default="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        description="日志格式"
    )

    rotation: str = Field(
        default="10 MB",
        description="日志轮转大小"
    )

    retention: str = Field(
        default="30 days",
        description="日志保留时间"
    )


class ETLConfig(BaseSettings):
    """ETL 配置"""

    data_path: str = Field(
        default="./data/etl",
        description="ETL 数据目录",
        env="ETL_DATA_PATH"
    )

    parquet_path: str = Field(
        default="./data/parquet",
        description="Parquet 文件目录",
        env="PARQUET_DATA_PATH"
    )

    batch_size: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="批处理大小"
    )

    @field_validator("data_path", "parquet_path", mode="before")
    @classmethod
    def create_directories(cls, v: str) -> str:
        """自动创建目录"""
        path = Path(v)
        path.mkdir(parents=True, exist_ok=True)
        return str(path)


class Settings(BaseSettings):
    """应用主配置"""

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH) if ENV_FILE_PATH.exists() else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_prefix="",  # 不使用前缀
    )

    # 子配置模块
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    data_source: DataSourceConfig = Field(default_factory=DataSourceConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    etl: ETLConfig = Field(default_factory=ETLConfig)


# 使用懒加载的方式，避免在模块导入时就创建实例
_settings_instance = None


def get_settings() -> Settings:
    """
    获取配置实例

    Returns:
        Settings: 配置实例（懒加载）
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance


def reload_settings() -> Settings:
    """
    重新加载配置

    Returns:
        Settings: 重新加载的配置实例
    """
    global _settings_instance
    _settings_instance = Settings()
    return _settings_instance
