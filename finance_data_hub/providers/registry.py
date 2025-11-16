"""
数据提供者注册表

提供Provider的注册、发现和管理功能。
"""

from typing import Dict, Type, Optional, List
from loguru import logger

from finance_data_hub.providers.base import BaseDataProvider, ProviderError


class ProviderRegistry:
    """
    数据提供者注册表

    使用单例模式管理所有已注册的数据提供者。
    """

    _instance: Optional["ProviderRegistry"] = None
    _providers: Dict[str, Type[BaseDataProvider]] = {}
    _instances: Dict[str, BaseDataProvider] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def register(
        cls, name: str, provider_class: Type[BaseDataProvider]
    ) -> None:
        """
        注册一个Provider类

        Args:
            name: Provider名称（例如 "tushare", "xtquant"）
            provider_class: Provider类（必须继承自BaseDataProvider）

        Raises:
            ValueError: 如果name已存在或provider_class不是BaseDataProvider的子类
        """
        if not issubclass(provider_class, BaseDataProvider):
            raise ValueError(
                f"Provider class {provider_class.__name__} must inherit from BaseDataProvider"
            )

        if name in cls._providers:
            logger.warning(f"Provider '{name}' is already registered, overwriting")

        cls._providers[name] = provider_class
        logger.info(f"Registered provider: {name} -> {provider_class.__name__}")

    @classmethod
    def unregister(cls, name: str) -> None:
        """
        注销一个Provider

        Args:
            name: Provider名称
        """
        if name in cls._providers:
            del cls._providers[name]
            logger.info(f"Unregistered provider: {name}")

        if name in cls._instances:
            del cls._instances[name]

    @classmethod
    def get_provider_class(cls, name: str) -> Type[BaseDataProvider]:
        """
        获取Provider类

        Args:
            name: Provider名称

        Returns:
            Type[BaseDataProvider]: Provider类

        Raises:
            ProviderError: 如果Provider未注册
        """
        if name not in cls._providers:
            raise ProviderError(
                f"Provider '{name}' is not registered. "
                f"Available providers: {list(cls._providers.keys())}",
                provider_name=name,
            )
        return cls._providers[name]

    @classmethod
    def create_provider(
        cls, name: str, config: Optional[dict] = None, cache: bool = True
    ) -> BaseDataProvider:
        """
        创建或获取Provider实例

        Args:
            name: Provider名称
            config: Provider配置
            cache: 是否缓存实例（默认True，单例模式）

        Returns:
            BaseDataProvider: Provider实例

        Raises:
            ProviderError: 如果Provider未注册或初始化失败
        """
        # 如果启用缓存且实例已存在，直接返回
        if cache and name in cls._instances:
            logger.debug(f"Reusing cached provider instance: {name}")
            return cls._instances[name]

        # 获取Provider类
        provider_class = cls.get_provider_class(name)

        # 创建实例
        try:
            instance = provider_class(name=name, config=config)
            instance.initialize()

            # 缓存实例
            if cache:
                cls._instances[name] = instance

            logger.info(f"Created provider instance: {name}")
            return instance

        except Exception as e:
            logger.exception(f"Failed to create provider '{name}'")
            raise ProviderError(
                f"Failed to create provider '{name}': {str(e)}",
                provider_name=name,
            ) from e

    @classmethod
    def list_providers(cls) -> List[str]:
        """
        列出所有已注册的Provider名称

        Returns:
            List[str]: Provider名称列表
        """
        return list(cls._providers.keys())

    @classmethod
    def has_provider(cls, name: str) -> bool:
        """
        检查Provider是否已注册

        Args:
            name: Provider名称

        Returns:
            bool: 是否已注册
        """
        return name in cls._providers

    @classmethod
    def clear(cls) -> None:
        """清空所有注册的Provider（主要用于测试）"""
        cls._providers.clear()
        cls._instances.clear()
        logger.warning("All providers have been cleared")


# ===========================
# 装饰器：简化Provider注册
# ===========================


def register_provider(name: str):
    """
    Provider注册装饰器

    用法:
        @register_provider("tushare")
        class TushareProvider(BaseDataProvider):
            ...

    Args:
        name: Provider名称
    """

    def decorator(provider_class: Type[BaseDataProvider]):
        ProviderRegistry.register(name, provider_class)
        return provider_class

    return decorator
