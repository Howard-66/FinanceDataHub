"""
技术指标基类

定义所有技术指标的通用接口和行为。
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Callable, Optional, Type
import pandas as pd
from loguru import logger


class BaseIndicator(ABC):
    """
    技术指标基类
    
    所有技术指标都需要继承此类，并实现 name, columns, calculate 方法。
    
    示例:
        >>> class MyIndicator(BaseIndicator):
        ...     @property
        ...     def name(self) -> str:
        ...         return "my_indicator"
        ...     
        ...     @property
        ...     def columns(self) -> List[str]:
        ...         return ["my_indicator"]
        ...     
        ...     def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        ...         result = df.copy()
        ...         result["my_indicator"] = ...
        ...         return result
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        指标名称
        
        用于标识指标类型和注册表查找。
        """
        pass
        
    @property
    @abstractmethod
    def columns(self) -> List[str]:
        """
        输出列名列表
        
        指标计算后添加到 DataFrame 的列名。
        """
        pass
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算指标
        
        Args:
            df: 输入 DataFrame，需包含 symbol, time, close 等列
            
        Returns:
            添加指标列后的 DataFrame
        """
        pass
    
    def validate_input(self, df: pd.DataFrame) -> bool:
        """
        验证输入数据
        
        Args:
            df: 输入 DataFrame
            
        Returns:
            是否有效
        """
        required_columns = ["symbol", "time", "close"]
        return all(col in df.columns for col in required_columns)
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseIndicator):
            return False
        return self.name == other.name


class IndicatorRegistry:
    """
    指标注册表
    
    用于管理和创建指标实例。
    
    示例:
        >>> registry = IndicatorRegistry()
        >>> registry.register("ma_20", lambda: MAIndicator(20))
        >>> indicator = registry.create("ma_20")
    """
    
    _instance: Optional["IndicatorRegistry"] = None
    _registry: Dict[str, Callable[[], BaseIndicator]] = {}
    _initialized: bool = False
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._registry = {}
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        pass  # 初始化在 __new__ 中完成
    
    def register(
        self, 
        name: str, 
        factory: Callable[[], BaseIndicator]
    ) -> None:
        """
        注册指标
        
        Args:
            name: 指标名称
            factory: 创建指标实例的工厂函数
        """
        self._registry[name] = factory
        logger.debug(f"Registered indicator: {name}")
    
    def create(self, name: str) -> BaseIndicator:
        """
        创建指标实例
        
        Args:
            name: 指标名称
            
        Returns:
            指标实例
            
        Raises:
            KeyError: 指标未注册
        """
        if name not in self._registry:
            raise KeyError(f"Indicator not registered: {name}")
        return self._registry[name]()
    
    def list_indicators(self) -> List[str]:
        """
        列出所有已注册的指标
        
        Returns:
            指标名称列表
        """
        return list(self._registry.keys())
    
    def is_registered(self, name: str) -> bool:
        """
        检查指标是否已注册
        
        Args:
            name: 指标名称
            
        Returns:
            是否已注册
        """
        return name in self._registry


# 全局注册表实例
indicator_registry = IndicatorRegistry()


def register_indicator(
    name: str, 
    factory: Callable[[], BaseIndicator]
) -> None:
    """
    注册指标的快捷函数
    
    Args:
        name: 指标名称
        factory: 创建指标实例的工厂函数
    """
    indicator_registry.register(name, factory)


def create_indicator(name: str) -> BaseIndicator:
    """
    创建指标实例的快捷函数
    
    Args:
        name: 指标名称
        
    Returns:
        指标实例
    """
    return indicator_registry.create(name)
