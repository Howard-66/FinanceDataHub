"""
智能数据源路由器

根据配置文件自动选择合适的数据提供者，并支持故障转移。
"""

import os
import time
import inspect
from typing import Optional, Dict, Any, List, Callable
from pathlib import Path
import yaml
from loguru import logger

from finance_data_hub.providers.base import (
    BaseDataProvider,
    ProviderError,
    ProviderConnectionError,
    ProviderRateLimitError,
)
from finance_data_hub.providers.registry import ProviderRegistry
from finance_data_hub.utils.market import infer_market_from_symbol, normalize_market


class RoutingConfig:
    """路由配置类"""

    def __init__(self, config_dict: Dict[str, Any]):
        self.providers = config_dict.get("providers", {})
        self.routing_strategy = config_dict.get("routing_strategy", {})
        self.failover = config_dict.get("failover", {})
        self.load_balance = config_dict.get("load_balance", {})
        self.logging_config = config_dict.get("logging", {})

    def get_providers_for_route(
        self,
        asset_class: str,
        data_type: str,
        freq: Optional[str] = None,
        market: Optional[str] = None,
    ) -> List[str]:
        """
        获取指定路由的提供者列表

        Args:
            asset_class: 资产类别（例如 "stock", "future"）
            data_type: 数据类型（例如 "daily", "minute", "basic"）
            freq: 频率（对于分钟数据，例如 "1m", "5m"）

        Returns:
            List[str]: 提供者名称列表，按优先级排序
        """
        strategy = self._get_strategy_for_market(asset_class, market)

        if freq and data_type in strategy:
            # 分钟数据有子频率配置
            freq_config = strategy.get(data_type, {}).get(freq, {})
            if freq_config:
                return freq_config.get("providers", [])

        # 没有子频率或者其他数据类型
        route_config = strategy.get(data_type, {})
        if isinstance(route_config, dict):
            return route_config.get("providers", [])

        return []

    def is_fallback_enabled(
        self,
        asset_class: str,
        data_type: str,
        freq: Optional[str] = None,
        market: Optional[str] = None,
    ) -> bool:
        """
        检查是否启用故障转移

        Args:
            asset_class: 资产类别
            data_type: 数据类型
            freq: 频率

        Returns:
            bool: 是否启用故障转移
        """
        strategy = self._get_strategy_for_market(asset_class, market)

        if freq and data_type in strategy:
            freq_config = strategy.get(data_type, {}).get(freq, {})
            if freq_config:
                return freq_config.get("fallback", False)

        route_config = strategy.get(data_type, {})
        if isinstance(route_config, dict):
            return route_config.get("fallback", False)

        return False

    def _get_strategy_for_market(
        self, asset_class: str, market: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return market-specific strategy while preserving old sources.yml shape."""
        strategy = self.routing_strategy.get(asset_class, {})
        if not isinstance(strategy, dict):
            return {}

        market_sections = {
            key.upper()
            for key, value in strategy.items()
            if key.upper() in {"CN", "HK", "US", "JP"} and isinstance(value, dict)
        }
        if not market_sections:
            return strategy

        normalized_market = normalize_market(market, default="CN")
        if normalized_market == "ALL":
            normalized_market = "CN"
        if normalized_market in strategy and isinstance(strategy[normalized_market], dict):
            return strategy[normalized_market]

        # Compatibility fallback if a mixed config keeps some routes at stock.daily.
        return strategy


class CircuitBreaker:
    """
    断路器模式实现

    当提供者连续失败达到阈值时，暂时停止使用该提供者。
    """

    def __init__(
        self, failure_threshold: int = 5, reset_timeout: float = 60.0
    ):
        """
        初始化断路器

        Args:
            failure_threshold: 连续失败次数阈值
            reset_timeout: 重置超时时间（秒）
        """
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout

        # 每个提供者的状态
        self._states: Dict[str, Dict[str, Any]] = {}

    def is_available(self, provider_name: str) -> bool:
        """
        检查提供者是否可用

        Args:
            provider_name: 提供者名称

        Returns:
            bool: 是否可用
        """
        if provider_name not in self._states:
            return True

        state = self._states[provider_name]

        # 如果断路器打开且超时时间已过，尝试重置
        if state["open"]:
            if time.time() - state["open_time"] >= self.reset_timeout:
                logger.info(f"Circuit breaker reset for provider: {provider_name}")
                state["open"] = False
                state["failure_count"] = 0
                return True
            return False

        return True

    def time_until_available(self, provider_name: str) -> float:
        """Return seconds until an open circuit can be retried."""
        state = self._states.get(provider_name)
        if not state or not state["open"]:
            return 0.0
        elapsed = time.time() - state["open_time"]
        return max(0.0, self.reset_timeout - elapsed)

    def record_success(self, provider_name: str) -> None:
        """
        记录成功调用

        Args:
            provider_name: 提供者名称
        """
        if provider_name in self._states:
            self._states[provider_name]["failure_count"] = 0

    def record_failure(self, provider_name: str) -> None:
        """
        记录失败调用

        Args:
            provider_name: 提供者名称
        """
        if provider_name not in self._states:
            self._states[provider_name] = {
                "failure_count": 0,
                "open": False,
                "open_time": 0,
            }

        state = self._states[provider_name]
        state["failure_count"] += 1

        # 检查是否达到阈值
        if state["failure_count"] >= self.failure_threshold:
            state["open"] = True
            state["open_time"] = time.time()
            logger.warning(
                f"Circuit breaker opened for provider: {provider_name} "
                f"(failures: {state['failure_count']})"
            )

    def reset(self, provider_name: str) -> None:
        """
        重置断路器

        Args:
            provider_name: 提供者名称
        """
        if provider_name in self._states:
            del self._states[provider_name]


class SmartRouter:
    """
    智能数据源路由器

    根据配置文件自动选择合适的数据提供者，支持：
    - 基于规则的路由
    - 故障转移
    - 断路器模式
    - 路由统计
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化智能路由器

        Args:
            config_path: 配置文件路径，默认为 ./sources.yml
        """
        if config_path is None:
            config_path = "sources.yml"

        self.config_path = Path(config_path)
        self.config: Optional[RoutingConfig] = None

        # 提供者实例缓存
        self._provider_instances: Dict[str, BaseDataProvider] = {}

        # 断路器
        self._circuit_breaker: Optional[CircuitBreaker] = None

        # 统计信息
        self._stats: Dict[str, Dict[str, int]] = {}

        # 加载配置
        self.load_config()

    def load_config(self) -> None:
        """
        加载配置文件

        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置文件格式错误
        """
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}"
            )

        with open(self.config_path, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)

        # 替换环境变量
        config_dict = self._expand_env_vars(config_dict)

        self.config = RoutingConfig(config_dict)

        # 初始化断路器
        if self.config.failover.get("circuit_breaker", {}).get("enabled", False):
            cb_config = self.config.failover["circuit_breaker"]
            self._circuit_breaker = CircuitBreaker(
                failure_threshold=cb_config.get("failure_threshold", 5),
                reset_timeout=cb_config.get("reset_timeout", 60.0),
            )

        logger.info(f"Loaded routing configuration from {self.config_path}")

    def _expand_env_vars(self, obj: Any) -> Any:
        """
        递归替换配置中的环境变量

        支持格式：
        - ${VAR_NAME}
        - ${VAR_NAME:-default_value}

        Args:
            obj: 配置对象

        Returns:
            替换后的配置对象
        """
        if isinstance(obj, dict):
            return {k: self._expand_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._expand_env_vars(item) for item in obj]
        elif isinstance(obj, str):
            # 简单的环境变量替换
            if obj.startswith("${") and obj.endswith("}"):
                var_expr = obj[2:-1]
                # 支持默认值：${VAR:-default}
                if ":-" in var_expr:
                    var_name, default = var_expr.split(":-", 1)
                    return os.getenv(var_name, default)
                else:
                    return os.getenv(var_expr, obj)
        return obj

    def _get_provider(
        self, provider_name: str, market: Optional[str] = None
    ) -> BaseDataProvider:
        """
        获取或创建提供者实例

        Args:
            provider_name: 提供者名称

        Returns:
            BaseDataProvider: 提供者实例

        Raises:
            ProviderError: 提供者不可用
        """
        provider_market = normalize_market(market, default="CN") or "CN"
        if provider_market == "ALL":
            provider_market = "CN"
        cache_key = f"{provider_name}:{provider_market}"

        # 检查是否已缓存
        if cache_key in self._provider_instances:
            return self._provider_instances[cache_key]

        # 检查配置
        if not self.config or provider_name not in self.config.providers:
            raise ProviderError(
                f"Provider '{provider_name}' not configured",
                provider_name=provider_name,
            )

        provider_config = self.config.providers[provider_name]

        # 检查是否启用
        if not provider_config.get("enabled", True):
            raise ProviderError(
                f"Provider '{provider_name}' is disabled",
                provider_name=provider_name,
            )

        # 创建实例
        provider = ProviderRegistry.create_provider(
            provider_name,
            config=provider_config,
            cache=True,
            market=provider_market,
        )

        # 缓存实例
        self._provider_instances[cache_key] = provider

        return provider

    def _record_call(self, provider_name: str, success: bool) -> None:
        """
        记录调用统计

        Args:
            provider_name: 提供者名称
            success: 是否成功
        """
        if provider_name not in self._stats:
            self._stats[provider_name] = {
                "success": 0,
                "failure": 0,
                "total": 0,
            }

        self._stats[provider_name]["total"] += 1
        if success:
            self._stats[provider_name]["success"] += 1
        else:
            self._stats[provider_name]["failure"] += 1

    def route(
        self,
        asset_class: str,
        data_type: str,
        freq: Optional[str] = None,
        market: Optional[str] = None,
        method_name: str = "get_daily_data",
        wait_for_circuit_breaker: bool = False,
        **kwargs,
    ) -> Any:
        """
        执行路由并调用相应的提供者方法

        Args:
            asset_class: 资产类别（例如 "stock"）
            data_type: 数据类型（例如 "daily", "minute"）
            freq: 频率（对于分钟数据）
            method_name: 提供者方法名
            wait_for_circuit_breaker: 断路器打开时是否等待恢复后再尝试
            **kwargs: 传递给提供者方法的参数

        Returns:
            提供者方法的返回值

        Raises:
            ProviderError: 所有提供者都失败
        """
        if not self.config:
            raise ProviderError("Router not configured")

        route_market = normalize_market(market)
        if not route_market:
            route_market = infer_market_from_symbol(kwargs.get("symbol"), default="CN")

        # 获取提供者列表
        providers = self.config.get_providers_for_route(
            asset_class, data_type, freq, route_market
        )

        if not providers:
            raise ProviderError(
                f"No providers configured for route: "
                f"{asset_class}/{route_market}/{data_type}" + (f"/{freq}" if freq else "")
            )

        # 是否启用故障转移
        fallback_enabled = self.config.is_fallback_enabled(
            asset_class, data_type, freq, route_market
        )

        last_error: Optional[Exception] = None

        # 尝试每个提供者
        for provider_name in providers:
            # 检查断路器
            if self._circuit_breaker and not self._circuit_breaker.is_available(
                provider_name
            ):
                if wait_for_circuit_breaker:
                    wait_time = self._circuit_breaker.time_until_available(provider_name)
                    if wait_time > 0:
                        logger.warning(
                            f"Provider '{provider_name}' circuit breaker is open; "
                            f"waiting {wait_time:.1f}s before retry"
                        )
                        time.sleep(wait_time)
                    if self._circuit_breaker.is_available(provider_name):
                        logger.info(
                            f"Provider '{provider_name}' circuit breaker is closed; retrying"
                        )
                    else:
                        logger.warning(
                            f"Provider '{provider_name}' is unavailable "
                            "(circuit breaker open)"
                        )
                        continue
                else:
                    logger.warning(
                        f"Provider '{provider_name}' is unavailable (circuit breaker open)"
                    )
                    continue

            try:
                # 获取提供者实例
                provider = self._get_provider(provider_name, route_market)

                # 调用方法
                method = getattr(provider, method_name, None)
                if not method:
                    raise ProviderError(
                        f"Method '{method_name}' not found in provider '{provider_name}'"
                    )

                logger.debug(
                    f"Routing to provider: {provider_name} "
                    f"(method: {method_name}, route: {asset_class}/{route_market}/{data_type})"
                )

                call_kwargs = dict(kwargs)

                # 如果有 freq 参数，将其添加到 kwargs 中传递给 provider 方法
                if freq:
                    call_kwargs["freq"] = freq
                    logger.debug(f"Passing freq={freq} to provider method")

                if route_market:
                    try:
                        signature = inspect.signature(method)
                        if "market" in signature.parameters and "market" not in call_kwargs:
                            call_kwargs["market"] = route_market
                    except (TypeError, ValueError):
                        pass

                result = method(**call_kwargs)

                # 记录成功
                self._record_call(provider_name, success=True)
                if self._circuit_breaker:
                    self._circuit_breaker.record_success(provider_name)

                logger.info(
                    f"Successfully fetched data from provider: {provider_name}"
                )

                return result

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Provider '{provider_name}' failed: {str(e)} "
                    f"(type: {type(e).__name__})"
                )

                # 记录失败
                self._record_call(provider_name, success=False)
                if self._circuit_breaker:
                    self._circuit_breaker.record_failure(provider_name)

                # 如果不启用故障转移，立即抛出错误
                if not fallback_enabled:
                    raise

                # 继续尝试下一个提供者

        # 所有提供者都失败
        error_msg = (
            f"All providers failed for route: {asset_class}/{data_type}"
            + f" (market={route_market})"
            + (f"/{freq}" if freq else "")
        )
        if last_error:
            raise ProviderError(error_msg) from last_error
        else:
            raise ProviderError(error_msg)

    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """
        获取路由统计信息

        Returns:
            Dict[str, Dict[str, int]]: 统计信息
        """
        return self._stats.copy()

    def reset_stats(self) -> None:
        """重置统计信息"""
        self._stats.clear()

    def reload_config(self) -> None:
        """重新加载配置文件"""
        logger.info("Reloading routing configuration")
        self.load_config()
