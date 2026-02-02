"""
FinanceDataHub SDK

提供统一的金融数据访问接口，支持多种数据类型查询：
- 日线数据（daily）：OHLCV + 成交量 + 复权因子
- 分钟数据（minute_*）：1/5/15/30/60分钟线 OHLCV 数据
- 每日基本面（daily_basic）：估值指标、财务指标、流动性指标
- 复权因子（adj_factor）：前复权、后复权因子
- 股票基本信息（basic）：股票基本信息，非时间序列

集成 SmartRouter 智能数据源路由，自动选择最优数据源并记录路由决策。
"""

import asyncio
from typing import List, Optional, Dict, Any
import pandas as pd
from datetime import datetime
from loguru import logger

from finance_data_hub.config import Settings
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.router.smart_router import SmartRouter



class FinanceDataHub:
    """
    FinanceDataHub SDK - 金融数据服务中心

    提供统一的金融数据访问接口，支持多种数据类型查询和高周期数据聚合。

    核心功能：
    1. 基础数据查询：日线、分钟线、每日基本面、复权因子、股票基本信息
    2. 高周期聚合：周线、月线数据自动聚合计算
    3. 智能路由：集成 SmartRouter 自动选择最优数据源
    4. 数据新鲜度检查：检查数据是否需要更新，提供更新建议
    5. 异步/同步双接口：支持 async/await 和同步调用两种方式

    SmartRouter 集成：
    - 自动读取 sources.yml 配置文件
    - 根据数据类型选择最优数据提供商
    - 记录所有路由决策到日志
    - 提供数据新鲜度检查和更新建议

    Example (异步模式):
        >>> import asyncio
        >>> from finance_data_hub import FinanceDataHub
        >>>
        >>> async def main():
        ...     fdh = FinanceDataHub(settings, backend="postgresql")
        ...     await fdh.initialize()
        ...
        ...     # 查询日线数据
        ...     daily = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
        ...     print(daily.head())
        ...
        ...     await fdh.close()
        >>>
        >>> asyncio.run(main())

    Example (同步模式):
        >>> fdh = FinanceDataHub(settings, backend="postgresql")
        >>> daily = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')
        >>> print(daily.head())
    """

    def __init__(self, settings: Settings, backend: str = "auto", router_config_path: Optional[str] = None):
        """
        初始化 FinanceDataHub SDK

        Args:
            settings: 应用配置对象，包含数据库连接等信息
            backend: 数据后端类型，支持:
                - 'postgresql': 使用 PostgreSQL + TimescaleDB 作为主存储（推荐）
                - 'auto': 自动选择后端（目前等同于 postgresql）
                注意：当前版本仅支持 PostgreSQL 作为数据后端
            router_config_path: SmartRouter 配置文件路径（sources.yml）
                如果为 None 且 settings 中包含 data_source.sources_config_path，
                则使用默认路径
                如果配置文件不存在或加载失败，将禁用智能路由功能
        """
        self.settings = settings
        self.db_manager = DatabaseManager(settings)
        # 同步初始化数据库引擎（确保后续查询可用）
        _ = self.db_manager.get_engine()
        self.ops = DataOperations(self.db_manager)
        self.backend = backend

        # 初始化 SmartRouter（如果配置文件存在）
        try:
            config_path = None
            if hasattr(settings, 'data_source') and settings.data_source is not None:
                config_path = settings.data_source.sources_config_path

            if router_config_path:
                config_path = router_config_path

            self.router = SmartRouter(config_path) if config_path else None
            if self.router:
                logger.info("SmartRouter initialized successfully")
        except FileNotFoundError as e:
            logger.warning(f"SmartRouter initialization failed: {e}")
            self.router = None
        except Exception as e:
            logger.error(f"Error initializing SmartRouter: {e}")
            self.router = None

    async def initialize(self) -> None:
        """初始化数据库连接"""
        await self.db_manager.initialize()

    def _log_routing_decision(
        self,
        data_type: str,
        symbols: List[str],
        decision: str,
        reason: Optional[str] = None
    ) -> None:
        """
        记录路由决策日志

        Args:
            data_type: 数据类型（daily, minute, daily_basic, adj_factor, basic）
            symbols: 股票代码列表
            decision: 路由决策
            reason: 决策原因
        """
        if not self.router:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = (
            f"[{timestamp}] SmartRouter Decision | "
            f"Type: {data_type} | "
            f"Symbols: {len(symbols)} | "
            f"Decision: {decision}"
        )
        if reason:
            log_msg += f" | Reason: {reason}"

        logger.info(log_msg)

    async def check_data_freshness(
        self,
        symbols: List[str],
        data_type: str,
        frequency: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        检查数据新鲜度并提供更新建议

        此方法与 SmartRouter 集成使用，检查指定数据类型的数据是否最新，
        并提供数据更新建议。

        Args:
            symbols: 股票代码列表，例如 ['600519.SH', '000858.SZ']
                如果为空列表，将检查该数据类型的整体情况
            data_type: 数据类型，支持:
                - 'daily': 日线数据
                - 'minute': 分钟数据（需配合 frequency 参数）
                - 'daily_basic': 每日基本面数据
                - 'adj_factor': 复权因子数据
                - 'basic': 股票基本信息
            frequency: 频率（仅对分钟数据有效），支持:
                - 'minute_1': 1分钟线
                - 'minute_5': 5分钟线
                - 'minute_15': 15分钟线
                - 'minute_30': 30分钟线
                - 'minute_60': 60分钟线

        Returns:
            Dict[str, Any]: 包含数据新鲜度信息的字典，包含以下键值:
                - 'is_stale': bool，数据是否过时
                - 'latest_date': str|None，最新数据日期（格式：YYYY-MM-DD）
                - 'recommendation': str|None，建议操作
                - 'available_providers': List[str]，可用数据提供商列表

        Example:
            >>> freshness = await fdh.check_data_freshness(
            ...     ['600519.SH'], 'daily'
            ... )
            >>> print(f"是否过时: {freshness['is_stale']}")
            >>> print(f"可用提供商: {freshness['available_providers']}")
            >>> print(f"建议: {freshness['recommendation']}")
        """
        result = {
            "is_stale": False,
            "latest_date": None,
            "recommendation": None,
            "available_providers": []
        }

        if not self.router:
            result["recommendation"] = "SmartRouter not available"
            return result

        # 获取可用提供商
        if not self.router.config:
            result["recommendation"] = "SmartRouter not configured"
            return result

        providers = self.router.config.get_providers_for_route("stock", data_type, frequency)
        result["available_providers"] = providers

        if not providers:
            result["recommendation"] = "No providers configured for this data type"
            return result

        # 检查最新的数据日期
        if symbols:
            # 这里可以扩展为查询数据库中的最新数据日期
            # 目前返回基本建议
            result["recommendation"] = (
                f"Data may need updating. "
                f"Available providers: {', '.join(providers)}"
            )

        return result

    # ============================================================================
    # 基础数据查询方法
    # ============================================================================

    def get_daily(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取日线 OHLCV 数据（同步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 日线数据，包含 time, symbol, open, high, low, close, volume, amount, adj_factor 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_daily(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')
            >>> print(data.head())
        """
        return asyncio.run(self.get_daily_async(symbols, start_date, end_date))

    async def get_daily_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取日线 OHLCV 数据（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 日线数据
        """
        # 检查数据源可用性
        freshness = await self.check_data_freshness(symbols, "daily")
        if freshness["is_stale"]:
            self._log_routing_decision(
                "daily",
                symbols,
                "Query from PostgreSQL (current data)",
                f"Latest: {freshness.get('latest_date')}, Recommendation: {freshness.get('recommendation')}"
            )
        else:
            self._log_routing_decision(
                "daily",
                symbols,
                "Query from PostgreSQL",
                f"Available providers: {', '.join(freshness.get('available_providers', []))}"
            )

        return await self.ops.get_symbol_daily(symbols, start_date, end_date)

    def get_minute(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        frequency: str = "minute_1"
    ) -> Optional[pd.DataFrame]:
        """
        获取分钟级 OHLCV 数据（同步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率，支持 minute_1, minute_5, minute_15, minute_30, minute_60

        Returns:
            Optional[pd.DataFrame]: 分钟数据，包含 time, symbol, open, high, low, close, volume, amount, frequency 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_minute(['600519.SH'], '2024-11-01', '2024-11-30', 'minute_5')
            >>> print(data.head())
        """
        return asyncio.run(self.get_minute_async(symbols, start_date, end_date, frequency))

    async def get_minute_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        frequency: str = "minute_1"
    ) -> Optional[pd.DataFrame]:
        """
        获取分钟级 OHLCV 数据（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            frequency: 数据频率，支持 minute_1, minute_5, minute_15, minute_30, minute_60

        Returns:
            Optional[pd.DataFrame]: 分钟数据
        """
        # 检查数据源可用性
        freshness = await self.check_data_freshness(symbols, "minute", frequency)
        self._log_routing_decision(
            "minute",
            symbols,
            "Query from PostgreSQL",
            f"Frequency: {frequency}, Available providers: {', '.join(freshness.get('available_providers', []))}"
        )

        return await self.ops.get_symbol_minute(symbols, start_date, end_date, frequency)

    def get_daily_basic(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取每日基本面指标数据（同步方法）

        Args:
            symbols: 股票代码列表，None表示不限制股票
            start_date: 开始日期 (YYYY-MM-DD)，None表示从最早开始
            end_date: 结束日期 (YYYY-MM-DD)，None表示到最新

        Returns:
            Optional[pd.DataFrame]: 每日基本面数据，包含 time, symbol, turnover_rate, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv 列

        Note:
            symbols、start_date、end_date 不能同时为 None，否则返回空数据框

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> # 指定股票和日期范围
            >>> data = fdh.get_daily_basic(['600519.SH'], '2024-01-01', '2024-12-31')
            >>> # 仅指定股票，获取全部历史数据
            >>> data = fdh.get_daily_basic(['600519.SH'])
            >>> # 仅指定日期范围，获取所有股票在该范围内的数据
            >>> data = fdh.get_daily_basic(start_date='2024-01-01', end_date='2024-12-31')
            >>> print(data[['symbol', 'time', 'pe', 'pb']].head())
        """
        return asyncio.run(self.get_daily_basic_async(symbols, start_date, end_date))

    async def get_daily_basic_async(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取每日基本面指标数据（异步方法）

        Args:
            symbols: 股票代码列表，None表示不限制股票
            start_date: 开始日期 (YYYY-MM-DD)，None表示从最早开始
            end_date: 结束日期 (YYYY-MM-DD)，None表示到最新

        Returns:
            Optional[pd.DataFrame]: 每日基本面数据

        Note:
            symbols、start_date、end_date 不能同时为 None，否则返回空数据框
        """
        # 验证参数：不能三个同时为空
        if symbols is None and start_date is None and end_date is None:
            logger.warning("get_daily_basic: symbols, start_date, and end_date cannot all be None")
            return None

        freshness = await self.check_data_freshness(symbols if symbols else ["ALL"], "daily_basic")
        self._log_routing_decision(
            "daily_basic",
            symbols if symbols else ["ALL"],
            "Query from PostgreSQL",
            f"Available providers: {', '.join(freshness.get('available_providers', []))}"
        )

        # 处理空列表为None
        symbols_param = symbols if symbols else None
        return await self.ops.get_daily_basic(symbols_param, start_date, end_date)

    def get_adj_factor(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取复权因子数据（同步方法）

        Args:
            symbols: 股票代码列表，None表示不限制股票
            start_date: 开始日期 (YYYY-MM-DD)，None表示从最早开始
            end_date: 结束日期 (YYYY-MM-DD)，None表示到最新

        Returns:
            Optional[pd.DataFrame]: 复权因子数据，包含 time, symbol, adj_factor 列

        Note:
            symbols、start_date、end_date 不能同时为 None，否则返回空数据框

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> # 指定股票和日期范围
            >>> data = fdh.get_adj_factor(['600519.SH'], '2020-01-01', '2024-12-31')
            >>> # 仅指定股票，获取全部历史数据
            >>> data = fdh.get_adj_factor(['600519.SH'])
            >>> # 仅指定日期范围，获取所有股票在该范围内的数据
            >>> data = fdh.get_adj_factor(start_date='2020-01-01', end_date='2024-12-31')
            >>> print(data.head())
        """
        return asyncio.run(self.get_adj_factor_async(symbols, start_date, end_date))

    async def get_adj_factor_async(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取复权因子数据（异步方法）

        Args:
            symbols: 股票代码列表，None表示不限制股票
            start_date: 开始日期 (YYYY-MM-DD)，None表示从最早开始
            end_date: 结束日期 (YYYY-MM-DD)，None表示到最新

        Returns:
            Optional[pd.DataFrame]: 复权因子数据

        Note:
            symbols、start_date、end_date 不能同时为 None，否则返回空数据框
        """
        # 验证参数：不能三个同时为空
        if symbols is None and start_date is None and end_date is None:
            logger.warning("get_adj_factor: symbols, start_date, and end_date cannot all be None")
            return None

        freshness = await self.check_data_freshness(symbols if symbols else ["ALL"], "adj_factor")
        self._log_routing_decision(
            "adj_factor",
            symbols if symbols else ["ALL"],
            "Query from PostgreSQL",
            f"Available providers: {', '.join(freshness.get('available_providers', []))}"
        )

        # 处理空列表为None
        symbols_param = symbols if symbols else None
        return await self.ops.get_adj_factor(symbols_param, start_date, end_date)

    def get_basic(
        self,
        symbols: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取股票基本信息（同步方法，非时间序列）

        Args:
            symbols: 股票代码列表，如果为None则返回所有股票

        Returns:
            Optional[pd.DataFrame]: 股票基本信息，包含 ts_code, symbol, name, area, industry, market, exchange, list_status, list_date, delist_date, is_hs 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_basic(['600519.SH', '000858.SZ'])
            >>> print(data[['symbol', 'name', 'industry']])
        """
        return asyncio.run(self.get_basic_async(symbols))

    async def get_basic_async(
        self,
        symbols: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取股票基本信息（异步方法，非时间序列）

        Args:
            symbols: 股票代码列表，如果为None则返回所有股票

        Returns:
            Optional[pd.DataFrame]: 股票基本信息
        """
        if symbols is None:
            symbols = []

        freshness = await self.check_data_freshness(symbols if symbols else ["ALL"], "basic")
        self._log_routing_decision(
            "basic",
            symbols if symbols else ["ALL"],
            "Query from PostgreSQL",
            f"Available providers: {', '.join(freshness.get('available_providers', []))}"
        )

        return await self.ops.get_asset_basic(symbols)

    # ============================================================================
    # 高周期数据查询方法
    # ============================================================================

    def get_weekly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的 OHLCV 数据（同步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线数据，包含 time, symbol, open, high, low, close, volume, amount, adj_factor 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_weekly(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')
            >>> print(data.head())
        """
        return asyncio.run(self.get_weekly_async(symbols, start_date, end_date))

    async def get_weekly_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的 OHLCV 数据（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线数据
        """
        return await self.ops.get_weekly_data(symbols, start_date, end_date)

    def get_monthly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的 OHLCV 数据（同步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线数据，包含 time, symbol, open, high, low, close, volume, amount, adj_factor 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_monthly(['600519.SH'], '2020-01-01', '2024-12-31')
            >>> print(data.head())
        """
        return asyncio.run(self.get_monthly_async(symbols, start_date, end_date))

    async def get_monthly_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的 OHLCV 数据（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线数据
        """
        return await self.ops.get_monthly_data(symbols, start_date, end_date)

    def get_daily_basic_weekly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的每日基础指标（同步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线基础指标数据，包含聚合后的各种指标列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_daily_basic_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
            >>> print(data[['symbol', 'time', 'avg_pe', 'avg_pb']].head())
        """
        return asyncio.run(self.get_daily_basic_weekly_async(symbols, start_date, end_date))

    async def get_daily_basic_weekly_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的每日基础指标（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线基础指标数据
        """
        return await self.ops.get_daily_basic_weekly(symbols, start_date, end_date)

    def get_daily_basic_monthly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的每日基础指标（同步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线基础指标数据，包含聚合后的各种指标列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_daily_basic_monthly(['000858.SZ'], '2020-01-01', '2024-12-31')
            >>> print(data[['symbol', 'time', 'avg_turnover_rate']].head())
        """
        return asyncio.run(self.get_daily_basic_monthly_async(symbols, start_date, end_date))

    async def get_daily_basic_monthly_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的每日基础指标（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线基础指标数据
        """
        return await self.ops.get_daily_basic_monthly(symbols, start_date, end_date)

    # ============================================================================
    # 复权因子高周期聚合查询
    # ============================================================================

    def get_adj_factor_weekly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的复权因子数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线复权因子数据，包含 time, symbol, adj_factor 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_adj_factor_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
            >>> print(data.head())
        """
        return asyncio.run(self.get_adj_factor_weekly_async(symbols, start_date, end_date))

    async def get_adj_factor_weekly_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取周线聚合的复权因子数据（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 周线复权因子数据
        """
        return await self.ops.get_adj_factor_weekly(symbols, start_date, end_date)

    def get_adj_factor_monthly(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的复权因子数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线复权因子数据，包含 time, symbol, adj_factor 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_adj_factor_monthly(['600519.SH'], '2020-01-01', '2024-12-31')
            >>> print(data.head())
        """
        return asyncio.run(self.get_adj_factor_monthly_async(symbols, start_date, end_date))

    async def get_adj_factor_monthly_async(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        获取月线聚合的复权因子数据（异步方法）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Optional[pd.DataFrame]: 月线复权因子数据
        """
        return await self.ops.get_adj_factor_monthly(symbols, start_date, end_date)

    # ============================================================================
    # 宏观经济数据查询方法
    # ============================================================================

    def get_cn_gdp(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国GDP宏观经济数据（同步方法）

        Args:
            start_date: 开始日期（季度末日期格式，如 '2020-03-31' 表示2020Q1），None表示从最早开始
            end_date: 结束日期（季度末日期格式，如 '2024-12-31' 表示2024Q4），None表示到最新

        Returns:
            Optional[pd.DataFrame]: GDP数据，包含 time, quarter, gdp, gdp_yoy, pi, pi_yoy, si, si_yoy, ti, ti_yoy 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_cn_gdp('2020-03-31', '2024-12-31')
            >>> print(data)
        """
        return asyncio.run(self.get_cn_gdp_async(start_date, end_date))

    async def get_cn_gdp_async(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国GDP宏观经济数据（异步方法）

        Args:
            start_date: 开始日期（季度末日期格式，如 '2020-03-31'），None表示从最早开始
            end_date: 结束日期（季度末日期格式，如 '2024-12-31'），None表示到最新

        Returns:
            Optional[pd.DataFrame]: GDP数据
        """
        return await self.ops.get_cn_gdp(start_date, end_date)

    def get_cn_ppi(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国PPI工业生产者出厂价格指数数据（同步方法）

        Args:
            start_date: 开始日期（月份末日期格式，如 '2020-01-31' 表示2020年1月），None表示从最早开始
            end_date: 结束日期（月份末日期格式，如 '2024-12-31' 表示2024年12月），None表示到最新

        Returns:
            Optional[pd.DataFrame]: PPI数据，包含 time, month 及所有PPI指标字段

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_cn_ppi('2020-01-31', '2024-12-31')
            >>> print(data)
        """
        return asyncio.run(self.get_cn_ppi_async(start_date, end_date))

    async def get_cn_ppi_async(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国PPI工业生产者出厂价格指数数据（异步方法）

        Args:
            start_date: 开始日期（月份末日期格式，如 '2020-01-31'），None表示从最早开始
            end_date: 结束日期（月份末日期格式，如 '2024-12-31'），None表示到最新

        Returns:
            Optional[pd.DataFrame]: PPI数据
        """
        return await self.ops.get_cn_ppi(start_date, end_date)

    def get_cn_m(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国货币供应量数据M0、M1、M2（同步方法）

        Args:
            start_date: 开始日期（月份末日期格式，如 '2020-01-31' 表示2020年1月），None表示从最早开始
            end_date: 结束日期（月份末日期格式，如 '2024-12-31' 表示2024年12月），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 货币供应量数据，包含 time, month 及所有指标字段

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_cn_m('2020-01-31', '2024-12-31')
            >>> print(data)
        """
        return asyncio.run(self.get_cn_m_async(start_date, end_date))

    async def get_cn_m_async(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国货币供应量数据M0、M1、M2（异步方法）

        Args:
            start_date: 开始日期（月份末日期格式，如 '2020-01-31'），None表示从最早开始
            end_date: 结束日期（月份末日期格式，如 '2024-12-31'），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 货币供应量数据
        """
        return await self.ops.get_cn_m(start_date, end_date)

    def get_cn_pmi(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国PMI采购经理人指数数据（同步方法）

        Args:
            start_date: 开始日期（月份末日期格式，如 '2020-01-31' 表示2020年1月），None表示从最早开始
            end_date: 结束日期（月份末日期格式，如 '2024-12-31' 表示2024年12月），None表示到最新

        Returns:
            Optional[pd.DataFrame]: PMI数据，包含 time, month 及所有PMI指标字段（制造业PMI、非制造业PMI、综合PMI等32个指标）

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_cn_pmi('2020-01-31', '2024-12-31')
            >>> print(data)
        """
        return asyncio.run(self.get_cn_pmi_async(start_date, end_date))

    async def get_cn_pmi_async(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取中国PMI采购经理人指数数据（异步方法）

        Args:
            start_date: 开始日期（月份末日期格式，如 '2020-01-31'），None表示从最早开始
            end_date: 结束日期（月份末日期格式，如 '2024-12-31'），None表示到最新

        Returns:
            Optional[pd.DataFrame]: PMI数据，包含32个PMI指标字段
        """
        return await self.ops.get_cn_pmi(start_date, end_date)

    def get_index_dailybasic(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取大盘指数每日指标数据（同步方法）

        Args:
            ts_code: 指数代码（如 '000001.SH' 上证综指，'399001.SZ' 深证成指），None表示所有指数
            start_date: 开始日期（YYYY-MM-DD格式），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 指数每日指标数据，包含 ts_code, trade_date, total_mv, float_mv, total_share, float_share, free_share, turnover_rate, turnover_rate_f, pe, pe_ttm, pb 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_index_dailybasic('000001.SH', '2024-01-01', '2024-12-31')
            >>> print(data[['trade_date', 'total_mv', 'pe', 'pb']])
        """
        return asyncio.run(self.get_index_dailybasic_async(ts_code, start_date, end_date))

    async def get_index_dailybasic_async(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取大盘指数每日指标数据（异步方法）

        Args:
            ts_code: 指数代码（如 '000001.SH' 上证综指），None表示所有指数
            start_date: 开始日期（YYYY-MM-DD格式），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 指数每日指标数据
        """
        return await self.ops.get_index_dailybasic(ts_code, start_date, end_date)

    def get_sw_daily(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业日线行情数据（同步方法）

        Args:
            ts_code: 行业代码（如 '801780.SI' 申万农林牧渔），None表示所有行业
            start_date: 开始日期（YYYY-MM-DD格式），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 申万行业日线行情数据，包含 ts_code, trade_date, name, open, high, low, close, change, pct_change, vol, amount, pe, pb, float_mv, total_mv 列

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_sw_daily('2024-01-01', '2024-12-31')
            >>> print(data[['trade_date', 'ts_code', 'name', 'close', 'pct_change']])
        """
        return asyncio.run(self.get_sw_daily_async(ts_code, start_date, end_date))

    async def get_sw_daily_async(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业日线行情数据（异步方法）

        Args:
            ts_code: 行业代码（如 '801780.SI'），None表示所有行业
            start_date: 开始日期（YYYY-MM-DD格式），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 申万行业日线行情数据
        """
        return await self.ops.get_sw_daily(ts_code, start_date, end_date)

    def get_fina_indicator(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司财务指标数据（同步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH' 上证综指），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 财务指标数据，包含 ts_code, end_date_time 及所有指标字段

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_fina_indicator('600519.SH', '2020-01-01', '2024-12-31')
            >>> print(data[['ts_code', 'end_date', 'eps', 'roe', 'debt_to_assets']])
        """
        return asyncio.run(self.get_fina_indicator_async(ts_code, start_date, end_date))

    async def get_fina_indicator_async(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司财务指标数据（异步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 财务指标数据
        """
        return await self.ops.get_fina_indicator(ts_code, start_date, end_date)

    def get_cashflow(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司现金流量表数据（同步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 现金流量表数据，包含 ts_code, end_date_time 及所有现金流量指标字段

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_cashflow('600519.SH', '2020-03-31', '2024-12-31')
            >>> print(data[['ts_code', 'end_date', 'net_profit', 'n_cashflow_act']])
        """
        return asyncio.run(self.get_cashflow_async(ts_code, start_date, end_date))

    async def get_cashflow_async(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司现金流量表数据（异步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 现金流量表数据
        """
        return await self.ops.get_cashflow(ts_code, start_date, end_date)

    def get_balancesheet(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司资产负债表数据（同步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 资产负债表数据，包含 ts_code, end_date_time 及所有资产负债表字段

        Example:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_balancesheet('600519.SH', '2020-03-31', '2024-12-31')
            >>> print(data[['ts_code', 'end_date', 'total_assets', 'total_liab']])
        """
        return asyncio.run(self.get_balancesheet_async(ts_code, start_date, end_date))

    async def get_balancesheet_async(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司资产负债表数据（异步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 资产负债表数据
        """
        return await self.ops.get_balancesheet(ts_code, start_date, end_date)

    # ============================================================================
    # 利润表数据查询
    # ============================================================================

    def get_income(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司利润表数据（同步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 利润表数据

        Examples:
            >>> fdh = FinanceDataHub(settings)
            >>> data = fdh.get_income('600519.SH', '2020-03-31', '2024-12-31')
            >>> print(data[['ts_code', 'end_date', 'total_revenue', 'n_income']])
        """
        return asyncio.run(self.get_income_async(ts_code, start_date, end_date))

    async def get_income_async(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取上市公司利润表数据（异步方法）

        Args:
            ts_code: 股票代码（如 '600519.SH'），None表示所有股票
            start_date: 开始日期（YYYY-MM-DD格式，报告期），None表示从最早开始
            end_date: 结束日期（YYYY-MM-DD格式，报告期），None表示到最新

        Returns:
            Optional[pd.DataFrame]: 利润表数据
        """
        return await self.ops.get_income(ts_code, start_date, end_date)

    # ============================================================================
    # 申万行业数据查询方法
    # ============================================================================

    def get_sw_industry_classify(
        self,
        level: str = "L1",
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业分类

        Args:
            level: 行业层级 (L1/L2/L3)

        Returns:
            Optional[pd.DataFrame]: 申万行业分类数据

        Examples:
            >>> fdh = FinanceDataHub(settings)
            >>> classify = fdh.get_sw_industry_classify(level="L1")
            >>> print(classify[['industry_code', 'industry_name', 'level']])
        """
        return asyncio.run(self.get_sw_industry_classify_async(level))

    async def get_sw_industry_classify_async(
        self,
        level: str = "L1",
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业分类（异步方法）

        Args:
            level: 行业层级 (L1/L2/L3)

        Returns:
            Optional[pd.DataFrame]: 申万行业分类数据
        """
        return await self.ops.get_sw_industry_classify(level)

    def get_sw_industry_members(
        self,
        l1_code: Optional[str] = None,
        l2_code: Optional[str] = None,
        l3_code: Optional[str] = None,
        ts_code: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业成分股

        Args:
            l1_code: 一级行业代码
            l2_code: 二级行业代码
            l3_code: 三级行业代码
            ts_code: 股票代码，如 '600519.SH'，用于查询股票所属的行业

        Returns:
            Optional[pd.DataFrame]: 申万行业成分股数据

        Examples:
            >>> fdh = FinanceDataHub(settings)
            >>> # 获取某一级行业下的所有成分股
            >>> members = fdh.get_sw_industry_members(l1_code="801010")
            >>> # 获取某三级行业下的成分股
            >>> members = fdh.get_sw_industry_members(l3_code="801010.SI")
            >>> # 查询股票所属行业
            >>> members = fdh.get_sw_industry_members(ts_code="600519.SH")
        """
        return asyncio.run(self.get_sw_industry_members_async(l1_code, l2_code, l3_code, ts_code))

    async def get_sw_industry_members_async(
        self,
        l1_code: Optional[str] = None,
        l2_code: Optional[str] = None,
        l3_code: Optional[str] = None,
        ts_code: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        获取申万行业成分股（异步方法）

        Args:
            l1_code: 一级行业代码
            l2_code: 二级行业代码
            l3_code: 三级行业代码
            ts_code: 股票代码，如 '600519.SH'，用于查询股票所属的行业

        Returns:
            Optional[pd.DataFrame]: 申万行业成分股数据
        """
        return await self.ops.get_sw_industry_members(l1_code, l2_code, l3_code, ts_code)

    # ============================================================================
    # 资源管理
    # ============================================================================

    async def close(self) -> None:
        """关闭数据库连接"""
        await self.db_manager.close()

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()

    def __enter__(self):
        """同步上下文管理器入口（不推荐使用，推荐使用异步版本）"""
        if not hasattr(self, '_initialized'):
            asyncio.run(self.initialize())
            self._initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """同步上下文管理器出口"""
        asyncio.run(self.close())
