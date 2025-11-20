"""
FinanceDataHub SDK

提供统一的金融数据访问接口，支持日线、分钟线以及高周期（周线、月线）数据查询。
"""

import asyncio
from typing import List, Optional
import pandas as pd

from finance_data_hub.config import Settings
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import DataOperations


class FinanceDataHub:
    """
    FinanceDataHub SDK - 金融数据服务中心

    提供统一的金融数据访问接口，支持高周期数据聚合查询。
    """

    def __init__(self, settings: Settings, backend: str = "auto"):
        """
        初始化 FinanceDataHub SDK

        Args:
            settings: 应用配置
            backend: 数据后端 ('postgresql', 'duckdb', 'auto')
        """
        self.settings = settings
        self.db_manager = DatabaseManager(settings)
        self.ops = DataOperations(self.db_manager)
        self.backend = backend

    async def initialize(self) -> None:
        """初始化数据库连接"""
        await self.db_manager.initialize()

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
