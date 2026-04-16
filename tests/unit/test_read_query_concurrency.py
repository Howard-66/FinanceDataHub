"""
读查询并发限流测试
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.database.manager import (
    DatabaseManager,
    ReadQueryExecutionError,
    ReadQueryOptions,
)
from finance_data_hub.preprocessing.storage import IndustryValuationStorage


class FakeResult:
    """简化版 SQLAlchemy Result。"""

    def __init__(self, rows=None, columns=None):
        self._rows = rows or []
        self._columns = columns or ["value"]

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def keys(self):
        return self._columns


class TrackingConnection:
    """可跟踪并发度的假连接。"""

    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def execute(self, *_args, **_kwargs):
        self.engine.inflight += 1
        self.engine.max_inflight = max(self.engine.max_inflight, self.engine.inflight)
        try:
            await asyncio.sleep(self.engine.delay)
            if self.engine.should_fail:
                raise RuntimeError("synthetic query failure")
            return FakeResult(rows=[(1,)], columns=["value"])
        finally:
            self.engine.inflight -= 1


class TrackingEngine:
    """返回可跟踪连接的假引擎。"""

    def __init__(self, delay=0.02, should_fail=False):
        self.delay = delay
        self.should_fail = should_fail
        self.inflight = 0
        self.max_inflight = 0

    def begin(self):
        return TrackingConnection(self)


def make_settings(query_limit: int, heavy_limit: int):
    """创建最小可用 settings mock。"""
    settings = Mock()
    settings.database.url = "postgresql://test:test@localhost:5432/test_db"
    settings.database.pool_size = 5
    settings.database.max_overflow = 5
    settings.database.query_max_concurrency = query_limit
    settings.database.heavy_query_max_concurrency = heavy_limit
    return settings


@pytest.fixture(autouse=True)
def clear_query_limiters():
    """避免测试间共享 semaphore 状态。"""
    DatabaseManager._read_query_limiters.clear()
    yield
    DatabaseManager._read_query_limiters.clear()


def test_execute_raw_sql_limits_normal_query_concurrency():
    """普通查询应受 query_max_concurrency 限制。"""
    async def run_test():
        manager = DatabaseManager(make_settings(query_limit=2, heavy_limit=1))
        manager._engine = TrackingEngine()

        await asyncio.gather(
            *[
                manager.execute_raw_sql(
                    "SELECT 1",
                    options=ReadQueryOptions(query_type="normal.query"),
                )
                for _ in range(6)
            ]
        )

        assert manager._engine.max_inflight == 2

    asyncio.run(run_test())


def test_execute_raw_sql_limits_heavy_query_concurrency():
    """重查询应额外受 heavy_query_max_concurrency 限制。"""
    async def run_test():
        manager = DatabaseManager(make_settings(query_limit=4, heavy_limit=1))
        manager._engine = TrackingEngine()

        await asyncio.gather(
            *[
                manager.execute_raw_sql(
                    "SELECT 1",
                    options=ReadQueryOptions(
                        query_type="processed_industry_valuation.query",
                        heavy=True,
                    ),
                )
                for _ in range(6)
            ]
        )

        assert manager._engine.max_inflight == 1

    asyncio.run(run_test())


def test_industry_valuation_query_marks_heavy_options():
    """行业估值查询应走 heavy 限流分类。"""
    async def run_test():
        db_manager = Mock()
        result = FakeResult(
            rows=[("2024-01-01", "000001.SZ", "银行")],
            columns=["time", "symbol", "l2_name"],
        )
        db_manager.execute_raw_sql = AsyncMock(return_value=result)
        storage = IndustryValuationStorage(db_manager)

        df = await storage.query(
            symbols=["000001.SZ"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        call = db_manager.execute_raw_sql.await_args
        options = call.kwargs["options"]
        assert options.heavy is True
        assert options.query_type == "processed_industry_valuation.query"
        assert options.symbols_count == 1

    asyncio.run(run_test())


def test_industry_valuation_query_does_not_swallow_failures():
    """数据库读失败不应被吞掉为 None 或空结果。"""
    async def run_test():
        db_manager = Mock()
        db_manager.execute_raw_sql = AsyncMock(
            side_effect=ReadQueryExecutionError(
                "processed_industry_valuation.query",
                RuntimeError("db exploded"),
            )
        )
        storage = IndustryValuationStorage(db_manager)

        with pytest.raises(ReadQueryExecutionError):
            await storage.query(symbols=["000001.SZ"])

    asyncio.run(run_test())
