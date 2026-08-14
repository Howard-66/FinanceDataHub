from datetime import date
import asyncio
from collections import namedtuple

import pandas as pd

from finance_data_hub.database.operations import DataOperations
from tests.unit.test_index_basic_storage import _DatabaseManager, _Engine, _Transaction


def test_fund_benchmark_and_portfolio_upserts():
    asyncio.run(_test_fund_benchmark_and_portfolio_upserts())


async def _test_fund_benchmark_and_portfolio_upserts():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))

    count = await operations.insert_mkt_idx_bmk_batch(pd.DataFrame({
        "ts_code": ["000300.SH"], "name": ["沪深300"], "bmk_level": ["一类库"],
    }))
    assert count == 1
    assert "INSERT INTO mkt_idx_bmk" in str(transaction.statement)
    assert "ON CONFLICT (ts_code)" in str(transaction.statement)

    count = await operations.insert_fund_portfolio_batch(pd.DataFrame({
        "ts_code": ["001753.OF"], "ann_date": [pd.Timestamp("2024-08-23")],
        "end_date": [pd.Timestamp("2024-06-30")], "symbol": ["603019.SH"],
        "mkv": [3130994.46],
    }))
    assert count == 1
    assert "INSERT INTO fund_portfolio" in str(transaction.statement)
    assert "ON CONFLICT (ts_code, ann_date, end_date, symbol)" in str(transaction.statement)
    assert transaction.records[0]["ann_date"] == date(2024, 8, 23)


async def _test_fund_portfolio_date_checkpoints():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))
    row_type = namedtuple("Row", ["latest_date"])
    transaction.result = type("Result", (), {"fetchone": lambda self: row_type(date(2024, 8, 23))})()

    latest = await operations.get_latest_fund_portfolio_ann_date()

    assert latest == "2024-08-23"
    assert "MAX(ann_date)" in str(transaction.statement)

    earliest_row_type = namedtuple("EarliestRow", ["earliest_date"])
    transaction.result = type(
        "Result", (), {"fetchone": lambda self: earliest_row_type(date(1998, 1, 5))}
    )()
    earliest = await operations.get_earliest_trade_cal_date(
        exchange="SSE", start_date="1998-01-01"
    )

    assert earliest == "1998-01-05"
    assert "MIN((cal_date AT TIME ZONE 'Asia/Shanghai')::date)" in str(transaction.statement)
    assert transaction.records["exchange"] == "SSE"
    assert transaction.records["start_date"] == date(1998, 1, 1)


def test_fund_portfolio_date_checkpoints():
    asyncio.run(_test_fund_portfolio_date_checkpoints())
