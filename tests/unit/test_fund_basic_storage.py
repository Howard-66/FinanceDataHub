from datetime import date

import pandas as pd
import pytest
from sqlalchemy.sql.elements import TextClause

from finance_data_hub.database.operations import DataOperations
from tests.unit.test_index_basic_storage import (
    _DatabaseManager,
    _Engine,
    _Result,
    _Transaction,
)


@pytest.mark.asyncio
async def test_insert_fund_basic_batch_upserts_all_tushare_fields_and_nulls():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))
    data = pd.DataFrame(
        {
            "ts_code": ["510300.SH"],
            "name": ["沪深300ETF"],
            "management": ["华泰柏瑞基金"],
            "found_date": [pd.Timestamp("2012-05-04")],
            "due_date": [pd.NaT],
            "issue_amount": [30.5],
            "benchmark": ["沪深300指数收益率"],
            "status": ["L"],
            "market": ["E"],
        }
    )

    result = await operations.insert_fund_basic_batch(data)

    assert result == 1
    assert isinstance(transaction.statement, TextClause)
    assert "INSERT INTO fund_basic" in str(transaction.statement)
    assert "ON CONFLICT (ts_code) DO UPDATE" in str(transaction.statement)
    record = transaction.records[0]
    assert record["found_date"] == date(2012, 5, 4)
    assert record["due_date"] is None
    assert record["benchmark"] == "沪深300指数收益率"
    assert record["redm_startdate"] is None


@pytest.mark.asyncio
async def test_get_earliest_fund_basic_date_uses_found_list_and_issue_date():
    transaction = _Transaction()
    transaction.result = _Result([(date(2002, 9, 1),)])
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))

    result = await operations.get_earliest_fund_basic_date()

    assert result == "2002-09-01"
    statement = str(transaction.statement)
    assert "MIN(COALESCE(found_date, list_date, issue_date))" in statement
    assert "FROM fund_basic" in statement
