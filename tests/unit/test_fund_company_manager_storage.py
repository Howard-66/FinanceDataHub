from datetime import date

import pandas as pd
import pytest
from sqlalchemy.sql.elements import TextClause

from finance_data_hub.database.operations import DataOperations
from tests.unit.test_index_basic_storage import _DatabaseManager, _Engine, _Transaction


@pytest.mark.asyncio
async def test_insert_fund_company_batch_upserts_all_fields_and_dates():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))
    data = pd.DataFrame(
        {
            "name": ["示例基金管理有限公司"],
            "shortname": ["示例基金"],
            "setup_date": [pd.Timestamp("2001-01-01")],
            "end_date": [pd.NaT],
            "reg_capital": [100.0],
            "credit_code": ["91110000EXAMPLE"],
        }
    )

    result = await operations.insert_fund_company_batch(data)

    assert result == 1
    assert isinstance(transaction.statement, TextClause)
    assert "INSERT INTO fund_company" in str(transaction.statement)
    assert "ON CONFLICT (name) DO UPDATE" in str(transaction.statement)
    assert transaction.records[0]["setup_date"] == date(2001, 1, 1)
    assert transaction.records[0]["end_date"] is None
    assert transaction.records[0]["main_business"] is None


@pytest.mark.asyncio
async def test_insert_fund_manager_batch_uses_documented_composite_identity():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))
    data = pd.DataFrame(
        {
            "ts_code": ["150018.SZ"],
            "ann_date": [pd.Timestamp("2010-05-08")],
            "name": ["周毅"],
            "begin_date": [pd.Timestamp("2010-05-07")],
            "end_date": [pd.NaT],
            "resume": ["基金经理简历"],
        }
    )

    result = await operations.insert_fund_manager_batch(data)

    assert result == 1
    assert "INSERT INTO fund_manager" in str(transaction.statement)
    assert "ON CONFLICT (ts_code, ann_date, name, begin_date) DO UPDATE" in str(
        transaction.statement
    )
    assert transaction.records[0]["ann_date"] == date(2010, 5, 8)
    assert transaction.records[0]["end_date"] is None
    assert transaction.records[0]["gender"] is None
