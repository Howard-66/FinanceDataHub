from datetime import date

import pandas as pd
import pytest
from sqlalchemy.sql.elements import TextClause

from finance_data_hub.database.operations import DataOperations


class _Transaction:
    def __init__(self):
        self.statement = None
        self.records = None
        self.result = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None

    async def execute(self, statement, records=None):
        self.statement = statement
        self.records = records
        return self.result


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Engine:
    def __init__(self, transaction):
        self.transaction = transaction

    def begin(self):
        return self.transaction


class _DatabaseManager:
    def __init__(self, engine):
        self._engine = engine


@pytest.mark.asyncio
async def test_insert_index_basic_batch_upserts_tushare_fields_and_nulls():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))
    data = pd.DataFrame(
        {
            "ts_code": ["000300.SH"],
            "name": ["沪深300"],
            "market": ["SSE"],
            "base_date": [pd.Timestamp("2004-12-31")],
            "base_point": [1000.0],
            "list_date": [pd.Timestamp("2005-04-08")],
            "exp_date": [pd.NaT],
        }
    )

    result = await operations.insert_index_basic_batch(data)

    assert result == 1
    assert isinstance(transaction.statement, TextClause)
    assert "ON CONFLICT (ts_code) DO UPDATE" in str(transaction.statement)
    record = transaction.records[0]
    assert record["base_date"] == date(2004, 12, 31)
    assert record["list_date"] == date(2005, 4, 8)
    assert record["exp_date"] is None
    assert record["desc"] is None


@pytest.mark.asyncio
async def test_get_index_basic_codes_filters_excluded_and_expired_catalog_entries():
    transaction = _Transaction()
    transaction.result = _Result(
        [
            ("000300.CSI",),
            ("000905.CSI",),
            ("000300.CSI",),
        ]
    )
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))

    result = await operations.get_index_basic_codes(
        exclude_markets=["SW"],
        active_date="2026-07-25",
    )

    assert result == ["000300.CSI", "000905.CSI"]
    statement = str(transaction.statement)
    assert "FROM index_basic" in statement
    assert "UPPER(market) = ANY(:exclude_markets)" in statement
    assert "exp_date IS NULL OR exp_date >= :active_date" in statement
    assert transaction.records["exclude_markets"] == ["SW"]
    assert transaction.records["active_date"] == date(2026, 7, 25)


@pytest.mark.asyncio
async def test_get_earliest_index_basic_list_date_uses_local_catalog_filters():
    transaction = _Transaction()
    transaction.result = _Result([(date(2005, 4, 8),)])
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))

    result = await operations.get_earliest_index_basic_list_date(
        exclude_markets=["SW"],
        active_date="2026-07-25",
    )

    assert result == "2005-04-08"
    statement = str(transaction.statement)
    assert "MIN(list_date)" in statement
    assert "exp_date IS NULL OR exp_date >= :active_date" in statement
    assert transaction.records["exclude_markets"] == ["SW"]
    assert transaction.records["active_date"] == date(2026, 7, 25)
