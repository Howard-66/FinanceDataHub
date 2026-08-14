from datetime import date
from collections import namedtuple
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from finance_data_hub.cli.main import app
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.providers.tushare import ETF_BASIC_MAX_RECORDS, TushareProvider
from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.update.updater import DataUpdater
from tests.unit.test_index_basic_storage import (
    _DatabaseManager,
    _Engine,
    _Result,
    _Transaction,
)


runner = CliRunner()


def test_etf_basic_requests_all_fields_and_paginates_with_offset():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": [
                        f"{index:06d}.SH" for index in range(ETF_BASIC_MAX_RECORDS)
                    ],
                    "list_date": ["20240102"] * ETF_BASIC_MAX_RECORDS,
                }
            ),
            pd.DataFrame({"ts_code": ["999999.SZ"], "list_date": ["20240103"]}),
        ]
    )

    result = provider.get_etf_basic(
        list_date="2024-01-02", list_status="L", exchange="SH", mgr="华夏基金"
    )

    assert len(result) == ETF_BASIC_MAX_RECORDS + 1
    first_call, second_call = provider._call_api.call_args_list
    assert first_call.args[0] == "etf_basic"
    assert first_call.kwargs["offset"] == 0
    assert second_call.kwargs["offset"] == ETF_BASIC_MAX_RECORDS
    assert first_call.kwargs["list_date"] == "20240102"
    assert first_call.kwargs["list_status"] == "L"
    assert first_call.kwargs["exchange"] == "SH"
    assert first_call.kwargs["mgr"] == "华夏基金"
    assert set(first_call.kwargs["fields"].split(",")) == {
        "ts_code",
        "csname",
        "extname",
        "cname",
        "index_code",
        "index_name",
        "setup_date",
        "list_date",
        "list_status",
        "exchange",
        "mgr_name",
        "custod_name",
        "mgt_fee",
        "etf_type",
    }


@pytest.mark.asyncio
async def test_update_etf_basic_routes_filters_and_persists():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(
        route=Mock(return_value=pd.DataFrame({"ts_code": ["510300.SH"]}))
    )
    updater.data_ops = Mock(insert_etf_basic_batch=AsyncMock(return_value=1))

    count = await updater.update_etf_basic(
        index_code="000300.SH", list_status="L", exchange="SH", mgr="华夏基金"
    )

    assert count == 1
    updater.router.route.assert_called_once_with(
        asset_class="fund",
        data_type="etf_basic",
        method_name="get_etf_basic",
        ts_code=None,
        index_code="000300.SH",
        list_date=None,
        list_status="L",
        exchange="SH",
        mgr="华夏基金",
    )
    updater.data_ops.insert_etf_basic_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_etf_basic_storage_preserves_all_fields_and_upserts():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))

    count = await operations.insert_etf_basic_batch(
        pd.DataFrame(
            {
                "ts_code": ["510300.SH"],
                "csname": ["300ETF"],
                "extname": ["沪深300ETF"],
                "cname": ["华泰柏瑞沪深300交易型开放式指数证券投资基金"],
                "index_code": ["000300.SH"],
                "index_name": ["沪深300指数"],
                "setup_date": [pd.Timestamp("2012-05-04")],
                "list_date": [pd.Timestamp("2012-05-28")],
                "list_status": ["L"],
                "exchange": ["SH"],
                "mgr_name": ["华泰柏瑞基金"],
                "custod_name": ["工商银行"],
                "mgt_fee": [0.5],
                "etf_type": ["境内"],
            }
        )
    )

    assert count == 1
    statement = str(transaction.statement)
    assert "INSERT INTO etf_basic" in statement
    assert "ON CONFLICT (ts_code) DO UPDATE" in statement
    assert transaction.records[0]["setup_date"] == date(2012, 5, 4)
    assert transaction.records[0]["list_date"] == date(2012, 5, 28)
    assert set(transaction.records[0]) == {
        "ts_code",
        "csname",
        "extname",
        "cname",
        "index_code",
        "index_name",
        "setup_date",
        "list_date",
        "list_status",
        "exchange",
        "mgr_name",
        "custod_name",
        "mgt_fee",
        "etf_type",
    }

    row_type = namedtuple(
        "EtfBasicRow",
        [
            "ts_code",
            "csname",
            "extname",
            "cname",
            "index_code",
            "index_name",
            "setup_date",
            "list_date",
            "list_status",
            "exchange",
            "mgr_name",
            "custod_name",
            "mgt_fee",
            "etf_type",
        ],
    )
    transaction.result = _Result(
        [
            row_type(
                "510300.SH",
                "300ETF",
                "沪深300ETF",
                "沪深300ETF全称",
                "000300.SH",
                "沪深300指数",
                date(2012, 5, 4),
                date(2012, 5, 28),
                "L",
                "SH",
                "华泰柏瑞基金",
                "工商银行",
                0.5,
                "境内",
            )
        ]
    )

    result = await operations.get_etf_basic(
        index_code="000300.SH", list_date="2012-05-28", exchange="SH"
    )

    assert result is not None
    assert result.iloc[0]["ts_code"] == "510300.SH"
    assert "FROM etf_basic" in str(transaction.statement)
    assert transaction.records["list_date"] == date(2012, 5, 28)


@pytest.mark.asyncio
async def test_etf_basic_sdk_delegates_all_filters():
    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock(
        get_etf_basic=AsyncMock(return_value=pd.DataFrame({"ts_code": ["510300.SH"]}))
    )

    result = await fdh.get_etf_basic_async(
        index_code="000300.SH", list_status="L", exchange="SH", etf_type="境内"
    )

    assert result is not None
    fdh.ops.get_etf_basic.assert_awaited_once_with(
        ts_code=None,
        index_code="000300.SH",
        list_date=None,
        list_status="L",
        exchange="SH",
        mgr_name=None,
        etf_type="境内",
    )


def test_cli_update_etf_basic_defaults_to_full_and_supports_one_code():
    fake_updater = Mock(update_etf_basic=AsyncMock(return_value=1))
    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(app, ["update", "--dataset", "etf_basic"])
        assert result.exit_code == 0
        fake_updater.update_etf_basic.assert_awaited_once_with(ts_code=None)

        fake_updater.update_etf_basic.reset_mock()
        result = runner.invoke(
            app, ["update", "--dataset", "etf_basic", "--symbols", "510300.SH"]
        )

    assert result.exit_code == 0
    fake_updater.update_etf_basic.assert_awaited_once_with(ts_code="510300.SH")


def test_cli_update_etf_basic_rejects_date_arguments():
    result = runner.invoke(
        app, ["update", "--dataset", "etf_basic", "--trade-date", "2024-01-02"]
    )

    assert result.exit_code != 0
    assert "etf_basic 是非时间序列数据" in result.output
