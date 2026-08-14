from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from finance_data_hub.providers.schema import (
    EtfIndexSchema, FundDailySchema, FundAdjSchema, EtfShareSizeSchema,
    EtfShConsSchema, EtfSzConsSchema, IdxAnnsSchema,
)
from finance_data_hub.providers.tushare import (
    ETF_INDEX_MAX_RECORDS, FUND_DAILY_MAX_RECORDS, FUND_ADJ_MAX_RECORDS,
    ETF_SHARE_SIZE_MAX_RECORDS, ETF_CONS_MAX_RECORDS, IDX_ANNS_MAX_RECORDS,
    TushareProvider,
)
from finance_data_hub.update.updater import DataUpdater
from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.cli.main import app
from typer.testing import CliRunner


runner = CliRunner()


@pytest.mark.parametrize(
    "method_name,kwargs,api_name,schema,max_records,key_columns",
    [
        ("get_etf_index", {}, "etf_index", EtfIndexSchema, ETF_INDEX_MAX_RECORDS,
         ["ts_code"]),
        ("get_fund_daily", {"trade_date": "2024-01-02"}, "fund_daily",
         FundDailySchema, FUND_DAILY_MAX_RECORDS, ["ts_code", "trade_date"]),
        ("get_fund_adj", {"trade_date": "2024-01-02"}, "fund_adj",
         FundAdjSchema, FUND_ADJ_MAX_RECORDS, ["ts_code", "trade_date"]),
        ("get_etf_share_size", {"trade_date": "2024-01-02"}, "etf_share_size",
         EtfShareSizeSchema, ETF_SHARE_SIZE_MAX_RECORDS, ["ts_code", "trade_date"]),
        ("get_etf_sh_cons", {"trade_date": "2024-01-02"}, "etf_sh_cons",
         EtfShConsSchema, ETF_CONS_MAX_RECORDS, ["trade_date", "ts_code", "con_code"]),
        ("get_etf_sz_cons", {"trade_date": "2024-01-02"}, "etf_sz_cons",
         EtfSzConsSchema, ETF_CONS_MAX_RECORDS, ["trade_date", "ts_code", "con_code"]),
        ("get_idx_anns", {"ann_date": "2024-01-02"}, "idx_anns",
         IdxAnnsSchema, IDX_ANNS_MAX_RECORDS, ["ann_date", "title", "source"]),
    ],
)
def test_extended_provider_requests_all_fields_and_paginates(
    method_name, kwargs, api_name, schema, max_records, key_columns
):
    provider = TushareProvider(config={"token": "test-token"})
    first = {column: [None] * max_records for column in schema.get_required_columns()}
    for index in range(max_records):
        if "ts_code" in first:
            first["ts_code"][index] = f"{index:06d}.SH"
        if "trade_date" in first:
            first["trade_date"][index] = "20240102"
        if "con_code" in first:
            first["con_code"][index] = f"{index:06d}.SZ"
        if "ann_date" in first:
            first["ann_date"][index] = "20240102"
        if "title" in first:
            first["title"][index] = f"公告{index}"
        if "source" in first:
            first["source"][index] = "中证指数"
    second = {column: [None] for column in schema.get_required_columns()}
    for key in key_columns:
        second[key] = [
            "20240103" if key in {"trade_date", "ann_date"}
            else ("最后公告" if key == "title" else ("中证指数" if key == "source" else "999999.SH"))
        ]
    provider._call_api = Mock(side_effect=[pd.DataFrame(first), pd.DataFrame(second)])

    result = getattr(provider, method_name)(**kwargs)

    assert len(result) == max_records + 1
    first_call, second_call = provider._call_api.call_args_list
    assert first_call.args[0] == api_name
    assert set(first_call.kwargs["fields"].split(",")) == set(schema.get_required_columns())
    assert first_call.kwargs["offset"] == 0
    assert second_call.kwargs["offset"] == max_records


def test_extended_provider_rejects_repeated_full_page():
    provider = TushareProvider(config={"token": "test-token"})
    page = pd.DataFrame({
        "ts_code": [f"{i:06d}.SH" for i in range(FUND_ADJ_MAX_RECORDS)],
        "trade_date": ["20240102"] * FUND_ADJ_MAX_RECORDS,
        "adj_factor": [1.0] * FUND_ADJ_MAX_RECORDS,
    })
    provider._call_api = Mock(side_effect=[page, page.copy()])
    with pytest.raises(Exception, match="ignored offset"):
        provider.get_fund_adj(trade_date="20240102")


@pytest.mark.asyncio
async def test_fund_adj_smart_incremental_uses_open_dates_and_persists_each_day():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(route=Mock(side_effect=[
        pd.DataFrame({"ts_code": ["510300.SH"], "trade_date": ["20240102"],
                      "adj_factor": [1.0]}),
        pd.DataFrame({"ts_code": ["510300.SH"], "trade_date": ["20240103"],
                      "adj_factor": [1.1]}),
    ]))
    updater.data_ops = Mock(
        get_latest_fund_adj_trade_date=AsyncMock(return_value="2024-01-02"),
        get_trade_dates=AsyncMock(return_value=["2024-01-02", "2024-01-03"]),
        insert_fund_adj_batch=AsyncMock(return_value=1),
    )

    count = await updater.update_fund_adj(
        end_date="2024-01-03", smart_incremental=True
    )

    assert count == 2
    assert updater.data_ops.insert_fund_adj_batch.await_count == 2
    assert [call.kwargs["trade_date"] for call in updater.router.route.call_args_list] == [
        "2024-01-02", "2024-01-03"
    ]


@pytest.mark.asyncio
async def test_idx_anns_incremental_uses_month_windows_and_lookback():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(route=Mock(return_value=pd.DataFrame({
        "ann_date": ["20240130"], "title": ["公告"], "source": ["中证指数"]
    })))
    updater.data_ops = Mock(
        get_latest_idx_anns_ann_date=AsyncMock(return_value="2024-01-30"),
        insert_idx_anns_batch=AsyncMock(return_value=1),
    )

    count = await updater.update_idx_anns(
        end_date="2024-02-02", smart_incremental=True
    )

    assert count == 2
    calls = updater.router.route.call_args_list
    assert calls[0].kwargs["start_date"] == "2024-01-23"
    assert calls[0].kwargs["end_date"] == "2024-01-31"
    assert calls[1].kwargs["start_date"] == "2024-02-01"
    assert calls[1].kwargs["end_date"] == "2024-02-02"


@pytest.mark.asyncio
async def test_extended_sdk_delegates_range_filters():
    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock(get_etf_share_size=AsyncMock(return_value=pd.DataFrame({
        "ts_code": ["510300.SH"]
    })))
    result = await fdh.get_etf_share_size_async(
        ts_code="510300.SH", start_date="2024-01-01", end_date="2024-01-31",
        exchange="SSE",
    )
    assert result is not None
    fdh.ops.get_etf_share_size.assert_awaited_once_with(
        "510300.SH", None, "2024-01-01", "2024-01-31", "SSE"
    )


def test_cli_fund_adj_supports_all_force():
    fake_updater = Mock(update_fund_adj=AsyncMock(return_value=2))
    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)
    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app, ["update", "--dataset", "fund_adj", "--symbols", "all", "--force"]
        )
    assert result.exit_code == 0, result.output
    kwargs = fake_updater.update_fund_adj.await_args.kwargs
    assert kwargs["all_funds"] is True
    assert kwargs["smart_incremental"] is False
    assert kwargs["fund_codes"] is None
