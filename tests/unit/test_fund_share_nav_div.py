from datetime import date
from unittest.mock import AsyncMock, Mock, call, patch

import pandas as pd
import pytest

from finance_data_hub.database.operations import DataOperations
from finance_data_hub.providers.tushare import (
    FUND_DIV_MAX_RECORDS,
    FUND_NAV_MAX_RECORDS,
    FUND_SERIES_MAX_RECORDS,
    TushareProvider,
)
from finance_data_hub.sdk import FinanceDataHub
from finance_data_hub.cli.main import app
from finance_data_hub.update.updater import DataUpdater
from tests.unit.test_index_basic_storage import _DatabaseManager, _Engine, _Transaction
from typer.testing import CliRunner


runner = CliRunner()


def test_fund_share_paginates_and_requests_all_fields():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": ["150018.SZ"] * FUND_SERIES_MAX_RECORDS,
                    "trade_date": ["20240102"] * FUND_SERIES_MAX_RECORDS,
                    "fd_share": [1.0] * FUND_SERIES_MAX_RECORDS,
                }
            ),
            pd.DataFrame(
                {
                    "ts_code": ["150019.SZ"],
                    "trade_date": ["20240103"],
                    "fd_share": [2.0],
                }
            ),
        ]
    )

    result = provider.get_fund_share(market="SZ")

    # The duplicate full page is de-duplicated on the documented primary key.
    assert len(result) == 2
    assert (
        provider._call_api.call_args_list[1].kwargs["offset"] == FUND_SERIES_MAX_RECORDS
    )
    assert set(provider._call_api.call_args.kwargs["fields"].split(",")) == {
        "ts_code",
        "trade_date",
        "fd_share",
    }


def test_fund_nav_and_div_request_every_documented_output_field():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {"ts_code": ["165509.SZ"], "nav_date": ["20240102"], "unit_nav": [1.0]}
            ),
            pd.DataFrame(
                {"ts_code": ["161618.OF"], "ann_date": ["20240102"], "div_cash": [0.1]}
            ),
        ]
    )

    nav = provider.get_fund_nav(ts_code="165509.SZ")
    div = provider.get_fund_div(ann_date="20240102")

    assert nav.iloc[0]["nav_date"] == pd.Timestamp("2024-01-02")
    assert div.iloc[0]["ann_date"] == pd.Timestamp("2024-01-02")
    nav_fields = set(provider._call_api.call_args_list[0].kwargs["fields"].split(","))
    div_fields = set(provider._call_api.call_args_list[1].kwargs["fields"].split(","))
    assert nav_fields == {
        "ts_code",
        "ann_date",
        "nav_date",
        "unit_nav",
        "accum_nav",
        "accum_div",
        "net_asset",
        "total_netasset",
        "adj_nav",
    }
    assert (
        "account_date" in div_fields
        and "base_year" in div_fields
        and len(div_fields) == 16
    )


def test_fund_series_normalize_cli_dates_to_tushare_format():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": ["510300.SH"],
                    "trade_date": ["20240102"],
                    "fd_share": [1.0],
                }
            ),
            pd.DataFrame(
                {
                    "ts_code": ["000001.OF"],
                    "nav_date": ["20240102"],
                    "unit_nav": [1.0],
                }
            ),
            pd.DataFrame(
                {
                    "ts_code": ["000001.OF"],
                    "ann_date": ["20240102"],
                    "div_cash": [0.1],
                }
            ),
        ]
    )

    provider.get_fund_share(
        trade_date="2024-01-02",
        start_date="2024-01-01",
        end_date="2024-01-03",
    )
    provider.get_fund_nav(nav_date="2024-01-02")
    provider.get_fund_div(ann_date="2024-01-02")

    share_kwargs = provider._call_api.call_args_list[0].kwargs
    nav_kwargs = provider._call_api.call_args_list[1].kwargs
    div_kwargs = provider._call_api.call_args_list[2].kwargs
    assert {
        key: share_kwargs[key] for key in ("trade_date", "start_date", "end_date")
    } == {
        "trade_date": "20240102",
        "start_date": "20240101",
        "end_date": "20240103",
    }
    assert nav_kwargs["nav_date"] == "20240102"
    assert div_kwargs["ann_date"] == "20240102"


def test_fund_series_ignores_router_cn_market_for_tushare_filters():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": ["510300.SH"],
                    "trade_date": ["20240102"],
                    "fd_share": [1.0],
                }
            ),
            pd.DataFrame(
                {
                    "ts_code": ["000001.OF"],
                    "nav_date": ["20240102"],
                    "unit_nav": [1.0],
                }
            ),
        ]
    )

    provider.get_fund_share(trade_date="2024-01-02", market="CN")
    provider.get_fund_nav(nav_date="2024-01-02", market="CN")

    share_kwargs = provider._call_api.call_args_list[0].kwargs
    nav_kwargs = provider._call_api.call_args_list[1].kwargs
    assert "market" not in share_kwargs
    assert "market" not in nav_kwargs


def test_fund_nav_paginates_at_the_observed_10500_record_limit():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": [
                        f"{index:06d}.OF" for index in range(FUND_NAV_MAX_RECORDS)
                    ],
                    "nav_date": ["20240102"] * FUND_NAV_MAX_RECORDS,
                    "unit_nav": [1.0] * FUND_NAV_MAX_RECORDS,
                }
            ),
            pd.DataFrame(
                {"ts_code": ["999999.OF"], "nav_date": ["20240102"], "unit_nav": [2.0]}
            ),
        ]
    )

    result = provider.get_fund_nav(nav_date="20240102")

    assert len(result) == FUND_NAV_MAX_RECORDS + 1
    assert provider._call_api.call_args_list[1].kwargs["offset"] == FUND_NAV_MAX_RECORDS


def test_fund_div_paginates_at_the_documented_1000_record_limit():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": [
                        f"{index:06d}.OF" for index in range(FUND_DIV_MAX_RECORDS)
                    ],
                    "ann_date": ["20240102"] * FUND_DIV_MAX_RECORDS,
                    "div_cash": [0.1] * FUND_DIV_MAX_RECORDS,
                }
            ),
            pd.DataFrame(
                {"ts_code": ["999999.OF"], "ann_date": ["20240102"], "div_cash": [0.2]}
            ),
        ]
    )

    result = provider.get_fund_div(ann_date="20240102")

    assert len(result) == FUND_DIV_MAX_RECORDS + 1
    assert provider._call_api.call_args_list[1].kwargs["offset"] == FUND_DIV_MAX_RECORDS


@pytest.mark.asyncio
async def test_update_fund_share_all_fetches_each_trade_date_in_history_order():
    updater = object.__new__(DataUpdater)
    updater.router = Mock()
    updater.router.route.side_effect = [
        pd.DataFrame(
            {"ts_code": ["510300.SH"], "trade_date": ["20240102"], "fd_share": [1.0]}
        ),
        pd.DataFrame(
            {"ts_code": ["510300.SH"], "trade_date": ["20240103"], "fd_share": [1.1]}
        ),
    ]
    updater.data_ops = Mock(
        get_earliest_fund_basic_date=AsyncMock(return_value="2024-01-02"),
        get_trade_cal=AsyncMock(
            return_value=pd.DataFrame(
                {
                    "cal_date": ["2024-01-02", "2024-01-03"],
                    "is_open": [1, 1],
                }
            )
        ),
        insert_fund_share_batch=AsyncMock(side_effect=[1, 1]),
    )
    progress = Mock()

    count = await updater.update_fund_share(
        all_funds=True,
        end_date="2024-01-03",
        progress_callback=progress,
    )

    assert count == 2
    updater.data_ops.get_earliest_fund_basic_date.assert_awaited_once_with()
    assert updater.router.route.call_args_list == [
        call(
            asset_class="fund",
            data_type="share",
            method_name="get_fund_share",
            trade_date="2024-01-02",
            market=None,
        ),
        call(
            asset_class="fund",
            data_type="share",
            method_name="get_fund_share",
            trade_date="2024-01-03",
            market=None,
        ),
    ]
    assert progress.call_args_list == [call(0, 2), call(1, 2), call(2, 2)]


@pytest.mark.asyncio
async def test_update_fund_nav_all_fetches_each_date_from_fund_basic_start():
    updater = object.__new__(DataUpdater)
    updater.router = Mock()
    updater.router.route.side_effect = [
        pd.DataFrame(
            {"ts_code": ["000001.OF"], "nav_date": ["20240102"], "unit_nav": [1.0]}
        ),
        pd.DataFrame(
            {"ts_code": ["000001.OF"], "nav_date": ["20240103"], "unit_nav": [1.1]}
        ),
    ]
    updater.data_ops = Mock(
        get_earliest_fund_basic_date=AsyncMock(return_value="2024-01-02"),
        get_trade_cal=AsyncMock(
            return_value=pd.DataFrame(
                {
                    "cal_date": ["2024-01-02", "2024-01-03"],
                    "is_open": [1, 1],
                }
            )
        ),
        insert_fund_nav_batch=AsyncMock(side_effect=[1, 1]),
    )
    progress = Mock()

    count = await updater.update_fund_nav(
        all_funds=True,
        end_date="2024-01-03",
        progress_callback=progress,
    )

    assert count == 2
    updater.data_ops.get_earliest_fund_basic_date.assert_awaited_once_with()
    assert updater.router.route.call_args_list == [
        call(
            asset_class="fund",
            data_type="nav",
            method_name="get_fund_nav",
            nav_date="2024-01-02",
            market=None,
        ),
        call(
            asset_class="fund",
            data_type="nav",
            method_name="get_fund_nav",
            nav_date="2024-01-03",
            market=None,
        ),
    ]
    assert progress.call_args_list == [call(0, 2), call(1, 2), call(2, 2)]


@pytest.mark.asyncio
async def test_update_fund_div_all_fetches_every_calendar_day_from_fund_basic_start():
    updater = object.__new__(DataUpdater)
    updater.router = Mock()
    updater.router.route.side_effect = [
        pd.DataFrame(
            {"ts_code": ["000001.OF"], "ann_date": ["20240102"], "div_cash": [0.1]}
        ),
        pd.DataFrame(),
        pd.DataFrame(
            {"ts_code": ["000001.OF"], "ann_date": ["20240104"], "div_cash": [0.2]}
        ),
    ]
    updater.data_ops = Mock(
        get_earliest_fund_basic_date=AsyncMock(return_value="2024-01-02"),
        insert_fund_div_batch=AsyncMock(side_effect=[1, 1]),
    )
    progress = Mock()

    count = await updater.update_fund_div(
        all_funds=True,
        end_date="2024-01-04",
        progress_callback=progress,
    )

    assert count == 2
    updater.data_ops.get_earliest_fund_basic_date.assert_awaited_once_with()
    assert updater.router.route.call_args_list == [
        call(
            asset_class="fund",
            data_type="div",
            method_name="get_fund_div",
            ann_date="2024-01-02",
        ),
        call(
            asset_class="fund",
            data_type="div",
            method_name="get_fund_div",
            ann_date="2024-01-03",
        ),
        call(
            asset_class="fund",
            data_type="div",
            method_name="get_fund_div",
            ann_date="2024-01-04",
        ),
    ]
    assert progress.call_args_list == [call(0, 3), call(1, 3), call(2, 3), call(3, 3)]


@pytest.mark.asyncio
async def test_fund_nav_storage_upserts_and_sdk_delegates():
    transaction = _Transaction()
    operations = DataOperations(_DatabaseManager(_Engine(transaction)))
    count = await operations.insert_fund_nav_batch(
        pd.DataFrame(
            {
                "ts_code": ["165509.SZ"],
                "ann_date": [pd.Timestamp("2024-01-03")],
                "nav_date": [pd.Timestamp("2024-01-02")],
                "unit_nav": [1.1],
            }
        )
    )
    assert count == 1
    assert "ON CONFLICT (ts_code, nav_date)" in str(transaction.statement)
    assert transaction.records[0]["nav_date"] == date(2024, 1, 2)

    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock(
        get_fund_div=AsyncMock(return_value=pd.DataFrame({"ts_code": ["161618.OF"]}))
    )
    result = await fdh.get_fund_div_async(ts_code="161618.OF")
    assert result.iloc[0]["ts_code"] == "161618.OF"
    fdh.ops.get_fund_div.assert_awaited_once_with("161618.OF", None, None, None)


def test_cli_routes_fund_nav_to_fund_updater():
    updater = Mock(update_fund_nav=AsyncMock(return_value=1))
    context = Mock()
    context.__aenter__ = AsyncMock(return_value=updater)
    context.__aexit__ = AsyncMock(return_value=None)
    with patch("finance_data_hub.cli.main.DataUpdater", return_value=context):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "fund_nav",
                "--symbols",
                "165509.SZ",
                "--trade-date",
                "2024-01-02",
            ],
        )
    assert result.exit_code == 0
    updater.update_fund_nav.assert_awaited_once_with(
        ts_code="165509.SZ",
        nav_date="2024-01-02",
        market=None,
        start_date=None,
        end_date=None,
    )


def test_cli_routes_fund_nav_all_to_date_based_full_update():
    async def update_fund_nav(**kwargs):
        kwargs["progress_callback"](1520, 8616)
        return 2

    updater = Mock(update_fund_nav=AsyncMock(side_effect=update_fund_nav))
    context = Mock()
    context.__aenter__ = AsyncMock(return_value=updater)
    context.__aexit__ = AsyncMock(return_value=None)
    with patch("finance_data_hub.cli.main.DataUpdater", return_value=context):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "fund_nav",
                "--symbols",
                "all",
                "--force",
                "--start-date",
                "2024-01-02",
                "--end-date",
                "2024-01-03",
            ],
        )

    assert result.exit_code == 0
    call_kwargs = updater.update_fund_nav.await_args.kwargs
    assert call_kwargs["ts_code"] is None
    assert call_kwargs["all_funds"] is True
    assert call_kwargs["start_date"] == "2024-01-02"
    assert call_kwargs["end_date"] == "2024-01-03"
    assert callable(call_kwargs["progress_callback"])
    assert "已下载 1520/8616 交易日" in result.output


@pytest.mark.parametrize(
    ("dataset", "updater_method", "date_label"),
    [
        ("fund_share", "update_fund_share", "交易日"),
        ("fund_div", "update_fund_div", "公告日"),
    ],
)
def test_cli_routes_fund_series_all_to_date_based_full_update(
    dataset, updater_method, date_label
):
    async def update_all(**kwargs):
        kwargs["progress_callback"](1, 2)
        return 2

    updater = Mock()
    setattr(updater, updater_method, AsyncMock(side_effect=update_all))
    context = Mock()
    context.__aenter__ = AsyncMock(return_value=updater)
    context.__aexit__ = AsyncMock(return_value=None)
    with patch("finance_data_hub.cli.main.DataUpdater", return_value=context):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                dataset,
                "--symbols",
                "all",
                "--force",
                "--start-date",
                "2024-01-02",
                "--end-date",
                "2024-01-03",
            ],
        )

    assert result.exit_code == 0
    call_kwargs = getattr(updater, updater_method).await_args.kwargs
    assert call_kwargs["ts_code"] is None
    assert call_kwargs["all_funds"] is True
    assert call_kwargs["start_date"] == "2024-01-02"
    assert call_kwargs["end_date"] == "2024-01-03"
    assert callable(call_kwargs["progress_callback"])
    assert f"已下载 1/2 {date_label}" in result.output
