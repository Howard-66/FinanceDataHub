from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.providers.tushare import TushareProvider
from finance_data_hub.update.updater import DataUpdater


@pytest.mark.asyncio
async def test_update_fund_company_and_manager_route_to_their_fund_endpoints():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.side_effect = [
        pd.DataFrame({"name": ["示例基金管理有限公司"]}),
        pd.DataFrame(
            {
                "ts_code": ["150018.SZ"], "ann_date": ["20100508"],
                "name": ["周毅"], "begin_date": ["20100507"],
            }
        ),
    ]
    updater.data_ops = Mock()
    updater.data_ops.insert_fund_company_batch = AsyncMock(return_value=1)
    updater.data_ops.insert_fund_manager_batch = AsyncMock(return_value=1)

    assert await updater.update_fund_company() == 1
    assert await updater.update_fund_manager(
        fund_codes=["150018.SZ"], ann_date="20100508"
    ) == 1

    assert updater.router.route.call_args_list[0].kwargs == {
        "asset_class": "fund", "data_type": "company", "method_name": "get_fund_company"
    }
    assert updater.router.route.call_args_list[1].kwargs == {
        "asset_class": "fund", "data_type": "manager", "method_name": "get_fund_manager",
        "ts_code": "150018.SZ", "ann_date": "20100508", "name": None,
    }


@pytest.mark.asyncio
async def test_update_index_daily_default_resolves_full_catalog():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.return_value = pd.DataFrame(
        {
            "ts_code": ["000300.CSI", "000905.CSI", "801010.SI"],
            "trade_date": [pd.Timestamp("2024-01-02")] * 3,
        }
    )
    updater.data_ops = Mock()
    updater.data_ops.get_index_basic_codes = AsyncMock(
        return_value=["000300.CSI", "000905.CSI"]
    )
    updater.data_ops.get_latest_index_daily_date = AsyncMock(return_value=None)
    updater.data_ops.get_earliest_index_basic_list_date = AsyncMock(
        return_value="2024-01-01"
    )
    updater.data_ops.get_trade_dates = AsyncMock(return_value=["2024-01-02"])
    updater.data_ops.insert_index_daily_batch = AsyncMock(return_value=2)

    result = await updater.update_index_daily(end_date="2024-01-31")

    assert result == 2
    updater.data_ops.get_index_basic_codes.assert_awaited_once_with(
        exclude_markets=["SW"],
        active_date="2024-01-31",
    )
    updater.data_ops.get_latest_index_daily_date.assert_awaited_once_with()
    updater.router.route.assert_called_once_with(
        asset_class="index",
        data_type="daily",
        method_name="get_index_daily",
        trade_date="2024-01-02",
    )
    inserted = updater.data_ops.insert_index_daily_batch.await_args.args[0]
    assert list(inserted["ts_code"]) == ["000300.CSI", "000905.CSI"]


@pytest.mark.asyncio
async def test_update_index_daily_force_all_uses_earliest_list_date_and_trade_dates():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.side_effect = [
        pd.DataFrame(
            {
                "ts_code": ["000300.CSI"],
                "trade_date": [pd.Timestamp("2005-04-08")],
            }
        ),
        pd.DataFrame(
            {
                "ts_code": ["000300.CSI"],
                "trade_date": [pd.Timestamp("2005-04-11")],
            }
        ),
    ]
    updater.data_ops = Mock()
    updater.data_ops.get_index_basic_codes = AsyncMock(return_value=["000300.CSI"])
    updater.data_ops.get_earliest_index_basic_list_date = AsyncMock(
        return_value="2005-04-08"
    )
    updater.data_ops.get_trade_dates = AsyncMock(
        return_value=["2005-04-08", "2005-04-11"]
    )
    updater.data_ops.insert_index_daily_batch = AsyncMock(side_effect=[1, 1])

    result = await updater.update_index_daily(
        ts_code_list=["all"],
        end_date="2005-04-30",
        force_update=True,
    )

    assert result == 2
    updater.data_ops.get_index_basic_codes.assert_awaited_once_with(
        exclude_markets=["SW"],
        active_date=None,
    )
    updater.data_ops.get_earliest_index_basic_list_date.assert_awaited_once_with(
        exclude_markets=["SW"],
    )
    updater.data_ops.get_trade_dates.assert_awaited_once_with(
        exchange="SSE",
        start_date="2005-04-08",
        end_date="2005-04-30",
    )


@pytest.mark.asyncio
async def test_update_index_daily_trade_date_fetches_once_and_filters_catalog():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.return_value = pd.DataFrame(
        {
            "ts_code": ["000300.CSI", "801010.SI"],
            "trade_date": [pd.Timestamp("2024-06-30")] * 2,
        }
    )
    updater.data_ops = Mock()
    updater.data_ops.get_index_basic_codes = AsyncMock(return_value=["000300.CSI"])
    updater.data_ops.insert_index_daily_batch = AsyncMock(return_value=1)

    result = await updater.update_index_daily(trade_date="2024-06-30")

    assert result == 1
    updater.data_ops.get_index_basic_codes.assert_awaited_once_with(
        exclude_markets=["SW"],
        active_date="2024-06-30",
    )
    updater.router.route.assert_called_once_with(
        asset_class="index",
        data_type="daily",
        method_name="get_index_daily",
        trade_date="2024-06-30",
    )
    inserted = updater.data_ops.insert_index_daily_batch.await_args.args[0]
    assert list(inserted["ts_code"]) == ["000300.CSI"]


@pytest.mark.asyncio
async def test_update_index_weight_symbols_all_resolves_full_catalog():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.side_effect = [
        pd.DataFrame(
            {
                "index_code": ["000300.CSI"],
                "con_code": ["000001.SZ"],
                "trade_date": [pd.Timestamp("2024-01-31")],
                "weight": [1.23],
            }
        ),
        pd.DataFrame(
            {
                "index_code": ["000905.CSI"],
                "con_code": ["000002.SZ"],
                "trade_date": [pd.Timestamp("2024-01-31")],
                "weight": [2.34],
            }
        ),
    ]
    updater.data_ops = Mock()
    updater.data_ops.get_index_basic_codes = AsyncMock(
        return_value=["000300.CSI", "000905.CSI"]
    )
    updater.data_ops.get_latest_index_weight_date = AsyncMock(return_value=None)
    updater.data_ops.insert_index_weight_batch = AsyncMock(side_effect=[1, 1])

    result = await updater.update_index_weight(
        index_list=["all"],
        end_date="2024-01-31",
    )

    assert result == 2
    updater.data_ops.get_index_basic_codes.assert_awaited_once_with(
        exclude_markets=[],
        active_date="2024-01-31",
    )
    assert updater.data_ops.get_latest_index_weight_date.await_count == 2


@pytest.mark.asyncio
async def test_update_index_basic_refreshes_selected_markets():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.return_value = pd.DataFrame(
        {
            "ts_code": ["000001.SH"],
            "name": ["上证综指"],
            "market": ["SSE"],
        }
    )
    updater.data_ops = Mock()
    updater.data_ops.insert_index_basic_batch = AsyncMock(return_value=1)

    result = await updater.update_index_basic(markets=["sse"])

    assert result == 1
    updater.router.route.assert_called_once_with(
        asset_class="index",
        data_type="basic",
        method_name="get_index_basic",
        markets=["SSE"],
    )
    updater.data_ops.insert_index_basic_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_fund_basic_refreshes_selected_markets():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.return_value = pd.DataFrame(
        {"ts_code": ["510300.SH"], "name": ["沪深300ETF"], "market": ["E"]}
    )
    updater.data_ops = Mock()
    updater.data_ops.insert_fund_basic_batch = AsyncMock(return_value=1)

    result = await updater.update_fund_basic(markets=["e"])

    assert result == 1
    updater.router.route.assert_called_once_with(
        asset_class="fund",
        data_type="basic",
        method_name="get_fund_basic",
        markets=["E"],
        status=None,
    )
    updater.data_ops.insert_fund_basic_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_sw_daily_symbols_all_resolves_catalog_and_normalizes_dates():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.return_value = pd.DataFrame()
    updater.data_ops = Mock()
    updater.data_ops.get_sw_industry_classify = AsyncMock(
        return_value=pd.DataFrame(
            {"index_code": ["801010.SI", "801020.SI"]}
        )
    )

    result = await updater.update_sw_daily(
        ts_code_list=["ALL"],
        start_date="2026-06-09",
        end_date="2026-08-13",
        force_update=True,
    )

    assert result == 0
    updater.data_ops.get_sw_industry_classify.assert_awaited_once_with(level=None)
    assert [call.kwargs for call in updater.router.route.call_args_list] == [
        {
            "asset_class": "index",
            "data_type": "sw_daily",
            "method_name": "get_sw_daily",
            "ts_code": "801010.SI",
            "start_date": "20260609",
            "end_date": "20260813",
        },
        {
            "asset_class": "index",
            "data_type": "sw_daily",
            "method_name": "get_sw_daily",
            "ts_code": "801020.SI",
            "start_date": "20260609",
            "end_date": "20260813",
        },
    ]


@pytest.mark.asyncio
async def test_update_sw_daily_trade_date_is_normalized():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.return_value = pd.DataFrame()
    updater.data_ops = Mock()

    result = await updater.update_sw_daily(trade_date="2026-08-13")

    assert result == 0
    updater.router.route.assert_called_once_with(
        asset_class="index",
        data_type="sw_daily",
        method_name="get_sw_daily",
        trade_date="20260813",
    )


@pytest.mark.asyncio
async def test_update_sw_daily_raises_when_all_industry_requests_fail():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.side_effect = RuntimeError("provider unavailable")
    updater.data_ops = Mock()

    with pytest.raises(RuntimeError, match="All 2 sw_daily requests failed"):
        await updater.update_sw_daily(
            ts_code_list=["801010.SI", "801020.SI"],
            start_date="2026-06-09",
            end_date="2026-08-13",
            force_update=True,
        )


def test_tushare_sw_daily_normalizes_api_dates():
    provider = TushareProvider(config={})
    provider._call_api = Mock(return_value=pd.DataFrame())

    provider.get_sw_daily(
        ts_code="801010.SI",
        trade_date="2026-08-13",
        start_date="2026-06-09",
        end_date="2026-08-13",
    )

    call = provider._call_api.call_args
    assert call.args == ("sw_daily",)
    assert call.kwargs["ts_code"] == "801010.SI"
    assert call.kwargs["trade_date"] == "20260813"
    assert call.kwargs["start_date"] == "20260609"
    assert call.kwargs["end_date"] == "20260813"
