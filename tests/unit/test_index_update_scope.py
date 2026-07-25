from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.update.updater import DataUpdater


@pytest.mark.asyncio
async def test_update_index_daily_default_resolves_full_catalog():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.side_effect = [
        ["000300.CSI", "000905.CSI"],
        pd.DataFrame(
            {
                "ts_code": ["000300.CSI"],
                "trade_date": [pd.Timestamp("2024-01-02")],
            }
        ),
        pd.DataFrame(
            {
                "ts_code": ["000905.CSI"],
                "trade_date": [pd.Timestamp("2024-01-02")],
            }
        ),
    ]
    updater.data_ops = Mock()
    updater.data_ops.get_latest_index_daily_date = AsyncMock(return_value=None)
    updater.data_ops.insert_index_daily_batch = AsyncMock(side_effect=[1, 1])

    result = await updater.update_index_daily(end_date="2024-01-31")

    assert result == 2
    first_call = updater.router.route.call_args_list[0]
    assert first_call.kwargs["method_name"] == "get_index_basic_codes"
    assert first_call.kwargs["exclude_markets"] == ["SW"]
    assert updater.data_ops.get_latest_index_daily_date.await_count == 2


@pytest.mark.asyncio
async def test_update_index_weight_symbols_all_resolves_full_catalog():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.router.route.side_effect = [
        ["000300.CSI", "000905.CSI"],
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
    updater.data_ops.get_latest_index_weight_date = AsyncMock(return_value=None)
    updater.data_ops.insert_index_weight_batch = AsyncMock(side_effect=[1, 1])

    result = await updater.update_index_weight(
        index_list=["all"],
        end_date="2024-01-31",
    )

    assert result == 2
    first_call = updater.router.route.call_args_list[0]
    assert first_call.kwargs["method_name"] == "get_index_basic_codes"
    assert first_call.kwargs["data_type"] == "index_weight"
    assert updater.data_ops.get_latest_index_weight_date.await_count == 2
