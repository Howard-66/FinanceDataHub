"""HK market support unit tests."""

import asyncio
from datetime import time
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from finance_data_hub.database.operations import (
    _is_non_finite_number,
    _normalize_datetime_for_db,
)
from finance_data_hub.providers.xtquant import XTQuantProvider
from finance_data_hub.providers.schema import DailyDataSchema
from finance_data_hub.router.smart_router import RoutingConfig
from finance_data_hub.update.updater import DataUpdater


def test_market_aware_routing_keeps_cn_and_hk_separate():
    config = RoutingConfig(
        {
            "routing_strategy": {
                "stock": {
                    "CN": {
                        "daily": {"providers": ["tushare", "xtquant"]},
                        "adj_factor": {"providers": ["tushare"]},
                    },
                    "HK": {
                        "daily": {"providers": ["xtquant"]},
                        "adj_factor": {"providers": ["xtquant"]},
                        "minute": {
                            "1m": {"providers": ["xtquant"]},
                        },
                    },
                }
            }
        }
    )

    assert config.get_providers_for_route("stock", "daily", market="CN") == [
        "tushare",
        "xtquant",
    ]
    assert config.get_providers_for_route("stock", "daily", market="HK") == [
        "xtquant"
    ]
    assert config.get_providers_for_route(
        "stock", "minute", freq="1m", market="HK"
    ) == ["xtquant"]
    assert config.get_providers_for_route("stock", "adj_factor", market="HK") == [
        "xtquant"
    ]


def test_legacy_routing_config_still_works():
    config = RoutingConfig(
        {
            "routing_strategy": {
                "stock": {
                    "daily": {"providers": ["tushare", "xtquant"]},
                }
            }
        }
    )

    assert config.get_providers_for_route("stock", "daily", market="HK") == [
        "tushare",
        "xtquant",
    ]


def test_xtquant_hk_stock_basic_from_sector_list():
    provider = XTQuantProvider(market="HK")

    def fake_call(endpoint, payload=None):
        if endpoint == "/download_sector_data":
            return {"ok": True}
        if endpoint == "/get_stock_list_in_sector":
            assert payload == {"sector_name": "香港联交所股票"}
            return {"result": ["HK.00700", "HK.00005"]}
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    with patch.object(provider, "_call_api", side_effect=fake_call):
        df = provider.get_stock_basic(market="HK")

    assert list(df["symbol"]) == ["00700.HK", "00005.HK"]
    assert set(df["exchange"]) == {"HK"}
    assert set(df["market"]) == {"HK"}
    assert set(df["list_status"]) == {"L"}


def test_xtquant_adj_factor_derived_from_back_ratio_close():
    provider = XTQuantProvider(market="HK")
    raw = pd.DataFrame(
        {
            "time": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "symbol": ["00700.HK", "00700.HK"],
            "preclose": [100.0, 100.0],
            "close": [100.0, 80.0],
        }
    )
    adjusted = pd.DataFrame(
        {
            "time": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "symbol": ["00700.HK", "00700.HK"],
            "preclose": [100.0, 100.0],
            "close": [120.0, 100.0],
        }
    )

    with patch.object(
        provider,
        "_fetch_daily_by_dividend",
        side_effect=[raw, adjusted],
    ) as fetch:
        df = provider.get_adj_factor(
            symbol="00700.HK",
            start_date="2024-01-02",
            end_date="2024-01-03",
            market="HK",
        )

    assert [call.args[3] for call in fetch.call_args_list] == ["none", "back_ratio"]
    assert list(df["adj_factor"].round(6)) == [1.2, 1.25]


def test_xtquant_hk_adj_factor_falls_back_to_preclose_chain_when_dividend_type_ignored():
    provider = XTQuantProvider(market="HK")
    raw = pd.DataFrame(
        {
            "time": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "symbol": ["00700.HK", "00700.HK", "00700.HK"],
            "close": [100.0, 97.0, 98.0],
            "preclose": [0.0, 95.0, 97.0],
        }
    )

    with patch.object(
        provider,
        "_fetch_daily_by_dividend",
        side_effect=[raw, raw.copy()],
    ) as fetch:
        df = provider.get_adj_factor(
            symbol="00700.HK",
            start_date="2024-01-02",
            end_date="2024-01-04",
            market="HK",
        )

    assert [call.args[3] for call in fetch.call_args_list] == ["none", "back_ratio"]
    assert list(df["adj_factor"].round(6)) == [1.0, 1.052632, 1.052632]


def test_xtquant_daily_change_fields_skip_zero_preclose():
    provider = XTQuantProvider(market="HK")
    raw = {
        "time": [pd.Timestamp("2004-06-16"), pd.Timestamp("2004-06-17")],
        "symbol": ["00700.HK", "00700.HK"],
        "open": [4.375, 4.15],
        "high": [4.625, 4.375],
        "low": [4.075, 4.125],
        "close": [4.15, 4.225],
        "volume": [439775000, 83801500],
        "amount": [0.0, 355633000.0],
        "preclose": [0.0, 4.15],
    }

    with patch.object(provider, "_convert_dict_to_dataframe", return_value=pd.DataFrame(raw)):
        df = provider._normalize_ohlcv_frame(raw, "00700.HK", DailyDataSchema)

    assert pd.isna(df.loc[0, "change_pct"])
    assert pd.isna(df.loc[0, "change_amount"])
    assert df.loc[1, "change_pct"] == pytest.approx(1.807228915662633)
    assert df.loc[1, "change_amount"] == pytest.approx(0.075)


def test_daily_level_timestamps_use_market_close_times():
    cn_dt = _normalize_datetime_for_db("2024-01-02", "daily", "CN")
    hk_dt = _normalize_datetime_for_db("2024-01-02", "daily", "HK")
    hk_tz_dt = _normalize_datetime_for_db(
        pd.Timestamp("2024-01-02 09:30:00", tz="Asia/Shanghai"),
        "adj_factor",
        "HK",
    )

    assert cn_dt.timetz().replace(tzinfo=None) == time(15, 0)
    assert hk_dt.timetz().replace(tzinfo=None) == time(16, 0)
    assert hk_tz_dt.timetz().replace(tzinfo=None) == time(16, 0)


def test_non_finite_number_helper_flags_inf():
    assert _is_non_finite_number(float("inf")) is True
    assert _is_non_finite_number(float("-inf")) is True
    assert _is_non_finite_number(1.23) is False


def test_updater_hk_daily_uses_market_specific_symbol_pool_and_route():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.data_ops = Mock()
    updater.data_ops.get_symbol_list = AsyncMock(return_value=["00700.HK"])
    updater.data_ops.insert_symbol_daily_batch = AsyncMock(return_value=1)
    updater.router.route.return_value = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "symbol": ["00700.HK"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "amount": [100000.0],
        }
    )

    count = asyncio.run(
        updater.update_daily_data(
            start_date="2024-01-02",
            end_date="2024-01-02",
            force_update=True,
            market="HK",
        )
    )

    assert count == 1
    updater.data_ops.get_symbol_list.assert_awaited_once_with(market="HK")
    updater.router.route.assert_called_once()
    assert updater.router.route.call_args.kwargs["market"] == "HK"
    assert updater.router.route.call_args.kwargs["symbol"] == "00700.HK"


def test_updater_single_symbol_daily_failure_is_raised():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.data_ops = Mock()
    updater.data_ops.insert_symbol_daily_batch = AsyncMock(
        side_effect=ValueError("bad daily row")
    )
    updater.router.route.return_value = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "symbol": ["00700.HK"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "amount": [100000.0],
        }
    )

    with pytest.raises(ValueError, match="bad daily row"):
        asyncio.run(
            updater.update_daily_data(
                symbols=["00700.HK"],
                start_date="2024-01-02",
                end_date="2024-01-02",
                force_update=True,
                market="HK",
            )
        )


def test_updater_single_symbol_adj_factor_failure_is_raised():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.data_ops = Mock()
    updater.data_ops.insert_adj_factor_batch = AsyncMock(
        side_effect=ValueError("bad adj factor row")
    )
    updater.router.route.return_value = pd.DataFrame(
        {
            "time": [pd.Timestamp("2024-01-02")],
            "symbol": ["00700.HK"],
            "adj_factor": [1.01],
        }
    )

    with pytest.raises(ValueError, match="bad adj factor row"):
        asyncio.run(
            updater.update_adj_factor(
                symbols=["00700.HK"],
                start_date="2024-01-02",
                end_date="2024-01-02",
                force_update=True,
                market="HK",
            )
        )
