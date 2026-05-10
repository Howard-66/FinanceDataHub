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
from finance_data_hub.cli.preprocess import (
    _build_market_scope_clause,
    _get_all_stock_symbols,
)
from finance_data_hub.providers.tushare import TushareProvider
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
                        "basic": {"providers": ["tushare"]},
                        "daily": {"providers": ["tushare", "xtquant"]},
                        "adj_factor": {"providers": ["tushare"]},
                    },
                    "HK": {
                        "basic": {"providers": ["tushare"]},
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
    assert config.get_providers_for_route("stock", "basic", market="CN") == [
        "tushare"
    ]
    assert config.get_providers_for_route("stock", "basic", market="HK") == [
        "tushare"
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


def test_tushare_hk_stock_basic_maps_hk_basic_fields():
    provider = TushareProvider(config={"token": "test-token"}, market="HK")

    raw = pd.DataFrame(
        {
            "ts_code": ["00700.HK", "00005.HK"],
            "name": ["腾讯控股", "汇丰控股"],
            "market": ["主板", "主板"],
            "list_status": ["L", "D"],
            "list_date": ["20040716", "19740612"],
            "delist_date": [None, "20250101"],
            "curr_type": ["HKD", "HKD"],
        }
    )

    with patch.object(provider, "_call_api", return_value=raw) as mock_call:
        df = provider.get_stock_basic(market="HK", list_status="L")

    mock_call.assert_called_once_with(
        "hk_basic",
        fields="ts_code,name,market,list_status,list_date,delist_date",
        list_status="L",
    )

    assert list(df["symbol"]) == ["00700.HK", "00005.HK"]
    assert list(df["name"]) == ["腾讯控股", "汇丰控股"]
    assert set(df["exchange"]) == {"HK"}
    assert set(df["market"]) == {"主板"}
    assert list(df["list_status"]) == ["L", "D"]
    assert df["list_date"].iloc[0] == pd.Timestamp("2004-07-16")
    assert pd.isna(df["delist_date"].iloc[0])
    assert df["delist_date"].iloc[1] == pd.Timestamp("2025-01-01")


def test_xtquant_cn_adj_factor_derived_from_back_ratio_close():
    provider = XTQuantProvider(market="CN")
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
            symbol="600519.SH",
            start_date="2024-01-02",
            end_date="2024-01-03",
            market="CN",
        )

    assert [call.args[3] for call in fetch.call_args_list] == ["none", "back_ratio"]
    assert list(df["adj_factor"].round(6)) == [1.2, 1.25]


def test_xtquant_hk_adj_factor_uses_preclose_chain_only():
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

    assert [call.args[3] for call in fetch.call_args_list] == ["none"]
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


def test_preprocess_market_scope_clause_supports_cn_and_hk():
    hk_clause = _build_market_scope_clause("d.symbol", "HK", asset_alias="b")
    cn_clause = _build_market_scope_clause("d.symbol", "CN", asset_alias="b")
    all_clause = _build_market_scope_clause("d.symbol", "ALL", asset_alias="b")

    assert "COALESCE(b.exchange" in hk_clause
    assert "IN ('HK')" in hk_clause
    assert "IN ('BJ', 'SH', 'SZ')" in cn_clause
    assert all_clause == ""


def test_preprocess_stock_pool_query_is_market_aware():
    class FakeResult:
        def fetchall(self):
            return [("00700.HK",), ("00005.HK",)]

    class FakeDBManager:
        def __init__(self):
            self.sql = None

        async def initialize(self):
            return None

        async def execute_raw_sql(self, sql, params=None):
            self.sql = sql
            return FakeResult()

    db_manager = FakeDBManager()
    symbols = asyncio.run(_get_all_stock_symbols(db_manager, market="HK"))

    assert symbols == ["00700.HK", "00005.HK"]
    assert "FROM symbol_daily d" in db_manager.sql
    assert "LEFT JOIN asset_basic b ON d.symbol = b.symbol" in db_manager.sql
    assert "IN ('HK')" in db_manager.sql


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
    updater.data_ops.get_latest_adj_factor_record = AsyncMock(return_value=None)
    updater.data_ops.get_latest_symbol_daily_bar = AsyncMock(return_value=None)
    updater.data_ops.get_symbol_daily_for_adj_factor = AsyncMock(
        return_value=pd.DataFrame(
            {
                "time": pd.to_datetime(["2024-01-02"]),
                "symbol": ["00700.HK"],
                "close": [100.0],
                "preclose": [0.0],
            }
        )
    )
    updater.data_ops.insert_adj_factor_batch = AsyncMock(
        side_effect=ValueError("bad adj factor row")
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


def test_updater_hk_adj_factor_uses_local_daily_data_not_router():
    updater = DataUpdater(settings=Mock())
    updater.router = Mock()
    updater.data_ops = Mock()
    updater.data_ops.get_latest_adj_factor_record = AsyncMock(return_value=None)
    updater.data_ops.get_latest_symbol_daily_bar = AsyncMock(return_value=None)
    updater.data_ops.get_symbol_daily_for_adj_factor = AsyncMock(
        return_value=pd.DataFrame(
            {
                "time": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                "symbol": ["00700.HK", "00700.HK", "00700.HK"],
                "close": [100.0, 97.0, 98.0],
                "preclose": [0.0, 95.0, 97.0],
            }
        )
    )
    updater.data_ops.insert_adj_factor_batch = AsyncMock(return_value=3)

    count = asyncio.run(
        updater.update_adj_factor(
            symbols=["00700.HK"],
            start_date="2024-01-02",
            end_date="2024-01-04",
            force_update=True,
            market="HK",
        )
    )

    assert count == 3
    updater.router.route.assert_not_called()
    updater.data_ops.get_symbol_daily_for_adj_factor.assert_awaited_once_with(
        "00700.HK",
        start_date="2024-01-02",
        end_date="2024-01-04",
        market="HK",
    )


def test_updater_hk_adj_factor_incremental_derivation_uses_anchor_bar_and_base_factor():
    updater = DataUpdater(settings=Mock())
    raw_df = pd.DataFrame(
        {
            "time": pd.to_datetime(["2024-01-03", "2024-01-04", "2024-01-05"]),
            "symbol": ["00700.HK", "00700.HK", "00700.HK"],
            "close": [97.0, 98.0, 99.0],
            "preclose": [95.0, 97.0, 98.0],
        }
    )

    result = updater._derive_hk_adj_factor_from_local_daily(
        raw_df,
        base_adj_factor=1.0526315789,
        output_start_date="2024-01-04",
    )

    assert list(result["time"].dt.strftime("%Y-%m-%d")) == ["2024-01-04", "2024-01-05"]
    assert list(result["adj_factor"].round(6)) == [1.052632, 1.052632]
