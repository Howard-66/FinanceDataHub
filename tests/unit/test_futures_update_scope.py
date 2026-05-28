import asyncio
import threading
import time
from unittest.mock import AsyncMock, Mock

import pytest
import pandas as pd

import finance_data_hub.update.updater as updater_module
from finance_data_hub.update.updater import (
    DataUpdater,
    _is_all_futures_selector,
    _iter_date_chunks,
    _normalize_futures_window,
)


def test_is_all_futures_selector():
    assert _is_all_futures_selector(["all"])
    assert _is_all_futures_selector(["ALL"])
    assert not _is_all_futures_selector(["RB"])
    assert not _is_all_futures_selector(["all", "RB"])


def test_iter_date_chunks_uses_inclusive_ascending_windows():
    assert list(_iter_date_chunks("2024-01-01", "2024-02-05", 31)) == [
        ("2024-01-01", "2024-01-31"),
        ("2024-02-01", "2024-02-05"),
    ]


def test_normalize_futures_window_for_trade_date():
    assert _normalize_futures_window("2024-04-30", None, None) == (
        "2024-04-30",
        "2024-04-30",
    )


def test_resolve_futures_symbol_universe_explicit_all_uses_contract_basic_overlap():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_futures_contract_symbols = AsyncMock(
            return_value=["RB2405.SHF", "RB.SHF", "RBL.SHF"]
        )

        resolved = await updater._resolve_futures_symbol_universe(
            symbols=["all"],
            product_codes=None,
            contract_types=["normal", "main", "continuous"],
            trade_date=None,
            start_date="2024-04-01",
            end_date="2024-04-30",
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB2405.SHF", "RB.SHF", "RBL.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_awaited_once_with(
            contract_types=["normal", "main", "continuous"],
            active_only=False,
            overlap_start="2024-04-01",
            overlap_end="2024-04-30",
        )

    asyncio.run(_run())


def test_resolve_futures_symbol_universe_default_incremental_uses_active_scope():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_futures_contract_symbols = AsyncMock(return_value=["RB.SHF"])

        resolved = await updater._resolve_futures_symbol_universe(
            symbols=None,
            product_codes=None,
            contract_types=["main"],
            trade_date=None,
            start_date=None,
            end_date="2024-04-30",
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_awaited_once_with(
            contract_types=["main"],
            active_only=False,
            overlap_start="2024-04-30",
            overlap_end="2024-04-30",
        )

    asyncio.run(_run())


def test_resolve_futures_symbol_universe_explicit_all_default_incremental_uses_active_scope():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_futures_contract_symbols = AsyncMock(return_value=["RB.SHF"])

        resolved = await updater._resolve_futures_symbol_universe(
            symbols=["all"],
            product_codes=None,
            contract_types=["normal", "main", "continuous"],
            trade_date=None,
            start_date=None,
            end_date=None,
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_awaited_once_with(
            contract_types=["normal", "main", "continuous"],
            active_only=True,
            overlap_start=None,
            overlap_end=None,
        )

    asyncio.run(_run())


def test_resolve_futures_symbol_universe_trade_date_filters_same_day_overlap():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_futures_contract_symbols = AsyncMock(return_value=["RB2405.SHF"])

        resolved = await updater._resolve_futures_symbol_universe(
            symbols=["all"],
            product_codes=None,
            contract_types=["normal", "main", "continuous"],
            trade_date="2024-04-30",
            start_date=None,
            end_date=None,
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB2405.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_awaited_once_with(
            contract_types=["normal", "main", "continuous"],
            active_only=False,
            overlap_start="2024-04-30",
            overlap_end="2024-04-30",
        )

    asyncio.run(_run())


def test_resolve_futures_symbol_universe_explicit_symbols_bypass_lookup():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_futures_contract_symbols = AsyncMock()

        resolved = await updater._resolve_futures_symbol_universe(
            symbols=["RB2405.SHF", "RB.SHF"],
            product_codes=None,
            contract_types=["normal", "main"],
            trade_date=None,
            start_date=None,
            end_date="2024-04-30",
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB2405.SHF", "RB.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_not_called()

    asyncio.run(_run())


def test_update_futures_spot_basis_force_without_dates_uses_full_history_range():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.insert_futures_spot_basis_batch = AsyncMock(return_value=1)
        updater.router = Mock()
        updater.router.route = Mock(
            return_value=pd.DataFrame({"time": ["2011-01-04"], "product_code": ["RB"]})
        )

        inserted = await updater.update_futures_spot_basis(
            product_codes=None,
            trade_date=None,
            start_date=None,
            end_date="2011-02-05",
            force_update=True,
        )

        assert inserted == 2
        updater.data_ops.get_latest_futures_date.assert_not_called()
        assert updater.router.route.call_count == 2
        assert updater.router.route.call_args_list[0].kwargs["start_date"] == "2011-01-04"
        assert updater.router.route.call_args_list[0].kwargs["end_date"] == "2011-02-03"
        assert updater.router.route.call_args_list[1].kwargs["start_date"] == "2011-02-04"
        assert updater.router.route.call_args_list[1].kwargs["end_date"] == "2011-02-05"
        assert updater.data_ops.insert_futures_spot_basis_batch.await_count == 2

    asyncio.run(_run())


def test_update_futures_spot_basis_smart_without_existing_data_uses_full_history_range():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_spot_basis_batch = AsyncMock(return_value=1)
        updater.router = Mock(
            route=Mock(
                return_value=pd.DataFrame(
                    {"time": ["2011-01-04"], "product_code": ["RB"]}
                )
            )
        )

        inserted = await updater.update_futures_spot_basis(
            product_codes=["all"],
            trade_date=None,
            start_date=None,
            end_date="2011-01-10",
            force_update=False,
        )

        assert inserted == 1
        updater.data_ops.get_latest_futures_date.assert_awaited_once_with("spot_basis")
        assert updater.router.route.call_args.kwargs["products"] is None
        assert updater.router.route.call_args.kwargs["start_date"] == "2011-01-04"
        assert updater.router.route.call_args.kwargs["end_date"] == "2011-01-10"
        updater.data_ops.insert_futures_spot_basis_batch.assert_awaited_once()

    asyncio.run(_run())


def test_update_futures_spot_basis_smart_uses_next_day_after_latest():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value="2024-04-30")
        updater.data_ops.insert_futures_spot_basis_batch = AsyncMock(return_value=1)
        updater.router = Mock(
            route=Mock(
                return_value=pd.DataFrame(
                    {"time": ["2024-05-01"], "product_code": ["RB"]}
                )
            )
        )

        inserted = await updater.update_futures_spot_basis(
            product_codes=None,
            trade_date=None,
            start_date=None,
            end_date="2024-05-10",
            force_update=False,
        )

        assert inserted == 1
        assert updater.router.route.call_args.kwargs["start_date"] == "2024-05-01"
        assert updater.router.route.call_args.kwargs["end_date"] == "2024-05-10"
        updater.data_ops.insert_futures_spot_basis_batch.assert_awaited_once()

    asyncio.run(_run())


def test_update_futures_spot_basis_persists_completed_chunks_before_failure():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.insert_futures_spot_basis_batch = AsyncMock(return_value=1)
        updater.router = Mock()
        updater.router.route = Mock(
            side_effect=[
                pd.DataFrame({"time": ["2011-01-04"], "product_code": ["RB"]}),
                RuntimeError("source failed"),
            ]
        )

        with pytest.raises(RuntimeError, match="source failed"):
            await updater.update_futures_spot_basis(
                product_codes=None,
                trade_date=None,
                start_date=None,
                end_date="2011-02-05",
                force_update=True,
            )

        assert updater.router.route.call_count == 2
        updater.data_ops.insert_futures_spot_basis_batch.assert_awaited_once()

    asyncio.run(_run())


def test_update_futures_minute_uses_bounded_parallel_downloads():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 2
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_minute_batch = AsyncMock(return_value=1)
        updater.router = Mock()

        active = 0
        max_seen = 0
        lock = threading.Lock()

        def route_side_effect(**kwargs):
            nonlocal active, max_seen
            with lock:
                active += 1
                max_seen = max(max_seen, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            return pd.DataFrame(
                {
                    "time": ["2024-04-30 09:30:00"],
                    "symbol": [kwargs["symbol"]],
                    "frequency": [kwargs["freq"]],
                }
            )

        updater.router.route = Mock(side_effect=route_side_effect)

        inserted = await updater.update_futures_minute(
            symbols=["RB2405.SHF", "RB2406.SHF", "RB2407.SHF"],
            trade_date=None,
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="1m",
            force_update=True,
        )

        assert inserted == 3
        assert max_seen == 2
        assert updater.router.route.call_count == 3
        assert all(
            call.kwargs["wait_for_circuit_breaker"]
            for call in updater.router.route.call_args_list
        )
        assert updater.data_ops.insert_futures_minute_batch.await_count == 3
        assert updater.last_futures_minute_summary == {
            "total_symbols": 3,
            "attempted_symbols": 3,
            "inserted_symbols": 3,
            "empty_symbols": 0,
            "up_to_date_symbols": 0,
            "failed_symbols": [],
            "inserted_records": 3,
        }

    asyncio.run(_run())


def test_update_futures_minute_derived_frequency_skips_provider_download():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 2
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.router = Mock()

        inserted = await updater.update_futures_minute(
            symbols=["RB2405.SHF"],
            trade_date=None,
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="15m",
            force_update=True,
        )

        assert inserted == 0
        updater.router.route.assert_not_called()
        assert updater.last_futures_minute_summary["total_symbols"] == 0

    asyncio.run(_run())


def test_update_futures_minute_5m_downloads_from_provider():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 2
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_minute_batch = AsyncMock(return_value=1)
        updater.router = Mock()
        updater.router.route = Mock(
            return_value=pd.DataFrame(
                {
                    "time": ["2024-04-30 09:30:00"],
                    "symbol": ["RB2405.SHF"],
                    "frequency": ["5m"],
                }
            )
        )

        inserted = await updater.update_futures_minute(
            symbols=["RB2405.SHF"],
            trade_date=None,
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="5m",
            force_update=True,
        )

        assert inserted == 1
        updater.router.route.assert_called_once()
        assert updater.router.route.call_args.kwargs["freq"] == "5m"
        updater.data_ops.insert_futures_minute_batch.assert_awaited_once()

    asyncio.run(_run())


def test_update_futures_minute_skips_symbols_without_download_when_up_to_date():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 4
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(
            return_value="2024-05-01"
        )
        updater.data_ops.insert_futures_minute_batch = AsyncMock()
        updater.router = Mock()
        updater.router.route = Mock()

        progress = []
        inserted = await updater.update_futures_minute(
            symbols=["RB2405.SHF", "RB2406.SHF"],
            trade_date=None,
            start_date=None,
            end_date="2024-04-30 10:00:00",
            freq="1m",
            force_update=False,
            progress_callback=lambda current, total: progress.append((current, total)),
        )

        assert inserted == 0
        updater.router.route.assert_not_called()
        updater.data_ops.insert_futures_minute_batch.assert_not_called()
        assert progress == [(1, 2), (2, 2)]
        assert updater.last_futures_minute_summary["up_to_date_symbols"] == 2

    asyncio.run(_run())


def test_update_futures_minute_skips_failed_symbol_and_continues():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 2
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_minute_batch = AsyncMock(return_value=1)
        updater.router = Mock()

        def route_side_effect(**kwargs):
            if kwargs["symbol"] == "BAD.SHF":
                raise AttributeError("'NoneType' object has no attribute 'lower'")
            return pd.DataFrame(
                {
                    "time": ["2024-04-30 09:30:00"],
                    "symbol": [kwargs["symbol"]],
                    "frequency": [kwargs["freq"]],
                }
            )

        updater.router.route = Mock(side_effect=route_side_effect)
        progress = []

        inserted = await updater.update_futures_minute(
            symbols=["RB2405.SHF", "BAD.SHF", "RB2406.SHF"],
            trade_date=None,
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="1m",
            force_update=True,
            progress_callback=lambda current, total: progress.append((current, total)),
        )

        assert inserted == 2
        assert updater.router.route.call_count == 3
        assert updater.data_ops.insert_futures_minute_batch.await_count == 2
        assert progress[-1] == (3, 3)
        assert updater.last_futures_minute_summary["total_symbols"] == 3
        assert updater.last_futures_minute_summary["inserted_symbols"] == 2
        assert updater.last_futures_minute_summary["failed_symbols"] == [
            {
                "symbol": "BAD.SHF",
                "error": "'NoneType' object has no attribute 'lower'",
            }
        ]

    asyncio.run(_run())


def test_update_futures_minute_skips_xtquant_unsupported_synthetic_symbols():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 2
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_minute_batch = AsyncMock(return_value=1)
        updater.router = Mock()
        updater.router.route = Mock(
            return_value=pd.DataFrame(
                {
                    "time": ["2024-04-30 09:30:00"],
                    "symbol": ["RB2405.SHF"],
                    "frequency": ["1m"],
                }
            )
        )

        progress = []
        inserted = await updater.update_futures_minute(
            symbols=["L_F.DCE", "PP_FL.DCE", "RB2405.SHF"],
            trade_date=None,
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="1m",
            force_update=True,
            progress_callback=lambda current, total: progress.append((current, total)),
        )

        assert inserted == 1
        updater.data_ops.get_latest_futures_date.assert_awaited_once_with(
            "minute", symbol="RB2405.SHF", frequency="1m"
        )
        updater.router.route.assert_called_once()
        assert updater.router.route.call_args.kwargs["symbol"] == "RB2405.SHF"
        assert progress == [(1, 1)]
        assert updater.last_futures_minute_summary["total_symbols"] == 1
        assert updater.last_futures_minute_summary["failed_symbols"] == []

    asyncio.run(_run())


def test_update_futures_minute_trade_date_filters_contract_universe():
    async def _run():
        settings = Mock()
        settings.data_source.futures_minute_max_workers = 2
        updater = DataUpdater(settings=settings)
        updater.data_ops = Mock()
        updater.data_ops.get_futures_contract_symbols = AsyncMock(
            return_value=["RB2405.SHF"]
        )
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_minute_batch = AsyncMock(return_value=1)
        updater.router = Mock(
            route=Mock(
                return_value=pd.DataFrame(
                    {
                        "time": ["2024-04-30 09:30:00"],
                        "symbol": ["RB2405.SHF"],
                        "frequency": ["1m"],
                    }
                )
            )
        )

        inserted = await updater.update_futures_minute(
            symbols=["all"],
            trade_date="2024-04-30",
            start_date=None,
            end_date=None,
            freq="1m",
            force_update=False,
        )

        assert inserted == 1
        updater.data_ops.get_futures_contract_symbols.assert_awaited_once_with(
            contract_types=["normal", "main", "continuous"],
            active_only=False,
            overlap_start="2024-04-30",
            overlap_end="2024-04-30",
        )
        assert updater.router.route.call_args.kwargs["start_date"] == "2024-04-30"
        assert updater.router.route.call_args.kwargs["end_date"] == "2024-04-30"

    asyncio.run(_run())


def test_preprocess_futures_term_metrics_flushes_batches_during_processing(monkeypatch):
    async def _run():
        monkeypatch.setattr(updater_module, "FUTURES_TERM_INSERT_BATCH_SIZE", 1)
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        raw = pd.DataFrame(
            {
                "time": [
                    pd.Timestamp("2024-04-30"),
                    pd.Timestamp("2024-04-30"),
                    pd.Timestamp("2024-05-01"),
                    pd.Timestamp("2024-05-01"),
                ],
                "symbol": ["ZZ2405.SHF", "ZZ2406.SHF", "ZZ2405.SHF", "ZZ2406.SHF"],
                "product_code": ["ZZ", "ZZ", "ZZ", "ZZ"],
                "exchange": ["SHFE", "SHFE", "SHFE", "SHFE"],
                "close": [3600.0, 3650.0, 3610.0, 3660.0],
                "settle": [3601.0, 3651.0, 3611.0, 3661.0],
                "open_interest": [1000, 900, 1000, 900],
                "last_ddate": [
                    pd.Timestamp("2024-05-15"),
                    pd.Timestamp("2024-06-15"),
                    pd.Timestamp("2024-05-15"),
                    pd.Timestamp("2024-06-15"),
                ],
                "delivery_month": [5, 6, 5, 6],
                "delivery_month_start": [
                    pd.Timestamp("2024-05-01"),
                    pd.Timestamp("2024-06-01"),
                    pd.Timestamp("2024-05-01"),
                    pd.Timestamp("2024-06-01"),
                ],
            }
        )
        updater.data_ops.get_futures_daily_for_preprocess = AsyncMock(return_value=raw)
        events = []

        async def insert_batch(data):
            events.append(("insert", len(data)))
            return len(data)

        updater.data_ops.insert_futures_term_metrics_batch = AsyncMock(
            side_effect=insert_batch
        )

        count = await updater.preprocess_futures_term_metrics(
            product_codes=["ZZ"],
            start_date="2024-04-30",
            end_date="2024-05-01",
            progress_callback=lambda current, total: events.append(
                ("progress", current, total)
            ),
        )

        assert count == 2
        assert events[0] == ("progress", 0, 2)
        assert events[1] == ("insert", 1)
        assert events[2] == ("progress", 1, 2)
        assert events[3] == ("insert", 1)
        assert events[-1] == ("progress", 2, 2)
        assert updater.data_ops.insert_futures_term_metrics_batch.await_count == 2

    asyncio.run(_run())


def test_update_futures_inventory_history_filters_unsupported_exchanges():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.get_futures_contracts = AsyncMock(
            return_value=pd.DataFrame(
                {
                    "product_code": ["IC", "RB", "SC", "SI"],
                    "name": ["中证500期货", "螺纹钢", "原油", "工业硅"],
                    "exchange": ["CFFEX", "SHFE", "INE", "GFEX"],
                }
            )
        )
        updater.data_ops.insert_futures_inventory_batch = AsyncMock(return_value=1)
        updater.router = Mock(
            route=Mock(return_value=pd.DataFrame({"product_code": ["RB"]}))
        )

        inserted = await updater.update_futures_inventory(
            product_codes=["all"],
            start_date=None,
            end_date="2024-04-30",
            force_update=True,
        )

        assert inserted == 1
        updater.data_ops.get_futures_contracts.assert_awaited_once_with(
            product_codes=None
        )
        assert updater.router.route.call_args.kwargs["product_names"] == {
            "RB": "RB",
            "SI": "SI",
        }
        assert updater.router.route.call_args.kwargs["products"] is None
        assert updater.router.route.call_args.kwargs["use_history"] is True

    asyncio.run(_run())
