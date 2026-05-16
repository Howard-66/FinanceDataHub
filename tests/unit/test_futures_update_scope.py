import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import pandas as pd

from finance_data_hub.update.updater import DataUpdater, _is_all_futures_selector


def test_is_all_futures_selector():
    assert _is_all_futures_selector(["all"])
    assert _is_all_futures_selector(["ALL"])
    assert not _is_all_futures_selector(["RB"])
    assert not _is_all_futures_selector(["all", "RB"])


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
            start_date=None,
            end_date="2024-04-30",
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_awaited_once_with(
            contract_types=["main"],
            active_only=True,
            overlap_start=None,
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
            start_date=None,
            end_date="2024-04-30",
            force_update=False,
            default_active_only=True,
        )

        assert resolved == ["RB2405.SHF", "RB.SHF"]
        updater.data_ops.get_futures_contract_symbols.assert_not_called()

    asyncio.run(_run())


def test_update_futures_spot_basis_falls_back_to_previous_trading_day_for_default_single_day():
    async def _run():
        updater = DataUpdater(settings=Mock())
        updater.data_ops = Mock()
        updater.data_ops.get_latest_futures_date = AsyncMock(return_value=None)
        updater.data_ops.insert_futures_spot_basis_batch = AsyncMock(return_value=1)
        updater.router = Mock()
        updater.router.route = Mock(
            side_effect=[
                pd.DataFrame(),
                pd.DataFrame({"time": ["2026-05-15"], "product_code": ["RB"]}),
            ]
        )

        inserted = await updater.update_futures_spot_basis(
            product_codes=None,
            trade_date=None,
            start_date=None,
            end_date="2026-05-16",
            force_update=True,
        )

        assert inserted == 1
        assert updater.router.route.call_count == 2
        assert updater.router.route.call_args_list[0].kwargs["start_date"] == "2026-05-16"
        assert updater.router.route.call_args_list[0].kwargs["end_date"] == "2026-05-16"
        assert updater.router.route.call_args_list[1].kwargs["start_date"] == "2026-05-15"
        assert updater.router.route.call_args_list[1].kwargs["end_date"] == "2026-05-15"
        updater.data_ops.insert_futures_spot_basis_batch.assert_awaited_once()

    asyncio.run(_run())
