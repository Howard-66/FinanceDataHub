from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.database.operations import (
    DataOperations,
    _normalize_futures_minute_frequency,
)


def test_normalize_futures_minute_frequency_aliases():
    assert _normalize_futures_minute_frequency("minute") == "1m"
    assert _normalize_futures_minute_frequency("minute_1") == "1m"
    assert _normalize_futures_minute_frequency("minute_15") == "15m"
    assert _normalize_futures_minute_frequency("1h") == "60m"


def test_insert_futures_minute_batch_writes_only_1m_raw_rows():
    async def _run():
        ops = DataOperations(Mock())
        ops._insert_futures_dataframe = AsyncMock(return_value=1)

        data = pd.DataFrame(
            {
                "time": ["2024-04-30 09:30:00", "2024-04-30 09:35:00"],
                "symbol": ["RB2405.SHF", "RB2405.SHF"],
                "frequency": ["1m", "5m"],
                "open": [100.0, 101.0],
            }
        )

        inserted = await ops.insert_futures_minute_batch(data)

        assert inserted == 1
        ops._insert_futures_dataframe.assert_awaited_once()
        args = ops._insert_futures_dataframe.await_args.args
        assert args[0] == "minute_1m"
        written = args[1]
        assert len(written) == 1
        assert written["frequency"].iloc[0] == "1m"
        assert "frequency" not in args[2]
        assert args[3] == ["symbol", "time"]

    import asyncio

    asyncio.run(_run())


def test_get_futures_minute_rejects_unsupported_frequency():
    async def _run():
        ops = DataOperations(Mock())

        with pytest.raises(ValueError, match="Unsupported futures minute frequency"):
            await ops.get_futures_minute(
                ["RB2405.SHF"],
                "2024-04-30",
                "2024-04-30",
                "2m",
            )

    import asyncio

    asyncio.run(_run())
