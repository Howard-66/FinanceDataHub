from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.sdk import FinanceDataHub


@pytest.mark.asyncio
async def test_get_index_basic_async_delegates_filters_to_data_operations():
    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock()
    fdh.ops.get_index_basic = AsyncMock(
        return_value=pd.DataFrame(
            {
                "ts_code": ["000300.SH"],
                "name": ["沪深300"],
                "market": ["SSE"],
            }
        )
    )

    result = await fdh.get_index_basic_async(market="SSE")

    assert result is not None
    assert result.iloc[0]["ts_code"] == "000300.SH"
    fdh.ops.get_index_basic.assert_awaited_once_with(
        ts_code=None,
        market="SSE",
        publisher=None,
        category=None,
    )
