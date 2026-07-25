from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.sdk import FinanceDataHub


@pytest.mark.asyncio
async def test_get_fund_basic_async_delegates_filters_to_data_operations():
    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock()
    fdh.ops.get_fund_basic = AsyncMock(
        return_value=pd.DataFrame(
            {
                "ts_code": ["510300.SH"],
                "name": ["沪深300ETF"],
                "market": ["E"],
            }
        )
    )

    result = await fdh.get_fund_basic_async(market="E", status="L")

    assert result is not None
    assert result.iloc[0]["ts_code"] == "510300.SH"
    fdh.ops.get_fund_basic.assert_awaited_once_with(
        ts_code=None,
        market="E",
        status="L",
        fund_type=None,
        management=None,
    )
