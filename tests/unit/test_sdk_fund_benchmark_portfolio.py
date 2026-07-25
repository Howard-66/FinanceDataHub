from unittest.mock import AsyncMock, Mock
import asyncio

import pandas as pd

from finance_data_hub.sdk import FinanceDataHub


def test_fund_benchmark_and_portfolio_sdk_delegate_to_operations():
    asyncio.run(_test_fund_benchmark_and_portfolio_sdk_delegate_to_operations())


async def _test_fund_benchmark_and_portfolio_sdk_delegate_to_operations():
    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock()
    fdh.ops.get_mkt_idx_bmk = AsyncMock(return_value=pd.DataFrame({"ts_code": ["000300.SH"]}))
    fdh.ops.get_fund_portfolio = AsyncMock(return_value=pd.DataFrame({"ts_code": ["001753.OF"]}))

    await fdh.get_mkt_idx_bmk_async(bmk_level="一类库")
    await fdh.get_fund_portfolio_async(ts_code="001753.OF", period="20240630")

    fdh.ops.get_mkt_idx_bmk.assert_awaited_once_with(None, None, "一类库")
    fdh.ops.get_fund_portfolio.assert_awaited_once_with(
        "001753.OF", None, None, "20240630", None, None
    )
