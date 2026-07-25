from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from finance_data_hub.sdk import FinanceDataHub


@pytest.mark.asyncio
async def test_fund_company_and_manager_sdk_delegate_filters_to_data_operations():
    fdh = object.__new__(FinanceDataHub)
    fdh.ops = Mock()
    fdh.ops.get_fund_company = AsyncMock(return_value=pd.DataFrame({"name": ["示例基金"]}))
    fdh.ops.get_fund_manager = AsyncMock(
        return_value=pd.DataFrame({"ts_code": ["150018.SZ"], "name": ["周毅"]})
    )

    companies = await fdh.get_fund_company_async(province="北京市")
    managers = await fdh.get_fund_manager_async(
        ts_code="150018.SZ", ann_date="20100508", name="周毅"
    )

    assert companies is not None
    assert managers is not None
    fdh.ops.get_fund_company.assert_awaited_once_with(
        name=None, province="北京市", city=None
    )
    fdh.ops.get_fund_manager.assert_awaited_once_with(
        ts_code="150018.SZ", ann_date="20100508", name="周毅"
    )
