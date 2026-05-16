from unittest.mock import Mock, patch

import pandas as pd
import pytest

from finance_data_hub.providers.base import ProviderRateLimitError
from finance_data_hub.providers.tushare import TushareProvider


def test_fut_settle_rate_limit_uses_endpoint_specific_interval():
    provider = TushareProvider(config={"token": "test-token"})

    with patch("finance_data_hub.providers.tushare.time.time") as mock_time:
        with patch("finance_data_hub.providers.tushare.time.sleep") as mock_sleep:
            mock_time.side_effect = [100.0, 100.0, 100.1, 100.84]

            provider._rate_limit_check("fut_settle")
            provider._rate_limit_check("fut_settle")

    mock_sleep.assert_called_once_with(pytest.approx(0.74, abs=1e-6))


def test_call_api_recognizes_futures_rate_limit_message():
    provider = TushareProvider(config={"token": "test-token"})
    provider._is_initialized = True
    provider.max_retry = 0

    api = Mock(
        side_effect=Exception(
            "抱歉，您访问接口(fut_settle)频率超限(80次/分钟)，具体频次详情："
            "https://tushare.pro/document/1?doc_id=108。"
        )
    )
    provider.pro_api = Mock(fut_settle=api)

    with patch.object(provider, "_rate_limit_check") as mock_rate_limit_check:
        with pytest.raises(ProviderRateLimitError):
            provider._call_api("fut_settle", fields="ts_code,trade_date")

    mock_rate_limit_check.assert_called_once_with("fut_settle")
    assert provider._api_rate_limits_per_minute["fut_settle"] == 80.0


def test_call_api_returns_dataframe_after_successful_call():
    provider = TushareProvider(config={"token": "test-token"})
    provider._is_initialized = True
    provider.pro_api = Mock(
        fut_settle=Mock(return_value=pd.DataFrame({"ts_code": ["RB2405.SHF"]}))
    )

    with patch.object(provider, "_rate_limit_check"):
        result = provider._call_api("fut_settle", fields="ts_code")

    assert not result.empty
    assert list(result["ts_code"]) == ["RB2405.SHF"]
