from unittest.mock import Mock

import pytest

from finance_data_hub.providers.base import ProviderDataError
from finance_data_hub.providers.xtquant import XTQuantProvider


def test_xtquant_dataframe_conversion_matches_lowercase_futures_symbol():
    provider = XTQuantProvider()
    payload = {
        "rb2405.SF": {
            "time": {0: 20240430000000},
            "open": {0: 100.0},
            "high": {0: 101.0},
            "low": {0: 99.0},
            "close": {0: 100.5},
            "volume": {0: 1000},
            "amount": {0: 100000.0},
        }
    }

    result = provider._convert_dict_to_dataframe(payload, "rb2405.SF")

    assert len(result) == 1
    assert result["open"].iloc[0] == 100.0


def test_xtquant_futures_minute_rejects_unsupported_frequency():
    provider = XTQuantProvider()

    with pytest.raises(ProviderDataError, match="only supports 1m, 5m"):
        provider.get_futures_minute(
            symbols=["rb2405.SF"],
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="15m",
        )


def test_xtquant_futures_minute_5m_uses_direct_xtquant_period():
    provider = XTQuantProvider()
    payload = {
        "rb2405.SF": {
            "time": {0: 20240430093000},
            "open": {0: 100.0},
            "high": {0: 101.0},
            "low": {0: 99.0},
            "close": {0: 100.5},
            "volume": {0: 1000},
            "amount": {0: 100000.0},
        }
    }
    provider._call_api = Mock(side_effect=[None, payload])

    result = provider.get_futures_minute(
        symbols=["rb2405.SF"],
        start_date="2024-04-30 09:30:00",
        end_date="2024-04-30 10:00:00",
        freq="5m",
    )

    assert len(result) == 1
    assert result["frequency"].iloc[0] == "5m"
    assert provider._call_api.call_args_list[0].args[1]["period"] == "5m"
    assert provider._call_api.call_args_list[1].args[1]["period"] == "5m"


def test_xtquant_futures_minute_rejects_monthly_average_symbol_before_api_call():
    provider = XTQuantProvider()

    with pytest.raises(ProviderDataError, match="Unsupported XTQuant futures symbol"):
        provider.get_futures_minute(
            symbols=["L_F.DCE"],
            start_date="2024-04-30 09:30:00",
            end_date="2024-04-30 10:00:00",
            freq="1m",
        )
