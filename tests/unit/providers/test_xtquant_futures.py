from unittest.mock import Mock

import httpx
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


def test_xtquant_initialize_falls_back_to_endpoint_probe_on_root_timeout():
    provider = XTQuantProvider(
        config={"api_url": "http://helper:8100", "health_timeout": 1}
    )
    provider.client = Mock()
    provider.client.get = Mock(
        side_effect=[
            httpx.Response(
                404,
                request=httpx.Request("GET", "http://helper:8100/health"),
            ),
            httpx.ReadTimeout("root timeout"),
            httpx.Response(
                405,
                request=httpx.Request(
                    "GET", "http://helper:8100/download_history_data"
                ),
            ),
        ]
    )

    provider._probe_helper_connectivity()

    assert provider.client.get.call_args_list[0].args[0] == "/health"
    assert provider.client.get.call_args_list[1].args[0] == "/"
    assert provider.client.get.call_args_list[2].args[0] == "/download_history_data"


def test_xtquant_initialize_uses_health_endpoint_first():
    provider = XTQuantProvider(
        config={"api_url": "http://helper:8100", "health_timeout": 1}
    )
    provider.client = Mock()
    provider.client.get = Mock(
        return_value=httpx.Response(
            200,
            json={"status": "ok", "service": "xtquant_helper"},
            request=httpx.Request("GET", "http://helper:8100/health"),
        )
    )

    provider._probe_helper_connectivity()

    provider.client.get.assert_called_once_with("/health", timeout=1)
