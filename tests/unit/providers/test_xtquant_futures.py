from unittest.mock import Mock, patch

import httpx
import pytest

from finance_data_hub.providers.base import ProviderDataError, ProviderRateLimitError
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
    assert (
        provider._call_api.call_args_list[0].args[1]["start_time"]
        == "20240430093000"
    )
    assert (
        provider._call_api.call_args_list[0].args[1]["end_time"]
        == "20240430100000"
    )


def test_xtquant_futures_minute_filters_cross_midnight_trading_window():
    provider = XTQuantProvider()
    payload = {
        "rb2405.SF": {
            "time": {
                0: 20240430150000,
                1: 20240430230000,
                2: 20240501023000,
                3: 20240501160000,
            },
            "open": {0: 99.0, 1: 100.0, 2: 101.0, 3: 102.0},
            "high": {0: 99.5, 1: 100.5, 2: 101.5, 3: 102.5},
            "low": {0: 98.5, 1: 99.5, 2: 100.5, 3: 101.5},
            "close": {0: 99.2, 1: 100.2, 2: 101.2, 3: 102.2},
            "volume": {0: 10, 1: 20, 2: 30, 3: 40},
            "amount": {0: 1000.0, 1: 2000.0, 2: 3000.0, 3: 4000.0},
        }
    }
    provider._call_api = Mock(side_effect=[None, payload])

    result = provider.get_futures_minute(
        symbols=["rb2405.SF"],
        start_date="2024-04-30 21:00:00",
        end_date="2024-05-01 15:00:00",
        freq="1m",
    )

    assert result["time"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist() == [
        "2024-04-30 23:00:00",
        "2024-05-01 02:30:00",
    ]
    assert (
        provider._call_api.call_args_list[0].args[1]["start_time"]
        == "20240430210000"
    )
    assert (
        provider._call_api.call_args_list[0].args[1]["end_time"]
        == "20240501150000"
    )
    assert (
        provider._call_api.call_args_list[1].args[1]["start_time"]
        == "20240430210000"
    )
    assert (
        provider._call_api.call_args_list[1].args[1]["end_time"]
        == "20240501150000"
    )


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


def test_xtquant_call_api_retries_helper_503_and_returns_success():
    provider = XTQuantProvider(config={"max_retry": 1, "retry_delay": 0})
    provider._is_initialized = True
    provider.client = Mock()
    provider.client.post = Mock(
        side_effect=[
            httpx.Response(
                503,
                request=httpx.Request("POST", "http://helper:8100/get_local_data"),
            ),
            httpx.Response(
                200,
                json={"result": {"ok": True}},
                request=httpx.Request("POST", "http://helper:8100/get_local_data"),
            ),
        ]
    )

    with patch("finance_data_hub.providers.base.time.sleep") as sleep:
        result = provider._call_api("/get_local_data", {"symbol": "00700.HK"})

    assert result == {"result": {"ok": True}}
    assert provider.client.post.call_count == 2
    sleep.assert_called_once_with(0)


def test_xtquant_call_api_exhausts_helper_503_retries():
    provider = XTQuantProvider(config={"max_retry": 1, "retry_delay": 0})
    provider._is_initialized = True
    provider.client = Mock()
    provider.client.post = Mock(
        return_value=httpx.Response(
            503,
            request=httpx.Request("POST", "http://helper:8100/get_local_data"),
        )
    )

    with patch("finance_data_hub.providers.base.time.sleep"):
        with pytest.raises(ProviderRateLimitError, match="503"):
            provider._call_api("/get_local_data", {"symbol": "00700.HK"})

    assert provider.client.post.call_count == 2
