from unittest.mock import Mock, patch

import pandas as pd
import pytest

from finance_data_hub.providers.base import ProviderRateLimitError
from finance_data_hub.providers.tushare import TUSHARE_INDEX_MARKETS, TushareProvider


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


def test_index_basic_codes_resolves_catalog_and_excludes_markets():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        side_effect=[
            pd.DataFrame(
                {
                    "ts_code": ["000300.CSI", "000905.CSI"],
                    "market": ["CSI", "CSI"],
                }
            ),
            pd.DataFrame(
                {
                    "ts_code": ["801010.SI", "801020.SI"],
                    "market": ["SW", "SW"],
                }
            ),
        ]
    )

    result = provider.get_index_basic_codes(
        markets=["CSI", "SW"],
        exclude_markets=["SW"],
    )

    assert result == ["000300.CSI", "000905.CSI"]
    assert provider._call_api.call_count == 2


def test_index_daily_trade_date_fetches_all_indexes_without_ts_code():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        return_value=pd.DataFrame(
            {
                "ts_code": ["000300.CSI", "000905.CSI"],
                "trade_date": ["20240102", "20240102"],
                "close": [1.0, 2.0],
                "open": [1.0, 2.0],
                "high": [1.1, 2.1],
                "low": [0.9, 1.9],
                "pre_close": [0.95, 1.95],
                "change": [0.05, 0.05],
                "pct_chg": [5.0, 2.5],
                "vol": [100.0, 200.0],
                "amount": [1000.0, 2000.0],
            }
        )
    )

    result = provider.get_index_daily(trade_date="2024-01-02")

    assert list(result["ts_code"]) == ["000300.CSI", "000905.CSI"]
    provider._call_api.assert_called_once()
    assert "ts_code" not in provider._call_api.call_args.kwargs
    assert provider._call_api.call_args.kwargs["trade_date"] == "20240102"


def test_index_basic_normalizes_dates_and_returns_tushare_fields():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        return_value=pd.DataFrame(
            {
                "ts_code": ["000300.SH"],
                "name": ["沪深300"],
                "fullname": ["沪深300指数"],
                "market": ["SSE"],
                "publisher": ["中证指数有限公司"],
                "index_type": ["规模指数"],
                "category": ["规模指数"],
                "base_date": ["20041231"],
                "base_point": [1000],
                "list_date": ["20050408"],
                "weight_rule": ["市值加权"],
                "desc": ["测试数据"],
                "exp_date": [None],
            }
        )
    )

    result = provider.get_index_basic(markets=["sse"])

    assert result.iloc[0]["ts_code"] == "000300.SH"
    assert result.iloc[0]["base_date"] == pd.Timestamp("2004-12-31")
    assert result.iloc[0]["list_date"] == pd.Timestamp("2005-04-08")
    assert pd.isna(result.iloc[0]["exp_date"])
    provider._call_api.assert_called_once()
    assert provider._call_api.call_args.kwargs["market"] == "SSE"


def test_index_basic_ignores_router_market_when_markets_are_explicit():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        return_value=pd.DataFrame(
            {"ts_code": ["000001.SH"], "name": ["上证综指"], "market": ["SSE"]}
        )
    )

    provider.get_index_basic(market="CN", markets=["SSE"])

    provider._call_api.assert_called_once()
    assert provider._call_api.call_args.kwargs["market"] == "SSE"


def test_index_basic_uses_all_tushare_markets_when_router_passes_cn():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        return_value=pd.DataFrame(
            {"ts_code": ["000001.SH"], "name": ["上证综指"], "market": ["SSE"]}
        )
    )

    result = provider.get_index_basic(market="CN")

    assert len(result) == 1
    assert provider._call_api.call_count == len(TUSHARE_INDEX_MARKETS)


def test_futures_monthly_normalizes_period_end_and_deduplicates():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        return_value=pd.DataFrame(
            {
                "ts_code": ["RB.SHF", "RB.SHF"],
                "trade_date": ["20260529", "20260531"],
                "close": [3157.0, 3158.0],
            }
        )
    )

    result = provider.get_futures_monthly(
        symbol="RB.SHF",
        start_date="2026-05-01",
        end_date="2026-05-31",
    )

    assert len(result) == 1
    assert result.iloc[0]["time"] == pd.Timestamp("2026-05-31")
    assert result.iloc[0]["close"] == 3158.0
