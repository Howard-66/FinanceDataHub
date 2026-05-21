import pandas as pd
import finance_data_hub.providers.akshare_provider as akshare_provider_module

from finance_data_hub.providers.akshare_provider import AKShareProvider
from finance_data_hub.providers.base import ProviderDataError


def test_akshare_spot_basis_recomputes_basis_sign():
    provider = AKShareProvider()
    provider.initialize()

    raw = pd.DataFrame(
        {
            "date": ["2024-04-30"],
            "symbol": ["RB"],
            "spot_price": [100.0],
            "near_contract": ["RB2405"],
            "near_contract_price": [90.0],
            "dominant_contract": ["RB2410"],
            "dominant_contract_price": [80.0],
            "near_basis": [-10.0],
            "dom_basis": [-20.0],
            "near_basis_rate": [-0.1],
            "dom_basis_rate": [-0.2],
        }
    )

    result = provider._normalize_spot_basis(raw)

    assert result["product_code"].iloc[0] == "RB"
    assert result["near_basis"].iloc[0] == 10.0
    assert result["dom_basis"].iloc[0] == 20.0
    assert result["near_basis_rate"].iloc[0] == 0.1
    assert result["dom_basis_rate"].iloc[0] == 0.2


def test_akshare_spot_basis_accepts_native_columns():
    provider = AKShareProvider()
    provider.initialize()

    raw = pd.DataFrame(
        {
            "date": ["20240430"],
            "var": ["RB"],
            "sp": [100.0],
            "near_symbol": ["RB2405"],
            "near_price": [90.0],
            "dom_symbol": ["RB2410"],
            "dom_price": [80.0],
            "near_basis": [-10.0],
            "dom_basis": [-20.0],
            "near_basis_rate": [-0.1],
            "dom_basis_rate": [-0.2],
        }
    )

    result = provider._normalize_spot_basis(raw)

    assert result["spot_price"].iloc[0] == 100.0
    assert result["near_contract"].iloc[0] == "RB2405"
    assert result["dominant_contract"].iloc[0] == "RB2410"
    assert result["near_basis"].iloc[0] == 10.0


def test_akshare_inventory_accepts_99_inventory_columns():
    provider = AKShareProvider()
    provider.initialize()

    raw = pd.DataFrame(
        {
            "日期": ["2024-04-30"],
            "库存": [1234.0],
            "product_code": ["RB"],
        }
    )

    result = provider._normalize_receipt(raw)

    assert set(result.columns) == {"time", "product_code", "inventory", "source"}
    assert result["product_code"].iloc[0] == "RB"
    assert result["inventory"].iloc[0] == 1234.0


def test_akshare_inventory_maps_receipt_to_inventory_and_drops_detail_columns():
    provider = AKShareProvider()
    provider.initialize()

    raw = pd.DataFrame(
        {
            "date": ["2024-04-30", "2024-04-30"],
            "var": ["rb", "RB"],
            "receipt": [100.0, 23.0],
            "exchange": ["SHF", "SHF"],
            "warehouse": ["A", "B"],
            "region": ["East", "East"],
        }
    )

    result = provider._normalize_receipt(raw)

    assert set(result.columns) == {"time", "product_code", "inventory", "source"}
    assert len(result) == 1
    assert result["product_code"].iloc[0] == "RB"
    assert result["inventory"].iloc[0] == 123.0


def test_akshare_spot_basis_all_products_does_not_pass_none_vars_list():
    provider = AKShareProvider()
    provider.initialize()
    calls = []

    def fake_call(func, *args, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame()

    provider._call_akshare = fake_call

    provider.get_futures_spot_basis(
        start_date="2024-04-01",
        end_date="2024-04-02",
        products=None,
    )

    assert "vars_list" not in calls[0]


def test_akshare_spot_basis_specific_products_passes_vars_list():
    provider = AKShareProvider()
    provider.initialize()
    calls = []

    def fake_call(func, *args, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame()

    provider._call_akshare = fake_call

    provider.get_futures_spot_basis(
        start_date="2024-04-01",
        end_date="2024-04-02",
        products=["RB"],
    )

    assert calls[0]["vars_list"] == ["RB"]


def test_akshare_inventory_all_products_does_not_pass_none_vars_list():
    provider = AKShareProvider()
    provider.initialize()
    calls = []

    def fake_call(func, *args, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame()

    provider._call_akshare = fake_call

    provider.get_futures_inventory(
        start_date="2024-04-01",
        end_date="2024-04-01",
        products=None,
    )

    assert "vars_list" not in calls[0]


def test_akshare_inventory_incremental_falls_back_when_get_receipt_fails():
    provider = AKShareProvider(config={"max_retry": 0})
    provider.initialize()

    def fake_call(func, *args, **kwargs):
        if func.__name__ == "get_receipt":
            raise ProviderDataError(
                "AKShare call get_receipt failed: Expecting value: line 1 column 1 (char 0)",
                provider_name=provider.name,
            )
        raise AssertionError(f"Unexpected function call: {func.__name__}")

    provider._call_akshare = fake_call
    provider._get_receipt_by_market = lambda **kwargs: pd.DataFrame(
        {
            "date": ["2024-04-30"],
            "var": ["rb"],
            "receipt": [123.0],
        }
    )

    result = provider.get_futures_inventory(
        start_date="2024-04-30",
        end_date="2024-04-30",
        products=["RB"],
    )

    assert len(result) == 1
    assert result["product_code"].iloc[0] == "RB"
    assert result["inventory"].iloc[0] == 123.0


def test_akshare_inventory_market_fallback_skips_failed_exchange(monkeypatch):
    provider = AKShareProvider(config={"max_retry": 0})
    provider.initialize()

    monkeypatch.setattr(
        akshare_provider_module.futures_cons,
        "contract_symbols",
        ["RB", "CF"],
    )
    monkeypatch.setattr(
        akshare_provider_module.futures_cons,
        "market_exchange_symbols",
        {"dce": ["RB"], "czce": ["CF"], "cffex": ["IF"]},
    )

    def dce_fetcher(**kwargs):
        return pd.DataFrame()

    def czce_fetcher(**kwargs):
        return pd.DataFrame()

    def fake_select_receipt_fetcher(market, trade_date):
        return {
            "dce": dce_fetcher,
            "czce": czce_fetcher,
        }.get(market)

    def fake_call(func, *args, **kwargs):
        if func.__name__ == "dce_fetcher":
            raise ProviderDataError(
                "AKShare call dce_fetcher failed: 412 Precondition Failed",
                provider_name=provider.name,
            )
        if func.__name__ == "czce_fetcher":
            return pd.DataFrame(
                {
                    "date": ["20240430"],
                    "var": ["CF"],
                    "receipt": [456.0],
                }
            )
        raise AssertionError(f"Unexpected function call: {func.__name__}")

    provider._select_receipt_fetcher = fake_select_receipt_fetcher
    provider._call_akshare = fake_call

    result = provider._get_receipt_by_market(
        start_date="2024-04-30",
        end_date="2024-04-30",
        products=["RB", "CF"],
    )
    normalized = provider._normalize_receipt(result)

    assert len(normalized) == 1
    assert normalized["product_code"].iloc[0] == "CF"
    assert normalized["inventory"].iloc[0] == 456.0


def test_akshare_inventory_history_skips_unsupported_products():
    provider = AKShareProvider(config={"max_retry": 0})
    provider.initialize()

    def fake_call(func, *args, **kwargs):
        symbol = kwargs["symbol"]
        if symbol in {"中证500期货", "IC", "ic"}:
            raise ProviderDataError(
                "AKShare call futures_inventory_99 unsupported product: "
                f"未找到品种 {symbol} 对应的编号",
                provider_name=provider.name,
            )
        return pd.DataFrame({"日期": ["2024-04-30"], "库存": [1234.0]})

    provider._call_akshare = fake_call

    result = provider.get_futures_inventory(
        use_history=True,
        product_names={"IC": "中证500期货", "RB": "螺纹钢"},
    )

    assert len(result) == 1
    assert result["product_code"].iloc[0] == "RB"
    assert result["inventory"].iloc[0] == 1234.0


def test_akshare_inventory_history_tries_code_case_variants():
    provider = AKShareProvider(config={"max_retry": 0})
    provider.initialize()
    calls = []

    def fake_call(func, *args, **kwargs):
        symbol = kwargs["symbol"]
        calls.append(symbol)
        if symbol != "rb":
            raise ProviderDataError(
                f"AKShare call futures_inventory_99 unsupported product: "
                f"未找到品种 {symbol} 对应的编号",
                provider_name=provider.name,
            )
        return pd.DataFrame({"日期": ["2024-04-30"], "库存": [1234.0]})

    provider._call_akshare = fake_call

    result = provider.get_futures_inventory(
        use_history=True,
        product_names={"RB": "RB"},
    )

    assert calls == ["RB", "rb"]
    assert result["product_code"].iloc[0] == "RB"
    assert result["inventory"].iloc[0] == 1234.0


def test_akshare_unsupported_product_error_is_not_retried():
    provider = AKShareProvider(config={"max_retry": 3, "retry_delay": 0})
    provider.initialize()
    calls = 0

    def unsupported_func(**kwargs):
        nonlocal calls
        calls += 1
        raise ValueError("未找到品种 中证500期货 对应的编号")

    try:
        provider._call_akshare(unsupported_func, symbol="中证500期货")
    except ProviderDataError as exc:
        assert "unsupported product" in str(exc)
    else:
        raise AssertionError("Expected ProviderDataError")

    assert calls == 1


def test_akshare_inventory_empty_source_error_is_not_retried():
    provider = AKShareProvider(config={"max_retry": 3, "retry_delay": 0})
    provider.initialize()
    calls = 0

    def futures_inventory_99(**kwargs):
        nonlocal calls
        calls += 1
        raise ValueError(
            "Length mismatch: Expected axis has 0 elements, new values have 3 elements"
        )

    try:
        provider._call_akshare(futures_inventory_99, symbol="PX")
    except ProviderDataError as exc:
        assert "empty inventory data" in str(exc)
    else:
        raise AssertionError("Expected ProviderDataError")

    assert calls == 1


def test_akshare_inventory_history_skips_empty_source_products():
    provider = AKShareProvider(config={"max_retry": 0})
    provider.initialize()

    def fake_call(func, *args, **kwargs):
        symbol = kwargs["symbol"]
        if symbol in {"PX", "px"}:
            raise ProviderDataError(
                "AKShare call futures_inventory_99 empty inventory data: "
                "Length mismatch: Expected axis has 0 elements, new values have 3 elements",
                provider_name=provider.name,
            )
        return pd.DataFrame({"日期": ["2024-04-30"], "库存": [1234.0]})

    provider._call_akshare = fake_call

    result = provider.get_futures_inventory(
        use_history=True,
        product_names={"PX": "PX", "RB": "RB"},
    )

    assert len(result) == 1
    assert result["product_code"].iloc[0] == "RB"


def test_akshare_inventory_history_skips_product_after_generic_provider_failure():
    provider = AKShareProvider(config={"max_retry": 0})
    provider.initialize()

    def fake_call(func, *args, **kwargs):
        symbol = kwargs["symbol"]
        if symbol in {"PL", "pl"}:
            raise ProviderDataError(
                "AKShare call futures_inventory_99 failed: "
                "string indices must be integers, not 'str'",
                provider_name=provider.name,
            )
        return pd.DataFrame({"日期": ["2024-04-30"], "库存": [1234.0]})

    provider._call_akshare = fake_call

    result = provider.get_futures_inventory(
        use_history=True,
        product_names={"PL": "PL", "RB": "RB"},
    )

    assert len(result) == 1
    assert result["product_code"].iloc[0] == "RB"


def test_akshare_hk_adj_factor_normalizes_qfq_factor_and_keeps_start_anchor():
    provider = AKShareProvider(config={"max_retry": 0}, market="HK")
    provider.initialize()
    calls = []

    def fake_call(func, *args, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame(
            {
                "date": ["1900-01-01", "2023-05-19", "2024-05-17", "2025-05-16"],
                "qfq_factor": [0.5, 0.75, 0.9, 1.0],
            }
        )

    provider._call_akshare = fake_call

    result = provider.get_adj_factor(
        symbol="00700.HK",
        start_date="2024-01-01",
        end_date="2024-12-31",
        market="HK",
    )

    assert calls == [{"symbol": "00700", "adjust": "qfq-factor"}]
    assert list(result["time"].dt.strftime("%Y-%m-%d")) == ["2023-05-19", "2024-05-17"]
    assert list(result["adj_factor"].round(6)) == [1.5, 1.8]
    assert set(result["symbol"]) == {"00700.HK"}
