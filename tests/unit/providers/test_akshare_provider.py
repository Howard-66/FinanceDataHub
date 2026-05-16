import pandas as pd

from finance_data_hub.providers.akshare_provider import AKShareProvider


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

    assert result["product_code"].iloc[0] == "RB"
    assert result["inventory"].iloc[0] == 1234.0
