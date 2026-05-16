from datetime import date

from finance_data_hub.utils.futures import (
    delivery_month_start,
    extract_delivery_month,
    extract_delivery_year,
    extract_futures_product_code,
    extract_quote_unit_value,
    get_futures_contract_type,
    get_tushare_exchange_code,
    get_xtquant_exchange_code,
    normalize_futures_exchange,
    normalize_tushare_futures_symbol,
    to_xtquant_futures_symbol,
)


def test_futures_exchange_mapping():
    assert normalize_futures_exchange("SHF") == "SHFE"
    assert normalize_futures_exchange("SF") == "SHFE"
    assert get_tushare_exchange_code("GFEX") == "GFE"
    assert get_xtquant_exchange_code("CZCE") == "ZF"


def test_futures_symbol_conversion():
    assert normalize_tushare_futures_symbol("rb2405.SF") == "RB2405.SHF"
    assert normalize_tushare_futures_symbol("zn00.SF") == "ZN.SHF"
    assert normalize_tushare_futures_symbol("znL0.SF") == "ZNL.SHF"
    assert to_xtquant_futures_symbol("ZN.SHF") == "zn00.SF"
    assert to_xtquant_futures_symbol("ZNL.SHF") == "znL0.SF"


def test_futures_contract_metadata_parsing():
    assert extract_futures_product_code("RB2405.SHF") == "RB"
    assert extract_futures_product_code("RBL.SHF") == "RB"
    assert get_futures_contract_type("RB2405.SHF") == "normal"
    assert get_futures_contract_type("RB.SHF") == "main"
    assert get_futures_contract_type("RBL.SHF") == "continuous"
    assert extract_delivery_month("RB2405.SHF") == 5
    assert extract_delivery_year("RB2405.SHF") == 2024
    assert delivery_month_start("RB2405.SHF") == date(2024, 5, 1)


def test_extract_quote_unit_value():
    assert extract_quote_unit_value("0.5人民币元/吨", "人民币元/吨") == 0.5
    assert extract_quote_unit_value("10 元/吨") == 10.0
    assert extract_quote_unit_value("人民币元/吨") is None

