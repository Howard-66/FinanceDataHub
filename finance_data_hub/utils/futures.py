"""Utilities for Chinese futures exchange and contract symbol handling."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class FuturesExchangeMapping:
    """Canonical and provider-specific exchange codes for one futures venue."""

    canonical: str
    tushare: str
    xtquant: str
    name: str


FUTURES_EXCHANGES: dict[str, FuturesExchangeMapping] = {
    "SHFE": FuturesExchangeMapping("SHFE", "SHF", "SF", "上期所"),
    "DCE": FuturesExchangeMapping("DCE", "DCE", "DF", "大商所"),
    "CZCE": FuturesExchangeMapping("CZCE", "ZCE", "ZF", "郑商所"),
    "CFFEX": FuturesExchangeMapping("CFFEX", "CFX", "IF", "中金所"),
    "INE": FuturesExchangeMapping("INE", "INE", "INE", "能源中心"),
    "GFEX": FuturesExchangeMapping("GFEX", "GFE", "GF", "广期所"),
}

_EXCHANGE_ALIAS_TO_CANONICAL: dict[str, str] = {}
for mapping in FUTURES_EXCHANGES.values():
    for alias in (mapping.canonical, mapping.tushare, mapping.xtquant, mapping.name):
        _EXCHANGE_ALIAS_TO_CANONICAL[alias.upper()] = mapping.canonical


def normalize_futures_exchange(exchange: Optional[str]) -> Optional[str]:
    """Normalize futures exchange aliases to canonical codes such as ``SHFE``."""
    if exchange is None:
        return None
    if exchange != exchange:
        return None
    raw = str(exchange).strip().upper()
    if not raw:
        return None
    return _EXCHANGE_ALIAS_TO_CANONICAL.get(raw, raw)


def get_tushare_exchange_code(exchange: Optional[str]) -> Optional[str]:
    """Return the Tushare exchange code for a canonical/provider exchange code."""
    canonical = normalize_futures_exchange(exchange)
    if not canonical:
        return None
    mapping = FUTURES_EXCHANGES.get(canonical)
    return mapping.tushare if mapping else canonical


def get_xtquant_exchange_code(exchange: Optional[str]) -> Optional[str]:
    """Return the XtQuant exchange code for a canonical/provider exchange code."""
    canonical = normalize_futures_exchange(exchange)
    if not canonical:
        return None
    mapping = FUTURES_EXCHANGES.get(canonical)
    return mapping.xtquant if mapping else canonical


def get_futures_exchange_from_symbol(symbol: Optional[str]) -> Optional[str]:
    """Extract and normalize the exchange suffix from a futures symbol."""
    if not symbol or "." not in symbol:
        return None
    return normalize_futures_exchange(symbol.rsplit(".", 1)[-1])


def extract_futures_product_code(symbol: Optional[str]) -> Optional[str]:
    """Extract the product code from a futures contract or synthetic symbol."""
    if not symbol:
        return None
    code = str(symbol).strip().split(".", 1)[0].upper()
    match = re.match(r"^([A-Z]+)", code)
    if not match:
        return None
    product = match.group(1)
    if product.endswith("L") and not re.search(r"\d", code):
        product = product[:-1]
    return product or None


def get_futures_contract_type(symbol: Optional[str]) -> str:
    """Infer whether a symbol is a normal, main, or continuous contract."""
    if not symbol:
        return "normal"
    code = str(symbol).strip().split(".", 1)[0].upper()
    if code.endswith("L0") or (code.endswith("L") and not re.search(r"\d", code)):
        return "continuous"
    if code.endswith("00") or not re.search(r"\d", code):
        return "main"
    return "normal"


def normalize_tushare_futures_symbol(symbol: Optional[str]) -> Optional[str]:
    """Normalize a futures symbol to Tushare style, e.g. ``RB2405.SHF``."""
    if not symbol:
        return None
    raw = str(symbol).strip()
    if "." not in raw:
        return raw.upper()

    code, exchange = raw.split(".", 1)
    code = code.strip().upper()
    exchange_code = get_tushare_exchange_code(exchange)
    if not exchange_code:
        return f"{code}.{exchange.strip().upper()}"

    # XtQuant synthetic contracts: rb00.SF -> RB.SHF, rbL0.SF -> RBL.SHF
    if code.endswith("00"):
        code = code[:-2]
    elif code.endswith("L0"):
        code = f"{code[:-2]}L"

    return f"{code}.{exchange_code}"


def to_xtquant_futures_symbol(
    symbol: Optional[str],
    contract_type: Optional[str] = None,
) -> Optional[str]:
    """Convert a Tushare-style futures symbol to XtQuant style."""
    if not symbol:
        return None
    raw = normalize_tushare_futures_symbol(symbol)
    if not raw or "." not in raw:
        return raw.lower() if raw else None

    code, exchange = raw.split(".", 1)
    xt_exchange = get_xtquant_exchange_code(exchange) or exchange
    inferred_type = contract_type or get_futures_contract_type(raw)
    code_lower = code.lower()

    if inferred_type == "continuous":
        if code_lower.endswith("l"):
            code_lower = code_lower[:-1]
        code_lower = f"{code_lower}L0"
    elif inferred_type == "main":
        code_lower = f"{extract_futures_product_code(raw).lower()}00"

    return f"{code_lower}.{xt_exchange}"


def extract_delivery_month(symbol: Optional[str]) -> Optional[int]:
    """Extract delivery month as an integer from a contract symbol."""
    if not symbol:
        return None
    code = str(symbol).strip().split(".", 1)[0].upper()
    match = re.search(r"(\d{3,4})$", code)
    if not match:
        return None
    digits = match.group(1)
    month = int(digits[-2:])
    return month if 1 <= month <= 12 else None


def extract_delivery_year(symbol: Optional[str], pivot: int = 70) -> Optional[int]:
    """Extract a four-digit delivery year from a futures contract symbol."""
    if not symbol:
        return None
    code = str(symbol).strip().split(".", 1)[0].upper()
    match = re.search(r"(\d{3,4})$", code)
    if not match:
        return None
    digits = match.group(1)
    year_digits = int(digits[:-2])
    if len(digits) == 3:
        year_digits = 2020 + year_digits
    elif year_digits >= pivot:
        year_digits = 1900 + year_digits
    else:
        year_digits = 2000 + year_digits
    return year_digits


def delivery_month_start(symbol: Optional[str]) -> Optional[date]:
    """Return the first day of the delivery month when it can be parsed."""
    year = extract_delivery_year(symbol)
    month = extract_delivery_month(symbol)
    if not year or not month:
        return None
    return date(year, month, 1)


def extract_quote_unit_value(
    quote_unit_desc: Optional[str],
    quote_unit: Optional[str] = None,
) -> Optional[float]:
    """Extract the numeric quote unit from strings like ``0.5人民币元/吨``."""
    if quote_unit_desc is None:
        return None
    text = str(quote_unit_desc).strip()
    if quote_unit:
        text = text.replace(str(quote_unit), "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group(0))


def supported_futures_exchanges() -> list[str]:
    """Return supported canonical futures exchange codes."""
    return list(FUTURES_EXCHANGES.keys())
