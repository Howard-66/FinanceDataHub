"""Market and exchange helpers used across routing, storage, and SDK layers."""

from __future__ import annotations

from datetime import time
from typing import Iterable, Optional


CN_EXCHANGES = {"SH", "SZ", "BJ"}
HK_EXCHANGES = {"HK"}
EXCHANGE_TO_MARKET = {
    **{exchange: "CN" for exchange in CN_EXCHANGES},
    **{exchange: "HK" for exchange in HK_EXCHANGES},
}
MARKET_TO_EXCHANGES = {
    "CN": CN_EXCHANGES,
    "HK": HK_EXCHANGES,
}
MARKET_CLOSE_TIMES = {
    "CN": time(15, 0),
    "HK": time(16, 0),
}


def normalize_market(market: Optional[str], default: Optional[str] = None) -> Optional[str]:
    """Normalize supported broad market codes."""
    if market is None:
        return default
    normalized = str(market).strip().upper()
    if not normalized:
        return default
    if normalized in {"ALL", "*"}:
        return "ALL"
    return normalized


def get_exchange_from_symbol(symbol: Optional[str]) -> Optional[str]:
    """Return the exchange suffix from a symbol such as 600519.SH or 00700.HK."""
    if not symbol or "." not in symbol:
        return None
    exchange = symbol.rsplit(".", 1)[-1].strip().upper()
    return exchange or None


def infer_market_from_symbol(symbol: Optional[str], default: str = "CN") -> str:
    """Infer broad market from symbol suffix."""
    exchange = get_exchange_from_symbol(symbol)
    if not exchange:
        return default
    return EXCHANGE_TO_MARKET.get(exchange, default)


def exchanges_for_market(market: Optional[str]) -> set[str]:
    """Return exchange suffixes for a broad market code."""
    normalized = normalize_market(market)
    if not normalized or normalized == "ALL":
        return set()
    return set(MARKET_TO_EXCHANGES.get(normalized, {normalized}))


def infer_market_from_symbols(
    symbols: Optional[Iterable[str]],
    default: str = "CN",
) -> str:
    """Infer a broad market from a symbol collection when all symbols agree."""
    if not symbols:
        return default

    markets = {
        infer_market_from_symbol(symbol, default=default)
        for symbol in symbols
        if symbol
    }
    if len(markets) == 1:
        return markets.pop()
    return default


def daily_close_time_for_market(market: Optional[str]) -> time:
    """Return the daily close timestamp used for daily-level rows."""
    normalized = normalize_market(market, default="CN")
    return MARKET_CLOSE_TIMES.get(normalized, MARKET_CLOSE_TIMES["CN"])
