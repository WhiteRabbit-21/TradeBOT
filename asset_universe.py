"""Classify exchange contracts that are not ordinary crypto assets.

The signal generator may keep recording these instruments for research.  This
module is used by the execution bot only, so classification never deletes a
signal or interferes with management of an existing position.
"""
from __future__ import annotations

import json
import re
from typing import Any


# Explicit fallbacks cover the TradFi symbols already observed in the signal
# database and common equity/ETF contracts.  Exchange metadata and the signal
# marker below are the primary future-proof detection paths.
EQUITY_BASES = {
    "AAPL", "AMD", "AMZN", "ARM", "ASML", "AVGO", "COIN", "GOOG",
    "GOOGL", "HK0700", "HK1810", "INTC", "META", "MSFT", "MSTR", "MU",
    "NFLX", "NVDA", "PLTR", "SNDK", "SKHY", "SKHYNIX", "TSLA", "TSM",
    "TENCENT", "XIAOMI", "ZHIPU",
}

ETF_BASES = {
    "DIA", "GLD", "IBIT", "IWM", "KORU", "MUU", "QQQ", "SLV", "SOXL",
    "SOXS", "SPY", "SPXU", "SQQQ", "TLT", "TQQQ", "UPRO", "VOO",
}

COMMODITY_BASES = {
    "BRENT", "COPPER", "GOLD", "NG", "SILVER", "UKOIL", "USOIL", "WTI",
    "XAG", "XAU",
}

INDEX_BASES = {"DJI", "NDX", "SPX", "US30", "US100", "US500", "VIX"}

PRE_IPO_BASES = {"MINIMEX", "OPENAI", "SPCX"}

FOREX_BASES = {
    "AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF",
    "USDJPY",
}


def normalize_base(value: Any) -> str:
    """Return a conservative uppercase base ticker from a CCXT/signal symbol."""
    text = str(value or "").upper().strip().replace("#", "")
    text = text.split("/", 1)[0].split(":", 1)[0]
    text = re.split(r"[-_\s]", text, maxsplit=1)[0]
    return re.sub(r"[^A-Z0-9]", "", text)


def _metadata_text(market: dict | None) -> str:
    if not market:
        return ""
    try:
        return json.dumps(market, ensure_ascii=False, default=str).casefold()
    except (TypeError, ValueError):
        return str(market).casefold()


def classify_non_crypto_asset(
    base_or_symbol: Any,
    *,
    market: dict | None = None,
    signal_text: str = "",
) -> str | None:
    """Return the non-crypto class, or ``None`` for an ordinary crypto asset.

    Detection order is deliberately explicit: a marker written by the signal
    generator, known tickers, then exchange metadata.  A symbol merely ending
    in USDT is never considered proof that its underlying is crypto.
    """
    base = normalize_base(base_or_symbol)
    message = str(signal_text or "").casefold()

    if (
        "тип активу: tradfi" in message
        or "asset class: tradfi" in message
        or "біржа для токенізованої" in message
    ):
        if "etf" in message:
            return "ETF"
        if "товар" in message or "commodity" in message:
            return "commodity"
        return "TradFi"

    if base in EQUITY_BASES:
        return "equity"
    if base in ETF_BASES:
        return "ETF"
    if base in COMMODITY_BASES:
        return "commodity"
    if base in INDEX_BASES:
        return "index"
    if base in PRE_IPO_BASES:
        return "pre-IPO equity"
    if base in FOREX_BASES:
        return "forex"

    metadata = _metadata_text(market)
    metadata_markers = (
        ("ETF", ("etf perpetual", "etf token", "underlying etf")),
        ("equity", (
            "tradfi", "equity perpetual", "equity securities",
            "underlying equity", "stock perpetual", "stock token",
            "tokenized stock", "tokenized equity",
        )),
        ("pre-IPO equity", ("pre-ipo", "pre ipo")),
        ("commodity", ("commodity perpetual", "underlying commodity")),
        ("forex", ("forex perpetual", "foreign exchange")),
        ("index", ("equity index", "stock index", "underlying index")),
    )
    for asset_class, markers in metadata_markers:
        if any(marker in metadata for marker in markers):
            return asset_class
    return None
