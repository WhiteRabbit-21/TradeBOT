import os
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime

import ccxt
from dotenv import load_dotenv
from pyrogram import Client, filters


# ===================== ENV =====================
load_dotenv()

def must_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        raise ValueError(f"Missing env var: {name}")
    return v

def env_flag(name: str) -> str:
    return "YES" if os.getenv(name) else "NO"

def env_preview(name: str) -> str:
    v = os.getenv(name)
    if not v:
        return "None"
    return v[:6] + "…" if len(v) > 6 else v

def parse_bool_env(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


# ===================== LOG =====================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"trade_{today}.txt")

def log(status, msg=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] [{status}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print("LOG WRITE ERROR:", e, flush=True)


# ===================== CONFIG =====================
DRY_RUN = parse_bool_env("DRY_RUN", default=True)
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))

# TG
api_id = int(must_env("TG_API_ID"))
api_hash = must_env("TG_API_HASH")
tg_session = must_env("TG_SESSION_STRING")

# Channel ID (optional: якщо нема — канал-хендлер не вмикається)
TARGET_CHANNEL_ID_RAW = os.getenv("TARGET_CHANNEL_ID")
TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_RAW) if TARGET_CHANNEL_ID_RAW else None

# BingX
BINGX_API_KEY = must_env("BINGX_API_KEY")
BINGX_API_SECRET = must_env("BINGX_API_SECRET")


# ===================== PYROGRAM CLIENT =====================
app = Client(
    name="tradebot",
    api_id=api_id,
    api_hash=api_hash,
    session_string=tg_session,
)


# ===================== EXCHANGE =====================
exchange = ccxt.bingx({
    "apiKey": BINGX_API_KEY,
    "secret": BINGX_API_SECRET,
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
})

_markets_loaded = False
def ensure_markets():
    global _markets_loaded
    if not _markets_loaded:
        log("BINGX", "Loading markets...")
        exchange.load_markets()
        _markets_loaded = True
        log("BINGX", f"Markets loaded: {len(exchange.markets)}")


def resolve_symbol(base: str, quote: str = "USDT") -> str | None:
    ensure_markets()
    base = base.upper()
    quote = quote.upper()

    candidates = [
        f"{base}/{quote}:USDT",
        f"{base}/{quote}:{quote}",
        f"{base}/{quote}",
        f"{base}{quote}",
        f"{base}-{quote}",
    ]
    for c in candidates:
        m = exchange.markets.get(c)
        if m and m.get("swap"):
            return c

    for sym, m in exchange.markets.items():
        if m.get("swap") and m.get("base") == base and m.get("quote") == quote:
            return sym

    return None


# ===================== PARSERS =====================
CLOSE_TRIGGERS = [
    r"\btake\s*profit\b",
    r"\btp\s*hit\b",
    r"\btp\s*reached\b",
    r"\bclose\b",
    r"\bexit\b",
    r"\bclosing\b",
    r"\bsecured\s*profit\b",
    r"\bachieved\b",
]

SYMBOL_PATTERNS = [
    r"\bCLOSE\s+([A-Z]{2,10})USDT\b",
    r"\bCLOSE\s+([A-Z]{2,10})\b",
    r"\bTP\s+([A-Z]{2,10})\b",
    r"\bIN\s+([A-Z]{2,10})\b",
    r"\bON\s+([A-Z]{2,10})\b",
    r"#\s*([A-Z]{2,10})USDT\b",
    r"\b(LONG|SHORT)\s+([A-Z0-9._-]{2,15})\b",
    r"\b([A-Z]{2,10})USDT\b",
]

def parse_close_signal(text: str):
    if not text:
        return None

    if not any(re.search(p, text, flags=re.I) for p in CLOSE_TRIGGERS):
        return None

    for p in SYMBOL_PATTERNS:
        m = re.search(p, text, flags=re.I)
        if not m:
            continue

        if len(m.groups()) == 2 and m.group(2):
            sym = m.group(2)
        else:
            sym = m.group(1)

        sym = sym.strip().upper()
        if sym in {"TP", "IN", "ON", "LONG", "SHORT", "CLOSE"}:
            continue
        return sym

    return None


@dataclass
class NewSignal:
    side: str
    base: str
    sl: float
    lev: int
    risk_pct: float
    tp_price: float | None = None
    tp_rr: float | None = None
    tp_pct: float | None = None


def parse_new_signal(text: str) -> NewSignal | None:
    if not text:
        return None
    t = text.strip()

    side = None
    if re.search(r"\bshort\b", t, re.I):
        side = "short"
    if re.search(r"\blong\b", t, re.I):
        side = "long"
    if side is None:
        return None

    base = None
    # #BTCUSDT або #BTC
    m = re.search(r"#\s*([A-Z0-9._-]{2,30})", t, re.I)
    if m:
        tag = m.group(1).upper()
        base = tag[:-4] if tag.endswith("USDT") else tag

    # Long BTCUSDT / Short BTC
    if base is None:
        m = re.search(r"\b(?:long|short)\s+([A-Z0-9._-]{2,30})\b", t, re.I)
        if m:
            sym = m.group(1).upper()
            if sym not in {"MARKET", "ENTRY", "NOW"}:
                base = sym[:-4] if sym.endswith("USDT") else sym

    if base is None:
        return None

    m_sl = re.search(r"\bSL\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\b", t, re.I)
    if not m_sl:
        return None
    sl = float(m_sl.group(1))

    # leverage: "Leverage 10x" / "Lev 10" / "Lev: 10x"
    m_lev = re.search(r"\bLev(?:erage)?\b.*?(?:([0-9]{1,3})\s*[xX]|[xX]\s*([0-9]{1,3})|[:=]?\s*([0-9]{1,3}))\b", t, re.I)
    if not m_lev:
        return None
    lev = int(m_lev.group(1) or m_lev.group(2) or m_lev.group(3))

    m_risk = re.search(r"\b(?:With|Margin)\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*balance\b", t, re.I)
    if not m_risk:
        m_risk = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\s*%\s*balance\b", t, re.I)
    if not m_risk:
        return None
    risk_pct = float(m_risk.group(1))

    tp_price = tp_rr = tp_pct = None

    m_rr = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*rr\b", t, re.I)
    if m_rr:
        tp_rr = float(m_rr.group(1))

    m_pct = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%\b", t, re.I)
    if m_pct:
        tp_pct = float(m_pct.group(1))

    if tp_rr is None and tp_pct is None:
        m_tp = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\b", t, re.I)
        if m_tp:
            tp_price = float(m_tp.group(1))

    if tp_price is None and tp_rr is None and tp_pct is None:
        return None

    return NewSignal(side=side, base=base, sl=sl, lev=lev, risk_pct=risk_pct,
                     tp_price=tp_price, tp_rr=tp_rr, tp_pct=tp_pct)


# ===================== TRADE HELPERS =====================
def validate_sl_tp(side: str, price: float, sl: float, tp: float) -> bool:
    if side == "short":
        return sl > price and tp < price
    return sl < price and tp > price

def calc_tp_from_rr(side: str, entry: float, sl: float, rr: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0:
        raise ValueError("Bad SL/entry: risk=0")
    return entry - risk * rr if side == "short" else entry + risk * rr

def calc_tp_from_pct(side: str, entry: float, pct: float) -> float:
    k = pct / 100.0
    return entry * (1.0 - k) if side == "short" else entry * (1.0 + k)

def get_usdt_free() -> float:
    bal = exchange.fetch_balance({"type": "swap"})
    usdt = bal.get("USDT") or {}
    return float(usdt.get("free") or 0.0)

def calc_qty(balance_usdt: float, risk_pct: float, lev: int, price: float) -> float:
    margin = balance_usdt * (risk_pct / 100.0)
    notional = margin * lev
    return notional / price

def set_leverage(symbol, lev):
    return exchange.set_leverage(lev, symbol, params={"side": "BOTH"})

def open_market(symbol: str, side: str, qty: float):
    order_side = "buy" if side == "long" else "sell"
    return exchange.create_order(
        symbol=symbol,
        type="market",
        side=order_side,
        amount=qty,
        params={"positionSide": "BOTH", "reduceOnly": False},
    )

def close_position_full_oneway(base: str):
    symbol = resolve_symbol(base)
    if not symbol:
        log("CLOSE", f"Symbol not found: {base}")
        return

    positions = exchange.fetch_positions([symbol], {"type": "swap"})
    pos = None
    for p in positions:
        contracts = p.get("contracts")
        if contracts is not None and abs(float(contracts)) > 0:
            pos = p
            break

    if not pos:
        log("CLOSE", f"No open position for {symbol}")
        return

    raw_qty = float(pos.get("contracts") or pos.get("positionAmt") or pos.get("size") or 0.0)
    if raw_qty == 0:
        log("CLOSE", f"Position qty = 0 for {symbol}")
        return

    qty = abs(raw_qty)
    qty = float(exchange.amount_to_precision(symbol, qty))

    side = (pos.get("side") or "").lower()
    if side not in {"long", "short"}:
        side = "long" if raw_qty > 0 else "short"
        log("CLOSE_DBG", f"Side fallback by qty sign -> {side}")

    close_side = "sell" if side == "long" else "buy"
    log("CLOSE", f"Closing 100% {symbol} qty={qty} side={side}")

    exchange.create_order(
        symbol=symbol,
        type="market",
        side=close_side,
        amount=qty,
        params={"reduceOnly": True, "positionSide": "BOTH"},
    )
    log("CLOSE", "Close order sent")

def place_sl_tp_market_oneway(symbol: str, entry_side: str, qty: float, sl: float, tp: float):
    close_side = "sell" if entry_side == "long" else "buy"
    sl_order = exchange.create_order(
        symbol=symbol, type="market", side=close_side, amount=qty,
        params={"triggerPrice": sl, "reduceOnly": True, "positionSide": "BOTH"},
    )
    tp_order = exchange.create_order(
        symbol=symbol, type="market", side=close_side, amount=qty,
        params={"triggerPrice": tp, "reduceOnly": True, "positionSide": "BOTH"},
    )
    return sl_order, tp_order


# ===================== DIAG: TWO HANDLERS =====================
_last_hb = 0.0

def heartbeat_tick():
    global _last_hb
    now = time.time()
    if now - _last_hb >= HEARTBEAT_SEC:
        _last_hb = now
        log("HB", "alive")


# --- Handler 1: Saved Messages ---
MY_ID = None

@app.on_message(filters.private & filters.me & (filters.text | filters.caption))
def on_saved_debug(client, message):
    global MY_ID
    try:
        heartbeat_tick()

        if MY_ID is None:
            me = client.get_me()
            MY_ID = me.id
            log("ME", f"Logged as {me.first_name} (@{me.username}), my_id={MY_ID}")
            log("SAVED", "Saved handler READY (listening Saved Messages)")

        if message.chat.id != MY_ID:
            log("SAVED_SKIP", f"private chat but not Saved: chat_id={message.chat.id}")
            return

        text = (message.text or message.caption or "").strip()
        log("SAVED_IN", f"Saved message received: {text[:160]}")

    except Exception as e:
        log("SAVED_ERR", f"{e}")
        log("SAVED_TRACE", traceback.format_exc())


# --- Handler 2: Channel/Chat by ID (only if TARGET_CHANNEL_ID set) ---
if TARGET_CHANNEL_ID is not None:
    @app.on_message(filters.chat(TARGET_CHANNEL_ID) & (filters.text | filters.caption))
    def on_channel_debug(client, message):
        try:
            heartbeat_tick()

            text = (message.text or message.caption or "").strip()
            log("CHAN_IN", f"Channel handler fired! type={message.chat.type} chat_id={message.chat.id} msg_id={message.id} text={text[:160]}")

        except Exception as e:
            log("CHAN_ERR", f"{e}")
            log("CHAN_TRACE", traceback.format_exc())
else:
    log("CHAN", "Channel handler DISABLED because TARGET_CHANNEL_ID is None")


# ===================== MAIN =====================
if __name__ == "__main__":
    log("BOOT", "Bot starting...")

    # Безпечний прев’ю ENV (без секретів)
    log("ENV", f"TG_API_ID={env_preview('TG_API_ID')} TG_API_HASH={env_flag('TG_API_HASH')} TG_SESSION_STRING={env_flag('TG_SESSION_STRING')}")
    log("ENV", f"BINGX_API_KEY={env_flag('BINGX_API_KEY')} BINGX_API_SECRET={env_flag('BINGX_API_SECRET')}")
    log("ENV", f"TARGET_CHANNEL_ID={os.getenv('TARGET_CHANNEL_ID')!r} DRY_RUN={DRY_RUN}")

    if TARGET_CHANNEL_ID is None:
        log("RUN", "Channel listener DISABLED (TARGET_CHANNEL_ID is None). Saved listener ENABLED.")
    else:
        log("RUN", f"Channel listener ENABLED. Listening TARGET_CHANNEL_ID={TARGET_CHANNEL_ID}. Saved listener ENABLED.")

    try:
        app.run()
    except Exception as e:
        log("FATAL", f"app.run() crashed: {e}")
        log("TRACE", traceback.format_exc())
        raise