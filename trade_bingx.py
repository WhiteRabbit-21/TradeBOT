import re
from dataclasses import dataclass
from datetime import datetime
import os

import ccxt
from dotenv import load_dotenv
from pyrogram import Client, filters, idle


# ===================== ENV =====================
load_dotenv()

def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise ValueError(f"Missing env var: {name}")
    return v

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"trade_{today}.txt")

DRY_RUN = False  # True = не отправляет ордера

# TG
api_id = int(must_env("TG_API_ID"))
api_hash = must_env("TG_API_HASH")
tg_session = must_env("TG_SESSION_STRING")

# Канал (ВАЖНО: для каналов это обычно отрицательный id вида -100xxxxxxxxxx)
TARGET_CHANNEL_ID = int(must_env("TARGET_CHANNEL_ID"))

# BingX
BINGX_API_KEY = must_env("BINGX_API_KEY")
BINGX_API_SECRET = must_env("BINGX_API_SECRET")


# ===================== LOG =====================
def log(status, msg=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] [{status}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print("LOG WRITE ERROR:", e)


# ===================== PYROGRAM CLIENT =====================
app = Client(
    name="tradebot",
    api_id=api_id,
    api_hash=api_hash,
    session_string=tg_session,
)


# ===================== TRIGGERS / PATTERNS =====================
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
    r"\bCLOSE\s+([A-Z]{2,10})USDT\b",       # close UNIUSDT
    r"\bCLOSE\s+([A-Z]{2,10})\b",           # close UNI
    r"\bTP\s+([A-Z]{2,10})\b",              # TP UNI
    r"\bIN\s+([A-Z]{2,10})\b",              # in UNI
    r"\bON\s+([A-Z]{2,10})\b",              # on UNI
    r"#\s*([A-Z]{2,10})USDT\b",             # #UNIUSDT
    r"\b(LONG|SHORT)\s+([A-Z0-9._-]{2,15})\b",
    r"\b([A-Z]{2,10})USDT\b",               # UNIUSDT
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


# ===================== SIGNAL PARSER =====================
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
    if re.search(r"\bshort\b", t, re.I): side = "short"
    if re.search(r"\blong\b", t, re.I): side = "long"
    if side is None:
        return None

    base = None
    m = re.search(r"#\s*([A-Z0-9._-]{2,20})", t, re.I)
    if m:
        tag = m.group(1).upper()
        base = tag[:-4] if tag.endswith("USDT") else tag
    if base is None:
        m = re.search(r"\b(?:long|short)\s+([A-Z0-9._-]{2,20})\b", t, re.I)
        if m:
            sym = m.group(1).upper()
            base = sym[:-4] if sym.endswith("USDT") else sym
    if base is None:
        return None

    m_sl = re.search(r"\bSL\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\b", t, re.I)
    if not m_sl:
        return None
    sl = float(m_sl.group(1))

    m_lev = re.search(r"\bLev(?:erage)?\s*x?\s*([0-9]{1,3})\b", t, re.I)
    if not m_lev:
        return None
    lev = int(m_lev.group(1))

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


# ===================== HANDLER: CHANNEL =====================
# ===================== TEST HANDLER =====================
# 1) Додай ENV: TEST_CHAT_ID (ID "Збережені" або твого тест-чату/групи)
TEST_CHAT_ID = int(must_env("TEST_CHAT_ID"))

@app.on_message(filters.chat(TEST_CHAT_ID))
def on_test_chat(client, message):
    try:
        text = message.text or message.caption or ""
        text = text.strip()

        # 1) Лог: що саме прийшло
        log(
            "TEST",
            f"chat_type={message.chat.type} chat_id={message.chat.id} "
            f"msg_id={message.id} from={getattr(message.from_user, 'id', None)} "
            f"text={text[:500]}"
        )

        if not text:
            log("TEST_SKIP", "Empty message")
            return

        # 2) Перевіряємо CLOSE-сигнал
        base_to_close = parse_close_signal(text)
        log("TEST_CLOSE_DBG", f"base_to_close={base_to_close}")
        if base_to_close:
            log("TEST_PARSE", f"CLOSE detected: {base_to_close}")
            # В тесті НЕ закриваємо позиції
            return

        # 3) Перевіряємо NEW-сигнал
        sig = parse_new_signal(text)
        if not sig:
            log("TEST_SKIP", "Not a NEW signal format")
            return

        log(
            "TEST_PARSE",
            f"NEW SIGNAL: {sig.side.upper()} {sig.base} SL={sig.sl} Lev={sig.lev} Risk={sig.risk_pct}% "
            f"TP(price={sig.tp_price}, rr={sig.tp_rr}, pct={sig.tp_pct})"
        )

        # В тесті НЕ відкриваємо угоди

    except Exception as e:
        log("TEST_ERR", f"Handler crashed: {e}")

# ===================== MAIN =====================
import time

if __name__ == "__main__":
    log("BOOT", "Bot starting...")

    try:
        app.start()
        me = app.get_me()
        log("ME", f"LOGGED AS: {me.id} {me.first_name} @{me.username}")
        log("RUN", f"Listening channel_id={TARGET_CHANNEL_ID} ...")

        # heartbeat чтобы точно видеть что живой
        while True:
            time.sleep(60)
            log("HB", "alive")

    except Exception as e:
        log("FATAL", f"Startup crashed: {e}")
        raise

    finally:
        try:
            app.stop()
            log("STOP", "Client stopped")
        except Exception:
            pass

