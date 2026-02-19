import re
from dataclasses import dataclass
from datetime import datetime
import os
from pyrogram import Client, filters
import ccxt
from dotenv import load_dotenv
from pyrogram import idle

load_dotenv()

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

today = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"trade_{today}.txt")

# ---------- LOG ----------
def log(status, msg=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] [{status}] {msg}"

    # в консоль
    print(line)

    # у файл
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print("LOG WRITE ERROR:", e)

load_dotenv()

TARGET_CHANNEL_ID = int(os.getenv("TARGET_CHANNEL_ID"))

DRY_RUN = False #true не відправляє ордери, False-відправляє

# ---------- TG ----------
api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")
tg_session = os.getenv("TG_SESSION_STRING")

if not tg_session:
    raise ValueError("TG_SESSION_STRING is missing in env")

app = Client(
    name="tradebot",
    api_id=api_id,
    api_hash=api_hash,
    session_string=tg_session,  # <-- ключове
)
# ---------- BINGX ----------
BINGX_API_KEY = os.getenv("BINGX_API_KEY")
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET")

if not BINGX_API_KEY or not BINGX_API_SECRET:
    raise ValueError("BINGX API keys not found in .env")

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

    # 1) чи є тригер закриття (ігноруємо регістр)
    if not any(re.search(p, text, flags=re.I) for p in CLOSE_TRIGGERS):
        return None

    # 2) шукаємо символ (ігноруємо регістр)
    for p in SYMBOL_PATTERNS:
        m = re.search(p, text, flags=re.I)
        if not m:
            continue

        # якщо 2 групи (LONG/SHORT + SYMBOL) — беремо 2
        if len(m.groups()) == 2 and m.group(2):
            sym = m.group(2)
        else:
            sym = m.group(1)

        sym = sym.strip().upper()
        if sym in {"TP", "IN", "ON", "LONG", "SHORT", "CLOSE"}:
            continue

        return sym

    return None

# ініціалізація підключення до API біржі.
exchange = ccxt.bingx({
    "apiKey": BINGX_API_KEY,
    "secret": BINGX_API_SECRET,
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
})

# відображає всі баланси аккаунтв
bal = exchange.fetch_balance()

print(bal["USDT"]["free"])

#if USE_DEMO:
    #exchange.set_sandbox_mode(True)

# Заванатажує всі торгові пари
_markets_loaded = False
def ensure_markets():
    global _markets_loaded
    if not _markets_loaded:
        log("BINGX", "Loading markets...")
        exchange.load_markets()
        _markets_loaded = True
        log("BINGX", f"Markets loaded: {len(exchange.markets)}")

# нормальзація спаршеної інформації до вимог біржі
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



@dataclass
class NewSignal:
    side: str          # long/short
    base: str          # UNI, HYPE...
    sl: float
    lev: int
    risk_pct: float

    tp_price: float | None = None   # TP як ціна
    tp_rr: float | None = None      # TP як RR (1.5rr)
    tp_pct: float | None = None     # TP як % (3%)

def parse_new_signal(text: str) -> NewSignal | None:
    if not text:
        return None
    t = text.strip()

    # SIDE
    side = None
    if re.search(r"\bshort\b", t, re.I): side = "short"
    if re.search(r"\blong\b", t, re.I): side = "long"
    if side is None:
        return None

    # SYMBOL / BASE (під #UNIUSDT або "Short UNI")
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

    # SL
    m_sl = re.search(r"\bSL\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\b", t, re.I)
    if not m_sl:
        return None
    sl = float(m_sl.group(1))

    # LEV
    m_lev = re.search(r"\bLev(?:erage)?\s*x?\s*([0-9]{1,3})\b", t, re.I)
    if not m_lev:
        return None
    lev = int(m_lev.group(1))

    # RISK %
    m_risk = re.search(r"\b(?:With|Margin)\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*balance\b", t, re.I)
    if not m_risk:
        m_risk = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\s*%\s*balance\b", t, re.I)
    if not m_risk:
        return None
    risk_pct = float(m_risk.group(1))

    # TP: ціна / rr / %
    tp_price = None
    tp_rr = None
    tp_pct = None

    # 1) RR: "Target 1.5rr"
    m_rr = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*rr\b", t, re.I)
    if m_rr:
        tp_rr = float(m_rr.group(1))

    # 2) %: "Target 3%"
    m_pct = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%\b", t, re.I)
    if m_pct:
        tp_pct = float(m_pct.group(1))

    # 3) ціна: "Target: 3" (і НЕ rr/%, щоб не переплутати)
    if tp_rr is None and tp_pct is None:
        m_tp = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\b", t, re.I)
        if m_tp:
            tp_price = float(m_tp.group(1))

    # якщо TP взагалі немає — краще SKIP (безпечно)
    if tp_price is None and tp_rr is None and tp_pct is None:
        return None

    return NewSignal(
        side=side, base=base, sl=sl, lev=lev, risk_pct=risk_pct,
        tp_price=tp_price, tp_rr=tp_rr, tp_pct=tp_pct
    )


def validate_sl_tp(side: str, price: float, sl: float, tp: float) -> bool:
    if side == "short":
        return sl > price and tp < price
    return sl < price and tp > price

def calc_tp_from_rr(side: str, entry: float, sl: float, rr: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0:
        raise ValueError("Bad SL/entry: risk=0")
    if side == "short":
        return entry - risk * rr
    else:
        return entry + risk * rr

def calc_tp_from_pct(side: str, entry: float, pct: float) -> float:
    k = pct / 100.0
    if side == "short":
        return entry * (1.0 - k)
    else:
        return entry * (1.0 + k)

def get_usdt_free() -> float:
    bal = exchange.fetch_balance({"type": "swap"})  # важливо для perpetual
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
        params={
            "positionSide": "BOTH",
            "reduceOnly": False
        }
    )

def close_position_full_oneway(base: str):
    symbol = resolve_symbol(base)
    if not symbol:
        log("CLOSE", f"Symbol not found: {base}")
        return

    positions = exchange.fetch_positions([symbol], {"type": "swap"})

    pos = None
    for p in positions:
        # беремо першу ненульову позицію
        contracts = p.get("contracts")
        if contracts is not None and abs(float(contracts)) > 0:
            pos = p
            break

    if not pos:
        log("CLOSE", f"No open position for {symbol}")
        return

    # --- КІЛЬКІСТЬ ---
    raw_qty = (
        pos.get("contracts")
        or pos.get("positionAmt")
        or pos.get("size")
        or 0
    )
    raw_qty = float(raw_qty)

    if raw_qty == 0:
        log("CLOSE", f"Position qty = 0 for {symbol}")
        return

    qty = abs(raw_qty)
    qty = float(exchange.amount_to_precision(symbol, qty))

    # --- НАПРЯМОК ---
    side = pos.get("side")
    if side:
        side = side.lower()

    # fallback якщо side немає
    if not side or side not in {"long", "short"}:
        if raw_qty > 0:
            side = "long"
        else:
            side = "short"
        log("CLOSE_DBG", f"Side fallback by qty sign -> {side}")

    close_side = "sell" if side == "long" else "buy"

    log("CLOSE", f"Closing 100% {symbol} qty={qty} side={side}")

    try:
        exchange.create_order(
            symbol=symbol,
            type="market",
            side=close_side,
            amount=qty,
            params={
                "reduceOnly": True,
                "positionSide": "BOTH"
            }
        )
        log("CLOSE", "Close order sent")
    except Exception as e:
        log("CLOSE_ERR", str(e))


def opposite_side(entry_side: str) -> str:
    # entry_side: "long" або "short"
    return "sell" if entry_side == "long" else "buy"

def place_sl_tp_market_oneway(symbol: str, entry_side: str, qty: float, sl: float, tp: float):
    close_side = "sell" if entry_side == "long" else "buy"

    sl_order = exchange.create_order(
        symbol=symbol,
        type="market",
        side=close_side,
        amount=qty,
        params={"triggerPrice": sl, "reduceOnly": True, "positionSide": "BOTH"}
    )

    tp_order = exchange.create_order(
        symbol=symbol,
        type="market",
        side=close_side,
        amount=qty,
        params={"triggerPrice": tp, "reduceOnly": True, "positionSide": "BOTH"}
    )

    return sl_order, tp_order


# ---------- SAVED MESSAGES FILTER ----------
MY_ID = None  # визначимо один раз

@app.on_message(filters.chat(TARGET_CHANNEL_ID))
def on_channel(client, message):
    text = message.text or message.caption or ""

    # --- лог базовий (щоб бачити що реально прилетіло) ---
    log("MSG", f"Channel({message.chat.id}) msg: {text[:120]}")

    if not text.strip():
        log("SKIP", "Empty message")
        return

    # ==== CLOSE SIGNAL CHECK ====
    base_to_close = parse_close_signal(text)
    log("CLOSE_DBG", f"base_to_close={base_to_close}")

    if base_to_close:
        log("PARSE", f"CLOSE detected: {base_to_close}")

        if DRY_RUN:
            log("DRY_RUN", "Close NOT sent")
            return

        close_position_full_oneway(base_to_close)
        return

    # ==== NEW SIGNAL PARSE ====
    sig = parse_new_signal(text)
    if not sig:
        log("SKIP", "Not a NEW signal format")
        return

    log("PARSE", f"{sig.side.upper()} {sig.base} SL={sig.sl} Lev={sig.lev} Risk={sig.risk_pct}%")

    # ==== resolve symbol ====
    symbol = resolve_symbol(sig.base)
    if not symbol:
        log("ERROR", f"Symbol not found on BingX swap: {sig.base}/USDT")
        return
    log("SYMBOL", symbol)

    # ==== fetch price ====
    try:
        ticker = exchange.fetch_ticker(symbol)
        price = float(ticker["last"])
        log("PRICE", str(price))
    except Exception as e:
        log("ERROR", f"fetch_ticker failed: {e}")
        return

    # ====== RR / % / PRICE TP ======
    entry = price
    tp_final = None

    if getattr(sig, "tp_price", None) is not None:
        tp_final = sig.tp_price
        log("TP_MODE", f"price TP={tp_final}")

    elif getattr(sig, "tp_rr", None) is not None:
        tp_final = calc_tp_from_rr(sig.side, entry, sig.sl, sig.tp_rr)
        log("TP_MODE", f"RR {sig.tp_rr} -> TP {tp_final}")

    elif getattr(sig, "tp_pct", None) is not None:
        tp_final = calc_tp_from_pct(sig.side, entry, sig.tp_pct)
        log("TP_MODE", f"{sig.tp_pct}% -> TP {tp_final}")

    if tp_final is None:
        log("SKIP", "TP is missing (no price/rr/%)")
        return

    # округлюємо TP під біржу
    try:
        tp_final = float(exchange.price_to_precision(symbol, tp_final))
        log("TP", f"tp_prec={tp_final}")
    except Exception as e:
        log("ERROR", f"price_to_precision failed: {e}")
        return

    # ==== validate SL/TP vs entry ====
    if not validate_sl_tp(sig.side, entry, sig.sl, tp_final):
        log("SKIP", f"Bad SL/TP vs price. price={entry} SL={sig.sl} TP={tp_final}")
        return

    # ==== balance & qty ====
    try:
        usdt_free = get_usdt_free()
        qty = calc_qty(usdt_free, sig.risk_pct, sig.lev, entry)
        log("BAL", f"USDT free={usdt_free}")
        log("QTY", f"qty_raw≈{qty}")
    except Exception as e:
        log("ERROR", f"balance/qty failed: {e}")
        return

    # ==== precision qty ====
    try:
        qty = float(exchange.amount_to_precision(symbol, qty))
        log("QTY", f"qty_prec={qty}")
    except Exception as e:
        log("ERROR", f"amount_to_precision failed: {e}")
        return

    if qty <= 0:
        log("SKIP", "qty became 0 after precision (too small risk% or wrong balance)")
        return

    # ==== DRY RUN ====
    if DRY_RUN:
        log("DRY_RUN", "Order NOT sent (test mode)")
        return

    # ==== EXECUTE TRADE ====
    try:
        # leverage
        log("LEV", f"Setting leverage x{sig.lev}")
        set_leverage(symbol, sig.lev)

        # open position
        log("ORDER", f"Opening {sig.side.upper()} {symbol} qty={qty}")
        resp = open_market(symbol, sig.side, qty)
        log("SUCCESS", f"Order placed id={resp.get('id')}")

        # protective orders
        sl_order, tp_order = place_sl_tp_market_oneway(symbol, sig.side, qty, sig.sl, tp_final)
        log("PROTECT", f"SL id={sl_order.get('id')} | TP id={tp_order.get('id')}")

    except Exception as e:
        log("ERROR", f"Trade failed: {e}")

if __name__ == "__main__":
    log("BOOT", "Bot starting...")

    try:
        app.start()  # запускаємо клієнт

        me = app.get_me()
        log("ME", f"LOGGED AS: {me.id} {me.first_name} @{me.username}")

        log("RUN", "Client started, waiting for messages...")

        idle()  # тримає процес живим

    except Exception as e:
        log("FATAL", f"Startup crashed: {e}")
        raise
    finally:
        try:
            app.stop()
            log("STOP", "Client stopped")
        except Exception:
            pass


