import os
import re
import json
import time
import base64
import asyncio
import threading
from datetime import datetime
from typing import Optional, Any

import ccxt
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, FloodWait, RPCError

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# =========================
# ENV / CONFIG
# =========================

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "-1002598403649"))
LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-1003828203122"))

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "")
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")

DRY_RUN = os.getenv("DRY_RUN", "1").strip() == "1"
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))

MEDIA_DELAY_SEC = float(os.getenv("MEDIA_DELAY_SEC", "5"))
CLOSE_BUNDLE_WINDOW_SEC = float(os.getenv("CLOSE_BUNDLE_WINDOW_SEC", "15"))

LOG_LEVEL = "INFO"
LOG_FLUSH_SEC = 20


# =========================
# PYROGRAM CLIENT
# =========================

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)


# =========================
# LOGGER
# =========================

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_min_level = _LEVELS.get(LOG_LEVEL, 20)

_log_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _send_to_tg(text: str):

    if not LOG_CHAT_ID:
        return

    try:
        await app.send_message(LOG_CHAT_ID, text[:4096])

    except FloodWait as e:
        await asyncio.sleep(int(e.value) + 1)

        try:
            await app.send_message(LOG_CHAT_ID, text[:4096])
        except:
            pass

    except RPCError:
        pass


def log(level: str, msg: str):

    lvl_name = (level or "INFO").upper()
    lvl = _LEVELS.get(lvl_name, 20)

    if lvl < _min_level:
        return

    line = f"[{_ts()}] [{lvl_name}] {msg}"

    print(line)

    if lvl_name == "ERROR":

        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_send_to_tg(line))
        except:
            pass

        return

    try:
        _log_queue.put_nowait((lvl_name, line))
    except:
        pass


async def log_pump():

    buf: list[str] = []

    while True:

        try:

            _, line = await _log_queue.get()

            buf.append(line)

            start = asyncio.get_event_loop().time()

            while True:

                now = asyncio.get_event_loop().time()

                if now - start >= LOG_FLUSH_SEC:
                    break

                try:
                    _, nxt = _log_queue.get_nowait()
                    buf.append(nxt)
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.2)

            chunk = ""

            for ln in buf:

                if len(chunk) + len(ln) + 1 > 3500:
                    await _send_to_tg(chunk.strip())
                    chunk = ""

                chunk += ln + "\n"

            if chunk.strip():
                await _send_to_tg(chunk.strip())

            buf.clear()

        except Exception:
            await asyncio.sleep(1)

# =========================
# EXCHANGE (BINGX HEDGE)
# =========================

exchange = ccxt.bingx({
    "apiKey": BINGX_API_KEY,
    "secret": BINGX_API_SECRET,
    "enableRateLimit": True,
    "options": {
        "defaultType": "swap",
        "hedgeMode": True
    }
})

ACTIVE_SL = {}


def ensure_markets_loaded():
    if not getattr(exchange, "markets", None):
        log("INFO", "Loading markets...")
        exchange.load_markets()
        log("INFO", f"Markets loaded: {len(exchange.markets)}")


def resolve_symbol(base: str) -> Optional[str]:

    ensure_markets_loaded()

    base = (base or "").upper().replace("USDT", "").strip()

    if not base:
        return None

    for c in (f"{base}/USDT:USDT", f"{base}/USDT"):
        if c in exchange.markets:
            return c

    for sym, m in exchange.markets.items():

        try:

            if (
                m.get("base", "").upper() == base
                and m.get("quote", "").upper() == "USDT"
            ):
                if ":USDT" in sym or m.get("swap") or m.get("contract"):
                    return sym

        except Exception:
            continue

    return None


def get_usdt_free() -> float:

    ensure_markets_loaded()

    for t in ("swap", "future", "futures", "contract"):

        try:

            bal = exchange.fetch_balance({"type": t})

            usdt = (bal.get("free") or {}).get("USDT")

            if usdt is not None:
                return float(usdt)

        except Exception:
            pass

    bal = exchange.fetch_balance()

    usdt = (bal.get("free") or {}).get("USDT")

    return float(usdt or 0)


def set_leverage(symbol: str, lev: int):

    try:
        exchange.set_leverage(int(lev), symbol)
    except Exception as e:
        log("WARNING", f"set_leverage not applied: {e}")


def open_market(symbol: str, side: str, qty: float):

    order_side = "buy" if side == "long" else "sell"

    params = {
        "positionSide": "LONG" if side == "long" else "SHORT",
        "marginMode": "cross"
    }

    return exchange.create_order(
        symbol,
        "market",
        order_side,
        qty,
        None,
        params
    )


def fetch_position_hedge(symbol: str, side: str):

    try:
        positions = exchange.fetch_positions([symbol])
    except Exception:
        return None

    want = "long" if side == "long" else "short"

    for p in positions:

        ps = (p.get("side") or "").lower()

        if ps == want:
            return p

    return None


def close_position_full_hedge(base: str, side: str):

    symbol = resolve_symbol(base)

    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_hedge(symbol, side)

    if not pos:
        return "NO_POSITION"

    contracts = float(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    )

    if contracts <= 0:
        return "NO_POSITION"

    close_side = "sell" if side == "long" else "buy"

    exchange.create_order(
        symbol,
        "market",
        close_side,
        contracts,
        None,
        {
            "reduceOnly": True,
            "positionSide": "LONG" if side == "long" else "SHORT"
        }
    )

    return "CLOSED"


# =========================
# SL / TP HELPERS
# =========================

def set_sl_hedge(base: str, side: str, sl_price: float):

    symbol = resolve_symbol(base)

    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_hedge(symbol, side)

    if not pos:
        return "NO_POSITION"

    contracts = float(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    )

    stop_side = "sell" if side == "long" else "buy"

    sl_prec = float(exchange.price_to_precision(symbol, sl_price))

    resp = exchange.create_order(
        symbol,
        "stop_market",
        stop_side,
        contracts,
        None,
        {
            "triggerPrice": sl_prec,
            "stopPrice": sl_prec,
            "positionSide": "LONG" if side == "long" else "SHORT"
        }
    )

    return f"SL_SET id={resp.get('id')} sl={sl_prec}"


def set_tp_hedge(base: str, side: str, tp_price: float):

    symbol = resolve_symbol(base)

    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_hedge(symbol, side)

    if not pos:
        return "NO_POSITION"

    contracts = float(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    )

    close_side = "sell" if side == "long" else "buy"

    resp = exchange.create_order(
        symbol,
        "limit",
        close_side,
        contracts,
        tp_price,
        {
            "reduceOnly": True,
            "positionSide": "LONG" if side == "long" else "SHORT"
        }
    )

    return f"TP_SET id={resp.get('id')} tp={tp_price}"


def breakeven_hedge(base: str, side: str):

    symbol = resolve_symbol(base)

    pos = fetch_position_hedge(symbol, side)

    if not pos:
        return "NO_POSITION"

    entry = pos.get("entryPrice") or pos.get("average")

    if entry is None:
        return "NO_ENTRY"

    return set_sl_hedge(base, side, float(entry))


def add_position_hedge(base: str, side: str, add_pct):

    symbol = resolve_symbol(base)

    pos = fetch_position_hedge(symbol, side)

    if not pos:
        return "NO_POSITION"

    contracts = float(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    )

    pct = float(add_pct or 50)

    add_qty = contracts * pct / 100

    add_qty = float(exchange.amount_to_precision(symbol, add_qty))

    resp = open_market(symbol, side, add_qty)

    return f"ADDED id={resp.get('id')} qty={add_qty}"

# =========================
# MATH / VALIDATION
# =========================

def validate_sl_tp(side: str, price: float, sl: float, tp: float):

    if side == "short":
        return sl > price and tp < price

    return tp > price and sl < price


def calc_qty(usdt_free: float, risk_pct: float, lev: int, entry_price: float):

    margin = usdt_free * (risk_pct / 100.0)

    notional = margin * lev

    if entry_price <= 0:
        return 0

    return notional / entry_price


def normalize_price_from_tail(raw: float, entry: float, side: str, kind: str):

    raw = float(raw)
    entry = float(entry)

    if 0 < raw < 100 and raw < entry * 50:
        return raw

    best = None
    best_score = float("inf")

    for k in range(0, 13):

        cand = raw / (10 ** k)

        if cand <= 0:
            continue

        ratio = cand / entry if entry > 0 else 999

        if ratio < 0.1 or ratio > 10:
            continue

        ok_dir = True

        if side == "short":

            if kind == "sl" and cand <= entry:
                ok_dir = False

            if kind == "tp" and cand >= entry:
                ok_dir = False

        else:

            if kind == "sl" and cand >= entry:
                ok_dir = False

            if kind == "tp" and cand <= entry:
                ok_dir = False

        if not ok_dir:
            continue

        score = abs(cand - entry) / entry

        if score < best_score:
            best_score = score
            best = cand

    if best is not None:
        return best

    return raw


# =========================
# CLOSE INTENT
# =========================

CLOSE_INTENT_PATTERNS = [
    r"\btp\b",
    r"\btp\d\b",
    r"\btake\s+profit\b",
    r"\btaking\s+profit\b",
    r"\bclose\b",
    r"\bclosing\b",
    r"\bexit\b",
    r"\broe\b",
    r"\broi\b",
]


def has_close_intent(text: str):

    t = (text or "").strip()

    if not t:
        return False

    for p in CLOSE_INTENT_PATTERNS:

        if re.search(p, t, re.I):
            return True

    return False


# =========================
# LOCAL SL PARSER
# =========================

SET_SL_BLOCK_WORDS = [
    r"\bbe\b",
    r"\bbreak\s*even\b",
    r"\bbreakeven\b",
    r"\badd\b",
    r"\bdca\b",
]


TOKEN_ALIASES = {
    "SOLANA": "SOL"
}


def _normalize_base_word(w: str):

    if not w:
        return None

    b = w.upper().strip()

    b = re.sub(r"[^A-Z0-9]", "", b)

    if not b:
        return None

    b = TOKEN_ALIASES.get(b, b)

    if b.endswith("USDT"):
        b = b[:-4]

    return b


def parse_set_sl_local(text: str):

    if not text:
        return None

    t = text.strip()

    low = t.lower()

    for p in SET_SL_BLOCK_WORDS:
        if re.search(p, low, re.I):
            return None

    if not re.search(r"\b(stop\s*loss|stoploss|sl)\b", low, re.I):
        return None

    m_price = re.search(
        r"\bto\s*([0-9]+(?:\.[0-9]+)?)\b",
        t,
        re.I
    )

    if not m_price:

        m_price = re.search(
            r"\b(?:stop\s*loss|stoploss|sl)\b[^0-9]*([0-9]+(?:\.[0-9]+)?)\b",
            t,
            re.I
        )

    if not m_price:
        return None

    sl = float(m_price.group(1))

    candidates = re.findall(r"\b[A-Z0-9]{2,15}\b", t.upper())

    bad = {
        "MOVE",
        "STOP",
        "LOSS",
        "SL",
        "TO",
        "THE",
        "A"
    }

    base = None

    for c in candidates:

        if c in bad:
            continue

        if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", c):
            continue

        base = _normalize_base_word(c)

        break

    if not base:
        return None

    return {
        "action": "SET_SL",
        "base": base,
        "sl": sl,
        "confidence": 1.0,
        "raw_text": t[:500]
    }


# =========================
# AI PARSER
# =========================

AI_SYSTEM = (
    "You are a trade-signal parser for USDT-M perpetual futures.\n"
    "Return ONLY JSON.\n"
    "Actions: OPEN CLOSE ADD SET_SL SET_TP BE NONE.\n"
)


AI_JSON_SHAPE = {
    "action": "OPEN | CLOSE | ADD | SET_SL | SET_TP | BE | NONE",
    "base": "string|null",
    "side": "long|short|null",
    "leverage": "int|null",
    "risk_pct": "number|null",
    "sl": "number|null",
    "tp": "number|null",
    "add_pct": "number|null",
    "confidence": "0..1"
}


def ai_parse_trade(text):

    if not OpenAI or not OPENAI_API_KEY:

        return {
            "action": "NONE",
            "confidence": 0
        }

    client = OpenAI(api_key=OPENAI_API_KEY)

    resp = client.chat.completions.create(

        model=OPENAI_MODEL,

        messages=[
            {"role": "system", "content": AI_SYSTEM},
            {"role": "user", "content": text}
        ],

        temperature=0
    )

    out = resp.choices[0].message.content.strip()

    try:

        data = json.loads(out)

        if not isinstance(data, dict):
            raise ValueError

        return data

    except Exception:

        return {
            "action": "NONE",
            "confidence": 0
        }
    
    # =========================
# ACTION CONF
# =========================

ACTION_MIN_CONF = {
    "CLOSE": 0.55,
    "SET_SL": 0.60,
    "SET_TP": 0.60,
    "BE": 0.60,
    "ADD": 0.65,
    "OPEN": 0.80,
}


# =========================
# EXECUTION ROUTER
# =========================

async def handle_ai_command(cmd: dict):

    action = (cmd.get("action") or "NONE").upper()
    conf = float(cmd.get("confidence") or 0)

    base = cmd.get("base")
    side = cmd.get("side")

    lev = cmd.get("leverage")
    risk_pct = cmd.get("risk_pct")

    sl = cmd.get("sl")
    tp = cmd.get("tp")

    add_pct = cmd.get("add_pct")

    log("INFO", f"AI action={action} conf={conf} base={base} side={side}")

    if action == "NONE":
        return

    min_conf = ACTION_MIN_CONF.get(action, 0.7)

    if conf < min_conf:

        log("INFO", f"AI SKIP low confidence {conf}")
        return

    # =========================
    # CLOSE
    # =========================

    if action == "CLOSE":

        if not has_close_intent(cmd.get("raw_text", "")):
            log("INFO", "CLOSE skipped (no close intent)")
            return

        symbol = resolve_symbol(base)

        if not symbol:
            log("ERROR", f"Symbol not found {base}")
            return

        if DRY_RUN:

            log("INFO", f"DRY_RUN CLOSE {base}")
            return

        try:

            res = await asyncio.to_thread(
                close_position_full_hedge,
                base,
                side
            )

            log("INFO", f"CLOSE {base}: {res}")

        except Exception as e:

            log("ERROR", f"CLOSE failed {e}")

        return

    # =========================
    # OPEN
    # =========================

    if action == "OPEN":

        symbol = resolve_symbol(base)

        if not symbol:

            log("ERROR", f"Symbol not found {base}")
            return

        try:

            ticker = await asyncio.to_thread(
                exchange.fetch_ticker,
                symbol
            )

            entry = float(ticker["last"])

        except Exception as e:

            log("ERROR", f"ticker error {e}")
            return

        try:

            usdt_free = get_usdt_free()

            qty_raw = calc_qty(
                usdt_free,
                float(risk_pct),
                int(lev),
                entry
            )

            qty = float(
                exchange.amount_to_precision(symbol, qty_raw)
            )

        except Exception as e:

            log("ERROR", f"qty error {e}")
            return

        if qty <= 0:

            log("INFO", "qty zero skip")
            return

        if DRY_RUN:

            log("INFO", "DRY_RUN OPEN")
            return

        try:

            await asyncio.to_thread(
                set_leverage,
                symbol,
                int(lev)
            )

            resp = await asyncio.to_thread(
                open_market,
                symbol,
                side,
                qty
            )

            log("INFO", f"OPEN id={resp.get('id')}")

        except Exception as e:

            log("ERROR", f"OPEN failed {e}")
            return

        try:

            if sl:

                await asyncio.to_thread(
                    set_sl_hedge,
                    base,
                    side,
                    float(sl)
                )

                log("INFO", f"SL set {sl}")

        except Exception as e:

            log("WARNING", f"SL failed {e}")

        try:

            if tp:

                await asyncio.to_thread(
                    set_tp_hedge,
                    base,
                    side,
                    float(tp)
                )

                log("INFO", f"TP set {tp}")

        except Exception as e:

            log("WARNING", f"TP failed {e}")

        return

    # =========================
    # ADD
    # =========================

    if action == "ADD":

        if DRY_RUN:

            log("INFO", "DRY_RUN ADD")
            return

        try:

            res = await asyncio.to_thread(
                add_position_hedge,
                base,
                side,
                add_pct
            )

            log("INFO", f"ADD {res}")

        except Exception as e:

            log("ERROR", f"ADD failed {e}")

        return

    # =========================
    # SET SL
    # =========================

    if action == "SET_SL":

        if DRY_RUN:

            log("INFO", "DRY_RUN SET_SL")
            return

        try:

            res = await asyncio.to_thread(
                set_sl_hedge,
                base,
                side,
                float(sl)
            )

            log("INFO", f"SET_SL {res}")

        except Exception as e:

            log("ERROR", f"SET_SL failed {e}")

        return

    # =========================
    # SET TP
    # =========================

    if action == "SET_TP":

        if DRY_RUN:

            log("INFO", "DRY_RUN SET_TP")
            return

        try:

            res = await asyncio.to_thread(
                set_tp_hedge,
                base,
                side,
                float(tp)
            )

            log("INFO", f"SET_TP {res}")

        except Exception as e:

            log("ERROR", f"SET_TP failed {e}")

        return

    # =========================
    # BREAKEVEN
    # =========================

    if action == "BE":

        if DRY_RUN:

            log("INFO", "DRY_RUN BE")
            return

        try:

            res = await asyncio.to_thread(
                breakeven_hedge,
                base,
                side
            )

            log("INFO", f"BE {res}")

        except Exception as e:

            log("ERROR", f"BE failed {e}")

        return


# =========================
# TELEGRAM SIGNAL HANDLER
# =========================

@app.on_message(filters.chat(TARGET_CHAT_ID))
async def on_signal(client, message):

    text = message.text

    if not text:
        return

    log("INFO", f"Signal: {text}")

    # local parser first
    cmd = parse_set_sl_local(text)

    if not cmd:

        cmd = ai_parse_trade(text)

    cmd["raw_text"] = text

    await handle_ai_command(cmd)


# =========================
# MAIN
# =========================

async def main():

    await app.start()

    log("INFO", "BOT STARTED")

    asyncio.create_task(log_pump())

    while True:

        await asyncio.sleep(HEARTBEAT_SEC)

        log("DEBUG", "heartbeat")


if __name__ == "__main__":

    asyncio.run(main())