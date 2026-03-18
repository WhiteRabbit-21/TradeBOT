# trade_app.py
import os
import re
import json
import time
import base64
import asyncio
from datetime import datetime
from typing import Optional, Any
from trade_notifier import pnl_watcher
import ccxt
from pyrogram import Client, filters, idle
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

TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "-1002598403649"))   # де читаємо сигнали
LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-1003828203122"))      # куди шлемо логи

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "")
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")

DRY_RUN = os.getenv("DRY_RUN", "1").strip() == "1"
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))  # 5 хв

MEDIA_DELAY_SEC = float(os.getenv("MEDIA_DELAY_SEC", "5"))  # wait for album completion
CLOSE_BUNDLE_WINDOW_SEC = float(os.getenv("CLOSE_BUNDLE_WINDOW_SEC", "15"))  # attach orphan photos

# --- TG logging config (hardcoded) ---
LOG_LEVEL = "INFO"   # DEBUG / INFO / WARNING / ERROR
LOG_FLUSH_SEC = 20   # INFO пачкою раз на N секунд

# =========================
# PYROGRAM CLIENT (USER)
# =========================
app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

# =========================
# TG LOGGER (batched)
# =========================
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_min_level = _LEVELS.get(LOG_LEVEL, 20)
_log_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()  # (level, line)

def _ts() -> str:
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
        except Exception:
            pass
    except RPCError:
        pass
    except Exception:
        pass

def log(level: str, msg: str):
    lvl_name = (level or "INFO").upper()
    lvl = _LEVELS.get(lvl_name, 20)
    if lvl < _min_level:
        return

    line = f"[{_ts()}] [{lvl_name}] {msg}"
    print(line)

    # ERROR -> immediately
    if lvl_name == "ERROR":
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_send_to_tg(line))
        except Exception:
            pass
        return

    # Important WARNING -> immediately
    if lvl_name == "WARNING" and any(k in msg.lower() for k in ["peer", "invalid", "failed", "error", "denied"]):
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_send_to_tg(line))
        except Exception:
            pass
        return

    try:
        _log_queue.put_nowait((lvl_name, line))
    except Exception:
        pass

async def log_pump():
    """Зливає INFO/DEBUG пачкою кожні LOG_FLUSH_SEC."""
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

            if not buf:
                continue

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
# PEER WARMUP
# =========================
async def ensure_peer_known(chat_id: int) -> bool:
    try:
        chat = await app.get_chat(chat_id)
        title = getattr(chat, "title", "") or getattr(chat, "first_name", "")
        log("INFO", f"🎯 get_chat OK: {title} ({chat.id})")
        return True
    except PeerIdInvalid:
        log("WARNING", f"get_chat({chat_id}) -> PEER_ID_INVALID. Шукаю через dialogs…")
    except Exception as e:
        log("WARNING", f"get_chat({chat_id}) error: {e}. Пробую через dialogs…")

    try:
        async for dialog in app.get_dialogs(limit=400):
            c = dialog.chat
            if c and c.id == chat_id:
                title = getattr(c, "title", "") or getattr(c, "first_name", "")
                log("INFO", f"✅ Found in dialogs: {title} ({c.id})")
                await app.get_chat(chat_id)
                log("INFO", "✅ Peer warmed up")
                return True

        log("ERROR", f"❌ Не знайшов chat_id={chat_id} у dialogs.")
        return False
    except Exception as e:
        log("ERROR", f"❌ dialogs scan error: {e}")
        return False


# =========================
# EXCHANGE (BINGX via CCXT)
# =========================
exchange = ccxt.bingx({
    "apiKey": BINGX_API_KEY,
    "secret": BINGX_API_SECRET,
    "enableRateLimit": True,
})

def ensure_markets_loaded_sync():
    if not getattr(exchange, "markets", None):
        exchange.load_markets()

async def ensure_markets_loaded():
    await asyncio.to_thread(ensure_markets_loaded_sync)

def resolve_symbol_sync(base: str) -> Optional[str]:
    ensure_markets_loaded_sync()
    base = (base or "").upper().replace("USDT", "").strip()
    if not base:
        return None

    for c in (f"{base}/USDT:USDT", f"{base}/USDT"):
        if c in exchange.markets:
            return c

    for sym, m in exchange.markets.items():
        try:
            if m.get("base", "").upper() == base and m.get("quote", "").upper() == "USDT":
                if ":USDT" in sym or m.get("swap") or m.get("contract"):
                    return sym
        except Exception:
            continue
    return None

async def resolve_symbol(base: str) -> Optional[str]:
    return await asyncio.to_thread(resolve_symbol_sync, base)

def get_usdt_free_sync() -> float:
    ensure_markets_loaded_sync()
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
    return float(usdt or 0.0)

async def get_usdt_free() -> float:
    return await asyncio.to_thread(get_usdt_free_sync)

def set_leverage_sync(symbol: str, lev: int):
    try:
        exchange.set_leverage(int(lev), symbol)
    except Exception as e:
        # не критично
        raise RuntimeError(f"set_leverage failed: {e}")

async def set_leverage(symbol: str, lev: int):
    await asyncio.to_thread(set_leverage_sync, symbol, lev)

def open_market_sync(symbol: str, side: str, qty: float):

    order_side = "sell" if side == "short" else "buy"
    position_side = "SHORT" if side == "short" else "LONG"

    return exchange.create_order(
        symbol,
        "market",
        order_side,
        qty,
        None,
        {
            "positionSide": position_side,
            "reduceOnly": False
        }
    )

async def open_market(symbol: str, side: str, qty: float):
    return await asyncio.to_thread(open_market_sync, symbol, side, qty)

def fetch_position_oneway_sync(symbol: str):

    try:

        positions = exchange.fetch_positions([symbol])

        for p in positions:
            side = (
                p.get("side")
                or p.get("positionSide")
                or (p.get("info") or {}).get("positionSide")
                or ""
            ).lower()

            if side in {"long","short"}:
                size = float(
                    p.get("contracts")
                    or p.get("size")
                    or p.get("positionAmt")
                    or 0
                )

                if size > 0:
                    return p

        return None

    except Exception:
        return None
        
async def fetch_position_oneway(symbol: str):

    return await asyncio.to_thread(
        fetch_position_oneway_sync,
        symbol
    )    

def close_position_full_hedge_sync(base: str):

    symbol = resolve_symbol_sync(base)

    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    positions = exchange.fetch_positions([symbol])

    closed = []

    for pos in positions:

        side = (
            pos.get("side")
            or pos.get("positionSide")
            or (pos.get("info") or {}).get("positionSide")
            or ""
        ).lower()

        if side not in {"long", "short"}:
            continue

        contracts = float(
            pos.get("contracts")
            or pos.get("size")
            or pos.get("positionAmt")
            or 0
        )

        if contracts <= 0:
            continue

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

        closed.append(side)

    if not closed:
        return "NO_POSITION"

    return f"CLOSED {'/'.join(closed)}"

async def close_position_full_oneway(base: str):
    return await asyncio.to_thread(close_position_full_hedge_sync, base)

# =========================
# STOP/TP helpers (best-effort)
# =========================
def _looks_like_stop(o: dict) -> bool:
    t = (o.get("type") or "").lower()
    info = o.get("info") or {}
    return (
        "stop" in t
        or "trigger" in t
        or o.get("stopPrice") is not None
        or o.get("triggerPrice") is not None
        or info.get("stopPrice") is not None
        or info.get("triggerPrice") is not None
    )

def cancel_order_safe_sync(symbol: str, order_id: str) -> bool:
    try:
        exchange.cancel_order(order_id, symbol)
        return True
    except Exception:
        return False

def find_and_cancel_existing_stop_sync(symbol: str, want_side: str) -> Optional[str]:
    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception:
        return None

    best = None
    best_score = -1

    for o in orders:
        try:
            o_side = (o.get("side") or "").lower()
            if o_side != want_side:
                continue
            if not _looks_like_stop(o):
                continue

            info = o.get("info") or {}
            reduce_only = bool(o.get("reduceOnly") or info.get("reduceOnly") or False)
            score = 2 if reduce_only else 1
            if score > best_score:
                best = o
                best_score = score
        except Exception:
            continue

    if not best or not best.get("id"):
        return None

    oid = best["id"]
    return oid if cancel_order_safe_sync(symbol, oid) else None

async def find_and_cancel_existing_stop(symbol: str, want_side: str) -> Optional[str]:
    return await asyncio.to_thread(find_and_cancel_existing_stop_sync, symbol, want_side)

def set_sl_oneway_sync(base: str, sl_price: float) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = (
        pos.get("side")
        or pos.get("positionSide")
        or (pos.get("info") or {}).get("positionSide")
        or ""
    ).lower()
    
    if pos_side not in {"long", "short"}:
        return "NO_POSITION"

    contracts = float(pos.get("contracts") or pos.get("size") or pos.get("positionAmt") or 0.0)
    if contracts <= 0:
        return "NO_POSITION"

    stop_side = "sell" if pos_side == "long" else "buy"

    try:
        sl_prec = float(exchange.price_to_precision(symbol, float(sl_price)))
    except Exception:
        sl_prec = float(sl_price)

    old = find_and_cancel_existing_stop_sync(symbol, stop_side)

    candidates = [
        ("stopMarket", {"stopPrice": sl_prec, "reduceOnly": True}),
        ("stop_market", {"stopPrice": sl_prec, "reduceOnly": True}),
        ("stop", {"stopPrice": sl_prec, "reduceOnly": True}),
        ("market", {"triggerPrice": sl_prec, "reduceOnly": True}),
    ]

    last_err = None
    for order_type, params in candidates:
        try:
            resp = exchange.create_order(
    symbol,
    order_type,
    stop_side,
    contracts,
    None,
    {
        **params,
        "positionSide": "LONG" if pos_side == "long" else "SHORT"
    }
)
            new_id = resp.get("id")
            if old:
                return f"SL_UPDATED old={old} new={new_id} sl={sl_prec}"
            return f"SL_SET id={new_id} sl={sl_prec}"
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Failed to set SL: {last_err}")

async def set_sl_oneway(base: str, sl_price: float) -> str:
    return await asyncio.to_thread(set_sl_oneway_sync, base, sl_price)

def set_tp_oneway_sync(base: str, tp_price: float) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = (pos.get("side") or "").lower()
    if pos_side not in {"long", "short"}:
        return "NO_POSITION"

    contracts = float(pos.get("contracts") or pos.get("size") or pos.get("positionAmt") or 0.0)
    if contracts <= 0:
        return "NO_POSITION"

    close_side = "sell" if pos_side == "long" else "buy"

    try:
        tp_prec = float(exchange.price_to_precision(symbol, float(tp_price)))
    except Exception:
        tp_prec = float(tp_price)

    resp = exchange.create_order(
    symbol,
    "limit",
    close_side,
    contracts,
    tp_prec,
    {
        "reduceOnly": True,
        "positionSide": "LONG" if pos_side == "long" else "SHORT"
    }
)
    return f"TP_SET id={resp.get('id')} tp={tp_prec}"

async def set_tp_oneway(base: str, tp_price: float) -> str:
    return await asyncio.to_thread(set_tp_oneway_sync, base, tp_price)

def breakeven_oneway_sync(base: str) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    entry = pos.get("entryPrice") or pos.get("average") or pos.get("avgPrice")
    if entry is None:
        return "NO_ENTRY_PRICE"

    # set_sl_oneway скасує старий SL
    return set_sl_oneway_sync(base, float(entry))

async def breakeven_oneway(base: str) -> str:
    return await asyncio.to_thread(breakeven_oneway_sync, base)

def add_position_oneway_sync(base: str, add_pct: Optional[float]) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = (pos.get("side") or "").lower()
    if pos_side not in {"long", "short"}:
        return "NO_POSITION"

    contracts = float(pos.get("contracts") or pos.get("size") or pos.get("positionAmt") or 0.0)
    if contracts <= 0:
        return "NO_POSITION"

    pct = float(add_pct if add_pct is not None else 50.0)
    if pct <= 0:
        return "BAD_ADD_PCT"

    add_qty = contracts * (pct / 100.0)
    try:
        add_qty = float(exchange.amount_to_precision(symbol, add_qty))
    except Exception:
        pass

    if add_qty <= 0:
        return "ADD_QTY_ZERO"

    resp = exchange.create_order(
    symbol,
    "market",
    "buy" if pos_side == "long" else "sell",
    add_qty,
    None,
    {
        "positionSide": "LONG" if pos_side == "long" else "SHORT",
        "reduceOnly": False
    }
)
    return f"ADDED id={resp.get('id')} qty={add_qty} pct={pct}"

async def add_position_oneway(base: str, add_pct: Optional[float]) -> str:
    return await asyncio.to_thread(add_position_oneway_sync, base, add_pct)


# =========================
# MATH / VALIDATION
# =========================
def validate_sl_tp(side: str, price: float, sl: float, tp: float) -> bool:
    if side == "short":
        return sl > price and tp < price
    return tp > price and sl < price

def calc_qty(usdt_free: float, risk_pct: float, lev: int, entry_price: float) -> float:
    margin = usdt_free * (risk_pct / 100.0)
    notional = margin * lev
    return 0.0 if entry_price <= 0 else notional / entry_price

def normalize_price_from_tail(raw: float, entry: float, side: str, kind: str) -> float:
    raw = float(raw)
    entry = float(entry)

    # якщо це вже адекватна “маленька” ціна — не чіпаємо
    if 0 < raw < 100 and raw < entry * 50:
        return raw

    best = None
    best_score = float("inf")

    for k in range(0, 13):
        cand = raw / (10 ** k)
        if cand <= 0:
            continue

        ratio = cand / entry if entry > 0 else 999.0
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

    return best if best is not None else raw


# =========================
# CLOSE INTENT (safe gate)
# =========================
CLOSE_INTENT_PATTERNS = [
    r"\btp\b", r"\btp\d\b", r"\btake\s+profit\b", r"\btaking\s+profit\b",
    r"\bclose\b", r"\bclosing\b", r"\bexit\b",
    r"\bзакр(ити|иваю|ив)\b", r"\bвихід\b", r"\bпрофіт\b",
    r"\bфикс\b", r"\bзакрыл\b",
    r"\broe\b", r"\broi\b",
    r"\btp\s+these\b",
]
def has_close_intent(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return any(re.search(p, t, re.I) for p in CLOSE_INTENT_PATTERNS)


# =========================
# LOCAL SET_SL PARSER (без AI)
# =========================
SET_SL_BLOCK_WORDS = [
    r"\bbe\b", r"\bbreak\s*even\b", r"\bbreakeven\b",
    r"\badd\b", r"\baverag", r"\bdca\b", r"\bscale\s*in\b",
]
TOKEN_ALIASES = {"SOLANA": "SOL"}

def _normalize_base_word(w: str) -> Optional[str]:
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

# =========================
# AI PARSER (text + images)
# =========================
AI_SYSTEM = """
You are a crypto futures trading signal parser.

Your ONLY task:
Convert ANY trading signal into VALID JSON.

CRITICAL RULES:
- ALWAYS return JSON
- NEVER return NONE if there is ANY of:
  base, SL, TP, LONG, SHORT

- If signal looks like a trade → ALWAYS return OPEN

- ENTRY can be missing → assume it's valid

---

EXTRACTION:

BASE:
- Extract from #ETHUSDT → ETH
- Remove USDT

SIDE:
- LONG or SHORT

LEVERAGE:
- X10, 10x → 10

RISK:
- "1.5% balance" → 1.5

SL:
- number after SL

TP:
- first number from TARGET

---

RR CALCULATION:

LONG:
RR = (TP - ENTRY) / (ENTRY - SL)

SHORT:
RR = (ENTRY - TP) / (SL - ENTRY)

---

CONFIDENCE:

- RR < 1 → 0.5
- RR 1-2 → 0.75
- RR > 2 → 0.9+

---

OUTPUT FORMAT:

{
  "action": "OPEN",
  "base": "...",
  "side": "...",
  "leverage": ...,
  "risk_pct": ...,
  "sl": ...,
  "tp": ...,
  "add_pct": null,
  "rr": ...,
  "confidence": ...
}
"""

AI_JSON_SHAPE = {
    "action": "OPEN | CLOSE | ADD | SET_SL | SET_TP | BE | NONE",
    "base": "string|null",
    "bases": "array<string>|null",
    "side": "long|short|null",
    "leverage": "int|null",
    "risk_pct": "number|null",
    "sl": "number|null",
    "tp": "number|null",
    "add_pct": "number|null",
    "confidence": "0..1",
    "raw_text": "string|null",
    "rr": "number|null",
}

def _img_to_data_url(path: str) -> str:
    with open(path, "rb") as f:
        b = f.read()
    b64 = base64.b64encode(b).decode("utf-8")
    # Pyrogram download часто дає .jpg/.png; тип не критичний, але залишимо jpeg як у тебе
    return f"data:image/jpeg;base64,{b64}"

def ai_parse_trade_multi(text: Optional[str], image_paths: Optional[list[str]]) -> dict:
    
    log("INFO", f"AI CHECK: OpenAI={OpenAI} KEY={bool(OPENAI_API_KEY)}")
    
    if not OpenAI or not OPENAI_API_KEY:
        return {"action": "NONE", "confidence": 0.0, "raw_text": "OpenAI not configured"}

    client = OpenAI(api_key=OPENAI_API_KEY)

    user_instructions = (
        "Відповідай ТІЛЬКИ JSON без пояснень.\n"
        f"Схема:\n{json.dumps(AI_JSON_SHAPE, ensure_ascii=False)}\n"
        "Якщо це CLOSE і на зображеннях кілька токенів — поверни всі в bases[].\n"
    )

    content: list[dict[str, Any]] = []
    if text and text.strip():
        content.append({"type": "text", "text": text.strip()})
    else:
        content.append({"type": "text", "text": "Розпізнай сигнал зі скрінів та поверни JSON-команду."})

    for p in (image_paths or []):
        content.append({"type": "image_url", "image_url": {"url": _img_to_data_url(p)}})

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": AI_SYSTEM},
            {"role": "user", "content": user_instructions},
            {"role": "user", "content": content},
        ],
        temperature=0,
    )

    out = (resp.choices[0].message.content or "").strip()
    log("INFO", f"AI_RESPONSE_RAW:\n{out[:1000]}")
    out = re.sub(r"^```(?:json)?\s*", "", out, flags=re.I).strip()
    out = re.sub(r"\s*```$", "", out).strip()

    try:
        data = json.loads(out)

        log("INFO", f"AI_PARSED: {data}")
        log("INFO", f"AI_JSON:\n{json.dumps(data, indent=2, ensure_ascii=False)}")

        if not isinstance(data, dict):
            raise ValueError("not dict")

        data.setdefault("action", "NONE")
        data.setdefault("confidence", 0.0)
        data.setdefault("raw_text", (text or "")[:500])

        return data

    except Exception as e:
        log("ERROR", f"AI JSON PARSE FAILED: {e}")
        log("ERROR", f"RAW WAS:\n{out}")

        return {
            "action": "NONE",
            "confidence": 0.0,
            "raw_text": out[:800]
        }

# =========================
# ACTION MIN CONF
# =========================
ACTION_MIN_CONF = {
    "CLOSE": 0.55,
    "SET_SL": 0.60,
    "SET_TP": 0.60,
    "BE": 0.60,
    "ADD": 0.65,
    "OPEN": 0.60,
}

# =========================
# EXECUTION ROUTER
# =========================
def _clean_base(x: str) -> str:
    b = str(x).upper().strip()
    b = b.replace("USDT", "")
    b = b.replace("/", "")
    b = b.replace(":", "")
    b = TOKEN_ALIASES.get(b, b)
    return b


async def handle_ai_command(cmd: dict):
    action = (cmd.get("action") or "NONE").upper()
    conf = float(cmd.get("confidence") or 0.0)

    base = cmd.get("base")
    bases = cmd.get("bases")
    side = cmd.get("side")
    lev = cmd.get("leverage")
    risk_pct = cmd.get("risk_pct")
    sl = cmd.get("sl")
    tp = cmd.get("tp")
    add_pct = cmd.get("add_pct")

    tg_text = cmd.get("_tg_text", "")

    log("INFO", f"AI action={action} conf={conf} base={base} side={side} lev={lev} risk={risk_pct} sl={sl} tp={tp} add_pct={add_pct}")

    if action == "NONE":
        log("DEBUG", "AI SKIP: action=NONE")
        return

    min_conf = ACTION_MIN_CONF.get(action, 0.70)
    if conf < min_conf:
        log("INFO", f"AI SKIP: low confidence {conf} < {min_conf} for action={action}")
        return

    # -------------------------
    # CLOSE
    # -------------------------
    if action == "CLOSE":
        if not has_close_intent(tg_text):
            log("INFO", "SAFE SKIP CLOSE: no close intent words in text")
            return

        cleaned: list[str] = []
        if isinstance(bases, list) and bases:
            for b in bases:
                b2 = _clean_base(b)
                if b2 and b2 not in cleaned:
                    cleaned.append(b2)
        else:
            if base:
                b2 = _clean_base(base)
                if b2:
                    cleaned.append(b2)

        if not cleaned:
            log("INFO", "AI SKIP CLOSE: no bases found")
            return

        log("INFO", f"AI_CLOSE bases={cleaned}")

        for b in cleaned:
            symbol = await resolve_symbol(b)
            if not symbol:
                log("ERROR", f"CLOSE skip: symbol not listed on BingX: {b}/USDT")
                continue

            if DRY_RUN:
                log("INFO", f"DRY_RUN: CLOSE {b} skipped (test mode)")
                continue

            try:
                res = await close_position_full_oneway(b)
                log("INFO", f"SUCCESS CLOSE {b}: {res}")
            except Exception as e:
                log("ERROR", f"CLOSE {b} failed: {e}")
        return

    # -------------------------
    # OPEN
    # -------------------------
    if action == "OPEN":
        if not base:
            log("INFO", "AI SKIP OPEN: base missing")
            return
        if side not in {"long", "short"}:
            log("INFO", "AI SKIP OPEN: side missing/invalid")
            return
        if not lev or not risk_pct:
            log("INFO", "AI SKIP OPEN: leverage or risk_pct missing")
            return
        if sl is None or tp is None:
            log("INFO", "AI SKIP OPEN: sl or tp missing")
            return

        base_clean = _clean_base(base)
        symbol = await resolve_symbol(base_clean)
        if not symbol:
            log("ERROR", f"Symbol not listed on BingX: {base_clean}/USDT")
            return

        try:
            entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
        except Exception as e:
            log("ERROR", f"fetch_ticker failed: {e}")
            return

        sl_fixed = normalize_price_from_tail(float(sl), entry, side, "sl")
        tp_fixed = normalize_price_from_tail(float(tp), entry, side, "tp")
        log("INFO", f"FIX {base_clean} entry={entry} rawSL={sl} -> {sl_fixed} | rawTP={tp} -> {tp_fixed}")

        try:
            sl_prec = float(await asyncio.to_thread(exchange.price_to_precision, symbol, sl_fixed))
        except Exception:
            sl_prec = float(sl_fixed)

        try:
            tp_prec = float(await asyncio.to_thread(exchange.price_to_precision, symbol, tp_fixed))
        except Exception:
            tp_prec = float(tp_fixed)

        if not validate_sl_tp(side, entry, sl_prec, tp_prec):
            log("INFO", f"SKIP Bad SL/TP vs entry. entry={entry} SL={sl_prec} TP={tp_prec}")
            return

        try:
            usdt_free = await get_usdt_free()
            qty_raw = calc_qty(usdt_free, float(risk_pct), int(lev), entry)
            qty = float(await asyncio.to_thread(exchange.amount_to_precision, symbol, qty_raw))
            log("INFO", f"QTY USDT free={usdt_free} qty_raw≈{qty_raw} qty_prec={qty}")
        except Exception as e:
            log("ERROR", f"balance/qty failed: {e}")
            return

        if qty <= 0:
            log("INFO", "SKIP qty became 0")
            return

        if DRY_RUN:
            log("INFO", "DRY_RUN OPEN skipped (test mode)")
            return

        try:
            try:
                await set_leverage(symbol, int(lev))
            except Exception as e:
                log("WARNING", f"set_leverage warning: {e}")

            resp = await open_market(symbol, side, qty)
            log("INFO", f"SUCCESS OPEN placed id={resp.get('id')} {base_clean} side={side} qty={qty}")

            try:
                r1 = await set_sl_oneway(base_clean, sl_prec)  # cancel old SL inside
                log("INFO", f"SL {r1}")
            except Exception as e:
                log("WARNING", f"SL not set: {e}")

            try:
                r2 = await set_tp_oneway(base_clean, tp_prec)
                log("INFO", f"TP {r2}")
            except Exception as e:
                log("WARNING", f"TP not set: {e}")

        except Exception as e:
            log("ERROR", f"OPEN failed: {e}")
        return
    # -------------------------
    # ADD (from balance)
    # -------------------------

    if action == "ADD":

        pct = risk_pct or add_pct

        if not base or pct is None:
            log("INFO", "ADD skip: base or pct missing")
            return

        base_clean = _clean_base(base)
        symbol = await resolve_symbol(base_clean)

        if not symbol:
            log("ERROR", f"symbol not listed: {base_clean}")
            return

        try:
            entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
        except Exception as e:
            log("ERROR", f"ticker failed: {e}")
            return

        pos = await fetch_position_oneway(symbol)

        if not pos:
            log("ERROR", "ADD: no existing position")
            return

        side = (pos.get("side") or "").lower()

        if side not in {"long", "short"}:
            log("ERROR", "ADD: no existing position")
            return

        lev = int(float(pos.get("leverage") or 1))

        try:
            usdt_free = await get_usdt_free()

            margin = usdt_free * (pct / 100)
            notional = margin * lev
            qty_raw = notional / entry

            qty = float(await asyncio.to_thread(exchange.amount_to_precision, symbol, qty_raw))

        except Exception as e:
            log("ERROR", f"balance calc failed: {e}")
            return

        if qty <= 0:
            log("INFO", "ADD qty=0")
            return

        if DRY_RUN:
            log("INFO", f"DRY_RUN ADD {base_clean}")
            return

        try:
            resp = await open_market(symbol, side, qty)
            log("INFO", f"SUCCESS ADD {base_clean} qty={qty}")

        except Exception as e:
            log("ERROR", f"ADD failed {e}")
            return

        if sl:
            try:
                r = await set_sl_oneway(base_clean, float(sl))
                log("INFO", f"UPDATED SL {r}")
            except Exception as e:
                log("ERROR", f"SL update failed {e}")

        return
    
    # -------------------------
    # SET_SL (always cancel old -> set new)
    # -------------------------
    if action == "SET_SL":
        if not base or sl is None:
            log("INFO", "AI SKIP SET_SL: base or sl missing")
            return

        base_clean = _clean_base(base)
        symbol = await resolve_symbol(base_clean)
        if not symbol:
            log("ERROR", f"SET_SL skip: symbol not listed: {base_clean}/USDT")
            return

        new_sl = float(sl)

        pos = await fetch_position_oneway(symbol)
        pos_side = (pos.get("side") or "").lower() if pos else None
        if pos_side in {"long", "short"}:
            try:
                last = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
                new_sl = normalize_price_from_tail(new_sl, last, pos_side, "sl")
                log("INFO", f"FIX SET_SL {base_clean} raw={sl} -> {new_sl}")
            except Exception:
                pass

        if DRY_RUN:
            log("INFO", f"DRY_RUN SET_SL {base_clean} skipped (test mode)")
            return

        try:
            res = await set_sl_oneway(base_clean, new_sl)  # cancels old SL inside
            log("INFO", f"SUCCESS SET_SL {base_clean}: {res}")
        except Exception as e:
            log("ERROR", f"SET_SL failed: {e}")
        return

    # -------------------------
    # SET_TP
    # -------------------------
    if action == "SET_TP":
        if not base or tp is None:
            log("INFO", "AI SKIP SET_TP: base or tp missing")
            return

        base_clean = _clean_base(base)
        symbol = await resolve_symbol(base_clean)
        if not symbol:
            log("ERROR", f"SET_TP skip: symbol not listed: {base_clean}/USDT")
            return

        new_tp = float(tp)

        pos = await fetch_position_oneway(symbol)
        pos_side = (pos.get("side") or "").lower() if pos else None
        if pos_side in {"long", "short"}:
            try:
                last = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
                new_tp = normalize_price_from_tail(new_tp, last, pos_side, "tp")
                log("INFO", f"FIX SET_TP {base_clean} raw={tp} -> {new_tp}")
            except Exception:
                pass

        if DRY_RUN:
            log("INFO", f"DRY_RUN SET_TP {base_clean} skipped (test mode)")
            return

        try:
            res = await set_tp_oneway(base_clean, new_tp)
            log("INFO", f"SUCCESS SET_TP {base_clean}: {res}")
        except Exception as e:
            log("ERROR", f"SET_TP failed: {e}")
        return

    # -------------------------
    # BE (cancel old SL -> set SL=entry)
    # -------------------------
    if action == "BE":
        if not base:
            log("INFO", "AI SKIP BE: base missing")
            return

        base_clean = _clean_base(base)
        symbol = await resolve_symbol(base_clean)
        if not symbol:
            log("ERROR", f"BE skip: symbol not listed: {base_clean}/USDT")
            return

        if DRY_RUN:
            log("INFO", f"DRY_RUN BE {base_clean} skipped (test mode)")
            return

        try:
            log("INFO", f"BE {base_clean}: cancel old SL and set SL=entry")
            res = await breakeven_oneway(base_clean)
            log("INFO", f"SUCCESS BE {base_clean}: {res}")
        except Exception as e:
            log("ERROR", f"BE failed: {e}")
        return

    log("INFO", f"Unknown/unsupported action: {action}")


# =========================
# MEDIA GROUP / CLOSE BUNDLE (async)
# =========================
MEDIA_BUF: dict[str, dict[str, Any]] = {}
MEDIA_LOCK = asyncio.Lock()

CLOSE_LOCK = asyncio.Lock()
CLOSE_BUNDLE = {"ts": 0.0, "text": "", "images": [], "task": None}

async def album_add(gid: str, text: str, img_path: str):
    async with MEDIA_LOCK:
        buf = MEDIA_BUF.get(gid)
        if not buf:
            buf = {"text": "", "images": [], "task": None, "last_ts": time.time()}
            MEDIA_BUF[gid] = buf

        if text and not buf["text"]:
            buf["text"] = text

        if img_path and img_path not in buf["images"]:
            buf["images"].append(img_path)

        buf["last_ts"] = time.time()

        if buf["task"]:
            buf["task"].cancel()

        buf["task"] = asyncio.create_task(album_flush_later(gid))

async def album_flush_later(gid: str):
    try:
        await asyncio.sleep(MEDIA_DELAY_SEC)
        await album_flush(gid)
    except asyncio.CancelledError:
        return

async def album_flush(gid: str):
    async with MEDIA_LOCK:
        payload = MEDIA_BUF.pop(gid, None)

    if not payload:
        return

    text = (payload.get("text") or "").strip()
    images = (payload.get("images") or [])[:4]

    log("INFO", f"ALBUM media_group={gid} images={len(images)}")
    log("INFO", f"AI_RAW {text[:400] if text else '<no text>'}")

    if not text and not images:
        return

    if text and has_close_intent(text):
        await close_bundle_start_or_update(text=text, images=images)
        return

    cmd = await asyncio.to_thread(ai_parse_trade_multi, text if text else None, images)
    cmd["_tg_text"] = text
    await handle_ai_command(cmd)

async def close_bundle_start_or_update(text: str, images: list[str]):
    async with CLOSE_LOCK:
        CLOSE_BUNDLE["ts"] = time.time()
        if text and has_close_intent(text):
            CLOSE_BUNDLE["text"] = text

        for p in images or []:
            if p and p not in CLOSE_BUNDLE["images"] and len(CLOSE_BUNDLE["images"]) < 8:
                CLOSE_BUNDLE["images"].append(p)

        if CLOSE_BUNDLE["task"]:
            CLOSE_BUNDLE["task"].cancel()

        CLOSE_BUNDLE["task"] = asyncio.create_task(close_bundle_flush_later())

async def close_bundle_flush_later():
    try:
        await asyncio.sleep(2.0)
        await close_bundle_flush()
    except asyncio.CancelledError:
        return

async def close_bundle_attach_orphan_photo(img_path: str) -> bool:
    async with CLOSE_LOCK:
        if not CLOSE_BUNDLE["text"]:
            return False
        if time.time() - float(CLOSE_BUNDLE["ts"]) > CLOSE_BUNDLE_WINDOW_SEC:
            return False

        if img_path and img_path not in CLOSE_BUNDLE["images"] and len(CLOSE_BUNDLE["images"]) < 8:
            CLOSE_BUNDLE["images"].append(img_path)

        if CLOSE_BUNDLE["task"]:
            CLOSE_BUNDLE["task"].cancel()

        CLOSE_BUNDLE["task"] = asyncio.create_task(close_bundle_flush_later())
        return True

async def close_bundle_flush():
    async with CLOSE_LOCK:
        text = (CLOSE_BUNDLE.get("text") or "").strip()
        images = (CLOSE_BUNDLE.get("images") or [])[:4]
        CLOSE_BUNDLE["text"] = ""
        CLOSE_BUNDLE["images"] = []
        CLOSE_BUNDLE["ts"] = 0.0
        CLOSE_BUNDLE["task"] = None

    if not text:
        return

    log("INFO", f"BUNDLE close_bundle images={len(images)}")
    log("INFO", f"AI_RAW {text[:400]}")

    cmd = await asyncio.to_thread(ai_parse_trade_multi, text, images)
    cmd["_tg_text"] = text
    await handle_ai_command(cmd)


# =========================
# TG HANDLER: read from TARGET_CHAT_ID
# =========================
_last_hb = 0.0

@app.on_message(
    filters.chat([TARGET_CHAT_ID, "me"])
    & (filters.text | filters.caption | filters.photo)
)

async def on_signal(_, message):
    global _last_hb

    # не логимо власний лог-чат, якщо раптом він = target
    if message.chat and int(message.chat.id) == int(LOG_CHAT_ID):
        return

    now = time.time()
    if now - _last_hb >= HEARTBEAT_SEC:
        _last_hb = now
        log("INFO", f"HB alive DRY_RUN={DRY_RUN} model={OPENAI_MODEL}")

    text = (message.text or message.caption or "").strip()

    img_path = None
    if message.photo:
        try:
            img_path = await message.download()
            log("INFO", f"IMG downloaded: {img_path}")
        except Exception as e:
            log("ERROR", f"photo download failed: {e}")
            img_path = None

    # album (media_group)
    if message.media_group_id and img_path:
        gid = str(message.media_group_id)
        await album_add(gid=gid, text=text, img_path=img_path)
        return

    # orphan photo without text -> attach to close bundle if possible
    if img_path and not text:
        if await close_bundle_attach_orphan_photo(img_path):
            log("INFO", "BUNDLE orphan photo attached to last CLOSE text")
            return
        log("INFO", "AI_RAW <no text> (single image)")
        cmd = await asyncio.to_thread(ai_parse_trade_multi, None, [img_path])
        cmd["_tg_text"] = ""
        await handle_ai_command(cmd)
        return
        
    # close intent -> bundle
    if text and has_close_intent(text):
        await close_bundle_start_or_update(text=text, images=[img_path] if img_path else [])
        return

    if not text and not img_path:
        return

    log("INFO", f"AI_RAW {text[:400] if text else '<no text>'}")
    cmd = await asyncio.to_thread(ai_parse_trade_multi, text if text else None, [img_path] if img_path else [])
    cmd["_tg_text"] = text
    await handle_ai_command(cmd)


# =========================
# MAIN (Railway safe)
# =========================

async def main():
    await app.start()
    asyncio.create_task(pnl_watcher(app, exchange, log, LOG_CHAT_ID))
    asyncio.create_task(log_pump())

    try:
        ok = await ensure_peer_known(TARGET_CHAT_ID)
        while not ok:
            log("WARNING", "⏳ TARGET retry in 60s…")
            await asyncio.sleep(60)
            ok = await ensure_peer_known(TARGET_CHAT_ID)

        if LOG_CHAT_ID:
            ok2 = await ensure_peer_known(LOG_CHAT_ID)
            if ok2:
                await _send_to_tg(f"[{_ts()}] [INFO] 🧾 Telegram logging ON. log_chat_id={LOG_CHAT_ID}")
            else:
                log("ERROR", f"Telegram logging FAILED. log_chat_id={LOG_CHAT_ID}")

        try:
            await ensure_markets_loaded()
            log("INFO", "BINGX markets loaded")
        except Exception as e:
            log("ERROR", f"BINGX load_markets failed: {e}")

        log("INFO", f"DRY_RUN={DRY_RUN} | Listening TARGET_CHAT_ID={TARGET_CHAT_ID}")
        await idle()

    finally:
        await app.stop()

if __name__ == "__main__":
    app.run(main())