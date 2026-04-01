import os
import re
import json
import time
import base64
import asyncio
import hashlib
import hmac
from urllib.parse import urlencode
from datetime import datetime

import requests
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
PNL_CHAT_ID = int(os.getenv("PNL_CHAT_ID", "-1003332013833")) # куди шлемо профіт/лос

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "")
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "")
BINGX_SWAP_HOST = os.getenv("BINGX_SWAP_HOST", "https://open-api.bingx.com").rstrip("/")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")

DRY_RUN = os.getenv("DRY_RUN", "1").strip() == "1"
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))  # 5 хв

MEDIA_DELAY_SEC = float(os.getenv("MEDIA_DELAY_SEC", "5"))  # wait for album completion
CLOSE_BUNDLE_WINDOW_SEC = float(os.getenv("CLOSE_BUNDLE_WINDOW_SEC", "15"))  # attach orphan photos

# --- TG logging config (hardcoded) ---
LOG_LEVEL = "INFO"   # DEBUG / INFO / WARNING / ERROR
LOG_FLUSH_SEC = 20   # INFO пачкою раз на N секунд
SLTP_FILE = "/data/sltp.json"
LAST_SLTP = {}

# =========================
# PYROGRAM CLIENT (USER)
# =========================
app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

def save_sltp():
    try:
        with open(SLTP_FILE, "w") as f:
            json.dump(LAST_SLTP, f)
    except Exception as e:
        print("SLTP save error:", e)

def cancel_all_stops_sync(symbol: str, side: str, pos_side: str, kind: str):
    try:
        orders = exchange.fetch_open_orders(symbol)

        for o in orders:
            try:
                o_side = (o.get("side") or "").lower()
                o_info = o.get("info") or {}

                o_pos_side = str(o_info.get("positionSide") or "").lower()

                # 🔥 фільтр
                if o_side != side:
                    continue

                
                if o_pos_side != pos_side.lower():
                    continue

                if kind == "sl":
                    if not is_sl_order(o):
                        continue

                if kind == "tp":
                    if not is_tp_order(o):
                        continue

                exchange.cancel_order(o["id"], symbol)
                print(
                    f"CANCEL kind={kind} symbol={symbol} "
                    f"type={o.get('type')} side={o_side} posSide={o_pos_side} id={o['id']}"
                )

            except Exception:
                continue

    except Exception as e:
        print("cancel_all_stops error:", e)

def load_sltp():
    global LAST_SLTP
    try:
        if os.path.exists(SLTP_FILE):
            with open(SLTP_FILE, "r") as f:
                LAST_SLTP = json.load(f)
    except Exception as e:
        print("SLTP load error:", e)

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

def set_leverage_sync(symbol: str, lev: int, side: str):

    pos_side = "LONG" if side == "long" else "SHORT"

    # 🔥 важливо
    exchange.set_margin_mode("cross", symbol)

    # 🔥 пробуємо різні варіанти (бо BingX кривий)
    variants = [
        {"positionSide": pos_side},
        {"side": pos_side},
        {"positionSide": pos_side, "side": pos_side},
    ]

    last_err = None

    for params in variants:
        try:
            exchange.set_leverage(int(lev), symbol, params)
            log("INFO", f"LEVERAGE SET OK {symbol} {lev} {pos_side} params={params}")
            return
        except Exception as e:
            last_err = e

    raise RuntimeError(f"set_leverage failed: {last_err}")

async def set_leverage(symbol: str, lev: int, side: str):
    await asyncio.to_thread(set_leverage_sync, symbol, lev, side)

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
        }
    )

async def open_market(symbol: str, side: str, qty: float):
    return await asyncio.to_thread(open_market_sync, symbol, side, qty)

def place_dca_order_sync(symbol: str, side: str, qty: float, price: float):
    order_side = "sell" if side == "short" else "buy"
    position_side = "SHORT" if side == "short" else "LONG"

    return exchange.create_order(
        symbol,
        "limit",
        order_side,
        qty,
        price,
        {
            "positionSide": position_side
        }
    )

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

def close_position_full_sync(base: str):
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
            abs(
                pos.get("contracts")
                or pos.get("size")
                or pos.get("positionAmt")
                or 0
            )
        )

        contracts = float(exchange.amount_to_precision(symbol, contracts))

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
                "positionSide": "LONG" if side == "long" else "SHORT"
            }
        )

        closed.append(side)

    if not closed:
        return "NO_POSITION"

    return f"CLOSED {'/'.join(closed)}"


async def close_position_full(base: str):
    return await asyncio.to_thread(close_position_full_sync, base)

def set_margin_mode_sync(symbol: str):
    try:
        exchange.set_margin_mode("cross", symbol)
    except Exception:
        pass

async def set_margin_mode(symbol: str):
    await asyncio.to_thread(set_margin_mode_sync, symbol)

# =========================
# STOP/TP helpers (RAW BingX API)
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

def _symbol_to_bingx_market_id(symbol: str) -> str:
    s = str(symbol or "").upper()
    s = s.replace(":USDT", "")
    s = s.replace("/", "-")
    return s

def _fmt_num(x: float) -> str:
    s = f"{float(x):.16f}"
    s = s.rstrip("0").rstrip(".")
    return s or "0"

def _bingx_raw_request_sync(method: str, path: str, params: dict) -> dict:
    if not BINGX_API_KEY or not BINGX_API_SECRET:
        raise RuntimeError("BingX API keys are missing")

    payload = {k: v for k, v in (params or {}).items() if v is not None}
    payload["timestamp"] = int(time.time() * 1000)
    query = urlencode(sorted(payload.items()), doseq=True)
    sign = hmac.new(
        BINGX_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url = f"{BINGX_SWAP_HOST}{path}?{query}&signature={sign}"
    headers = {
        "X-BX-APIKEY": BINGX_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    log("INFO", f"RAW HTTP {method.upper()} {path} payload={payload}")
    resp = requests.request(method.upper(), url, headers=headers, timeout=5)
    log("INFO", f"RAW HTTP DONE status={resp.status_code} path={path}")
    body_text = resp.text

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"BingX RAW {method} {path} bad json: {body_text[:500]}")

    code = str(data.get("code", ""))
    if resp.status_code >= 400 or code not in {"0", "", "None"}:
        raise RuntimeError(
            f"BingX RAW {method} {path} failed status={resp.status_code} code={data.get('code')} msg={data.get('msg') or data.get('message')} body={body_text[:500]}"
        )

    return data

def _place_bingx_tpsl_raw_sync(symbol: str, pos_side: str, trigger_price: float, quantity: float, kind: str) -> dict:
    side = "SELL" if pos_side.lower() == "long" else "BUY"
    order_type = "STOP_MARKET" if kind == "sl" else "TAKE_PROFIT_MARKET"

    payload = {
        "symbol": _symbol_to_bingx_market_id(symbol),
        "side": side,
        "positionSide": pos_side.upper(),
        "type": order_type,
        "quantity": _fmt_num(quantity),
        "stopPrice": _fmt_num(trigger_price),
        "workingType": "MARK_PRICE",
        "reduceOnly": "true",
    }

    return _bingx_raw_request_sync("POST", "/openApi/swap/v2/trade/order", payload)

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
    log("INFO", f"SET_SL start base={base} sl={sl_price}")
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

    contracts = float(abs(pos.get("contracts") or pos.get("size") or pos.get("positionAmt") or 0))
    contracts = float(exchange.amount_to_precision(symbol, contracts))
    if contracts <= 0:
        return "NO_POSITION"

    stop_side = "sell" if pos_side == "long" else "buy"

    try:
        sl_prec = float(exchange.price_to_precision(symbol, float(sl_price)))
    except Exception:
        sl_prec = float(sl_price)

    log("INFO", f"SET_SL prepared symbol={symbol} pos_side={pos_side} stop_side={stop_side} qty={contracts} sl_prec={sl_prec}")
    log("INFO", "SET_SL cancel old stops start")
    cancel_all_stops_sync(symbol, stop_side, pos_side, "sl")
    log("INFO", "SET_SL cancel old stops done")
    resp = _place_bingx_tpsl_raw_sync(symbol, pos_side, sl_prec, contracts, "sl")
    log("INFO", f"SET_SL raw response={resp}")

    data = resp.get("data") or {}
    new_id = data.get("orderId") or data.get("id") or data.get("clientOrderId")
    return f"SL_SET_RAW id={new_id} sl={sl_prec}"

async def set_sl_oneway(base: str, sl_price: float) -> str:
    return await asyncio.to_thread(set_sl_oneway_sync, base, sl_price)

def set_tp_oneway_sync(base: str, tp_price: float) -> str:
    log("INFO", f"SET_TP start base={base} tp={tp_price}")
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

    contracts = float(abs(pos.get("contracts") or pos.get("size") or pos.get("positionAmt") or 0))
    contracts = float(exchange.amount_to_precision(symbol, contracts))
    if contracts <= 0:
        return "NO_POSITION"

    close_side = "sell" if pos_side == "long" else "buy"

    try:
        tp_prec = float(exchange.price_to_precision(symbol, float(tp_price)))
    except Exception:
        tp_prec = float(tp_price)

    log("INFO", f"SET_TP prepared symbol={symbol} pos_side={pos_side} close_side={close_side} qty={contracts} tp_prec={tp_prec}")
    log("INFO", "SET_TP cancel old tps start")
    cancel_all_stops_sync(symbol, close_side, pos_side, "tp")
    log("INFO", "SET_TP cancel old tps done")
    resp = _place_bingx_tpsl_raw_sync(symbol, pos_side, tp_prec, contracts, "tp")
    log("INFO", f"SET_TP raw response={resp}")

    data = resp.get("data") or {}
    new_id = data.get("orderId") or data.get("id") or data.get("clientOrderId")
    return f"TP_SET_RAW id={new_id} tp={tp_prec}"

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

    contracts = float(
    abs(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    )
)

    contracts = float(exchange.amount_to_precision(symbol, contracts))

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
    }
)
    return f"ADDED id={resp.get('id')} qty={add_qty} pct={pct}"

async def add_position_oneway(base: str, add_pct: Optional[float]) -> str:
    return await asyncio.to_thread(add_position_oneway_sync, base, add_pct)


async def wait_position_update(symbol: str, timeout: float = 5.0) -> bool:
    start = time.time()

    while time.time() - start < timeout:
        pos = await fetch_position_oneway(symbol)
        if pos:
            size = float(
                pos.get("contracts")
                or pos.get("size")
                or pos.get("positionAmt")
                or 0
            )
            if abs(size) > 0:
                return True

        await asyncio.sleep(0.3)

    return False


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


def calc_tp_from_rr(entry: float, sl: float, rr: float, side: str) -> float:
    entry = float(entry)
    sl = float(sl)
    rr = float(rr)

    risk = abs(entry - sl)
    if risk <= 0 or rr <= 0:
        raise ValueError("bad rr inputs")

    if side == "short":
        return entry - risk * rr
    return entry + risk * rr


def extract_rr_from_text(text: str) -> Optional[float]:
    t = (text or "").strip()
    if not t:
        return None

    patterns = [
        r'\brr\s*[:=\-]?\s*(\d+(?:[\.,]\d+)?)\b',      # RR2 / RR 2 / RR:2
        r'\brr\s*1\s*[:/]\s*(\d+(?:[\.,]\d+)?)\b',     # RR 1:2
        r'\b(\d+(?:[\.,]\d+)?)\s*r\b',                   # 2R / 2.5R
        r'\btp\s*(?:at|@)?\s*(\d+(?:[\.,]\d+)?)\s*r\b' # TP at 3R
    ]

    for pat in patterns:
        m = re.search(pat, t, re.I)
        if not m:
            continue
        raw = m.group(1).replace(",", ".")
        try:
            rr = float(raw)
            if rr > 0:
                return rr
        except Exception:
            continue

    return None

def normalize_price_from_tail(raw: float, entry: float, side: str, kind: str) -> float:
    raw = float(raw)
    entry = float(entry)

    # якщо це вже адекватна “маленька” ціна — не чіпаємо
    if 0.5 * entry <= raw <= 1.5 * entry:
        return raw

    best = None
    best_score = float("inf")

    if raw > entry * 1000:
        raw = raw / 100

    for k in range(0, 13):
        cand = raw / (10 ** k)
        if cand <= 0:
            continue

        ratio = cand / entry if entry > 0 else 999.0
        if ratio < 0.00001 or ratio > 100:
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
    r"\btp\b",
    r"\btp\d\b",
    r"\btake\s+profit\b",
    r"\btaking\s+profit\b",
    r"\bclose\b",
    r"\bclosing\b",
    r"\bclosed\b",
    r"\bexit\b",
    r"\bexiting\b",
    r"\broe\b",
    r"\broi\b",
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
Convert any trading signal into VALID JSON.

CRITICAL RULES:
- ALWAYS return JSON only.
- NEVER return NONE if there is any actionable trading intent.
- Confidence must reflect PARSING certainty, not trade quality.
- Do not return confidence 0.0 when the command is structurally clear.

ACTION RULES:
- If signal contains add / adding / add more / increasing size / increase size / increase position / scale in / averaging / dca -> action = ADD
- If signal contains stop loss / stoploss / SL -> action = SET_SL unless it is clearly a full OPEN signal
- If signal contains take profit / TP / target update -> action = SET_TP unless it is clearly a full OPEN signal
- If signal contains break even / breakeven / BE -> action = BE
- If signal contains close / closing / closed / exit / take profit hit / TP hit -> action = CLOSE
- Otherwise, if it is a full entry setup -> action = OPEN

EXTRACTION RULES:
- BASE: extract ticker and remove USDT. Example: #ETHUSDT -> ETH
- SIDE: long or short
- LEVERAGE: parse X10 / 10x / leverage 10
- RISK_PCT: parse phrases like "1.5% balance", "risk 2%", "margin 0.75%"
- ADD_PCT: for ADD signals parse the percentage being added now
- SL: number after SL / stop loss
- TP: first target PRICE number if a real target price is present
- RR: if take profit is expressed as RR instead of a price, extract rr as a positive number
- Examples of RR targets:
  - RR2 -> rr=2
  - RR 1:2 -> rr=2
  - TP at 3R -> rr=3
  - Target 2R -> rr=2
- If TP is expressed only as RR, set "tp": null and fill "rr"
- DCA_PRICE: if ADD includes a specific price/zone, extract it
- PRICE: if signal explicitly provides an add/limit price, extract it
- ENTRY may be missing and that is acceptable

DCA RULES:
- If ADD has a specific price -> treat it as DCA
- If ADD has no specific price -> treat it as market ADD

CONFIDENCE RULES:
- If action/base/add_pct or risk_pct are clearly present for ADD, confidence should be at least 0.85
- If action/base/side/sl and either tp or rr are clearly present for OPEN, confidence should be at least 0.85
- If CLOSE intent is explicit and ticker is clear, confidence should be at least 0.85
- Use low confidence only when fields are ambiguous or missing
- RR may be used only as an extra hint for OPEN setups
- Do not reduce confidence for ADD / SET_SL / SET_TP / CLOSE just because RR is unavailable

RR CALCULATION (ONLY when entry/sl/tp are available for OPEN):
- LONG: RR = (TP - ENTRY) / (ENTRY - SL)
- SHORT: RR = (ENTRY - TP) / (SL - ENTRY)

OUTPUT FORMAT:
{
  "action": "OPEN | CLOSE | ADD | SET_SL | SET_TP | BE | NONE",
  "base": "string|null",
  "bases": ["string"] | null,
  "side": "long|short|null",
  "leverage": 10,
  "risk_pct": 1.5,
  "sl": 123.45,
  "tp": 120.00,
  "add_pct": 0.75,
  "confidence": 0.92,
  "raw_text": "string|null",
  "rr": 2.5,
  "dca_price": 123.0,
  "dca_pct": 0.75,
  "price": 123.0
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
    "dca_price": "number|null",
    "dca_pct": "number|null",
    "price": "number|null", 
}

def is_sl_order(o):
    t = (o.get("type") or "").lower()
    info = o.get("info") or {}

    if "take" in t or "profit" in t:
        return False

    if str(info.get("takeProfit") or "").strip():
        return False

    client_oid = str(
        info.get("clientOrderId")
        or info.get("clientOrderID")
        or ""
    ).lower()

    if "tp" in client_oid or "take" in client_oid or "profit" in client_oid:
        return False

    if "stop" in t and "take" not in t and "profit" not in t:
        return True

    if str(info.get("stopLoss") or "").strip():
        return True

    if "sl" in client_oid or "stop" in client_oid:
        return True

    return False


def is_tp_order(o):
    t = (o.get("type") or "").lower()
    info = o.get("info") or {}

    if "take" in t or "profit" in t:
        return True

    if str(info.get("takeProfit") or "").strip():
        return True

    client_oid = str(
        info.get("clientOrderId")
        or info.get("clientOrderID")
        or ""
    ).lower()

    if "tp" in client_oid or "take" in client_oid or "profit" in client_oid:
        return True

    return False

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
    "Reply with JSON only, without explanations.\n"
    f"Schema:\n{json.dumps(AI_JSON_SHAPE, ensure_ascii=False)}\n"
    "If this is CLOSE and there are multiple tickers in the images, return all of them in bases[].\n"
    )

    content: list[dict[str, Any]] = []
    if text and text.strip():
        content.append({"type": "text", "text": text.strip()})
    else:
        content.append({"type": "text", "text": "Parse the trading signal from the screenshots and return a JSON command."})

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
    out = re.sub(r"^```(?:json)?\s*", "", out, flags=re.I).strip()
    out = re.sub(r"\s*```$", "", out).strip()

    try:
        data = json.loads(out)

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
    "ADD": 0.00,
    "OPEN": 0.00,
}

# =========================
# EXECUTION ROUTER
# =========================
def _has_add_intent_text(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return bool(re.search(r"\b(add|adding|increase|increasing|scale\s*in|averag\w*|dca)\b", t, re.I))


def _has_add_intent_text(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return bool(re.search(r"\b(add|adding|increase|increasing|scale\s*in|averag\w*|dca)\b", t, re.I))


def calc_tp_from_rr(entry: float, sl: float, rr: float, side: str) -> float:
    entry = float(entry)
    sl = float(sl)
    rr = float(rr)

    risk = abs(entry - sl)
    if risk <= 0 or rr <= 0:
        raise ValueError("bad rr inputs")

    if side == "short":
        return entry - risk * rr
    return entry + risk * rr


def extract_rr_from_text(text: str) -> Optional[float]:
    t = (text or "").strip()
    if not t:
        return None

    patterns = [
        r'\brr\s*[:=\-]?\s*(\d+(?:[\.,]\d+)?)\b',
        r'\brr\s*1\s*[:/]\s*(\d+(?:[\.,]\d+)?)\b',
        r'\b(\d+(?:[\.,]\d+)?)\s*r\b',
        r'\btp\s*(?:at|@)?\s*(\d+(?:[\.,]\d+)?)\s*r\b',
    ]

    for pat in patterns:
        m = re.search(pat, t, re.I)
        if not m:
            continue
        raw = m.group(1).replace(",", ".")
        try:
            rr = float(raw)
            if rr > 0:
                return rr
        except Exception:
            continue

    return None


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

    # -------------------------
    # TEXT-BASED FORCE FIXES
    # -------------------------
    if action in {"NONE", "OPEN"} and _has_add_intent_text(tg_text):
        if base and (add_pct is not None or risk_pct is not None):
            log("WARNING", "FORCE FIX: OPEN/NONE -> ADD by text rule")
            action = "ADD"

    if action == "NONE":
        log("DEBUG", "AI SKIP: action=NONE")
        return

    min_conf = ACTION_MIN_CONF.get(action, 0.70)

    # -------------------------
    # CONFIDENCE GATE
    # -------------------------
    # Для ADD опираємось на структуру, а не на self-reported confidence.
    if action == "ADD":
        if not base:
            log("INFO", "AI SKIP ADD: base missing")
            return
        if add_pct is None and risk_pct is None:
            log("INFO", "AI SKIP ADD: no add_pct/risk_pct")
            return
    elif action not in {"SET_SL", "SET_TP", "BE", "CLOSE"} and conf < min_conf:
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
                res = await close_position_full(b)
                if b in LAST_SLTP:
                    del LAST_SLTP[b]
                    save_sltp()
                log("INFO", f"SLTP cleared for {b}")
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

        rr_value = cmd.get("rr")
        if rr_value is None:
            rr_value = extract_rr_from_text(tg_text)

        if sl is None:
            log("INFO", "AI SKIP OPEN: sl missing")
            return

        if tp is None and rr_value is None:
            log("INFO", "AI SKIP OPEN: tp/rr missing")
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

        if tp is None and rr_value is not None:
            try:
                tp = calc_tp_from_rr(entry, sl_fixed, float(rr_value), side)
                log("INFO", f"TP_FROM_RR {base_clean} entry={entry} sl={sl_fixed} rr={rr_value} -> tp={tp}")
            except Exception as e:
                log("ERROR", f"TP_FROM_RR failed: {e}")
                return

        tp_fixed = normalize_price_from_tail(float(tp), entry, side, "tp")

        log("INFO", f"FIX {base_clean} entry={entry} rawSL={sl} -> {sl_fixed} | rawTP={tp} -> {tp_fixed} | rr={rr_value}")

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

        log("INFO", f"TRY SET LEVERAGE {symbol} lev={lev} side={side}")

        try:
            await set_margin_mode(symbol)
            await set_leverage(symbol, int(lev), side)
            await asyncio.sleep(1.0)
            log("INFO", "LEVERAGE SET OK")
        except Exception as e:
            log("ERROR", f"LEVERAGE FAILED: {e}")

        log("INFO", f"TRY OPEN {symbol} side={side} qty={qty}")

        try:
            resp = await open_market(symbol, side, qty)
            log("INFO", f"SUCCESS OPEN placed id={resp.get('id')} {base_clean} side={side} qty={qty}")

            pos_seen = await wait_position_update(symbol, timeout=5.0)
            log("INFO", f"POSITION_VISIBLE_AFTER_OPEN {base_clean}={pos_seen}")
            await asyncio.sleep(0.5)

            LAST_SLTP[base_clean] = {"sl": sl_prec, "tp": tp_prec}
            save_sltp()

            sltp = LAST_SLTP.get(base_clean)
            if sltp:
                log("INFO", f"Reapplying SL/TP for {base_clean}")
                try:
                    log("INFO", f"REAPPLY SL start {base_clean} sl={sltp.get('sl')}")
                    sl_res = await set_sl_oneway(base_clean, sltp["sl"])
                    log("INFO", f"REAPPLY SL done {base_clean}: {sl_res}")
                except Exception as e:
                    log("WARNING", f"SL reset failed: {e}")

                try:
                    log("INFO", f"REAPPLY TP start {base_clean} tp={sltp.get('tp')}")
                    tp_res = await set_tp_oneway(base_clean, sltp["tp"])
                    log("INFO", f"REAPPLY TP done {base_clean}: {tp_res}")
                except Exception as e:
                    log("WARNING", f"TP reset failed: {e}")
            else:
                log("WARNING", f"No SL/TP stored for {base_clean}")

            dca_price = cmd.get("dca_price")
            dca_pct = cmd.get("dca_pct")
            if dca_price and dca_pct:
                await place_dca(symbol, side, float(dca_pct), float(dca_price), lev)

        except Exception as e:
            log("ERROR", f"OPEN FAILED: {e}")
            return
        return

    # -------------------------
    # ADD
    # -------------------------
    if action == "ADD":
        base_clean = _clean_base(base)
        symbol = await resolve_symbol(base_clean)

        if not symbol:
            log("ERROR", f"symbol not listed: {base_clean}")
            return

        pos = await fetch_position_oneway(symbol)
        if not pos:
            log("ERROR", "ADD: no existing position")
            return

        side = (pos.get("side") or "").lower()
        lev = int(float(pos.get("leverage") or (pos.get("info") or {}).get("leverage") or 1))

        pct = add_pct if add_pct is not None else risk_pct
        if pct is None:
            log("INFO", "ADD skip: no pct")
            return

        mode = detect_add_mode(cmd)
        log("INFO", f"ADD MODE = {mode}")

        if mode == "DCA":
            dca_price = cmd.get("price") or cmd.get("dca_price")
            if not dca_price:
                log("ERROR", "DCA but no price")
                return

            dca_price = float(dca_price)

            try:
                usdt_free = await get_usdt_free()
                entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])

                margin = usdt_free * (float(pct) / 100.0)
                notional = margin * lev
                qty_raw = notional / entry
                qty = float(await asyncio.to_thread(exchange.amount_to_precision, symbol, qty_raw))

                if DRY_RUN:
                    log("INFO", f"DRY_RUN DCA {base_clean} at {dca_price} qty={qty}")
                    return

                await place_dca(symbol, side, float(pct), dca_price, lev)
                log("INFO", f"DCA placed {base_clean} at {dca_price} qty={qty}")
            except Exception as e:
                log("ERROR", f"DCA failed: {e}")
            return

        try:
            usdt_free = await get_usdt_free()
            pct = float(pct)
            margin = usdt_free * (pct / 100.0)
            notional = margin * lev

            entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
            qty_raw = notional / entry
            qty = float(await asyncio.to_thread(exchange.amount_to_precision, symbol, qty_raw))

            market = exchange.market(symbol)
            min_qty = market.get("limits", {}).get("amount", {}).get("min")
            if min_qty and qty < min_qty:
                qty = float(min_qty)
                log("INFO", f"ADD qty adjusted to min: {qty}")

            if DRY_RUN:
                log("INFO", f"DRY_RUN ADD {base_clean} qty={qty} (~{pct}% balance)")
                return

            resp = await open_market(symbol, side, qty)
            log("INFO", f"MARKET ADD {base_clean} qty={qty} (~{pct}% balance)")
            log("INFO", f"ADD RESPONSE: {resp}")

            pos_seen = await wait_position_update(symbol, timeout=5.0)
            log("INFO", f"POSITION_VISIBLE_AFTER_ADD {base_clean}={pos_seen}")
            await asyncio.sleep(0.5)

            sltp = LAST_SLTP.get(base_clean)
            if sltp:
                log("INFO", f"Reapplying SL/TP for {base_clean}")
                try:
                    log("INFO", f"REAPPLY SL start {base_clean} sl={sltp.get('sl')}")
                    sl_res = await set_sl_oneway(base_clean, sltp["sl"])
                    log("INFO", f"REAPPLY SL done {base_clean}: {sl_res}")
                except Exception as e:
                    log("WARNING", f"SL reset failed: {e}")

                try:
                    log("INFO", f"REAPPLY TP start {base_clean} tp={sltp.get('tp')}")
                    tp_res = await set_tp_oneway(base_clean, sltp["tp"])
                    log("INFO", f"REAPPLY TP done {base_clean}: {tp_res}")
                except Exception as e:
                    log("WARNING", f"TP reset failed: {e}")
            return
        except Exception as e:
            log("ERROR", f"ADD failed: {e}")
            return

    # -------------------------
    # SET_SL
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
            res = await set_sl_oneway(base_clean, new_sl)
            if base_clean in LAST_SLTP:
                LAST_SLTP[base_clean]["sl"] = new_sl
            else:
                LAST_SLTP[base_clean] = {"sl": new_sl, "tp": None}
            save_sltp()
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
            if base_clean in LAST_SLTP:
                LAST_SLTP[base_clean]["tp"] = new_tp
            else:
                LAST_SLTP[base_clean] = {"sl": None, "tp": new_tp}
            save_sltp()
            log("INFO", f"SUCCESS SET_TP {base_clean}: {res}")
        except Exception as e:
            log("ERROR", f"SET_TP failed: {e}")
        return

    # -------------------------
    # BE
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

def detect_add_mode(cmd: dict) -> str:
    """
    return:
    - "DCA"
    - "MARKET"
    """

    # 🔥 якщо є явний dca_price → це DCA
    if cmd.get("dca_price"):
        return "DCA"

    # 🔥 якщо є price → теж DCA
    if cmd.get("price"):
        return "DCA"

    return "MARKET"

async def place_dca(symbol, side, pct, price, lev):

    try:
        entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])

        if side == "long" and price >= entry:
            log("WARNING", "DCA skipped: price above entry for LONG")
            return

        if side == "short" and price <= entry:
            log("WARNING", "DCA skipped: price below entry for SHORT")
            return

        usdt_free = await get_usdt_free()
        margin = usdt_free * (pct / 100)
        notional = margin * lev
        qty_raw = notional / entry

        qty = float(await asyncio.to_thread(exchange.amount_to_precision, symbol, qty_raw))

        # 🔥 защита от дубля
        orders = await asyncio.to_thread(exchange.fetch_open_orders, symbol)

        for o in orders:
            o_price = float(o.get("price") or 0)

            if abs(o_price - float(price)) / float(price) < 0.001:
                log("INFO", "DCA already exists (approx match)")
                return

        await asyncio.to_thread(
            place_dca_order_sync,
            symbol,
            side,
            qty,
            price
        )

        log("INFO", f"DCA placed {symbol} {side} price={price} qty={qty}")

    except Exception as e:
        log("ERROR", f"DCA failed: {e}")

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
    log("INFO", f"MSG RECEIVED chat={message.chat.id} text={bool(message.text)} photo={bool(message.photo)}")

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
    load_sltp()
    await app.start()

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

        pnl_ready = False
        if PNL_CHAT_ID:
            ok3 = await ensure_peer_known(PNL_CHAT_ID)
            if ok3:
                pnl_ready = True
                log("INFO", f"PNL chat ready. pnl_chat_id={PNL_CHAT_ID}")
            else:
                log("ERROR", f"PNL chat FAILED. pnl_chat_id={PNL_CHAT_ID}")

        try:
            await ensure_markets_loaded()
            log("INFO", "BINGX markets loaded")
        except Exception as e:
            log("ERROR", f"BINGX load_markets failed: {e}")

        if pnl_ready:
            asyncio.create_task(
            pnl_watcher(
                app,
                exchange,
                log,
                PNL_CHAT_ID,
                BINGX_API_KEY,
                BINGX_API_SECRET,
            )
        )

        log("INFO", f"DRY_RUN={DRY_RUN} | Listening TARGET_CHAT_ID={TARGET_CHAT_ID}")
        await idle()

    finally:
        await app.stop()

if __name__ == "__main__":
    app.run(main())