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
ORDER_IDS_FILE = "/data/order_ids.json"
LAST_ORDER_IDS = {}

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


def save_order_ids():
    try:
        with open(ORDER_IDS_FILE, "w") as f:
            json.dump(LAST_ORDER_IDS, f)
    except Exception as e:
        print("ORDER_IDS save error:", e)

def load_order_ids():
    global LAST_ORDER_IDS
    try:
        if os.path.exists(ORDER_IDS_FILE):
            with open(ORDER_IDS_FILE, "r") as f:
                LAST_ORDER_IDS = json.load(f)
    except Exception as e:
        print("ORDER_IDS load error:", e)

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

def get_usdt_total_sync() -> float:
    ensure_markets_loaded_sync()
    for t in ("swap", "future", "futures", "contract"):
        try:
            bal = exchange.fetch_balance({"type": t})
            usdt_total = (bal.get("total") or {}).get("USDT")
            if usdt_total is not None:
                return float(usdt_total)

            usdt_free = (bal.get("free") or {}).get("USDT")
            usdt_used = (bal.get("used") or {}).get("USDT")
            if usdt_free is not None or usdt_used is not None:
                return float(usdt_free or 0.0) + float(usdt_used or 0.0)
        except Exception:
            pass

    bal = exchange.fetch_balance()
    usdt_total = (bal.get("total") or {}).get("USDT")
    if usdt_total is not None:
        return float(usdt_total)

    usdt_free = (bal.get("free") or {}).get("USDT")
    usdt_used = (bal.get("used") or {}).get("USDT")
    return float(usdt_free or 0.0) + float(usdt_used or 0.0)

async def get_usdt_total() -> float:
    return await asyncio.to_thread(get_usdt_total_sync)

def normalize_order_qty_sync(symbol: str, qty_raw: float) -> tuple[float, float | None]:
    ensure_markets_loaded_sync()
    market = exchange.market(symbol)
    min_amount = (((market.get("limits") or {}).get("amount") or {}).get("min"))

    try:
        qty = float(exchange.amount_to_precision(symbol, qty_raw))
    except Exception:
        qty = float(qty_raw)

    min_amount_f = None
    if min_amount is not None:
        try:
            min_amount_f = float(min_amount)
            if qty < min_amount_f:
                qty = float(exchange.amount_to_precision(symbol, min_amount_f))
        except Exception:
            min_amount_f = None

    return qty, min_amount_f

async def normalize_order_qty(symbol: str, qty_raw: float) -> tuple[float, float | None]:
    return await asyncio.to_thread(normalize_order_qty_sync, symbol, qty_raw)

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

        best = None
        best_size = 0.0

        for p in positions:
            side = (
                p.get("side")
                or p.get("positionSide")
                or (p.get("info") or {}).get("positionSide")
                or ""
            ).lower()

            if side in {"long", "short"}:
                size = float(
                    p.get("contracts")
                    or p.get("size")
                    or p.get("positionAmt")
                    or 0
                )

                if size > best_size:
                    best_size = size
                    best = p

        return best if best_size > 0 else None

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

    time.sleep(0.8)

    canceled_total = 0
    for side in closed:
        try:
            canceled_total += cancel_all_open_orders_for_symbol_sync(symbol, side)
        except Exception as e:
            log("WARNING", f"cancel_all_open_orders after close failed for {symbol}/{side}: {e}")

    _clear_symbol_state(base)

    log("INFO", f"CLOSE cleanup done symbol={symbol} canceled_orders={canceled_total}")
    return f"CLOSED {'/'.join(closed)} | canceled={canceled_total}"


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

    log("DEBUG", f"RAW HTTP {method.upper()} {path} payload={payload}")
    resp = requests.request(method.upper(), url, headers=headers, timeout=5)
    log("DEBUG", f"RAW HTTP DONE status={resp.status_code} path={path}")
    body_text = resp.text
    log("DEBUG", f"RAW HTTP RESPONSE TEXT: {body_text[:1000]}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"BingX RAW {method} {path} bad json: {body_text[:500]}")

    log("DEBUG", f"RAW HTTP RESPONSE JSON: {data}")

    code = str(data.get("code", ""))
    success = data.get("success")

    if resp.status_code >= 400:
        raise RuntimeError(
            f"BingX RAW {method} {path} HTTP_FAIL status={resp.status_code} code={data.get('code')} msg={data.get('msg') or data.get('message')} body={body_text[:500]}"
        )

    if success not in (None, True, "true", "True", 1, "1"):
        raise RuntimeError(
            f"BingX RAW {method} {path} REJECTED success={success} code={data.get('code')} msg={data.get('msg') or data.get('message')} body={body_text[:500]}"
        )

    if code not in {"0", "", "None"}:
        raise RuntimeError(
            f"BingX RAW {method} {path} REJECTED code={data.get('code')} msg={data.get('msg') or data.get('message')} body={body_text[:500]}"
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
    }

    return _bingx_raw_request_sync("POST", "/openApi/swap/v2/trade/order", payload)

def _extract_bingx_order_id(data: dict):
    try:
        return (
            data.get("data", {}).get("order", {}).get("orderId")
            or data.get("data", {}).get("order", {}).get("orderID")
            or data.get("data", {}).get("orderId")
            or data.get("data", {}).get("id")
        )
    except Exception:
        return None

def _state_key(base: str, pos_side: str) -> str:
    return f"{str(base).upper()}:{str(pos_side).lower()}"

def _extract_position_side_sync(pos: dict) -> str:
    return str(
        pos.get("side")
        or pos.get("positionSide")
        or (pos.get("info") or {}).get("positionSide")
        or ""
    ).lower()

def _extract_position_qty_sync(pos: dict, symbol: str) -> float:
    qty = float(abs(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    ))
    return float(exchange.amount_to_precision(symbol, qty))

def _load_saved_sltp_for_position(base: str, pos_side: str) -> dict:
    return ((LAST_SLTP.get(str(base).upper()) or {}).get(pos_side) or {}).copy()

def _store_sltp_state(base: str, pos_side: str, sl=None, tp=None, sl_id=None, tp_id=None):
    base_u = str(base).upper()
    key = _state_key(base_u, pos_side)

    LAST_SLTP.setdefault(base_u, {})
    LAST_SLTP[base_u].setdefault(pos_side, {"sl": None, "tp": None})

    if sl is not None:
        LAST_SLTP[base_u][pos_side]["sl"] = sl
    if tp is not None:
        LAST_SLTP[base_u][pos_side]["tp"] = tp

    LAST_ORDER_IDS.setdefault(key, {})
    if sl_id is not None:
        LAST_ORDER_IDS[key]["sl_id"] = sl_id
    if tp_id is not None:
        LAST_ORDER_IDS[key]["tp_id"] = tp_id

    save_sltp()
    save_order_ids()

def _clear_symbol_state(base: str):
    base_u = str(base).upper()

    if base_u in LAST_SLTP:
        del LAST_SLTP[base_u]
        save_sltp()

    for k in list(LAST_ORDER_IDS.keys()):
        if k.startswith(f"{base_u}:"):
            del LAST_ORDER_IDS[k]
    save_order_ids()

def cancel_order_exact_sync(symbol: str, order_id: str) -> bool:
    if not order_id:
        return False
    try:
        exchange.cancel_order(str(order_id), symbol)
        log("INFO", f"CANCEL EXACT OK symbol={symbol} id={order_id}")
        return True
    except Exception as e:
        log("WARNING", f"CANCEL EXACT FAILED symbol={symbol} id={order_id} err={e}")
        return False

async def cancel_order_exact(symbol: str, order_id: str) -> bool:
    return await asyncio.to_thread(cancel_order_exact_sync, symbol, order_id)

def cancel_all_open_orders_for_symbol_sync(symbol: str, pos_side: Optional[str] = None) -> int:
    canceled = 0

    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception as e:
        log("WARNING", f"fetch_open_orders failed for {symbol}: {e}")
        return 0

    for o in orders:
        try:
            info = o.get("info") or {}
            oid = o.get("id")
            if not oid:
                continue

            if pos_side:
                o_pos_side = str(info.get("positionSide") or "").lower()
                if o_pos_side and o_pos_side != pos_side.lower():
                    continue

            exchange.cancel_order(str(oid), symbol)
            canceled += 1
            log("INFO", f"CANCEL OPEN ORDER symbol={symbol} id={oid} type={o.get('type')} side={o.get('side')}")
        except Exception as e:
            log("WARNING", f"CANCEL OPEN ORDER failed symbol={symbol} id={o.get('id')}: {e}")

    return canceled

async def cancel_all_open_orders_for_symbol(symbol: str, pos_side: Optional[str] = None) -> int:
    return await asyncio.to_thread(cancel_all_open_orders_for_symbol_sync, symbol, pos_side)

def _cancel_known_order_ids_sync(symbol: str, key: str):
    saved = LAST_ORDER_IDS.get(key) or {}

    sl_id = saved.get("sl_id")
    tp_id = saved.get("tp_id")

    if sl_id:
        cancel_order_exact_sync(symbol, sl_id)
    if tp_id:
        cancel_order_exact_sync(symbol, tp_id)

    LAST_ORDER_IDS[key] = {}
    save_order_ids()

def _cancel_existing_sltp_sync(symbol: str, pos_side: str):
    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception as e:
        log("WARNING", f"fetch_open_orders failed for cancel_existing_sltp: {e}")
        return

    for o in orders:
        try:
            info = o.get("info") or {}
            o_pos_side = str(info.get("positionSide") or "").lower()
            if o_pos_side and o_pos_side != pos_side.lower():
                continue

            if is_sl_order(o) or is_tp_order(o):
                oid = o.get("id")
                if oid:
                    try:
                        exchange.cancel_order(str(oid), symbol)
                        log("INFO", f"CANCEL existing SLTP symbol={symbol} pos_side={pos_side} id={oid} type={o.get('type')}")
                    except Exception as e:
                        log("WARNING", f"CANCEL existing failed symbol={symbol} id={oid}: {e}")
        except Exception:
            continue

def apply_sltp_sync(base: str, *, sl_price=None, tp_price=None, cancel_first: bool = True) -> str:
    base = str(base).upper().strip()

    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    if pos_side not in {"long", "short"}:
        return "NO_POSITION"

    qty = _extract_position_qty_sync(pos, symbol)
    if qty <= 0:
        return "NO_POSITION"

    key = _state_key(base, pos_side)

    if cancel_first:
        _cancel_known_order_ids_sync(symbol, key)
        _cancel_existing_sltp_sync(symbol, pos_side)
        time.sleep(0.25)

    result_parts = []

    if sl_price is not None:
        try:
            sl_prec = float(exchange.price_to_precision(symbol, float(sl_price)))
        except Exception:
            sl_prec = float(sl_price)

        sl_resp = _place_bingx_tpsl_raw_sync(symbol, pos_side, sl_prec, qty, "sl")
        sl_id = _extract_bingx_order_id(sl_resp)
        _store_sltp_state(base, pos_side, sl=sl_prec, sl_id=sl_id)
        result_parts.append(f"SL={sl_prec}")
        log("INFO", f"SL APPLIED symbol={symbol} pos_side={pos_side} qty={qty} sl={sl_prec} id={sl_id}")

    if tp_price is not None:
        try:
            tp_prec = float(exchange.price_to_precision(symbol, float(tp_price)))
        except Exception:
            tp_prec = float(tp_price)

        tp_resp = _place_bingx_tpsl_raw_sync(symbol, pos_side, tp_prec, qty, "tp")
        tp_id = _extract_bingx_order_id(tp_resp)
        _store_sltp_state(base, pos_side, tp=tp_prec, tp_id=tp_id)
        result_parts.append(f"TP={tp_prec}")
        log("INFO", f"TP APPLIED symbol={symbol} pos_side={pos_side} qty={qty} tp={tp_prec} id={tp_id}")

    if not result_parts:
        return "NOTHING_TO_APPLY"

    return " | ".join(result_parts)

async def apply_sltp(base: str, *, sl_price=None, tp_price=None, cancel_first: bool = True) -> str:
    return await asyncio.to_thread(
        apply_sltp_sync,
        base,
        sl_price=sl_price,
        tp_price=tp_price,
        cancel_first=cancel_first,
    )

def reapply_saved_sltp_sync(base: str) -> str:
    base = str(base).upper().strip()

    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    if pos_side not in {"long", "short"}:
        return "NO_POSITION"

    saved = _load_saved_sltp_for_position(base, pos_side)
    sl = saved.get("sl")
    tp = saved.get("tp")

    if sl is None and tp is None:
        return "NO_SAVED_SLTP"

    return apply_sltp_sync(base, sl_price=sl, tp_price=tp, cancel_first=True)

async def reapply_saved_sltp(base: str) -> str:
    return await asyncio.to_thread(reapply_saved_sltp_sync, base)

def set_sl_oneway_sync(base: str, sl_price: float) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    saved = _load_saved_sltp_for_position(str(base).upper(), pos_side)
    tp_saved = saved.get("tp")

    return apply_sltp_sync(base, sl_price=sl_price, tp_price=tp_saved, cancel_first=True)

async def set_sl_oneway(base: str, sl_price: float) -> str:
    return await asyncio.to_thread(set_sl_oneway_sync, base, sl_price)

def set_tp_oneway_sync(base: str, tp_price: float) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    saved = _load_saved_sltp_for_position(str(base).upper(), pos_side)
    sl_saved = saved.get("sl")

    return apply_sltp_sync(base, sl_price=sl_saved, tp_price=tp_price, cancel_first=True)

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

    pos_side = _extract_position_side_sync(pos)
    saved = _load_saved_sltp_for_position(str(base).upper(), pos_side)
    tp_saved = saved.get("tp")

    return apply_sltp_sync(base, sl_price=float(entry), tp_price=tp_saved, cancel_first=True)

async def breakeven_oneway(base: str) -> str:
    return await asyncio.to_thread(breakeven_oneway_sync, base)

def add_position_oneway_sync(base: str, add_pct: Optional[float]) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    if pos_side not in {"long", "short"}:
        return "NO_POSITION"

    contracts = _extract_position_qty_sync(pos, symbol)
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


async def wait_position_update(symbol: str, old_size: float = 0.0, timeout: float = 5.0, min_target_size: Optional[float] = None):
    start = time.time()
    last_pos = None

    while time.time() - start < timeout:
        pos = await fetch_position_oneway(symbol)
        if pos:
            size = float(
                pos.get("contracts")
                or pos.get("size")
                or pos.get("positionAmt")
                or 0
            )
            last_pos = pos

            if min_target_size is not None:
                if size >= min_target_size:
                    return pos
            elif abs(size - old_size) > 1e-12:
                return pos

        await asyncio.sleep(0.25)

    return last_pos


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
    r"\bclose\s+now\b",
    r"\bclose\s+all\b",
    r"\bfully\s+close\b",
    r"\bexit\s+now\b",
    r"\bclosed\b",
    r"\bclosing\s+now\b",
    r"\btp\s*hit\b",
    r"\btp\d+\s*hit\b",
    r"\btake\s+profit\s+hit\b",
    r"\btake\s+profits?\s+hit\b",
    r"\bbook(?:ing)?\s+profit\b",
    r"\bsecure(?:d|ing)?\s+profit\b",
    r"\bclose\s+these\b",
    r"\btp\s+these\b",
]

CLOSE_NEGATIVE_PATTERNS = [
    r"\bexit\s+point\b",
    r"\bthis\s+will\s+be\s+my\s+exit\b",
    r"\bwill\s+be\s+my\s+exit\b",
    r"\bin\s+coming\s+days\b",
    r"\blikely\s+to\b",
    r"\bif\s+.+\s+won[’']?t\s+hold\b",
    r"\bsupport\s+level\b",
    r"\bresistance\s+level\b",
    r"\bmarket\s+ranges?\b",
    r"\btarget\s+area\b",
    r"\bvaluable\s+point\b",
    r"\bwe\s+hold\b",
    r"\bswing\s+shorts?\b",
]

def has_close_intent(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    if any(re.search(p, t, re.I | re.S) for p in CLOSE_NEGATIVE_PATTERNS):
        return False
    return any(re.search(p, t, re.I) for p in CLOSE_INTENT_PATTERNS)


# =========================
# LOCAL SET_SL PARSER (без AI)
# =========================
SET_SL_BLOCK_WORDS = [
    r"\bbe\b", r"\bbreak\s*even\b", r"\bbreakeven\b",
    r"\badd\b", r"\baverag", r"\bdca\b", r"\bscale\s*in\b",
]
TOKEN_ALIASES = {"SOLANA": "SOL", "XBT": "BTC"}
BAD_BASE_WORDS = {
    "ALTCOINS", "ALTCOIN", "ALTS", "SHORTS", "LONGS", "SWINGS",
    "POSITIONS", "POSITION", "COINS", "MARKET", "FUTURES", "USDT",
    "TP", "SL", "ENTRY", "EXIT",
}

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
    if b in BAD_BASE_WORDS or len(b) > 12:
        return None
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
- Use action = CLOSE ONLY for direct execution commands such as: "close now", "close all", "fully close", "tp hit", "take profit hit", "exit now".
- Do NOT use CLOSE for market commentary or future plans such as: "exit point", "this will be my exit", "in coming days", "if support won't hold", "we hold swing shorts".
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
- DCA_PRICE: extract only when the ADD message explicitly means a pending limit add, not informational fields
- PRICE: extract only when the message explicitly provides a pending add/limit price
- Ignore informational fields such as "new open", "new entry", "average entry", "avg entry", "current entry" for ADD
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
- Do not treat generic words like altcoins, shorts, market, positions as ticker bases

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
    b = re.sub(r"[^A-Z0-9]", "", b)
    b = b.replace("USDT", "")
    b = TOKEN_ALIASES.get(b, b)
    if not b:
        return ""
    if b in BAD_BASE_WORDS:
        return ""
    if len(b) > 12:
        return ""
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

    if action in {"NONE", "OPEN"} and _has_add_intent_text(tg_text):
        if base and (add_pct is not None or risk_pct is not None):
            log("WARNING", "FORCE FIX: OPEN/NONE -> ADD by text rule")
            action = "ADD"

    if action == "NONE":
        log("DEBUG", "AI SKIP: action=NONE")
        return

    min_conf = ACTION_MIN_CONF.get(action, 0.70)

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

    if action == "CLOSE":
        if not has_close_intent(tg_text):
            log("INFO", "SAFE SKIP CLOSE: informational/analysis text, no direct close command")
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
            log("INFO", f"AI SKIP CLOSE: no valid bases after cleanup raw={bases if isinstance(bases, list) and bases else ([base] if base else [])}")
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
                log("INFO", f"SUCCESS CLOSE {b}: {res}")
            except Exception as e:
                log("ERROR", f"CLOSE {b} failed: {e}")
        return

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
            usdt_total = await get_usdt_total()
            qty_raw = calc_qty(usdt_total, float(risk_pct), int(lev), entry)
            qty, min_amount = await normalize_order_qty(symbol, qty_raw)
            log("INFO", f"QTY USDT total={usdt_total} qty_raw≈{qty_raw} qty_prec={qty} min_amount={min_amount}")
            if min_amount is not None and qty_raw < min_amount:
                log("WARNING", f"QTY raised to exchange minimum for {symbol}: raw={qty_raw} -> min={min_amount}")
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
            old_pos = await fetch_position_oneway(symbol)
            old_size = float(
                old_pos.get("contracts")
                or old_pos.get("size")
                or old_pos.get("positionAmt")
                or 0
            ) if old_pos else 0.0

            resp = await open_market(symbol, side, qty)
            log("INFO", f"SUCCESS OPEN placed id={resp.get('id')} {base_clean} side={side} qty={qty}")

            pos_seen = await wait_position_update(
                symbol,
                old_size=old_size,
                timeout=6.0,
                min_target_size=old_size + qty * 0.7,
            )
            log("INFO", f"POSITION_VISIBLE_AFTER_OPEN {base_clean}={bool(pos_seen)}")
            await asyncio.sleep(0.7)

            LAST_SLTP.setdefault(base_clean, {})
            LAST_SLTP[base_clean][side] = {"sl": sl_prec, "tp": tp_prec}
            save_sltp()

            res = await apply_sltp(base_clean, sl_price=sl_prec, tp_price=tp_prec, cancel_first=True)
            log("INFO", f"APPLY SL/TP after OPEN done: {res}")

            dca_price = cmd.get("dca_price")
            dca_pct = cmd.get("dca_pct")
            if dca_price and dca_pct:
                await place_dca(symbol, side, float(dca_pct), float(dca_price), lev)

        except Exception as e:
            log("ERROR", f"OPEN FAILED: {e}")
            return
        return

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

        side = (pos.get("side") or (pos.get("info") or {}).get("positionSide") or "").lower()
        lev = int(float(pos.get("leverage") or (pos.get("info") or {}).get("leverage") or 1))

        pct = add_pct if add_pct is not None else risk_pct
        if pct is None:
            log("INFO", "ADD skip: no pct")
            return

        cmd = sanitize_add_prices(cmd, tg_text)
        mode = detect_add_mode(cmd, tg_text)
        log("INFO", f"ADD MODE = {mode}")

        if mode == "DCA":
            dca_price = cmd.get("price") or cmd.get("dca_price")
            if not dca_price:
                log("ERROR", "DCA but no price")
                return

            dca_price = float(dca_price)

            try:
                if DRY_RUN:
                    usdt_total = await get_usdt_total()
                    entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
                    margin = usdt_total * (float(pct) / 100.0)
                    notional = margin * lev
                    qty_raw = notional / entry
                    qty, _ = await normalize_order_qty(symbol, qty_raw)
                    log("INFO", f"DRY_RUN DCA {base_clean} at {dca_price} qty={qty}")
                    return

                dca_res = await place_dca(symbol, side, float(pct), dca_price, lev)
                if dca_res.get("ok"):
                    log("INFO", f"DCA placed {base_clean} at {dca_res['price']} qty={dca_res['qty']}")
                else:
                    log("INFO", f"DCA not placed {base_clean}: {dca_res.get('reason')}")
            except Exception as e:
                log("ERROR", f"DCA failed: {e}")
            return

        try:
            usdt_total = await get_usdt_total()
            pct = float(pct)
            margin = usdt_total * (pct / 100.0)
            notional = margin * lev

            entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
            qty_raw = notional / entry
            qty, min_amount = await normalize_order_qty(symbol, qty_raw)
            if min_amount is not None and qty_raw < min_amount:
                log("WARNING", f"ADD qty raised to exchange minimum for {symbol}: raw={qty_raw} -> min={min_amount}")

            if DRY_RUN:
                log("INFO", f"DRY_RUN ADD {base_clean} qty={qty} (~{pct}% balance)")
                return

            old_pos = await fetch_position_oneway(symbol)
            old_size = float(
                old_pos.get("contracts")
                or old_pos.get("size")
                or old_pos.get("positionAmt")
                or 0
            ) if old_pos else 0.0

            resp = await open_market(symbol, side, qty)
            log("INFO", f"MARKET ADD {base_clean} qty={qty} (~{pct}% balance)")
            log("INFO", f"ADD RESPONSE: {resp}")

            pos_seen = await wait_position_update(
                symbol,
                old_size=old_size,
                timeout=6.0,
                min_target_size=old_size + qty * 0.7,
            )
            log("INFO", f"POSITION_VISIBLE_AFTER_ADD {base_clean}={bool(pos_seen)}")
            await asyncio.sleep(0.7)

            reapply_res = await reapply_saved_sltp(base_clean)
            log("INFO", f"REAPPLY after ADD done: {reapply_res}")
            return
        except Exception as e:
            log("ERROR", f"ADD failed: {e}")
            return

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
        pos_side = (
            pos.get("side")
            or pos.get("positionSide")
            or (pos.get("info") or {}).get("positionSide")
            or ""
        ).lower() if pos else None

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
            log("INFO", f"SUCCESS SET_SL {base_clean}: {res}")
        except Exception as e:
            log("ERROR", f"SET_SL failed: {e}")
        return

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
        pos_side = (
            pos.get("side")
            or pos.get("positionSide")
            or (pos.get("info") or {}).get("positionSide")
            or ""
        ).lower() if pos else None

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
            log("INFO", f"BE {base_clean}: move SL to entry and keep TP")
            res = await breakeven_oneway(base_clean)
            log("INFO", f"SUCCESS BE {base_clean}: {res}")
        except Exception as e:
            log("ERROR", f"BE failed: {e}")
        return

    log("INFO", f"Unknown/unsupported action: {action}")

def detect_add_mode(cmd: dict, tg_text: str = "") -> str:
    """
    return:
    - "DCA"     -> only for explicit limit/pending add intent
    - "MARKET"  -> default add by current market price
    """
    t = (tg_text or "").strip().lower()

    if re.search(r"\b(dca at|buy limit|sell limit|pending|set buy|set sell|limit add|limit dca)\b", t, re.I):
        return "DCA"

    return "MARKET"


def sanitize_add_prices(cmd: dict, tg_text: str) -> dict:
    """
    'New Open', 'New Entry', 'Avg Entry' etc are informational only.
    They must not turn a normal ADD into a DCA.
    """
    t = (tg_text or "").lower()

    has_info_entry_text = re.search(
        r"\b(new open|new entry|avg entry|average entry|current entry)\b",
        t,
        re.I,
    )

    has_real_dca_intent = re.search(
        r"\b(dca at|buy limit|sell limit|pending|set buy|set sell|limit add|limit dca)\b",
        t,
        re.I,
    )

    if has_info_entry_text and not has_real_dca_intent:
        cmd["price"] = None
        cmd["dca_price"] = None

    return cmd


async def place_dca(symbol, side, pct, price, lev):
    try:
        market_last = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
        log("INFO", f"DCA CHECK {symbol} side={side} market_last={market_last} order_price={price}")

        usdt_total = await get_usdt_total()
        margin = usdt_total * (pct / 100)
        notional = margin * lev
        qty_raw = notional / market_last

        qty, min_amount = await normalize_order_qty(symbol, qty_raw)
        if min_amount is not None and qty_raw < min_amount:
            log("WARNING", f"DCA qty raised to exchange minimum for {symbol}: raw={qty_raw} -> min={min_amount}")

        orders = await asyncio.to_thread(exchange.fetch_open_orders, symbol)
        for o in orders:
            o_price = float(o.get("price") or 0)
            if float(price) > 0 and o_price > 0 and abs(o_price - float(price)) / float(price) < 0.001:
                log("INFO", "DCA already exists (approx match)")
                return {"ok": False, "reason": "already_exists"}

        resp = await asyncio.to_thread(
            place_dca_order_sync,
            symbol,
            side,
            qty,
            float(price)
        )

        log("INFO", f"DCA placed {symbol} {side} price={price} qty={qty}")
        return {"ok": True, "qty": qty, "price": float(price), "order_id": resp.get("id")}

    except Exception as e:
        log("ERROR", f"DCA failed: {e}")
        return {"ok": False, "reason": str(e)}

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
    load_order_ids()
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