import os
import re
import time
import asyncio
from dataclasses import dataclass
from datetime import datetime

import ccxt
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid, FloodWait, RPCError


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

DRY_RUN = os.getenv("DRY_RUN", "1").strip() == "1"
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))  # 5 хв

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
# TG LOGGER (priority)
# =========================

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_min_level = _LEVELS.get(LOG_LEVEL, 20)
_log_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()  # (level, line)


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _send_to_tg(text: str):
    if LOG_CHAT_ID == 0:
        return
    try:
        await app.send_message(LOG_CHAT_ID, text)
    except FloodWait as e:
        await asyncio.sleep(int(e.value) + 1)
        try:
            await app.send_message(LOG_CHAT_ID, text)
        except Exception:
            pass
    except RPCError:
        pass
    except Exception:
        pass


def log(level: str, msg: str):
    lvl_name = level.upper()
    lvl = _LEVELS.get(lvl_name, 20)
    if lvl < _min_level:
        return

    line = f"[{_ts()}] [{lvl_name}] {msg}"

    # ERROR -> immediately
    if lvl_name == "ERROR":
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_send_to_tg(line))
        except Exception:
            pass
        return

    # IMPORTANT WARNING -> immediately
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
# TRADEBOT LOGIC (from Tradebot)
# =========================

@dataclass
class NewSignal:
    side: str          # "short" | "long"
    base: str          # "BTC" | "1000PEPE" ...
    sl: float
    lev: int
    risk_pct: float
    tp_price: float | None = None
    tp_rr: float | None = None
    tp_pct: float | None = None


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


def resolve_symbol_sync(base: str) -> str | None:
    ensure_markets_loaded_sync()
    base = base.upper()
    candidates = [f"{base}/USDT:USDT", f"{base}/USDT"]
    for c in candidates:
        if c in exchange.markets:
            return c
    for sym, m in exchange.markets.items():
        try:
            if (m.get("base", "").upper() == base and m.get("quote", "").upper() == "USDT"):
                if ":USDT" in sym or m.get("swap") or m.get("contract"):
                    return sym
        except Exception:
            continue
    return None


async def resolve_symbol(base: str) -> str | None:
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
        exchange.set_leverage(lev, symbol)
    except Exception as e:
        # не критично
        raise RuntimeError(f"set_leverage failed: {e}")


async def set_leverage(symbol: str, lev: int):
    await asyncio.to_thread(set_leverage_sync, symbol, lev)


def fetch_ticker_last_sync(symbol: str) -> float:
    t = exchange.fetch_ticker(symbol)
    return float(t["last"])


async def fetch_ticker_last(symbol: str) -> float:
    return await asyncio.to_thread(fetch_ticker_last_sync, symbol)


def open_market_sync(symbol: str, side: str, qty: float):
    order_side = "sell" if side == "short" else "buy"
    return exchange.create_order(symbol, "market", order_side, qty)


async def open_market(symbol: str, side: str, qty: float):
    return await asyncio.to_thread(open_market_sync, symbol, side, qty)


def close_position_full_oneway_sync(base: str):
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found for close: {base}")

    pos = None
    try:
        positions = exchange.fetch_positions([symbol])
        if positions:
            pos = positions[0]
    except Exception:
        pos = None

    if not pos:
        return "NO_POSITION"

    contracts = float(pos.get("contracts") or pos.get("contractSize") or pos.get("size") or 0.0)
    side = (pos.get("side") or "").lower()
    if contracts <= 0 or side not in {"long", "short"}:
        return "NO_POSITION"

    close_side = "sell" if side == "long" else "buy"
    params = {"reduceOnly": True}
    exchange.create_order(symbol, "market", close_side, contracts, None, params)
    return "CLOSED"


async def close_position_full_oneway(base: str) -> str:
    return await asyncio.to_thread(close_position_full_oneway_sync, base)


def place_sl_tp_market_oneway_sync(symbol: str, side: str, qty: float, sl: float, tp: float):
    # У тебе в Tradebot це було NotImplemented.
    # Якщо в тебе вже є робоча реалізація SL/TP для BingX — встав сюди.
    raise NotImplementedError("place_sl_tp_market_oneway: add your BingX trigger orders implementation.")


async def place_sl_tp_market_oneway(symbol: str, side: str, qty: float, sl: float, tp: float):
    return await asyncio.to_thread(place_sl_tp_market_oneway_sync, symbol, side, qty, sl, tp)


def validate_sl_tp(side: str, price: float, sl: float, tp: float) -> bool:
    if side == "short":
        return sl > price and tp < price
    return tp > price and sl < price


def calc_tp_from_rr(side: str, entry: float, sl: float, rr: float) -> float:
    risk = abs(entry - sl)
    if risk <= 0:
        return entry
    return entry - rr * risk if side == "short" else entry + rr * risk


def calc_tp_from_pct(side: str, entry: float, pct: float) -> float:
    k = pct / 100.0
    return entry * (1.0 - k) if side == "short" else entry * (1.0 + k)


def calc_qty(usdt_free: float, risk_pct: float, lev: int, entry_price: float) -> float:
    margin = usdt_free * (risk_pct / 100.0)
    notional = margin * lev
    return 0.0 if entry_price <= 0 else notional / entry_price


def parse_new_signal(text: str) -> NewSignal | None:
    if not text:
        return None
    t = text.strip()

    if re.search(r"\bshort\b", t, re.I):
        side = "short"
    elif re.search(r"\blong\b", t, re.I):
        side = "long"
    else:
        return None

    base = None
    m = re.search(r"\b([A-Z0-9]{2,30})USDT\b", t, re.I)
    if m:
        base = m.group(1).upper()

    if base is None:
        m = re.search(r"\b(?:long|short)\s+([A-Z0-9._-]{2,30})\b", t, re.I)
        if m:
            sym = m.group(1).upper()
            if sym not in {"MARKET", "ENTRY", "NOW"}:
                base = sym[:-4] if sym.endswith("USDT") else sym

    if base is None:
        m = re.search(r"#\s*([A-Z0-9._-]{2,30})USDT\b", t, re.I)
        if m:
            base = m.group(1).upper()[:-4]

    if base is None:
        m = re.search(r"\b(?:RE[- ]?SHORT|RE[- ]?OPENING|REOPENING|OPENING)\s+([A-Z0-9._-]{2,30})\b", t, re.I)
        if m:
            sym = m.group(1).upper()
            base = sym[:-4] if sym.endswith("USDT") else sym

    if base is None:
        return None

    m_sl = re.search(r"\bSL\s*[:=;]?\s*([0-9]+(?:\.[0-9]+)?)\b", t, re.I)
    if not m_sl:
        return None
    sl = float(m_sl.group(1))

    m_lev = re.search(
        r"\bLev(?:erage)?\b.*?(?:[xX]\s*([0-9]{1,3})|([0-9]{1,3})\s*[xX])\b",
        t, re.I
    )
    if not m_lev:
        return None
    lev = int(m_lev.group(1) or m_lev.group(2))

    m_risk = re.search(r"\b(?:With|Margin)\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*balance\b", t, re.I)
    if not m_risk:
        m_risk = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\s*%\s*balance\b", t, re.I)
    if not m_risk:
        return None
    risk_pct = float(m_risk.group(1))

    tp_price = None
    tp_rr = None
    tp_pct = None

    m_rr = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*rr\b", t, re.I)
    if m_rr:
        tp_rr = float(m_rr.group(1))

    m_pct = re.search(r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%\b", t, re.I)
    if m_pct:
        tp_pct = float(m_pct.group(1))

    if tp_rr is None and tp_pct is None:
        m_tp = re.search(
            r"\bTarget(?:s)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)(?:\s*-\s*[0-9]+(?:\.[0-9]+)?)?\b",
            t, re.I
        )
        if m_tp:
            tp_price = float(m_tp.group(1))

    if tp_price is None and tp_rr is None and tp_pct is None:
        return None

    return NewSignal(
        side=side, base=base, sl=sl, lev=lev, risk_pct=risk_pct,
        tp_price=tp_price, tp_rr=tp_rr, tp_pct=tp_pct
    )


def parse_close_signal(text: str) -> str | None:
    if not text:
        return None
    t = text.strip()

    close_triggers = [
        r"\btake\s+profit\b",
        r"\btaking\s+small\s+profit\b",
        r"\bfull\s+tp\b",
        r"\bfull\s+target\b",
        r"\btp\s*\d\b",
        r"\btp\b",
        r"\breached\b",
        r"\bachieved\b",
    ]
    if not any(re.search(p, t, re.I) for p in close_triggers):
        return None

    patterns = [
        r"\btake\s+profit\s+([A-Z0-9]{2,30})\b",
        r"\btake\s+profit\s+in\s+([A-Z0-9]{2,30})\b",
        r"\btaking\s+small\s+profit\s+in\s+([A-Z0-9]{2,30})\b",
        r"\bprofit\s+in\s+([A-Z0-9]{2,30})\b",
        r"\btp\s*\d?\s*(?:reached)?\s*([A-Z0-9]{2,30})\b",
        r"\b#\s*([A-Z0-9]{2,30})USDT\b",
        r"\b([A-Z0-9]{2,30})USDT\b",
    ]

    base = None
    for pat in patterns:
        m = re.search(pat, t, re.I)
        if m:
            sym = m.group(1).upper()
            base = sym[:-4] if sym.endswith("USDT") else sym
            break

    if not base:
        return None

    bad = {"TP", "TAKE", "PROFIT", "FULL", "TARGET", "REACHED", "ACHIEVED", "IN", "FOR", "US"}
    if base in bad:
        return None

    return base


# =========================
# SIGNAL EXECUTOR
# =========================

_last_hb = 0.0


async def handle_text_signal(text: str):
    global _last_hb

    now = time.time()
    if now - _last_hb >= HEARTBEAT_SEC:
        _last_hb = now
        log("INFO", "HB alive")

    t = (text or "").strip()
    if not t:
        return

    # 1) NEW
    sig = parse_new_signal(t)
    if sig:
        log("INFO", f"NEW: {sig.side.upper()} {sig.base} SL={sig.sl} Lev={sig.lev} Risk={sig.risk_pct}%")

        symbol = await resolve_symbol(sig.base)
        if not symbol:
            log("ERROR", f"Symbol not found on BingX: {sig.base}/USDT")
            return
        log("INFO", f"SYMBOL: {symbol}")

        try:
            entry = await fetch_ticker_last(symbol)
            log("INFO", f"PRICE last={entry}")
        except Exception as e:
            log("ERROR", f"fetch_ticker failed: {e}")
            return

        # TP
        tp_final = None
        if sig.tp_price is not None:
            tp_final = float(sig.tp_price)
            log("INFO", f"TP_MODE price TP={tp_final}")
        elif sig.tp_rr is not None:
            tp_final = float(calc_tp_from_rr(sig.side, entry, sig.sl, sig.tp_rr))
            log("INFO", f"TP_MODE RR {sig.tp_rr} -> TP {tp_final}")
        elif sig.tp_pct is not None:
            tp_final = float(calc_tp_from_pct(sig.side, entry, sig.tp_pct))
            log("INFO", f"TP_MODE {sig.tp_pct}% -> TP {tp_final}")

        if tp_final is None:
            log("WARNING", "SKIP: TP missing")
            return

        try:
            # precision
            tp_final = float(await asyncio.to_thread(exchange.price_to_precision, symbol, tp_final))
        except Exception as e:
            log("ERROR", f"price_to_precision failed: {e}")
            return

        if not validate_sl_tp(sig.side, entry, sig.sl, tp_final):
            log("WARNING", f"SKIP: bad SL/TP. price={entry} SL={sig.sl} TP={tp_final}")
            return

        # qty
        try:
            usdt_free = await get_usdt_free()
            qty_raw = calc_qty(usdt_free, sig.risk_pct, sig.lev, entry)
            qty = float(await asyncio.to_thread(exchange.amount_to_precision, symbol, qty_raw))
            log("INFO", f"BAL USDT={usdt_free} | QTY raw≈{qty_raw} prec={qty}")
        except Exception as e:
            log("ERROR", f"balance/qty failed: {e}")
            return

        if qty <= 0:
            log("WARNING", "SKIP: qty became 0 after precision")
            return

        if DRY_RUN:
            log("INFO", "DRY_RUN: OPEN not sent")
            return

        try:
            await set_leverage(symbol, sig.lev)
        except Exception as e:
            log("WARNING", f"set_leverage warning: {e}")

        try:
            resp = await open_market(symbol, sig.side, qty)
            log("INFO", f"OPEN OK id={resp.get('id')}")
        except Exception as e:
            log("ERROR", f"OPEN failed: {e}")
            return

        # SL/TP protect (поки заглушка як у Tradebot)
        try:
            sl_order, tp_order = await place_sl_tp_market_oneway(symbol, sig.side, qty, sig.sl, tp_final)
            log("INFO", f"PROTECT SL id={sl_order.get('id')} | TP id={tp_order.get('id')}")
        except NotImplementedError as e:
            log("WARNING", f"{e} (OPEN placed, but SL/TP not set)")
        except Exception as e:
            log("ERROR", f"PROTECT failed: {e}")

        return

    # 2) CLOSE
    base_to_close = parse_close_signal(t)
    if base_to_close:
        log("INFO", f"CLOSE detected: {base_to_close}")

        if DRY_RUN:
            log("INFO", "DRY_RUN: CLOSE not sent")
            return

        try:
            res = await close_position_full_oneway(base_to_close)
            log("INFO", f"CLOSE result: {res} for {base_to_close}")
        except Exception as e:
            log("ERROR", f"CLOSE failed: {e}")
        return

    # 3) SKIP
    log("DEBUG", "SKIP: not NEW or CLOSE format")


# =========================
# HANDLER: read from TARGET_CHAT
# =========================

@app.on_message(filters.chat(TARGET_CHAT_ID) & (filters.text | filters.caption))
async def on_message(_, message):
    text = (message.text or message.caption or "").strip()
    log("INFO", f"📩 msg_id={message.id} text={text[:300]}")
    await handle_text_signal(text)


# =========================
# MAIN (Railway safe)
# =========================

async def main():
    await app.start()
    asyncio.create_task(log_pump())

    try:
        # прогріваємо і target, і log канал
        ok = await ensure_peer_known(TARGET_CHAT_ID)
        while not ok:
            log("WARNING", "⏳ TARGET retry in 60s…")
            await asyncio.sleep(60)
            ok = await ensure_peer_known(TARGET_CHAT_ID)

        if LOG_CHAT_ID != 0:
            ok2 = await ensure_peer_known(LOG_CHAT_ID)
            if ok2:
                await _send_to_tg(f"[{_ts()}] [INFO] 🧾 Telegram logging ON. log_chat_id={LOG_CHAT_ID}")
            else:
                # навіть якщо не прогрівся — не валимо весь бот
                log("ERROR", f"Telegram logging FAILED. log_chat_id={LOG_CHAT_ID}")

        # прогріваємо біржу
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