import asyncio
import time
from typing import Any, Dict, List, Optional

# =========================
# GLOBAL STATE
# =========================
last_checked = 0  # latest processed trade timestamp (ms)
weekly_pnl = 0.0
week_start = time.time()

# store already-sent close events so we don't resend them
SENT_EVENT_KEYS = set()
MAX_SENT_EVENT_KEYS = 5000


# =========================
# HELPERS
# =========================
def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _extract_pnl(trade: dict) -> float:
    info = trade.get("info") or {}

    for candidate in (
        trade.get("realizedPnl"),
        trade.get("closedPnl"),
        trade.get("profit"),
        info.get("realizedPnl"),
        info.get("closedPnl"),
        info.get("profit"),
        info.get("profitValue"),
    ):
        pnl = _to_float(candidate, 0.0)
        if pnl != 0:
            return pnl

    return 0.0


def _is_close_trade(trade: dict) -> bool:
    info = trade.get("info") or {}

    reduce_only = trade.get("reduceOnly")
    if reduce_only is None:
        reduce_only = info.get("reduceOnly")

    if isinstance(reduce_only, str):
        reduce_only = reduce_only.lower() in {"true", "1", "yes"}

    if reduce_only:
        return True

    # fallback: many exchanges expose non-zero realized pnl only on closing fills
    return _extract_pnl(trade) != 0


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").replace(":", "_").replace("/", "_")


def _event_key(trade: dict) -> str:
    info = trade.get("info") or {}
    symbol = _normalize_symbol(trade.get("symbol", ""))
    side = str(trade.get("side") or info.get("side") or "").lower()
    pos_side = str(
        trade.get("positionSide")
        or info.get("positionSide")
        or info.get("posSide")
        or ""
    ).lower()

    order_id = (
        trade.get("order")
        or trade.get("orderId")
        or info.get("orderId")
        or info.get("clientOrderId")
        or info.get("clientOid")
    )
    if order_id:
        return f"{symbol}|{order_id}|{side}|{pos_side}"

    # fallback if exchange doesn't provide order id
    ts_bucket = int(trade.get("timestamp") or 0) // 5000
    return f"{symbol}|{ts_bucket}|{side}|{pos_side}"


def _format_close_message(event: dict) -> str:
    pnl = event["pnl"]
    qty = event["qty"]
    symbol = event["symbol"]
    side = (event.get("side") or "").upper()
    status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

    lines = [
        f"{status} #{symbol}",
        f"PnL: {round(pnl, 4)} USDT",
        f"Qty: {round(qty, 4)}",
    ]

    if side:
        lines.insert(1, f"Close side: {side}")

    if event.get("fills", 1) > 1:
        lines.append(f"Fills: {event['fills']}")

    return "\n".join(lines)


async def _fetch_all_recent_trades(exchange, since_ms: int) -> List[dict]:
    """
    Quiet fetcher:
    - tries global trade history first
    - if not supported, falls back to symbols from positions/open orders
    - no warnings in normal polling loop
    """
    try:
        trades = await asyncio.to_thread(exchange.fetch_my_trades, None, since_ms or None, 100)
        if trades:
            return trades
    except Exception:
        pass

    symbols: List[str] = []

    try:
        positions = await asyncio.to_thread(exchange.fetch_positions)
        for pos in positions or []:
            symbol = pos.get("symbol")
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    except Exception:
        pass

    try:
        open_orders = await asyncio.to_thread(exchange.fetch_open_orders)
        for order in open_orders or []:
            symbol = order.get("symbol")
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    except Exception:
        pass

    if not symbols:
        return []

    all_trades: List[dict] = []
    for symbol in symbols[:15]:
        try:
            part = await asyncio.to_thread(exchange.fetch_my_trades, symbol, since_ms or None, 50)
            if part:
                all_trades.extend(part)
        except Exception:
            pass

    return all_trades


# =========================
# PUBLIC WATCHER
# =========================
async def pnl_watcher(app, exchange, log, log_chat_id, interval: int = 5):
    """
    Sends ONE message for the latest closed trade event.

    Rules:
    - no warning spam during normal polling
    - only new closing trades are processed
    - multiple fills of the same close are merged into one event
    - only the latest close event is sent per cycle
    - already sent events are deduplicated
    """
    global last_checked, weekly_pnl, week_start, SENT_EVENT_KEYS

    while True:
        try:
            trades = await _fetch_all_recent_trades(exchange, last_checked)
            if not trades:
                await asyncio.sleep(interval)
                continue

            trades.sort(key=lambda t: int(t.get("timestamp") or 0), reverse=True)

            latest_seen_ts = last_checked
            close_events: Dict[str, dict] = {}

            for trade in trades:
                ts = int(trade.get("timestamp") or 0)
                if ts > latest_seen_ts:
                    latest_seen_ts = ts

                if ts <= last_checked:
                    continue

                if not _is_close_trade(trade):
                    continue

                pnl = _extract_pnl(trade)
                if pnl == 0:
                    continue

                key = _event_key(trade)
                if key in SENT_EVENT_KEYS:
                    continue

                event = close_events.get(key)
                if not event:
                    close_events[key] = {
                        "key": key,
                        "symbol": str(trade.get("symbol") or ""),
                        "side": str(trade.get("side") or (trade.get("info") or {}).get("side") or ""),
                        "pnl": pnl,
                        "qty": abs(_to_float(trade.get("amount"), 0.0)),
                        "fills": 1,
                        "ts": ts,
                    }
                else:
                    event["pnl"] += pnl
                    event["qty"] += abs(_to_float(trade.get("amount"), 0.0))
                    event["fills"] += 1
                    if ts > event["ts"]:
                        event["ts"] = ts

            if close_events:
                latest_event = max(close_events.values(), key=lambda item: item["ts"])
                msg = _format_close_message(latest_event)
                await app.send_message(log_chat_id, msg)

                SENT_EVENT_KEYS.add(latest_event["key"])
                if len(SENT_EVENT_KEYS) > MAX_SENT_EVENT_KEYS:
                    SENT_EVENT_KEYS = set(list(SENT_EVENT_KEYS)[-2000:])

                weekly_pnl += latest_event["pnl"]
                log(
                    "INFO",
                    f"PNL notifier sent: {latest_event['symbol']} pnl={latest_event['pnl']} fills={latest_event['fills']}",
                )

            last_checked = latest_seen_ts

            if time.time() - week_start >= 7 * 24 * 60 * 60:
                status = "🟢 PROFIT" if weekly_pnl > 0 else "🔴 LOSS"
                report = (
                    "📊 WEEKLY REPORT\n\n"
                    f"{status}\n"
                    f"Total PnL: {round(weekly_pnl, 4)} USDT"
                )
                await app.send_message(log_chat_id, report)
                weekly_pnl = 0.0
                week_start = time.time()

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)
