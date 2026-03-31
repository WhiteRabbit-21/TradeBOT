import asyncio
import time
from typing import Any, Dict, List, Optional

# =========================
# GLOBAL STATE
# =========================
last_checked = 0  # ms timestamp of latest processed trade
weekly_pnl = 0.0
week_start = time.time()

# dedupe already notified close events
SENT_CLOSE_KEYS = set()
MAX_SENT_KEYS = 5000


# =========================
# HELPERS
# =========================

def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _norm_symbol(symbol: str) -> str:
    s = str(symbol or "")
    return s.replace(":", "_").replace("/", "_")


def _extract_pnl(trade: dict) -> float:
    info = trade.get("info") or {}
    candidates = [
        trade.get("realizedPnl"),
        trade.get("closedPnl"),
        trade.get("profit"),
        info.get("realizedPnl"),
        info.get("closedPnl"),
        info.get("profit"),
        info.get("profitValue"),
    ]
    for value in candidates:
        pnl = _to_float(value, default=0.0)
        if pnl != 0:
            return pnl
    return 0.0


def _is_reduce_only_close(trade: dict) -> bool:
    info = trade.get("info") or {}

    ro = trade.get("reduceOnly")
    if ro is None:
        ro = info.get("reduceOnly")
    if isinstance(ro, str):
        ro = ro.lower() in {"true", "1", "yes"}

    if ro:
        return True

    # fallback: if exchange does not expose reduceOnly reliably,
    # realized pnl on a user trade is a strong signal of closing.
    pnl = _extract_pnl(trade)
    return pnl != 0


def _close_group_key(trade: dict) -> str:
    info = trade.get("info") or {}
    symbol = _norm_symbol(trade.get("symbol", ""))
    order_id = (
        trade.get("order")
        or trade.get("orderId")
        or info.get("orderId")
        or info.get("clientOrderId")
        or info.get("clientOid")
    )
    side = str(trade.get("side") or info.get("side") or "").lower()
    pos_side = str(
        trade.get("positionSide")
        or info.get("positionSide")
        or info.get("posSide")
        or ""
    ).lower()
    ts = int(trade.get("timestamp") or 0)

    if order_id:
        return f"{symbol}|{order_id}|{side}|{pos_side}"

    # fallback when order id is absent: group nearby close fills together
    bucket = ts // 5000  # 5 sec bucket
    return f"{symbol}|{bucket}|{side}|{pos_side}"


def _format_msg(event: dict) -> str:
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
    if event.get("fills", 0) > 1:
        lines.append(f"Fills: {event['fills']}")
    return "\n".join(lines)


async def _fetch_recent_trades(exchange, log, since_ms: int) -> List[dict]:
    """Try one global fetch first, fallback to a small recent symbol set."""
    try:
        trades = await asyncio.to_thread(exchange.fetch_my_trades, None, since_ms or None, 100)
        if trades:
            return trades
    except Exception as e:
        log("WARNING", f"fetch_my_trades(all) failed: {e}")

    symbols = []

    # fallback to currently relevant symbols only
    try:
        positions = await asyncio.to_thread(exchange.fetch_positions)
        for p in positions or []:
            symbol = p.get("symbol")
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    except Exception as e:
        log("WARNING", f"fetch_positions fallback failed: {e}")

    try:
        orders = await asyncio.to_thread(exchange.fetch_open_orders)
        for o in orders or []:
            symbol = o.get("symbol")
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    except Exception:
        pass

    # tiny safety fallback
    if not symbols:
        symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    all_trades: List[dict] = []
    for symbol in symbols[:10]:
        try:
            part = await asyncio.to_thread(exchange.fetch_my_trades, symbol, since_ms or None, 50)
            if part:
                all_trades.extend(part)
        except Exception as e:
            log("WARNING", f"fetch_my_trades({symbol}) failed: {e}")

    return all_trades


async def pnl_watcher(app, exchange, log, log_chat_id, interval=5):
    global last_checked, weekly_pnl, week_start, SENT_CLOSE_KEYS

    while True:
        try:
            trades = await _fetch_recent_trades(exchange, log, last_checked)
            if not trades:
                await asyncio.sleep(interval)
                continue

            # newest first
            trades = sorted(trades, key=lambda t: int(t.get("timestamp") or 0), reverse=True)

            close_events: Dict[str, dict] = {}
            max_ts = last_checked

            for t in trades:
                ts = int(t.get("timestamp") or 0)
                if ts > max_ts:
                    max_ts = ts

                if ts <= last_checked:
                    continue

                if not _is_reduce_only_close(t):
                    continue

                pnl = _extract_pnl(t)
                if pnl == 0:
                    continue

                key = _close_group_key(t)
                if key in SENT_CLOSE_KEYS:
                    continue

                symbol = str(t.get("symbol") or "")
                qty = abs(_to_float(t.get("amount"), 0.0))
                side = str(t.get("side") or (t.get("info") or {}).get("side") or "")

                event = close_events.get(key)
                if not event:
                    close_events[key] = {
                        "key": key,
                        "symbol": symbol,
                        "pnl": pnl,
                        "qty": qty,
                        "side": side,
                        "fills": 1,
                        "ts": ts,
                    }
                else:
                    event["pnl"] += pnl
                    event["qty"] += qty
                    event["fills"] += 1
                    if ts > event["ts"]:
                        event["ts"] = ts

            if close_events:
                # take only the latest fully grouped close event per cycle
                latest_event = max(close_events.values(), key=lambda x: x["ts"])
                msg = _format_msg(latest_event)
                await app.send_message(log_chat_id, msg)

                SENT_CLOSE_KEYS.add(latest_event["key"])
                if len(SENT_CLOSE_KEYS) > MAX_SENT_KEYS:
                    SENT_CLOSE_KEYS = set(list(SENT_CLOSE_KEYS)[-2000:])

                weekly_pnl += latest_event["pnl"]
                log("INFO", f"PNL notifier sent: {latest_event['symbol']} pnl={latest_event['pnl']} key={latest_event['key']}")

            # move watermark after processing
            last_checked = max_ts

            # weekly report
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
