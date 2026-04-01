import asyncio
import time
import json
from typing import Any, Dict, Optional

# =========================
# GLOBAL STATE
# =========================
LAST_POSITIONS: Dict[str, dict] = {}
SENT_CLOSE_CACHE: Dict[str, int] = {}   # symbol -> last sent ts
CACHE_TTL_SEC = 30

weekly_pnl = 0.0
week_start = time.time()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _extract_pos_side(pos: dict) -> str:
    return str(
        pos.get("side")
        or pos.get("positionSide")
        or (pos.get("info") or {}).get("positionSide")
        or ""
    ).lower()


def _extract_pos_size(pos: dict) -> float:
    return abs(_to_float(
        pos.get("contracts")
        or pos.get("size")
        or pos.get("positionAmt")
        or 0
    ))


def _extract_entry(pos: dict) -> float:
    return _to_float(
        pos.get("entryPrice")
        or pos.get("average")
        or pos.get("avgPrice")
        or 0
    )


def _extract_trade_pnl(trade: dict) -> float:
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


def _extract_trade_qty(trade: dict) -> float:
    return abs(_to_float(trade.get("amount"), 0.0))


def _format_close_message(symbol: str, side: str, pnl: float, qty: float) -> str:
    status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"
    lines = [
        f"{status} #{symbol}",
        f"Close side: {side.upper()}",
        f"PnL: {round(pnl, 4)} USDT",
        f"Qty: {round(qty, 4)}",
    ]
    return "\n".join(lines)


def _should_send(symbol: str) -> bool:
    now = int(time.time())
    last = SENT_CLOSE_CACHE.get(symbol, 0)
    if now - last < CACHE_TTL_SEC:
        return False
    SENT_CLOSE_CACHE[symbol] = now
    return True


async def _fetch_positions_map(exchange) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    try:
        positions = await asyncio.to_thread(exchange.fetch_positions)
    except Exception:
        return result

    for pos in positions or []:
        symbol = pos.get("symbol")
        if not symbol:
            continue

        side = _extract_pos_side(pos)
        size = _extract_pos_size(pos)

        if side in {"long", "short"} and size > 0:
            result[symbol] = {
                "side": side,
                "size": size,
                "entry": _extract_entry(pos),
                "raw": pos,
            }

    return result


async def _get_last_closed_trade_info(exchange, symbol: str) -> Optional[dict]:
    try:
        trades = await asyncio.to_thread(exchange.fetch_my_trades, symbol, None, 20)
    except Exception:
        return None

    if not trades:
        return None

    trades = sorted(trades, key=lambda t: int(t.get("timestamp") or 0), reverse=True)

    # DEBUG: show raw fields from the last trades
    for t in trades[:10]:
        info = t.get("info") or {}

        debug_payload = {
            "id": t.get("id"),
            "order": t.get("order"),
            "timestamp": t.get("timestamp"),
            "side": t.get("side"),
            "amount": t.get("amount"),
            "realizedPnl": t.get("realizedPnl"),
            "closedPnl": t.get("closedPnl"),
            "profit": t.get("profit"),
            "info_realizedPnl": info.get("realizedPnl"),
            "info_closedPnl": info.get("closedPnl"),
            "info_profit": info.get("profit"),
            "info_profitValue": info.get("profitValue"),
            "info_reduceOnly": info.get("reduceOnly"),
            "info_positionSide": info.get("positionSide"),
        }

        print(f"PNL DEBUG {symbol} trade={json.dumps(debug_payload, ensure_ascii=False)}")

    best = None
    for t in trades:
        pnl = _extract_trade_pnl(t)
        qty = _extract_trade_qty(t)

        print(f"PNL DEBUG EXTRACT {symbol} id={t.get('id')} pnl={pnl} qty={qty}")

        if pnl != 0 and qty > 0:
            best = {
                "pnl": pnl,
                "qty": qty,
                "ts": int(t.get("timestamp") or 0),
                "side": str(t.get("side") or (t.get("info") or {}).get("side") or ""),
            }
            break

    return best


async def pnl_watcher(app, exchange, log, log_chat_id, interval: int = 3):
    global LAST_POSITIONS, weekly_pnl, week_start

    while True:
        try:
            current_positions = await _fetch_positions_map(exchange)

            # only full close: prev > 0 and current == 0
            just_closed = []
            for symbol, prev in LAST_POSITIONS.items():
                prev_size = float(prev.get("size", 0.0))
                current = current_positions.get(symbol)
                curr_size = float(current.get("size", 0.0)) if current else 0.0

                if prev_size > 0 and curr_size == 0:
                    just_closed.append((symbol, prev))

            for symbol, prev in just_closed:
                if not _should_send(symbol):
                    continue

                info = await _get_last_closed_trade_info(exchange, symbol)

                pnl = 0.0
                qty = prev.get("size", 0.0)
                side = prev.get("side", "")

                if info:
                    pnl = float(info["pnl"])
                    qty = info["qty"] or qty

                # don't send fake zero-pnl messages
                if pnl == 0.0:
                    log("INFO", f"PNL notifier skip: {symbol} pnl=0.0")
                    continue

                msg = _format_close_message(symbol, side, pnl, qty)

                try:
                    await app.send_message(log_chat_id, msg)
                    weekly_pnl += pnl
                    log("INFO", f"PNL notifier sent: {symbol} pnl={pnl} qty={qty}")
                except Exception as e:
                    log("ERROR", f"PNL send failed for {symbol}: {e}")

            LAST_POSITIONS = current_positions

            if time.time() - week_start >= 7 * 24 * 60 * 60:
                status = "🟢 PROFIT" if weekly_pnl >= 0 else "🔴 LOSS"
                report = (
                    "📊 WEEKLY REPORT\n\n"
                    f"{status}\n"
                    f"Total PnL: {round(weekly_pnl, 4)} USDT"
                )
                try:
                    await app.send_message(log_chat_id, report)
                except Exception as e:
                    log("ERROR", f"Weekly report send failed: {e}")

                weekly_pnl = 0.0
                week_start = time.time()

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)