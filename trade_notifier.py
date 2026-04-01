import asyncio
import time
import json
import hmac
import hashlib
import urllib.parse
from typing import Any, Dict, Optional

import requests


# =========================
# GLOBAL STATE
# =========================
LAST_POSITIONS: Dict[str, dict] = {}
SENT_CLOSE_CACHE: Dict[str, int] = {}
CACHE_TTL_SEC = 30

weekly_pnl = 0.0
week_start = time.time()


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


def _extract_pos_side(pos: dict) -> str:
    return str(
        pos.get("side")
        or pos.get("positionSide")
        or (pos.get("info") or {}).get("positionSide")
        or ""
    ).lower()


def _extract_pos_size(pos: dict) -> float:
    return abs(
        _to_float(
            pos.get("contracts")
            or pos.get("size")
            or pos.get("positionAmt")
            or 0
        )
    )


def _extract_entry(pos: dict) -> float:
    return _to_float(
        pos.get("entryPrice")
        or pos.get("average")
        or pos.get("avgPrice")
        or 0
    )


def _should_send(symbol: str) -> bool:
    now = int(time.time())
    last = SENT_CLOSE_CACHE.get(symbol, 0)
    if now - last < CACHE_TTL_SEC:
        return False
    SENT_CLOSE_CACHE[symbol] = now
    return True


def _format_pnl_message(
    symbol: str,
    side: str,
    pnl: float,
    qty: float,
    entry_price: float = 0.0,
    income_count: int = 0,
) -> str:
    status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"
    side_text = side.upper() if side else "UNKNOWN"

    lines = [
        f"{status} #{symbol}",
        f"Side: {side_text}",
        f"Realized PnL: {round(pnl, 4)} USDT",
        f"Qty: {round(qty, 4)}",
    ]

    if entry_price > 0:
        lines.append(f"Entry: {round(entry_price, 6)}")

    if income_count > 0:
        lines.append(f"Income rows: {income_count}")

    return "\n".join(lines)


def _normalize_symbol_for_compare(symbol: str) -> str:
    return (
        str(symbol or "")
        .upper()
        .replace("/", "-")
        .replace(":USDT", "")
        .replace("_", "-")
        .strip()
    )


# =========================
# POSITIONS SNAPSHOT
# =========================
async def _fetch_positions_map(exchange) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    now_ms = int(time.time() * 1000)

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
            prev = LAST_POSITIONS.get(symbol) or {}
            result[symbol] = {
                "side": side,
                "size": size,
                "entry": _extract_entry(pos),
                "opened_at": prev.get("opened_at", now_ms),
                "raw": pos,
            }

    return result


# =========================
# BINGX INCOME API
# =========================
def bingx_signed_get(path: str, params: dict, api_key: str, api_secret: str):
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)

    query = urllib.parse.urlencode(sorted(params.items()))
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url = f"https://open-api.bingx.com{path}?{query}&signature={signature}"
    headers = {"X-BX-APIKEY": api_key}

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_swap_income(
    api_key: str,
    api_secret: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    limit: int = 100,
):
    params = {
        "limit": limit,
    }

    if start_ms is not None:
        params["startTime"] = start_ms
    if end_ms is not None:
        params["endTime"] = end_ms

    return bingx_signed_get(
        "/openApi/swap/v2/user/income",
        params,
        api_key,
        api_secret,
    )


def _extract_income_rows(resp: dict) -> list:
    if not isinstance(resp, dict):
        return []

    data = resp.get("data")

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("rows", "list", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def _extract_income_symbol(row: dict) -> str:
    return str(
        row.get("symbol")
        or row.get("market")
        or row.get("currency")
        or row.get("asset")
        or ""
    )


def _extract_income_time(row: dict) -> int:
    for key in ("time", "timestamp", "createdTime", "updateTime"):
        try:
            value = row.get(key)
            if value is not None:
                return int(value)
        except Exception:
            pass
    return 0


def _extract_income_value(row: dict) -> float:
    for key in ("income", "profit", "realizedPnl", "amount"):
        value = _to_float(row.get(key), 0.0)
        if value != 0.0:
            return value
    return 0.0


def _extract_income_type(row: dict) -> str:
    return str(
        row.get("incomeType")
        or row.get("type")
        or row.get("bizType")
        or ""
    ).lower()


async def _get_position_income_summary(
    symbol: str,
    api_key: str,
    api_secret: str,
    log,
    opened_at_ms: int,
    close_ts_ms: int,
) -> Optional[dict]:
    try:
        start_ms = max(0, opened_at_ms - 60_000)
        end_ms = close_ts_ms + 120_000

        resp = await asyncio.to_thread(
            get_swap_income,
            api_key,
            api_secret,
            start_ms,
            end_ms,
            100,
        )
    except Exception as e:
        log("ERROR", f"PNL DEBUG income request failed for {symbol}: {e}")
        return None

    rows = _extract_income_rows(resp)
    if not rows:
        log("INFO", f"PNL DEBUG income empty for {symbol}: {resp}")
        return None

    target_symbol = _normalize_symbol_for_compare(symbol)

    matched_rows = []
    total_pnl = 0.0

    for row in rows:
        row_symbol_raw = _extract_income_symbol(row)
        row_symbol = _normalize_symbol_for_compare(row_symbol_raw)
        income_value = _extract_income_value(row)
        income_type = _extract_income_type(row)
        ts = _extract_income_time(row)

        log(
            "INFO",
            f"PNL DEBUG INCOME EXTRACT target={target_symbol} row_symbol={row_symbol} type={income_type} pnl={income_value} ts={ts}",
        )

        if row_symbol and target_symbol not in row_symbol:
            continue

        if ts and (ts < start_ms or ts > end_ms):
            continue

        if income_value == 0.0:
            continue

        matched_rows.append(row)
        total_pnl += income_value

    for row in matched_rows[:20]:
        log("INFO", f"PNL DEBUG INCOME MATCHED {symbol}: {json.dumps(row, ensure_ascii=False)}")

    if not matched_rows:
        return None

    return {
        "pnl": total_pnl,
        "count": len(matched_rows),
        "rows": matched_rows,
    }


# =========================
# MAIN WATCHER
# =========================
async def pnl_watcher(
    app,
    exchange,
    log,
    log_chat_id,
    api_key: str,
    api_secret: str,
    interval: int = 3,
):
    global LAST_POSITIONS, weekly_pnl, week_start

    while True:
        try:
            current_positions = await _fetch_positions_map(exchange)

            just_closed = []
            for symbol, prev in LAST_POSITIONS.items():
                prev_size = float(prev.get("size", 0.0))
                current = current_positions.get(symbol)
                curr_size = float(current.get("size", 0.0)) if current else 0.0

                # тільки повне закриття
                if prev_size > 0 and curr_size == 0:
                    just_closed.append((symbol, prev))

            for symbol, prev in just_closed:
                if not _should_send(symbol):
                    continue

                side = str(prev.get("side", ""))
                qty = float(prev.get("size", 0.0))
                entry_price = float(prev.get("entry", 0.0))
                opened_at_ms = int(prev.get("opened_at", int(time.time() * 1000) - 10 * 60 * 1000))
                close_ts_ms = int(time.time() * 1000)

                income_info = None
                for _ in range(4):
                    await asyncio.sleep(1.5)
                    income_info = await _get_position_income_summary(
                        symbol=symbol,
                        api_key=api_key,
                        api_secret=api_secret,
                        log=log,
                        opened_at_ms=opened_at_ms,
                        close_ts_ms=close_ts_ms,
                    )
                    if income_info and float(income_info.get("pnl", 0.0)) != 0.0:
                        break

                pnl = float(income_info["pnl"]) if income_info else 0.0

                if pnl == 0.0:
                    log("INFO", f"PNL notifier skip: {symbol} pnl=0.0")
                    continue

                msg = _format_pnl_message(
                    symbol=symbol,
                    side=side,
                    pnl=pnl,
                    qty=qty,
                    entry_price=entry_price,
                    income_count=int(income_info.get("count", 0)) if income_info else 0,
                )

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