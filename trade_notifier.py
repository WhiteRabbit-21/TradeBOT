import asyncio
import time
import hmac
import hashlib
import urllib.parse
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests


LAST_POSITIONS: Dict[str, dict] = {}
SENT_CLOSE_CACHE: Dict[str, int] = {}
CACHE_TTL_SEC = 90

weekly_pnl = 0.0
week_start = time.time()
weekly_start_equity: Optional[float] = None
last_weekly_report_key: Optional[str] = None
KYIV_TZ = ZoneInfo("Europe/Kiev")


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


def _extract_total_usdt_balance(balance: dict) -> float:
    if not isinstance(balance, dict):
        return 0.0

    total_usdt = _to_float((balance.get("total") or {}).get("USDT"), 0.0)
    if total_usdt > 0:
        return total_usdt

    free_usdt = _to_float((balance.get("free") or {}).get("USDT"), 0.0)
    used_usdt = _to_float((balance.get("used") or {}).get("USDT"), 0.0)
    total = free_usdt + used_usdt
    if total > 0:
        return total

    return 0.0


async def _get_total_usdt_balance(exchange) -> float:
    for account_type in ("swap", "future", "futures", "contract"):
        try:
            balance = await asyncio.to_thread(exchange.fetch_balance, {"type": account_type})
            total = _extract_total_usdt_balance(balance)
            if total > 0:
                return total
        except Exception:
            pass

    try:
        balance = await asyncio.to_thread(exchange.fetch_balance)
        return _extract_total_usdt_balance(balance)
    except Exception:
        return 0.0




def _week_monday_date(dt: datetime):
    return (dt - timedelta(days=dt.weekday())).date()


def _current_week_key() -> str:
    now_local = datetime.now(KYIV_TZ)
    monday = _week_monday_date(now_local)
    return monday.isoformat()


def _should_send_weekly_report_now() -> tuple[bool, str]:
    now_local = datetime.now(KYIV_TZ)

    # Надсилаємо звіт у понеділок вночі, щоб не залежати від вузького
    # вікна Sunday 23:55-23:59. Так бот не пропустить звіт через рестарт.
    should_send = now_local.weekday() == 0 and 0 <= now_local.hour < 3

    # Формуємо ключ попереднього тижня, за який і шлемо звіт.
    prev_week_monday = _week_monday_date(now_local - timedelta(days=7))
    week_key = prev_week_monday.isoformat()

    return should_send, week_key

def _should_send(close_key: str) -> bool:
    now = int(time.time())
    last = SENT_CLOSE_CACHE.get(close_key, 0)
    if now - last < CACHE_TTL_SEC:
        return False
    SENT_CLOSE_CACHE[close_key] = now
    return True


def _format_pnl_message(
    symbol: str,
    side: str,
    pnl: float,
    qty: float,
    entry_price: float = 0.0,
) -> str:
    status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"
    side_text = side.upper() if side else "UNKNOWN"

    lines = [
        f"{status} #{symbol}",
        f"Side: {side_text}",
        f"Net PnL: {round(pnl, 4)} USDT",
        f"Qty: {round(qty, 4)}",
    ]

    if entry_price > 0:
        lines.append(f"Entry: {round(entry_price, 6)}")

    return "\n".join(lines)


def _normalize_symbol_for_compare(symbol: str) -> str:
    s = str(symbol or "").upper().strip()

    if ":USDT" in s:
        s = s.replace(":USDT", "")

    s = s.replace("/", "")
    s = s.replace("-", "")
    s = s.replace("_", "")

    return s


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
    params = {"limit": limit}

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
        or row.get("pair")
        or row.get("contract")
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
    ).upper()


def _extract_income_info_text(row: dict) -> str:
    return str(row.get("info") or "").lower()


def _is_relevant_income_type(income_type: str, info_text: str) -> bool:
    t = (income_type or "").upper()
    info_text = (info_text or "").lower()

    if "PNL" in t:
        return True
    if "TRADING_FEE" in t:
        return True
    if "COMMISSION" in t:
        return True
    if "FUNDING" in t:
        return True
    if "FEE" in t:
        return True

    if "pnl" in info_text:
        return True
    if "fee" in info_text:
        return True
    if "funding" in info_text:
        return True
    if "commission" in info_text:
        return True

    return False


def _has_real_pnl_signal(rows: list[dict]) -> bool:
    for row in rows:
        income_type = _extract_income_type(row)
        info_text = _extract_income_info_text(row)

        if "PNL" in income_type:
            return True

        if "realized" in info_text and "pnl" in info_text:
            return True

    return False


async def _get_position_income_summary(
    symbol: str,
    api_key: str,
    api_secret: str,
    log,
    opened_at_ms: int,
    close_ts_ms: int,
) -> Optional[dict]:
    try:
        start_ms = max(0, opened_at_ms - 300_000)
        end_ms = close_ts_ms + 600_000

        resp = await asyncio.to_thread(
            get_swap_income,
            api_key,
            api_secret,
            start_ms,
            end_ms,
            100,
        )
    except Exception as e:
        log("ERROR", f"PNL income request failed for {symbol}: {e}")
        return None

    rows = _extract_income_rows(resp)
    if not rows:
        return None

    target_symbol = _normalize_symbol_for_compare(symbol)

    matched_rows = []
    total_pnl = 0.0

    rows = sorted(rows, key=_extract_income_time, reverse=True)

    for row in rows:
        row_symbol_raw = _extract_income_symbol(row)
        row_symbol = _normalize_symbol_for_compare(row_symbol_raw)
        income_value = _extract_income_value(row)
        income_type = _extract_income_type(row)
        info_text = _extract_income_info_text(row)
        ts = _extract_income_time(row)

        if ts and (ts < start_ms or ts > end_ms):
            continue

        if income_value == 0.0:
            continue

        if not _is_relevant_income_type(income_type, info_text):
            continue

        if row_symbol and row_symbol != target_symbol:
            continue

        matched_rows.append(row)
        total_pnl += income_value

    if not matched_rows:
        return None

    return {
        "pnl": total_pnl,
        "count": len(matched_rows),
        "rows": matched_rows,
        "has_real_pnl_signal": _has_real_pnl_signal(matched_rows),
    }


def _is_better_income_snapshot(new_info: dict, best_info: Optional[dict]) -> bool:
    if best_info is None:
        return True

    new_has_real = bool(new_info.get("has_real_pnl_signal"))
    best_has_real = bool(best_info.get("has_real_pnl_signal"))

    if new_has_real and not best_has_real:
        return True
    if best_has_real and not new_has_real:
        return False

    new_count = int(new_info.get("count", 0))
    best_count = int(best_info.get("count", 0))

    if new_count > best_count:
        return True
    if new_count < best_count:
        return False

    new_pnl = float(new_info.get("pnl", 0.0))
    best_pnl = float(best_info.get("pnl", 0.0))

    if abs(new_pnl) > abs(best_pnl):
        return True

    return False


async def _wait_final_income_summary(
    symbol: str,
    api_key: str,
    api_secret: str,
    log,
    opened_at_ms: int,
    close_ts_ms: int,
) -> Optional[dict]:
    best_income_info = None
    stable_rounds = 0
    last_signature = None

    for _ in range(15):
        await asyncio.sleep(2)

        income_info = await _get_position_income_summary(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            log=log,
            opened_at_ms=opened_at_ms,
            close_ts_ms=close_ts_ms,
        )

        if not income_info:
            continue

        if _is_better_income_snapshot(income_info, best_income_info):
            best_income_info = income_info

        count = int(income_info.get("count", 0))
        pnl_now = round(float(income_info.get("pnl", 0.0)), 10)
        has_real = bool(income_info.get("has_real_pnl_signal"))
        signature = (count, pnl_now, has_real)

        if signature == last_signature:
            stable_rounds += 1
        else:
            stable_rounds = 0

        last_signature = signature

        if stable_rounds >= 2 and has_real:
            break

    return best_income_info


async def pnl_watcher(
    app,
    exchange,
    log,
    log_chat_id,
    api_key: str,
    api_secret: str,
    interval: int = 3,
):
    global LAST_POSITIONS, weekly_pnl, week_start, weekly_start_equity, last_weekly_report_key

    while True:
        try:
            if weekly_start_equity is None:
                weekly_start_equity = await _get_total_usdt_balance(exchange)
                log("INFO", f"WEEKLY baseline equity set: {weekly_start_equity}")

            current_positions = await _fetch_positions_map(exchange)

            if weekly_start_equity is None:
                weekly_start_equity = await _get_total_usdt_balance(exchange)

            just_closed = []
            for symbol, prev in LAST_POSITIONS.items():
                prev_size = float(prev.get("size", 0.0))
                current = current_positions.get(symbol)
                curr_size = float(current.get("size", 0.0)) if current else 0.0

                if prev_size > 0 and curr_size == 0:
                    just_closed.append((symbol, prev))

            for symbol, prev in just_closed:
                close_ts_ms = int(time.time() * 1000)
                close_key = f"{symbol}:{close_ts_ms // 60000}"

                if not _should_send(close_key):
                    continue

                side = str(prev.get("side", ""))
                qty = float(prev.get("size", 0.0))
                entry_price = float(prev.get("entry", 0.0))
                opened_at_ms = int(
                    prev.get("opened_at", int(time.time() * 1000) - 10 * 60 * 1000)
                )

                log("INFO", f"PNL close detected: {symbol}")

                income_info = await _wait_final_income_summary(
                    symbol=symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    log=log,
                    opened_at_ms=opened_at_ms,
                    close_ts_ms=close_ts_ms,
                )

                if income_info is None:
                    log("WARNING", f"PNL notifier: no income rows found for {symbol}")
                    continue

                if not income_info.get("has_real_pnl_signal"):
                    log("WARNING", f"PNL notifier skip: no real pnl row yet for {symbol}")
                    continue

                pnl = float(income_info["pnl"])

                if pnl == 0.0:
                    log("INFO", f"PNL notifier skip: {symbol} pnl=0.0")
                    continue

                msg = _format_pnl_message(
                    symbol=symbol,
                    side=side,
                    pnl=pnl,
                    qty=qty,
                    entry_price=entry_price,
                )

                try:
                    await app.send_message(log_chat_id, msg)
                    weekly_pnl += pnl
                    log("INFO", f"PNL notifier sent: {symbol} net_pnl={pnl} qty={qty}")
                except Exception as e:
                    log("ERROR", f"PNL send failed for {symbol}: {e}")

            LAST_POSITIONS = current_positions

            should_send_weekly, week_key = _should_send_weekly_report_now()
            if should_send_weekly and last_weekly_report_key != week_key:
                status = "🟢 PROFIT" if weekly_pnl >= 0 else "🔴 LOSS"

                pct_text = "n/a"
                start_equity = float(weekly_start_equity or 0.0)
                if start_equity > 0:
                    weekly_pct = (weekly_pnl / start_equity) * 100.0
                    pct_text = f"{round(weekly_pct, 2)}%"

                week_start_date = datetime.fromisoformat(week_key).date()
                week_end_date = week_start_date + timedelta(days=6)
                report = (
                    "📊 WEEKLY REPORT\n\n"
                    f"{status}\n"
                    f"Week: {week_start_date.isoformat()} → {week_end_date.isoformat()}\n"
                    f"Total Net PnL: {round(weekly_pnl, 4)} USDT\n"
                    f"Percent Growth: {pct_text}"
                )
                try:
                    await app.send_message(log_chat_id, report)
                    last_weekly_report_key = week_key
                    log("INFO", f"Weekly report sent for week={week_key}")
                except Exception as e:
                    log("ERROR", f"Weekly report send failed: {e}")
                else:
                    weekly_pnl = 0.0
                    week_start = time.time()
                    weekly_start_equity = await _get_total_usdt_balance(exchange)
                    log("INFO", f"NEW weekly baseline equity: {weekly_start_equity}")

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)