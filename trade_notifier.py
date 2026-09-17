import asyncio
import json
import os
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
weekly_strategy_pnl: Dict[str, float] = {}
week_start = time.time()
weekly_start_equity: Optional[float] = None
last_weekly_report_key: Optional[str] = None
KYIV_TZ = ZoneInfo("Europe/Kiev")
_STATE_DIR = (
    os.getenv("DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "/data"
).rstrip("/\\")
WEEKLY_STATE_FILE = os.path.join(_STATE_DIR, "weekly_report_state.json")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _timestamp_ms(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError):
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

def _extract_liquidation(pos: dict) -> float:
    info = pos.get("info") or {}
    return _to_float(
        pos.get("liquidationPrice")
        or pos.get("liquidation_price")
        or info.get("liquidationPrice")
        or info.get("liqPrice")
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


def _load_weekly_state() -> dict:
    try:
        with open(WEEKLY_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def _save_weekly_state(state: dict) -> None:
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        tmp_path = WEEKLY_STATE_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, WEEKLY_STATE_FILE)
    except OSError:
        # Reporting persistence must never stop position protection/notifying.
        return


def _income_cashflow_kind(row: dict) -> Optional[str]:
    """Classify only explicit capital transfers, never PnL/fees/funding."""
    income_type = _extract_income_type(row)
    info_text = _extract_income_info_text(row).upper()
    marker = f"{income_type} {info_text}"
    if any(word in marker for word in ("PNL", "FEE", "FUNDING", "COMMISSION")):
        return None
    if "DEPOSIT" in marker or "TRANSFER_IN" in marker or "TRANSFER IN" in marker:
        return "deposit"
    if "WITHDRAW" in marker or "TRANSFER_OUT" in marker or "TRANSFER OUT" in marker:
        return "withdrawal"
    if "TRANSFER" in marker:
        return "deposit" if _extract_income_value(row) > 0 else "withdrawal"
    return None


async def _get_week_cashflows(
    api_key: str,
    api_secret: str,
    start_ms: int,
    end_ms: int,
    log,
) -> dict:
    try:
        response = await asyncio.to_thread(
            get_swap_income, api_key, api_secret, start_ms, end_ms, 1000
        )
        rows = _extract_income_rows(response)
    except Exception as exc:
        log("WARNING", f"WEEKLY cashflow request failed: {exc}")
        return {"deposits": 0.0, "withdrawals": 0.0, "available": False}

    deposits = 0.0
    withdrawals = 0.0
    for row in rows:
        kind = _income_cashflow_kind(row)
        value = _extract_income_value(row)
        if kind == "deposit":
            deposits += abs(value)
        elif kind == "withdrawal":
            withdrawals += abs(value)
    return {
        "deposits": round(deposits, 8),
        "withdrawals": round(withdrawals, 8),
        "available": True,
    }


def _format_weekly_report(
    *,
    week_start_date,
    week_end_date,
    start_equity: float,
    end_equity: float,
    tracked_pnl: float,
    deposits: float,
    withdrawals: float,
    cashflows_available: bool,
    strategy_pnl: Optional[Dict[str, float]] = None,
) -> str:
    net_cashflow = deposits - withdrawals
    adjusted_pnl = end_equity - start_equity - net_cashflow
    pct_text = (
        f"{adjusted_pnl / start_equity * 100.0:.2f}%"
        if start_equity > 0 else "n/a"
    )
    status = "🟢 PROFIT" if adjusted_pnl >= 0 else "🔴 LOSS"
    cashflow_note = "" if cashflows_available else " (дані API недоступні)"
    strategy_lines = "".join(
        f"\n{strategy}: {value:+.4f} USDT"
        for strategy, value in sorted((strategy_pnl or {}).items())
    )
    return (
        "📊 WEEKLY REPORT\n\n"
        f"{status}\n"
        f"Week: {week_start_date.isoformat()} → {week_end_date.isoformat()}\n"
        f"Starting balance: {start_equity:.4f} USDT\n"
        f"Ending balance: {end_equity:.4f} USDT\n"
        f"Deposits: +{deposits:.4f} USDT{cashflow_note}\n"
        f"Withdrawals: -{withdrawals:.4f} USDT{cashflow_note}\n"
        f"Net cashflow: {net_cashflow:+.4f} USDT\n"
        f"Trading PnL (balance-adjusted): {adjusted_pnl:+.4f} USDT\n"
        f"Closed-trade PnL tracked: {tracked_pnl:+.4f} USDT{strategy_lines}\n"
        f"Percent Growth excluding deposits: {pct_text}"
    )


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
    liquidation_price: float = 0.0,
    strategy: Optional[str] = None,
) -> str:
    status = "🟢 ПРОФІТ" if pnl > 0 else "🔴 ЗБИТОК"
    side_text = side.upper() if side else "UNKNOWN"
    pnl_sign = "+" if pnl > 0 else ""

    lines = [
        "✅ УГОДУ ЗАКРИТО НА BINGX",
        "",
        f"{status}",
        f"Пара: {symbol}",
        f"Стратегія: {strategy}" if strategy else "Стратегія: UNKNOWN",
        f"Напрямок: {side_text}",
        f"Результат: {pnl_sign}{pnl:.4f} USDT",
        f"Кількість: {qty:.4f}",
    ]

    if entry_price > 0:
        lines.append(f"Вхід: {entry_price:.6g}")

    lines += ["", "Джерело: фактичний realized PnL BingX з комісіями та funding."]

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
            position_key = f"{symbol}:{side}"
            prev = LAST_POSITIONS.get(position_key) or {}
            result[position_key] = {
                "symbol": symbol,
                "side": side,
                "size": size,
                "entry": _extract_entry(pos),
                "liquidation": _extract_liquidation(pos),
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


def get_swap_fill_history(
    api_key: str,
    api_secret: str,
    start_ms: int,
    end_ms: int,
):
    """Return actual BingX fills, including realizedPnl for closing fills."""
    return bingx_signed_get(
        "/openApi/swap/v2/trade/allFillOrders",
        {
            "tradingUnit": "COIN",
            "startTs": start_ms,
            "endTs": end_ms,
            "currency": "USDT",
        },
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


def _extract_fill_rows(resp: dict) -> list:
    if not isinstance(resp, dict):
        return []
    data = resp.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("rows", "list", "result", "fills"):
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

def _extract_income_side(row: dict) -> str:
    return str(
        row.get("positionSide")
        or row.get("side")
        or row.get("posSide")
        or ""
    ).lower()


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


async def _get_fill_realized_pnl(
    symbol: str,
    api_key: str,
    api_secret: str,
    log,
    opened_at_ms: int,
    close_ts_ms: int,
) -> Optional[float]:
    """Recover realized PnL when BingX income contains only fees/funding."""
    start_ms = max(0, opened_at_ms - 300_000)
    end_ms = close_ts_ms + 600_000
    try:
        resp = await asyncio.to_thread(
            get_swap_fill_history,
            api_key,
            api_secret,
            start_ms,
            end_ms,
        )
    except Exception as exc:
        log("WARNING", f"PNL fill-history request failed for {symbol}: {exc}")
        return None

    target_symbol = _normalize_symbol_for_compare(symbol)
    matched = []
    for row in _extract_fill_rows(resp):
        row_symbol = _normalize_symbol_for_compare(_extract_income_symbol(row))
        ts = _extract_income_time(row)
        if ts and (ts < start_ms or ts > end_ms):
            continue
        if row_symbol and row_symbol != target_symbol:
            continue
        matched.append(row)

    if not matched:
        return None

    realized = sum(
        _to_float(row.get("realizedPnl") or row.get("realisedPnl"), 0.0)
        for row in matched
    )
    return realized if realized != 0.0 else None


async def _get_position_income_summary(
    symbol: str,
    api_key: str,
    api_secret: str,
    log,
    opened_at_ms: int,
    close_ts_ms: int,
    position_side: str = "",
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
        log("WARNING", f"PNL income request failed for {symbol}: {e}")
        return {
            "pnl": 0.0,
            "count": 0,
            "rows": [],
            "has_real_pnl_signal": False,
        }

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
        row_side = _extract_income_side(row)
        ts = _extract_income_time(row)

        if ts and (ts < start_ms or ts > end_ms):
            continue

        if income_value == 0.0:
            continue

        if not _is_relevant_income_type(income_type, info_text):
            continue

        if row_symbol and row_symbol != target_symbol:
            continue
        if row_side and position_side and row_side != position_side.lower():
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
    position_side: str = "",
) -> Optional[dict]:
    best_income_info = None
    stable_rounds = 0
    last_signature = None

    # BingX income rows can appear late, especially after volatile moves or API lag.
    # Wait up to ~2 minutes before giving up.
    for _ in range(30):
        await asyncio.sleep(4)

        income_info = await _get_position_income_summary(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            log=log,
            opened_at_ms=opened_at_ms,
            close_ts_ms=close_ts_ms,
            position_side=position_side,
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

        # Once the income snapshot is stable, either use its realized PnL or
        # fall back to fill history below. There is no value in waiting two
        # minutes when BingX is consistently returning fee-only rows.
        if stable_rounds >= 2:
            break

    # BingX can publish fees first while REALIZED_PNL is absent from the
    # income response. Fill history is authoritative for executed closes.
    if best_income_info is None or not best_income_info.get("has_real_pnl_signal"):
        realized = await _get_fill_realized_pnl(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            log=log,
            opened_at_ms=opened_at_ms,
            close_ts_ms=close_ts_ms,
        )
        if realized is not None:
            income_cashflows = float((best_income_info or {}).get("pnl", 0.0))
            best_income_info = {
                "pnl": realized + income_cashflows,
                "count": int((best_income_info or {}).get("count", 0)),
                "rows": list((best_income_info or {}).get("rows", [])),
                "has_real_pnl_signal": True,
                "realized_source": "fill_history",
            }

    return best_income_info


async def pnl_watcher(
    app,
    exchange,
    log,
    log_chat_id,
    api_key: str,
    api_secret: str,
    interval: int = 3,
    strategy_resolver=None,
    pending_closed_resolver=None,
    closed_notified_callback=None,
):
    global LAST_POSITIONS, weekly_pnl, weekly_strategy_pnl, week_start, weekly_start_equity, last_weekly_report_key

    persisted = _load_weekly_state()
    if persisted:
        weekly_pnl = _to_float(persisted.get("tracked_pnl"), weekly_pnl)
        stored_strategy_pnl = persisted.get("strategy_pnl") or {}
        weekly_strategy_pnl = {
            str(key): _to_float(value) for key, value in stored_strategy_pnl.items()
        }
        weekly_start_equity = _to_float(
            persisted.get("start_equity"), weekly_start_equity or 0.0
        ) or None
        week_start = _to_float(persisted.get("started_at"), week_start)
        last_weekly_report_key = persisted.get("last_report_key")

    while True:
        try:
            if weekly_start_equity is None:
                weekly_start_equity = await _get_total_usdt_balance(exchange)
                log("INFO", f"WEEKLY baseline equity set: {weekly_start_equity}")
                _save_weekly_state({
                    "week_key": _current_week_key(),
                    "started_at": week_start,
                    "start_equity": weekly_start_equity,
                    "tracked_pnl": weekly_pnl,
                    "strategy_pnl": weekly_strategy_pnl,
                    "last_report_key": last_weekly_report_key,
                })

            current_positions = await _fetch_positions_map(exchange)

            if weekly_start_equity is None:
                weekly_start_equity = await _get_total_usdt_balance(exchange)

            just_closed = []
            for position_key, prev in LAST_POSITIONS.items():
                prev_size = float(prev.get("size", 0.0))
                current = current_positions.get(position_key)
                curr_size = float(current.get("size", 0.0)) if current else 0.0

                if prev_size > 0 and curr_size == 0:
                    just_closed.append((position_key, prev))

            if pending_closed_resolver:
                try:
                    pending = pending_closed_resolver() or {}
                    already_queued = {key for key, _ in just_closed}
                    for position_key, row in pending.items():
                        if position_key not in already_queued:
                            just_closed.append((position_key, row))
                except Exception as exc:
                    log("WARNING", f"PNL pending-close recovery failed: {exc}")

            for position_key, prev in just_closed:
                symbol = str(prev.get("symbol") or position_key.rsplit(":", 1)[0])
                close_ts_ms = int(time.time() * 1000)
                side = str(prev.get("side", ""))
                close_key = f"{symbol}:{side}:{close_ts_ms // 60000}"

                if not _should_send(close_key):
                    continue

                qty = float(prev.get("size", 0.0))
                entry_price = float(prev.get("entry", 0.0))
                liquidation_price = float(prev.get("liquidation", 0.0))
                opened_at_ms = _timestamp_ms(
                    prev.get("opened_at_ms") or prev.get("opened_at"),
                    int(time.time() * 1000) - 10 * 60 * 1000,
                )

                log("INFO", f"PNL close detected: {symbol}")

                income_info = await _wait_final_income_summary(
                    symbol=symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    log=log,
                    opened_at_ms=opened_at_ms,
                    close_ts_ms=close_ts_ms,
                    position_side=side,
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

                strategy = "UNKNOWN"
                if strategy_resolver:
                    try:
                        strategy = str(strategy_resolver(position_key) or "UNKNOWN").upper()
                    except Exception as exc:
                        log("WARNING", f"PNL strategy resolve failed {position_key}: {exc}")
                msg = _format_pnl_message(
                    symbol=symbol,
                    side=side,
                    pnl=pnl,
                    qty=qty,
                    entry_price=entry_price,
                    liquidation_price=liquidation_price,
                    strategy=strategy,
                )

                try:
                    await app.send_message(log_chat_id, msg)
                    weekly_pnl += pnl
                    weekly_strategy_pnl[strategy] = weekly_strategy_pnl.get(strategy, 0.0) + pnl
                    state = _load_weekly_state()
                    state.update({
                        "week_key": state.get("week_key") or _current_week_key(),
                        "started_at": state.get("started_at") or week_start,
                        "start_equity": weekly_start_equity,
                        "tracked_pnl": weekly_pnl,
                        "strategy_pnl": weekly_strategy_pnl,
                        "last_report_key": last_weekly_report_key,
                    })
                    _save_weekly_state(state)
                    if closed_notified_callback:
                        try:
                            closed_notified_callback(position_key, pnl)
                        except Exception as exc:
                            log("WARNING", f"PNL notified marker failed {position_key}: {exc}")
                    log("INFO", f"PNL notifier sent: {symbol} net_pnl={pnl} qty={qty}")
                except Exception as e:
                    log("ERROR", f"PNL send failed for {symbol}: {e}")

            LAST_POSITIONS = current_positions

            should_send_weekly, week_key = _should_send_weekly_report_now()
            if should_send_weekly and last_weekly_report_key != week_key:
                start_equity = float(weekly_start_equity or 0.0)
                week_start_date = datetime.fromisoformat(week_key).date()
                week_end_date = week_start_date + timedelta(days=6)
                end_equity = await _get_total_usdt_balance(exchange)
                start_ms = int(
                    datetime.combine(
                        week_start_date, datetime.min.time(), tzinfo=KYIV_TZ
                    ).timestamp() * 1000
                )
                end_ms = int(
                    datetime.combine(
                        week_end_date + timedelta(days=1),
                        datetime.min.time(),
                        tzinfo=KYIV_TZ,
                    ).timestamp() * 1000 - 1
                )
                cashflows = await _get_week_cashflows(
                    api_key, api_secret, start_ms, end_ms, log
                )
                report = _format_weekly_report(
                    week_start_date=week_start_date,
                    week_end_date=week_end_date,
                    start_equity=start_equity,
                    end_equity=end_equity,
                    tracked_pnl=weekly_pnl,
                    deposits=cashflows["deposits"],
                    withdrawals=cashflows["withdrawals"],
                    cashflows_available=cashflows["available"],
                    strategy_pnl=weekly_strategy_pnl,
                )
                try:
                    await app.send_message(log_chat_id, report)
                    last_weekly_report_key = week_key
                    log("INFO", f"Weekly report sent for week={week_key}")
                except Exception as e:
                    log("ERROR", f"Weekly report send failed: {e}")
                else:
                    weekly_pnl = 0.0
                    weekly_strategy_pnl = {}
                    week_start = time.time()
                    weekly_start_equity = await _get_total_usdt_balance(exchange)
                    _save_weekly_state({
                        "week_key": _current_week_key(),
                        "started_at": week_start,
                        "start_equity": weekly_start_equity,
                        "tracked_pnl": weekly_pnl,
                        "strategy_pnl": weekly_strategy_pnl,
                        "last_report_key": last_weekly_report_key,
                    })
                    log("INFO", f"NEW weekly baseline equity: {weekly_start_equity}")

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)
