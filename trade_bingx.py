import os
import re
import json
import time
import base64
import asyncio
import hashlib
import hmac
import math
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from decimal import Decimal
from urllib.parse import urlencode
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from typing import Optional, Any
from trade_notifier import pnl_watcher
from asset_universe import classify_non_crypto_asset
from trade_rules import (
    ENTRY_RULES, SHADOW_S1_RULES, SHADOW_S2_RULES, SWING_RULES,
    A3, StrategyDecision, select_strategy,
)
from shadow_trading import (
    ShadowBook, ShadowS1Book, extract_calibrated_probability,
    qualifies_s1, qualifies_s2,
)
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
_REQUIRED_LIVE_ENV = (
    "TG_API_ID",
    "TG_API_HASH",
    "TG_SESSION_STRING",
    "BINGX_API_KEY",
    "BINGX_API_SECRET",
)
_missing_live_env = [name for name in _REQUIRED_LIVE_ENV if not os.getenv(name)]
if _missing_live_env and os.getenv("WAIT_FOR_CONFIG", "0").strip() == "1":
    print(
        "TradeBOT is waiting for required configuration: "
        + ", ".join(_missing_live_env),
        flush=True,
    )
    while True:
        time.sleep(300)

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "-5486330898"))   # SignalBot+ automatic source
CONTROL_CHAT_ID = int(os.getenv("CONTROL_CHAT_ID", "566620979"))   # Saved Messages manual fallback
SOURCE_CHAT_IDS = list(dict.fromkeys((TARGET_CHAT_ID, CONTROL_CHAT_ID)))
LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-1003828203122"))      # куди шлемо логи
PNL_CHAT_ID = int(os.getenv("PNL_CHAT_ID", "-1003332013833")) # куди шлемо профіт/лос

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "")
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "")
BINGX_SWAP_HOST = os.getenv("BINGX_SWAP_HOST", "https://open-api.bingx.com").rstrip("/")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")

DRY_RUN = os.getenv("DRY_RUN", "1").strip() == "1"
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))  # 5 хв
SWING_EXIT_WATCH_SEC = float(os.getenv("SWING_EXIT_WATCH_SEC", "4"))
POSITION_MISSING_CONFIRMATIONS_REQUIRED = max(
    2,
    int(os.getenv("POSITION_MISSING_CONFIRMATIONS_REQUIRED", "3")),
)
# Only the combined A1/A4/A5/A3 system may create new live positions.
# The policy is deliberately code-owned so stale Railway variables cannot
# silently re-enable a rejected style or side.
# Existing position management (SL/TP/BE/CLOSE) remains active around the clock.
ACTIVE_ENTRY_RULES = (ENTRY_RULES,)
TRADE_LONG_ONLY = all(rules.allowed_side == "long" for rules in ACTIVE_ENTRY_RULES)
# Module-specific session filtering is code-owned in ``select_strategy``.
ENTRY_BLOCK_START_HOUR_KYIV = 0
ENTRY_BLOCK_END_HOUR_KYIV = 0
RR1_POLICY_EPSILON = 1e-6
KYIV_TZ = ZoneInfo("Europe/Kyiv")

ALLOWED_SIGNAL_STYLES = {
    style for rules in ACTIVE_ENTRY_RULES for style in rules.allowed_styles
}


def _rules_for_signal_style(style: Optional[str]):
    normalized_style = str(style or "").strip().upper()
    if normalized_style == "INTRADAY":
        return A3
    # Retained for protective-order management of SWING positions opened
    # before live Rule 1 was paused. New SWING entries are rejected earlier by
    # ALLOWED_SIGNAL_STYLES.
    if normalized_style in SWING_RULES.allowed_styles:
        return SWING_RULES
    return None


SWING_MAX_ENTRY_DRIFT_R = float(
    os.getenv(
        "SWING_MAX_ENTRY_DRIFT_R",
        str(SWING_RULES.max_adverse_entry_drift_r),
    )
)
FIXED_RISK_PCT = ENTRY_RULES.risk_per_trade_pct
MAX_AUTO_LEVERAGE = int(os.getenv("MAX_AUTO_LEVERAGE", "20"))
LEVERAGE_DISTANCE_BUFFER = float(os.getenv("LEVERAGE_DISTANCE_BUFFER", "3.0"))
MAX_MARGIN_USAGE_PCT = float(os.getenv("MAX_MARGIN_USAGE_PCT", "95"))

MEDIA_DELAY_SEC = float(os.getenv("MEDIA_DELAY_SEC", "5"))  # wait for album completion
CLOSE_BUNDLE_WINDOW_SEC = float(os.getenv("CLOSE_BUNDLE_WINDOW_SEC", "15"))  # attach orphan photos

# --- TG logging config (hardcoded) ---
LOG_LEVEL = "INFO"   # DEBUG / INFO / WARNING / ERROR
LOG_FLUSH_SEC = 20   # INFO пачкою раз на N секунд
STATE_DIR = (
    os.getenv("DATA_DIR")
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
    or "/data"
).rstrip("/\\")
SLTP_FILE = os.path.join(STATE_DIR, "sltp.json")
LAST_SLTP = {}
ORDER_IDS_FILE = os.path.join(STATE_DIR, "order_ids.json")
LAST_ORDER_IDS = {}
POSITION_MISSING_COUNTS = {}
EXECUTION_STATE_FILE = os.path.join(STATE_DIR, "execution_state.json")
EXECUTION_STATE = {"signals": {}, "positions": {}}
EXECUTION_STATE_LOCK = threading.RLock()
EXECUTION_SYNC_SEC = max(10.0, float(os.getenv("EXECUTION_SYNC_SEC", "30")))
POSITION_API_TOKEN = os.getenv("POSITION_API_TOKEN", "").strip()
POSITION_API_PORT = int(os.getenv("POSITION_API_PORT", "8080"))
SHADOW_S2_FILE = os.path.join(STATE_DIR, "shadow_s2.json")
SHADOW_S2 = ShadowBook(SHADOW_S2_FILE)
SHADOW_S1_FILE = os.path.join(STATE_DIR, "shadow_s1.json")
SHADOW_S1 = ShadowS1Book(SHADOW_S1_FILE)
SHADOW_S2_WATCH_SEC = max(2.0, float(os.getenv("SHADOW_S2_WATCH_SEC", "5")))


class PositionLookupError(RuntimeError):
    """BingX position state could not be read reliably."""

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


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_execution_state():
    """Load the durable SignalBot -> BingX execution journal."""
    global EXECUTION_STATE
    with EXECUTION_STATE_LOCK:
        try:
            if os.path.exists(EXECUTION_STATE_FILE):
                with open(EXECUTION_STATE_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    EXECUTION_STATE = {
                        "signals": dict(loaded.get("signals") or {}),
                        "positions": dict(loaded.get("positions") or {}),
                    }
        except Exception as e:
            print("EXECUTION_STATE load error:", e)


def save_execution_state():
    """Atomically persist the journal on the Railway volume."""
    with EXECUTION_STATE_LOCK:
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            tmp_path = EXECUTION_STATE_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(EXECUTION_STATE, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, EXECUTION_STATE_FILE)
        except Exception as e:
            print("EXECUTION_STATE save error:", e)


def signal_execution_key(chat_id: Any, message_id: Any, text: str) -> str:
    """Stable identity for Telegram messages, with a content fallback."""
    if chat_id is not None and message_id is not None:
        return f"tg:{chat_id}:{message_id}"
    normalized = re.sub(r"\s+", " ", str(text or "").strip().casefold())
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def signal_content_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "").strip().casefold())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def update_execution_signal(signal_key: Optional[str], status: str, **fields):
    if not signal_key:
        return
    with EXECUTION_STATE_LOCK:
        row = EXECUTION_STATE["signals"].setdefault(
            signal_key, {"signal_key": signal_key, "received_at": _utc_iso()}
        )
        row.update({k: v for k, v in fields.items() if v is not None})
        row["status"] = status
        row["updated_at"] = _utc_iso()
        save_execution_state()


def execution_signal_is_duplicate(
    signal_key: Optional[str], content_hash: Optional[str] = None
) -> bool:
    if not signal_key:
        return False
    terminal_statuses = {
        "order_placed", "open", "open_protected", "closed", "emergency_closed"
    }
    signals = EXECUTION_STATE.get("signals", {})
    status = str((signals.get(signal_key) or {}).get("status") or "")
    if status in terminal_statuses:
        return True
    if content_hash:
        return any(
            row.get("content_hash") == content_hash
            and str(row.get("status") or "") in terminal_statuses
            for row in signals.values()
        )
    return False


def bind_signal_to_position(
    signal_key: Optional[str], symbol: str, side: str, order_id: Any = None, **fields
):
    position_key = f"{symbol}:{str(side).lower()}"
    with EXECUTION_STATE_LOCK:
        EXECUTION_STATE["positions"][position_key] = {
            "position_key": position_key,
            "signal_key": signal_key,
            "symbol": symbol,
            "side": str(side).lower(),
            "entry_order_id": str(order_id) if order_id is not None else None,
            "status": "open",
            "opened_at": _utc_iso(),
            "updated_at": _utc_iso(),
            **{k: v for k, v in fields.items() if v is not None},
        }
        save_execution_state()


def _live_positions_snapshot_sync() -> dict[str, dict]:
    positions = exchange.fetch_positions()
    result = {}
    for pos in positions or []:
        symbol = str(pos.get("symbol") or (pos.get("info") or {}).get("symbol") or "")
        side = _extract_position_side_sync(pos)
        if not symbol or side not in {"long", "short"}:
            continue
        qty = _extract_position_qty_sync(pos, symbol)
        if qty <= 0:
            continue
        result[f"{symbol}:{side}"] = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry": _position_entry_price(pos),
        }
    return result


def reconcile_execution_state_sync() -> dict:
    """Make BingX the source of truth for tracked open positions."""
    live = _live_positions_snapshot_sync()
    now = _utc_iso()
    closed = []
    unmatched = []
    with EXECUTION_STATE_LOCK:
        tracked = EXECUTION_STATE["positions"]
        for position_key, row in list(tracked.items()):
            actual = live.get(position_key)
            if actual:
                row.update(actual)
                row["status"] = "open"
                row["last_seen_at"] = now
                row["updated_at"] = now
            elif row.get("status") == "open":
                row["status"] = "closed"
                row["closed_at"] = now
                row["updated_at"] = now
                signal_key = row.get("signal_key")
                if signal_key:
                    signal_row = EXECUTION_STATE["signals"].get(signal_key) or {}
                    signal_row.update({"status": "closed", "closed_at": now, "updated_at": now})
                    EXECUTION_STATE["signals"][signal_key] = signal_row
                closed.append(position_key)
        for position_key, actual in live.items():
            if position_key not in tracked or tracked[position_key].get("status") != "open":
                tracked[position_key] = {
                    "position_key": position_key,
                    **actual,
                    "signal_key": None,
                    "status": "open",
                    "origin": "unmatched_exchange_position",
                    "opened_at": now,
                    "last_seen_at": now,
                    "updated_at": now,
                }
                unmatched.append(position_key)
        save_execution_state()
    return {"live": len(live), "closed": closed, "unmatched": unmatched}


def executed_open_positions_payload() -> dict:
    """Expose only signal-linked positions confirmed open by BingX sync."""
    with EXECUTION_STATE_LOCK:
        rows = []
        for position in EXECUTION_STATE.get("positions", {}).values():
            if position.get("status") != "open" or not position.get("signal_key"):
                continue
            signal = EXECUTION_STATE.get("signals", {}).get(position["signal_key"]) or {}
            rows.append({
                key: position.get(key)
                for key in (
                    "position_key", "symbol", "base", "side", "qty", "entry",
                    "actual_entry", "risk_pct", "risk_budget", "sl", "tp1", "tp2",
                    "tp3", "style", "strategy", "opened_at", "last_seen_at",
                )
            } | {"execution_status": signal.get("status")})
    rows.sort(key=lambda row: str(row.get("opened_at") or ""))
    return {"positions": rows, "count": len(rows), "generated_at": _utc_iso()}


class _PositionStatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?", 1)[0] != "/positions":
            self.send_error(404)
            return
        supplied = self.headers.get("Authorization", "")
        if not POSITION_API_TOKEN or not hmac.compare_digest(
            supplied, f"Bearer {POSITION_API_TOKEN}"
        ):
            self.send_error(401)
            return
        body = json.dumps(executed_open_positions_payload(), ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        return


def start_position_status_server() -> None:
    if not POSITION_API_TOKEN:
        log("WARNING", "POSITION API disabled: POSITION_API_TOKEN is missing")
        return
    server = ThreadingHTTPServer(("0.0.0.0", POSITION_API_PORT), _PositionStatusHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="position-api").start()
    log("INFO", f"POSITION API listening on port {POSITION_API_PORT}")


def portfolio_entry_block_reason(
    symbol: str,
    risk_pct: float,
    positions: Optional[dict[str, dict]] = None,
) -> Optional[str]:
    """Enforce one coin, four slots and 3% aggregate open risk."""
    rows = positions if positions is not None else EXECUTION_STATE.get("positions", {})
    open_rows = [row for row in rows.values() if row.get("status") == "open"]
    wanted_base = str(symbol or "").split("/")[0].split(":")[0].upper()
    for row in open_rows:
        row_base = str(row.get("symbol") or row.get("base") or "").split("/")[0].split(":")[0].upper()
        if wanted_base and row_base == wanted_base:
            return f"position for {wanted_base} already exists; one position per coin"
    if len(open_rows) >= ENTRY_RULES.max_concurrent_positions:
        return f"all {ENTRY_RULES.max_concurrent_positions} strategy slots are occupied"
    # Unknown legacy/exchange positions receive the largest module risk. This
    # fails safely instead of understating aggregate portfolio exposure.
    current_risk = sum(float(row.get("risk_pct") or FIXED_RISK_PCT) for row in open_rows)
    if current_risk + float(risk_pct) > ENTRY_RULES.max_open_risk_pct + 1e-9:
        return (
            f"open risk would be {current_risk + float(risk_pct):.2f}% "
            f"above the {ENTRY_RULES.max_open_risk_pct:.2f}% cap"
        )
    return None


async def execution_reconcile_loop():
    while True:
        try:
            result = await asyncio.to_thread(reconcile_execution_state_sync)
            if result["closed"]:
                log("INFO", f"EXEC SYNC closed_on_exchange={result['closed']}")
            if result["unmatched"]:
                log("WARNING", f"EXEC SYNC unmatched_exchange_positions={result['unmatched']}")
        except Exception as e:
            log("ERROR", f"EXEC SYNC failed: {e}")
        await asyncio.sleep(EXECUTION_SYNC_SEC)

# =========================
# TG LOGGER (batched)
# =========================
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_min_level = _LEVELS.get(LOG_LEVEL, 20)

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
    """Write diagnostics to Railway only.

    Telegram LOG_CHAT_ID is reserved for lifecycle messages belonging to
    signals that actually reached BingX.  Rejected signals, policy skips and
    worker diagnostics must never be published there.
    """
    lvl_name = (level or "INFO").upper()
    lvl = _LEVELS.get(lvl_name, 20)
    if lvl < _min_level:
        return

    line = f"[{_ts()}] [{lvl_name}] {msg}"
    print(line)

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

def _normalized_contract_base(value: Any) -> str:
    value = str(value or "").upper().strip()
    value = re.sub(r"(?:/|-|_)USDT(?::USDT)?$", "", value)
    return re.sub(r"[^A-Z0-9]", "", value)

def _find_swap_symbol(markets: dict[str, Any], base: str) -> Optional[str]:
    """Find USDT perpetuals whose BingX API asset differs from displayName."""
    raw_base = str(base or "").upper().strip()
    raw_base = re.sub(r"[^A-Z0-9]", "", raw_base)
    if raw_base.endswith("USDT"):
        raw_base = raw_base[:-4]

    base = SPECIAL_BASES.get(raw_base, raw_base)
    if not base:
        return None

    preferred = f"{base}/USDT:USDT"
    m = markets.get(preferred)
    if m and (m.get("swap") or m.get("contract")):
        return preferred

    for sym, m in markets.items():
        try:
            if not (m.get("swap") or m.get("contract")):
                continue
            if m.get("quote", "").upper() != "USDT":
                continue

            info = m.get("info") or {}
            candidates = (
                m.get("base"),
                info.get("displayName"),
                info.get("symbol"),
            )
            if any(_normalized_contract_base(candidate) == base for candidate in candidates):
                return sym
        except Exception:
            continue

    return None

def resolve_symbol_sync(base: str) -> Optional[str]:
    ensure_markets_loaded_sync()

    symbol = _find_swap_symbol(exchange.markets, base)
    if symbol:
        return symbol

    # A long-running worker can retain CCXT's market cache across new listings.
    try:
        refreshed = exchange.load_markets(True)
        markets = refreshed if isinstance(refreshed, dict) else exchange.markets
        symbol = _find_swap_symbol(markets, base)
        if symbol:
            log("INFO", f"MARKET RESOLVED AFTER REFRESH {base} -> {symbol}")
            return symbol
    except Exception as e:
        log("WARNING", f"BINGX market refresh failed for {base}: {e}")

    return None

def market_api_open_disabled_sync(symbol: str) -> Optional[str]:
    """Return a diagnostic when BingX lists a contract but blocks API opens."""
    try:
        market = exchange.market(symbol)
    except Exception:
        market = (getattr(exchange, "markets", {}) or {}).get(symbol) or {}

    info = market.get("info") or {}
    state = info.get("apiStateOpen")
    if isinstance(state, bool):
        disabled = not state
    else:
        disabled = str(state).strip().lower() in {"false", "0", "no", "off"}

    if not disabled:
        return None

    display_name = info.get("displayName") or symbol
    internal_symbol = info.get("symbol") or market.get("id") or symbol
    return (
        f"BingX lists {display_name}, but opening this contract via API is disabled "
        f"(apiStateOpen=false, internal symbol={internal_symbol})"
    )


def non_crypto_open_block_reason(
    base: str,
    signal_text: str = "",
    symbol: Optional[str] = None,
    style: Optional[str] = None,
) -> Optional[str]:
    """Apply the asset universe of the selected trading rule."""
    rules = _rules_for_signal_style(style) or SWING_RULES
    if not rules.ordinary_crypto_only:
        return None

    market = None
    if symbol:
        try:
            market = exchange.market(symbol)
        except Exception:
            market = (getattr(exchange, "markets", {}) or {}).get(symbol)

    asset_class = classify_non_crypto_asset(
        base,
        market=market,
        signal_text=signal_text,
    )
    if asset_class:
        return (
            f"asset={str(base).upper()} classified as {asset_class}; "
            f"{rules.version} allows ordinary crypto assets only"
        )
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

def fetch_position_oneway_sync(
    symbol: str,
    position_side: Optional[str] = None,
    *,
    raise_on_error: bool = False,
):

    try:
        positions = exchange.fetch_positions([symbol])

        wanted_side = str(position_side or "").lower()
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
                if wanted_side and side != wanted_side:
                    continue
                size = abs(
                    float(
                        p.get("contracts")
                        or p.get("size")
                        or p.get("positionAmt")
                        or 0
                    )
                )

                if size > best_size:
                    best_size = size
                    best = p

        return best if best_size > 0 else None

    except Exception as e:
        log(
            "ERROR",
            f"POSITION FETCH FAILED symbol={symbol} "
            f"position_side={position_side or 'any'} err={e}",
        )
        if raise_on_error:
            raise PositionLookupError(
                f"cannot verify {symbol}/{position_side or 'any'} position: {e}"
            ) from e
        return None
        
async def fetch_position_oneway(symbol: str, position_side: Optional[str] = None):

    return await asyncio.to_thread(
        fetch_position_oneway_sync,
        symbol,
        position_side,
    )    


def close_position_full_sync(base: str, position_side: Optional[str] = None):
    symbol = resolve_symbol_sync(base)

    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    positions = exchange.fetch_positions([symbol])
    closed = []

    wanted_side = str(position_side or "").lower()

    for pos in positions:
        side = (
            pos.get("side")
            or pos.get("positionSide")
            or (pos.get("info") or {}).get("positionSide")
            or ""
        ).lower()

        if side not in {"long", "short"}:
            continue
        if wanted_side and side != wanted_side:
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

    for side in closed:
        _clear_position_state(base, side)

    log("INFO", f"CLOSE cleanup done symbol={symbol} canceled_orders={canceled_total}")
    return f"CLOSED {'/'.join(closed)} | canceled={canceled_total}"


async def close_position_full(base: str, position_side: Optional[str] = None):
    return await asyncio.to_thread(close_position_full_sync, base, position_side)

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

def _place_bingx_tpsl_raw_sync(
    symbol: str,
    pos_side: str,
    trigger_price: float,
    quantity: float,
    kind: str,
    *,
    close_position: bool = False,
) -> dict:
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
    if close_position:
        # BingX Position TP/SL dynamically closes the entire remaining hedge
        # leg when the trigger fires. Quantity is still mandatory in the API,
        # but closePosition prevents precision dust after TP1/TP2 reductions.
        payload["closePosition"] = "true"

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

def _extract_position_liquidation(pos: Optional[dict]) -> Optional[float]:
    if not pos:
        return None
    info = pos.get("info") or {}
    value = (
        pos.get("liquidationPrice")
        or pos.get("liquidation_price")
        or info.get("liquidationPrice")
        or info.get("liquidation_price")
        or info.get("liqPrice")
    )
    try:
        value_f = float(value)
        return value_f if value_f > 0 else None
    except (TypeError, ValueError):
        return None

def estimate_liquidation_price(entry: float, side: str, leverage: int) -> Optional[float]:
    """Fallback estimate; the exchange value is preferred in cross-margin mode."""
    try:
        entry_f = float(entry)
        leverage_i = int(leverage)
        if entry_f <= 0 or leverage_i <= 0:
            return None
        if side == "long":
            return max(0.0, entry_f * (1.0 - 1.0 / leverage_i))
        if side == "short":
            return entry_f * (1.0 + 1.0 / leverage_i)
    except (TypeError, ValueError):
        pass
    return None

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

def _clear_position_state(base: str, pos_side: str):
    """Clear only one hedge leg, preserving the opposite side's protection."""
    base_u = str(base).upper()
    side = str(pos_side).lower()

    saved_by_side = LAST_SLTP.get(base_u) or {}
    saved_by_side.pop(side, None)
    if saved_by_side:
        LAST_SLTP[base_u] = saved_by_side
    else:
        LAST_SLTP.pop(base_u, None)

    LAST_ORDER_IDS.pop(_state_key(base_u, side), None)
    POSITION_MISSING_COUNTS.pop(_state_key(base_u, side), None)
    save_sltp()
    save_order_ids()

def cancel_order_exact_sync(symbol: str, order_id: str) -> bool:
    if not order_id:
        return False
    try:
        exchange.cancel_order(str(order_id), symbol)
        log("INFO", f"CANCEL EXACT OK symbol={symbol} id={order_id}")
        return True
    except Exception as e:
        error_text = str(e).lower()
        if "109400" in error_text or "order not exist" in error_text:
            # A filled TP/SL disappears before the next same-symbol signal.
            # Treat that as successful cleanup instead of a false alarm.
            log("INFO", f"CANCEL EXACT ALREADY GONE symbol={symbol} id={order_id}")
            return True
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
                if o_pos_side != pos_side.lower():
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

    for name, order_id in list(saved.items()):
        if name.endswith("_id") and order_id:
            cancel_order_exact_sync(symbol, order_id)

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
            # In hedge mode an order without positionSide cannot be safely
            # attributed. Never cancel it merely because the symbol matches.
            if o_pos_side != pos_side.lower():
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


def _position_entry_price(pos: Optional[dict], fallback: Optional[float] = None) -> Optional[float]:
    if pos:
        info = pos.get("info") or {}
        value = (
            pos.get("entryPrice")
            or pos.get("average")
            or pos.get("avgPrice")
            or info.get("avgPrice")
            or info.get("averagePrice")
        )
        try:
            value_f = float(value)
            if value_f > 0:
                return value_f
        except (TypeError, ValueError):
            pass
    try:
        fallback_f = float(fallback)
        return fallback_f if fallback_f > 0 else None
    except (TypeError, ValueError):
        return None


def calculate_swing_breakeven_price(
    entry: float,
    initial_sl: float,
    side: str = "long",
    buffer_r: float = SWING_RULES.breakeven_buffer_r,
) -> float:
    """Return Entry minus/plus the configured fraction of the original 1R."""
    entry_f = float(entry)
    initial_sl_f = float(initial_sl)
    buffer_f = float(buffer_r)
    normalized_side = str(side or "").lower()
    if normalized_side == "long":
        risk = entry_f - initial_sl_f
        if risk <= 0:
            raise ValueError("LONG SWING requires SL below entry")
        return entry_f - risk * buffer_f
    if normalized_side == "short":
        risk = initial_sl_f - entry_f
        if risk <= 0:
            raise ValueError("SHORT SWING requires SL above entry")
        return entry_f + risk * buffer_f
    raise ValueError(f"unsupported side={normalized_side or 'missing'}")


def _split_swing_target_quantities(
    symbol: str,
    total_qty: float,
    reference_price: Optional[float] = None,
    target_split: Optional[tuple[float, float, float]] = None,
) -> tuple[float, float, float]:
    """Split an exchange-precision position without exceeding its total size.

    The helper is shared by SWING and SCALP partial-exit rules. Keep the
    historical function name for compatibility, but make diagnostics
    style-neutral so a SCALP rejection is not mislabeled as SWING.
    """
    total = float(total_qty)
    if total <= 0:
        raise ValueError("partial-exit position quantity must be positive")

    def precise(value: float) -> float:
        return float(exchange.amount_to_precision(symbol, max(0.0, value)))

    split = target_split or SWING_RULES.target_split
    total_decimal = Decimal(str(total))
    qty1 = precise(float(total_decimal * Decimal(str(split[0]))))
    qty2 = precise(float(total_decimal * Decimal(str(split[1]))))
    # Do the residual arithmetic in decimal space. With binary floats,
    # 113.65 - 45.46 - 34.09 becomes 34.099999..., which BingX truncates to
    # 34.09 and leaves 0.01 open after TP3.
    remaining_decimal = (
        total_decimal - Decimal(str(qty1)) - Decimal(str(qty2))
    )
    qty3 = precise(float(max(Decimal("0"), remaining_decimal)))

    # Defensive correction for exchanges whose formatter rounds instead of
    # truncating. TP quantities must never sum above the current position.
    if qty1 + qty2 + qty3 > total + max(1e-12, total * 1e-10):
        qty3 = precise(max(0.0, total - qty1 - qty2))

    market = exchange.market(symbol)
    min_amount = (((market.get("limits") or {}).get("amount") or {}).get("min"))
    min_amount_f = float(min_amount) if min_amount is not None else 0.0
    quantities = (qty1, qty2, qty3)
    if any(q <= 0 for q in quantities):
        raise ValueError(f"position {total} is too small for three partial targets")
    if min_amount_f > 0 and any(q < min_amount_f for q in quantities):
        raise ValueError(
            f"one of partial target quantities {quantities} is below exchange minimum {min_amount_f}"
        )
    min_cost = (((market.get("limits") or {}).get("cost") or {}).get("min"))
    if min_cost is None:
        min_cost = (market.get("info") or {}).get("tradeMinUSDT")
    min_cost_f = float(min_cost) if min_cost not in (None, "") else 0.0
    reference_price_f = float(reference_price) if reference_price is not None else 0.0
    if min_cost_f > 0 and reference_price_f > 0:
        costs = tuple(q * reference_price_f for q in quantities)
        if any(cost < min_cost_f for cost in costs):
            raise ValueError(
                f"one of partial target notionals {costs} is below exchange minimum {min_cost_f} USDT"
            )
    if sum(quantities) > total + max(1e-12, total * 1e-10):
        raise ValueError(f"partial target quantities {quantities} exceed position {total}")
    return quantities


def _choose_partial_exit_mode(
    symbol: str,
    total_qty: float,
    reference_price: Optional[float] = None,
    target_split: Optional[tuple[float, float, float]] = None,
) -> tuple[str, Optional[tuple[float, float, float]], Optional[str]]:
    """Choose Balanced when executable, otherwise a risk-safe full-TP1 exit.

    Raising the position size to satisfy an exchange minimum would breach the
    configured 0.5% risk budget. A 100% TP1 fallback keeps the original
    position size and always arms both a full-size SL and TP.
    """
    try:
        quantities = _split_swing_target_quantities(
            symbol,
            total_qty,
            reference_price=reference_price,
            target_split=target_split,
        )
    except Exception as split_error:
        return "full_tp1", None, str(split_error)
    return "balanced", quantities, None

def apply_sltp_sync(
    base: str,
    *,
    sl_price=None,
    tp_price=None,
    cancel_first: bool = True,
    position_side: Optional[str] = None,
) -> str:
    base = str(base).upper().strip()

    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol, position_side)
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

async def apply_sltp(
    base: str,
    *,
    sl_price=None,
    tp_price=None,
    cancel_first: bool = True,
    position_side: Optional[str] = None,
) -> str:
    return await asyncio.to_thread(
        apply_sltp_sync,
        base,
        sl_price=sl_price,
        tp_price=tp_price,
        cancel_first=cancel_first,
        position_side=position_side,
    )


def apply_swing_sltp_sync(
    base: str,
    *,
    entry_price: float,
    sl_price: float,
    tp1_price: float,
    tp2_price: float,
    tp3_price: float,
    position_side: str = "long",
    signal_style: str = "SWING",
) -> str:
    """Arm one full SL plus rule-specific 40/30/30 take-profit orders.

    BingX hedge mode rejects ``reduceOnly``. Closing semantics are provided by
    the opposite order side together with the explicit LONG/SHORT
    ``positionSide`` and the partial quantities.
    """
    style = str(signal_style or "").strip().upper()
    exit_rules = _rules_for_signal_style(style)
    if not exit_rules or not getattr(exit_rules, "live_partial_exit_ready", False):
        raise ValueError(f"{style or 'missing'} has no live partial-exit rule")

    base_u = str(base).upper().strip()
    symbol = resolve_symbol_sync(base_u)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base_u}")

    pos = fetch_position_oneway_sync(symbol, position_side)
    if not pos:
        return "NO_POSITION"
    pos_side = _extract_position_side_sync(pos)
    if pos_side != "long":
        raise RuntimeError(f"Live {style} partial-exit model is LONG-only")

    actual_entry = _position_entry_price(pos, entry_price)
    if actual_entry is None:
        raise RuntimeError(f"Cannot determine {style} entry price")

    qty = _extract_position_qty_sync(pos, symbol)
    if qty <= 0:
        return "NO_POSITION"

    prices = []
    for value in (sl_price, tp1_price, tp2_price, tp3_price):
        try:
            prices.append(float(exchange.price_to_precision(symbol, float(value))))
        except Exception:
            prices.append(float(value))
    sl_prec, tp1_prec, tp2_prec, tp3_prec = prices
    if not (sl_prec < actual_entry < tp1_prec < tp2_prec < tp3_prec):
        raise ValueError(
            f"{style} prices must be ordered SL < entry < TP1 < TP2 < TP3; "
            f"got {sl_prec}, {actual_entry}, {tp1_prec}, {tp2_prec}, {tp3_prec}"
        )

    try:
        qty1, qty2, qty3 = _split_swing_target_quantities(
            symbol,
            qty,
            reference_price=actual_entry,
            target_split=exit_rules.target_split,
        )
    except Exception as partial_error:
        log(
            "ERROR",
            f"{style} position cannot support 40/30/30 for {symbol}: {partial_error}; "
            "arming full-size SL + TP1 fallback",
        )
        fallback = apply_sltp_sync(
            base_u,
            sl_price=sl_prec,
            tp_price=tp1_prec,
            cancel_first=True,
            position_side=pos_side,
        )
        return f"FALLBACK_FULL_TP1 after split error={partial_error} | {fallback}"

    be_prec = calculate_swing_breakeven_price(
        actual_entry,
        sl_prec,
        pos_side,
        buffer_r=exit_rules.breakeven_buffer_r,
    )
    try:
        be_prec = float(exchange.price_to_precision(symbol, be_prec))
    except Exception:
        pass

    key = _state_key(base_u, pos_side)
    _cancel_known_order_ids_sync(symbol, key)
    _cancel_existing_sltp_sync(symbol, pos_side)
    time.sleep(0.25)

    placed_ids: dict[str, Any] = {}
    try:
        sl_resp = _place_bingx_tpsl_raw_sync(symbol, pos_side, sl_prec, qty, "sl")
        placed_ids["sl_id"] = _extract_bingx_order_id(sl_resp)
        for label, target_price, target_qty, close_position in (
            ("tp1", tp1_prec, qty1, False),
            ("tp2", tp2_prec, qty2, False),
            ("tp3", tp3_prec, qty3, True),
        ):
            response = _place_bingx_tpsl_raw_sync(
                symbol,
                pos_side,
                target_price,
                target_qty,
                "tp",
                close_position=close_position,
            )
            placed_ids[f"{label}_id"] = _extract_bingx_order_id(response)

        LAST_SLTP.setdefault(base_u, {})
        LAST_SLTP[base_u][pos_side] = {
            "sl": sl_prec,
            "tp": tp1_prec,
            "tp1": tp1_prec,
            "tp2": tp2_prec,
            "tp3": tp3_prec,
            "swing_plan": {
                "version": exit_rules.version,
                "style": style,
                "stage": "armed",
                "entry": actual_entry,
                "initial_sl": sl_prec,
                "be_sl": be_prec,
                "initial_qty": qty,
                "tp1_qty": qty1,
                "tp2_qty": qty2,
                "tp3_qty": qty3,
                "buffer_r": exit_rules.breakeven_buffer_r,
            },
        }
        LAST_ORDER_IDS[key] = placed_ids
        save_sltp()
        save_order_ids()
        log(
            "INFO",
            f"{style} BALANCED EXIT ARMED symbol={symbol} qty={qty} SL={sl_prec} "
            f"TP1={tp1_prec}/{qty1} TP2={tp2_prec}/{qty2} "
            f"TP3={tp3_prec}/{qty3} AFTER_TP1_SL={be_prec}",
        )
        return (
            f"SL={sl_prec}/{qty} | TP1={tp1_prec}/{qty1} | "
            f"TP2={tp2_prec}/{qty2} | TP3={tp3_prec}/{qty3} | "
            f"after TP1 SL={be_prec}"
        )
    except Exception as partial_error:
        for order_id in placed_ids.values():
            if order_id:
                cancel_order_exact_sync(symbol, str(order_id))
        _cancel_existing_sltp_sync(symbol, pos_side)
        _clear_position_state(base_u, pos_side)
        log(
            "ERROR",
            f"{style} partial protection failed for {symbol}: {partial_error}; "
            "arming full-size SL + TP1 fallback",
        )
        fallback = apply_sltp_sync(
            base_u,
            sl_price=sl_prec,
            tp_price=tp1_prec,
            cancel_first=True,
            position_side=pos_side,
        )
        return f"FALLBACK_FULL_TP1 after partial error={partial_error} | {fallback}"


async def apply_swing_sltp(
    base: str,
    *,
    entry_price: float,
    sl_price: float,
    tp1_price: float,
    tp2_price: float,
    tp3_price: float,
    position_side: str = "long",
    signal_style: str = "SWING",
) -> str:
    return await asyncio.to_thread(
        apply_swing_sltp_sync,
        base,
        entry_price=entry_price,
        sl_price=sl_price,
        tp1_price=tp1_price,
        tp2_price=tp2_price,
        tp3_price=tp3_price,
        position_side=position_side,
        signal_style=signal_style,
    )


def _replace_swing_sl_sync(
    base: str,
    position_side: str,
    new_sl_price: float,
    next_stage: str,
) -> str:
    """Replace only the SWING SL; all three target orders stay untouched."""
    base_u = str(base).upper().strip()
    pos_side = str(position_side).lower()
    symbol = resolve_symbol_sync(base_u)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base_u}")
    pos = fetch_position_oneway_sync(
        symbol,
        pos_side,
        raise_on_error=True,
    )
    if not pos:
        return "NO_POSITION"
    qty = _extract_position_qty_sync(pos, symbol)
    if qty <= 0:
        return "NO_POSITION"

    state = (LAST_SLTP.get(base_u) or {}).get(pos_side) or {}
    plan = state.get("swing_plan")
    if not isinstance(plan, dict):
        return "NO_SWING_PLAN"

    key = _state_key(base_u, pos_side)
    saved_ids = LAST_ORDER_IDS.get(key) or {}
    old_sl = state.get("sl") or plan.get("initial_sl")
    known_sl_id = saved_ids.get("sl_id")
    if known_sl_id:
        cancel_order_exact_sync(symbol, str(known_sl_id))

    # Confirm that no stale SL remains before creating its replacement. This
    # avoids two full-quantity stops competing for one hedge leg.
    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception as e:
        raise RuntimeError(f"cannot verify old SWING SL cancellation: {e}") from e
    for order in orders:
        info = order.get("info") or {}
        order_pos_side = str(info.get("positionSide") or "").lower()
        if order_pos_side == pos_side and is_sl_order(order):
            order_id = order.get("id")
            if order_id:
                cancel_order_exact_sync(symbol, str(order_id))

    try:
        try:
            sl_prec = float(exchange.price_to_precision(symbol, float(new_sl_price)))
        except Exception:
            sl_prec = float(new_sl_price)
        response = _place_bingx_tpsl_raw_sync(symbol, pos_side, sl_prec, qty, "sl")
        sl_id = _extract_bingx_order_id(response)
    except Exception as replace_error:
        # Best-effort rollback to the previous protective price.
        try:
            rollback = _place_bingx_tpsl_raw_sync(
                symbol, pos_side, float(old_sl), qty, "sl"
            )
            saved_ids["sl_id"] = _extract_bingx_order_id(rollback)
            LAST_ORDER_IDS[key] = saved_ids
            save_order_ids()
        except Exception as rollback_error:
            raise RuntimeError(
                f"SWING SL replacement failed ({replace_error}); rollback also failed ({rollback_error})"
            ) from replace_error
        raise RuntimeError(
            f"SWING SL replacement failed ({replace_error}); old SL restored"
        ) from replace_error

    state["sl"] = sl_prec
    plan["stage"] = next_stage
    plan["current_qty"] = qty
    plan["sl_updated_at"] = int(time.time())
    state["swing_plan"] = plan
    LAST_SLTP[base_u][pos_side] = state
    saved_ids["sl_id"] = sl_id
    LAST_ORDER_IDS[key] = saved_ids
    save_sltp()
    save_order_ids()
    log(
        "INFO",
        f"SWING SL REPLACED symbol={symbol} stage={next_stage} qty={qty} sl={sl_prec} id={sl_id}",
    )
    return f"SL={sl_prec}/{qty} stage={next_stage}"


def _swing_qty_reached(current_qty: float, initial_qty: float, closed_qty: float) -> bool:
    tolerance = max(1e-12, float(initial_qty) * 1e-6, float(closed_qty) * 0.02)
    return float(current_qty) <= float(initial_qty) - float(closed_qty) + tolerance


def _position_mark_price(pos: dict) -> Optional[float]:
    info = pos.get("info") or {}
    for value in (
        pos.get("markPrice"), pos.get("mark_price"),
        info.get("markPrice"), info.get("mark_price"),
    ):
        try:
            if value is not None and float(value) > 0:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _final_target_reached(pos_side: str, mark_price: Optional[float], tp3: float) -> bool:
    if mark_price is None:
        return False
    if str(pos_side).lower() == "long":
        return float(mark_price) >= float(tp3)
    if str(pos_side).lower() == "short":
        return float(mark_price) <= float(tp3)
    return False


def advance_swing_exit_state_sync(base: str, position_side: str = "long") -> str:
    """Observe actual position reductions and advance the live SWING stop."""
    base_u = str(base).upper().strip()
    pos_side = str(position_side).lower()
    state = (LAST_SLTP.get(base_u) or {}).get(pos_side) or {}
    plan = state.get("swing_plan")
    if not isinstance(plan, dict):
        return "NO_SWING_PLAN"
    symbol = resolve_symbol_sync(base_u)
    if not symbol:
        return "NO_SYMBOL"
    position_key = _state_key(base_u, pos_side)
    try:
        pos = fetch_position_oneway_sync(
            symbol,
            pos_side,
            raise_on_error=True,
        )
    except PositionLookupError:
        # An API error is not evidence that the position is closed. Reset any
        # previous empty-result streak and leave every protective order intact.
        POSITION_MISSING_COUNTS.pop(position_key, None)
        raise
    if not pos:
        missing_count = POSITION_MISSING_COUNTS.get(position_key, 0) + 1
        POSITION_MISSING_COUNTS[position_key] = missing_count
        if missing_count < POSITION_MISSING_CONFIRMATIONS_REQUIRED:
            return (
                "POSITION_MISSING_UNCONFIRMED "
                f"{missing_count}/{POSITION_MISSING_CONFIRMATIONS_REQUIRED}"
            )

        # Only a repeated sequence of successful, empty BingX responses proves
        # that the position is gone. At that point orphan SL/TP orders and local
        # state can be removed safely.
        cancel_all_open_orders_for_symbol_sync(symbol, pos_side)
        _clear_position_state(base_u, pos_side)
        return "POSITION_CLOSED_CONFIRMED"

    POSITION_MISSING_COUNTS.pop(position_key, None)

    current_qty = _extract_position_qty_sync(pos, symbol)
    initial_qty = float(plan["initial_qty"])
    tp1_qty = float(plan["tp1_qty"])
    tp2_qty = float(plan["tp2_qty"])
    stage = str(plan.get("stage") or "armed")
    messages = []

    # Exchange-side closePosition is the first line of defence. This watcher
    # is the independent backstop for precision dust or a partially executed
    # TP3: once MARK_PRICE has reached the final target, no remainder is
    # allowed to stay open.
    tp3_price = float(state.get("tp3") or plan.get("tp3") or 0)
    mark_price = _position_mark_price(pos)
    if tp3_price > 0 and _final_target_reached(pos_side, mark_price, tp3_price):
        close_result = close_position_full_sync(base_u, pos_side)
        return (
            f"TP3_REMAINDER_FORCE_CLOSED mark={mark_price:g} target={tp3_price:g} "
            f"qty={current_qty:g} result={close_result}"
        )

    if stage == "armed" and _swing_qty_reached(current_qty, initial_qty, tp1_qty):
        next_stage = (
            "tp2_done"
            if _swing_qty_reached(current_qty, initial_qty, tp1_qty + tp2_qty)
            else "tp1_done"
        )
        target_floor = float(plan["be_sl"])
        target_sl = max(target_floor, float(state.get("sl") or target_floor))
        messages.append(
            _replace_swing_sl_sync(
                base_u,
                pos_side,
                target_sl,
                next_stage,
            )
        )
        state = (LAST_SLTP.get(base_u) or {}).get(pos_side) or {}
        plan = state.get("swing_plan") or plan
        stage = str(plan.get("stage") or next_stage)
        pos = fetch_position_oneway_sync(
            symbol,
            pos_side,
            raise_on_error=True,
        )
        if not pos:
            # Do not clean up after a single empty response, even during the
            # short transition immediately after replacing the SL. The next
            # watcher cycles perform the normal three-step confirmation.
            return " | ".join(messages + ["POSITION_MISSING_UNCONFIRMED"])
        current_qty = _extract_position_qty_sync(pos, symbol)

    if stage == "tp1_done" and _swing_qty_reached(
        current_qty, initial_qty, tp1_qty + tp2_qty
    ):
        # Keep the same Entry - 0.08R price; only resize the protective
        # quantity to the final 30% after TP2.
        messages.append(
            _replace_swing_sl_sync(
                base_u,
                pos_side,
                max(
                    float(plan["be_sl"]),
                    float(state.get("sl") or plan["be_sl"]),
                ),
                "tp2_done",
            )
        )

    return " | ".join(messages) if messages else "NO_CHANGE"


async def swing_exit_watcher_loop():
    while True:
        await asyncio.sleep(max(1.0, SWING_EXIT_WATCH_SEC))
        plans = [
            (base, side)
            for base, sides in list(LAST_SLTP.items())
            for side, state in list((sides or {}).items())
            if isinstance((state or {}).get("swing_plan"), dict)
        ]
        for base, side in plans:
            try:
                plan = (
                    ((LAST_SLTP.get(base) or {}).get(side) or {}).get("swing_plan")
                    or {}
                )
                style = str(plan.get("style") or "SWING").upper()
                buffer_r = float(
                    plan.get("buffer_r", SWING_RULES.breakeven_buffer_r)
                )
                result = await asyncio.to_thread(
                    advance_swing_exit_state_sync, base, side
                )
                if result not in {"NO_CHANGE", "NO_SWING_PLAN"}:
                    log("INFO", f"{style} PARTIAL WATCH {base}/{side}: {result}")
                    if result.startswith("TP3_REMAINDER_FORCE_CLOSED"):
                        await _send_to_tg(
                            f"✅ {base}/USDT TP3 досягнуто. "
                            "Увесь біржовий залишок позиції закрито."
                        )
                    elif "stage=tp2_done" in result:
                        await _send_to_tg(
                            f"🎯 {base}/USDT TP2 виконано. "
                            f"SL останніх 30% залишається на Entry − {buffer_r:.2f}R."
                        )
                    elif "stage=tp1_done" in result:
                        await _send_to_tg(
                            f"🎯 {base}/USDT TP1 виконано. "
                            f"SL залишку перенесено на Entry − {buffer_r:.2f}R."
                        )
            except Exception as e:
                log("ERROR", f"PARTIAL WATCH {base}/{side} failed: {e}")

def reapply_saved_sltp_sync(base: str, position_side: Optional[str] = None) -> str:
    base = str(base).upper().strip()

    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol, position_side)
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

    swing_plan = saved.get("swing_plan")
    if isinstance(swing_plan, dict):
        if str(swing_plan.get("stage") or "armed") != "armed":
            return "SWING_REAPPLY_BLOCKED_AFTER_TP1"
        return apply_swing_sltp_sync(
            base,
            entry_price=float(swing_plan["entry"]),
            sl_price=float(swing_plan["initial_sl"]),
            tp1_price=float(saved["tp1"]),
            tp2_price=float(saved["tp2"]),
            tp3_price=float(saved["tp3"]),
            position_side=pos_side,
            signal_style=str(swing_plan.get("style") or "SWING"),
        )

    return apply_sltp_sync(base, sl_price=sl, tp_price=tp, cancel_first=True, position_side=pos_side)

async def reapply_saved_sltp(base: str, position_side: Optional[str] = None) -> str:
    return await asyncio.to_thread(reapply_saved_sltp_sync, base, position_side)

def set_sl_oneway_sync(base: str, sl_price: float, position_side: Optional[str] = None) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol, position_side)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    saved = _load_saved_sltp_for_position(str(base).upper(), pos_side)
    swing_plan = saved.get("swing_plan")
    if isinstance(swing_plan, dict):
        return _replace_swing_sl_sync(
            base,
            pos_side,
            sl_price,
            str(swing_plan.get("stage") or "armed"),
        )
    tp_saved = saved.get("tp")

    return apply_sltp_sync(base, sl_price=sl_price, tp_price=tp_saved, cancel_first=True, position_side=pos_side)

async def set_sl_oneway(base: str, sl_price: float, position_side: Optional[str] = None) -> str:
    return await asyncio.to_thread(set_sl_oneway_sync, base, sl_price, position_side)

def set_tp_oneway_sync(base: str, tp_price: float, position_side: Optional[str] = None) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol, position_side)
    if not pos:
        return "NO_POSITION"

    pos_side = _extract_position_side_sync(pos)
    saved = _load_saved_sltp_for_position(str(base).upper(), pos_side)
    if isinstance(saved.get("swing_plan"), dict):
        return "SWING_TP_EDIT_REQUIRES_TP1_TP2_TP3"
    sl_saved = saved.get("sl")

    return apply_sltp_sync(base, sl_price=sl_saved, tp_price=tp_price, cancel_first=True, position_side=pos_side)

async def set_tp_oneway(base: str, tp_price: float, position_side: Optional[str] = None) -> str:
    return await asyncio.to_thread(set_tp_oneway_sync, base, tp_price, position_side)

def breakeven_oneway_sync(base: str, position_side: Optional[str] = None) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol, position_side)
    if not pos:
        return "NO_POSITION"

    entry = pos.get("entryPrice") or pos.get("average") or pos.get("avgPrice")
    if entry is None:
        return "NO_ENTRY_PRICE"

    pos_side = _extract_position_side_sync(pos)
    saved = _load_saved_sltp_for_position(str(base).upper(), pos_side)
    swing_plan = saved.get("swing_plan")
    if isinstance(swing_plan, dict):
        return _replace_swing_sl_sync(
            base,
            pos_side,
            float(entry),
            str(swing_plan.get("stage") or "armed"),
        )
    tp_saved = saved.get("tp")

    return apply_sltp_sync(base, sl_price=float(entry), tp_price=tp_saved, cancel_first=True, position_side=pos_side)

async def breakeven_oneway(base: str, position_side: Optional[str] = None) -> str:
    return await asyncio.to_thread(breakeven_oneway_sync, base, position_side)

def add_position_oneway_sync(base: str, add_pct: Optional[float], position_side: Optional[str] = None) -> str:
    symbol = resolve_symbol_sync(base)
    if not symbol:
        raise RuntimeError(f"Symbol not found: {base}")

    pos = fetch_position_oneway_sync(symbol, position_side)
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

async def add_position_oneway(base: str, add_pct: Optional[float], position_side: Optional[str] = None) -> str:
    return await asyncio.to_thread(add_position_oneway_sync, base, add_pct, position_side)


async def wait_position_update(
    symbol: str,
    old_size: float = 0.0,
    timeout: float = 5.0,
    min_target_size: Optional[float] = None,
    position_side: Optional[str] = None,
):
    start = time.time()
    last_pos = None

    while time.time() - start < timeout:
        pos = await fetch_position_oneway(symbol, position_side)
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

def calculate_auto_trade_plan(
    capital_usdt: float,
    entry: float,
    sl: float,
    tp: float,
    *,
    risk_pct: float = FIXED_RISK_PCT,
    max_leverage: int = MAX_AUTO_LEVERAGE,
    distance_buffer: float = LEVERAGE_DISTANCE_BUFFER,
    max_margin_usage_pct: float = MAX_MARGIN_USAGE_PCT,
) -> dict:
    """Size by loss at SL and choose conservative leverage from SL/TP span."""
    capital = float(capital_usdt)
    entry = float(entry)
    sl = float(sl)
    tp = float(tp)
    if capital <= 0 or entry <= 0 or risk_pct <= 0:
        raise ValueError("capital, entry and risk_pct must be positive")

    stop_distance_pct = abs(entry - sl) / entry
    tp_distance_pct = abs(tp - entry) / entry
    if stop_distance_pct <= 0 or tp_distance_pct <= 0:
        raise ValueError("SL and TP1 must differ from entry")

    protected_distance = max(stop_distance_pct, tp_distance_pct)
    raw_leverage = math.floor(1.0 / (protected_distance * max(distance_buffer, 1.0)))
    leverage = max(1, min(int(max_leverage), raw_leverage))

    risk_budget = capital * (float(risk_pct) / 100.0)
    risk_sized_notional = risk_budget / stop_distance_pct
    max_margin = capital * (float(max_margin_usage_pct) / 100.0)
    margin_capped_notional = max_margin * leverage
    notional = min(risk_sized_notional, margin_capped_notional)

    return {
        "risk_pct": float(risk_pct),
        "risk_budget": risk_budget,
        "stop_distance_pct": stop_distance_pct * 100.0,
        "tp_distance_pct": tp_distance_pct * 100.0,
        "leverage": leverage,
        "notional": notional,
        "margin": notional / leverage,
        "expected_loss_at_sl": notional * stop_distance_pct,
        "expected_profit_at_tp1": notional * tp_distance_pct,
        "margin_limited": notional < risk_sized_notional,
    }


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


def calculate_rr_from_prices(entry: float, sl: float, tp: float, side: str) -> float:
    """Calculate the effective RR that will actually be sent to the exchange."""
    entry = float(entry)
    sl = float(sl)
    tp = float(tp)
    normalized_side = str(side or "").strip().lower()

    if normalized_side == "long":
        risk = entry - sl
        reward = tp - entry
    elif normalized_side == "short":
        risk = sl - entry
        reward = entry - tp
    else:
        raise ValueError(f"unsupported side={normalized_side or 'missing'}")

    if risk <= 0 or reward <= 0:
        raise ValueError("entry/SL/TP1 do not form a valid positive-RR setup")
    return reward / risk


def source_signal_rr1(command: dict, side: str) -> Optional[float]:
    """Return the RR1 used by the signal-level statistical rule.

    Prefer the RR printed by SignalBot because its database keeps the original
    unrounded levels while the Telegram card contains rounded prices. Falling
    back to the card's Entry/SL/TP1 keeps older generated messages usable.
    Live fill RR is logged separately and must not silently change membership
    in the historical RR1 0.8-0.99 cohort.
    """
    declared_rr1 = command.get("signal_rr1")
    if declared_rr1 is not None:
        value = float(declared_rr1)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"invalid signal RR1={declared_rr1!r}")
        return value

    signal_entry = command.get("entry")
    signal_sl = command.get("sl")
    signal_tp1 = command.get("tp")
    if signal_entry is None or signal_sl is None or signal_tp1 is None:
        return None
    return calculate_rr_from_prices(signal_entry, signal_sl, signal_tp1, side)


def swing_entry_drift_block_reason(
    side: str,
    signal_entry: float,
    market_entry: float,
    planned_sl: float,
    *,
    max_adverse_drift_r: float = SWING_MAX_ENTRY_DRIFT_R,
) -> Optional[str]:
    """Return a reason when a delayed SWING entry has materially worsened."""
    normalized_side = str(side or "").strip().lower()
    signal_entry_f = float(signal_entry)
    market_entry_f = float(market_entry)
    planned_sl_f = float(planned_sl)

    if normalized_side == "long":
        planned_risk = signal_entry_f - planned_sl_f
        adverse_drift = market_entry_f - signal_entry_f
    elif normalized_side == "short":
        planned_risk = planned_sl_f - signal_entry_f
        adverse_drift = signal_entry_f - market_entry_f
    else:
        return f"unsupported side={normalized_side or 'missing'}"

    if planned_risk <= 0:
        return "signal Entry and SL do not define positive planned risk"

    drift_r = max(0.0, adverse_drift) / planned_risk
    if drift_r > float(max_adverse_drift_r):
        return (
            f"stale SWING entry: market={market_entry_f:g}, signal={signal_entry_f:g}, "
            f"adverse drift={drift_r:.3f}R exceeds {float(max_adverse_drift_r):g}R"
        )
    return None


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
    r"\bзакрий\b",
    r"\bзакрити\b",
    r"\bзакрой\b",
    r"\bзакрыть\b",
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

def extract_signal_style(text: str) -> Optional[str]:
    """Return a style only for an actual signal header, not a stats mention."""
    header = "\n".join((text or "").splitlines()[:3])
    match = re.search(r"\b(SCALP|INTRADAY|SWING)\b[^\n]*\b(LONG|SHORT)\b", header, re.I)
    return match.group(1).upper() if match else None

def is_allowed_signal_style(text: str) -> bool:
    style = extract_signal_style(text)
    return style is None or style in ALLOWED_SIGNAL_STYLES


def is_new_entry_signal_text(text: str) -> bool:
    """Recognize a full signal card without blocking later management events."""
    if extract_signal_style(text) is None:
        return False
    normalized = text or ""
    return all(
        re.search(pattern, normalized, re.I)
        for pattern in (
            r"(?:📍\s*)?Entry\s*:",
            r"(?:🛑\s*)?SL\s*:",
            r"(?:🎯\s*)?TP1\s*:",
        )
    )


CONTROL_PREFIX_RE = re.compile(
    r"^\s*/(?:tb|tradebot)(?:@[A-Z0-9_]+)?(?:\s+|\s*:\s*)",
    re.I,
)


def normalize_source_text(chat_id: int, text: str) -> Optional[str]:
    """Gate Saved Messages without exposing the executor to arbitrary notes.

    SignalBot+ remains the automatic source. Saved Messages accepts either a
    complete generated signal card or an explicit /tb (or /tradebot) command.
    This preserves a manual recovery path without re-enabling automatic private
    chat duplicates.
    """
    raw = (text or "").strip()
    try:
        source_id = int(chat_id)
    except (TypeError, ValueError):
        return None

    if source_id == TARGET_CHAT_ID:
        return raw
    if source_id != CONTROL_CHAT_ID:
        return None

    prefix = CONTROL_PREFIX_RE.match(raw)
    if prefix:
        command = raw[prefix.end():].strip()
        return command or None

    if is_new_entry_signal_text(raw):
        return raw

    return None


def is_entry_time_allowed(now: Optional[datetime] = None) -> bool:
    """Return whether a new entry may be opened under the Kyiv-time sleep rule."""
    current = now or datetime.now(KYIV_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=KYIV_TZ)
    else:
        current = current.astimezone(KYIV_TZ)

    start = ENTRY_BLOCK_START_HOUR_KYIV % 24
    end = ENTRY_BLOCK_END_HOUR_KYIV % 24
    hour = current.hour

    if start == end:
        return True
    if start < end:
        return not (start <= hour < end)
    return not (hour >= start or hour < end)


def source_stop_distance_pct(cmd: dict, side: str) -> Optional[float]:
    """Return stop distance from the declared signal levels, in percent."""
    if cmd.get("entry") is None or cmd.get("sl") is None:
        return None
    entry = float(cmd["entry"])
    sl = float(cmd["sl"])
    if entry <= 0:
        raise ValueError("signal entry must be positive")
    normalized_side = str(side or "").lower()
    distance = entry - sl if normalized_side == "long" else sl - entry
    if distance <= 0:
        raise ValueError("signal SL is on the wrong side of entry")
    return distance / entry * 100.0


def open_policy_block_reason(
    side: Optional[str],
    now: Optional[datetime] = None,
    style: Optional[str] = None,
    rr1: Optional[float] = None,
    stop_distance_pct: Optional[float] = None,
    *,
    require_allowed_style: bool = False,
) -> Optional[str]:
    normalized_side = str(side or "").strip().lower()
    normalized_style = str(style or "").strip().upper()
    if TRADE_LONG_ONLY and normalized_side != "long":
        return f"side={normalized_side or 'missing'}; policy allows LONG only"
    if require_allowed_style and normalized_style not in ALLOWED_SIGNAL_STYLES:
        return (
            f"style={normalized_style or 'missing'}; policy allows "
            f"{', '.join(sorted(ALLOWED_SIGNAL_STYLES))} only"
        )
    if not is_entry_time_allowed(now):
        swing_long_exception = (
            normalized_style in SWING_RULES.allowed_styles
            and normalized_side == SWING_RULES.allowed_side
            and SWING_RULES.allow_kyiv_00_06
        )
        if not swing_long_exception:
            current = (now or datetime.now(KYIV_TZ))
            if current.tzinfo is None:
                current = current.replace(tzinfo=KYIV_TZ)
            else:
                current = current.astimezone(KYIV_TZ)
            return (
                f"Kyiv time {current:%Y-%m-%d %H:%M:%S}; entries are blocked "
                f"{ENTRY_BLOCK_START_HOUR_KYIV:02d}:00-{ENTRY_BLOCK_END_HOUR_KYIV:02d}:00"
            )

    if rr1 is not None:
        try:
            effective_rr1 = float(rr1)
        except (TypeError, ValueError):
            return f"invalid effective TP1 RR={rr1!r}"
        if not math.isfinite(effective_rr1) or effective_rr1 <= 0:
            return f"invalid effective TP1 RR={effective_rr1}"
    if stop_distance_pct is not None and float(stop_distance_pct) <= 0:
        return "source SL distance must be positive"
    return None

def _parse_message_number_token(raw: str) -> float:
    """Parse decimal values while preserving Telegram thousands separators.

    SignalBot formats large prices as ``77,277.00``. The old parser captured
    only ``77,277`` and interpreted it as ``77.277``, which made otherwise
    valid BTC/ETH protection levels fail the live-price safety check.
    """
    token = str(raw or "").strip().replace(" ", "")
    if not token:
        raise ValueError("empty numeric token")

    if "," in token and "." in token:
        decimal_separator = "." if token.rfind(".") > token.rfind(",") else ","
        thousands_separator = "," if decimal_separator == "." else "."
        token = token.replace(thousands_separator, "")
        if decimal_separator == ",":
            token = token.replace(",", ".")
    elif token.count(",") > 1:
        groups = token.split(",")
        if all(len(group) == 3 for group in groups[1:]):
            token = "".join(groups)
        else:
            token = "".join(groups[:-1]) + "." + groups[-1]
    elif token.count(".") > 1:
        groups = token.split(".")
        if all(len(group) == 3 for group in groups[1:]):
            token = "".join(groups)
        else:
            token = "".join(groups[:-1]) + "." + groups[-1]
    else:
        # A single comma remains supported as the decimal separator used by
        # manually forwarded Ukrainian/European-style messages.
        token = token.replace(",", ".")

    value = float(token)
    if not math.isfinite(value):
        raise ValueError(f"non-finite numeric token={raw!r}")
    return value


def extract_liquidation_from_text(text: str) -> Optional[float]:
    match = re.search(
        r"(?:liquidation(?:\s+price)?|liq(?:uid)?|ліквід\w*|ликвид\w*)\s*[:=]\s*([0-9][0-9.,]*)",
        text or "",
        re.I,
    )
    if not match:
        return None
    try:
        value = _parse_message_number_token(match.group(1))
        return value if value > 0 else None
    except ValueError:
        return None

def _extract_message_number(text: str, pattern: str) -> Optional[float]:
    match = re.search(pattern, text or "", re.I | re.M)
    if not match:
        return None
    try:
        return _parse_message_number_token(match.group(1))
    except (TypeError, ValueError):
        return None

def parse_structured_signal(text: str) -> Optional[dict]:
    """Deterministic parser for generated SCALP/INTRADAY/SWING messages."""
    style = extract_signal_style(text)
    if style not in {"SCALP", "INTRADAY", "SWING"}:
        return None

    header = re.search(
        rf"\b{style}\s+(LONG|SHORT)\b[^\n]*?([A-Z0-9]{{1,15}})\s*/\s*USDT(?::USDT)?",
        text or "",
        re.I,
    )
    if not header:
        return None

    entry = _extract_message_number(text, r"^\s*[^\n]*\bEntry\s*:\s*([0-9][0-9.,]*)")
    sl = _extract_message_number(text, r"^\s*[^\n]*\bSL\s*:\s*([0-9][0-9.,]*)")
    tp1 = _extract_message_number(text, r"^\s*[^\n]*\bTP1\s*:\s*([0-9][0-9.,]*)")
    tp2 = _extract_message_number(text, r"^\s*[^\n]*\bTP2\s*:\s*([0-9][0-9.,]*)")
    tp3 = _extract_message_number(text, r"^\s*[^\n]*\bTP3\s*:\s*([0-9][0-9.,]*)")
    signal_rr1 = _extract_message_number(
        text,
        r"^\s*[^\n]*\bTP1\s*:.*?\bRR\s*[:=]?\s*([0-9][0-9.,]*)",
    )
    leverage = _extract_message_number(text, r"(?:Плече|Leverage)\s*:\s*([0-9][0-9.,]*)\s*x")
    position_usdt = _extract_message_number(text, r"(?:Позиція|Позиция|Position)\s*:\s*([0-9][0-9.,]*)\s*USDT")
    margin_usdt = _extract_message_number(text, r"(?:маржа|margin)\s*:\s*([0-9][0-9.,]*)\s*USDT")
    balance_usdt = _extract_message_number(text, r"(?:Баланс|Balance)\s*:\s*([0-9][0-9.,]*)\s*USDT")
    risk_pct = _extract_message_number(
        text,
        r"(?:ризик|риск|risk)\s*:\s*[0-9][0-9.,]*\s*USDT\s*\(([0-9][0-9.,]*)\s*%\)",
    )

    if sl is None or tp1 is None:
        return None

    # Preserve all generated targets; the selected strategy decides execution.
    style_rules = _rules_for_signal_style(style)
    uses_partial_targets = style in {"SCALP", "INTRADAY"} or bool(
        style_rules and getattr(style_rules, "live_partial_exit_ready", False)
    )

    return {
        "action": "OPEN",
        "base": header.group(2).upper(),
        "bases": None,
        "side": header.group(1).lower(),
        "leverage": int(leverage) if leverage is not None else None,
        "risk_pct": risk_pct,
        "entry": entry,
        "sl": sl,
        "tp": tp1,
        "signal_rr1": signal_rr1,
        # Preserve all declared levels for audit and management. The active
        # strategy decides whether TP2/TP3 are actually sent to the exchange.
        "tp2": tp2 if uses_partial_targets else None,
        "tp3": tp3 if uses_partial_targets else None,
        "position_usdt": position_usdt,
        "margin_usdt": margin_usdt,
        "balance_usdt": balance_usdt,
        "liquidation": extract_liquidation_from_text(text),
        "confidence": 1.0,
        "raw_text": (text or "")[:500],
    }


def parse_structured_scalp_signal(text: str) -> Optional[dict]:
    """Backward-compatible alias retained for older tests and callers."""
    if extract_signal_style(text) != "SCALP":
        return None
    return parse_structured_signal(text)


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

SPECIAL_BASES = {
    "1000PEPE": "1000PEPE",
    "1000BONK": "1000BONK",
    "1000SHIB": "1000SHIB",
    "1000FLOKI": "1000FLOKI",
}

KNOWN_BASES = [
    "1000PEPE", "1000BONK", "1000SHIB", "1000FLOKI",
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "BNB",
    "HYPE", "ONDO", "LINK", "AVAX", "PARTI", "PEPE",
]

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


def parse_manual_control_command(text: str) -> Optional[dict]:
    """Parse a small, explicit Saved Messages command language without AI."""
    raw = (text or "").strip()
    if not raw:
        return None

    base_pattern = r"#?([A-Z0-9]{1,15})(?:\s*/\s*USDT(?::USDT)?)?"
    side_pattern = r"(?:\s+(LONG|SHORT))?"

    close_match = re.fullmatch(
        rf"(?:close(?:\s+now)?|закрий|закрити|закрой|закрыть)\s+{base_pattern}{side_pattern}",
        raw,
        re.I,
    )
    if close_match:
        base = _normalize_base_word(close_match.group(1))
        if base:
            return {
                "action": "CLOSE",
                "base": base,
                "bases": None,
                "side": (close_match.group(2) or "").lower() or None,
                "confidence": 1.0,
                "raw_text": raw[:500],
            }

    be_match = re.fullmatch(
        rf"(?:be|breakeven|break\s+even|беззбиток|безубыток)\s+{base_pattern}{side_pattern}",
        raw,
        re.I,
    )
    if be_match:
        base = _normalize_base_word(be_match.group(1))
        if base:
            return {
                "action": "BE",
                "base": base,
                "side": (be_match.group(2) or "").lower() or None,
                "confidence": 1.0,
                "raw_text": raw[:500],
            }

    price_match = re.fullmatch(
        rf"(sl|stop(?:\s+loss)?|стоп|tp|take\s+profit|тейк)\s+"
        rf"{base_pattern}\s+([0-9]+(?:[.,][0-9]+)?){side_pattern}",
        raw,
        re.I,
    )
    if price_match:
        token = price_match.group(1).lower()
        base = _normalize_base_word(price_match.group(2))
        if base:
            is_sl = token in {"sl", "stop", "stop loss", "стоп"}
            price = float(price_match.group(3).replace(",", "."))
            return {
                "action": "SET_SL" if is_sl else "SET_TP",
                "base": base,
                "side": (price_match.group(4) or "").lower() or None,
                "sl": price if is_sl else None,
                "tp": None if is_sl else price,
                "confidence": 1.0,
                "raw_text": raw[:500],
            }

    return None


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
- BASE: extract ticker and remove USDT, but preserve indexed token prefixes such as 1000PEPE, 1000BONK, 1000SHIB, 1000FLOKI. Example: #ETHUSDT -> ETH; 1000PEPEUSDT -> 1000PEPE
- SIDE: long or short
- LEVERAGE: parse X10 / 10x / leverage 10
- RISK_PCT: parse phrases like "1.5% balance", "risk 2%", "margin 0.75%"
- ADD_PCT: for ADD signals parse the percentage being added now
- SL: number after SL / stop loss
- TP: always put TP1 in "tp"
- For SCALP and SWING OPEN signals, also extract TP2 into "tp2" and TP3 into "tp3"
- For INTRADAY, set "tp2" and "tp3" to null
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
- LIQUIDATION: extract the estimated liquidation price from fields such as "Liquidation", "Liq" or "Орієнт. ліквід"

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
  "tp2": null,
  "tp3": null,
  "add_pct": 0.75,
  "confidence": 0.92,
  "raw_text": "string|null",
  "rr": 2.5,
  "dca_price": 123.0,
  "dca_pct": 0.75,
  "price": 123.0,
  "liquidation": 100.0,
  "position_usdt": 250.0,
  "margin_usdt": 50.0,
  "balance_usdt": 1000.0
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
    "tp2": "number|null",
    "tp3": "number|null",
    "add_pct": "number|null",
    "confidence": "0..1",
    "raw_text": "string|null",
    "rr": "number|null",
    "dca_price": "number|null",
    "dca_pct": "number|null",
    "price": "number|null", 
    "liquidation": "number|null",
    "position_usdt": "number|null",
    "margin_usdt": "number|null",
    "balance_usdt": "number|null",
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

def parse_trade_multi(text: Optional[str], image_paths: Optional[list[str]]) -> dict:
    manual = parse_manual_control_command(text or "")
    if manual:
        log("INFO", f"LOCAL CONTROL parsed action={manual['action']} base={manual.get('base')}")
        return manual

    structured = parse_structured_signal(text or "")
    if structured:
        style = extract_signal_style(text or "")
        style_rules = _rules_for_signal_style(style)
        target_mode = (
            "TP1/TP2/TP3 partial"
            if style_rules and getattr(style_rules, "live_partial_exit_ready", False)
            else "TP1 only"
        )
        log(
            "INFO",
            f"STRUCTURED {style} parsed locally; execution target={target_mode}",
        )
        return structured
    return ai_parse_trade_multi(text, image_paths)

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
    b = str(x or "").upper().strip()
    b = re.sub(r"[^A-Z0-9]", "", b)
    if b.endswith("USDT"):
        b = b[:-4]
    b = TOKEN_ALIASES.get(b, b)
    b = SPECIAL_BASES.get(b, b)
    if not b:
        return ""
    if b in BAD_BASE_WORDS:
        return ""
    if len(b) > 12:
        return ""
    return b


def extract_multiple_bases(text: str) -> list[str]:
    t = str(text or "").upper()
    found: list[str] = []

    for b in KNOWN_BASES:
        pattern = rf"(?<![A-Z0-9]){re.escape(b)}(?:USDT)?(?![A-Z0-9])"
        if re.search(pattern, t):
            clean = _clean_base(b)
            if clean and clean not in found:
                found.append(clean)

    return found


def _clean_base_from_context(base_value: str, text: str = "") -> str:
    t = str(text or "").upper()
    for special in SPECIAL_BASES:
        pattern = rf"(?<![A-Z0-9]){re.escape(special)}(?:USDT)?(?![A-Z0-9])"
        if re.search(pattern, t):
            return SPECIAL_BASES[special]
    return _clean_base(base_value)


def _log_action_result(action_name: str, base: str, res: str) -> bool:
    if res == "NO_POSITION":
        log("WARNING", f"SKIP {action_name} {base}: NO_POSITION")
        return False
    if res in {"NO_ENTRY_PRICE", "NOTHING_TO_APPLY", "NO_SAVED_SLTP"}:
        log("WARNING", f"SKIP {action_name} {base}: {res}")
        return False
    log("INFO", f"SUCCESS {action_name} {base}: {res}")
    return True

def _format_open_notification(
    *,
    base: str,
    side: str,
    style: Optional[str],
    entry: float,
    sl: float,
    tp: float,
    tp2: Optional[float] = None,
    tp3: Optional[float] = None,
    swing_partial: bool = False,
    qty: float,
    leverage: int,
    liquidation: Optional[float],
    risk_usdt: Optional[float] = None,
    expected_tp1_profit: Optional[float] = None,
    margin_usdt: Optional[float] = None,
    breakeven_buffer_r: Optional[float] = None,
) -> str:
    lines = [
        f"✅ OPENED {style or 'SIGNAL'} · {base}/USDT",
        f"Side: {side.upper()}",
        f"Entry: {entry}",
        f"SL: {sl}",
        f"TP1 ({'40%' if swing_partial else '100%'} position): {tp}",
        f"Qty: {qty}",
        f"Leverage: {leverage}x",
        f"Liquidation: {liquidation if liquidation is not None else 'n/a'}",
    ]
    if swing_partial:
        lines.insert(5, f"TP2 (30% position): {tp2}")
        lines.insert(6, f"TP3 (30% position): {tp3}")
        lines.insert(
            7,
            f"After TP1: SL → Entry − {float(breakeven_buffer_r or 0.0):.2f}R",
        )
    if risk_usdt is not None:
        lines.append(f"Risk to SL: {risk_usdt:.4f} USDT ({FIXED_RISK_PCT}%)")
    if expected_tp1_profit is not None:
        lines.append(f"Expected TP1 profit: {expected_tp1_profit:.4f} USDT")
    if margin_usdt is not None:
        lines.append(f"Estimated margin: {margin_usdt:.4f} USDT")
    return "\n".join(lines)


def _register_s1_shadow_if_eligible(cmd: dict) -> bool:
    """Record the disabled S1 policy as a balanced shadow position."""
    text = str(cmd.get("_tg_text") or "")
    style = str(cmd.get("_signal_style") or "").upper()
    side = str(cmd.get("side") or "").lower()
    try:
        rr1 = source_signal_rr1(cmd, side)
    except (TypeError, ValueError):
        return False
    if not qualifies_s1(
        style=style,
        side=side,
        rr1=rr1,
        now_kyiv=datetime.now(KYIV_TZ),
    ):
        return False
    if any(cmd.get(field) is None for field in ("entry", "sl", "tp", "tp2", "tp3")):
        log("WARNING", "S1 SHADOW SKIP: Entry/SL/TP1/TP2/TP3 missing")
        return False
    base = _clean_base_from_context(cmd.get("base"), text)
    if not base or non_crypto_open_block_reason(base, signal_text=text, style=style):
        log("WARNING", f"S1 SHADOW SKIP {base or 'unknown'}: non-crypto or missing base")
        return False
    signal_key = str(cmd.get("_signal_key") or cmd.get("_signal_content_hash") or "")
    if not signal_key:
        return False
    try:
        added = SHADOW_S1.register(
            signal_key=signal_key,
            base=base,
            entry=float(cmd["entry"]),
            sl=float(cmd["sl"]),
            tp1=float(cmd["tp"]),
            tp2=float(cmd["tp2"]),
            tp3=float(cmd["tp3"]),
        )
    except (TypeError, ValueError, OSError) as exc:
        log("WARNING", f"S1 SHADOW SKIP {base}: {exc}")
        return False
    if added:
        log(
            "INFO",
            f"S1 SHADOW OPEN {base} RR1={rr1:.4f} entry={cmd['entry']} "
            f"sl={cmd['sl']} tp1={cmd['tp']} tp2={cmd['tp2']} tp3={cmd['tp3']}",
        )
    return added


def _register_s2_shadow_if_eligible(cmd: dict) -> bool:
    """Record S2 independently; this path has no order-placement capability."""
    text = str(cmd.get("_tg_text") or "")
    style = str(cmd.get("_signal_style") or "").upper()
    side = str(cmd.get("side") or "").lower()
    probability = extract_calibrated_probability(text)
    now_kyiv = datetime.now(KYIV_TZ)
    if not qualifies_s2(
        style=style,
        side=side,
        probability=probability,
        now_kyiv=now_kyiv,
    ):
        return False
    if any(cmd.get(field) is None for field in ("entry", "sl", "tp")):
        log("WARNING", "S2 SHADOW SKIP: entry/SL/TP1 missing")
        return False
    base = _clean_base_from_context(cmd.get("base"), text)
    if not base or non_crypto_open_block_reason(base, signal_text=text, style=style):
        log("WARNING", f"S2 SHADOW SKIP {base or 'unknown'}: non-crypto or missing base")
        return False
    signal_key = str(cmd.get("_signal_key") or cmd.get("_signal_content_hash") or "")
    if not signal_key:
        return False
    try:
        added = SHADOW_S2.register(
            signal_key=signal_key,
            base=base,
            entry=float(cmd["entry"]),
            sl=float(cmd["sl"]),
            tp1=float(cmd["tp"]),
            probability=float(probability),
        )
    except (TypeError, ValueError, OSError) as exc:
        log("WARNING", f"S2 SHADOW SKIP {base}: {exc}")
        return False
    if added:
        log(
            "INFO",
            f"S2 SHADOW OPEN {base} P(TP1)={probability:g}% "
            f"entry={cmd['entry']} sl={cmd['sl']} tp1={cmd['tp']}",
        )
    return added


def execution_strategy_for_position(position_key: str) -> str:
    row = (EXECUTION_STATE.get("positions") or {}).get(position_key) or {}
    return str(row.get("strategy") or row.get("style") or "UNKNOWN").upper()


def pending_execution_closures_for_pnl() -> dict[str, dict]:
    """Return recent signal-linked BingX closes that still need a PnL message."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    pending = {}
    with EXECUTION_STATE_LOCK:
        for position_key, row in (EXECUTION_STATE.get("positions") or {}).items():
            if row.get("status") != "closed" or not row.get("signal_key"):
                continue
            if row.get("pnl_notified_at"):
                continue
            try:
                closed_at = datetime.fromisoformat(
                    str(row.get("closed_at") or "").replace("Z", "+00:00")
                )
            except ValueError:
                continue
            if closed_at < cutoff:
                continue
            pending[position_key] = {
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "size": row.get("qty") or row.get("size") or 0.0,
                "entry": row.get("actual_entry") or row.get("entry") or 0.0,
                "opened_at": row.get("opened_at"),
            }
    return pending


def mark_execution_closure_pnl_notified(position_key: str, pnl: float) -> None:
    with EXECUTION_STATE_LOCK:
        row = (EXECUTION_STATE.get("positions") or {}).get(position_key)
        if not row:
            return
        row["pnl_notified_at"] = _utc_iso()
        row["realized_pnl_usdt"] = float(pnl)
        row["updated_at"] = _utc_iso()
        save_execution_state()


def _format_s2_shadow_close(trade: dict, summary: dict) -> str:
    status = "🟢 TP1" if trade["status"] == "tp1_hit" else "🔴 STOP LOSS"
    pf = summary["profit_factor"]
    pf_text = "∞" if pf is None else f"{pf:.2f}"
    return (
        "📊 S2 SHADOW — закрита угода\n\n"
        f"{status} · {trade['base']}/USDT\n"
        f"Net: {trade['pnl_balance']:+.4f} virtual units ({trade['net_r']:+.3f}R)\n"
        f"S2 total: {summary['trades']} trades · {summary['wins']} TP · "
        f"{summary['losses']} SL · Net {summary['net_pct']:+.2f}% · PF {pf_text}\n"
        "Mode: SHADOW — реальний ордер не створювався"
    )


def _format_s1_shadow_close(trade: dict, summary: dict) -> str:
    labels = {
        "tp3_hit": "🟢 TP3",
        "breakeven_exit": "🟢 PARTIAL + BE",
        "sl_hit": "🔴 STOP LOSS",
    }
    pf = summary["profit_factor"]
    pf_text = "∞" if pf is None else f"{pf:.2f}"
    return (
        "📊 S1 SHADOW — закрита угода\n\n"
        f"{labels.get(trade['status'], trade['status'])} · {trade['base']}/USDT\n"
        f"Net: {trade['pnl_balance']:+.4f} virtual units ({trade['net_r']:+.3f}R)\n"
        f"S1 total: {summary['trades']} trades · {summary['wins']} profit · "
        f"{summary['losses']} loss · Net {summary['net_pct']:+.2f}% · PF {pf_text}\n"
        f"Exits: TP3 {summary['tp3_hit']} · BE {summary['breakeven_exit']} · SL {summary['sl_hit']}\n"
        "Mode: SHADOW — реальний ордер не створювався"
    )


async def shadow_s2_watcher():
    while True:
        try:
            for position in SHADOW_S2.open_positions():
                symbol = await resolve_symbol(position["base"])
                if not symbol:
                    continue
                ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
                price = float(ticker.get("last") or ticker.get("close") or 0)
                if price <= 0:
                    continue
                trade = SHADOW_S2.observe(position["signal_key"], price)
                if trade:
                    log(
                        "INFO",
                        f"S2 SHADOW CLOSE {position['base']} status={trade['status']} "
                        f"net={trade['pnl_balance']:+.4f}; PNL chat suppressed",
                    )
        except Exception as exc:
            log("ERROR", f"S2 shadow watcher error: {exc}")
        await asyncio.sleep(SHADOW_S2_WATCH_SEC)


async def shadow_s1_watcher():
    while True:
        try:
            for position in SHADOW_S1.open_positions():
                symbol = await resolve_symbol(position["base"])
                if not symbol:
                    continue
                ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
                price = float(ticker.get("last") or ticker.get("close") or 0)
                if price <= 0:
                    continue
                trade = SHADOW_S1.observe(position["signal_key"], price)
                if trade:
                    log(
                        "INFO",
                        f"S1 SHADOW CLOSE {position['base']} status={trade['status']} "
                        f"net={trade['pnl_balance']:+.4f}; PNL chat suppressed",
                    )
        except Exception as exc:
            log("ERROR", f"S1 shadow watcher error: {exc}")
        await asyncio.sleep(SHADOW_S2_WATCH_SEC)


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
    tp2 = cmd.get("tp2")
    tp3 = cmd.get("tp3")
    add_pct = cmd.get("add_pct")
    position_usdt = cmd.get("position_usdt")
    margin_usdt = cmd.get("margin_usdt")
    signal_balance_usdt = cmd.get("balance_usdt")
    liquidation = cmd.get("liquidation")
    if liquidation is None:
        liquidation = extract_liquidation_from_text(cmd.get("_tg_text", ""))

    tg_text = cmd.get("_tg_text", "")
    signal_key = cmd.get("_signal_key")

    log("INFO", f"AI action={action} conf={conf} base={base} side={side} lev={lev} risk={risk_pct} signal_balance={signal_balance_usdt} position_usdt={position_usdt} margin_usdt={margin_usdt} sl={sl} tp={tp} tp2={tp2} tp3={tp3} liq={liquidation} add_pct={add_pct}")

    # Safety: never convert a clear OPEN signal to ADD.
    # DCA inside an OPEN setup means "open now + place pending DCA order", not market ADD.
    # We only rescue NONE -> ADD when there is already an open position.
    if action == "NONE" and base and _has_add_intent_text(tg_text):
        if not ENTRY_RULES.allow_position_additions:
            log(
                "WARNING",
                f"POLICY SKIP ADD {base}: fixed {FIXED_RISK_PCT:g}% entry risk; "
                "position additions are disabled",
            )
            return
        try:
            base_for_add = _clean_base_from_context(base, tg_text)
            symbol_for_add = await resolve_symbol(base_for_add)
            pos_for_add = await fetch_position_oneway(symbol_for_add) if symbol_for_add else None
            if pos_for_add and (add_pct is not None or risk_pct is not None):
                log("WARNING", "FORCE FIX: NONE -> ADD by text rule")
                action = "ADD"
        except Exception as e:
            log("WARNING", f"FORCE ADD CHECK failed: {e}")

    if action == "NONE":
        log("DEBUG", "AI SKIP: action=NONE")
        return

    if action == "ADD" and not ENTRY_RULES.allow_position_additions:
        log(
            "WARNING",
            f"POLICY SKIP ADD {base}: fixed {FIXED_RISK_PCT:g}% entry risk; "
            "position additions are disabled",
        )
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
                res = await close_position_full(b, side if side in {"long", "short"} else None)
                _log_action_result("CLOSE", b, res)
            except Exception as e:
                log("ERROR", f"CLOSE {b} failed: {e}")
        return

    if action == "OPEN":
        update_execution_signal(
            signal_key,
            "evaluating",
            source_chat_id=cmd.get("_source_chat_id"),
            source_message_id=cmd.get("_source_message_id"),
            content_hash=cmd.get("_signal_content_hash"),
            style=cmd.get("_signal_style"),
            base=base,
            side=side,
        )
        if not base:
            log("INFO", "AI SKIP OPEN: base missing")
            update_execution_signal(signal_key, "skipped", reason="base missing")
            return

        if side not in {"long", "short"}:
            log("INFO", "AI SKIP OPEN: side missing/invalid")
            update_execution_signal(signal_key, "skipped", reason="side missing/invalid")
            return

        _register_s1_shadow_if_eligible(cmd)
        _register_s2_shadow_if_eligible(cmd)

        policy_block = open_policy_block_reason(
            side,
            style=cmd.get("_signal_style"),
            require_allowed_style=True,
        )
        if policy_block:
            log("WARNING", f"POLICY SKIP OPEN {base}: {policy_block}")
            update_execution_signal(signal_key, "skipped", reason=policy_block)
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

        signal_style = str(cmd.get("_signal_style") or "").upper()
        try:
            source_rr1 = source_signal_rr1(cmd, side)
            stop_distance_pct = source_stop_distance_pct(cmd, side)
        except (TypeError, ValueError) as source_error:
            reason = f"source levels are invalid: {source_error}"
            log("WARNING", f"POLICY SKIP OPEN {base}: {reason}")
            update_execution_signal(signal_key, "skipped", reason=reason)
            return
        if source_rr1 is None or stop_distance_pct is None:
            reason = "combined strategy requires source Entry, SL and TP1"
            log("WARNING", f"POLICY SKIP OPEN {base}: {reason}")
            update_execution_signal(signal_key, "skipped", reason=reason)
            return
        strategy_decision, strategy_block = select_strategy(
            style=signal_style,
            side=side,
            signal_text=tg_text,
            rr1=source_rr1,
            stop_distance_pct=stop_distance_pct,
        )
        if strategy_block or strategy_decision is None:
            reason = strategy_block or "no strategy module matched"
            log("WARNING", f"POLICY SKIP OPEN {base}: {reason}")
            update_execution_signal(signal_key, "skipped", reason=reason)
            return
        partial_rules = strategy_decision
        use_partial_exit = strategy_decision.live_partial_exit_ready
        if use_partial_exit and (tp2 is None or tp3 is None):
            reason = f"{strategy_decision.rule_id} requires TP1, TP2 and TP3"
            log("ERROR", f"SAFE SKIP OPEN {base}: {reason}")
            update_execution_signal(signal_key, "skipped", reason=reason)
            return
        log(
            "INFO",
            f"STRATEGY MATCH {base}: {strategy_decision.rule_id} "
            f"risk={strategy_decision.risk_pct:.2f}% source_rr1={source_rr1:.4f} "
            f"source_sl={stop_distance_pct:.4f}%",
        )

        base_clean = _clean_base_from_context(base, tg_text)

        # Fail closed for explicitly known TradFi symbols before resolving an
        # exchange contract.  This both gives a policy reason for markets that
        # are unavailable through the standard crypto API (for example HOOD)
        # and prevents a future exchange/CCXT listing from bypassing Rule 1/2A.
        asset_policy_block = non_crypto_open_block_reason(
            base_clean,
            signal_text=tg_text,
            style=signal_style,
        )
        if asset_policy_block:
            log("WARNING", f"POLICY SKIP OPEN {base_clean}: {asset_policy_block}")
            update_execution_signal(signal_key, "skipped", reason=asset_policy_block)
            return

        symbol = await resolve_symbol(base_clean)

        if not symbol:
            log("ERROR", f"Symbol not listed on BingX: {base_clean}/USDT")
            update_execution_signal(signal_key, "skipped", reason="symbol not listed on BingX")
            return

        if not ENTRY_RULES.allow_same_symbol_side_reentry:
            existing_same_side = await fetch_position_oneway(symbol, side)
            if existing_same_side:
                reason = (
                    f"{side.upper()} position already exists on BingX; "
                    "repeat signal is not added to the aggregated position"
                )
                log(
                    "WARNING",
                    f"POLICY SKIP OPEN {base_clean}: {reason}",
                )
                update_execution_signal(
                    signal_key, "skipped_existing_position", reason=reason,
                    symbol=symbol, base=base_clean, side=side,
                )
                return

        # A second pass uses exchange metadata to block new/unknown TradFi
        # symbols that are not yet present in the explicit fallback lists.
        asset_policy_block = non_crypto_open_block_reason(
            base_clean,
            signal_text=tg_text,
            symbol=symbol,
            style=signal_style,
        )
        if asset_policy_block:
            log("WARNING", f"POLICY SKIP OPEN {base_clean}: {asset_policy_block}")
            update_execution_signal(signal_key, "skipped", reason=asset_policy_block)
            return

        api_open_issue = await asyncio.to_thread(market_api_open_disabled_sync, symbol)
        if api_open_issue:
            log("ERROR", api_open_issue)
            update_execution_signal(signal_key, "skipped", reason=api_open_issue)
            return

        try:
            await asyncio.to_thread(reconcile_execution_state_sync)
            portfolio_block = portfolio_entry_block_reason(
                symbol, strategy_decision.risk_pct
            )
        except Exception as portfolio_error:
            portfolio_block = f"cannot verify portfolio limits: {portfolio_error}"
        if portfolio_block:
            log("WARNING", f"PORTFOLIO SKIP OPEN {base_clean}: {portfolio_block}")
            update_execution_signal(signal_key, "skipped", reason=portfolio_block)
            return

        try:
            entry = float((await asyncio.to_thread(exchange.fetch_ticker, symbol))["last"])
        except Exception as e:
            log("ERROR", f"fetch_ticker failed: {e}")
            return

        if signal_style == "SWING" and cmd.get("entry") is not None:
            try:
                signal_entry = normalize_price_from_tail(
                    float(cmd["entry"]), entry, side, "entry"
                )
                signal_sl = normalize_price_from_tail(
                    float(sl), signal_entry, side, "sl"
                )
                drift_block = swing_entry_drift_block_reason(
                    side,
                    signal_entry,
                    entry,
                    signal_sl,
                )
            except (TypeError, ValueError) as drift_error:
                drift_block = f"cannot validate SWING entry freshness: {drift_error}"
            if drift_block:
                log("WARNING", f"POLICY SKIP OPEN {base_clean}: {drift_block}")
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

        tp2_fixed = None
        tp3_fixed = None
        if use_partial_exit:
            tp2_fixed = normalize_price_from_tail(float(tp2), entry, side, "tp")
            tp3_fixed = normalize_price_from_tail(float(tp3), entry, side, "tp")

        log("INFO", f"FIX {base_clean} entry={entry} rawSL={sl} -> {sl_fixed} | rawTP={tp} -> {tp_fixed} | TP2={tp2_fixed} | TP3={tp3_fixed} | rr={rr_value}")

        try:
            sl_prec = float(await asyncio.to_thread(exchange.price_to_precision, symbol, sl_fixed))
        except Exception:
            sl_prec = float(sl_fixed)

        try:
            tp_prec = float(await asyncio.to_thread(exchange.price_to_precision, symbol, tp_fixed))
        except Exception:
            tp_prec = float(tp_fixed)

        tp2_prec = None
        tp3_prec = None
        if use_partial_exit:
            try:
                tp2_prec = float(
                    await asyncio.to_thread(exchange.price_to_precision, symbol, tp2_fixed)
                )
            except Exception:
                tp2_prec = float(tp2_fixed)
            try:
                tp3_prec = float(
                    await asyncio.to_thread(exchange.price_to_precision, symbol, tp3_fixed)
                )
            except Exception:
                tp3_prec = float(tp3_fixed)

        if not validate_sl_tp(side, entry, sl_prec, tp_prec):
            log("INFO", f"SKIP Bad SL/TP vs entry. entry={entry} SL={sl_prec} TP={tp_prec}")
            return
        if use_partial_exit and not (tp_prec < tp2_prec < tp3_prec):
            log(
                "INFO",
                f"SKIP Bad {signal_style} target order: "
                f"TP1={tp_prec} TP2={tp2_prec} TP3={tp3_prec}",
            )
            return

        try:
            effective_rr1 = calculate_rr_from_prices(entry, sl_prec, tp_prec, side)
        except (TypeError, ValueError) as e:
            log("WARNING", f"POLICY SKIP OPEN {base_clean}: RR1 calculation failed: {e}")
            return

        log(
            "INFO",
            f"RR1 POLICY PASS {base_clean} style={cmd.get('_signal_style')} "
            f"signal_rr1={source_rr1 if source_rr1 is not None else 'n/a'} "
            f"live_fill_rr1={effective_rr1:.4f}",
        )

        try:
            usdt_total = await get_usdt_total()
            applied_risk_pct = strategy_decision.risk_pct
            trade_plan = calculate_auto_trade_plan(
                usdt_total,
                entry,
                sl_prec,
                tp_prec,
                risk_pct=applied_risk_pct,
            )
            lev = int(trade_plan["leverage"])
            target_notional = float(trade_plan["notional"])
            qty_raw = target_notional / entry
            qty, min_amount = await normalize_order_qty(symbol, qty_raw)
            log(
                "INFO",
                f"AUTO PLAN capital={usdt_total} risk={trade_plan['risk_pct']}% "
                f"risk_budget={trade_plan['risk_budget']} leverage={lev}x "
                f"SL_distance={trade_plan['stop_distance_pct']:.4f}% "
                f"TP1_distance={trade_plan['tp_distance_pct']:.4f}% "
                f"notional={target_notional} margin={trade_plan['margin']} qty={qty}",
            )
            if min_amount is not None and qty_raw < min_amount:
                log("WARNING", f"SKIP {symbol}: exchange minimum qty={min_amount} would exceed the {FIXED_RISK_PCT}% risk limit")
                return
        except Exception as e:
            log("ERROR", f"balance/qty failed: {e}")
            return

        if qty <= 0:
            log("INFO", "SKIP qty became 0")
            return


        partial_exit_mode = "single"
        partial_exit_fallback_reason = None
        if use_partial_exit:
            partial_exit_mode, planned_parts, partial_exit_fallback_reason = (
                _choose_partial_exit_mode(
                    symbol,
                    qty,
                    reference_price=entry,
                    target_split=partial_rules.target_split,
                )
            )
            if partial_exit_mode == "balanced":
                log(
                    "INFO",
                    f"{signal_style} SIZE PREFLIGHT PASS {base_clean} "
                    f"qty={qty} parts={planned_parts}",
                )
            else:
                log(
                    "WARNING",
                    f"{signal_style} FULL TP1 FALLBACK PLANNED {base_clean}: "
                    f"position qty={qty:g} cannot execute 40/30/30 without "
                    f"exceeding the risk budget; {partial_exit_fallback_reason}",
                )

        if DRY_RUN:
            log("INFO", "DRY_RUN OPEN skipped (test mode)")
            update_execution_signal(signal_key, "dry_run", symbol=symbol, base=base_clean)
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
            old_pos = await fetch_position_oneway(symbol, side)
            old_size = float(
                old_pos.get("contracts")
                or old_pos.get("size")
                or old_pos.get("positionAmt")
                or 0
            ) if old_pos else 0.0

            resp = await open_market(symbol, side, qty)
            log("INFO", f"SUCCESS OPEN placed id={resp.get('id')} {base_clean} side={side} qty={qty}")
            bind_signal_to_position(
                signal_key,
                symbol,
                side,
                order_id=resp.get("id"),
                base=base_clean,
                requested_qty=qty,
                risk_pct=float(trade_plan["risk_pct"]),
                risk_budget=float(trade_plan["risk_budget"]),
                sl=sl_prec,
                tp1=tp_prec,
                tp2=tp2_prec,
                tp3=tp3_prec,
                style=signal_style,
                strategy=strategy_decision.rule_id,
            )
            update_execution_signal(
                signal_key,
                "order_placed",
                symbol=symbol,
                base=base_clean,
                side=side,
                entry_order_id=resp.get("id"),
                requested_qty=qty,
            )

            pos_seen = await wait_position_update(
                symbol,
                old_size=old_size,
                timeout=6.0,
                min_target_size=old_size + qty * 0.7,
                position_side=side,
            )
            log("INFO", f"POSITION_VISIBLE_AFTER_OPEN {base_clean}={bool(pos_seen)}")
            if not pos_seen:
                update_execution_signal(
                    signal_key,
                    "order_unconfirmed",
                    reason="market order placed but position was not visible before timeout",
                )
            await asyncio.sleep(0.7)

            try:
                if use_partial_exit and partial_exit_mode == "balanced":
                    res = await apply_swing_sltp(
                        base_clean,
                        entry_price=entry,
                        sl_price=sl_prec,
                        tp1_price=tp_prec,
                        tp2_price=float(tp2_prec),
                        tp3_price=float(tp3_prec),
                        position_side=side,
                        signal_style=signal_style,
                    )
                elif use_partial_exit:
                    fallback = await apply_sltp(
                        base_clean,
                        sl_price=sl_prec,
                        tp_price=tp_prec,
                        cancel_first=True,
                        position_side=side,
                    )
                    res = (
                        "FALLBACK_FULL_TP1 after size preflight error="
                        f"{partial_exit_fallback_reason} | {fallback}"
                    )
                else:
                    LAST_SLTP.setdefault(base_clean, {})
                    LAST_SLTP[base_clean][side] = {
                        "sl": sl_prec,
                        "tp": tp_prec,
                        "strategy": strategy_decision.rule_id,
                        "risk_pct": strategy_decision.risk_pct,
                    }
                    save_sltp()
                    res = await apply_sltp(
                        base_clean,
                        sl_price=sl_prec,
                        tp_price=tp_prec,
                        cancel_first=True,
                        position_side=side,
                    )
            except Exception as protection_error:
                log(
                    "ERROR",
                    f"CRITICAL {base_clean}: protective orders failed after OPEN: "
                    f"{protection_error}; emergency closing position",
                )
                try:
                    close_result = await close_position_full(base_clean, side)
                except Exception as close_error:
                    close_result = f"EMERGENCY CLOSE FAILED: {close_error}"
                await _send_to_tg(
                    f"🚨 {base_clean}/USDT: не вдалося встановити захист після входу. "
                    f"Аварійне закриття: {close_result}"
                )
                update_execution_signal(
                    signal_key,
                    "emergency_closed",
                    reason=f"protective orders failed: {protection_error}",
                )
                return
            log("INFO", f"APPLY SL/TP after OPEN done: {res}")
            update_execution_signal(
                signal_key,
                "open_protected",
                protection_result=res,
                actual_qty=(
                    _extract_position_qty_sync(pos_seen, symbol) if pos_seen else None
                ),
                actual_entry=_position_entry_price(pos_seen) if pos_seen else None,
            )
            partial_exit_active = use_partial_exit and not res.startswith(
                "FALLBACK_FULL_TP1"
            )
            if use_partial_exit and not partial_exit_active:
                await _send_to_tg(
                    f"⚠️ {base_clean}/USDT: три часткові TP не встановились. "
                    "Увімкнено безпечний fallback: повний SL + 100% TP1."
                )

            actual_liquidation = _extract_position_liquidation(pos_seen)
            shown_liquidation = actual_liquidation or estimate_liquidation_price(entry, side, int(lev))
            await _send_to_tg(
                _format_open_notification(
                    base=base_clean,
                    side=side,
                    style=cmd.get("_signal_style"),
                    entry=entry,
                    sl=sl_prec,
                    tp=tp_prec,
                    tp2=tp2_prec,
                    tp3=tp3_prec,
                    swing_partial=partial_exit_active,
                    qty=qty,
                    leverage=int(lev),
                    liquidation=shown_liquidation,
                    risk_usdt=float(trade_plan["expected_loss_at_sl"]),
                    expected_tp1_profit=(
                        float(trade_plan["expected_profit_at_tp1"])
                        * (partial_rules.target_split[0] if partial_exit_active else 1.0)
                    ),
                    margin_usdt=float(trade_plan["margin"]),
                    breakeven_buffer_r=(
                        partial_rules.breakeven_buffer_r
                        if partial_exit_active
                        else None
                    ),
                )
            )

            dca_price = cmd.get("dca_price") or cmd.get("price")
            dca_pct = cmd.get("dca_pct")
            if dca_price and dca_pct:
                log(
                    "WARNING",
                    f"POLICY SKIP DCA {base_clean}: fixed {FIXED_RISK_PCT:g}% entry risk; "
                    "position additions are disabled",
                )

        except Exception as e:
            log("ERROR", f"OPEN FAILED: {e}")
            return
        return

    if action == "ADD":
        early_policy_block = open_policy_block_reason(side or "long")
        if early_policy_block:
            log("WARNING", f"POLICY SKIP ADD {base}: {early_policy_block}")
            return

        base_clean = _clean_base_from_context(base, tg_text)
        symbol = await resolve_symbol(base_clean)

        if not symbol:
            log("ERROR", f"symbol not listed: {base_clean}")
            return

        pos = await fetch_position_oneway(symbol, side if side in {"long", "short"} else None)
        if not pos:
            log("WARNING", f"SKIP ADD {base_clean}: NO_POSITION")
            return

        side = (pos.get("side") or (pos.get("info") or {}).get("positionSide") or "").lower()
        lev = int(float(pos.get("leverage") or (pos.get("info") or {}).get("leverage") or 1))

        policy_block = open_policy_block_reason(side)
        if policy_block:
            log("WARNING", f"POLICY SKIP ADD {base_clean}: {policy_block}")
            return

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

            old_pos = await fetch_position_oneway(symbol, side)
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
                position_side=side,
            )
            log("INFO", f"POSITION_VISIBLE_AFTER_ADD {base_clean}={bool(pos_seen)}")
            await asyncio.sleep(0.7)

            reapply_res = await reapply_saved_sltp(base_clean, side)
            log("INFO", f"REAPPLY after ADD done: {reapply_res}")
            return
        except Exception as e:
            log("ERROR", f"ADD failed: {e}")
            return

    if action == "SET_SL":
        if not base or sl is None:
            log("INFO", "AI SKIP SET_SL: base or sl missing")
            return

        base_clean = _clean_base_from_context(base, tg_text)
        symbol = await resolve_symbol(base_clean)
        if not symbol:
            log("ERROR", f"SET_SL skip: symbol not listed: {base_clean}/USDT")
            return

        new_sl = float(sl)
        pos = await fetch_position_oneway(symbol, side if side in {"long", "short"} else None)
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
            res = await set_sl_oneway(base_clean, new_sl, pos_side)
            _log_action_result("SET_SL", base_clean, res)
        except Exception as e:
            log("ERROR", f"SET_SL failed: {e}")
        return

    if action == "SET_TP":
        if not base or tp is None:
            log("INFO", "AI SKIP SET_TP: base or tp missing")
            return

        base_clean = _clean_base_from_context(base, tg_text)
        symbol = await resolve_symbol(base_clean)
        if not symbol:
            log("ERROR", f"SET_TP skip: symbol not listed: {base_clean}/USDT")
            return

        new_tp = float(tp)
        pos = await fetch_position_oneway(symbol, side if side in {"long", "short"} else None)
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
            res = await set_tp_oneway(base_clean, new_tp, pos_side)
            _log_action_result("SET_TP", base_clean, res)
        except Exception as e:
            log("ERROR", f"SET_TP failed: {e}")
        return

    if action == "BE":
        target_bases: list[str] = []

        if base:
            base_clean = _clean_base_from_context(base, tg_text)
            if base_clean:
                target_bases.append(base_clean)

        if not target_bases:
            target_bases = extract_multiple_bases(tg_text)

        if not target_bases:
            log("INFO", "AI SKIP BE: base missing")
            return

        for base_clean in target_bases:
            symbol = await resolve_symbol(base_clean)
            if not symbol:
                log("ERROR", f"BE skip: symbol not listed: {base_clean}/USDT")
                continue

            if DRY_RUN:
                log("INFO", f"DRY_RUN BE {base_clean} skipped (test mode)")
                continue

            try:
                log("INFO", f"BE {base_clean}: move SL to entry and keep TP")
                res = await breakeven_oneway(base_clean, side if side in {"long", "short"} else None)
                _log_action_result("BE", base_clean, res)
            except Exception as e:
                log("ERROR", f"BE {base_clean} failed: {e}")
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

    if (
        text
        and is_new_entry_signal_text(text)
        and not is_allowed_signal_style(text)
    ):
        log("INFO", f"STYLE SKIP album={gid} style={extract_signal_style(text)} allowed={sorted(ALLOWED_SIGNAL_STYLES)}")
        return

    log("INFO", f"ALBUM media_group={gid} images={len(images)}")
    log("INFO", f"AI_RAW {text[:400] if text else '<no text>'}")

    if not text and not images:
        return

    if text and has_close_intent(text):
        await close_bundle_start_or_update(text=text, images=images)
        return

    cmd = await asyncio.to_thread(parse_trade_multi, text if text else None, images)
    cmd["_tg_text"] = text
    cmd["_signal_style"] = extract_signal_style(text)
    cmd["liquidation"] = cmd.get("liquidation") or extract_liquidation_from_text(text)
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

    cmd = await asyncio.to_thread(parse_trade_multi, text, images)
    cmd["_tg_text"] = text
    cmd["_signal_style"] = extract_signal_style(text)
    await handle_ai_command(cmd)


# =========================
# TG HANDLER: automatic SignalBot+ source plus guarded Saved Messages control
# =========================
_last_hb = 0.0

@app.on_message(
    filters.chat(SOURCE_CHAT_IDS)
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

    raw_text = (message.text or message.caption or "").strip()
    text = normalize_source_text(message.chat.id, raw_text)
    source_message_id = getattr(message, "id", None)
    signal_key = signal_execution_key(message.chat.id, source_message_id, text or raw_text)
    content_hash = signal_content_hash(text or raw_text)
    if int(message.chat.id) == CONTROL_CHAT_ID and text is None:
        log("INFO", "CONTROL SKIP: Saved Messages payload needs a full signal card or /tb command")
        return

    if (
        text
        and not message.media_group_id
        and is_new_entry_signal_text(text)
        and not is_allowed_signal_style(text)
    ):
        log("INFO", f"STYLE SKIP style={extract_signal_style(text)} allowed={sorted(ALLOWED_SIGNAL_STYLES)}")
        update_execution_signal(
            signal_key,
            "skipped",
            reason=f"style={extract_signal_style(text)} is not live-enabled",
            source_chat_id=int(message.chat.id),
            source_message_id=source_message_id,
            content_hash=content_hash,
        )
        return

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
        cmd = await asyncio.to_thread(parse_trade_multi, None, [img_path])
        cmd["_tg_text"] = ""
        cmd["_signal_style"] = None
        await handle_ai_command(cmd)
        return
        
    # close intent -> bundle
    if text and has_close_intent(text):
        await close_bundle_start_or_update(text=text, images=[img_path] if img_path else [])
        return

    if not text and not img_path:
        return

    if (
        text
        and is_new_entry_signal_text(text)
        and execution_signal_is_duplicate(signal_key, content_hash)
    ):
        log("WARNING", f"EXEC DUPLICATE SKIP signal_key={signal_key}")
        return

    log("INFO", f"AI_RAW {text[:400] if text else '<no text>'}")
    cmd = await asyncio.to_thread(parse_trade_multi, text if text else None, [img_path] if img_path else [])
    cmd["_tg_text"] = text
    cmd["_signal_style"] = extract_signal_style(text)
    cmd["_source_chat_id"] = int(message.chat.id)
    cmd["_source_message_id"] = source_message_id
    cmd["_signal_key"] = signal_key
    cmd["_signal_content_hash"] = content_hash
    cmd["liquidation"] = cmd.get("liquidation") or extract_liquidation_from_text(text)
    await handle_ai_command(cmd)


# =========================
# MAIN (Railway safe)
# =========================

async def main():
    load_sltp()
    load_order_ids()
    load_execution_state()
    start_position_status_server()
    await app.start()

    try:
        ok = await ensure_peer_known(TARGET_CHAT_ID)
        while not ok:
            log("WARNING", "⏳ TARGET retry in 60s…")
            await asyncio.sleep(60)
            ok = await ensure_peer_known(TARGET_CHAT_ID)

        if CONTROL_CHAT_ID != TARGET_CHAT_ID:
            control_ok = await ensure_peer_known(CONTROL_CHAT_ID)
            if control_ok:
                log("INFO", f"Saved Messages control ready. control_chat_id={CONTROL_CHAT_ID}")
            else:
                log("WARNING", f"Saved Messages control unavailable. control_chat_id={CONTROL_CHAT_ID}")

        if LOG_CHAT_ID:
            ok2 = await ensure_peer_known(LOG_CHAT_ID)
            if not ok2:
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
            sync_result = await asyncio.to_thread(reconcile_execution_state_sync)
            log(
                "INFO",
                f"EXEC SYNC startup live={sync_result['live']} "
                f"closed={len(sync_result['closed'])} "
                f"unmatched={len(sync_result['unmatched'])}",
            )
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
                strategy_resolver=execution_strategy_for_position,
                pending_closed_resolver=pending_execution_closures_for_pnl,
                closed_notified_callback=mark_execution_closure_pnl_notified,
            )
        )

        asyncio.create_task(swing_exit_watcher_loop())
        asyncio.create_task(execution_reconcile_loop())
        asyncio.create_task(shadow_s1_watcher())
        asyncio.create_task(shadow_s2_watcher())

        log(
            "INFO",
            "ENTRY POLICY "
            f"active_rules={[rules.version for rules in ACTIVE_ENTRY_RULES]} "
            f"styles={sorted(ALLOWED_SIGNAL_STYLES)} "
            f"long_only={TRADE_LONG_ONLY} "
            "modules=A1>A4>A5>A3 risks=0.70/0.70/0.50/0.60% "
            f"assets={'ordinary_crypto_only' if ENTRY_RULES.ordinary_crypto_only else 'all_bingx_listed'} "
            "exits=A1/A4/A5_full_TP1,A3_40/30/30 "
            f"s1_shadow={SHADOW_S1_RULES.rr1_min_inclusive:g}<=RR<"
            f"{SHADOW_S1_RULES.rr1_max_exclusive:g}@10:00-23:00_Kyiv "
            f"s2_shadow=P(TP1)>={SHADOW_S2_RULES.probability_min_inclusive:g}%@16:30-18:30_Kyiv "
            f"additions={ENTRY_RULES.allow_position_additions} "
            f"same_side_reentry={ENTRY_RULES.allow_same_symbol_side_reentry} "
            f"max_positions={ENTRY_RULES.max_concurrent_positions} "
            f"max_open_risk={ENTRY_RULES.max_open_risk_pct:g}% "
            f"execution_sync={EXECUTION_SYNC_SEC:g}s "
            f"blocked_kyiv={ENTRY_BLOCK_START_HOUR_KYIV:02d}:00-"
            f"{ENTRY_BLOCK_END_HOUR_KYIV:02d}:00",
        )
        log(
            "INFO",
            f"DRY_RUN={DRY_RUN} | Listening source_chat_ids={SOURCE_CHAT_IDS} "
            "| Saved Messages accepts full signal cards or /tb commands",
        )
        await idle()

    finally:
        await app.stop()

if __name__ == "__main__":
    app.run(main())
