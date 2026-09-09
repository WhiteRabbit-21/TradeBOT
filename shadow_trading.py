"""Persistent observation-only execution for strategy S2.

This module never receives exchange credentials and cannot place or cancel an
order.  It only records qualifying signal levels and marks TP1/SL from prices
supplied by the caller.
"""
from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from trade_rules import SHADOW_S2_RULES

_LOCK = threading.RLock()


def extract_calibrated_probability(text: str) -> Optional[float]:
    match = re.search(
        r"P\s*\(\s*TP1[^)]*SL\s*\)\s*:\s*(?:<[^>]+>\s*)?([0-9]+(?:[.,][0-9]+)?)\s*%",
        str(text or ""),
        re.IGNORECASE,
    )
    if not match:
        return None
    value = float(match.group(1).replace(",", "."))
    return value if 0 <= value <= 100 else None


def qualifies_s2(*, style: str, side: str, probability: Optional[float], now_kyiv: datetime) -> bool:
    minute = now_kyiv.hour * 60 + now_kyiv.minute
    return (
        str(style or "").upper() in SHADOW_S2_RULES.allowed_styles
        and str(side or "").lower() == SHADOW_S2_RULES.allowed_side
        and probability is not None
        and probability >= SHADOW_S2_RULES.probability_min_inclusive
        and SHADOW_S2_RULES.start_minute_kyiv <= minute < SHADOW_S2_RULES.end_minute_kyiv
    )


class ShadowBook:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.state = self._load()

    @staticmethod
    def _empty() -> dict:
        return {"version": 1, "balance": 100.0, "positions": {}, "trades": []}

    def _load(self) -> dict:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                value.setdefault("balance", 100.0)
                value.setdefault("positions", {})
                value.setdefault("trades", [])
                return value
        except (FileNotFoundError, OSError, ValueError):
            pass
        return self._empty()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        temporary.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, self.path)

    def register(self, *, signal_key: str, base: str, entry: float, sl: float, tp1: float,
                 probability: float, opened_at: Optional[str] = None) -> bool:
        entry, sl, tp1 = float(entry), float(sl), float(tp1)
        if not (sl < entry < tp1):
            raise ValueError("S2 LONG levels must satisfy SL < entry < TP1")
        with _LOCK:
            if signal_key in self.state["positions"] or any(
                row.get("signal_key") == signal_key for row in self.state["trades"]
            ):
                return False
            balance = float(self.state["balance"])
            self.state["positions"][signal_key] = {
                "signal_key": signal_key,
                "strategy": "S2",
                "mode": "shadow",
                "base": str(base).upper(),
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "probability": float(probability),
                "risk_amount": balance * SHADOW_S2_RULES.risk_per_trade_pct / 100.0,
                "opened_at": opened_at or datetime.now(timezone.utc).isoformat(),
            }
            self._save()
            return True

    def open_positions(self) -> list[dict]:
        with _LOCK:
            return [dict(row) for row in self.state["positions"].values()]

    def observe(self, signal_key: str, price: float, observed_at: Optional[str] = None) -> Optional[dict]:
        with _LOCK:
            position = self.state["positions"].get(signal_key)
            if not position:
                return None
            price = float(price)
            outcome = "sl_hit" if price <= position["sl"] else "tp1_hit" if price >= position["tp1"] else None
            if not outcome:
                return None
            stop_fraction = (position["entry"] - position["sl"]) / position["entry"]
            gross_r = -1.0 if outcome == "sl_hit" else (
                (position["tp1"] - position["entry"]) / (position["entry"] - position["sl"])
            )
            cost_r = SHADOW_S2_RULES.round_trip_cost_notional / stop_fraction
            net_r = gross_r - cost_r
            before = float(self.state["balance"])
            pnl = float(position["risk_amount"]) * net_r
            after = before + pnl
            trade = {
                **position,
                "status": outcome,
                "exit_price": price,
                "closed_at": observed_at or datetime.now(timezone.utc).isoformat(),
                "gross_r": gross_r,
                "cost_r": cost_r,
                "net_r": net_r,
                "pnl_balance": pnl,
                "balance_before": before,
                "balance_after": after,
            }
            self.state["balance"] = after
            self.state["trades"].append(trade)
            del self.state["positions"][signal_key]
            self._save()
            return dict(trade)

    def summary(self) -> dict:
        with _LOCK:
            trades = self.state["trades"]
            wins = sum(row.get("status") == "tp1_hit" for row in trades)
            losses = sum(row.get("status") == "sl_hit" for row in trades)
            gross_win = sum(max(float(row.get("net_r", 0)), 0) for row in trades)
            gross_loss = -sum(min(float(row.get("net_r", 0)), 0) for row in trades)
            return {
                "trades": len(trades),
                "wins": wins,
                "losses": losses,
                "win_rate_pct": 100 * wins / len(trades) if trades else None,
                "balance": float(self.state["balance"]),
                "net_pct": (float(self.state["balance"]) / 100.0 - 1) * 100,
                "profit_factor": gross_win / gross_loss if gross_loss else None,
                "open": len(self.state["positions"]),
            }
