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

from trade_rules import SHADOW_S1_RULES, SHADOW_S2_RULES

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


def qualifies_s1(*, style: str, side: str, rr1: Optional[float], now_kyiv: datetime) -> bool:
    minute = now_kyiv.hour * 60 + now_kyiv.minute
    return (
        str(style or "").upper() in SHADOW_S1_RULES.allowed_styles
        and str(side or "").lower() == SHADOW_S1_RULES.allowed_side
        and rr1 is not None
        and SHADOW_S1_RULES.rr1_min_inclusive <= rr1 < SHADOW_S1_RULES.rr1_max_exclusive
        and SHADOW_S1_RULES.start_minute_kyiv <= minute < SHADOW_S1_RULES.end_minute_kyiv
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


class ShadowS1Book(ShadowBook):
    """Balanced 40/30/30 shadow book with the legacy post-TP1 stop."""

    def register(self, *, signal_key: str, base: str, entry: float, sl: float,
                 tp1: float, tp2: float, tp3: float, opened_at: Optional[str] = None) -> bool:
        entry, sl = float(entry), float(sl)
        targets = [float(tp1), float(tp2), float(tp3)]
        if not (sl < entry < targets[0] <= targets[1] <= targets[2]):
            raise ValueError("S1 LONG levels must satisfy SL < entry < TP1 <= TP2 <= TP3")
        with _LOCK:
            if signal_key in self.state["positions"] or any(
                row.get("signal_key") == signal_key for row in self.state["trades"]
            ):
                return False
            balance = float(self.state["balance"])
            risk_amount = balance * SHADOW_S1_RULES.risk_per_trade_pct / 100.0
            risk_distance = entry - sl
            self.state["positions"][signal_key] = {
                "signal_key": signal_key,
                "strategy": "S1",
                "mode": "shadow",
                "base": str(base).upper(),
                "entry": entry,
                "sl": sl,
                "tp1": targets[0],
                "tp2": targets[1],
                "tp3": targets[2],
                "be_sl": entry - SHADOW_S1_RULES.breakeven_buffer_r * risk_distance,
                "risk_amount": risk_amount,
                "stage": 0,
                "realized_r": 0.0,
                "opened_at": opened_at or datetime.now(timezone.utc).isoformat(),
            }
            self._save()
            return True

    def observe(self, signal_key: str, price: float, observed_at: Optional[str] = None) -> Optional[dict]:
        with _LOCK:
            position = self.state["positions"].get(signal_key)
            if not position:
                return None
            price = float(price)
            risk_distance = position["entry"] - position["sl"]
            split = SHADOW_S1_RULES.target_split
            stage = int(position["stage"])

            protective_sl = position["sl"] if stage == 0 else position["be_sl"]
            outcome = None
            if price <= protective_sl:
                remaining = 1.0 - sum(split[:stage])
                stop_r = -1.0 if stage == 0 else -SHADOW_S1_RULES.breakeven_buffer_r
                delta_r = remaining * stop_r
                position["realized_r"] += delta_r
                self.state["balance"] += position["risk_amount"] * delta_r
                outcome = "sl_hit" if stage == 0 else "breakeven_exit"
            else:
                for index, target_name in enumerate(("tp1", "tp2", "tp3"), start=1):
                    if stage >= index or price < position[target_name]:
                        continue
                    target_r = (position[target_name] - position["entry"]) / risk_distance
                    delta_r = split[index - 1] * target_r
                    position["realized_r"] += delta_r
                    self.state["balance"] += position["risk_amount"] * delta_r
                    position["stage"] = index
                    stage = index
                if stage == 3:
                    outcome = "tp3_hit"

            if not outcome:
                self._save()
                return None

            stop_fraction = risk_distance / position["entry"]
            cost_r = SHADOW_S1_RULES.round_trip_cost_notional / stop_fraction
            self.state["balance"] -= position["risk_amount"] * cost_r
            net_r = float(position["realized_r"]) - cost_r
            trade = {
                **position,
                "status": outcome,
                "exit_price": price,
                "closed_at": observed_at or datetime.now(timezone.utc).isoformat(),
                "gross_r": float(position["realized_r"]),
                "cost_r": cost_r,
                "net_r": net_r,
                "pnl_balance": position["risk_amount"] * net_r,
                "balance_after": float(self.state["balance"]),
            }
            self.state["trades"].append(trade)
            del self.state["positions"][signal_key]
            self._save()
            return dict(trade)

    def summary(self) -> dict:
        with _LOCK:
            trades = self.state["trades"]
            wins = sum(float(row.get("net_r", 0)) > 0 for row in trades)
            losses = sum(float(row.get("net_r", 0)) < 0 for row in trades)
            gross_win = sum(max(float(row.get("net_r", 0)), 0) for row in trades)
            gross_loss = -sum(min(float(row.get("net_r", 0)), 0) for row in trades)
            statuses = {
                key: sum(row.get("status") == key for row in trades)
                for key in ("tp3_hit", "breakeven_exit", "sl_hit")
            }
            return {
                "trades": len(trades), "wins": wins, "losses": losses,
                "win_rate_pct": 100 * wins / len(trades) if trades else None,
                "balance": float(self.state["balance"]),
                "net_pct": (float(self.state["balance"]) / 100.0 - 1) * 100,
                "profit_factor": gross_win / gross_loss if gross_loss else None,
                "open": len(self.state["positions"]), **statuses,
            }
