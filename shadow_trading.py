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

from trade_rules import SHADOW_S1_RULES, SHADOW_S2_RULES, StrategyDecision

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


class LegacyCombinedShadowBook(ShadowBook):
    """Observation-only book for the former A1/A4/A5/A3 live system."""

    @staticmethod
    def _empty() -> dict:
        return {"version": 1, "balance": 1000.0, "positions": {}, "trades": []}

    def register(
        self, *, signal_key: str, base: str, entry: float, sl: float,
        tp1: float, tp2: Optional[float], tp3: Optional[float],
        decision: StrategyDecision, opened_at: Optional[str] = None,
    ) -> bool:
        entry, sl, tp1 = float(entry), float(sl), float(tp1)
        if not (sl < entry < tp1):
            raise ValueError("legacy LONG levels must satisfy SL < entry < TP1")
        partial = bool(decision.live_partial_exit_ready)
        if partial:
            if tp2 is None or tp3 is None:
                raise ValueError(f"{decision.rule_id} shadow needs TP2 and TP3")
            tp2, tp3 = float(tp2), float(tp3)
            if not (tp1 <= tp2 <= tp3):
                raise ValueError("legacy partial targets must satisfy TP1 <= TP2 <= TP3")
        else:
            tp2 = tp3 = tp1

        with _LOCK:
            if signal_key in self.state["positions"] or any(
                row.get("signal_key") == signal_key for row in self.state["trades"]
            ):
                return False
            balance = float(self.state["balance"])
            risk_distance = entry - sl
            self.state["positions"][signal_key] = {
                "signal_key": signal_key,
                "strategy": decision.rule_id,
                "mode": "shadow",
                "base": str(base).upper(),
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "target_split": list(decision.target_split),
                "breakeven_buffer_r": float(decision.breakeven_buffer_r),
                "partial": partial,
                "be_sl": entry - float(decision.breakeven_buffer_r) * risk_distance,
                "risk_pct": float(decision.risk_pct),
                "risk_amount": balance * float(decision.risk_pct) / 100.0,
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
            risk_distance = float(position["entry"]) - float(position["sl"])
            stage = int(position.get("stage", 0))
            outcome = None

            if not position.get("partial"):
                if price <= float(position["sl"]):
                    position["realized_r"] = -1.0
                    self.state["balance"] += float(position["risk_amount"]) * -1.0
                    outcome = "sl_hit"
                elif price >= float(position["tp1"]):
                    target_r = (float(position["tp1"]) - float(position["entry"])) / risk_distance
                    position["realized_r"] = target_r
                    self.state["balance"] += float(position["risk_amount"]) * target_r
                    outcome = "tp1_hit"
            else:
                split = tuple(float(value) for value in position["target_split"])
                protective_sl = float(position["sl"]) if stage == 0 else float(position["be_sl"])
                if price <= protective_sl:
                    remaining = 1.0 - sum(split[:stage])
                    stop_r = -1.0 if stage == 0 else -float(position["breakeven_buffer_r"])
                    delta_r = remaining * stop_r
                    position["realized_r"] += delta_r
                    self.state["balance"] += float(position["risk_amount"]) * delta_r
                    outcome = "sl_hit" if stage == 0 else "breakeven_exit"
                else:
                    for index, target_name in enumerate(("tp1", "tp2", "tp3"), start=1):
                        if stage >= index or price < float(position[target_name]):
                            continue
                        target_r = (float(position[target_name]) - float(position["entry"])) / risk_distance
                        delta_r = split[index - 1] * target_r
                        position["realized_r"] += delta_r
                        self.state["balance"] += float(position["risk_amount"]) * delta_r
                        position["stage"] = index
                        stage = index
                    if stage == 3:
                        outcome = "tp3_hit"

            if not outcome:
                self._save()
                return None

            stop_fraction = risk_distance / float(position["entry"])
            cost_r = 0.0014 / stop_fraction
            self.state["balance"] -= float(position["risk_amount"]) * cost_r
            net_r = float(position["realized_r"]) - cost_r
            trade = {
                **position,
                "status": outcome,
                "exit_price": price,
                "closed_at": observed_at or datetime.now(timezone.utc).isoformat(),
                "gross_r": float(position["realized_r"]),
                "cost_r": cost_r,
                "net_r": net_r,
                "pnl_balance": float(position["risk_amount"]) * net_r,
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
            flat = len(trades) - wins - losses
            gross_win = sum(max(float(row.get("net_r", 0)), 0) for row in trades)
            gross_loss = -sum(min(float(row.get("net_r", 0)), 0) for row in trades)
            open_positions = list(self.state["positions"].values())
            by_rule = {
                rule: sum(row.get("strategy") == rule for row in trades)
                for rule in ("A1", "A4", "A5", "A3")
            }
            breakdown = {}
            for rule in ("A1", "A4", "A5", "A3"):
                rows = [row for row in trades if row.get("strategy") == rule]
                rule_wins = sum(float(row.get("net_r", 0)) > 0 for row in rows)
                rule_losses = sum(float(row.get("net_r", 0)) < 0 for row in rows)
                rule_gross_win = sum(max(float(row.get("net_r", 0)), 0) for row in rows)
                rule_gross_loss = -sum(min(float(row.get("net_r", 0)), 0) for row in rows)
                breakdown[rule] = {
                    "trades": len(rows),
                    "wins": rule_wins,
                    "losses": rule_losses,
                    "flat": len(rows) - rule_wins - rule_losses,
                    "win_rate_pct": 100 * rule_wins / len(rows) if rows else None,
                    "pnl_usdt": sum(float(row.get("pnl_balance", 0)) for row in rows),
                    "profit_factor": rule_gross_win / rule_gross_loss if rule_gross_loss else None,
                    "open": sum(row.get("strategy") == rule for row in open_positions),
                }

            peak = 1000.0
            max_drawdown_usdt = 0.0
            max_drawdown_pct = 0.0
            for row in trades:
                balance_after = float(row.get("balance_after", peak))
                peak = max(peak, balance_after)
                drawdown = max(0.0, peak - balance_after)
                max_drawdown_usdt = max(max_drawdown_usdt, drawdown)
                if peak > 0:
                    max_drawdown_pct = max(max_drawdown_pct, 100 * drawdown / peak)

            timestamps = [
                str(row.get("opened_at"))
                for row in [*trades, *open_positions]
                if row.get("opened_at")
            ]
            balance = float(self.state["balance"])
            return {
                "start_balance": 1000.0,
                "trades": len(trades), "wins": wins, "losses": losses, "flat": flat,
                "win_rate_pct": 100 * wins / len(trades) if trades else None,
                "balance": balance,
                "pnl_usdt": balance - 1000.0,
                "net_pct": (balance / 1000.0 - 1) * 100,
                "profit_factor": gross_win / gross_loss if gross_loss else None,
                "max_drawdown_usdt": max_drawdown_usdt,
                "max_drawdown_pct": max_drawdown_pct,
                "first_opened_at": min(timestamps) if timestamps else None,
                "open": len(open_positions),
                "by_rule": by_rule,
                "breakdown": breakdown,
            }
