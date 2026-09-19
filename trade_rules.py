"""Explicit trading rules for the live executor."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Optional
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class LiveEntryRules:
    """Code-owned portfolio policy for the live RR1-3 weekday system."""

    version: str = "rr1-3-intraday-weekdays-v1"
    allowed_styles: tuple[str, ...] = ("INTRADAY",)
    allowed_side: str = "long"
    ordinary_crypto_only: bool = True
    risk_per_trade_pct: float = 1.0
    allow_position_additions: bool = False
    allow_same_symbol_side_reentry: bool = False
    max_concurrent_positions: int = 4
    max_open_risk_pct: float = 3.0

    def validate(self) -> None:
        if self.allowed_styles != ("INTRADAY",):
            raise ValueError("live entry policy must remain INTRADAY-only")
        if self.allowed_side != "long":
            raise ValueError("live entry policy must remain LONG-only")
        if not (0 < self.risk_per_trade_pct <= 10):
            raise ValueError("risk_per_trade_pct must be in (0, 10]")
        if self.allow_position_additions:
            raise ValueError("position additions would exceed the fixed entry risk")
        if self.allow_same_symbol_side_reentry:
            raise ValueError("same-side re-entry would aggregate position risk")
        if self.max_concurrent_positions != 4:
            raise ValueError("live system must use four concurrent slots")
        if self.max_open_risk_pct != 3.0:
            raise ValueError("live system must cap open risk at 3%")


@dataclass(frozen=True)
class StrategyDecision:
    rule_id: str
    risk_pct: float
    target_split: tuple[float, float, float]
    live_partial_exit_ready: bool
    breakeven_buffer_r: float = 0.0


A1 = StrategyDecision("A1", 0.70, (1.0, 0.0, 0.0), False)
A4 = StrategyDecision("A4", 0.70, (1.0, 0.0, 0.0), False)
A5 = StrategyDecision("A5", 0.50, (1.0, 0.0, 0.0), False)
A3 = StrategyDecision("A3", 0.60, (0.40, 0.30, 0.30), True, 0.05)
RR1_3 = StrategyDecision("RR1_3", 1.0, (1.0, 0.0, 0.0), False)
KYIV_TZ = ZoneInfo("Europe/Kyiv")


def _kyiv_minute(now: Optional[datetime]) -> int:
    current = now or datetime.now(KYIV_TZ)
    current = current.replace(tzinfo=KYIV_TZ) if current.tzinfo is None else current.astimezone(KYIV_TZ)
    return current.hour * 60 + current.minute


def _calibrated_probability(text: str) -> Optional[float]:
    if re.search(r"(?:калібрування|calibration)\s*:\s*(?:навчання|training)", text, re.I):
        return None
    match = re.search(r"P\s*\(\s*TP1[^)]*SL\s*\)\s*:\s*(?:<[^>]+>)*\s*(\d+(?:[.,]\d+)?)\s*%", text, re.I)
    return float(match.group(1).replace(",", ".")) if match else None


def _bullish_orderflow(text: str) -> bool:
    return bool(
        re.search(r"Order\s*Flow\s*imbalance[^\n]{0,160}(?:на\s+користь\s+bullish|bullish)", text, re.I)
        or re.search(r"✅\s*Bid\s*/\s*Ask\s+imbalance", text, re.I)
    )


def select_strategy(
    *, style: str, side: str, signal_text: str, rr1: float,
    stop_distance_pct: float, now: Optional[datetime] = None,
) -> tuple[Optional[StrategyDecision], Optional[str]]:
    """Select the only live setup: weekday LONG INTRADAY with 1<=RR1<=3."""
    normalized_style = str(style or "").strip().upper()
    if str(side or "").strip().lower() != "long":
        return None, "live RR1-3 strategy allows LONG only"
    if normalized_style not in ENTRY_RULES.allowed_styles:
        return None, "live RR1-3 strategy allows INTRADAY only"
    try:
        rr = float(rr1)
        stop_pct = float(stop_distance_pct)
    except (TypeError, ValueError):
        return None, "signal Entry, SL and TP1 are required"
    if rr <= 0 or stop_pct <= 0:
        return None, "signal RR1 and stop distance must be positive"

    current = now or datetime.now(KYIV_TZ)
    current = current.replace(tzinfo=KYIV_TZ) if current.tzinfo is None else current.astimezone(KYIV_TZ)
    minute = current.hour * 60 + current.minute
    if current.weekday() >= 5:
        return None, "live RR1-3 strategy trades Monday-Friday only"
    if not (10 * 60 <= minute <= 16 * 60 + 30):
        return None, "live RR1-3 strategy trades 10:00-16:30 Kyiv only"
    if not (1.0 <= rr <= 3.0):
        return None, "live RR1-3 strategy requires 1<=RR1<=3"
    return RR1_3, None


def select_legacy_combined_strategy(
    *, style: str, side: str, signal_text: str, rr1: float,
    stop_distance_pct: float, now: Optional[datetime] = None,
) -> tuple[Optional[StrategyDecision], Optional[str]]:
    """Select the former A1>A4>A5>A3 system for shadow tracking only."""
    normalized_style = str(style or "").strip().upper()
    if str(side or "").strip().lower() != "long":
        return None, "legacy shadow strategy allows LONG only"
    if normalized_style not in ("SCALP", "INTRADAY"):
        return None, "legacy shadow strategy allows SCALP or INTRADAY only"
    try:
        rr = float(rr1)
        stop_pct = float(stop_distance_pct)
    except (TypeError, ValueError):
        return None, "signal Entry, SL and TP1 are required"
    if rr <= 0 or stop_pct <= 0:
        return None, "signal RR1 and stop distance must be positive"

    in_session = 16 * 60 + 30 <= _kyiv_minute(now) <= 18 * 60 + 30
    if normalized_style == "SCALP" and in_session:
        probability = _calibrated_probability(signal_text or "")
        if probability is not None and probability >= 55.0:
            return A1, None
        if _bullish_orderflow(signal_text or ""):
            return A4, None
    if normalized_style == "SCALP" and 0.795 <= rr < 1.0 and stop_pct >= 6.0:
        return A5, None
    if normalized_style == "INTRADAY" and rr < 2.0 and 3.0 <= stop_pct < 4.0:
        return A3, None
    return None, "signal does not match legacy A1, A4, A5 or A3"


@dataclass(frozen=True)
class ShadowS2Rules:
    """S2 observation-only policy; it must never place exchange orders."""

    version: str = "s2-shadow-scalp-long-probability-session"
    allowed_styles: tuple[str, ...] = ("SCALP",)
    allowed_side: str = "long"
    probability_min_inclusive: float = 55.0
    start_minute_kyiv: int = 16 * 60 + 30
    end_minute_kyiv: int = 18 * 60 + 30
    risk_per_trade_pct: float = 0.5
    round_trip_cost_notional: float = 0.0014

    def validate(self) -> None:
        if self.allowed_styles != ("SCALP",) or self.allowed_side != "long":
            raise ValueError("S2 shadow policy must remain SCALP LONG")
        if not (0 <= self.probability_min_inclusive <= 100):
            raise ValueError("S2 probability threshold must be in [0, 100]")
        if not (0 <= self.start_minute_kyiv < self.end_minute_kyiv <= 24 * 60):
            raise ValueError("S2 Kyiv session is invalid")
        if not (0 < self.risk_per_trade_pct <= 10):
            raise ValueError("S2 shadow risk must be in (0, 10]")
        if not (0 <= self.round_trip_cost_notional < 1):
            raise ValueError("S2 cost rate must be in [0, 1)")


@dataclass(frozen=True)
class ShadowS1Rules:
    """Disabled live S1 retained as an observation-only balanced strategy."""

    version: str = "s1-shadow-scalp-long-rr-session-balanced"
    allowed_styles: tuple[str, ...] = ("SCALP",)
    allowed_side: str = "long"
    rr1_min_inclusive: float = 0.8
    rr1_max_exclusive: float = 0.95
    start_minute_kyiv: int = 10 * 60
    end_minute_kyiv: int = 23 * 60
    risk_per_trade_pct: float = 0.5
    target_split: tuple[float, float, float] = (0.40, 0.30, 0.30)
    breakeven_buffer_r: float = 0.05
    round_trip_cost_notional: float = 0.0014

    def validate(self) -> None:
        if self.allowed_styles != ("SCALP",) or self.allowed_side != "long":
            raise ValueError("S1 shadow policy must remain SCALP LONG")
        if not (0 < self.rr1_min_inclusive < self.rr1_max_exclusive):
            raise ValueError("S1 RR range is invalid")
        if abs(sum(self.target_split) - 1.0) > 1e-9:
            raise ValueError("S1 shadow target split must sum to 1")
@dataclass(frozen=True)
class SwingTradingRules:
    """Rule 1 - Swing: retained three-target swing execution model."""

    version: str = "rule-1-swing"
    allowed_styles: tuple[str, ...] = ("SWING",)
    allowed_side: str = "long"
    ordinary_crypto_only: bool = True
    allow_kyiv_00_06: bool = True
    risk_per_trade_pct: float = 0.5
    use_order_book_for_targets: bool = False

    # Live SWING exit model.
    target_rr: tuple[float, float, float] = (2.0, 3.0, 5.0)
    target_split: tuple[float, float, float] = (0.40, 0.30, 0.30)
    move_sl_to_breakeven_after_tp1: bool = True
    # Keep a small part of the original risk after TP1 so fees/noise around the
    # entry do not turn the remainder into an immediate stop-out.
    breakeven_buffer_r: float = 0.08

    # Reject a market entry when price has already moved too far beyond the
    # signal entry. This keeps a planned 2R TP1 from collapsing after delay.
    max_adverse_entry_drift_r: float = 0.25

    live_partial_exit_ready: bool = True

    # Observed on a very small sample; collect it, do not block by it yet.
    shadow_volume_spike_min: float = 2.0

    def validate(self) -> None:
        if self.allowed_styles != ("SWING",):
            raise ValueError("Rule 1 must remain SWING-only")
        if self.allowed_side != "long":
            raise ValueError("SWING policy must remain LONG-only")
        if not (0 < self.risk_per_trade_pct <= 10):
            raise ValueError("SWING risk_per_trade_pct must be in (0, 10]")
        if abs(sum(self.target_split) - 1.0) > 1e-9:
            raise ValueError("SWING target split must sum to 1.0")
        if sorted(self.target_rr) != list(self.target_rr):
            raise ValueError("SWING RR targets must be ordered")
        if not (0 <= self.breakeven_buffer_r < 1):
            raise ValueError("SWING breakeven_buffer_r must be in [0, 1)")
        if not (0 <= self.max_adverse_entry_drift_r < 1):
            raise ValueError("SWING max_adverse_entry_drift_r must be in [0, 1)")


ENTRY_RULES = LiveEntryRules()
ENTRY_RULES.validate()

SHADOW_S2_RULES = ShadowS2Rules()
SHADOW_S2_RULES.validate()

SHADOW_S1_RULES = ShadowS1Rules()
SHADOW_S1_RULES.validate()

SWING_RULES = SwingTradingRules()
SWING_RULES.validate()


def risk_pct_for_style(style: str | None, fallback: float) -> float:
    normalized_style = str(style or "").strip().upper()
    if normalized_style in ENTRY_RULES.allowed_styles:
        return ENTRY_RULES.risk_per_trade_pct
    if normalized_style in SWING_RULES.allowed_styles:
        return SWING_RULES.risk_per_trade_pct
    return float(fallback)
