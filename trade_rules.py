"""Explicit trading rules for the live executor."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SwingTradingRules:
    version: str = "swing-long-crypto-v2"
    allowed_side: str = "long"
    ordinary_crypto_only: bool = True
    allow_kyiv_00_06: bool = True
    risk_per_trade_pct: float = 0.5
    accepted_six_trade_batch_risk_pct: float = 3.0
    use_order_book_for_targets: bool = False

    # Live SWING exit model.
    target_rr: tuple[float, float, float] = (2.0, 3.0, 5.0)
    target_split: tuple[float, float, float] = (0.40, 0.30, 0.30)
    move_sl_to_breakeven_after_tp1: bool = True
    # Keep a small part of the original risk after TP1 so fees/noise around the
    # entry do not turn the remainder into an immediate stop-out.
    breakeven_buffer_r: float = 0.08

    live_partial_exit_ready: bool = True

    # Observed on a very small sample; collect it, do not block by it yet.
    shadow_volume_spike_min: float = 2.0

    def validate(self) -> None:
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
        if self.accepted_six_trade_batch_risk_pct < self.risk_per_trade_pct * 6:
            raise ValueError("SWING batch allowance must cover six configured risks")


SWING_RULES = SwingTradingRules()
SWING_RULES.validate()


def risk_pct_for_style(style: str | None, fallback: float) -> float:
    if str(style or "").strip().upper() == "SWING":
        return SWING_RULES.risk_per_trade_pct
    return float(fallback)
