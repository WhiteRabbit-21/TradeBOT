"""Explicit trading rules for the live executor."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LiveEntryRules:
    """Rule 2A - Scalping Balanced, all BingX-listed assets."""

    version: str = "rule-2a-scalping-balanced-all-assets"
    allowed_styles: tuple[str, ...] = ("SCALP",)
    allowed_side: str = "long"
    ordinary_crypto_only: bool = False
    risk_per_trade_pct: float = 0.5
    rr1_min_inclusive: float = 0.8
    rr1_max_exclusive: float = 1.0
    target_split: tuple[float, float, float] = (0.40, 0.30, 0.30)
    move_sl_to_breakeven_after_tp1: bool = True
    breakeven_buffer_r: float = 0.05
    live_partial_exit_ready: bool = True
    allow_position_additions: bool = False
    # BingX aggregates repeated same-side orders into one position. Rejecting a
    # duplicate preserves the first trade's SL and its fixed 0.5% risk budget.
    allow_same_symbol_side_reentry: bool = False
    # None is intentional: the user accepted aggregate risk from any number of
    # simultaneous signals. Each individual position still risks only 0.5%.
    max_concurrent_positions: int | None = None

    def validate(self) -> None:
        if self.allowed_styles != ("SCALP",):
            raise ValueError("live entry policy must remain SCALP-only")
        if self.allowed_side != "long":
            raise ValueError("live entry policy must remain LONG-only")
        if not (0 < self.risk_per_trade_pct <= 10):
            raise ValueError("risk_per_trade_pct must be in (0, 10]")
        if not (0 < self.rr1_min_inclusive < self.rr1_max_exclusive):
            raise ValueError("RR1 range must be positive and ordered")
        if abs(sum(self.target_split) - 1.0) > 1e-9:
            raise ValueError("target split must sum to 1.0")
        if not self.live_partial_exit_ready:
            raise ValueError("Rule 2A requires live 40/30/30 partial exits")
        if not self.move_sl_to_breakeven_after_tp1:
            raise ValueError("Rule 2A requires protective SL movement after TP1")
        if not (0 <= self.breakeven_buffer_r < 1):
            raise ValueError("breakeven_buffer_r must be in [0, 1)")
        if self.allow_position_additions:
            raise ValueError("position additions would exceed the fixed entry risk")
        if self.allow_same_symbol_side_reentry:
            raise ValueError("same-side re-entry would aggregate position risk")
        if self.max_concurrent_positions is not None:
            raise ValueError("live entry policy must not impose a position-count cap")


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

SWING_RULES = SwingTradingRules()
SWING_RULES.validate()


def risk_pct_for_style(style: str | None, fallback: float) -> float:
    normalized_style = str(style or "").strip().upper()
    if normalized_style in ENTRY_RULES.allowed_styles:
        return ENTRY_RULES.risk_per_trade_pct
    if normalized_style in SWING_RULES.allowed_styles:
        return SWING_RULES.risk_per_trade_pct
    return float(fallback)
