import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from trade_rules import select_legacy_combined_strategy, select_strategy


KYIV = ZoneInfo("Europe/Kyiv")


class LiveRRStrategyTests(unittest.TestCase):
    def select(self, **overrides):
        values = {
            "style": "INTRADAY",
            "side": "long",
            "signal_text": "",
            "rr1": 2.0,
            "stop_distance_pct": 4.0,
            "now": datetime(2026, 9, 16, 12, 0, tzinfo=KYIV),
        }
        values.update(overrides)
        return select_strategy(**values)

    def test_accepts_weekday_long_intraday_rr_one_to_three(self):
        for rr1 in (1.0, 2.0, 3.0):
            decision, reason = self.select(rr1=rr1)
            self.assertIsNone(reason)
            self.assertEqual(decision.rule_id, "RR1_3")
            self.assertEqual(decision.risk_pct, 1.0)
            self.assertEqual(decision.target_split, (1.0, 0.0, 0.0))
            self.assertFalse(decision.live_partial_exit_ready)

    def test_rejects_outside_rr_session_weekday_style_or_side(self):
        cases = (
            {"rr1": 0.999}, {"rr1": 3.001}, {"style": "SCALP"}, {"side": "short"},
            {"now": datetime(2026, 9, 16, 9, 59, tzinfo=KYIV)},
            {"now": datetime(2026, 9, 16, 16, 31, tzinfo=KYIV)},
            {"now": datetime(2026, 9, 19, 12, 0, tzinfo=KYIV)},
        )
        for overrides in cases:
            decision, reason = self.select(**overrides)
            self.assertIsNone(decision)
            self.assertIsNotNone(reason)


class LegacyCombinedShadowTests(unittest.TestCase):
    def select(self, **overrides):
        values = {
            "style": "SCALP", "side": "long", "signal_text": "",
            "rr1": 0.8, "stop_distance_pct": 6.0,
            "now": datetime(2026, 9, 16, 17, 0, tzinfo=KYIV),
        }
        values.update(overrides)
        return select_legacy_combined_strategy(**values)

    def test_legacy_a1_a4_a5_a3_are_still_selected_for_shadow(self):
        decision, _ = self.select(signal_text="P(TP1 раніше SL): 56.2%\nКалібрування: segment 42")
        self.assertEqual(decision.rule_id, "A1")
        decision, _ = self.select(rr1=1.2, stop_distance_pct=2.0, signal_text="Order Flow imbalance на користь bullish")
        self.assertEqual(decision.rule_id, "A4")
        decision, _ = self.select(now=datetime(2026, 9, 16, 12, 0, tzinfo=KYIV))
        self.assertEqual(decision.rule_id, "A5")
        decision, _ = self.select(style="INTRADAY", rr1=1.8, stop_distance_pct=3.5)
        self.assertEqual(decision.rule_id, "A3")
        self.assertTrue(decision.live_partial_exit_ready)


if __name__ == "__main__":
    unittest.main()
