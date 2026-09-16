import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from trade_rules import select_strategy


KYIV = ZoneInfo("Europe/Kyiv")


class CombinedStrategyTests(unittest.TestCase):
    def select(self, **overrides):
        values = {
            "style": "SCALP",
            "side": "long",
            "signal_text": "",
            "rr1": 0.8,
            "stop_distance_pct": 6.0,
            "now": datetime(2026, 9, 16, 17, 0, tzinfo=KYIV),
        }
        values.update(overrides)
        return select_strategy(**values)

    def test_a1_has_priority_and_requires_calibrated_probability(self):
        decision, reason = self.select(
            signal_text="📈 P(TP1 раніше SL): <b>56.2%</b>\nКалібрування: segment 42"
        )
        self.assertIsNone(reason)
        self.assertEqual(decision.rule_id, "A1")
        self.assertEqual(decision.risk_pct, 0.70)

        decision, _ = self.select(
            signal_text="📈 P(TP1 раніше SL): 80%\nКалібрування: навчання"
        )
        self.assertEqual(decision.rule_id, "A5")

    def test_a4_matches_confirmed_bullish_orderflow(self):
        decision, reason = self.select(
            rr1=1.2,
            stop_distance_pct=2.0,
            signal_text="Order Flow imbalance на користь bullish",
        )
        self.assertIsNone(reason)
        self.assertEqual(decision.rule_id, "A4")

    def test_a3_uses_balanced_exit_and_its_risk(self):
        decision, reason = self.select(
            style="INTRADAY", rr1=1.8, stop_distance_pct=3.5
        )
        self.assertIsNone(reason)
        self.assertEqual(decision.rule_id, "A3")
        self.assertEqual(decision.risk_pct, 0.60)
        self.assertEqual(decision.target_split, (0.40, 0.30, 0.30))
        self.assertTrue(decision.live_partial_exit_ready)

    def test_short_and_unmatched_signal_are_rejected(self):
        self.assertIsNone(self.select(side="short")[0])
        self.assertIsNone(self.select(rr1=1.2, stop_distance_pct=2.0)[0])


if __name__ == "__main__":
    unittest.main()
