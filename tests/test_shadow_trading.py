import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from shadow_trading import (
    LegacyCombinedShadowBook, ShadowBook, ShadowS1Book, extract_calibrated_probability,
    qualifies_s1, qualifies_s2,
)
from trade_rules import A3, A5


class ShadowS2Tests(unittest.TestCase):
    def test_extracts_only_calibrated_probability(self):
        text = "📈 P(TP1 раніше SL): 58.6% (орієнт. 95% інтервал)"
        self.assertEqual(extract_calibrated_probability(text), 58.6)
        self.assertIsNone(extract_calibrated_probability("🧮 Setup score: 73%"))

    def test_session_is_kyiv_1630_inclusive_1830_exclusive(self):
        tz = ZoneInfo("Europe/Kiev")
        self.assertTrue(qualifies_s2(style="SCALP", side="long", probability=55, now_kyiv=datetime(2026, 9, 10, 16, 30, tzinfo=tz)))
        self.assertFalse(qualifies_s2(style="SCALP", side="long", probability=55, now_kyiv=datetime(2026, 9, 10, 18, 30, tzinfo=tz)))

    def test_records_shadow_tp_without_exchange_access(self):
        with tempfile.TemporaryDirectory() as directory:
            book = ShadowBook(Path(directory) / "shadow.json")
            self.assertTrue(book.register(signal_key="x", base="BTC", entry=100, sl=90, tp1=108, probability=60))
            self.assertIsNone(book.observe("x", 105))
            trade = book.observe("x", 108)
            self.assertEqual(trade["status"], "tp1_hit")
            self.assertAlmostEqual(trade["gross_r"], 0.8)
            self.assertEqual(book.summary()["wins"], 1)


class ShadowS1Tests(unittest.TestCase):
    def test_s1_session_and_rr_filter(self):
        tz = ZoneInfo("Europe/Kiev")
        self.assertTrue(qualifies_s1(style="SCALP", side="long", rr1=0.8, now_kyiv=datetime(2026, 9, 10, 10, 0, tzinfo=tz)))
        self.assertFalse(qualifies_s1(style="SCALP", side="long", rr1=0.95, now_kyiv=datetime(2026, 9, 10, 10, 0, tzinfo=tz)))
        self.assertFalse(qualifies_s1(style="SCALP", side="long", rr1=0.9, now_kyiv=datetime(2026, 9, 10, 23, 0, tzinfo=tz)))

    def test_s1_tracks_partial_then_breakeven_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            book = ShadowS1Book(Path(directory) / "s1.json")
            book.register(signal_key="s1", base="ETH", entry=100, sl=90, tp1=108, tp2=120, tp3=130)
            self.assertIsNone(book.observe("s1", 108))
            trade = book.observe("s1", 99.5)
            self.assertEqual(trade["status"], "breakeven_exit")
            self.assertAlmostEqual(trade["gross_r"], 0.29)
            self.assertEqual(book.summary()["wins"], 1)

    def test_s1_tracks_all_three_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            book = ShadowS1Book(Path(directory) / "s1.json")
            book.register(signal_key="s1", base="ETH", entry=100, sl=90, tp1=108, tp2=120, tp3=130)
            trade = book.observe("s1", 130)
            self.assertEqual(trade["status"], "tp3_hit")
            self.assertAlmostEqual(trade["gross_r"], 1.82)


class LegacyCombinedShadowBookTests(unittest.TestCase):
    def test_tracks_full_tp1_module_from_1000_balance(self):
        with tempfile.TemporaryDirectory() as directory:
            book = LegacyCombinedShadowBook(Path(directory) / "legacy.json")
            book.register(
                signal_key="a5", base="SOL", entry=100, sl=90, tp1=108,
                tp2=None, tp3=None, decision=A5,
            )
            trade = book.observe("a5", 108)
            self.assertEqual(trade["strategy"], "A5")
            self.assertEqual(trade["status"], "tp1_hit")
            self.assertGreater(book.summary()["balance"], 1000)

    def test_tracks_balanced_a3_module(self):
        with tempfile.TemporaryDirectory() as directory:
            book = LegacyCombinedShadowBook(Path(directory) / "legacy.json")
            book.register(
                signal_key="a3", base="ETH", entry=100, sl=90,
                tp1=108, tp2=120, tp3=130, decision=A3,
            )
            self.assertIsNone(book.observe("a3", 108))
            trade = book.observe("a3", 99.5)
            self.assertEqual(trade["strategy"], "A3")
            self.assertEqual(trade["status"], "breakeven_exit")


if __name__ == "__main__":
    unittest.main()
