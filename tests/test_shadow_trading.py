import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from shadow_trading import ShadowBook, extract_calibrated_probability, qualifies_s2


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


if __name__ == "__main__":
    unittest.main()
