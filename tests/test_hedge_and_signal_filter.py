import asyncio
import importlib
import os
import sys
import types
import unittest


if importlib.util.find_spec("requests") is None:
    requests_stub = types.ModuleType("requests")
    requests_stub.get = lambda *args, **kwargs: None
    sys.modules["requests"] = requests_stub


class _Filter:
    def __and__(self, other):
        return self

    def __or__(self, other):
        return self


class _Filters:
    text = _Filter()
    caption = _Filter()
    photo = _Filter()

    @staticmethod
    def chat(_):
        return _Filter()


class _Client:
    def __init__(self, *args, **kwargs):
        pass

    def on_message(self, _):
        return lambda func: func


class _BingX:
    def __init__(self, _config):
        self.positions = []
        self.open_orders = []
        self.canceled = []

    def fetch_positions(self, _symbols=None):
        return self.positions

    def fetch_open_orders(self, _symbol):
        return self.open_orders

    def cancel_order(self, order_id, _symbol):
        self.canceled.append(str(order_id))

    @staticmethod
    def amount_to_precision(_symbol, qty):
        return str(qty)


def _load_trade_module():
    os.environ.setdefault("TG_API_ID", "1")
    os.environ.setdefault("TG_API_HASH", "test")
    os.environ.setdefault("TG_SESSION_STRING", "test")

    ccxt_stub = types.ModuleType("ccxt")
    ccxt_stub.bingx = _BingX
    sys.modules["ccxt"] = ccxt_stub

    pyrogram_stub = types.ModuleType("pyrogram")
    pyrogram_stub.Client = _Client
    pyrogram_stub.filters = _Filters()
    pyrogram_stub.idle = lambda: None
    sys.modules["pyrogram"] = pyrogram_stub

    errors_stub = types.ModuleType("pyrogram.errors")
    errors_stub.PeerIdInvalid = type("PeerIdInvalid", (Exception,), {})
    errors_stub.FloodWait = type("FloodWait", (Exception,), {})
    errors_stub.RPCError = type("RPCError", (Exception,), {})
    sys.modules["pyrogram.errors"] = errors_stub

    sys.modules.pop("trade_bingx", None)
    return importlib.import_module("trade_bingx")


class SignalFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = _load_trade_module()

    def test_only_scalp_signal_header_is_allowed(self):
        intraday = "📈 INTRADAY  SHORT 🔴 — SUI/USDT:USDT\nTF: 4H / 1H / 15M"
        scalp = "⚡ SCALP  LONG 🟢 — SUI/USDT:USDT\nTF: 1H / 15M / 5M"
        stats = "📊 Paper-trading статистика\n⚡ SCALP: угод 325\n📈 INTRADAY: угод 180"

        self.assertFalse(self.bot.is_allowed_signal_style(intraday))
        self.assertTrue(self.bot.is_allowed_signal_style(scalp))
        self.assertIsNone(self.bot.extract_signal_style(stats))

    def test_liquidation_is_extracted_from_source_message(self):
        text = "⚡ SCALP LONG\n💧 Орієнт. ліквід: 0.507049"
        self.assertEqual(self.bot.extract_liquidation_from_text(text), 0.507049)

    def test_structured_feed_parses_tp1_and_ignores_later_targets(self):
        text = """⚡ SCALP  LONG 🟢  —  SUI/USDT:USDT
TF: 1H / 15M / 5M
📍 Entry:   0.726500
🛑 SL:      0.698735  (3.82%)
🎯 TP1:     0.748712  +3.2 USDT
🎯 TP2:     0.768148  +4.5 USDT
🎯 TP3:     0.795913  +7.5 USDT
💼 Баланс:  1000.0 USDT | ризик: 10.0 USDT (1.0%)
📦 Позиція: 261.66 USDT | маржа: 87.22 USDT
⚡ Плече:   3x
💧 Орієнт. ліквід: 0.507049
"""
        parsed = self.bot.parse_structured_scalp_signal(text)

        self.assertEqual(parsed["action"], "OPEN")
        self.assertEqual(parsed["base"], "SUI")
        self.assertEqual(parsed["side"], "long")
        self.assertEqual(parsed["tp"], 0.748712)
        self.assertNotEqual(parsed["tp"], 0.768148)
        self.assertEqual(parsed["position_usdt"], 261.66)
        self.assertEqual(parsed["balance_usdt"], 1000.0)
        self.assertEqual(parsed["leverage"], 3)

    def test_auto_plan_risks_half_percent_and_adapts_leverage(self):
        plan = self.bot.calculate_auto_trade_plan(
            1000.0,
            0.726500,
            0.698735,
            0.748712,
        )

        self.assertEqual(plan["risk_budget"], 5.0)
        self.assertAlmostEqual(plan["expected_loss_at_sl"], 5.0, places=8)
        self.assertEqual(plan["leverage"], 8)
        self.assertAlmostEqual(plan["expected_profit_at_tp1"], 4.0, places=2)
        self.assertLess(plan["margin"], plan["notional"])

        wide_target = self.bot.calculate_auto_trade_plan(1000.0, 100.0, 96.0, 120.0)
        self.assertLess(wide_target["leverage"], plan["leverage"])


class HedgeIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = _load_trade_module()

    def setUp(self):
        self.exchange = _BingX({})
        self.bot.exchange = self.exchange

    def test_position_lookup_selects_requested_hedge_side(self):
        self.exchange.positions = [
            {"symbol": "SUI/USDT:USDT", "side": "short", "contracts": 10},
            {"symbol": "SUI/USDT:USDT", "side": "long", "contracts": 2},
        ]

        selected = self.bot.fetch_position_oneway_sync("SUI/USDT:USDT", "long")
        self.assertEqual(selected["side"], "long")

    def test_cancel_sltp_never_touches_opposite_or_unknown_side(self):
        self.exchange.open_orders = [
            {"id": "short-sl", "type": "stop_market", "info": {"positionSide": "SHORT"}},
            {"id": "long-sl", "type": "stop_market", "info": {"positionSide": "LONG"}},
            {"id": "unknown-sl", "type": "stop_market", "info": {}},
        ]

        self.bot._cancel_existing_sltp_sync("SUI/USDT:USDT", "long")
        self.assertEqual(self.exchange.canceled, ["long-sl"])


class NotifierHedgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_notifier_tracks_both_sides_of_same_symbol(self):
        notifier = importlib.import_module("trade_notifier")
        notifier.LAST_POSITIONS = {}
        exchange = _BingX({})
        exchange.positions = [
            {"symbol": "SUI/USDT:USDT", "side": "short", "contracts": 10},
            {"symbol": "SUI/USDT:USDT", "side": "long", "contracts": 2},
        ]

        positions = await notifier._fetch_positions_map(exchange)
        self.assertEqual(set(positions), {"SUI/USDT:USDT:short", "SUI/USDT:USDT:long"})


if __name__ == "__main__":
    unittest.main()
