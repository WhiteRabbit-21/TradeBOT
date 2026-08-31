import asyncio
import importlib
import os
import sys
import types
import unittest
from unittest import mock
from datetime import datetime
from zoneinfo import ZoneInfo


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
    last_chat = None

    @classmethod
    def chat(cls, chat_id):
        cls.last_chat = chat_id
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
        self.markets = {}
        self.refreshed_markets = None
        self.load_markets_calls = []

    def load_markets(self, reload=False):
        self.load_markets_calls.append(bool(reload))
        if reload and self.refreshed_markets is not None:
            self.markets = self.refreshed_markets
        return self.markets

    def market(self, symbol):
        return self.markets[symbol]

    def fetch_positions(self, _symbols=None):
        return self.positions

    def fetch_open_orders(self, _symbol):
        return self.open_orders

    def cancel_order(self, order_id, _symbol):
        self.canceled.append(str(order_id))

    @staticmethod
    def amount_to_precision(_symbol, qty):
        return str(qty)

    @staticmethod
    def price_to_precision(_symbol, price):
        return str(price)


def _load_trade_module():
    os.environ.setdefault("TG_API_ID", "1")
    os.environ.setdefault("TG_API_HASH", "test")
    os.environ.setdefault("TG_SESSION_STRING", "test")
    # Deliberately stale v3 variables: v4 entry policy must ignore them.
    os.environ["TRADE_LONG_ALL_STYLES"] = "0"
    os.environ["ALLOWED_SIGNAL_STYLES"] = "SWING"
    os.environ["FIXED_RISK_PCT"] = "9.0"
    os.environ.pop("TARGET_CHAT_ID", None)

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

    def test_rule_one_swing_and_rule_two_scalp_are_allowed(self):
        intraday = "📈 INTRADAY  LONG 🟢 — SUI/USDT:USDT\nTF: 4H / 1H / 15M"
        scalp = "⚡ SCALP  LONG 🟢 — SUI/USDT:USDT\nTF: 1H / 15M / 5M"
        swing = "🌊 SWING  LONG 🟢 — SUI/USDT:USDT\nTF: 1D / 4H / 1H"
        stats = "📊 Paper-trading статистика\n⚡ SCALP: угод 325\n📈 INTRADAY: угод 180"

        self.assertFalse(self.bot.is_allowed_signal_style(intraday))
        self.assertTrue(self.bot.is_allowed_signal_style(scalp))
        self.assertTrue(self.bot.is_allowed_signal_style(swing))
        self.assertIsNone(self.bot.extract_signal_style(stats))

    def test_only_signalbot_channel_is_registered_as_source(self):
        self.assertEqual(self.bot.TARGET_CHAT_ID, -5486330898)
        self.assertEqual(_Filters.last_chat, -5486330898)

    def test_non_scalp_management_event_is_not_mistaken_for_new_entry(self):
        event = "🟢 TP1 ДОСЯГНУТО — позиція ще відкрита\nСтиль: SWING | LONG"
        signal = """🌊 SWING LONG 🟢 — SUI/USDT:USDT
📍 Entry: 1.00
🛑 SL: 0.95
🎯 TP1: 1.05
"""
        self.assertFalse(self.bot.is_new_entry_signal_text(event))
        self.assertTrue(self.bot.is_new_entry_signal_text(signal))

    def test_non_crypto_assets_are_blocked_but_crypto_is_allowed(self):
        for base in ("SNDK", "MU", "MUU", "SKHY", "ZHIPU", "SPY", "XAU"):
            reason = self.bot.non_crypto_open_block_reason(base, style="SWING")
            self.assertIsNotNone(reason, base)
            self.assertIn("ordinary crypto assets only", reason)
            self.assertIsNone(
                self.bot.non_crypto_open_block_reason(base, style="SCALP"),
                base,
            )

        for base in ("BTC", "HYPE", "ONDO", "1000SHIB", "DOGE"):
            self.assertIsNone(self.bot.non_crypto_open_block_reason(base), base)

    def test_non_crypto_filter_uses_signal_marker_and_market_metadata(self):
        marked = "🌊 SWING LONG\n🏷 Тип активу: TradFi — акція"
        self.assertIsNotNone(
            self.bot.non_crypto_open_block_reason("FUTURETICKER", marked)
        )

        self.bot.exchange.markets = {
            "NEWSTOCK/USDT:USDT": {
                "symbol": "NEWSTOCK/USDT:USDT",
                "info": {"underlyingType": "Equity Securities"},
            }
        }
        self.assertIsNotNone(
            self.bot.non_crypto_open_block_reason(
                "NEWSTOCK", symbol="NEWSTOCK/USDT:USDT"
            )
        )

    def test_liquidation_is_extracted_from_source_message(self):
        text = "⚡ SCALP LONG\n💧 Орієнт. ліквід: 0.507049"
        self.assertEqual(self.bot.extract_liquidation_from_text(text), 0.507049)

    def test_structured_scalp_feed_parses_all_balanced_targets(self):
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
        self.assertEqual(parsed["tp2"], 0.768148)
        self.assertEqual(parsed["tp3"], 0.795913)
        self.assertEqual(parsed["position_usdt"], 261.66)
        self.assertEqual(parsed["balance_usdt"], 1000.0)
        self.assertEqual(parsed["leverage"], 3)

    def test_structured_parser_supports_intraday_and_swing_long(self):
        for style in ("INTRADAY", "SWING"):
            text = f"""📈 {style} LONG 🟢 — HYPE/USDT:USDT
📍 Entry: 30.9600
🛑 SL: 29.9745
🎯 TP1: 32.1114
🎯 TP2: 32.5164
🎯 TP3: 33.4237
"""
            parsed = self.bot.parse_structured_signal(text)
            self.assertEqual(parsed["action"], "OPEN")
            self.assertEqual(parsed["base"], "HYPE")
            self.assertEqual(parsed["side"], "long")
            self.assertEqual(parsed["tp"], 32.1114)
            if style == "SWING":
                self.assertEqual(parsed["tp2"], 32.5164)
                self.assertEqual(parsed["tp3"], 33.4237)
            else:
                self.assertIsNone(parsed["tp2"])
                self.assertIsNone(parsed["tp3"])

    def test_open_policy_allows_scalp_long_only_outside_sleep_window(self):
        kyiv = ZoneInfo("Europe/Kyiv")
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "short",
                datetime(2026, 8, 26, 12, 0, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 0, 0, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 5, 59, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 12, 0, tzinfo=kyiv),
                style="INTRADAY",
                require_allowed_style=True,
            )
        )
        self.assertIsNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 12, 0, tzinfo=kyiv),
                style="SWING",
                require_allowed_style=True,
            )
        )
        self.assertIsNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 6, 0, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )
        self.assertIsNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 23, 59, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )

    def test_full_tp1_rr_policy_is_exactly_point_eight_to_below_one(self):
        kyiv = ZoneInfo("Europe/Kyiv")
        noon = datetime(2026, 8, 26, 12, 0, tzinfo=kyiv)

        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long", noon, style="SCALP", rr1=0.79, require_allowed_style=True
            )
        )
        self.assertIsNone(
            self.bot.open_policy_block_reason(
                "long",
                noon,
                style="SCALP",
                rr1=0.799999999,
                require_allowed_style=True,
            )
        )
        for rr1 in (0.8, 0.9, 0.999999):
            self.assertIsNone(
                self.bot.open_policy_block_reason(
                    "long",
                    noon,
                    style="SCALP",
                    rr1=rr1,
                    require_allowed_style=True,
                )
            )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long", noon, style="SCALP", rr1=1.0, require_allowed_style=True
            )
        )

    def test_rule_one_swing_is_allowed_at_night_and_keeps_its_own_rr_model(self):
        kyiv = ZoneInfo("Europe/Kyiv")
        night = datetime(2026, 8, 26, 2, 0, tzinfo=kyiv)

        for rr1 in (0.9, 2.0, 5.0):
            self.assertIsNone(
                self.bot.open_policy_block_reason(
                    "long",
                    night,
                    style="SWING",
                    rr1=rr1,
                    require_allowed_style=True,
                )
            )

    def test_effective_rr_is_calculated_from_execution_prices(self):
        self.assertAlmostEqual(
            self.bot.calculate_rr_from_prices(100.0, 96.0, 108.0, "long"),
            2.0,
        )
        self.assertAlmostEqual(
            self.bot.calculate_rr_from_prices(100.0, 104.0, 92.0, "short"),
            2.0,
        )

    def test_stale_swing_entry_is_blocked_in_r_units(self):
        self.assertIsNone(
            self.bot.swing_entry_drift_block_reason("long", 4.205, 4.21, 4.14)
        )
        reason = self.bot.swing_entry_drift_block_reason(
            "long", 4.205, 4.311, 4.14
        )
        self.assertIn("stale SWING entry", reason)
        self.assertIn("exceeds 0.25R", reason)

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

        tight_stop = self.bot.calculate_auto_trade_plan(1000.0, 100.0, 99.0, 100.8)
        wide_stop = self.bot.calculate_auto_trade_plan(1000.0, 100.0, 90.0, 108.0)
        self.assertAlmostEqual(tight_stop["expected_loss_at_sl"], 5.0, places=8)
        self.assertAlmostEqual(wide_stop["expected_loss_at_sl"], 5.0, places=8)
        self.assertGreater(tight_stop["notional"], wide_stop["notional"])

    def test_live_entry_rules_are_explicit_and_have_no_position_cap(self):
        rules = self.bot.ENTRY_RULES
        self.assertEqual(rules.version, "rule-2a-scalping-balanced-all-assets")
        self.assertEqual(rules.allowed_styles, ("SCALP",))
        self.assertEqual(rules.allowed_side, "long")
        self.assertFalse(rules.ordinary_crypto_only)
        self.assertEqual(rules.risk_per_trade_pct, 0.5)
        self.assertEqual(rules.rr1_min_inclusive, 0.8)
        self.assertEqual(rules.rr1_max_exclusive, 1.0)
        self.assertEqual(rules.target_split, (0.40, 0.30, 0.30))
        self.assertTrue(rules.move_sl_to_breakeven_after_tp1)
        self.assertEqual(rules.breakeven_buffer_r, 0.05)
        self.assertTrue(rules.live_partial_exit_ready)
        self.assertFalse(rules.allow_position_additions)
        self.assertFalse(rules.allow_same_symbol_side_reentry)
        self.assertIsNone(rules.max_concurrent_positions)
        self.assertEqual(self.bot.FIXED_RISK_PCT, 0.5)
        self.assertEqual(self.bot.ALLOWED_SIGNAL_STYLES, {"SCALP", "SWING"})
        self.assertEqual(self.bot.risk_pct_for_style("SCALP", 9.0), 0.5)
        self.assertEqual(self.bot.risk_pct_for_style("SWING", 9.0), 0.5)

    def test_position_addition_is_rejected_before_exchange_access(self):
        asyncio.run(
            self.bot.handle_ai_command(
                {
                    "action": "ADD",
                    "confidence": 1.0,
                    "base": "BTC",
                    "side": "long",
                    "add_pct": 0.5,
                    "_tg_text": "Add BTC",
                }
            )
        )

    def test_rule_one_swing_rules_are_active(self):
        rules = self.bot.SWING_RULES
        self.assertEqual(rules.version, "rule-1-swing")
        self.assertEqual(rules.allowed_styles, ("SWING",))
        self.assertEqual(rules.allowed_side, "long")
        self.assertTrue(rules.ordinary_crypto_only)
        self.assertTrue(rules.allow_kyiv_00_06)
        self.assertEqual(rules.risk_per_trade_pct, 0.5)
        self.assertEqual(rules.target_rr, (2.0, 3.0, 5.0))
        self.assertEqual(rules.target_split, (0.40, 0.30, 0.30))
        self.assertTrue(rules.move_sl_to_breakeven_after_tp1)
        self.assertEqual(rules.breakeven_buffer_r, 0.08)
        self.assertEqual(rules.max_adverse_entry_drift_r, 0.25)
        self.assertFalse(rules.use_order_book_for_targets)
        self.assertTrue(rules.live_partial_exit_ready)


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


class SwingPartialExitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = _load_trade_module()

    def setUp(self):
        self.exchange = _BingX({})
        self.symbol = "SUI/USDT:USDT"
        self.exchange.markets = {
            self.symbol: {
                "id": "SUI-USDT",
                "symbol": self.symbol,
                "base": "SUI",
                "quote": "USDT",
                "swap": True,
                "contract": True,
                "limits": {"amount": {"min": 0.01}},
                "info": {"symbol": "SUI-USDT"},
            }
        }
        self.exchange.positions = [
            {
                "symbol": self.symbol,
                "side": "long",
                "contracts": 10.0,
                "entryPrice": 100.0,
            }
        ]
        self.bot.exchange = self.exchange
        self.bot.LAST_SLTP = {}
        self.bot.LAST_ORDER_IDS = {}
        self.bot.save_sltp = lambda: None
        self.bot.save_order_ids = lambda: None

    def test_entry_minus_point_zero_eight_r(self):
        self.assertAlmostEqual(
            self.bot.calculate_swing_breakeven_price(100.0, 96.0),
            99.68,
        )

    def test_arms_40_30_30_and_advances_sl_after_actual_reductions(self):
        calls = []

        def fake_place(symbol, side, price, quantity, kind):
            calls.append((symbol, side, price, quantity, kind))
            order_id = f"order-{len(calls)}"
            return {"data": {"order": {"orderId": order_id}}}

        with mock.patch.object(
            self.bot, "_place_bingx_tpsl_raw_sync", side_effect=fake_place
        ):
            result = self.bot.apply_swing_sltp_sync(
                "SUI",
                entry_price=100.0,
                sl_price=96.0,
                tp1_price=108.0,
                tp2_price=112.0,
                tp3_price=120.0,
            )
            self.assertIn("TP1=108.0/4.0", result)
            self.assertEqual([call[3] for call in calls[:4]], [10.0, 4.0, 3.0, 3.0])

            plan = self.bot.LAST_SLTP["SUI"]["long"]["swing_plan"]
            self.assertEqual(plan["stage"], "armed")
            self.assertAlmostEqual(plan["be_sl"], 99.68)

            # TP1 actually reduced the exchange position from 10 to 6.
            self.exchange.positions[0]["contracts"] = 6.0
            advanced = self.bot.advance_swing_exit_state_sync("SUI", "long")
            self.assertIn("stage=tp1_done", advanced)
            state = self.bot.LAST_SLTP["SUI"]["long"]
            self.assertEqual(state["swing_plan"]["stage"], "tp1_done")
            self.assertAlmostEqual(state["sl"], 99.68)
            self.assertEqual(calls[-1][3], 6.0)

            # TP2 reduced it to the final 30%; keep Entry - 0.08R and only
            # resize its protective quantity.
            self.exchange.positions[0]["contracts"] = 3.0
            advanced = self.bot.advance_swing_exit_state_sync("SUI", "long")
            self.assertIn("stage=tp2_done", advanced)
            state = self.bot.LAST_SLTP["SUI"]["long"]
            self.assertEqual(state["swing_plan"]["stage"], "tp2_done")
            self.assertAlmostEqual(state["sl"], 99.68)
            self.assertEqual(calls[-1][3], 3.0)

    def test_rule_2a_scalp_arms_40_30_30_with_point_zero_five_r_buffer(self):
        calls = []

        def fake_place(symbol, side, price, quantity, kind):
            calls.append((symbol, side, price, quantity, kind))
            return {"data": {"order": {"orderId": f"scalp-{len(calls)}"}}}

        with mock.patch.object(
            self.bot, "_place_bingx_tpsl_raw_sync", side_effect=fake_place
        ):
            result = self.bot.apply_swing_sltp_sync(
                "SUI",
                entry_price=100.0,
                sl_price=96.0,
                tp1_price=103.2,
                tp2_price=108.0,
                tp3_price=112.0,
                signal_style="SCALP",
            )

        self.assertIn("TP1=103.2/4.0", result)
        self.assertEqual([call[3] for call in calls], [10.0, 4.0, 3.0, 3.0])
        plan = self.bot.LAST_SLTP["SUI"]["long"]["swing_plan"]
        self.assertEqual(plan["style"], "SCALP")
        self.assertEqual(plan["version"], "rule-2a-scalping-balanced-all-assets")
        self.assertAlmostEqual(plan["be_sl"], 99.8)
        self.assertAlmostEqual(plan["buffer_r"], 0.05)

    def test_partial_order_failure_falls_back_to_full_sl_and_tp1(self):
        call_number = 0

        def fake_place(_symbol, _side, _price, _quantity, _kind):
            nonlocal call_number
            call_number += 1
            if call_number == 3:
                raise RuntimeError("simulated TP2 rejection")
            return {"data": {"order": {"orderId": f"order-{call_number}"}}}

        with mock.patch.object(
            self.bot, "_place_bingx_tpsl_raw_sync", side_effect=fake_place
        ):
            result = self.bot.apply_swing_sltp_sync(
                "SUI",
                entry_price=100.0,
                sl_price=96.0,
                tp1_price=108.0,
                tp2_price=112.0,
                tp3_price=120.0,
            )

        self.assertTrue(result.startswith("FALLBACK_FULL_TP1"))
        self.assertNotIn("swing_plan", self.bot.LAST_SLTP["SUI"]["long"])
        self.assertEqual(self.bot.LAST_SLTP["SUI"]["long"]["tp"], 108.0)

    def test_position_too_small_for_three_targets_falls_back_safely(self):
        self.exchange.markets[self.symbol]["limits"]["amount"]["min"] = 0.48
        self.exchange.positions[0]["contracts"] = 0.66
        calls = []

        def fake_place(symbol, side, price, quantity, kind):
            calls.append((symbol, side, price, quantity, kind))
            return {"data": {"order": {"orderId": f"fallback-{len(calls)}"}}}

        with mock.patch.object(
            self.bot, "_place_bingx_tpsl_raw_sync", side_effect=fake_place
        ):
            result = self.bot.apply_swing_sltp_sync(
                "SUI",
                entry_price=100.0,
                sl_price=96.0,
                tp1_price=108.0,
                tp2_price=112.0,
                tp3_price=120.0,
            )

        self.assertTrue(result.startswith("FALLBACK_FULL_TP1"))
        self.assertEqual([call[4] for call in calls], ["sl", "tp"])
        self.assertEqual([call[3] for call in calls], [0.66, 0.66])


class BingXSymbolResolutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = _load_trade_module()

    def setUp(self):
        self.exchange = _BingX({})
        self.bot.exchange = self.exchange

    @staticmethod
    def _mvll_market(api_state_open="true"):
        return {
            "id": "NCSKMVLL2USD-USDT",
            "symbol": "NCSKMVLL2USD/USDT:USDT",
            "base": "NCSKMVLL2USD",
            "quote": "USDT",
            "swap": True,
            "contract": True,
            "info": {
                "symbol": "NCSKMVLL2USD-USDT",
                "displayName": "MVLL-USDT",
                "apiStateOpen": api_state_open,
            },
        }

    def test_resolves_bingx_display_name_alias(self):
        symbol = "NCSKMVLL2USD/USDT:USDT"
        self.exchange.markets = {symbol: self._mvll_market()}
        self.assertEqual(self.bot.resolve_symbol_sync("MVLL"), symbol)

    def test_refreshes_ccxt_markets_after_cache_miss(self):
        symbol = "NCSKMVLL2USD/USDT:USDT"
        self.exchange.markets = {
            "BTC/USDT:USDT": {
                "base": "BTC", "quote": "USDT", "swap": True, "info": {}
            }
        }
        self.exchange.refreshed_markets = {symbol: self._mvll_market()}

        self.assertEqual(self.bot.resolve_symbol_sync("MVLL"), symbol)
        self.assertEqual(self.exchange.load_markets_calls, [True])

    def test_reports_contract_with_api_open_disabled(self):
        symbol = "NCSKMVLL2USD/USDT:USDT"
        self.exchange.markets = {symbol: self._mvll_market("false")}

        issue = self.bot.market_api_open_disabled_sync(symbol)
        self.assertIn("apiStateOpen=false", issue)
        self.assertIn("MVLL-USDT", issue)


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
