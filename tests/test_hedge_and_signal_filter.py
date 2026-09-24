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
        self.margin_mode_calls = []
        self.leverage_calls = []

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

    def set_margin_mode(self, mode, symbol):
        self.margin_mode_calls.append((mode, symbol))

    def set_leverage(self, leverage, symbol, params):
        self.leverage_calls.append((leverage, symbol, params))

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
    os.environ.pop("CONTROL_CHAT_ID", None)

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

    def test_combined_system_styles_are_allowed_for_new_entries(self):
        intraday = "📈 INTRADAY  LONG 🟢 — SUI/USDT:USDT\nTF: 4H / 1H / 15M"
        scalp = "⚡ SCALP  LONG 🟢 — SUI/USDT:USDT\nTF: 1H / 15M / 5M"
        swing = "🌊 SWING  LONG 🟢 — SUI/USDT:USDT\nTF: 1D / 4H / 1H"
        stats = "📊 Paper-trading статистика\n⚡ SCALP: угод 325\n📈 INTRADAY: угод 180"

        self.assertTrue(self.bot.is_allowed_signal_style(intraday))
        self.assertTrue(self.bot.is_allowed_signal_style(scalp))
        self.assertFalse(self.bot.is_allowed_signal_style(swing))
        self.assertIsNone(self.bot.extract_signal_style(stats))

    def test_signalbot_and_saved_messages_are_registered_as_sources(self):
        self.assertEqual(self.bot.TARGET_CHAT_ID, -5486330898)
        self.assertEqual(self.bot.CONTROL_CHAT_ID, 566620979)
        self.assertEqual(_Filters.last_chat, [-5486330898, 566620979])

    def test_saved_messages_accepts_full_signal_or_explicit_command_only(self):
        signal = """⚡ SCALP LONG 🟢 — HYPE/USDT:USDT
📍 Entry: 83.889
🛑 SL: 80.3994
🎯 TP1: 86.6806
🎯 TP2: 89.1233
🎯 TP3: 92.6129
"""
        self.assertEqual(
            self.bot.normalize_source_text(566620979, signal),
            signal.strip(),
        )
        self.assertEqual(
            self.bot.normalize_source_text(566620979, "/tb close HYPE"),
            "close HYPE",
        )
        self.assertEqual(
            self.bot.normalize_source_text(566620979, "/tradebot: sl HYPE 80.5"),
            "sl HYPE 80.5",
        )
        self.assertIsNone(
            self.bot.normalize_source_text(566620979, "нагадати перевірити HYPE")
        )
        self.assertIsNone(self.bot.normalize_source_text(123456, signal))

    def test_manual_control_commands_parse_without_openai(self):
        close = self.bot.parse_manual_control_command("закрий HYPE")
        self.assertEqual(close["action"], "CLOSE")
        self.assertEqual(close["base"], "HYPE")
        self.assertTrue(self.bot.has_close_intent("закрий HYPE"))

        be = self.bot.parse_manual_control_command("be HYPE long")
        self.assertEqual(be["action"], "BE")
        self.assertEqual(be["side"], "long")

        sl = self.bot.parse_manual_control_command("sl HYPE 80,5")
        self.assertEqual(sl["action"], "SET_SL")
        self.assertEqual(sl["sl"], 80.5)

        tp = self.bot.parse_manual_control_command("tp HYPE 90")
        self.assertEqual(tp["action"], "SET_TP")
        self.assertEqual(tp["tp"], 90.0)

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
        for base in ("HOOD", "SNDK", "MU", "MUU", "SKHY", "ZHIPU", "SPY", "XAU"):
            for style in ("SCALP", "INTRADAY", "SWING"):
                reason = self.bot.non_crypto_open_block_reason(base, style=style)
                self.assertIsNotNone(reason, f"{style} {base}")
                self.assertIn("ordinary crypto assets only", reason)

        for base in ("BTC", "HYPE", "ONDO", "1000SHIB", "DOGE"):
            self.assertIsNone(self.bot.non_crypto_open_block_reason(base), base)

    def test_intraday_asset_policy_is_separate_from_legacy_a3_exit_rules(self):
        self.assertIs(self.bot._rules_for_signal_style("INTRADAY"), self.bot.A3)
        self.assertIs(
            self.bot._asset_policy_rules_for_signal_style("INTRADAY"),
            self.bot.ENTRY_RULES,
        )
        self.assertTrue(
            self.bot._asset_policy_rules_for_signal_style("INTRADAY").ordinary_crypto_only
        )

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

    def test_structured_parser_preserves_thousands_separators(self):
        text = """⚡ SCALP LONG 🟢 — BTC/USDT:USDT
📍 Entry: 77,277.00
🛑 SL: 76,107.00 (1.51%)
🎯 TP1: 78,286.00 (40%, RR 0.8)
🎯 TP2: 79,133.00 (30%)
🎯 TP3: 80,343.00 (30%)
💼 Баланс: 1,000.00 USDT | ризик: 10.0 USDT (1.0%)
💧 Орієнт. ліквід: 65,818.00
"""
        parsed = self.bot.parse_structured_signal(text)

        self.assertEqual(parsed["entry"], 77277.0)
        self.assertEqual(parsed["sl"], 76107.0)
        self.assertEqual(parsed["tp"], 78286.0)
        self.assertEqual(parsed["tp2"], 79133.0)
        self.assertEqual(parsed["tp3"], 80343.0)
        self.assertEqual(parsed["balance_usdt"], 1000.0)
        self.assertEqual(parsed["liquidation"], 65818.0)

    def test_structured_scalp_feed_parses_all_balanced_targets(self):
        text = """⚡ SCALP  LONG 🟢  —  SUI/USDT:USDT
TF: 1H / 15M / 5M
📍 Entry:   0.726500
🛑 SL:      0.698735  (3.82%)
🎯 TP1:     0.748712  +3.2 USDT (40%, RR 0.8)
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
        self.assertEqual(parsed["signal_rr1"], 0.8)
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
            if style in {"INTRADAY", "SWING"}:
                self.assertEqual(parsed["tp2"], 32.5164)
                self.assertEqual(parsed["tp3"], 33.4237)
            else:
                self.assertIsNone(parsed["tp2"])
                self.assertIsNone(parsed["tp3"])

    def test_open_policy_allows_only_intraday_for_live_entry(self):
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
        self.assertIsNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 12, 0, tzinfo=kyiv),
                style="INTRADAY",
                require_allowed_style=True,
            )
        )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 12, 0, tzinfo=kyiv),
                style="SWING",
                require_allowed_style=True,
            )
        )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 6, 0, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )
        self.assertIsNotNone(
            self.bot.open_policy_block_reason(
                "long",
                datetime(2026, 8, 26, 23, 59, tzinfo=kyiv),
                style="SCALP",
                require_allowed_style=True,
            )
        )

    def test_legacy_shadow_strategy_selects_a5_by_rr_and_wide_stop(self):
        kyiv = ZoneInfo("Europe/Kyiv")
        noon = datetime(2026, 8, 26, 12, 0, tzinfo=kyiv)

        for rr1 in (0.795, 0.8, 0.9, 0.999999):
            decision, reason = self.bot.select_legacy_combined_strategy(
                style="SCALP", side="long", signal_text="", rr1=rr1,
                stop_distance_pct=6.0, now=noon,
            )
            self.assertIsNone(reason)
            self.assertEqual(decision.rule_id, "A5")
        for rr1, stop in ((0.79, 6.0), (1.0, 6.0), (0.8, 5.999)):
            decision, reason = self.bot.select_legacy_combined_strategy(
                style="SCALP", side="long", signal_text="", rr1=rr1,
                stop_distance_pct=stop, now=noon,
            )
            self.assertIsNone(decision)
            self.assertIsNotNone(reason)

    def test_rule_one_swing_is_paper_only_and_blocked_for_live_entry(self):
        kyiv = ZoneInfo("Europe/Kyiv")
        night = datetime(2026, 8, 26, 2, 0, tzinfo=kyiv)

        for rr1 in (0.9, 2.0, 5.0):
            self.assertIsNotNone(
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

    def test_scalp_statistical_filter_uses_declared_signal_rr(self):
        command = {
            "entry": 100.0,
            "sl": 90.0,
            "tp": 107.98,
            "signal_rr1": 0.8,
        }
        self.assertAlmostEqual(
            self.bot.calculate_rr_from_prices(100.0, 90.0, 107.98, "long"),
            0.798,
        )
        self.assertEqual(self.bot.source_signal_rr1(command, "long"), 0.8)
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

    def test_auto_plan_uses_combined_system_fallback_risk(self):
        plan = self.bot.calculate_auto_trade_plan(
            1000.0,
            0.726500,
            0.698735,
            0.748712,
        )

        self.assertAlmostEqual(plan["risk_budget"], 10.0)
        self.assertAlmostEqual(plan["expected_loss_at_sl"], 10.0, places=8)
        self.assertEqual(plan["leverage"], 8)
        self.assertAlmostEqual(plan["expected_profit_at_tp1"], 8.0, places=2)
        self.assertLess(plan["margin"], plan["notional"])

        wide_target = self.bot.calculate_auto_trade_plan(1000.0, 100.0, 96.0, 120.0)
        self.assertLess(wide_target["leverage"], plan["leverage"])

        tight_stop = self.bot.calculate_auto_trade_plan(1000.0, 100.0, 99.0, 100.8)
        wide_stop = self.bot.calculate_auto_trade_plan(1000.0, 100.0, 90.0, 108.0)
        self.assertAlmostEqual(tight_stop["expected_loss_at_sl"], 10.0, places=8)
        self.assertAlmostEqual(wide_stop["expected_loss_at_sl"], 10.0, places=8)
        self.assertGreater(tight_stop["notional"], wide_stop["notional"])

    def test_live_entry_rules_are_explicit_and_uncapped(self):
        rules = self.bot.ENTRY_RULES
        self.assertEqual(
            rules.version,
            "rr1-3-intraday-weekdays-unlimited-isolated-v2",
        )
        self.assertEqual(rules.allowed_styles, ("INTRADAY",))
        self.assertEqual(rules.allowed_side, "long")
        self.assertTrue(rules.ordinary_crypto_only)
        self.assertEqual(rules.risk_per_trade_pct, 1.0)
        self.assertFalse(rules.allow_position_additions)
        self.assertFalse(rules.allow_same_symbol_side_reentry)
        self.assertIsNone(rules.max_concurrent_positions)
        self.assertIsNone(rules.max_open_risk_pct)
        self.assertEqual(rules.margin_mode, "isolated")
        self.assertEqual(self.bot.FIXED_RISK_PCT, 1.0)
        self.assertEqual(self.bot.ALLOWED_SIGNAL_STYLES, {"INTRADAY"})
        self.assertEqual(self.bot.TRACKED_SIGNAL_STYLES, {"SCALP", "INTRADAY"})

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

    def test_open_failure_journal_preserves_a_placed_order_status(self):
        key = "test:placed-order"
        self.bot.EXECUTION_STATE["signals"][key] = {"status": "order_placed"}
        try:
            with mock.patch.object(self.bot, "save_execution_state"):
                self.bot.record_open_workflow_failure(key, "notification failed")
            row = self.bot.EXECUTION_STATE["signals"][key]
            self.assertEqual(row["status"], "order_placed")
            self.assertEqual(row["workflow_error"], "notification failed")
        finally:
            self.bot.EXECUTION_STATE["signals"].pop(key, None)

    def test_open_failure_journal_marks_pre_order_processing_failure(self):
        key = "test:pre-order"
        self.bot.EXECUTION_STATE["signals"][key] = {"status": "evaluating"}
        try:
            with mock.patch.object(self.bot, "save_execution_state"):
                self.bot.record_open_workflow_failure(key, "bad live price")
            row = self.bot.EXECUTION_STATE["signals"][key]
            self.assertEqual(row["status"], "processing_failed")
            self.assertEqual(row["reason"], "bad live price")
        finally:
            self.bot.EXECUTION_STATE["signals"].pop(key, None)

    def test_matching_intraday_crypto_reaches_dry_run_execution(self):
        key = "test:rr1-3-dry-run"
        decision = self.bot.StrategyDecision(
            "RR1_3", 1.0, (1.0, 0.0, 0.0), False
        )
        command = {
            "action": "OPEN",
            "confidence": 1.0,
            "base": "BTC",
            "side": "long",
            "entry": 100.0,
            "sl": 96.0,
            "tp": 104.0,
            "signal_rr1": 1.0,
            "_signal_style": "INTRADAY",
            "_signal_key": key,
            "_tg_text": "INTRADAY LONG BTC/USDT Entry 100 SL 96 TP1 104",
        }
        try:
            with (
                mock.patch.object(self.bot, "DRY_RUN", True),
                mock.patch.object(self.bot, "save_execution_state"),
                mock.patch.object(
                    self.bot, "select_strategy", return_value=(decision, None)
                ),
                mock.patch.object(self.bot, "_register_legacy_shadow_if_eligible"),
                mock.patch.object(self.bot, "_register_s1_shadow_if_eligible"),
                mock.patch.object(self.bot, "_register_s2_shadow_if_eligible"),
                mock.patch.object(
                    self.bot, "resolve_symbol", new=mock.AsyncMock(
                        return_value="BTC/USDT:USDT"
                    )
                ),
                mock.patch.object(
                    self.bot, "fetch_position_oneway", new=mock.AsyncMock(return_value=None)
                ),
                mock.patch.object(
                    self.bot, "reconcile_execution_state_sync",
                    return_value={"closed": [], "unmatched": []},
                ),
                mock.patch.object(
                    self.bot.exchange, "fetch_ticker", return_value={"last": 100.0},
                    create=True,
                ),
                mock.patch.object(
                    self.bot, "get_usdt_total", new=mock.AsyncMock(return_value=1000.0)
                ),
                mock.patch.object(
                    self.bot, "normalize_order_qty",
                    new=mock.AsyncMock(return_value=(2.5, 0.001)),
                ),
            ):
                asyncio.run(self.bot.handle_ai_command(command))

            row = self.bot.EXECUTION_STATE["signals"][key]
            self.assertEqual(row["status"], "dry_run")
            self.assertEqual(row["symbol"], "BTC/USDT:USDT")
        finally:
            self.bot.EXECUTION_STATE["signals"].pop(key, None)

    def test_portfolio_gate_only_blocks_duplicate_coin(self):
        rows = {
            "BTC/USDT:USDT:long": {"status": "open", "symbol": "BTC/USDT:USDT", "risk_pct": 0.7},
            "ETH/USDT:USDT:long": {"status": "open", "symbol": "ETH/USDT:USDT", "risk_pct": 0.7},
        }
        self.assertIsNotNone(
            self.bot.portfolio_entry_block_reason("BTC/USDT:USDT", 0.5, rows)
        )
        self.assertIsNone(
            self.bot.portfolio_entry_block_reason("SOL/USDT:USDT", 0.6, rows)
        )
        many_open = {
            **rows,
            "SOL/USDT:USDT:long": {"status": "open", "symbol": "SOL/USDT:USDT", "risk_pct": 0.6},
            "XRP/USDT:USDT:long": {"status": "open", "symbol": "XRP/USDT:USDT", "risk_pct": 0.5},
        }
        self.assertIsNone(
            self.bot.portfolio_entry_block_reason("ADA/USDT:USDT", 1.0, many_open)
        )
        risk_heavy = {
            key: {**row, "risk_pct": 1.0} for key, row in list(rows.items())
        }
        risk_heavy["SOL"] = {"status": "open", "symbol": "SOL/USDT:USDT", "risk_pct": 0.7}
        self.assertIsNone(
            self.bot.portfolio_entry_block_reason("ADA/USDT:USDT", 1.0, risk_heavy),
        )

    def test_margin_and_leverage_are_set_in_isolated_mode(self):
        exchange = self.bot.exchange
        exchange.margin_mode_calls.clear()
        exchange.leverage_calls.clear()

        self.bot.set_margin_mode_sync("BTC/USDT:USDT")
        self.bot.set_leverage_sync("BTC/USDT:USDT", 5, "long")

        self.assertEqual(
            exchange.margin_mode_calls,
            [("isolated", "BTC/USDT:USDT")],
        )
        self.assertEqual(exchange.leverage_calls[0][0:2], (5, "BTC/USDT:USDT"))

    def test_rule_one_swing_rules_are_retained_for_management_and_statistics(self):
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
        self.bot.POSITION_MISSING_COUNTS = {}
        self.bot.save_sltp = lambda: None
        self.bot.save_order_ids = lambda: None

    def test_entry_minus_point_zero_eight_r(self):
        self.assertAlmostEqual(
            self.bot.calculate_swing_breakeven_price(100.0, 96.0),
            99.68,
        )

    def test_arms_40_30_30_and_advances_sl_after_actual_reductions(self):
        calls = []

        def fake_place(
            symbol, side, price, quantity, kind, *, close_position=False
        ):
            calls.append((symbol, side, price, quantity, kind, close_position))
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
            self.assertEqual(
                [call[5] for call in calls[:4]],
                [False, False, False, True],
            )

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

    def test_s3_scalp_cannot_arm_legacy_partial_targets(self):
        with self.assertRaisesRegex(ValueError, "no live partial-exit rule"):
            self.bot.apply_swing_sltp_sync(
                "SUI",
                entry_price=100.0,
                sl_price=94.0,
                tp1_price=104.8,
                tp2_price=108.0,
                tp3_price=112.0,
                signal_style="SCALP",
            )

    def test_missing_position_must_be_confirmed_before_orders_are_deleted(self):
        self.bot.LAST_SLTP = {
            "SUI": {
                "long": {
                    "sl": 96.0,
                    "tp": 103.2,
                    "swing_plan": {
                        "style": "SCALP",
                        "initial_qty": 10.0,
                        "tp1_qty": 4.0,
                        "tp2_qty": 3.0,
                        "stage": "armed",
                    },
                }
            }
        }
        self.bot.LAST_ORDER_IDS = {
            "SUI:long": {"sl_id": "sl-1", "tp1_id": "tp-1"}
        }
        self.exchange.positions = []
        self.exchange.open_orders = [
            {
                "id": "sl-1",
                "type": "stop_market",
                "side": "sell",
                "info": {"positionSide": "LONG"},
            },
            {
                "id": "tp-1",
                "type": "take_profit_market",
                "side": "sell",
                "info": {"positionSide": "LONG"},
            },
        ]

        first = self.bot.advance_swing_exit_state_sync("SUI", "long")
        second = self.bot.advance_swing_exit_state_sync("SUI", "long")
        self.assertEqual(first, "POSITION_MISSING_UNCONFIRMED 1/3")
        self.assertEqual(second, "POSITION_MISSING_UNCONFIRMED 2/3")
        self.assertEqual(self.exchange.canceled, [])
        self.assertIn("SUI", self.bot.LAST_SLTP)

        third = self.bot.advance_swing_exit_state_sync("SUI", "long")
        self.assertEqual(third, "POSITION_CLOSED_CONFIRMED")
        self.assertEqual(self.exchange.canceled, ["sl-1", "tp-1"])
        self.assertNotIn("SUI", self.bot.LAST_SLTP)

    def test_position_api_error_never_deletes_protection(self):
        self.bot.LAST_SLTP = {
            "SUI": {
                "long": {
                    "sl": 96.0,
                    "tp": 103.2,
                    "swing_plan": {"style": "SCALP", "stage": "armed"},
                }
            }
        }
        self.bot.LAST_ORDER_IDS = {"SUI:long": {"sl_id": "sl-1"}}
        self.exchange.open_orders = [
            {
                "id": "sl-1",
                "type": "stop_market",
                "side": "sell",
                "info": {"positionSide": "LONG"},
            }
        ]
        self.bot.POSITION_MISSING_COUNTS["SUI:long"] = 2

        with mock.patch.object(
            self.exchange,
            "fetch_positions",
            side_effect=RuntimeError("temporary BingX timeout"),
        ):
            with self.assertRaises(self.bot.PositionLookupError):
                self.bot.advance_swing_exit_state_sync("SUI", "long")

        self.assertEqual(self.exchange.canceled, [])
        self.assertIn("SUI", self.bot.LAST_SLTP)
        self.assertNotIn("SUI:long", self.bot.POSITION_MISSING_COUNTS)

    def test_partial_order_failure_falls_back_to_full_sl_and_tp1(self):
        call_number = 0

        def fake_place(
            _symbol,
            _side,
            _price,
            _quantity,
            _kind,
            *,
            close_position=False,
        ):
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

        def fake_place(
            symbol, side, price, quantity, kind, *, close_position=False
        ):
            calls.append((symbol, side, price, quantity, kind, close_position))
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

    def test_small_scalp_preflight_uses_full_tp1_without_raising_risk(self):
        self.exchange.markets[self.symbol]["limits"]["amount"]["min"] = 0.48

        mode, quantities, reason = self.bot._choose_partial_exit_mode(
            self.symbol,
            0.66,
            reference_price=100.0,
            target_split=(0.4, 0.3, 0.3),
        )

        self.assertEqual(mode, "full_tp1")
        self.assertIsNone(quantities)
        self.assertIn("partial target quantities", reason)
        self.assertNotIn("SWING", reason)

    def test_executable_scalp_preflight_keeps_balanced_split(self):
        mode, quantities, reason = self.bot._choose_partial_exit_mode(
            self.symbol,
            10.0,
            reference_price=100.0,
            target_split=(0.4, 0.3, 0.3),
        )

        self.assertEqual(mode, "balanced")
        self.assertEqual(quantities, (4.0, 3.0, 3.0))
        self.assertIsNone(reason)

    def test_final_target_uses_exact_decimal_remainder(self):
        def truncate_to_two_decimals(_symbol, qty):
            return f"{int(float(qty) * 100) / 100:.2f}"

        with mock.patch.object(
            self.exchange,
            "amount_to_precision",
            side_effect=truncate_to_two_decimals,
        ):
            quantities = self.bot._split_swing_target_quantities(
                self.symbol,
                113.65,
                reference_price=0.344,
                target_split=(0.4, 0.3, 0.3),
            )

        self.assertEqual(quantities, (45.46, 34.09, 34.1))
        self.assertAlmostEqual(sum(quantities), 113.65)

    def test_raw_final_tp_requests_close_entire_remaining_position(self):
        with mock.patch.object(
            self.bot,
            "_bingx_raw_request_sync",
            return_value={"code": 0},
        ) as raw_request:
            self.bot._place_bingx_tpsl_raw_sync(
                self.symbol,
                "long",
                120.0,
                3.0,
                "tp",
                close_position=True,
            )

        payload = raw_request.call_args.args[2]
        self.assertEqual(payload["closePosition"], "true")
        self.assertEqual(payload["quantity"], "3")

    def test_watcher_force_closes_any_remainder_after_tp3_mark_price(self):
        self.exchange.positions[0].update(contracts=0.01, markPrice=120.1)
        self.bot.LAST_SLTP = {
            "SUI": {
                "long": {
                    "sl": 99.8,
                    "tp3": 120.0,
                    "swing_plan": {
                        "style": "SCALP",
                        "stage": "tp2_done",
                        "initial_qty": 10.0,
                        "tp1_qty": 4.0,
                        "tp2_qty": 3.0,
                        "tp3_qty": 3.0,
                    },
                }
            }
        }
        with mock.patch.object(
            self.bot, "close_position_full_sync", return_value="CLOSED long"
        ) as close_full:
            result = self.bot.advance_swing_exit_state_sync("SUI", "long")

        close_full.assert_called_once_with("SUI", "long")
        self.assertIn("TP3_REMAINDER_FORCE_CLOSED", result)
        self.assertIn("qty=0.01", result)

    def test_missing_old_order_is_successful_cleanup(self):
        with mock.patch.object(
            self.exchange,
            "cancel_order",
            side_effect=RuntimeError('bingx {"code":109400,"msg":"order not exist"}'),
        ), mock.patch.object(self.bot, "log") as logger:
            result = self.bot.cancel_order_exact_sync(self.symbol, "old-order")

        self.assertTrue(result)
        logger.assert_called_once_with(
            "INFO",
            f"CANCEL EXACT ALREADY GONE symbol={self.symbol} id=old-order",
        )


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


class ExecutionSynchronizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = _load_trade_module()

    def setUp(self):
        self.exchange = _BingX({})
        self.bot.exchange = self.exchange
        self.bot.EXECUTION_STATE = {"signals": {}, "positions": {}}

    def test_signal_identity_uses_telegram_message(self):
        first = self.bot.signal_execution_key(-5486330898, 123, "signal")
        repeated = self.bot.signal_execution_key(-5486330898, 123, "changed text")
        another = self.bot.signal_execution_key(-5486330898, 124, "signal")
        self.assertEqual(first, repeated)
        self.assertNotEqual(first, another)

    def test_forwarded_copy_is_deduplicated_by_content(self):
        content_hash = self.bot.signal_content_hash("SCALP LONG SUI Entry: 1")
        self.bot.EXECUTION_STATE["signals"]["tg:-5486330898:1"] = {
            "status": "open_protected",
            "content_hash": content_hash,
        }
        self.assertTrue(
            self.bot.execution_signal_is_duplicate("tg:566620979:9", content_hash)
        )

    def test_reconcile_closes_missing_tracked_position(self):
        self.bot.EXECUTION_STATE = {
            "signals": {"tg:1:2": {"status": "open_protected"}},
            "positions": {
                "SUI/USDT:USDT:long": {
                    "status": "open",
                    "signal_key": "tg:1:2",
                    "symbol": "SUI/USDT:USDT",
                    "side": "long",
                }
            },
        }
        with mock.patch.object(self.bot, "save_execution_state"):
            result = self.bot.reconcile_execution_state_sync()
        self.assertEqual(result["closed"], ["SUI/USDT:USDT:long"])
        self.assertEqual(
            self.bot.EXECUTION_STATE["signals"]["tg:1:2"]["status"], "closed"
        )

    def test_reconcile_registers_unmatched_exchange_position(self):
        self.exchange.positions = [
            {
                "symbol": "SUI/USDT:USDT",
                "side": "long",
                "contracts": 3,
                "entryPrice": 1.25,
            }
        ]
        with mock.patch.object(self.bot, "save_execution_state"):
            result = self.bot.reconcile_execution_state_sync()
        self.assertEqual(result["unmatched"], ["SUI/USDT:USDT:long"])
        row = self.bot.EXECUTION_STATE["positions"]["SUI/USDT:USDT:long"]
        self.assertEqual(row["origin"], "unmatched_exchange_position")
        self.assertEqual(row["qty"], 3.0)

    def test_position_api_returns_only_signal_linked_open_positions(self):
        self.bot.EXECUTION_STATE = {
            "signals": {"tg:1:10": {"status": "open_protected"}},
            "positions": {
                "LTC/USDT:USDT:long": {
                    "position_key": "LTC/USDT:USDT:long",
                    "signal_key": "tg:1:10",
                    "symbol": "LTC/USDT:USDT",
                    "side": "long",
                    "status": "open",
                    "strategy": "A3",
                    "risk_pct": 0.6,
                },
                "DASH/USDT:USDT:long": {
                    "position_key": "DASH/USDT:USDT:long",
                    "signal_key": None,
                    "symbol": "DASH/USDT:USDT",
                    "side": "long",
                    "status": "open",
                    "origin": "unmatched_exchange_position",
                },
                "OLD/USDT:USDT:long": {
                    "position_key": "OLD/USDT:USDT:long",
                    "signal_key": "tg:1:9",
                    "symbol": "OLD/USDT:USDT",
                    "side": "long",
                    "status": "closed",
                },
            },
        }
        payload = self.bot.executed_open_positions_payload()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["positions"][0]["symbol"], "LTC/USDT:USDT")
        self.assertEqual(payload["positions"][0]["execution_status"], "open_protected")

    def test_shadow_statistics_payload_is_explicitly_virtual(self):
        with mock.patch.object(
            self.bot.SHADOW_LEGACY,
            "summary",
            return_value={
                "start_balance": 1000.0,
                "balance": 1012.5,
                "pnl_usdt": 12.5,
                "trades": 3,
                "open": 1,
                "breakdown": {},
            },
        ), mock.patch.object(
            self.bot.SHADOW_SWING_PROBABILITY,
            "summary",
            return_value={
                "start_balance": 1000.0,
                "balance": 1005.0,
                "pnl_usdt": 5.0,
                "trades": 1,
                "open": 0,
            },
        ):
            payload = self.bot.legacy_shadow_statistics_payload()

        self.assertEqual(payload["mode"], "shadow")
        self.assertEqual(payload["strategy"], "A1/A4/A5/A3")
        self.assertEqual(payload["start_balance"], 1000.0)
        self.assertEqual(payload["balance"], 1012.5)
        self.assertEqual(payload["round_trip_cost_pct"], 0.14)
        self.assertEqual(payload["swing_probability"]["mode"], "shadow")
        self.assertEqual(payload["swing_probability"]["risk_per_trade_pct"], 0.5)
        self.assertEqual(payload["swing_probability"]["balance"], 1005.0)
        self.assertIn("generated_at", payload)


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
