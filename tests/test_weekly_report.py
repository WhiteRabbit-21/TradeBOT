import unittest
from unittest.mock import AsyncMock, patch
from datetime import date, datetime
import json
import tempfile

import trade_notifier as notifier


class WeeklyReportTests(unittest.TestCase):
    def test_real_pnl_statistics_group_days_months_and_year(self):
        rows = [
            {"trade_id": "a", "pnl": 3.0, "closed_at": "2026-09-17T08:00:00+00:00"},
            {"trade_id": "b", "pnl": -1.0, "closed_at": "2026-09-16T08:00:00+00:00"},
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            notifier, "PNL_TRADES_FILE", tmp + "/pnl.json"
        ):
            with open(notifier.PNL_TRADES_FILE, "w", encoding="utf-8") as handle:
                json.dump(rows, handle)
            stats = notifier.build_pnl_statistics_payload(
                datetime.fromisoformat("2026-09-17T12:00:00+03:00")
            )

        self.assertEqual(len(stats["days"]), 7)
        self.assertEqual(len(stats["weeks"]), 8)
        self.assertEqual(len(stats["months"]), 6)
        self.assertEqual(stats["days"][-1]["pnl"], 3.0)
        self.assertEqual(stats["days"][-2]["pnl"], -1.0)
        self.assertEqual(stats["year"]["pnl"], 2.0)

    def test_timestamp_ms_accepts_persisted_iso_time(self):
        self.assertEqual(
            notifier._timestamp_ms("2026-09-17T09:34:03+00:00", 0),
            1789637643000,
        )

    def test_cashflow_classifier_excludes_trading_results(self):
        self.assertEqual(
            notifier._income_cashflow_kind(
                {"incomeType": "TRANSFER_IN", "income": "10"}
            ),
            "deposit",
        )
        self.assertEqual(
            notifier._income_cashflow_kind(
                {"incomeType": "TRANSFER_OUT", "income": "-3"}
            ),
            "withdrawal",
        )
        self.assertIsNone(
            notifier._income_cashflow_kind(
                {"incomeType": "REALIZED_PNL", "income": "4"}
            )
        )

    def test_report_separates_deposit_from_trading_profit(self):
        report = notifier._format_weekly_report(
            week_start_date=date(2026, 9, 7),
            week_end_date=date(2026, 9, 13),
            start_equity=100.0,
            end_equity=114.0,
            tracked_pnl=4.0,
            deposits=10.0,
            withdrawals=0.0,
            cashflows_available=True,
            strategy_pnl={"S3": 4.0},
        )
        self.assertIn("Starting balance: 100.0000 USDT", report)
        self.assertIn("Ending balance: 114.0000 USDT", report)
        self.assertIn("Deposits: +10.0000 USDT", report)
        self.assertIn("Trading PnL (balance-adjusted): +4.0000 USDT", report)
        self.assertIn("S3: +4.0000 USDT", report)
        self.assertIn("Percent Growth excluding deposits: 4.00%", report)

    def test_closed_trade_message_is_strategy_labeled(self):
        message = notifier._format_pnl_message(
            symbol="BTC/USDT:USDT", side="long", pnl=2.5, qty=0.01,
            strategy="S3",
        )
        self.assertIn("УГОДУ ЗАКРИТО НА BINGX", message)
        self.assertIn("Стратегія: S3", message)
        self.assertIn("Результат: +2.5000 USDT", message)
        self.assertIn("фактичний realized PnL BingX", message)
        self.assertNotIn("SHADOW", message)

    def test_fill_rows_support_list_and_nested_responses(self):
        rows = [{"symbol": "SYN-USDT", "realizedPnl": "1.25"}]
        self.assertEqual(notifier._extract_fill_rows({"data": rows}), rows)
        self.assertEqual(notifier._extract_fill_rows({"data": {"rows": rows}}), rows)

    def test_symbol_normalization_deduplicates_ccxt_variants(self):
        self.assertEqual(
            notifier._normalize_symbol_for_compare("SYN/USDT"),
            notifier._normalize_symbol_for_compare("SYN/USDT:USDT"),
        )


class PnlFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_income_fee_plus_fill_realized_pnl_produces_net_result(self):
        fee_only = {
            "pnl": -0.12,
            "count": 1,
            "rows": [{"incomeType": "TRADING_FEE", "income": "-0.12"}],
            "has_real_pnl_signal": False,
        }
        with patch.object(
            notifier, "_get_position_income_summary", new=AsyncMock(return_value=fee_only)
        ), patch.object(
            notifier, "_get_fill_realized_pnl", new=AsyncMock(return_value=2.0)
        ), patch.object(notifier.asyncio, "sleep", new=AsyncMock()):
            result = await notifier._wait_final_income_summary(
                symbol="SYN/USDT:USDT",
                api_key="key",
                api_secret="secret",
                log=lambda *_: None,
                opened_at_ms=1_000,
                close_ts_ms=2_000,
                position_side="long",
            )

        self.assertTrue(result["has_real_pnl_signal"])
        self.assertEqual(result["realized_source"], "fill_history")
        self.assertAlmostEqual(result["pnl"], 1.88)


if __name__ == "__main__":
    unittest.main()
