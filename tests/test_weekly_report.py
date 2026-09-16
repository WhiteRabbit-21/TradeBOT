import unittest
from datetime import date

import trade_notifier as notifier


class WeeklyReportTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
