import asyncio
import os
import time

LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-1003332013833"))

last_checked = 0
weekly_pnl = 0.0
week_start = time.time()


async def pnl_watcher(app, exchange, log, interval=5):
    global last_checked, weekly_pnl, week_start

    while True:
        try:
            trades = await asyncio.to_thread(exchange.fetch_my_trades)

            for t in trades:
                ts = t.get("timestamp", 0)

                if ts <= last_checked:
                    continue

                pnl = float(
                    t.get("info", {}).get("realizedPnl") or 0
                )

                if pnl == 0:
                    continue

                symbol = t.get("symbol", "")
                side = t.get("side", "")
                amount = t.get("amount", 0)

                status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

                msg = (
                    f"{status}\n"
                    f"Symbol: {symbol}\n"
                    f"Side: {side}\n"
                    f"PnL: {round(pnl, 4)}\n"
                    f"Qty: {amount}"
                )

                weekly_pnl += pnl

                await app.send_message(LOG_CHAT_ID, msg)

                last_checked = ts

            # 📊 weekly report
            if time.time() - week_start >= 7 * 24 * 60 * 60:
                status = "🟢 PROFIT" if weekly_pnl > 0 else "🔴 LOSS"

                report = (
                    f"📊 WEEKLY REPORT\n\n"
                    f"{status}\n"
                    f"Total PnL: {round(weekly_pnl, 4)} USDT"
                )

                await app.send_message(LOG_CHAT_ID, report)

                weekly_pnl = 0.0
                week_start = time.time()

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)