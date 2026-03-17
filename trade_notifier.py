import asyncio
import time
import os

LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-5184386267"))

last_checked = 0

async def pnl_watcher(app, exchange, log, interval=10):
    global last_checked

    while True:
        try:
            trades = await asyncio.to_thread(exchange.fetch_my_trades)

            for t in trades:
                ts = t.get("timestamp", 0)

                if ts <= last_checked:
                    continue

                info = t.get("info", {})
                pnl_raw = info.get("realizedPnl") or info.get("profit")

                if pnl_raw is None:
                    continue

                pnl = float(pnl_raw)
                symbol = t.get("symbol", "")
                side = t.get("side", "")
                amount = t.get("amount", 0)

                status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

                msg = (
                    f"{status}\n"
                    f"Symbol: {symbol}\n"
                    f"Side: {side}\n"
                    f"PnL: {pnl}\n"
                    f"Qty: {amount}"
                )

                await app.send_message(chat_id=LOG_CHAT_ID, text=msg)

                last_checked = ts

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)