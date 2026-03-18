import asyncio
import os

LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-1003828203122"))

last_positions = {}


async def pnl_watcher(app, exchange, log, interval=5):

    global last_positions

    while True:
        try:
            positions = await asyncio.to_thread(exchange.fetch_positions)

            current = {}

            for p in positions:
                symbol = p.get("symbol")
                size = float(p.get("contracts") or p.get("positionAmt") or 0)

                if size > 0:
                    current[symbol] = p

            # 🔥 перевіряємо що закрилось
            for symbol, old_pos in last_positions.items():

                if symbol not in current:
                    # позиція ЗАКРИЛАСЬ
                    pnl = float(
                        old_pos.get("unrealizedPnl")
                        or old_pos.get("info", {}).get("unrealizedProfit")
                        or 0
                    )

                    side = old_pos.get("side")
                    qty = old_pos.get("contracts")

                    status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

                    msg = (
                        f"{status}\n"
                        f"Symbol: {symbol}\n"
                        f"Side: {side}\n"
                        f"PnL: {round(pnl, 4)}\n"
                        f"Qty: {qty}"
                    )

                    await app.send_message(LOG_CHAT_ID, msg)

            # оновлюємо стан
            last_positions = current

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)