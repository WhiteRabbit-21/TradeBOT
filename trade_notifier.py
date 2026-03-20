import asyncio
import time

last_checked = 0
weekly_pnl = 0.0
week_start = time.time()

seen_ids = set()


async def pnl_watcher(app, exchange, log, log_chat_id, interval=5):
    global last_checked, weekly_pnl, week_start, seen_ids

    while True:
        try:
            trades = await asyncio.to_thread(
                exchange.fetch_my_trades,
                None,
                50
            )

            positions = {}

            for t in trades:
                ts = t.get("timestamp", 0)

                if ts <= last_checked:
                    continue

                trade_id = t.get("id")
                if trade_id in seen_ids:
                    continue

                seen_ids.add(trade_id)

                info = t.get("info", {})

                pnl = float(
                    info.get("realizedPnl")
                    or info.get("profit")
                    or info.get("closedPnl")
                    or 0
                )

                if pnl == 0:
                    continue

                symbol = t.get("symbol", "")
                side = t.get("side", "")
                amount = float(t.get("amount", 0))

                # 🔥 ключ позиції
                key = f"{symbol}_{side}"

                if key not in positions:
                    positions[key] = {
                        "pnl": 0.0,
                        "qty": 0.0,
                        "trades": 0,
                        "ts": ts
                    }

                positions[key]["pnl"] += pnl
                positions[key]["qty"] += amount
                positions[key]["trades"] += 1

                if ts > positions[key]["ts"]:
                    positions[key]["ts"] = ts

            # 📤 відправка 1 повідомлення на позицію
            for key, data in positions.items():
                symbol, side = key.split("_")

                pnl = data["pnl"]
                qty = data["qty"]
                trades_count = data["trades"]

                status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

                msg = (
                    f"{status} #{symbol}\n"
                    f"Side: {side}\n"
                    f"Total PnL: {round(pnl, 4)} USDT\n"
                    f"Total Qty: {round(qty, 4)}\n"
                    f"Trades: {trades_count}"
                )

                weekly_pnl += pnl

                await app.send_message(log_chat_id, msg)

            # 🧠 оновлюємо last_checked
            if positions:
                last_checked = max(p["ts"] for p in positions.values())

            # 🧹 чистка памʼяті
            if len(seen_ids) > 1000:
                seen_ids.clear()

            # 📊 weekly report
            if time.time() - week_start >= 7 * 24 * 60 * 60:
                status = "🟢 PROFIT" if weekly_pnl > 0 else "🔴 LOSS"

                report = (
                    f"📊 WEEKLY REPORT\n\n"
                    f"{status}\n"
                    f"Total PnL: {round(weekly_pnl, 4)} USDT"
                )

                await app.send_message(log_chat_id, report)

                weekly_pnl = 0.0
                week_start = time.time()

        except Exception as e:
            log("ERROR", f"PNL watcher error: {e}")

        await asyncio.sleep(interval)