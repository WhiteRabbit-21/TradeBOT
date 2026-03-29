import asyncio
import time

last_checked = 0
weekly_pnl = 0.0
week_start = time.time()

seen_ids = set()

# 🔥 список активних символів
ACTIVE_SYMBOLS = set()


def register_symbol(symbol: str):
    if symbol:
        ACTIVE_SYMBOLS.add(symbol)


async def pnl_watcher(app, exchange, log, log_chat_id, interval=5):
    global last_checked, weekly_pnl, week_start, seen_ids

    while True:
        try:
            # =========================
            # 🔥 беремо тільки активні символи
            # =========================
            symbols = list(ACTIVE_SYMBOLS)

            # fallback якщо ще нічого нема
            if not symbols:
                await asyncio.sleep(interval)
                continue

            all_trades = []

            for symbol in symbols:
                try:
                    trades = await asyncio.to_thread(
                        exchange.fetch_my_trades,
                        symbol,
                        None,
                        50
                    )
                    all_trades.extend(trades)

                except Exception as e:
                    log("WARNING", f"fetch trades failed {symbol}: {e}")

            if not all_trades:
                await asyncio.sleep(interval)
                continue

            positions = {}

            # =========================
            # 🔥 обробка трейдів
            # =========================
            for t in all_trades:
                ts = t.get("timestamp", 0)

                if ts <= last_checked:
                    continue

                trade_id = t.get("id")
                if not trade_id:
                    continue

                if trade_id in seen_ids:
                    continue

                seen_ids.add(trade_id)

                info = t.get("info", {})

                pnl = float(
                    info.get("realizedPnl")
                    or info.get("profit")
                    or info.get("closedPnl")
                    or t.get("cost")  # fallback
                    or 0
                )

                if pnl == 0:
                    continue

                symbol = t.get("symbol", "")
                side = t.get("side", "")
                amount = float(t.get("amount", 0))

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

            # =========================
            # 🔥 відправка повідомлень
            # =========================
            for key, data in positions.items():
                symbol, side = key.split("_")

                pnl = data["pnl"]
                qty = data["qty"]
                trades_count = data["trades"]

                status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

                msg = (
                    f"{status} #{symbol}\n"
                    f"Side: {side}\n"
                    f"PnL: {round(pnl, 4)} USDT\n"
                    f"Qty: {round(qty, 4)}\n"
                    f"Trades: {trades_count}"
                )

                weekly_pnl += pnl

                await app.send_message(log_chat_id, msg)

            # =========================
            # 🔥 оновлення часу
            # =========================
            if all_trades:
                last_checked = max(t.get("timestamp", 0) for t in all_trades)

            # =========================
            # 🔥 чистка seen_ids
            # =========================
            if len(seen_ids) > 5000:
                seen_ids = set(list(seen_ids)[-2000:])

            # =========================
            # 🔥 weekly report
            # =========================
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