import asyncio
import time

# =========================
# GLOBAL STATE
# =========================
last_checked = 0
weekly_pnl = 0.0
week_start = time.time()

seen_ids = set()

# антиспам
LAST_SENT = {}
COOLDOWN = 10  # секунд


async def pnl_watcher(app, exchange, log, log_chat_id, interval=5):
    global last_checked, weekly_pnl, week_start, seen_ids, LAST_SENT

    while True:
        try:
            # =========================
            # 🔥 ВСІ трейди
            # =========================
            trades = await asyncio.to_thread(
                exchange.fetch_my_trades,
                None,
                None,
                100
            )

            if not trades:
                await asyncio.sleep(interval)
                continue

            closed_positions = {}

            for t in trades:
                ts = t.get("timestamp", 0)

                if ts <= last_checked:
                    continue

                trade_id = t.get("id")
                if not trade_id or trade_id in seen_ids:
                    continue

                seen_ids.add(trade_id)

                info = t.get("info", {})

                # =========================
                # 🔥 тільки закриття
                # =========================
                reduce_only = (
                    t.get("reduceOnly")
                    or info.get("reduceOnly")
                    or False
                )

                if not reduce_only:
                    continue

                pnl = float(
                    info.get("realizedPnl")
                    or info.get("closedPnl")
                    or info.get("profit")
                    or 0
                )

                if pnl == 0:
                    continue

                symbol = t.get("symbol", "")
                amount = float(t.get("amount", 0))

                if symbol not in closed_positions:
                    closed_positions[symbol] = {
                        "pnl": 0.0,
                        "qty": 0.0,
                        "trades": 0,
                        "ts": ts
                    }

                closed_positions[symbol]["pnl"] += pnl
                closed_positions[symbol]["qty"] += amount
                closed_positions[symbol]["trades"] += 1

                if ts > closed_positions[symbol]["ts"]:
                    closed_positions[symbol]["ts"] = ts

            # =========================
            # 🔥 відправка
            # =========================
            for symbol, data in closed_positions.items():
                pnl = data["pnl"]
                qty = data["qty"]

                now = time.time()
                last = LAST_SENT.get(symbol, 0)

                # антиспам
                if now - last < COOLDOWN:
                    continue

                LAST_SENT[symbol] = now

                status = "🟢 PROFIT" if pnl > 0 else "🔴 LOSS"

                msg = (
                    f"{status} #{symbol}\n"
                    f"PnL: {round(pnl, 4)} USDT\n"
                    f"Qty: {round(qty, 4)}"
                )

                weekly_pnl += pnl

                await app.send_message(log_chat_id, msg)

            # =========================
            # 🔥 оновлюємо timestamp
            # =========================
            last_checked = max(t.get("timestamp", 0) for t in trades)

            # =========================
            # 🔥 чистка кеша
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