import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid, FloodWait, RPCError


API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "-1002598403649"))

# твій приватний канал для логів:
LOG_CHAT_ID = int(os.getenv("TG_LOG_CHAT_ID", "-1003828203122"))

# --- Logging config (hardcoded) ---
LOG_LEVEL = "INFO"     # DEBUG / INFO / WARNING / ERROR
LOG_FLUSH_SEC = 20     # сек, раз на скільки зливаємо INFO пачкою

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

# -------------------------
# TG LOGGER (priority)
# -------------------------
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_min_level = _LEVELS.get(LOG_LEVEL, 20)

_log_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()  # (level, line)


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _send_to_tg(text: str):
    """Надсилає текст в LOG_CHAT_ID, з анти-flood."""
    if LOG_CHAT_ID == 0:
        return
    try:
        await app.send_message(LOG_CHAT_ID, text)
    except FloodWait as e:
        await asyncio.sleep(int(e.value) + 1)
        try:
            await app.send_message(LOG_CHAT_ID, text)
        except Exception:
            pass
    except RPCError:
        pass
    except Exception:
        pass


def log(level: str, msg: str):
    """
    INFO/DEBUG -> в буфер
    ERROR -> одразу
    WARNING -> одразу тільки якщо "важливий" (можеш поміняти правило)
    """
    lvl_name = level.upper()
    lvl = _LEVELS.get(lvl_name, 20)
    if lvl < _min_level:
        return

    line = f"[{_ts()}] [{lvl_name}] {msg}"

    # ERROR -> immediately
    if lvl_name == "ERROR":
        # одразу в TG (через task, бо ми в sync контексті інколи)
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_send_to_tg(line))
        except Exception:
            pass
        return

    # IMPORTANT WARNING -> immediately (правило: якщо є ключові слова)
    if lvl_name == "WARNING" and any(k in msg.lower() for k in ["peer", "invalid", "failed", "error", "denied"]):
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_send_to_tg(line))
        except Exception:
            pass
        return

    # everything else -> queue
    try:
        _log_queue.put_nowait((lvl_name, line))
    except Exception:
        pass


async def log_pump():
    """
    Зливає INFO/DEBUG (і не-термінові WARNING) пачкою кожні LOG_FLUSH_SEC.
    """
    buf: list[str] = []

    while True:
        try:
            # чекаємо хоча б один лог
            _, line = await _log_queue.get()
            buf.append(line)

            start = asyncio.get_event_loop().time()
            while True:
                now = asyncio.get_event_loop().time()
                if now - start >= LOG_FLUSH_SEC:
                    break
                try:
                    _, nxt = _log_queue.get_nowait()
                    buf.append(nxt)
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.2)

            if not buf:
                continue

            # Ріжемо на шматки до ~3500 символів
            chunk = ""
            for ln in buf:
                if len(chunk) + len(ln) + 1 > 3500:
                    await _send_to_tg(chunk.strip())
                    chunk = ""
                chunk += ln + "\n"

            if chunk.strip():
                await _send_to_tg(chunk.strip())

            buf.clear()

        except Exception:
            await asyncio.sleep(1)


# -------------------------
# PEER WARMUP
# -------------------------
async def ensure_peer_known(chat_id: int) -> bool:
    try:
        chat = await app.get_chat(chat_id)
        title = getattr(chat, "title", "") or getattr(chat, "first_name", "")
        log("INFO", f"🎯 get_chat OK: {title} ({chat.id})")
        return True
    except PeerIdInvalid:
        log("WARNING", f"get_chat({chat_id}) -> PEER_ID_INVALID. Шукаю через dialogs…")
    except Exception as e:
        log("WARNING", f"get_chat({chat_id}) error: {e}. Пробую через dialogs…")

    try:
        async for dialog in app.get_dialogs(limit=300):
            c = dialog.chat
            if c and c.id == chat_id:
                title = getattr(c, "title", "") or getattr(c, "first_name", "")
                log("INFO", f"✅ Found in dialogs: {title} ({c.id})")
                await app.get_chat(chat_id)
                log("INFO", "✅ Peer warmed up")
                return True

        log("ERROR", f"❌ Не знайшов chat_id={chat_id} у dialogs.")
        return False

    except Exception as e:
        log("ERROR", f"❌ dialogs scan error: {e}")
        return False


# -------------------------
# HANDLERS
# -------------------------
@app.on_message(filters.chat(TARGET_CHAT_ID))
async def on_message(_, message):
    text = (message.text or message.caption or "").replace("\n", " ").strip()
    log("INFO", f"📩 TARGET msg_id={message.id} text={text[:400]}")


async def main():
    await app.start()

    # запускаємо лог-памп (для INFO пачок)
    asyncio.create_task(log_pump())

    me = await app.get_me()
    log("INFO", f"✅ Logged in as: @{me.username or ''} {me.first_name or ''}".strip())

    # прогріваємо target
    ok = await ensure_peer_known(TARGET_CHAT_ID)
    while not ok:
        log("WARNING", "⏳ TARGET retry in 60s…")
        await asyncio.sleep(60)
        ok = await ensure_peer_known(TARGET_CHAT_ID)

    # прогріваємо лог-канал
    if LOG_CHAT_ID != 0:
        ok2 = await ensure_peer_known(LOG_CHAT_ID)
        if ok2:
            # ці 2 — важливо бачити одразу
            await _send_to_tg(f"[{_ts()}] [INFO] 🧾 Telegram logging ON. log_chat_id={LOG_CHAT_ID}")
        else:
            await _send_to_tg(f"[{_ts()}] [ERROR] 🧾 Telegram logging FAILED. log_chat_id={LOG_CHAT_ID}")

    log("INFO", "👂 Listening…")
    await idle()
    await app.stop()


if __name__ == "__main__":
    app.run(main)