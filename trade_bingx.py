import os
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid


# =========================
# CONFIG
# =========================

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHAT_ID = -1002598403649

# Volume path (Railway)
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)


# =========================
# LOGGING SETUP
# =========================

def setup_logger():
    logger = logging.getLogger("tradebot")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # Console handler (Railway logs)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (daily rotation)
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(LOG_DIR, "bot.log"),
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()


# =========================
# PYROGRAM CLIENT
# =========================

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)


# =========================
# FUNCTIONS
# =========================

async def ensure_peer_known():
    """
    Для приватних чатів/каналів:
    переконуємось, що Pyrogram 'знає' peer.
    """
    try:
        chat = await app.get_chat(TARGET_CHAT_ID)
        logger.info(f"🎯 get_chat OK: {chat.title} ({chat.id})")
        return True
    except PeerIdInvalid:
        logger.warning("get_chat -> PEER_ID_INVALID. Пробую через dialogs…")
    except Exception as e:
        logger.warning(f"get_chat error: {e}. Пробую через dialogs…")

    try:
        async for dialog in app.get_dialogs(limit=200):
            c = dialog.chat
            if c and c.id == TARGET_CHAT_ID:
                logger.info(f"✅ Found in dialogs: {c.title} ({c.id})")
                await app.get_chat(TARGET_CHAT_ID)
                logger.info("✅ Peer warmed up")
                return True

        logger.error("❌ Чат не знайдено у dialogs.")
        return False

    except Exception as e:
        logger.exception(f"❌ dialogs scan error: {e}")
        return False


@app.on_message(filters.chat(TARGET_CHAT_ID))
async def on_message(_, message):
    text = (message.text or message.caption or "").replace("\n", " ").strip()
    logger.info(f"📩 msg_id={message.id} text={text[:400]}")


async def main():
    logger.info("🚀 Starting client...")
    await app.start()

    me = await app.get_me()
    logger.info(f"✅ Logged in as: @{me.username or ''} {me.first_name or ''}".strip())

    ok = await ensure_peer_known()
    while not ok:
        logger.warning("⏳ Retry in 60s...")
        await asyncio.sleep(60)
        ok = await ensure_peer_known()

    logger.info("👂 Listening...")
    await idle()
    await app.stop()


if __name__ == "__main__":
    app.run(main())