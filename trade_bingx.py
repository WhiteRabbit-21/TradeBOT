import os
import sys
import asyncio
from datetime import datetime

from dotenv import load_dotenv
from pyrogram import Client, filters

load_dotenv()

# -------------------- LOG --------------------
def log(status: str, msg: str = ""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [{status}] {msg}", flush=True)

def die(msg: str, code: int = 1):
    log("FATAL", msg)
    raise SystemExit(code)

# -------------------- ENV --------------------
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "").strip()

TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "").strip()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()

TARGET_CHANNEL_ID_RAW = os.getenv("TARGET_CHANNEL_ID", "").strip()
ENABLE_SAVED = os.getenv("ENABLE_SAVED", "false").lower() in ("1", "true", "yes")

if not TG_API_ID or not TG_API_HASH:
    die("TG_API_ID / TG_API_HASH not set")

if not TARGET_CHANNEL_ID_RAW:
    die("TARGET_CHANNEL_ID not set (example: -1001234567890 or @channelusername)")

TARGET_CHANNEL_ID = TARGET_CHANNEL_ID_RAW
if TARGET_CHANNEL_ID_RAW.lstrip("-").isdigit():
    TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_RAW)

# -------------------- CLIENT --------------------
def build_client() -> Client:
    if TG_BOT_TOKEN:
        log("ENV", "Using TG_BOT_TOKEN")
        return Client(
            "tradebot_bot",
            api_id=TG_API_ID,
            api_hash=TG_API_HASH,
            bot_token=TG_BOT_TOKEN,
            in_memory=True,
        )

    if TG_SESSION_STRING:
        log("ENV", "Using TG_SESSION_STRING")
        return Client(
            "tradebot_user",
            api_id=TG_API_ID,
            api_hash=TG_API_HASH,
            session_string=TG_SESSION_STRING,
            in_memory=True,
        )

    die("Neither TG_BOT_TOKEN nor TG_SESSION_STRING provided")

app = build_client()

# -------------------- HANDLERS --------------------
def extract_text(message) -> str:
    return (message.text or message.caption or "").strip()

@app.on_message(filters.chat(TARGET_CHANNEL_ID) & (filters.text | filters.caption))
async def on_channel_message(client: Client, message):
    text = extract_text(message)
    if not text:
        return
    chat = message.chat
    log("CHAN", f"from={chat.title or chat.username or chat.id} | msg_id={message.id} | text={text[:200]}")
    # TODO: handle_signal(text)

if ENABLE_SAVED:
    @app.on_message(filters.me & (filters.text | filters.caption))
    async def on_saved_message(client: Client, message):
        text = extract_text(message)
        if text:
            log("SAVED", f"text={text[:200]}")

# -------------------- RUN --------------------
async def main():
    log("BOOT", "Bot starting...")
    log("ENV", f"TG_API_ID={TG_API_ID} TG_API_HASH={'YES' if TG_API_HASH else 'NO'} "
               f"TG_SESSION_STRING={'YES' if TG_SESSION_STRING else 'NO'} TG_BOT_TOKEN={'YES' if TG_BOT_TOKEN else 'NO'} "
               f"TARGET_CHANNEL_ID={TARGET_CHANNEL_ID} ENABLE_SAVED={ENABLE_SAVED}")

    await app.start()
    log("BOOT", "Pyrogram started. Blocking forever...")
    await asyncio.Event().wait()   # держим процесс живым всегда

if __name__ == "__main__":
    asyncio.run(main())