import os
import sys
from datetime import datetime
from dotenv import load_dotenv

from pyrogram import Client, filters

load_dotenv()

# -------------------- ENV --------------------
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")

# ВАРІАНТ 1 (кращий для деплою): Session String (не створює session-файл)
TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "")

# ВАРІАНТ 2: Bot token (якщо читаєш як бот; бот має бути в каналі/адміном)
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")

# ВАРІАНТ 3: session file name (не рекомендую для Railway, але лишив як fallback)
SESSION_NAME = os.getenv("TG_SESSION", "tradebot_session")

TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "").strip()  # наприклад: -1002598403649 або @username

if not TG_API_ID or not TG_API_HASH:
    print("❌ TG_API_ID / TG_API_HASH not set")
    sys.exit(1)

if not TARGET_CHANNEL_ID:
    print("❌ TARGET_CHANNEL_ID not set (example: -1001234567890 or @channelusername)")
    sys.exit(1)

# Приводимо ID до int якщо це число
try:
    if TARGET_CHANNEL_ID.lstrip("-").isdigit():
        TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID)
except Exception:
    pass


# -------------------- LOG --------------------
def log(status: str, msg: str = ""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [{status}] {msg}", flush=True)


# -------------------- CLIENT --------------------
def build_client() -> Client:
    # 1) BOT
    if TG_BOT_TOKEN:
        log("ENV", "Using TG_BOT_TOKEN")
        return Client(
            name="tradebot_bot",
            api_id=TG_API_ID,
            api_hash=TG_API_HASH,
            bot_token=TG_BOT_TOKEN,
            in_memory=True,
        )

    # 2) SESSION STRING (рекомендовано)
    if TG_SESSION_STRING:
        log("ENV", "Using TG_SESSION_STRING")
        return Client(
            name="tradebot_user",
            api_id=TG_API_ID,
            api_hash=TG_API_HASH,
            session_string=TG_SESSION_STRING,
            in_memory=True,
        )

    # 3) SESSION FILE (fallback)
    log("ENV", f"Using session file name: {SESSION_NAME}")
    return Client(
        name=SESSION_NAME,
        api_id=TG_API_ID,
        api_hash=TG_API_HASH,
    )


app = build_client()


# -------------------- HANDLERS --------------------
def extract_text(message) -> str:
    return (message.text or message.caption or "").strip()


@app.on_message(filters.chat(TARGET_CHANNEL_ID) & (filters.text | filters.caption))
def on_channel_message(client: Client, message):
    text = extract_text(message)
    if not text:
        return

    chat = message.chat
    log(
        "CHAN",
        f"from={chat.title or chat.username or chat.id} | msg_id={message.id} | text={text[:200]}"
    )

    # TODO: тут викликай свій парсер/трейдинг-логіку
    # handle_signal(text)


# OPTIONAL: якщо ще хочеш ловити Saved Messages
ENABLE_SAVED = os.getenv("ENABLE_SAVED", "false").lower() in ("1", "true", "yes")


if ENABLE_SAVED:
    @app.on_message(filters.me & (filters.text | filters.caption))
    def on_saved_message(client: Client, message):
        text = extract_text(message)
        if not text:
            return
        log("SAVED", f"text={text[:200]}")
        # handle_signal(text)


# -------------------- RUN --------------------
if __name__ == "__main__":
    log("BOOT", "Bot starting...")
    log("ENV", f"TG_API_ID={TG_API_ID} TG_API_HASH={'YES' if TG_API_HASH else 'NO'} "
               f"TG_SESSION_STRING={'YES' if TG_SESSION_STRING else 'NO'} TG_BOT_TOKEN={'YES' if TG_BOT_TOKEN else 'NO'}")
    log("ENV", f"TARGET_CHANNEL_ID={TARGET_CHANNEL_ID} ENABLE_SAVED={ENABLE_SAVED}")

    try:
        app.run()
    except Exception as e:
        log("FATAL", f"app.run() crashed: {e}")
        raise