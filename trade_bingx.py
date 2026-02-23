import os
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid
import asyncio

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHAT = -1002483915667  # <-- твій чат

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

def log(line: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}", flush=True)

@app.on_message(filters.chat(TARGET_CHAT))
async def on_message(_, message):
    text = (message.text or message.caption or "").strip()
    log(f"📩 msg_id={message.id} text={text[:400]}")

async def wait_until_chat_available():
    while True:
        try:
            chat = await app.get_chat(TARGET_CHAT)
            log(f"🎯 Tracking: {chat.title} ({chat.id})")
            return
        except PeerIdInvalid:
            log("⚠️ Нема доступу до чату. Перевір що акаунт підписаний. Retry in 30s…")
            await asyncio.sleep(30)
        except Exception as e:
            log(f"⚠️ get_chat error: {e}. Retry in 30s…")
            await asyncio.sleep(30)

async def main():
    await app.start()

    me = await app.get_me()
    log(f"✅ Logged in as: @{me.username or ''} {me.first_name or ''}".strip())

    await wait_until_chat_available()

    log("👂 Listening…")
    await idle()

    await app.stop()

if __name__ == "__main__":
    app.run(main())