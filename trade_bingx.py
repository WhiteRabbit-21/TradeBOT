import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters
from pyrogram import idle

def req(key: str) -> str:
    v = os.getenv(key)
    if v is None or not v.strip():
        raise RuntimeError(f"Missing/empty ENV: {key}")
    return v.strip()

API_ID = int(req("TG_API_ID"))
API_HASH = req("TG_API_HASH")
SESSION_STRING = req("TG_SESSION_STRING")
TARGET_CHAT = req("TARGET_CHAT")
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))

print("SESSION_STRING length:", len(SESSION_STRING))  # діагностика, потім прибереш

app = Client(
    name="prod_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,   # <-- ключове
)

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    text = message.text or message.caption or ""
    if text:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] {text}")

async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)

from pyrogram import idle

async def main():
    await app.start()
    print("✅ started")
    asyncio.create_task(heartbeat())

    print("➡️ waiting in idle() ...")
    await idle()

    print("🛑 idle() returned, stopping app ...")
    await app.stop()
    print("✅ app stopped cleanly")
    
if __name__ == "__main__":
    asyncio.run(main())