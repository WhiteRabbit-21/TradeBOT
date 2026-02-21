import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.session import StringSession

def req(key: str) -> str:
    v = os.getenv(key)
    if v is None or not v.strip():
        raise RuntimeError(f"Missing/empty ENV: {key}")
    return v.strip()

API_ID = int(req("TG_API_ID"))
API_HASH = req("TG_API_HASH")
SESSION_STRING = req("TG_SESSION_STRING")
TARGET_CHAT = req("TARGET_CHAT")

print("SESSION_STRING length:", len(SESSION_STRING))  # тимчасово для перевірки

app = Client(
    StringSession(SESSION_STRING),
    api_id=API_ID,
    api_hash=API_HASH,
)

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    text = message.text or message.caption or ""
    if text:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] {text}")

async def main():
    await app.start()
    print("✅ started")
    await app.idle()

if __name__ == "__main__":
    asyncio.run(main())