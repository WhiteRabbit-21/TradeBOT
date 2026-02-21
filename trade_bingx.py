import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters

def env_required(key: str) -> str:
    v = os.getenv(key)
    if v is None or not v.strip():
        raise RuntimeError(f"Missing/empty ENV: {key}")
    return v.strip()

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TARGET_CHAT = os.getenv("TARGET_CHAT", "")  # @channel або -100...

HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))

if not API_ID or not API_HASH or not TARGET_CHAT:
    raise RuntimeError("Заповни TG_API_ID, TG_API_HASH, TARGET_CHAT в Environment Variables")

app = Client(
    name="prod_user",
    api_id=API_ID,
    api_hash=API_HASH,
)

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(client, message):
    text = message.text or message.caption or ""
    print(f"[{datetime.now().isoformat(timespec='seconds')}] chat={message.chat.id} from={getattr(message.from_user,'id',None)}")
    if text:
        print(text)

async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)

async def main():
    await app.start()
    print("✅ User session started")
    asyncio.create_task(heartbeat())
    await app.idle()

if __name__ == "__main__":
    asyncio.run(main())