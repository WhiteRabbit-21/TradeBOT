import os
from datetime import datetime
from pyrogram import Client, filters, idle

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]
TARGET_CHAT = os.environ["TARGET_CHAT"].strip()  # @username або -100...

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

def log(line: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}", flush=True)

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    text = (message.text or message.caption or "").replace("\n", " ")
    log(f"chat={message.chat.id} title={message.chat.title!r} msg_id={message.id} text={text[:250]}")

async def main():
    await app.start()
    me = await app.get_me()
    log(f"✅ Logged in as: @{me.username or ''} {me.first_name or ''}".strip())

    chat = await app.get_chat(TARGET_CHAT)
    log(f"🎯 Tracking: {chat.title} ({chat.id})")

    log("👂 Listening…")
    await idle()

    await app.stop()

if __name__ == "__main__":
    app.run(main())