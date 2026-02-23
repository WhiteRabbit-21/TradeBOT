import os
from datetime import datetime
from pyrogram import Client, filters, idle

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

def log(line: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}", flush=True)

# Saved Messages == "me"
@app.on_message(filters.chat("me"))
async def on_saved_message(_, message):
    text = (message.text or message.caption or "").strip()
    # Можна ще message.from_user, але в Saved Messages це завжди ти
    log(f"📝 Saved msg_id={message.id} text={text[:400]}")

async def main():
    await app.start()
    me = await app.get_me()
    log(f"✅ Logged in as: @{me.username or ''} {me.first_name or ''}".strip())
    log("👂 Listening Saved Messages…")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())