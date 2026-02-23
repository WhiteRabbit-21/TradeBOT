import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters, idle


def req(key: str) -> str:
    v = os.getenv(key)
    if v is None or not v.strip():
        raise RuntimeError(f"Missing/empty ENV: {key}")
    return v.strip()


API_ID = int(req("TG_API_ID"))
API_HASH = req("TG_API_HASH")
SESSION_STRING = req("TG_SESSION_STRING")

TARGET_CHAT_RAW = req("TARGET_CHAT")  # може бути @username або -100...
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))
DUMP_HISTORY = os.getenv("DUMP_HISTORY", "0").strip() == "1"  # опційно
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "5"))

# ✅ ВАЖЛИВО: якщо chat id — конвертуємо в int
if TARGET_CHAT_RAW.lstrip("-").isdigit():
    TARGET_CHAT = int(TARGET_CHAT_RAW)
else:
    TARGET_CHAT = TARGET_CHAT_RAW

print("SESSION_STRING length:", len(SESSION_STRING))
print("TARGET_CHAT =", TARGET_CHAT, "(raw:", TARGET_CHAT_RAW, ")")

app = Client(
    name="prod_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

# ✅ DEBUG: показує, що реально прилітає (chat.id, title, type)
# Якщо не хочеш спам — постав DEBUG_ALL=0 в Railway
DEBUG_ALL = os.getenv("DEBUG_ALL", "1").strip() == "1"


@app.on_message()
async def debug_all(_, message):
    if not DEBUG_ALL:
        return
    # покажемо тільки мету, без всіх текстів, щоб не спамити
    print(
        f"DEBUG CHAT => id={message.chat.id} title={getattr(message.chat, 'title', None)} "
        f"type={message.chat.type}"
    )


@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    # ловимо текст/підпис або хоч якийсь тип контенту
    text = message.text or message.caption
    if text:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] MSG: {text}")
    else:
        # якщо нема тексту — покажемо тип контенту
        print(
            f"[{datetime.now().isoformat(timespec='seconds')}] NON-TEXT message: "
            f"photo={bool(message.photo)} video={bool(message.video)} document={bool(message.document)} "
            f"sticker={bool(message.sticker)}"
        )


async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)


async def dump_last_messages():
    print(f"📥 Dump last {HISTORY_LIMIT} messages from {TARGET_CHAT} ...")
    try:
        async for m in app.get_chat_history(TARGET_CHAT, limit=HISTORY_LIMIT):
            t = m.text or m.caption or ""
            if t:
                print("-", t[:200])
            else:
                print("-", "[no text]")
    except Exception as e:
        print("❌ dump_last_messages error:", repr(e))


async def main():
    await app.start()
    print("✅ started")

    asyncio.create_task(heartbeat())

    # опційно: зчитати останні повідомлення одразу після старту
    if DUMP_HISTORY:
        await dump_last_messages()

    print("➡️ waiting in idle() ...")
    await idle()

    print("🛑 idle() returned, stopping app ...")
    await app.stop()
    print("✅ app stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())