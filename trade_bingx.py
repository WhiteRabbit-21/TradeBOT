import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHAT_ID = -1002598403649

app = Client(
    name="user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

def log(line: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}", flush=True)

async def ensure_peer_known():
    """
    Для приватних чатів/каналів: переконуємось, що Pyrogram 'знає' peer,
    прогнавши dialogs і зустрівши чат.
    """
    # 1) спроба напряму
    try:
        chat = await app.get_chat(TARGET_CHAT_ID)
        log(f"🎯 get_chat OK: {chat.title} ({chat.id})")
        return True
    except PeerIdInvalid:
        log("⚠️ get_chat -> PEER_ID_INVALID. Спробую знайти чат через dialogs…")
    except Exception as e:
        log(f"⚠️ get_chat error: {e}. Спробую через dialogs…")

    # 2) fallback: шукаємо в dialogs
    try:
        async for dialog in app.get_dialogs(limit=200):
            c = dialog.chat
            if c and c.id == TARGET_CHAT_ID:
                log(f"✅ Found in dialogs: {c.title} ({c.id})")
                # додатково “торкнемось” чату
                _ = await app.get_chat(TARGET_CHAT_ID)
                log("✅ Peer warmed up")
                return True

        log("❌ Не знайшов цей чат у dialogs.")
        log("👉 Перевір: ти точно під цим акаунтом підписана на канал саме зараз?")
        log("👉 Відкрий канал у Telegram (телефон/desktop), пролистай, і перезапусти Railway.")
        return False

    except Exception as e:
        log(f"❌ dialogs scan error: {e}")
        return False

@app.on_message(filters.chat(TARGET_CHAT_ID))
async def on_message(_, message):
    text = (message.text or message.caption or "").replace("\n", " ").strip()
    log(f"📩 msg_id={message.id} text={text[:400]}")

async def main():
    await app.start()
    me = await app.get_me()
    log(f"✅ Logged in as: @{me.username or ''} {me.first_name or ''}".strip())

    ok = await ensure_peer_known()
    if not ok:
        # не падаємо — просто чекаємо і пробуємо ще раз
        while True:
            log("⏳ Waiting 60s before retry…")
            await asyncio.sleep(60)
            ok = await ensure_peer_known()
            if ok:
                break

    log("👂 Listening…")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())