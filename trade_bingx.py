import os
import asyncio
from datetime import datetime
from pyrogram import Client, filters

def req(key: str) -> str:
    v = os.getenv(key)
    if v is None or not v.strip():
        raise RuntimeError(f"Missing/empty ENV: {key}")
    return v.strip()

API_ID = int(req("TG_API_ID"))
API_HASH = req("TG_API_HASH")
SESSION_STRING = req("TG_SESSION_STRING")

TARGET_CHAT_RAW = req("TARGET_CHAT")
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))
DEBUG_ALL = os.getenv("DEBUG_ALL", "0").strip() == "1"

# chat id як int
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

@app.on_message()
async def debug_all(_, message):
    if not DEBUG_ALL:
        return
    print(f"DEBUG CHAT => id={message.chat.id} title={getattr(message.chat,'title',None)} type={message.chat.type}")

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    text = message.text or message.caption
    if text:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] MSG: {text}")
    else:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] NON-TEXT message")

async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)

# ---- Async health server (без потоків) ----
async def handle_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        # прочитаємо хоч щось з запиту
        await reader.read(1024)
        body = b"ok"
        resp = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n"
            b"\r\n" + body
        )
        writer.write(resp)
        await writer.drain()
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def start_health_server():
    port = int(os.getenv("PORT", "8080"))
    server = await asyncio.start_server(handle_http, "0.0.0.0", port)
    print(f"✅ health server started on 0.0.0.0:{port}")
    return server

async def main():
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # SIGTERM/SIGINT (Railway) → завершуємося акуратно
    for sig in ("SIGTERM", "SIGINT"):
        if hasattr(asyncio, "signals") and False:
            pass
    try:
        import signal
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
    except Exception:
        # якщо сигнал-хендлери недоступні — нічого страшного
        pass

    health_server = await start_health_server()

    await app.start()
    print("✅ started")
    asyncio.create_task(heartbeat())

    print("➡️ running (waiting stop signal) ...")
    await stop_event.wait()

    print("🛑 stop signal received, shutting down...")

    # закриваємо health server
    health_server.close()
    await health_server.wait_closed()

    # Pyrogram stop інколи дає "different loop" на платформах/рестартах
    try:
        await app.stop()
    except RuntimeError as e:
        print("⚠️ app.stop RuntimeError (ignored):", e)

if __name__ == "__main__":
    asyncio.run(main())