import os
import asyncio
import threading
import signal
import contextlib
from datetime import datetime
from pyrogram import Client, filters

# ================= ENV =================
def req(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Missing ENV: {key}")
    return v.strip()

API_ID = int(req("TG_API_ID"))
API_HASH = req("TG_API_HASH")
SESSION_STRING = req("TG_SESSION_STRING")
TARGET_CHAT_RAW = req("TARGET_CHAT")

TARGET_CHAT = int(TARGET_CHAT_RAW) if TARGET_CHAT_RAW.lstrip("-").isdigit() else TARGET_CHAT_RAW
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "300"))

print("SESSION_STRING length:", len(SESSION_STRING))
print("TARGET_CHAT =", TARGET_CHAT)

# ================= HEALTH SERVER =================
def start_health_server():
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await reader.read(2048)
            first_line = data.split(b"\r\n", 1)[0] if data else b""

            if first_line.startswith(b"GET /health "):
                body = b"ok"
                resp = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: 2\r\n"
                    b"Connection: close\r\n"
                    b"\r\n" + body
                )
            else:
                body = b"not found"
                resp = (
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: 9\r\n"
                    b"Connection: close\r\n"
                    b"\r\n" + body
                )

            writer.write(resp)
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def server():
        port = int(os.getenv("PORT", "8080"))
        srv = await asyncio.start_server(handle, "0.0.0.0", port)
        print(f"✅ health server running on {port}")
        async with srv:
            await srv.serve_forever()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(server())

# Запускаємо health server в окремому потоці
threading.Thread(target=start_health_server, daemon=True).start()

# ================= PYROGRAM =================
app = Client(
    name="prod_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    text = message.text or message.caption
    ts = datetime.now().isoformat(timespec="seconds")
    if text:
        print(f"[{ts}] {text}")
    else:
        print(f"[{ts}] (non-text message)")

async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)

async def main():
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop():
        if not stop_event.is_set():
            stop_event.set()

    # Railway шле SIGTERM при зупинці/деплої
    with contextlib.suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGTERM, _stop)
        loop.add_signal_handler(signal.SIGINT, _stop)

    hb_task = asyncio.create_task(heartbeat())
    print("✅ BOT STARTED")

    try:
        # чекаємо поки Railway не попросить завершитись
        await stop_event.wait()
    finally:
        print("🛑 stop signal received, shutting down...")

        hb_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb_task

if __name__ == "__main__":
    app.run(main())