import os
import asyncio
import signal
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

TARGET_CHAT = int(TARGET_CHAT_RAW) if TARGET_CHAT_RAW.lstrip("-").isdigit() else TARGET_CHAT_RAW

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
    print(
        f"DEBUG CHAT => id={message.chat.id} "
        f"title={getattr(message.chat,'title',None)} type={message.chat.type}"
    )

@app.on_message(filters.chat(TARGET_CHAT))
async def on_msg(_, message):
    text = message.text or message.caption
    ts = datetime.now().isoformat(timespec="seconds")
    if text:
        print(f"[{ts}] MSG: {text}")
    else:
        print(f"[{ts}] NON-TEXT message")

async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)

# -------------------------
# Health server for Railway
# -------------------------
async def handle_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        req_bytes = await reader.read(2048)
        first_line = req_bytes.split(b"\r\n", 1)[0] if req_bytes else b""

        # Expecting something like: b"GET /health HTTP/1.1"
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
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def start_health_server():
    port = int(os.getenv("PORT", "8080"))  # Railway дає PORT
    server = await asyncio.start_server(handle_http, "0.0.0.0", port)
    print(f"✅ health server started on 0.0.0.0:{port}")
    return server

# -------------------------
# Async "idle" (SIGTERM/SIGINT)
# -------------------------
async def wait_for_stop_signal():
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop():
        if not stop_event.is_set():
            stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _stop)
        loop.add_signal_handler(signal.SIGINT, _stop)
    except NotImplementedError:
        # на всяк випадок (наприклад, якщо десь Windows)
        pass

    await stop_event.wait()

async def main():
    health_server = await start_health_server()
    hb_task = asyncio.create_task(heartbeat())

    print("✅ started")
    print("➡️ running (waiting stop signal) ...")

    try:
        await wait_for_stop_signal()
    finally:
        print("🛑 stop signal received, shutting down...")

        hb_task.cancel()
        try:
            await hb_task
        except Exception:
            pass

        health_server.close()
        await health_server.wait_closed()

if __name__ == "__main__":
    # ВАЖЛИВО: саме main() (корутина), не main
    app.run(main())