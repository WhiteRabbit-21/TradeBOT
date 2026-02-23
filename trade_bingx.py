import os
import asyncio
import threading
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
    async def handle(reader, writer):
        try:
            data = await reader.read(1024)
            if b"GET /health" in data:
                body = b"ok"
                resp = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: 2\r\n"
                    b"\r\n" + body
                )
            else:
                resp = b"HTTP/1.1 404 Not Found\r\n\r\n"

            writer.write(resp)
            await writer.drain()
        finally:
            writer.close()

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
    print(f"[{ts}] {text}")

async def heartbeat():
    while True:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] BOT IS ALIVE")
        await asyncio.sleep(HEARTBEAT_SEC)

async def main():
    asyncio.create_task(heartbeat())
    print("✅ BOT STARTED")
    await asyncio.Event().wait()  # тримає процес живим

if __name__ == "__main__":
    app.run(main())