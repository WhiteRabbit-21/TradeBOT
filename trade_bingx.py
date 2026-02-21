import os
import sys
import signal
import time
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv
from pyrogram import Client, filters, idle

load_dotenv()

# -------------------- ENV --------------------
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")

# Варіант 1: Session String (рекомендовано для Railway)
TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "").strip()

# Варіант 2: Bot token (якщо читаєш як бот; бот має бути в каналі/адміном)
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()

# Варіант 3: session file name (fallback)
SESSION_NAME = os.getenv("TG_SESSION", "tradebot_session").strip()

TARGET_CHANNEL_ID_RAW = os.getenv("TARGET_CHANNEL_ID", "").strip()  # -100... або @username
ENABLE_SAVED = os.getenv("ENABLE_SAVED", "false").lower() in ("1", "true", "yes")


def log(status: str, msg: str = ""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [{status}] {msg}", flush=True)


def die(msg: str, code: int = 1):
    print(msg, flush=True)
    raise SystemExit(code)


# -------------------- VALIDATION --------------------
if not TG_API_ID or not TG_API_HASH:
    die("❌ TG_API_ID / TG_API_HASH not set")

if not TARGET_CHANNEL_ID_RAW:
    die("❌ TARGET_CHANNEL_ID not set (example: -1001234567890 or @channelusername)")

# Приводимо TARGET_CHANNEL_ID до int якщо це число
TARGET_CHANNEL_ID = TARGET_CHANNEL_ID_RAW
if TARGET_CHANNEL_ID_RAW.lstrip("-").isdigit():
    TARGET_CHANNEL_ID = int(TARGET_CHANNEL_ID_RAW)


# -------------------- HEALTH SERVER (for Railway/Web expectations) --------------------
def start_health_server():
    """
    Railway інколи очікує, що процес слухає PORT (як web).
    Цей сервер НЕ заважає Pyrogram-боту, але прибирає "Stopping Container".
    """
    port = int(os.getenv("PORT", "8080"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/health"):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            return  # не спамимо в лог

    server = HTTPServer(("0.0.0.0", port), Handler)
    log("HTTP", f"Listening on 0.0.0.0:{port} (GET /health -> 200)")
    server.serve_forever()


# -------------------- SIGNALS --------------------
def _on_stop(signum, frame):
    # Не завжди встигає спрацювати (інколи Railway робить жорсткий stop),
    # але нехай буде.
    log("SIG", f"Got signal {signum}. Platform is stopping the container.")
    time.sleep(0.5)
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _on_stop)
signal.signal(signal.SIGINT, _on_stop)


# -------------------- PYROGRAM CLIENT --------------------
def build_client() -> Client:
    # 1) BOT
    if TG_BOT_TOKEN:
        log("ENV", "Using TG_BOT_TOKEN")
        return Client(
            name="tradebot_bot",
            api_id=TG_API_ID,
            api_hash=TG_API_HASH,
            bot_token=TG_BOT_TOKEN,
            in_memory=True,
        )

    # 2) SESSION STRING (recommended)
    if TG_SESSION_STRING:
        log("ENV", "Using TG_SESSION_STRING")
        return Client(
            name="tradebot_user",
            api_id=TG_API_ID,
            api_hash=TG_API_HASH,
            session_string=TG_SESSION_STRING,
            in_memory=True,
        )

    # 3) SESSION FILE (fallback)
    log("ENV", f"Using session file name: {SESSION_NAME}")
    return Client(
        name=SESSION_NAME,
        api_id=TG_API_ID,
        api_hash=TG_API_HASH,
    )


app = build_client()


# -------------------- HANDLERS --------------------
def extract_text(message) -> str:
    return (message.text or message.caption or "").strip()


@app.on_message(filters.chat(TARGET_CHANNEL_ID) & (filters.text | filters.caption))
def on_channel_message(client: Client, message):
    text = extract_text(message)
    if not text:
        return

    chat = message.chat
    log(
        "CHAN",
        f"from={chat.title or chat.username or chat.id} | msg_id={message.id} | text={text[:200]}"
    )

    # TODO: тут викликай свій парсер/трейдинг-логіку
    # handle_signal(text)


if ENABLE_SAVED:
    @app.on_message(filters.me & (filters.text | filters.caption))
    def on_saved_message(client: Client, message):
        text = extract_text(message)
        if not text:
            return
        log("SAVED", f"text={text[:200]}")
        # handle_signal(text)


# -------------------- RUN --------------------
if __name__ == "__main__":
    log("BOOT", "Bot starting...")
    log(
        "ENV",
        f"TG_API_ID={TG_API_ID} TG_API_HASH={'YES' if TG_API_HASH else 'NO'} "
        f"TG_SESSION_STRING={'YES' if TG_SESSION_STRING else 'NO'} TG_BOT_TOKEN={'YES' if TG_BOT_TOKEN else 'NO'}",
    )
    log("ENV", f"TARGET_CHANNEL_ID={TARGET_CHANNEL_ID} ENABLE_SAVED={ENABLE_SAVED}")

    # Підняти health-сервер у бекграунді
    threading.Thread(target=start_health_server, daemon=True).start()

    try:
        log("BOOT", "Starting Pyrogram...")
        app.start()
        log("BOOT", "Pyrogram started. Idling...")

        idle()  # тримає процес живим

    except Exception as e:
        log("FATAL", f"Runtime error: {e}")
        raise
    finally:
        try:
            log("BOOT", "Stopping Pyrogram...")
            app.stop()
            log("BOOT", "Pyrogram stopped.")
        except Exception as e:
            log("WARN", f"Error on stop(): {e}")