# copyright 2023 © Xron Trix | https://github.com/Xrontrix10

import logging, json, asyncio, os
from uvloop import install
from pyrogram.client import Client

# Headless/container defaults to avoid SDL/ALSA probing errors from media libs.
os.environ.setdefault("XDG_RUNTIME_DIR", "/tmp")
os.environ.setdefault("SDL_AUDIODRIVER", "dummy")
os.environ.setdefault("SDL_VIDEODRIVER", "dummy")
os.environ.setdefault("PYGAME_HIDE_SUPPORT_PROMPT", "1")

# Read the dictionary from the txt file
with open("/content/Telegram-Leecher/credentials.json", "r") as file:
    credentials = json.loads(file.read())

API_ID = credentials["API_ID"]
API_HASH = credentials["API_HASH"]
BOT_TOKEN = credentials["BOT_TOKEN"]
OWNER = credentials["USER_ID"]
DUMP_ID = credentials["DUMP_ID"]


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


disable_log = _is_truthy(os.getenv("DISABLE_LOG", ""))
debug_mode = _is_truthy(os.getenv("DEBUG_MODE", "")) and not disable_log

if disable_log:
    logging.disable(logging.CRITICAL)
else:
    logging.basicConfig(level=logging.DEBUG if debug_mode else logging.INFO)

install()

# Python 3.12 may not auto-create an event loop for the main thread.
# Pyrogram's Dispatcher expects one during Client initialization.
try:
    asyncio.get_running_loop()
except RuntimeError:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

colab_bot = Client("my_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
