import asyncio
import json
import os
from pathlib import Path

import socks
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from handlers.group_media_forwarder import forward_private_media
from handlers.class_harry import HarryClass

import textwrap

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".lz.env")
load_dotenv(BASE_DIR / ".harry.env", override=True)

try:
    CONFIGURATION = json.loads(os.getenv("CONFIGURATION", "") or "{}")
except Exception:
    CONFIGURATION = {}

API_ID = int(CONFIGURATION.get("api_id", os.getenv("API_ID", 0)) or 0)
API_HASH = CONFIGURATION.get("api_hash", os.getenv("API_HASH", ""))
SESSION_STRING = CONFIGURATION.get("session_string", os.getenv("SESSION_STRING", ""))
USER_SESSION = os.getenv("USER_SESSION", f"{API_ID}harry")
FORWARD_DELAY_SECONDS = float(os.getenv("HARRY_FORWARD_DELAY_SECONDS", "1"))
FORWARD_TARGETS_RAW = os.getenv("HARRY_FORWARD_TARGETS", "")
ADMIN_IDS_RAW = os.getenv("HARRY_ADMIN_IDS", os.getenv("ADMIN_IDS", ""))
PROXY_TYPE = os.getenv("HARRY_PROXY_TYPE", "").strip().lower()
PROXY_HOST = os.getenv("HARRY_PROXY_HOST", "127.0.0.1").strip()
PROXY_PORT = int(os.getenv("HARRY_PROXY_PORT", "0") or 0)
PROXY_USERNAME = os.getenv("HARRY_PROXY_USERNAME", "").strip() or None
PROXY_PASSWORD = os.getenv("HARRY_PROXY_PASSWORD", "").strip() or None
GROUP_BOTS_RAW = os.getenv("HARRY_GROUP_BOTS", os.getenv("BOT_INIT", ""))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_RIGHTS_CHAT_RAW = os.getenv("HARRY_BOT_RIGHTS_CHAT", "").strip()




def parse_csv_values(raw: str) -> list[int | str]:
    values: list[int | str] = []
    for part in raw.replace(";", ",").split(","):
        item = part.strip()
        if not item:
            continue
        if item.lstrip("-").isdigit():
            values.append(int(item))
        else:
            values.append(item)
    return values


def parse_int_set(raw: str) -> set[int]:
    ids: set[int] = set()
    for value in parse_csv_values(raw):
        if isinstance(value, int):
            ids.add(value)
    return ids


FORWARD_TARGETS = parse_csv_values(FORWARD_TARGETS_RAW)
ADMIN_IDS = parse_int_set(ADMIN_IDS_RAW)
GROUP_BOTS = parse_csv_values(GROUP_BOTS_RAW)
BOT_RIGHTS_CHAT = parse_csv_values(BOT_RIGHTS_CHAT_RAW)


def build_proxy():
    if not PROXY_TYPE or PROXY_TYPE in {"none", "off", "false", "0"}:
        return None
    if not PROXY_HOST or not PROXY_PORT:
        raise RuntimeError("Set HARRY_PROXY_HOST and HARRY_PROXY_PORT when proxy is enabled")

    proxy_types = {
        "socks5": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
    }
    if PROXY_TYPE not in proxy_types:
        raise RuntimeError("HARRY_PROXY_TYPE only supports socks5, socks4, http, or none")

    return (
        proxy_types[PROXY_TYPE],
        PROXY_HOST,
        PROXY_PORT,
        True,
        PROXY_USERNAME,
        PROXY_PASSWORD,
    )


def build_user_client() -> TelegramClient:
    proxy = build_proxy()
    # proxy = None
    if SESSION_STRING:
        # 人类账号 session
        user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH, proxy=proxy)

        # Telegram Bot session
        bot_client = TelegramClient("bot_session", API_ID, API_HASH, proxy=proxy)

        return user_client,bot_client
    


    user_client = TelegramClient(USER_SESSION or "harry", API_ID, API_HASH, proxy=proxy)
    bot_client = TelegramClient("bot_session", API_ID, API_HASH, proxy=proxy)
    return user_client,bot_client


client,bot_client = build_user_client()
harry = HarryClass(client, bot_client, GROUP_BOTS)


@client.on(events.NewMessage(incoming=True))
async def handle_private_message(event: events.NewMessage.Event) -> None:
    message = event.message
    # if not message.is_private:
    #     return

    print()
    sender_id = event.sender_id
    text = (message.raw_text or "").strip()
    inst_row = text.split(" ")
    command = inst_row[0].strip().lower() if inst_row else ""
    params = inst_row[1:] if len(inst_row) > 1 else []
    board_info = {
        "sender_id": sender_id,
        "chat_id": event.chat_id,
        "message_id": message.id,
        "message_thread_id": message.reply_to or 0,
    }
   

    print(f"[Bot] /me for user_id={sender_id}, command={command}, params={params}")
    

    list_text = textwrap.dedent("""
        <blockquote>增加</blockquote>
        <code>!addchannel</code>
        <code>!addgroup</code>
        <code>!addforum</code>
        <blockquote>设置群组/频道</blockquote>
        <code>!setchat school</code> 学院群
        <code>!setchat public</code> 公开群
        <code>!setchat oldfriend</code> 老铁群    
        <code>!setchat jwc</code> 教务处 
        <code>!setchat tr_rw</code> 资源审核群
    """).strip()


    if sender_id in ADMIN_IDS:
        if command == "!admin":
            await event.reply(list_text,parse_mode="html")
            return
        elif command == "!add":
            if params and params[0] == "channel":
                await event.reply(await harry.create_group(broadcast=True))
            elif params and params[0] == "group":
                await event.reply(await harry.create_group(megagroup=True))
            elif params and params[0] == "forum":
                await event.reply(await harry.create_group(forum=True))
            
        elif command == "!setchat":
            if params:
                reply_text = await harry.set_chat(params,board_info)
                if reply_text:
                    await event.reply(reply_text, parse_mode="html")
            else:
                pass
            
        
        return
    else:
        print(f"[Bot] user_id={sender_id} is not in admin list; ignored", flush=True)

    if not message.media:
        return

    if not FORWARD_TARGETS:
        print("[harry] private media received but HARRY_FORWARD_TARGETS is not configured; skipped", flush=True)
        return

    await forward_private_media(message, FORWARD_TARGETS, delay_seconds=FORWARD_DELAY_SECONDS)


async def main() -> None:
    if not API_ID or not API_HASH:
        raise RuntimeError("Set API_ID and API_HASH first")

    await client.start()
    me = await client.get_me()
    await bot_client.start(bot_token=os.getenv("BOT_TOKEN", ""))
    bot_me = await bot_client.get_me()
    print(f"[harry] Telethon userbot online: id={me.id} username={me.username}", flush=True)
    print(f"[harry] Telethon bot online: id={bot_me.id} username={bot_me.username}", flush=True)
    print(f"[harry] media forward targets: {FORWARD_TARGETS}", flush=True)
    print(f"[harry] /admin whitelist: {sorted(ADMIN_IDS)}", flush=True)
    await client.run_until_disconnected()



if __name__ == "__main__":
    asyncio.run(main())
