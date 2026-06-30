import asyncio
from datetime import datetime
import json
import os
from pathlib import Path

import socks
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import CreateChannelRequest, EditAdminRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.types import ChatAdminRights, InputUser

from handlers.group_media_forwarder import forward_private_media

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


def anonymous_admin_rights() -> ChatAdminRights:
    return ChatAdminRights(
        change_info=True,
        invite_users=True,
        ban_users=True,
        add_admins=True,
        post_messages=True,
        edit_messages=True,
        delete_messages=True,
        pin_messages=True,
        manage_call=True,
        anonymous=True,
        manage_topics=True,
        post_stories=True,
        edit_stories=True,
        delete_stories=True,
        other=True,
        manage_direct_messages=True,   
    )


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
    if SESSION_STRING:
        return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH, proxy=proxy)
    return TelegramClient(USER_SESSION or "harry", API_ID, API_HASH, proxy=proxy)


client = build_user_client()


def normalize_username(value: str) -> str:
    item = value.strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if item.startswith(prefix):
            item = item[len(prefix):]
            break
    return item.split("?", 1)[0].strip("/")


async def resolve_group_bot(bot: int | str):
    if isinstance(bot, int):
        raise ValueError("cannot invite by bare user_id; use @username or user_id:access_hash")

    item = normalize_username(bot)
    if ":" in item:
        user_id, access_hash = item.split(":", 1)
        if user_id.strip().lstrip("-").isdigit() and access_hash.strip().lstrip("-").isdigit():
            return InputUser(int(user_id), int(access_hash))

    if item.lstrip("-").isdigit():
        raise ValueError("cannot invite by bare user_id; use @username or user_id:access_hash")

    return await client.get_entity(item)


async def create_group(megagroup=False, forum=False, broadcast=False) -> str:

    if megagroup:
        group_title = f"GROUP (megagroup)" 
    elif forum:
        group_title = f"Forum (forum)"
    elif broadcast:
        group_title = f"Channel (broadcast)"
    else:
        group_title = f"GROUP"

    try:
        result = await client(
            CreateChannelRequest(
                title=group_title,
                about="for Harry",
                megagroup=megagroup,
                forum=forum,
                broadcast=broadcast
            )
        )
    except FloodWaitError as exc:
        wait_seconds = int(exc.seconds)
        wait_minutes = wait_seconds // 60
        return f"create failed: Telegram flood wait, retry after {wait_seconds} seconds ({wait_minutes} minutes)"

    group = result.chats[0]
    group_id = int(f"-100{group.id}")
    self_admin_result = "self: skipped"
    bot_results: list[str] = []
    book_result = "/book: skipped"
    time_result = "time message: skipped"

    try:
        me = await client.get_me()
        await client(
            EditAdminRequest(
                group,
                utils.get_input_user(me),
                anonymous_admin_rights(),
                rank="owner",
            )
        )
        self_admin_result = f"self: anonymous admin enabled ({me.id})"
    except Exception as exc:
        self_admin_result = f"self: failed: {exc}"

    for bot in GROUP_BOTS:
        try:
            entity = await resolve_group_bot(bot)
            await client(
                EditAdminRequest(
                    group,
                    entity,
                    anonymous_admin_rights(),
                    rank="bot",
                )
            )
            bot_results.append(f"{bot}: invited as admin")
        except Exception as exc:
            bot_results.append(f"{bot}: failed: {exc}")

    try:
        await client.send_message(group, "/book")
        book_result = "/book: sent"
    except Exception as exc:
        book_result = f"/book: failed: {exc}"

    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await client.send_message(group, f"current time: {current_time}")
        time_result = f"time message: sent ({current_time})"
    except Exception as exc:
        time_result = f"time message: failed: {exc}"

    try:
        invite = await client(ExportChatInviteRequest(group))
        invite_link = getattr(invite, "link", "")
    except Exception as exc:
        invite_link = f"invite link failed: {exc}"

    bots_text = "\n".join(bot_results) if bot_results else "no bots configured"
    return f"group created: {group.title}\nid: {group_id}\ninvite: {invite_link}\nadmin:\n{self_admin_result}\nbots:\n{bots_text}\ncommand:\n{book_result}\n{time_result}"


@client.on(events.NewMessage(incoming=True))
async def handle_private_message(event: events.NewMessage.Event) -> None:
    message = event.message
    if not message.is_private:
        return

    sender_id = event.sender_id
    text = (message.raw_text or "").strip()

    if sender_id in ADMIN_IDS:
        if text == "/admin":
            await event.reply("hello")
            return
        elif text == "/addchannel":
            await event.reply(await create_group(broadcast=True))
            return
        elif text == "/addgroup":
            await event.reply(await create_group(megagroup=True))
            return
        elif text == "/addforum":
            await event.reply(await create_group(forum=True))
            return

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
    print(f"[harry] Telethon userbot online: id={me.id} username={me.username}", flush=True)
    print(f"[harry] media forward targets: {FORWARD_TARGETS}", flush=True)
    print(f"[harry] /admin whitelist: {sorted(ADMIN_IDS)}", flush=True)

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
