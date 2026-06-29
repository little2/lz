import asyncio
import json
import os
from pathlib import Path

import socks
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import CreateChannelRequest, EditAdminRequest, InviteToChannelRequest
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


async def create_group() -> str:
    result = await client(
        CreateChannelRequest(
            title="GROUP",
            about="Group for Harry",
            megagroup=True,
        )
    )
    group = result.chats[0]
    group_id = int(f"-100{group.id}")
    bot_results: list[str] = []

    for bot in GROUP_BOTS:
        try:
            entity = await resolve_group_bot(bot)
            await client(InviteToChannelRequest(group, [entity]))
            await client(
                EditAdminRequest(
                    group,
                    entity,
                    ChatAdminRights(
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
                        post_stories=True,             # Manage stories 1/3
                        edit_stories=True,             # Manage stories 2/3
                        delete_stories=True,           # Manage stories 3/3
                        other=True,                    # Other admin privileges
                    ),
                    rank="bot",
                )
            )
            bot_results.append(f"{bot}: added as admin")
        except Exception as exc:
            bot_results.append(f"{bot}: failed: {exc}")

    try:
        invite = await client(ExportChatInviteRequest(group))
        invite_link = getattr(invite, "link", "")
    except Exception as exc:
        invite_link = f"invite link failed: {exc}"

    bots_text = "\n".join(bot_results) if bot_results else "no bots configured"
    return f"group created: {group.title}\nid: {group_id}\ninvite: {invite_link}\nbots:\n{bots_text}"


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
        elif text == "/addgroup":
            await event.reply(await create_group())
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
