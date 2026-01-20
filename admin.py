import asyncio
import json
import os
from datetime import datetime

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiohttp import web

from lz_mysql import MySQLPool

from pg_stats_db import PGStatsDB
from group_stats_tracker import GroupStatsTracker

from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact

# ======== 载入配置 ========
from ly_config import (
    API_ID,
    API_HASH,
    SESSION_STRING,
    COMMAND_RECEIVERS,
    ALLOWED_PRIVATE_IDS,
    ALLOWED_GROUP_IDS,
    PG_DSN,
    PG_MIN_SIZE,
    PG_MAX_SIZE,
    STAT_FLUSH_INTERVAL,
    STAT_FLUSH_BATCH_SIZE,
    KEY_USER_ID,
    BOT_INIT
)

# ======== Telethon 启动方式 ========
client = TelegramClient(
    session=StringSession(SESSION_STRING),
    api_id=API_ID,
    api_hash=API_HASH
)

# ======== 设置群组发言统计（class classmethod 风格） ========
GroupStatsTracker.configure(
    client,
    flush_interval=STAT_FLUSH_INTERVAL,
    flush_batch_size=STAT_FLUSH_BATCH_SIZE
)


# async def notify_command_bot_on_start():
#     for bot_username in BOT_INIT:
#         target = await client.get_entity(bot_username)
#         me = await client.get_me()
#         await client.send_message(target, f"/start")
#     return

async def notify_command_bot_on_start():
    for username in BOT_INIT:
        if not username.startswith("@"):
            username = "@" + username
        try:
            await client.send_message(username, "/start")
        except Exception as e:
            print(f"给 {username} 发送 /start 失败: {e}")

async def notify_command_receivers_on_start():
    target = await client.get_entity(KEY_USER_ID)     # 7550420493
    me = await client.get_me()
    await client.send_message(target, f"你好, 我是 {me.id} - {me.first_name} {me.last_name or ''}")
    return   

async def add_contact():

    # 构造一个要导入的联系人
    contact = InputPhoneContact(
        client_id=0, 
        phone="+18023051359", 
        first_name="DrXP", 
        last_name=""
    )

    result = await client(ImportContactsRequest([contact]))
    print("导入结果:", result)
    target = await client.get_entity(KEY_USER_ID)     # 7550420493


    me = await client.get_me()
    await client.send_message(target, f"你好, 我是 {me.id} - {me.first_name} {me.last_name or ''}")

async def join(invite_hash):
    from telethon.tl.functions.messages import ImportChatInviteRequest
    try:
        await client(ImportChatInviteRequest(invite_hash))
        print("已成功加入群组",flush=True)
    except Exception as e:
        if 'InviteRequestSentError' in str(e):
            print("加入请求已发送，等待审批",flush=True)
        else:
            print(f"失败-加入群组: {invite_hash} {e}", flush=True)

# ==================================================================
# 指令 /hb fee n2
# ==================================================================
@client.on(events.NewMessage(pattern=r'^/(\w+)\s+(\d+)\s+(\d+)(?:\s+(.*))?$'))
async def handle_group_command(event):
    if event.is_private:
        print(f"不是群组消息，忽略。",flush=True)
        return

    cmd = event.pattern_match.group(1).lower()
    fee = abs(int(event.pattern_match.group(2)))
    cnt = int(event.pattern_match.group(3))
    extra_text = event.pattern_match.group(4)  # 可选，可为 None

    if cmd not in COMMAND_RECEIVERS:
        print(f"未知指令 /{cmd}，忽略。",flush=True)
        return


   

    receiver_id = COMMAND_RECEIVERS[cmd]
    sender_id = event.sender_id
    chat_id = event.chat_id
    msg_id = event.id


    # ====== 新增：群组白名单过滤 ======
    if chat_id not in ALLOWED_GROUP_IDS:
        print(f"{chat_id} 不在白名单 → 直接忽略，不处理、不回覆",flush=True)
        # 不在白名单 → 直接忽略，不处理、不回覆
        return
    # =================================

    if fee < 2:
        return
    elif fee < cnt:
        return
    elif fee >666:
        return
    elif cnt > 60:
        return

    transaction_data = {
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "transaction_type": cmd,
        "transaction_description": f"{chat_id}_{msg_id}",
        "sender_fee": -fee,
        "receiver_fee": fee,
    }

    await MySQLPool.ensure_pool()
    result = await MySQLPool.transaction_log(transaction_data)
    print("🔍 交易结果:", result)

    if result.get("ok") == "1":
        json = json.dumps({
            "ok": 1 ,
            "chatinfo": f"{chat_id}_{msg_id}"
        })
        print(f"json={json}",flush=True)
        await client.send_message(sender_id, json)
    #     await event.reply(
    #         f"✅ 交易成功\n指令: /{cmd}\n扣分: {fee}\n接收者: {receiver_id} chatinfo: {chat_id}_{msg_id}"
    #     )
    # else:
    #     await event.reply("⚠️ 交易失败")


# ==================================================================
# 私聊 JSON 处理
# ==================================================================
@client.on(events.NewMessage)
async def handle_private_json(event):
    if not event.is_private:
        return
    


    text = event.raw_text.strip()

    if text == "/hello":
        await event.reply("hi")
        return

    elif text == "/addcontact":
        await add_contact()
        return
    elif text.startswith("/tell"):
        parts = text.split(maxsplit=2)
        # parts: ["/say", "7550420493", "hi there"]
        if len(parts) < 3:
            # await event.reply("用法：/say <user_id 或 @username> <内容>")
            return

        _, uid, word = parts

        # uid 如果是纯数字，转 int 更稳
        if uid.isdigit():
            uid = int(uid)

        await client.send_message(uid, word)
        return
       
        
    elif text.startswith("/join"):
        # 这里 text 可能是：
        # /join
        # /join https://t.me/xxxx
        # /join@bot something
        # /join_xxx （若你只想匹配 '/join ' 带空格的，也可改 startswith("/join ")）

        # 若需要解析后面的参数，可 split
        parts = text.split(maxsplit=1)
        cmd = parts[0]            # "/join"
        link = parts[1] if len(parts) > 1 else None
        print(f"尝试加入群组，link={link}")
        if link:
            await join(link)
        return

    if event.sender_id not in ALLOWED_PRIVATE_IDS:
        print(f"用户 {event.sender_id} 不在允许名单，忽略。")
        return

    # 尝试解析 JSON
    try:
        data = json.loads(event.raw_text)
        if not isinstance(data, dict):
            return
    except Exception:
        print(f"📩 私人消息非 JSON，忽略。")
        return
    print(f"📩 收到私人 JSON 请求: {data}",flush=True)
    await MySQLPool.ensure_pool()
    # === 查交易 ===
    if "chatinfo" in data:    
        row = await MySQLPool.find_transaction_by_description(data["chatinfo"])
        await event.reply(json.dumps({
            "ok": 1 if row else 0,
            "chatinfo": data["chatinfo"]
        }))
        return

    # === payment ===
    elif "receiver_id" in data and "receiver_fee" in data:
        print(f"处理 payment 请求: {data}",flush=True)
        rid = int(data["receiver_id"])
        fee = int(data["receiver_fee"])
        memo = data.get("sender_id", "")
        keyword = data.get("keyword", "")

        result = await MySQLPool.transaction_log({
            "sender_id": event.sender_id,
            "receiver_id": rid,
            "transaction_type": "payment",
            "transaction_description": keyword,
            "sender_fee": -fee,
            "receiver_fee": fee,
            "memo": memo
        })
        
        await event.reply(json.dumps({
            "ok": 1 if result.get("ok") == "1" else 0,
            "status": result.get("status"),
            "transaction_id": (result.get("transaction_data", "")).get("transaction_id", ""),
            "receiver_id": rid,
            "receiver_fee": fee,
            "keyword": keyword,
            "memo": data.get("memo", "")
        }))
        return

    await event.reply(json.dumps({"ok": 0, "error": "unknown_json"}))


# ==================================================================
# 启动 bot
# ==================================================================
async def main():
   
    # ===== MySQL 初始化 =====
    await MySQLPool.init_pool()

    # ===== PostgreSQL 初始化 =====
    await PGStatsDB.init_pool(PG_DSN, PG_MIN_SIZE, PG_MAX_SIZE)
    await PGStatsDB.ensure_table()

    # ===== 启动后台统计器 =====
    # print("🤖 ly bot 启动中(SESSION_STRING)...")
    await GroupStatsTracker.start_background_tasks()



    # ====== 获取自身帐号资讯 ======
    await client.start()
    me = await client.get_me()
    user_id = me.id
    full_name = (me.first_name or "") + " " + (me.last_name or "")
    phone = me.phone

    print("======================================")
    print("🤖 Telethon 已上线")
    print(f"👤 User ID      : {user_id}")
    print(f"📛 Full Name    : {full_name.strip()}")
    print(f"📱 Phone Number : {phone}")
    print("======================================", flush=True)
    # =====================================


    # await notify_command_receivers_on_start()
    await notify_command_bot_on_start()

    print("📡 开始监听所有事件...")

    # Render 用 PORT
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    await web._run_app(app, host="0.0.0.0", port=port)

    await client.run_until_disconnected()

    # 优雅关闭
    await GroupStatsTracker.stop_background_tasks()
    await PGStatsDB.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
