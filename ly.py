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
    STAT_FLUSH_BATCH_SIZE
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


async def notify_command_receivers_on_start():
    for uid in ALLOWED_PRIVATE_IDS:
        try:
            await client.send_message(uid, "/start")
            await asyncio.sleep(0.5)
            await client.send_message(uid, "hi")
            print(f"📨 已向 {uid} 发送 /start", flush=True)
        except Exception as e:
            print(f"⚠️ 发送 /start 给 {uid} 失败: {e}", flush=True)


# ==================================================================
# 指令 /hb fee n2
# ==================================================================
@client.on(events.NewMessage(pattern=r'^/(\w+)\s+(\d+)\s+(\d+)(?:\s+(.*))?$'))
async def handle_group_command(event):
    if event.is_private:
        return

    cmd = event.pattern_match.group(1).lower()
    fee = abs(int(event.pattern_match.group(2)))
    n2 = int(event.pattern_match.group(3))
    extra_text = event.pattern_match.group(4)  # 可选，可为 None

    if cmd not in COMMAND_RECEIVERS:
        return

    receiver_id = COMMAND_RECEIVERS[cmd]
    sender_id = event.sender_id
    chat_id = event.chat_id
    msg_id = event.id


    # ====== 新增：群组白名单过滤 ======
    if chat_id not in ALLOWED_GROUP_IDS:
        # 不在白名单 → 直接忽略，不处理、不回覆
        return
    # =================================

    transaction_data = {
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "transaction_type": cmd,
        "transaction_description": f"{chat_id}_{msg_id}",
        "sender_fee": -fee,
        "receiver_fee": fee,
    }

    MySQLPool.ensure_pool()
    result = await MySQLPool.transaction_log(transaction_data)
    print("🔍 交易结果:", result)

    if result.get("ok") == "1":
        await event.reply(
            f"✅ 交易成功\n指令: /{cmd}\n扣分: {fee}\n接收者: {receiver_id}"
        )
    else:
        await event.reply("⚠️ 交易失败")


# ==================================================================
# 私聊 JSON 处理
# ==================================================================
@client.on(events.NewMessage)
async def handle_private_json(event):
    if not event.is_private:
        return

    if event.raw_text.strip() == "/hello":
        await event.reply("hi")
        return

    if event.sender_id not in ALLOWED_PRIVATE_IDS:
        return

    # 尝试解析 JSON
    try:
        data = json.loads(event.raw_text)
        if not isinstance(data, dict):
            return
    except Exception:
        return
    
    MySQLPool.ensure_pool()
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
            "transaction_id": result.get("transaction_id"),
            "receiver_id": rid,
            "receiver_fee": fee,
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
    await GroupStatsTracker.start_background_tasks()

    print("🤖 ly bot 启动中(SESSION_STRING)...")

    await client.start()

    me = await client.get_me()
    print("已登入:", me.id, me.first_name)

    await notify_command_receivers_on_start()

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
