import asyncio
import json
import os
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiohttp import web
from lz_mysql import MySQLPool

# ======== 载入环境参数 ========
from ly_config import (
    API_ID,
    API_HASH,
    SESSION_STRING,
)

"""
本版本只使用 SESSION_STRING 登录。
不再使用 USER_SESSION、本地文件 session、PHONE_NUMBER 等。
"""

# ======== Telethon 启动方式 ========
client = TelegramClient(
    session=StringSession(SESSION_STRING),
    api_id=API_ID,
    api_hash=API_HASH
)

# ======== 业务参数 ========





# ==================================================================
# 1) 群组指令: /hb [fee]  或  /play [fee]
# ==================================================================
@client.on(events.NewMessage(pattern=r'^/(\w+)\s+(\d+)$'))
async def handle_group_command(event: events.NewMessage.Event):

    if event.is_private:
        return

    cmd = event.pattern_match.group(1).lower()
    fee = abs(int(event.pattern_match.group(2)))

    # 沒有对应指令就忽略
    if cmd not in COMMAND_RECEIVERS:
        return

    receiver_id = COMMAND_RECEIVERS[cmd]

    sender_id = event.sender_id
    chat_id = event.chat_id
    message_id = event.id

    # sender_fee 一律扣分 → 使用负值
    transaction_data = {
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "transaction_type": cmd,
        "transaction_description": f"{chat_id}_{message_id}",
        "sender_fee": -fee,    # 扣款
        "receiver_fee": fee,   # 加款
    }

    result = await MySQLPool.transaction_log(transaction_data)
    # status = result.get("status")
    print(f"🔍 交易结果: {result}")
   

    if result.get("ok") == "1":
        await event.reply(
            f"✅ 交易成功\n"
            f"指令: /{cmd}\n"
            f"扣分: {fee}\n"
            f"接收者: {receiver_id}\n"
            f"记录: {chat_id}_{message_id}\n"
           
        )
    else:
        await event.reply(f"⚠️ 交易失败")


# ==================================================================
# 2 & 3) 私聊 JSON：检查交易 or 创建 payment
# ==================================================================
@client.on(events.NewMessage)
async def handle_private_json(event: events.NewMessage.Event):

    if not event.is_private:
        return

    text = event.raw_text.strip()

    # ====== 新增：私信 /hello ======
    if text == "/hello":
        await event.reply("hi")
        return
    # =================================


    # ❗只有列在 COMMAND_RECEIVERS 的 user_id 才能私信控制
    if event.sender_id not in ALLOWED_PRIVATE_IDS:
        return

    # 尝试解析 JSON
    try:
        data = json.loads(event.raw_text.strip())
        if not isinstance(data, dict):
            return
    except Exception:
        return

    # --- 需求 2: 查交易 ---
    if "chatinfo" in data:
        chatinfo = data["chatinfo"]
        row = await MySQLPool.find_transaction_by_description(chatinfo)

        await event.reply(json.dumps({
            "ok": 1 if row else 0,
            "chatinfo": chatinfo
        }, ensure_ascii=False))
        return

    # --- 需求 3: payment ---
    if "receiver_id" in data and "receiver_fee" in data:
        receiver_id = int(data["receiver_id"])
        receiver_fee = int(data["receiver_fee"])
        memo = data.get("memo", "")
        # 令 transaction_description 为当成时间
        from datetime import datetime
        times = datetime.now().strftime("%Y%m%d%H%M%S")  # 作为
        

        result = await MySQLPool.transaction_log({
            "sender_id": event.sender_id,
            "receiver_id": receiver_id,
            "transaction_type": "payment",
            "transaction_description": times,
            "sender_fee": -receiver_fee,
            "receiver_fee": receiver_fee,
            "memo": memo
        })

        await event.reply(json.dumps({
            "ok": 1 if result.get("ok") == "1" else None,
            "status": result.get("status"),
            "receiver_id": receiver_id,
            "receiver_fee": receiver_fee,
            "memo": memo
        }, ensure_ascii=False))
        return

    # --- 需求 4: payment ---
    if "receiver_id" in data and "fee" in data and "sender_id" in data and "keyword" in data:
        receiver_id = int(data["receiver_id"])
        sender_id = int(data["sender_id"])
        fee = abs(int(data["fee"]))
        sender_fee = fee * (-1)
        receiver_fee = int(fee*0.6)
        keyword = data.get("keyword", "")
        # 令 transaction_description 为当成时间
      
        result = await MySQLPool.transaction_log({
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "transaction_type": "proxy",
            "transaction_description": keyword,
            "sender_fee": sender_fee,
            "receiver_fee": receiver_fee,
            "memo": event.sender_id
        })
         
        await event.reply(json.dumps({
            "ok": 1 if result.get("ok") == "1" else None,
            "status": result.get("status"),
            "receiver_id": receiver_id,
            "receiver_fee": receiver_fee,
            "memo": keyword,
            "transaction_id": (result.get("transaction_data")).get("transaction_id")
        }, ensure_ascii=False))
        return

    # 其他格式
    await event.reply(json.dumps({
        "ok": 0,
        "error": "unknown_json_format"
    }, ensure_ascii=False))




# ==================================================================
# 启动 bot
# ==================================================================
async def main():
    await MySQLPool.init_pool()
    print("🤖 ly (human-bot) 只使用 Session String 启动中...", flush=True)

    await client.start()

    # ====== 获取自身帐号资讯 ======
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

    print("📡 开始监听群组指令与私聊 JSON ...")

    # ✅ Render 环境用 PORT，否则本地用 8080
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    await web._run_app(app, host="0.0.0.0", port=port)
    await client.run_until_disconnected()



if __name__ == "__main__":
    asyncio.run(main())
