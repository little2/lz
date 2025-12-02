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
    KEY_USER_ID
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
# 交易回写
# ==================================================================

async def replay_offline_transactions(max_batch: int = 200):
    """
    MySQL 恢复后，把 PG 里的 offline_transaction_queue 回放到 MySQL，
    并把 PostgreSQL 的 user.point 强制对齐为 MySQL 的最新值。

    max_batch: 每次最多处理多少笔离线交易，避免一次拉太多。
    """
    # PG / MySQL 必须已初始化
    if PGStatsDB.pool is None:
        print("⚠️ PGStatsDB 未初始化，略过离线交易回放。", flush=True)
        return

    # 如果 MySQL 还是连不上，这里会直接抛错，下一轮再试
    await MySQLPool.ensure_pool()

    # 先同步用户资料，确保 user.point 是最新的
    await PGStatsDB.sync_user_from_mysql()


    # 先从 PG 拉出一批 pending 的离线交易
    async with PGStatsDB.pool.acquire() as conn_pg:
        rows = await conn_pg.fetch(
            """
            SELECT
                id,
                sender_id,
                receiver_id,
                transaction_type,
                transaction_description,
                sender_fee,
                receiver_fee
            FROM offline_transaction_queue
            WHERE status = 'pending'
            ORDER BY id ASC
            LIMIT $1
            """,
            max_batch,
        )

    if not rows:
        print("✅ 当前没有待回放的离线交易。", flush=True)
        return

    print(f"🧾 本次准备回放离线交易 {len(rows)} 笔...", flush=True)

    for r in rows:
        offline_id = r["id"]
        tx = {
            "sender_id": int(r["sender_id"]) if r["sender_id"] is not None else None,
            "receiver_id": int(r["receiver_id"]) if r["receiver_id"] is not None else None,
            "transaction_type": r["transaction_type"],
            "transaction_description": r["transaction_description"],
            "sender_fee": int(r["sender_fee"]),
            "receiver_fee": int(r["receiver_fee"]),
        }

        # 1) 写回 MySQL 真正扣款 / 加款
        try:
            result = await MySQLPool.transaction_log(tx)
        except Exception as e:
            print(f"❌ 回放离线交易 #{offline_id} 写入 MySQL 失败: {e}", flush=True)
            # 不动这笔的 status，保留为 pending，等下一轮再试
            break

        if result.get("ok") != "1":
            # 写入失败的话，把这笔标记为 failed，避免无限重试
            err = f"mysql_status={result.get('status', '')}"
            async with PGStatsDB.pool.acquire() as conn_pg:
                await conn_pg.execute(
                    """
                    UPDATE offline_transaction_queue
                    SET status = 'failed',
                        last_error = $2,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                    """,
                    offline_id,
                    err,
                )
            print(f"⚠️ 离线交易 #{offline_id} 写入 MySQL 失败，已标记为 failed: {err}", flush=True)
            continue

        # 2) 从 MySQL 读出 sender / receiver 的最新 point
        sender_point = receiver_point = None
        conn_mysql = cur_mysql = None
        try:
            conn_mysql, cur_mysql = await MySQLPool.get_conn_cursor()
            if tx["sender_id"]:
                await cur_mysql.execute(
                    "SELECT point FROM user WHERE user_id = %s LIMIT 1",
                    (tx["sender_id"],),
                )
                row = await cur_mysql.fetchone()
                sender_point = row["point"] if row else None

            if tx["receiver_id"]:
                await cur_mysql.execute(
                    "SELECT point FROM user WHERE user_id = %s LIMIT 1",
                    (tx["receiver_id"],),
                )
                row = await cur_mysql.fetchone()
                receiver_point = row["point"] if row else None
        except Exception as e:
            print(f"⚠️ 查询 MySQL 用户 point 失败 (offline_id={offline_id}): {e}", flush=True)
        finally:
            if conn_mysql and cur_mysql:
                await MySQLPool.release(conn_mysql, cur_mysql)

        # 3) 把最新 point 写回 PG 的 "user" 表，并把这笔离线交易标记为 synced
        async with PGStatsDB.pool.acquire() as conn_pg:
            async with conn_pg.transaction():
                if sender_point is not None and tx["sender_id"]:
                    await conn_pg.execute(
                        'UPDATE "user" SET point = $1 WHERE user_id = $2',
                        int(sender_point),
                        int(tx["sender_id"]),
                    )
                if receiver_point is not None and tx["receiver_id"]:
                    await conn_pg.execute(
                        'UPDATE "user" SET point = $1 WHERE user_id = $2',
                        int(receiver_point),
                        int(tx["receiver_id"]),
                    )

                await conn_pg.execute(
                    """
                    UPDATE offline_transaction_queue
                    SET status = 'synced',
                        processed_at = CURRENT_TIMESTAMP,
                        last_error = NULL
                    WHERE id = $1
                    """,
                    offline_id,
                )

        print(f"✅ 离线交易 #{offline_id} 回放完成并同步 PG.user.point", flush=True)

    print("🟢 本轮离线交易回放结束。", flush=True)

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


    backend = "mysql"
    try:
        await MySQLPool.ensure_pool()
        result = await MySQLPool.transaction_log(transaction_data)
    except Exception as e:
        print(f"❌ MySQLPool.ensure_pool/transaction_log 出错，改用 PostgreSQL 离线队列: {e}", flush=True)
        backend = "postgres_offline"
        # 这里使用 PGStatsDB
        result = await PGStatsDB.record_offline_transaction(transaction_data)

    print(f"🔍 交易结果 backend={backend} result={result}", flush=True)



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
    await PGStatsDB.ensure_offline_tx_table()

    # # ===== 启动后台统计器 =====
    # await GroupStatsTracker.start_background_tasks()

    # 启动群组统计 + 定期离线交易回放
    await GroupStatsTracker.start_background_tasks(
        offline_replay_coro=replay_offline_transactions,
        offline_interval=60   # 每 60 秒跑一次，你可以改成 300 等
    )


    print("🤖 ly bot 启动中(SESSION_STRING)...")

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
