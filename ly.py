import asyncio
import json
import os
from datetime import datetime

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiohttp import web
import aiohttp


from lz_mysql import MySQLPool

from pg_stats_db import PGStatsDB
from group_stats_tracker import GroupStatsTracker

from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact
from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, PeerIdInvalidError




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
    target = await client.get_entity(int(KEY_USER_ID))     
    me = await client.get_me()
    await client.send_message(target, f"你好, 我是 {me.id} - {me.first_name} {me.last_name or ''}")
    return
   
async def add_contact():

    # 构造一个要导入的联系人
    contact = InputPhoneContact(
        client_id=0, 
        phone="+14699234886", 
        first_name="Man", 
        last_name=""
    )

    # contact = InputPhoneContact(
    #     client_id=0, 
    #     phone="+12702701761", 
    #     first_name="哪吒", 
    #     last_name=""
    # )
    # //7501358629 +1 270 270 1761+1 270 270 1761

    result = await client(ImportContactsRequest([contact]))
    # print("导入结果:", result)
    # print(f"{KEY_USER_ID}")
    target = await client.get_entity(int(KEY_USER_ID))     # 7550420493


    me = await client.get_me()
    await client.send_message(target, f"你好, 我是 {me.id} 请加我好友 - {me.first_name} {me.last_name or ''}")

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

    # ⚠️ 注意：这里不要先调用 sync_user_from_mysql()
    # 如果先同步，会把「尚未回放到 MySQL 的离线扣点」给覆盖掉。
    # await PGStatsDB.sync_user_from_mysql()

    # 先从 PG 拉出一批「尚未处理」的离线交易
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
            WHERE processed = FALSE        -- ✅ 用 processed 作为 pending 依据
            ORDER BY id ASC
            LIMIT $1
            """,
            max_batch,
        )

    if not rows:
        # print("✅ 当前没有待回放的离线交易。", flush=True)
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
            # 不动这笔的 processed，让它维持 FALSE，等下一轮再试
            break

        if result.get("ok") != "1":
            # 写入失败的话，把这笔标记为「已处理但失败」，避免无限重试
            err = f"mysql_status={result.get('status', '')}"
            async with PGStatsDB.pool.acquire() as conn_pg:
                await conn_pg.execute(
                    """
                    UPDATE offline_transaction_queue
                    SET processed   = TRUE,
                        last_error  = $2,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                    """,
                    offline_id,
                    err,
                )
            print(f"⚠️ 离线交易 #{offline_id} 写入 MySQL 失败，已标记为失败: {err}", flush=True)
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

        # 3) 把最新 point 写回 PG 的 "user" 表，并把这笔离线交易标记为 processed=TRUE
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
                    SET processed   = TRUE,
                        processed_at = CURRENT_TIMESTAMP,
                        last_error   = NULL
                    WHERE id = $1
                    """,
                    offline_id,
                )

        print(f"✅ 离线交易 #{offline_id} 回放完成并同步 PG.user.point", flush=True)

    print("🟢 本轮离线交易回放结束。", flush=True)


# @client.on(events.NewMessage)
# async def debug_group_id(event):
#     if event.is_private:
#         return
#     msg = event.message
#     print(
#         f"[DBG] date={msg.date}, "
#         f"out={msg.out}, {event.chat_id}"
#         f"sender={event.sender_id}, "
#         f"text={event.raw_text!r}",
#         flush=True
#     )
   


# ==================================================================
# 指令 /hb fee n2
# ==================================================================
@client.on(events.NewMessage(pattern=r'^/(\w+)\s+(\d+)\s+(\d+)(?:\s+(.*))?$'))
async def handle_group_command(event):
    print(f"[DEBUG2] 收到群消息 chat_id={event.chat_id}, text={event.raw_text!r}", flush=True)
    if event.is_private:
        print(f"不是群组消息，忽略。",flush=True)
        return

    chat_id = event.chat_id
    # ====== 新增：群组白名单过滤 ======
    if chat_id not in ALLOWED_GROUP_IDS:
        print(f"{chat_id} 不在白名单 → 直接忽略，不处理、不回覆",flush=True)
        # 不在白名单 → 直接忽略，不处理、不回覆
        return
    # =================================

    cmd = event.pattern_match.group(1).lower()
    fee = abs(int(event.pattern_match.group(2)))
    cnt = int(event.pattern_match.group(3))
    extra_text = event.pattern_match.group(4)  # 可选，可为 None

    if cmd not in COMMAND_RECEIVERS:
        print(f"未知指令 /{cmd}，忽略。",flush=True)
        return
    
    print(f"收到指令 /{cmd} fee={fee} cnt={cnt} extra_text={extra_text}",flush=True)


   

    receiver_id = COMMAND_RECEIVERS[cmd]
    sender_id = event.sender_id
    
    msg_id = event.id




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
        payload = json.dumps({
            "ok": 1 ,
            "chatinfo": f"{chat_id}_{msg_id}"
        })
        entity = await client.get_entity(receiver_id)
        result = await client.send_message(entity, payload)

      
        print(f"发送结果: {result}",flush=True)
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
            await event.reply("用法：/tell <user_id 或 @username> <内容>")
            return

        # 权限控制：避免被陌生人拿来当「转发器」
        if event.sender_id not in ALLOWED_PRIVATE_IDS:
            await event.reply("⚠️ 你没有权限使用 /tell 指令。")
            return

        _, target_raw, word = parts

        # 尝试把纯数字当成 user_id
        target = target_raw
        if target_raw.isdigit():
            target = int(target_raw)

        # 先解析 entity，统一处理各种错误
        try:
            entity = await client.get_input_entity(target)
        except (UsernameNotOccupiedError, UsernameInvalidError, PeerIdInvalidError, ValueError):
            await event.reply(f"❌ 找不到目标用户：{target_raw}")
            return
        except Exception as e:
            await event.reply(f"❌ 无法解析目标：{e}")
            return

        try:
            await client.send_message(entity, word)
            await event.reply("✅ 已转发。")
        except Exception as e:
            # 这里可能会是 USER_PRIVACY_RESTRICTED, FLOOD_WAIT 等
            await event.reply(f"❌ 发送失败：{e}")
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
    
    await MySQLPool.ensure_pool()
    # === 查交易 ===
    if "chatinfo" in data:    
        try:
            print(f"📩 收到私人 JSON 请求: {data}",flush=True)
            row = await MySQLPool.find_transaction_by_description(data["chatinfo"])
        except Exception as e:
            print(f"📩 使用 PG",flush=True)
            row = await PGStatsDB.find_transaction_by_description(data["chatinfo"])
            if not row:
                print(f"❌ 查交易出错: {e}", flush=True)
                row = None
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
        try:
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
        except Exception as e:
            print(f"❌ 处理 payment 出错: {e}", flush=True)
           

    await event.reply(json.dumps({"ok": 0, "error": "unknown_json"}))



# ==================================================================
# 访问 
# ==================================================================

async def _fetch_and_consume(session: aiohttp.ClientSession, url: str):
    """
    并发读取网页内容：
    - 加一个时间戳参数，避免缓存
    - 真正把内容 read() 回来，让对方服务器感觉有人在看页面
    """
    try:
        params = {"t": int(datetime.now().timestamp())}
        async with session.get(url, params=params) as resp:
            content = await resp.read()  # 真实读取内容
            length = len(content)
            # print(f"🌐 keep-alive fetch => {url} status={resp.status} bytes={length}", flush=True)
    except Exception as e:
        print(f"⚠️ keep-alive fetch failed => {url} error={e}", flush=True)


async def ping_keepalive_task():
    """
    每 4 分钟并发访问一轮 URL，读取完整内容。
    """
    ping_urls = [
        "https://tgone-da0b.onrender.com",  # TGOND  park
        "https://lz-qjap.onrender.com",     # 上传 luzai02bot
        "https://lz-v2p3.onrender.com",     # 鲁仔 lz04bot   # 
        "https://twork-vdoh.onrender.com",  # TGtworkONE freebsd666bot
        "https://twork-f1im.onrender.com",  # News  news05251
        "https://lz-9bfp.onrender.com",     # 菊次郎 stcxp1069
        "https://lz-rhxh.onrender.com",     # 红包 stoverepmaria
        "https://lz-6q45.onrender.com"      # 布施 yaoqiang648
    ]

    timeout = aiohttp.ClientTimeout(total=10)
    headers = {
        # 用正常浏览器 UA，更像「真人访问」
        "User-Agent": "Mozilla/5.0 (keep-alive-bot) Chrome/120.0"
    }

    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                tasks = [
                    _fetch_and_consume(session, url)
                    for url in ping_urls
                ]
                # 并发执行所有请求
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # 只在需要时检查异常（这里仅打印，有需求可加统计）
                for url, r in zip(ping_urls, results):
                    if isinstance(r, Exception):
                        print(f"⚠️ task error for {url}: {r}", flush=True)

        except Exception as outer:
            print(f"🔥 keep-alive loop outer error: {outer}", flush=True)

        # 间隔 4 分钟
        try:
            await client.catch_up()
        except Exception as e:
            print("⚠️ catch_up() 失败，准备重连:", e, flush=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await client.connect()
            await client.catch_up()
        await asyncio.sleep(240)


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


    # 启动群组统计 + 定期离线交易回放
    await GroupStatsTracker.start_background_tasks(
        offline_replay_coro=replay_offline_transactions,
        offline_interval=90   # 每 90 秒跑一次，你可以改成 300 等
    )


    print("🤖 ly bot 启动中(SESSION_STRING)...")

    await client.start()
    await client.catch_up()


    # ✅ 启动 keep-alive 背景任务（每 4 分钟并发访问一轮）
    asyncio.create_task(ping_keepalive_task())

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
# 
    # await add_contact()
    if int(user_id) == int(KEY_USER_ID):
        print("⚠️ 警告：你正在使用 KEY_USER_ID 账号运行 Bot，请确认这是你想要的。", flush=True) 
    else:
        try:
            print(f"✅ KEY_USER_ID 检查通过，当前运行账号 {user_id} , 主要用户是  {KEY_USER_ID} 。", flush=True)
            await notify_command_receivers_on_start()
        except Exception as e:
            print(f"⚠️ 通知命令接收者时出错: {e}", flush=True)
            await add_contact()

    print("📡 开始监听所有事件...")

    # Render 用 PORT
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    await web._run_app(app, host="0.0.0.0", port=port)

    await client.run_until_disconnected()

    # 优雅关闭
    await GroupStatsTracker.stop_background_tasks()
    await PGStatsDB.close_pool()
    await MySQLPool.close()


if __name__ == "__main__":
    asyncio.run(main())
