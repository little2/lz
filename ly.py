import asyncio
import json
import os

from datetime import datetime, timedelta, date, timezone
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiohttp import web
import aiohttp


from lz_mysql import MySQLPool


from pg_stats_db import PGStatsDB
from group_stats_tracker import GroupStatsTracker

from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import InputPhoneContact,DocumentAttributeFilename,InputDocument
from telethon.tl.types import PeerUser
from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, PeerIdInvalidError


from typing import Optional, List, Any
 

import importlib
import inspect
import time
import re





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
    KEY_USER_PHONE,
    SWITCHBOT_USERNAME,
    THUMB_DISPATCH_INTERVAL,
    THUMB_BOTS,
    THUMB_PREFIX,
    DEBUG_HB_GROUP_ID,
    FORWARD_THUMB_USER
)

TG_TEXT_LIMIT = 4096
BILIBILI_URL_RE = re.compile(r"https://www\.bilibili\.com/\S+")
BILIBILI_VIDEO_CODE_RE = re.compile(r"https://www\.bilibili\.com/video/([^/?#]+)/?", re.IGNORECASE)
STARTUP_BILIBILI_TARGET = "@bilibiliparse_bot"
STARTUP_BILIBILI_URL = "https://www.bilibili.com/video/BV1MiotYGEzK/?share_source=copy_web&vd_source=ff5c3551704f213ef5ae6be4679feb21"
STARTUP_BILIBILI_TIMEOUT = 120
STARTUP_BILIBILI_SUBSCRIBE_HINT = "用解析前需要订阅频道"
STARTUP_BILIBILI_WAITING_TEXT = "✅ Summarizing with AI... Please wait a moment..."
STARTUP_BILIBILI_DOWNLOAD_FAILED_TEXT = "Download failed"
STARTUP_ZTJIANBAO_TARGET = "@ztjianbaobot"
STARTUP_ZTJIANBAO_TIMEOUT = 120

# ======== Telethon 启动方式 ========
client = TelegramClient(
    session=StringSession(SESSION_STRING),
    api_id=API_ID,
    api_hash=API_HASH,
    connection_retries=999999,
    retry_delay=2
)

# ======== 设置群组发言统计（class classmethod 风格） ========
GroupStatsTracker.configure(
    client,
    flush_interval=STAT_FLUSH_INTERVAL,
    flush_batch_size=STAT_FLUSH_BATCH_SIZE
)




# async def notify_command_receivers_on_start():
#     target = await client.get_entity(int(KEY_USER_ID))     
#     me = await client.get_me()
#     await client.send_message(target, f"[LY-HB] <code>{me.id}</code> - {me.first_name} {me.last_name or ''} {me.phone or ''}。我在执行 LY 任务！",parse_mode='html')  

#     if SWITCHBOT_USERNAME:
#         await client.send_message(SWITCHBOT_USERNAME, f"/start",parse_mode='html')      
#     return
   
async def add_contact():

    # 构造一个要导入的联系人
    contact = InputPhoneContact(
        client_id=0, 
        phone="+447447471403", 
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



@client.on(events.MessageDeleted)
async def handle_message_deleted(event):
    """
    监听群组消息删除
    """
    if not event.chat_id:
        return

    chat_id = int(event.chat_id)
    deleted_ids = [int(mid) for mid in event.deleted_ids]

    print(
        f"🗑️ 检测到删除事件 chat_id={chat_id} message_ids={deleted_ids}",
        flush=True
    )

    try:
        await PGStatsDB.mark_message_deleted(chat_id, deleted_ids)
    except Exception as e:
        print(f"❌ 更新删除时间失败: {e}", flush=True)


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
        print(f"🔍 PostgreSQL 离线队列结果: {result}", flush=True)

   

    if result.get("ok") == "1":
        payload = json.dumps({
            "ok": 1 ,
            "chatinfo": f"{chat_id}_{msg_id}"
        })
        
        print(f"🔍 交易数据 {backend} {payload}", flush=True)

        entity = await client.get_entity(receiver_id)
        result = await client.send_message(entity, payload)
        print(f"🔍 交易结果 result={result} ", flush=True)

        
       
        
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
    
    msg = event.message
    text = event.raw_text.strip()

    sender = await event.get_sender()
    sender_username = (getattr(sender, "username", "") or "").lower()
    if sender_username == STARTUP_BILIBILI_TARGET.lstrip("@").lower():
        print(f"📩 收到 {STARTUP_BILIBILI_TARGET} 回覆，略过自动私聊处理。", flush=True)
        return

    bilibili_url = _extract_first_bilibili_url(text)
    if bilibili_url:
        print(f"📩 私聊匹配到 bilibili URL，开始解析。 url={bilibili_url}", flush=True)
        await parse_bilibili(bilibili_url)
        return

    # [NEW] 私聊视频 + caption 以 |_thumbnail_| 开头：登记任务
    if msg and getattr(msg, "video", None):
        fu = _parse_thumb_caption(text)
        if fu:
            try:
                created = await insert_thumbnail_task_if_absent(fu, msg)
                await event.reply("✅ thumbnail 任务已登记" if created else "ℹ️ thumbnail 任务已存在，忽略重复")
            except Exception as e:
                await event.reply(f"❌ thumbnail 入库失败: {e}")
            return
        else:
            print(f"📩 私聊视频消息，但 caption 不符合 thumbnail 任务格式，忽略。 text={text}", flush=True)

        # [NEW] bot 回传缩图：photo 且 reply_to_msg_id 存在
    elif msg and getattr(msg, "photo", None) and getattr(msg, "reply_to_msg_id", None):
        
        try:
            print(f"📩 收到私聊 photo 消息，尝试处理为 thumbnail 回传，reply_to_msg_id={msg.reply_to_msg_id}", flush=True)
            sender = await event.get_sender()
            bot_name = getattr(sender, "username", None) or ""
            if bot_name != "": 
                bot_name = f"@{bot_name}"
            if bot_name and bot_name in THUMB_BOTS:
                
                ok = await complete_task_by_reply(
                    bot_name=bot_name,
                    chat_id=int(event.chat_id),
                    reply_to_msg_id=int(msg.reply_to_msg_id),
                    photo_obj=msg.photo,
                    recv_message_id=int(msg.id),
                )

                # ===== [NEW] 解析 reply 的那条“派工视频消息”的 file_unique_id =====
                fu = None
                try:
                    reply_msg = await event.get_reply_message()  # Telethon 会取到被 reply 的那条消息
                    if reply_msg:
                        # 你的派工视频 caption 是：f"{THUMB_PREFIX}{fu}"
                        # 直接沿用现有解析器
                        fu = _parse_thumb_caption((reply_msg.raw_text or "").strip())
                except Exception as e:
                    print(f"⚠️ 取 reply_msg / 解析 file_unique_id 失败: {e}", flush=True)

                # ===== [NEW] 再把这张图传给指定用户，caption=fu =====
                # 只有在 ok==True 且 fu 解析到时才转发，避免误发
                if ok and fu:
                    try:
                        target = await client.get_entity(FORWARD_THUMB_USER)
                        await client.send_file(
                            target,
                            file=msg.photo,
                            caption=f"|_SET_THUMB_|{str(fu)}"
                        )
                        print(f"📤 已转发缩图给 {FORWARD_THUMB_USER}, caption=fu={fu}", flush=True)
                    except Exception as e:
                        print(f"❌ 转发缩图给 {FORWARD_THUMB_USER} 失败: {e}", flush=True)

                # 原本的回覆逻辑保留，但如果 fu 取到，可顺便带出便于你确认
                if ok:
                    if fu:
                        await event.reply(f"✅ thumbnail completed 已记录\nfu={fu}")
                    else:
                        await event.reply("✅ thumbnail completed 已记录（但未能从 reply 消息解析 fu）")
                else:
                    await event.reply("⚠️ 未匹配到 working 任务（请确认是回复派工消息）")

                return
                
            else:
                print(f"📩 私聊 photo 消息，但发送者不是已知 thumbnail bot，忽略。 bot_name={bot_name}", flush=True)
        except Exception as e:
            await event.reply(f"❌ 处理回传缩图失败: {e}")
            return


    

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
            result = await client.send_message(entity, word)
            
            await event.reply(f"✅ 已转发。{target_raw} | {word}")
            print(f"🔍 tell 发送结果: {result} {target_raw} | {word}", flush=True)
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

    elif text.startswith("/catch"):
        # 可选：先给个即时反馈，避免你以为没反应
        await event.reply("⏳ 正在执行 catch_up()，请稍候…")

        try:
            print(f"[CATCH] 手动触发重连 + catch_up(), from user_id={event.sender_id}", flush=True)
            await event.reply("⏳ 正在同步最新消息，请稍候…")
            await client.catch_up()
            await event.reply("✅ catch_up() 已执行完成。")

            print("[CATCH] catch_up() 执行完成。", flush=True)
        except Exception as e:
            err = f"[CATCH] 执行 catch_up() 失败: {e!r}"
            print(err, flush=True)
            await event.reply(f"❌ catch_up() 失败：{e!r}")    

        try:
            client.iter_dialogs(limit=1)
        except Exception as e:
            print(f"[WD] keep_updates_warm 出错: {e}", flush=True)
        return


    elif text.startswith("/topics"):
        # 用法：/topics <chat_id> <YYYY-MM-DD> <hour>
        parts = text.split()
        if len(parts) != 4:
            await event.reply("用法：/topics <chat_id> <YYYY-MM-DD> <hour>\n例如：/topics -1001234567890 2025-12-17 13")
            return

        chat_id = int(parts[1])
        day = parts[2]
        hour = int(parts[3])

        from datetime import datetime
        stat_date = datetime.strptime(day, "%Y-%m-%d").date()

        topics = await PGStatsDB.get_topics_hourly(chat_id, stat_date, hour)
        if not topics:
            await event.reply("该小时尚无主题结果（可能还没跑 topic worker）。")
            return

        lines = []
        lines.append(f"📌 chat_id={chat_id} | {stat_date} {hour:02d}:00")
        for t in topics[:10]:
            kws = (t.get("keywords") or "").split(",")
            mids = t.get("message_ids") or []
            
            lines.append("")
            lines.append(f"msg_count={t.get('msg_count',0)}")
            lines.append(f"关键词：{' / '.join(kws[:12])}")
            lines.append(f"message_id：{', '.join(str(x) for x in mids[:30])}")

        await event.reply("\n".join(lines))
        return


    if event.sender_id not in ALLOWED_PRIVATE_IDS:
        print(f"用户 {event.sender_id} 不在允许名单，忽略。 text={text}")
        return

    # 尝试解析 JSON
    try:
        data = json.loads(event.raw_text)
        if not isinstance(data, dict):
            return
    except Exception:
        print(f"📩 私人消息非 JSON，忽略。")
        return
    
    # await MySQLPool.ensure_pool()
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


@client.on(events.NewMessage)
async def handle_group_bilibili(event):
    if event.is_private:
        return

    text = (event.raw_text or "").strip()
    bilibili_url = _extract_first_bilibili_url(text)
    if not bilibili_url:
        return

    print(f"📩 群组匹配到 bilibili URL，开始解析。 chat_id={event.chat_id} url={bilibili_url}", flush=True)
    await parse_bilibili(bilibili_url)



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
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            content = await resp.read()  # 真实读取内容
            length = len(content)
            # print(f"🌐 keep-alive fetch => {url} status={resp.status} bytes={length}", flush=True)
    except asyncio.TimeoutError:
        print(f"⚠️ keep-alive fetch timeout => {url} (10s)", flush=True)
    except Exception as e:
        print(f"⚠️ keep-alive fetch failed => {url}: {type(e).__name__}: {e}", flush=True)


async def ping_keepalive_task():
    """
    每 4 分钟并发访问一轮 URL，读取完整内容。
    """
    ping_urls = [
        "https://tgone-da0b.onrender.com",  # TGOND  park
        "https://lz-qjap.onrender.com",     # 上传 luzai02bot
        "https://lz-v2p3.onrender.com",     # LZ-No1    
        "https://twork-vdoh.onrender.com",  # TGtworkONE freebsd666bot
        "https://twork-f1im.onrender.com",  # News  news05251
        "https://lz-9bfp.onrender.com",     # 菊次郎 stcxp1069
        "https://lz-rhxh.onrender.com",     # 调察宝 stoverepmaria
        "https://lz-6q45.onrender.com",     # 布施 yaoqiang648
        "https://tgone-ah13.onrender.com",  # Rely
        "https://hb-lp3a.onrender.com",     # HB  
        "https://lz-upload.onrender.com",   # LZ-No2
        "https://lz-pbtb.onrender.com"      # LZ-1002
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

        # 间隔 50 秒
        try:
            await client.catch_up()
            client.iter_dialogs(limit=1)
        except Exception as e:
            print("⚠️ catch_up() 失败，准备重连:", e, flush=True)
            try:
                await client.disconnect()
            except Exception:
                pass
            await client.connect()
            await client.catch_up()
        await asyncio.sleep(50)



# ==================================================================
# 预览图
# =================================================================
def _parse_thumb_caption(text: str) -> str | None:
    if not text:
        return None
    t = text.strip()
    if not t.startswith(THUMB_PREFIX):
        return None
    rest = t[len(THUMB_PREFIX):].strip()
    if not rest:
        return None
    return rest.split()[0].strip() or None


def _extract_doc_filename(doc) -> str | None:
    try:
        for a in (doc.attributes or []):
            if isinstance(a, DocumentAttributeFilename):
                return a.file_name
    except Exception:
        pass
    return None


async def insert_thumbnail_task_if_absent(file_unique_id: str, msg) -> bool:
    """
    私聊视频任务登记：若已存在则忽略，不存在则新增。
    """
    doc = getattr(msg, "video", None)
    if not doc:
        return False

    doc_id = int(doc.id)
    access_hash = int(doc.access_hash)
    file_reference = getattr(doc, "file_reference", None)  # bytes
    mime_type = getattr(doc, "mime_type", None)
    file_size = int(getattr(doc, "size", 0) or 0)
    file_name = _extract_doc_filename(doc)

    return await PGStatsDB.insert_thumbnail_task_if_absent(
        file_unique_id=file_unique_id,
        file_type="video",
        doc_id=doc_id,
        access_hash=access_hash,
        file_reference=file_reference,
        mime_type=mime_type,
        file_name=file_name,
        file_size=file_size,
    )


async def pick_available_bot_from_tasks() -> str | None:
    """
    不建 thumbnail_bot_status：只靠 thumbnail_task 推断 bot 是否 working
    """
    if not THUMB_BOTS:
        return None

    working = await PGStatsDB.get_working_counts(THUMB_BOTS)
    for b in THUMB_BOTS:
        if working.get(b, 0) == 0:
            return b
    return None


async def lock_one_pending_task_for_bot(bot_name: str) -> dict | None:
    return await PGStatsDB.lock_one_pending_task_for_bot(bot_name)


async def update_task_sent_info(file_unique_id: str, chat_id: int, message_id: int):
    await PGStatsDB.update_task_sent_info(file_unique_id, chat_id, message_id)



async def complete_task_by_reply(bot_name: str, chat_id: int, reply_to_msg_id: int, photo_obj, recv_message_id: int) -> bool:
    """
    bot 用 photo 回复派工消息：
    用 (assigned_bot_name + sent_chat_id + sent_message_id + status=working) 精确定位任务并完成。
    """
    thumb_doc_id = int(photo_obj.id)
    thumb_access_hash = int(photo_obj.access_hash)
    thumb_file_reference = getattr(photo_obj, "file_reference", None)  # bytes

    return await PGStatsDB.complete_task_by_reply(
        bot_name=bot_name,
        chat_id=int(chat_id),
        reply_to_msg_id=int(reply_to_msg_id),
        thumb_doc_id=thumb_doc_id,
        thumb_access_hash=thumb_access_hash,
        thumb_file_reference=thumb_file_reference,
        recv_message_id=int(recv_message_id),
    )


async def thumbnail_dispatch_loop():
    if not THUMB_BOTS:
        print("ℹ️ THUMB_BOTS 未配置，thumbnail_dispatch_loop 不启动。", flush=True)
        return

    while True:
        try:

            # ly.py 的 thumbnail_dispatch_loop() while True: try: 里面，最前面加
            timeout_n = await PGStatsDB.mark_working_tasks_failed(older_than_seconds=3600)
            if timeout_n:
                print(f"⏱️ thumbnail_task timeout sweep: {timeout_n} rows -> failed", flush=True)

            bot_name = await pick_available_bot_from_tasks()
            if not bot_name:
                await asyncio.sleep(THUMB_DISPATCH_INTERVAL)
                continue

            task = await lock_one_pending_task_for_bot(bot_name)
            if not task:
                await asyncio.sleep(THUMB_DISPATCH_INTERVAL)
                continue

            fu = task["file_unique_id"]

            # ===== 从 PostgreSQL 取出原视频的引用三件套 =====
            doc_id = int(task["doc_id"])
            access_hash = int(task["access_hash"])

            file_ref = task.get("file_reference")
            if file_ref is None:
                raise RuntimeError(f"thumbnail_task.file_reference is NULL, file_unique_id={fu}")

            # asyncpg 可能返回 memoryview，必须转 bytes
            if isinstance(file_ref, memoryview):
                file_ref = file_ref.tobytes()
            elif not isinstance(file_ref, (bytes, bytearray)):
                file_ref = bytes(file_ref)

            input_doc = InputDocument(id=doc_id, access_hash=access_hash, file_reference=file_ref)

            # 建议 caption 带上 file_unique_id，便于 bot 端识别
            # 也能让你肉眼排查更方便
            caption = f"{THUMB_PREFIX}{fu}"

            entity = await client.get_entity(bot_name)





            # ===== 关键：把“原媒体”直接发给 bot =====

            # ly.py 的 thumbnail_dispatch_loop() 里，拿到 task 后（fu/task_id 等），send_file 改为：

            task_id = int(task["id"])
            fu = task["file_unique_id"]

            try:
                sent = await client.send_file(
                    entity,
                    file=input_doc,
                    caption=caption
                )
            except Exception as send_err:
                # 1) 标记 failed
                await PGStatsDB.mark_task_failed_by_id(task_id)
                print(f"❌ thumbnail send_file failed -> mark failed: task_id={task_id} fu={fu} err={send_err}", flush=True)
                await asyncio.sleep(THUMB_DISPATCH_INTERVAL)
                continue

            # send 成功才记录派发消息
            await update_task_sent_info(fu, int(sent.chat_id), int(sent.id))
            print(f"📤 thumbnail 派发媒体: fu={fu} -> bot={bot_name} msg_id={sent.id}", flush=True)


        except Exception as e:
            print(f"❌ thumbnail_dispatch_loop error: {e}", flush=True)

        await asyncio.sleep(THUMB_DISPATCH_INTERVAL)








async def _safe_get_user_entity(client, uid: int):
    """
    尝试用多种方式拿到 user entity。
    拿不到就返回 None，不抛异常。
    """
    # 1) 直接用 int（依赖 cache）
    try:
        return await client.get_entity(uid)
    except Exception:
        pass

    # 2) 用 PeerUser（有时比 int 更稳定）
    try:
        return await client.get_entity(PeerUser(uid))
    except Exception:
        return None


async def ensure_user_names_via_telethon(
    client,
    user_ids: list[int],
    chat_id: int | None = None,     # 可选：传入群 chat_id，用于 fallback
    max_fallback_scan: int = 2000,   # 可选：fallback 扫描的人数上限
):
    """
    只补齐缺名字的 user。抓不到 entity 不报错，直接跳过。
    如果传入 chat_id，则可尝试从群成员列表/参与者中补（有限度）。
    """
    from pg_stats_db import PGStatsDB

    # 找出缺名字的
    missing = await PGStatsDB.get_users_missing_names(user_ids)
    if not missing:
        return

    # 可选 fallback：先准备一个 uid->entity 的映射（从群参与者列表抓）
    fallback_map = {}
    if chat_id is not None:
        try:
            # 注意：大群会很慢/很重，所以加上上限
            count = 0
            async for u in client.iter_participants(chat_id):
                fallback_map[int(u.id)] = u
                count += 1
                if count >= max_fallback_scan:
                    break
        except Exception as e:
            print(f"⚠️ fallback iter_participants 失败 chat_id={chat_id}: {e}", flush=True)

    # 逐个补齐
    for uid in missing:
        ent = await _safe_get_user_entity(client, int(uid))
        if ent is None:
            # fallback：如果我们扫到了群参与者
            ent = fallback_map.get(int(uid))

        if ent is None:
            # 这里不要再报错，只记录一下即可
            print(f"⚠️ 无法抓取 user_name uid={uid}: entity not found (no cache / not accessible)", flush=True)
            continue

        first_name = getattr(ent, "first_name", None)
        last_name  = getattr(ent, "last_name", None)

        # 写回 PG
        await PGStatsDB.upsert_user_profile(int(uid), first_name, last_name)

def _fmt_manager_line(managers: list[dict[str, Any]]) -> str:
    if not managers:
        return "—"
    parts = []
    for m in managers:
        name = (f"{m.get('first_name','')} {m.get('last_name','')}").strip()
        if not name:
            name = str(m.get("manager_user_id"))
        parts.append(f"{name} {m.get('manager_msg_count', 0)}")
    return "、".join(parts) if parts else "—"

async def build_board_rank_text(
    stat_date_from: date,
    stat_date_to: date,
    tg_chat_id: int,
    include_bots: bool = False,
    top_n: Optional[int] = None,   # None = 全列
) -> str:
    """
    单一榜单：
    - 排名规则：msg_count DESC → funds DESC
    - 区间汇总（不分天）
    """

    from pg_stats_db import PGStatsDB

    rows = await PGStatsDB.get_board_thread_stats_range_sum(
        stat_date_from=stat_date_from,
        stat_date_to=stat_date_to,
        tg_chat_id=tg_chat_id,
        include_bots=include_bots,
    )

    if not rows:
        return "（该区间内没有任何板块数据）"

    # ✅ 单一排序：msg → funds
    rows = sorted(
        rows,
        key=lambda r: (
            int(r.get("msg_count", 0)),
            int(r.get("funds", 0)),
        ),
        reverse=True,
    )

    if top_n is not None:
        rows = rows[:top_n]

    # ===== 文本输出 =====
    out: list[str] = [
        "📊 板块活跃排行榜",
        f"🗓 {stat_date_from} ～ {stat_date_to}",
        "",
    ]

    for idx, r in enumerate(rows, start=1):
        board_title = r.get("board_title") or "（未命名板块）"
        thread_id = r.get("thread_id")
        msg_count = int(r.get("msg_count", 0))
        funds = int(r.get("funds", 0))
        mgr_text = _fmt_manager_line(r.get("managers") or [])

        out.append(
            f"{idx}. {board_title}\n"
            f"   💬 {msg_count} ｜ 💎 {funds}  \n"
            f"   🧑‍💼 版主：{mgr_text}\n"
        )

    return "\n".join(out)

def _split_telegram_text(text: str, limit: int = TG_TEXT_LIMIT) -> List[str]:
    """
    优先按换行切段，避免硬切断行；若仍超长才硬切。
    """
    text = (text or "").strip()
    if not text:
        return [""]

    parts: List[str] = []
    buf: List[str] = []
    buf_len = 0

    for line in text.splitlines(True):  # keepends
        if buf_len + len(line) <= limit:
            buf.append(line)
            buf_len += len(line)
            continue

        # flush buffer
        if buf:
            parts.append("".join(buf).rstrip())
            buf = []
            buf_len = 0

        # if single line still too long -> hard cut
        while len(line) > limit:
            parts.append(line[:limit])
            line = line[limit:]
        if line:
            buf.append(line)
            buf_len = len(line)

    if buf:
        parts.append("".join(buf).rstrip())

    return parts

async def send_text_via_telethon(
    client,
    target_chat_id: int,
    text: str,
    target_thread_id: int = 0,
    silent: bool = False,
):
    """
    用 Telethon 发送文本到指定 chat_id；如果 target_thread_id>0，则尝试投递到对应 topic。
    自动分段（Telegram 4096 限制）。
    """
    chunks = _split_telegram_text(text)

    # 先解析 entity（避免 PeerIdInvalid）
    entity = await client.get_input_entity(int(target_chat_id))

    for i, chunk in enumerate(chunks):
        if not chunk:
            continue

        # 仅第一段带 reply_to（把消息落到 topic），后续分段默认直接跟随同一会话发
        reply_to = int(target_thread_id) if (i == 0 and int(target_thread_id) > 0) else None

        try:
            await client.send_message(
                entity=entity,
                message=chunk,
                reply_to=reply_to,
                silent=silent,
            )
        except Exception as e:
            # topic 投递失败时降级
            if reply_to is not None:
                print(f"⚠️ send_message(topic) 失败，降级到普通发送: chat_id={target_chat_id} thread_id={target_thread_id} err={e}", flush=True)
                await client.send_message(
                    entity=entity,
                    message=chunk,
                    silent=silent,
                )
            else:
                raise

async def send_board_rank_report(
    client,
    stat_date_from: date,
    stat_date_to: date,
    source_tg_chat_id: int,      # 用来统计的群（你原来的 tg_chat_id）
    target_chat_id: int,         # 要发送到哪里
    target_thread_id: int = 0,   # 要发送到哪个 topic（0=不指定）
    include_bots: bool = False,
    top_n: Optional[int] = None,
):
    """
    统计区间榜单（不分天）并发送到指定 chat/topic。
    """
    text = await build_board_rank_text(
        stat_date_from=stat_date_from,
        stat_date_to=stat_date_to,
        tg_chat_id=source_tg_chat_id,
        include_bots=include_bots,
        top_n=top_n,
    )

    await send_text_via_telethon(
        client=client,
        target_chat_id=int(target_chat_id),
        target_thread_id=int(target_thread_id),
        text=text,
    )



def now_taipei() -> datetime:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)


async def exec_send_yesterday_board_rank(client, task: dict, params: dict | None = None) -> None:
    """
    task_value 建议 JSON，示例：
    {
      "source_chat_id": -100xxx,
      "target_chat_id": -100yyy,
      "target_thread_id": 123,
      "top_n": 20,
      "include_bots": false
    }
    """
    from pg_stats_db import PGStatsDB

    cfg = params or {}
    if not cfg and task.get("task_value"):
        try:
            cfg = json.loads(task["task_value"])
        except Exception:
            cfg = {}

    await PGStatsDB.sync_board_from_mysql()

    source_chat_id = int(cfg.get("source_chat_id"))
    target_chat_id = int(cfg.get("target_chat_id"))
    target_thread_id = int(cfg.get("target_thread_id", 0))
    top_n = cfg.get("top_n", None)
    include_bots = bool(cfg.get("include_bots", False))

    # “昨日”用台北时间定义
    now_local = now_taipei()

    end_date = now_local.date() - timedelta(days=1)      # 昨日
    start_date = end_date - timedelta(days=6)            # 含昨日共 7 天
  


    await send_board_rank_report(
        client=client,
        stat_date_from=start_date,
        stat_date_to=end_date,
        source_tg_chat_id=source_chat_id,
        target_chat_id=target_chat_id,
        target_thread_id=target_thread_id,
        include_bots=include_bots,
        top_n=top_n,
    )


from typing import Any

def calc_board_manager_salary_weighted_from_stat(
    board_stat: dict[str, Any],
    base_salary: int = 150,
    bonus_ratio: float = 0.10,
    min_msg_count: int = 3,
) -> dict[str, Any]:
    """
    按“发言数占比”分配分成的版主工资计算（仅对有效版主：manager_msg_count > min_msg_count）。

    规则：
    - 若 funds < base_salary * N：将 funds 平均分配给 N 个有效版主（整数；余数留在 funds）
    - 若 funds >= base_salary * N：
        1) 先扣基本工资 base_salary * N
        2) 剩余 remain 的 10% 为 bonus_pool（整数向下取整）
        3) bonus_pool 按各版主 msg_count 占比加权分配（最大余数法）
        4) 每位工资 = base_salary + bonus_i
        5) 总扣除 = base_salary * N + bonus_pool

    回传：
    {
      "eligible_managers": [...],  # 原 manager dict（仅有效版主）
      "funds": int,
      "mode": "split_all" | "base_plus_weighted_bonus",
      "base_salary": int,
      "total_base": int,
      "remain": int,
      "bonus_pool": int,
      "total_deducted": int,
      "payouts": [
         {"manager_user_id": int, "manager_msg_count": int, "base": int, "bonus": int, "salary": int}
      ]
    }
    """

    funds = int(board_stat.get("funds", 0) or 0)
    managers = board_stat.get("managers") or []

    # 1) 有效版主：msg_count > min_msg_count
    eligible: list[dict[str, Any]] = []
    for m in managers:
        try:
            cnt = int(m.get("manager_msg_count", 0) or 0)
        except Exception:
            cnt = 0
        if cnt > min_msg_count:
            eligible.append(m)

    n = len(eligible)
    if n <= 0 or funds <= 0:
        return {
            "eligible_managers": [],
            "funds": funds,
            "mode": "split_all",
            "base_salary": base_salary,
            "total_base": 0,
            "remain": 0,
            "bonus_pool": 0,
            "total_deducted": 0,
            "payouts": [],
        }

    total_base = n * base_salary

    # 2) 版金不足：平分版金（在有效版主之间）
    if funds < total_base:
        per = funds // n
        total_deducted = per * n
        payouts = []
        for m in eligible:
            mid = int(m.get("manager_user_id") or 0)
            cnt = int(m.get("manager_msg_count") or 0)
            payouts.append({
                "manager_user_id": mid,
                "manager_msg_count": cnt,
                "base": 0,          # 版金不足时不再拆 base/bonus，工资就是平分额
                "bonus": 0,
                "salary": per,
            })
        return {
            "eligible_managers": eligible,
            "funds": funds,
            "mode": "split_all",
            "base_salary": base_salary,
            "total_base": total_base,
            "remain": 0,
            "bonus_pool": 0,
            "total_deducted": total_deducted,
            "payouts": payouts,
        }

    # 3) 版金充足：基本工资 + 加权分成
    remain = funds - total_base
    bonus_pool = int(remain * float(bonus_ratio))
    # 若 bonus_pool 为 0，仍照常发 base
    total_deducted = total_base + bonus_pool

    # 3.1 分母：有效版主发言数加总
    weights: list[int] = []
    for m in eligible:
        try:
            weights.append(max(0, int(m.get("manager_msg_count", 0) or 0)))
        except Exception:
            weights.append(0)

    weight_sum = sum(weights)
    # 若权重总和为 0（极端情况），则把分成池平均分（避免除 0）
    if bonus_pool <= 0:
        bonuses = [0] * n
    elif weight_sum <= 0:
        per = bonus_pool // n
        bonuses = [per] * n
        # 把余数用最大余数法补齐（此处等价于顺序补 1）
        for i in range(bonus_pool - per * n):
            bonuses[i] += 1
    else:
        # 3.2 最大余数法：先取 floor，再按余数大小补齐
        raw = [bonus_pool * w / weight_sum for w in weights]  # float
        floor_parts = [int(x) for x in raw]
        used = sum(floor_parts)
        left = bonus_pool - used

        remainders = [(raw[i] - floor_parts[i], i) for i in range(n)]
        remainders.sort(reverse=True, key=lambda t: t[0])

        bonuses = floor_parts[:]
        for k in range(left):
            bonuses[remainders[k % n][1]] += 1  # left <= n 通常成立，但用 %n 更稳

    payouts = []
    for i, m in enumerate(eligible):
        mid = int(m.get("manager_user_id") or 0)
        cnt = int(m.get("manager_msg_count") or 0)
        b = int(bonuses[i])
        payouts.append({
            "manager_user_id": mid,
            "manager_msg_count": cnt,
            "base": base_salary,
            "bonus": b,
            "salary": base_salary + b,
        })

    return {
        "eligible_managers": eligible,
        "funds": funds,
        "mode": "base_plus_weighted_bonus",
        "base_salary": base_salary,
        "total_base": total_base,
        "remain": remain,
        "bonus_pool": bonus_pool,
        "total_deducted": total_deducted,
        "payouts": payouts,
    }


async def exec_pay_board_manager_salary(client, task: dict, params: dict | None = None) -> None:
    """
    近 7 天版主工资（按板块独立发放）：

    - 统计口径：PGStatsDB.get_board_thread_stats_range_sum(start_date, end_date, source_chat_id)
    - 有效版主：manager_msg_count > 3
    - 工资规则：基本工资 150；若版金不足则平分版金；若充足则扣基本工资后，剩余的 10% 平均分成
    - 发放：MySQLPool.transaction_log(tx)
    - 若 result['status'] == 'insert' 才在目标版面公告（避免重复公告）
    """
    from pg_stats_db import PGStatsDB

    print(f"{task}")

    cfg = params or {}
    if not cfg and task.get("task_value"):
        task_value = task.get("task_value")
        # 如果已经是 dict 就直接用，否则尝试 JSON 解析
        if isinstance(task_value, dict):
            cfg = task_value
        else:
            try:
                cfg = json.loads(task_value)
            except Exception:
                cfg = {}


    source_chat_id = int(cfg.get("source_chat_id"))
    target_chat_id = int(cfg.get("target_chat_id"))
    target_thread_id = int(cfg.get("target_thread_id", 0))
    include_bots = bool(cfg.get("include_bots", False))

    print(
        f"💰 [salary] start pay_board_manager_salary source_chat_id={source_chat_id} "
        f"target_chat_id={target_chat_id} target_thread_id={target_thread_id} include_bots={include_bots}",
        flush=True,
    )

    # 近七天：含昨日共 7 天（台北时间）
    now_local = now_taipei()
    end_date = now_local.date() - timedelta(days=1)  # 昨日
    start_date = end_date - timedelta(days=6)
    pay_day = end_date.strftime("%Y-%m-%d")

    # 可选：先同步一次 board 基础资料（避免 board_key / title 缺失）
    try:
        await PGStatsDB.sync_board_from_mysql()
    except Exception as e:
        print(f"⚠️ [salary] sync_board_from_mysql failed: {e}", flush=True)

    rows = await PGStatsDB.get_board_thread_stats_range_sum(
        stat_date_from=start_date,
        stat_date_to=end_date,
        tg_chat_id=source_chat_id,
        include_bots=include_bots,
    )

    if not rows:
        print("ℹ️ [salary] no board stats found, nothing to pay", flush=True)
        return

    await MySQLPool.ensure_pool()
    

    for r in rows:
        board_key = (r.get("board_key") or "").strip()
        if not board_key:
            board_key = f"thread_{r.get('thread_id') or ''}".strip("_")

        board_title = (r.get("board_title") or "").strip() or board_key

        # —— 计算该板块：每位有效版主工资、总扣除（仅计算，不写回扣款）——
        calc = calc_board_manager_salary_weighted_from_stat(
            board_stat=r,
            base_salary=150,
            bonus_ratio=0.10,
            min_msg_count=5,
        )

        payouts = calc["payouts"]
        if not payouts:

            
            continue

        # （可选）先补齐名字：针对 eligible_managers
        missing_ids = []
        for m in calc["eligible_managers"]:
            name = (f"{m.get('first_name','')} {m.get('last_name','')}").strip()
            if not name:
                missing_ids.append(int(m.get("manager_user_id") or 0))
        missing_ids = [x for x in missing_ids if x]
        if missing_ids:
            await ensure_user_names_via_telethon(client=client, user_ids=missing_ids, chat_id=source_chat_id)

        # 发薪（每位不同）
        for p in payouts:
            manager_id = p["manager_user_id"]
            salary = p["salary"]          # ✅ 这里是 150 + 按占比分到的 bonus
            bonus = p["bonus"]            # ✅ 可用于公告
            manager_cnt = p["manager_msg_count"]

            tx = {
                "sender_id": 0,
                "receiver_id": manager_id,
                "transaction_type": "salary",
                "transaction_description": f"{pay_day}_{board_key}_{manager_id}",
                "sender_fee": 0,
                "receiver_fee": salary,   # ✅ 替换原本 +10
            }

            result = await MySQLPool.transaction_log(tx)
            # result = {"status":"insert"}  # TODO: 删除测试代码
        
            print(f"💰result={result}", flush=True)

            if result.get("status") == "insert": 
                await MySQLPool.update_board_funds(board_id=r.get("board_id"), pay_funds=-salary)    # TODO: 删除测试代码
                new_expire_timestamp = await MySQLPool.extend_bm_membership(manager_id=manager_id,manager_cnt=manager_cnt) 
                

                # 公告示例（你可按风格再精简）
                salary_detail = (
                    f"基本 150 + 分成 {bonus}"
                    if calc["mode"] == "base_plus_weighted_bonus"
                    else "版金不足，平分版金"
                )
                # 取回原 manager dict 用 _fmt_manager_line 展示名字
                m = next((mm for mm in calc["eligible_managers"] if int(mm.get("manager_user_id") or 0) == manager_id), None)
                manager_line = _fmt_manager_line([m] if m else [{"manager_user_id": manager_id, "manager_msg_count": manager_cnt}])
                # 将timestamp 转成 Y-m-d H:i:s
                new_expire_timestamp_date = datetime.fromtimestamp(new_expire_timestamp).strftime("%Y-%m-%d %H:%M:%S") if new_expire_timestamp else ""
                
                notice = (
                    "💰 版主工资已发放\n"
                    f"🗓 {pay_day}\n"
                    f"🏷 {board_title} ({board_key})\n"
                    f"👤 {manager_line}\n"
                    f"💎 +{salary}（{salary_detail}）\n"
                    f"💬 近7天发言 {manager_cnt}\n"
                    f"🏦 本板版金 {calc['funds']}｜分成池 {calc['bonus_pool']}｜本板扣款 {calc['total_deducted']}\n"
                )
                
                if new_expire_timestamp_date:
                    notice =f"{notice}👿 延长小懒觉期限至 {new_expire_timestamp_date}\n"

                await MySQLPool.set_media_auto_send({
                    "chat_id": manager_id,
                    "type": "text",
                    "text": notice,
                    "bot": "xiaolongyang005bot",
                    "create_timestamp": int(time.time()),
                    "plan_send_timestamp": int(time.time()),  # 一小时后
                })


                try:
                    await send_text_via_telethon(
                        client=client,
                        target_chat_id=target_chat_id,
                        target_thread_id=target_thread_id,
                        text=notice,
                    )
                except Exception as e:
                    print(
                        f"⚠️ [salary] send notice failed chat_id={target_chat_id} thread_id={target_thread_id} err={e}",
                        flush=True,
                    )

async def exec_notify_mass_delete_disable_points(client, task: dict, params: dict | None = None) -> None:
    """
    功能：
    1) 查过去24小时内：tg_group_messages_raw 中 chat_id=-1001943193056 且 deleted_at IS NOT NULL 的记录数
    2) 按 user_id group by，筛出 count > 10 的 user_id
    3) 用 MySQLPool.set_media_auto_send 给这些 user_id 发私聊通知

    可选 task_value / params（不传就用默认）：
    {
      "source_chat_id": -1001943193056,
      "min_deleted_cnt": 10,
      "bot": "xiaolongyang002bot"
    }
    """
    from pg_stats_db import PGStatsDB
    from lz_mysql import MySQLPool

    # ---- 解析配置（对齐你现有 exec_* 风格）----
    cfg = params or {}
    if not cfg and task.get("task_value"):
        task_value = task.get("task_value")
        if isinstance(task_value, dict):
            cfg = task_value
        else:
            try:
                import json
                cfg = json.loads(task_value)
            except Exception:
                cfg = {}

    source_chat_id = int(cfg.get("source_chat_id", -1001943193056))
    min_deleted_cnt = int(cfg.get("min_deleted_cnt", 10))
    bot_name = (cfg.get("bot") or "xiaolongyang005bot").strip()

    notice = (
        "我们理解，也尊重部分群友希望在网络上不留痕迹的想法。\n\n"
        "不过由于发言会产生积分奖励，为了避免机制被反复利用，\n"
        "因此，凡是出现批量发言后再集中删除内容的情况，\n"
        "将取消其发言获得积分的资格。\n\n"
        "希望大家理解，这个调整只是为了维护机制的公平与长期稳定运行。\n\n"
        f"任何问题，可以联系教务处小助手 @lyjwcbot\n\n"
    )

    # ---- 时间窗口：过去 24 小时（用 msg_time_utc 更精确；stat_date 只是 date）----
    # 这里按 UTC 计算窗口；若你希望按台北/新加坡时间，可改 now_taipei() 再转 UTC。
    now_utc = datetime.now(timezone.utc)
    since_utc = now_utc - timedelta(hours=24)

    if PGStatsDB.pool is None:
        print("⚠️ PGStatsDB 未初始化，无法执行 exec_notify_mass_delete_disable_points。", flush=True)
        return

    # ---- 查 PG：deleted_at 非空的删除记录，按 user_id 聚合 ----
    async with PGStatsDB.pool.acquire() as conn_pg:
        rows = await conn_pg.fetch(
            """
            SELECT
                user_id,
                COUNT(*)::int AS deleted_cnt
            FROM tg_group_messages_raw
            WHERE chat_id = $1
              AND deleted_at IS NOT NULL
              AND msg_time_utc >= $2
            GROUP BY user_id
            HAVING COUNT(*) > $3
            ORDER BY deleted_cnt DESC
            """,
            source_chat_id,
            since_utc,
            min_deleted_cnt,
        )

    if not rows:
        print(
            f"ℹ️ [mass_delete_notice] no target users. chat_id={source_chat_id} "
            f"since_utc={since_utc.isoformat()} min_deleted_cnt>{min_deleted_cnt}",
            flush=True,
        )
        return

    # ---- MySQL 发消息队列 ----
    await MySQLPool.ensure_pool()

    sent = 0
    failed = 0

    for r in rows:
        uid = int(r["user_id"])
        cnt = int(r["deleted_cnt"])

        payload = {
            "chat_id": uid,
            "type": "text",
            "text": notice,
            "bot": bot_name,
            "create_timestamp": int(time.time()),
            "plan_send_timestamp": int(time.time()),
        }

        try:
            await MySQLPool.set_media_auto_send(payload)
            sent += 1
            print(f"📨 [mass_delete_notice] queued uid={uid} deleted_cnt={cnt}", flush=True)
        except Exception as e:
            failed += 1
            print(f"❌ [mass_delete_notice] queue failed uid={uid} deleted_cnt={cnt} err={e}", flush=True)

    print(
        f"✅ [mass_delete_notice] done. total={len(rows)} sent={sent} failed={failed} "
        f"chat_id={source_chat_id} since_utc={since_utc.isoformat()}",
        flush=True,
    )

async def run_taskrec_scheduler(client, poll_seconds: int = 180, stop_event: asyncio.Event | None = None):
    """
    不使用 TASK_EXECUTORS：
    - task_rec 到期就抓
    - 用 task_exec 动态定位函数并执行
    - 成功后 touch task_time = now
    - 失败则不 touch（下次继续重试；你也可以改成失败也 touch 避免刷屏）
    """
    from pg_stats_db import PGStatsDB

    while True:
        if stop_event and stop_event.is_set():
            print("🛑 task_rec scheduler stopped", flush=True)
            return

        now_epoch = int(time.time())

        try:
            tasks = await PGStatsDB.fetch_due_tasks_locked(now_epoch=now_epoch, limit=20)

            if not tasks:
                # print("ℹ️ task_rec: no due tasks", flush=True)
                await asyncio.sleep(poll_seconds)
                continue

            for t in tasks:
                print(f"🔍 task_rec: found due task_id={t.get('task_id')} exec={t.get('task_exec')}", flush=True)
                task_id = t["task_id"]
                exec_path = (t.get("task_exec") or "").strip()

                try:
                    await _run_task_exec(client, t)

                    # ✅ 执行完才 touch
                    await PGStatsDB.touch_task_time(task_id, int(time.time()))
                    print(f"✅ task done task_id={task_id} exec={exec_path}", flush=True)

                except Exception as e:
                    # ❌ 失败不 touch：下轮继续重试
                    print(f"❌ task failed task_id={task_id} exec={exec_path} err={e}", flush=True)

        except Exception as e:
            print(f"❌ scheduler loop error: {e}", flush=True)

        await asyncio.sleep(poll_seconds)

def _load_exec_callable(exec_path: str):
    """
    支持：
    - "func_name"：从 ly.py globals() 取
    - "module.func_name"：import module 后 getattr
    """
    exec_path = (exec_path or "").strip()
    if not exec_path:
        return None

    if "." in exec_path:
        module_name, func_name = exec_path.rsplit(".", 1)
        mod = importlib.import_module(module_name)
        return getattr(mod, func_name, None)

    # 不带 module 前缀：从当前 ly.py 全局找
    return globals().get(exec_path)

async def _run_task_exec(client, task: dict):
    """
    约定：执行函数签名为
      async def xxx(client, task, params) -> None
    params 来自 task_value(JSON)，解析失败则 {}
    """
    exec_path = (task.get("task_exec") or "").strip()
    fn = _load_exec_callable(exec_path)
    if fn is None:
        raise RuntimeError(f"task_exec not found: {exec_path}")

    # task_value -> params（建议 JSON）
    params = {}
    raw_val = task.get("task_value")
    if raw_val:
        try:
            params = json.loads(raw_val)
            if not isinstance(params, dict):
                params = {"_value": params}
        except Exception:
            params = {"_raw": raw_val}

    argc = len(inspect.signature(fn).parameters)

    if inspect.iscoroutinefunction(fn):
        if argc >= 3:
            await fn(client, task, params)
        else:
            await fn(client, task)
    else:
        if argc >= 3:
            fn(client, task, params)
        else:
            fn(client, task)



'''
為run_taskrec_scheduler新增一個任務,執行一個新的function, 該function ,先從get_board_thread_stats_range_sum取得近七天的板塊以及發言數大於3則的管理員
（    source_chat_id = int(cfg.get("source_chat_id"))
）

，遍循並執行
tx = {
    "sender_id": 0,
    "receiver_id": [管理員id],
    "transaction_type": 'salary',
    "transaction_description": [年月日]_[board_key],
    "sender_fee": 0,
    "receiver_fee": 10,
}

result = await MySQLPool.transaction_log(tx)
若 result['status'] == 'insert' ,則在以下版面張貼訊息
target_chat_id = int(cfg.get("target_chat_id"))
target_thread_id = int(cfg.get("target_thread_id", 0))
'''


async def say_hello():
     # 构造一个要导入的联系人
    contact = InputPhoneContact(
        client_id=0, 
        phone=KEY_USER_PHONE, 
        first_name="KeyMan", 
        last_name=""
    )
    result = await client(ImportContactsRequest([contact]))
    # print("导入结果:", result)
    target = await client.get_entity(int(KEY_USER_ID))     # 7550420493

    me = await client.get_me()
    await client.send_message(target, f"[LY-HB] <code>{me.id}</code> - {me.first_name} {me.last_name or ''} {me.phone or ''}。我在执行TGONE任务！",parse_mode='html') 

    try:
        await client.send_message(SWITCHBOT_USERNAME, f"/start",parse_mode='html')
        print(f"✅ 已向 @{SWITCHBOT_USERNAME} 发送启动消息。", flush=True)
    except Exception as e:
        print(f"⚠️ 向 @{SWITCHBOT_USERNAME} 发送消息失败（可能未关联或未启动）：{e}", flush=True)
        pass


async def parse_bilibili(bilibili_url: str):
    try:
        async with client.conversation(STARTUP_BILIBILI_TARGET, timeout=STARTUP_BILIBILI_TIMEOUT) as conv:
            await conv.send_message(bilibili_url)
            print(f"✅ 已发送启动 Bilibili URL 给 {STARTUP_BILIBILI_TARGET}", flush=True)
            response = await _await_bilibili_final_response(conv, bilibili_url)
            if response is None:
                print(f"⚠️ {STARTUP_BILIBILI_TARGET} 返回下载失败，放弃后续处理", flush=True)
                return

            print(f"📬 收到 {STARTUP_BILIBILI_TARGET} 的回覆: {response}", flush=True)

            if getattr(response, "media", None) is not None:
                file_unique_id = await _relay_media_to_ztjianbaobot(response)
                bilibibli_key = parse_bilibili_video_code(bilibili_url)
                bilibibli_caption = response.message or ""
                print(f"{bilibibli_caption} 🎬 Bilibili 视频 {bilibibli_key} 的 file_unique_id: {file_unique_id}", flush=True)
                if bilibibli_key and file_unique_id:
                    db_result = await MySQLPool.upsert_bilibibli(
                        enc_str=bilibibli_key,
                        file_unique_id=file_unique_id,
                    )
                    print(f"💾 bilibibli upsert: {db_result}", flush=True)
                else:
                    print(
                        f"⚠️ 略过 bilibibli upsert: enc_str={bilibibli_key!r} file_unique_id={file_unique_id!r}",
                        flush=True,
                    )

        # target = await client.get_entity(int(KEY_USER_ID))
        # await client.forward_messages(target, response)
        # print(f"✅ 已将 {STARTUP_BILIBILI_TARGET} 的回覆转传给 KEY_USER_ID={KEY_USER_ID}", flush=True)
    except asyncio.TimeoutError:
        print(
            f"⚠️ 等待 {STARTUP_BILIBILI_TARGET} 回覆超时（{STARTUP_BILIBILI_TIMEOUT}s）",
            flush=True,
        )
    except Exception as e:
        print(f"⚠️ 发送启动 Bilibili URL 失败: target={STARTUP_BILIBILI_TARGET} err={e}", flush=True)





def _get_response_text(response) -> str:
    return (getattr(response, "raw_text", None) or getattr(response, "message", None) or "").strip()


def _extract_first_bilibili_url(text: str) -> str:
    urls = BILIBILI_URL_RE.findall(text or "")
    return urls[0] if urls else ""


def parse_bilibili_video_code(url: str) -> str:
    text = (url or "").strip()
    if not text:
        return ""

    match = BILIBILI_VIDEO_CODE_RE.search(text)
    if not match:
        return ""

    return match.group(1).strip()


def _get_first_line(text: str) -> str:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return lines[0] if lines else ""


async def _await_bilibili_final_response(conv, bilibili_url: str):
    while True:
        response = await conv.get_response()
        text = _get_response_text(response)

        if STARTUP_BILIBILI_DOWNLOAD_FAILED_TEXT in text:
            print(f"⚠️ 收到 {STARTUP_BILIBILI_TARGET} 下载失败回覆，停止处理", flush=True)
            return None

        if STARTUP_BILIBILI_SUBSCRIBE_HINT in text:
            channel_ref = _extract_bilibili_subscription_target(response)
            if channel_ref:
                joined = await _join_public_channel(channel_ref)
                if joined:
                    print(f"✅ 已先订阅频道 {channel_ref}，重新发送 Bilibili URL", flush=True)
                    await conv.send_message(bilibili_url)
                    continue

                print(f"⚠️ 自动订阅频道失败: {channel_ref}", flush=True)
            else:
                print(f"⚠️ 收到订阅提示，但未解析出频道目标: {response}", flush=True)
            continue

        if text == STARTUP_BILIBILI_WAITING_TEXT:
            print(f"⏳ 收到 {STARTUP_BILIBILI_TARGET} 的处理中提示，继续等待下一则回覆", flush=True)
            continue

        if getattr(response, "media", None) is not None:
            print(f"📦 收到 {STARTUP_BILIBILI_TARGET} 的媒体回覆: {response}", flush=True)
            return response

        print(f"📨 收到 {STARTUP_BILIBILI_TARGET} 的非媒体回覆，继续等待媒体: {response}", flush=True)


async def _relay_media_to_ztjianbaobot(response) -> None:
    try:
        async with client.conversation(STARTUP_ZTJIANBAO_TARGET, timeout=STARTUP_ZTJIANBAO_TIMEOUT) as conv:
            await conv.send_file(
                file=response.media,
                caption=_get_response_text(response) or None,
            )
            print(f"📤 已将媒体转发给 {STARTUP_ZTJIANBAO_TARGET}", flush=True)

            zt_response = await conv.get_response()
            zt_text = _get_response_text(zt_response)
            first_line = _get_first_line(zt_text)

            if first_line:
                print(f"📝 {STARTUP_ZTJIANBAO_TARGET} 首行回覆: {first_line}", flush=True)
                return first_line
            else:
                print(f"⚠️ {STARTUP_ZTJIANBAO_TARGET} 回覆没有可用文字: {zt_response}", flush=True)
    except asyncio.TimeoutError:
        print(f"⚠️ 等待 {STARTUP_ZTJIANBAO_TARGET} 回覆超时（{STARTUP_ZTJIANBAO_TIMEOUT}s）", flush=True)
    except Exception as e:
        print(f"⚠️ 转发媒体给 {STARTUP_ZTJIANBAO_TARGET} 失败: {e}", flush=True)


def _extract_bilibili_subscription_target(response) -> str:
    text = _get_response_text(response)

    match = re.search(r"@([A-Za-z0-9_]{5,})", text)
    if match:
        return f"@{match.group(1)}"

    reply_markup = getattr(response, "reply_markup", None)
    rows = getattr(reply_markup, "rows", None) or []
    for row in rows:
        for button in getattr(row, "buttons", None) or []:
            url = (getattr(button, "url", None) or "").strip()
            if not url:
                continue

            tg_match = re.search(r"https://t\.me/([A-Za-z0-9_+]+)", url)
            if tg_match:
                name = tg_match.group(1)
                if name.startswith("+"):
                    return url
                return f"@{name}"

    return ""


async def _join_public_channel(channel_ref: str) -> bool:
    ref = (channel_ref or "").strip()
    if not ref:
        return False

    try:
        if ref.startswith("https://t.me/+"):
            invite_hash = ref.rsplit("+", 1)[-1].strip("/")
            await join(invite_hash)
            return True

        entity = await client.get_entity(ref)
        await client(JoinChannelRequest(entity))
        return True
    except Exception as e:
        msg = str(e)
        if "already" in msg.lower() or "participant" in msg.lower():
            return True
        print(f"⚠️ 订阅频道失败 ref={ref} err={e}", flush=True)
        return False
  


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
    if True:
        await GroupStatsTracker.start_background_tasks(
            offline_replay_coro=replay_offline_transactions,
            offline_interval=90   # 每 90 秒跑一次，你可以改成 300 等
        )

    

    print("🤖 ly bot 启动中(SESSION_STRING)...")

    await client.start()
    await client.catch_up()
   

    asyncio.create_task(run_taskrec_scheduler(client, poll_seconds=180))

    


    # ✅ 启动 keep-alive 背景任务（每 4 分钟并发访问一轮）
    asyncio.create_task(ping_keepalive_task())

    asyncio.create_task(thumbnail_dispatch_loop())

    


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
            await say_hello()
        except Exception as e:
            print(f"⚠️ 通知命令接收者时出错: {e}", flush=True)
            await add_contact()

    
    # await exec_pay_board_manager_salary(client=client, task={"task_value":{"source_chat_id": -1001943193056,"target_chat_id": -1003802600020,"target_thread_id": 3, "include_bots": False}})

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


'''
多加一个功能。
当userbot收到私聊消息且是属于视频时，且这个视频的caption是以 |_thumbnail_| 开头，紧接著是 file_unqiue_id
就会先把这个视频的信息存在一张新的表 (若已存在则不理会,不存在新增)
之后配合 start_background_tasks, 每隔一段时间，就检查 bot 是否是完成任务的，若是完成任务，就把还没有完成任务的 file_unqiue_id 发送给他 

因为要新建一张表
这张表有这个媒体的 file_unique_id, file_type, doc_id, access_hash, file_reference, mine_type, file_name, file_size, 
以及这个这个媒体发送给哪一个 bot_name ,发送后的 chat_id, message_id, 以及目前的状态 status (pending, working, failed, completed), 发送的时间, 以及机器人回报完成的时间, 最近的更新时间
bot 有多组机器人，可以由这个版看得出哪些机器人的最后的更新时间，知道目前这个机器人是否在 working, 若是 working 就不派给他, 若最近的状况不是 working, 就可以把这个媒体传送给他

 不建 thumbnail_bot_status, 直接从 thumbnail_task 查找
工作的bot，会以一张图来回覆 userbot 传给他媒体，当 userbot 收到时，记录这个收到的图的 doc_id, access_hash, file_reference, 收到 chat_id, 收到的 message_id, 并把状况改成 completed
'''