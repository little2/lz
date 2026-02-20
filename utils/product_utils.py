from __future__ import annotations
from typing import Optional, Dict, Any, List
from aiogram import Bot

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import InputMediaPhoto, InputMediaDocument, InputMediaVideo, InputMediaAudio
from utils.aes_crypto import AESCrypto
from utils.tpl import Tplate
from lz_mysql import MySQLPool
from lz_pgsql import PGPool
from lz_config import AES_KEY,UPLOADER_BOT_NAME
import lz_var
from lz_db import db
import json
import asyncio
from aiogram.fsm.storage.base import StorageKey

class MenuBase:
    @classmethod
    async def get_menu_status(cls, state):
        data = await state.get_data()
        return data



    '''
    "current_message": product_message,
    "current_chat_id": product_message.chat.id,
    "current_messsage_id": product_message.message_id
    "fetch_thumb_file_unique_id": f"{fetch_thumb_file_unique_id}"
                    
    '''    
    
    @classmethod
    async def set_menu_status(cls, state, data: dict):
        await state.update_data(data)

        storage_data = await state.get_data()
        
        storage = state.storage
        key = StorageKey(bot_id=lz_var.bot.id, chat_id=lz_var.x_man_bot_id , user_id=lz_var.x_man_bot_id )
        # storage_data = await storage.get_data(key)
        # if data.get('fetch_thumb_file_unique_id'):
        #     storage_data["fetch_thumb_file_unique_id"] = f"{data.get('fetch_thumb_file_unique_id')}"

        # if data.get('fetch_file_unique_id'):
        #     storage_data["fetch_file_unique_id"] = f"{data.get('fetch_file_unique_id')}"


        # if data.get('current_message'):
        #     storage_data["current_message"] = data.get('current_message')

        await storage.set_data(key, storage_data)

    



    #     await MenuBase.set_menu_status(state, {
    #     "current_chat_id": menu_message.chat.id,
    #     "current_messsage_id": menu_message.message_id,
    #     "return_function": "search_list",
    #     "return_chat_id": menu_message.chat.id,
    #     "return_message_id": menu_message.message_id,
    # })


async def submit_resource_to_chat(content_id: int, bot: Optional[Bot] = None):
    await MySQLPool.ensure_pool()  # ✅ 初始化 MySQL 连接池
    try:
        tpl_data = await MySQLPool.search_sora_content_by_id(int(content_id))

        review_status_json = await submit_resource_to_chat_action(content_id,bot,tpl_data)
        review_status = review_status_json.get("review_status")
        print(f"review_status={review_status}", flush=True)
        if review_status is not None:
            await MySQLPool.set_product_review_status(content_id, review_status)
    except Exception as e:
        print(f"❌ submit_resource_to_chat error: {e}", flush=True)
    finally:
        await MySQLPool.close()

async def submit_resource_to_chat_action(content_id: int, bot: Optional[Bot] = None, tpl_data: dict = {}):
    """
    将 product 的内容提交到 guild 频道 / 资源频道。
    - bot: 可选，传入指定的 Bot；默认使用 lz_var.bot
    """
    _bot = bot or lz_var.bot

    me = await _bot.get_me()
    bot_username = me.username

    retGuild = None
    review_status = None
    content = ""


    aes = AESCrypto(AES_KEY)
    content_id_str = aes.aes_encode(content_id)
    content = None
    kb = None



    try:
        
        # print(f"tpl_data: {tpl_data}", flush=True)

        from lz_db import db  # 延迟导入避免循环依赖
            # ✅ 统一在这里连一次
        await db.connect()

        if tpl_data.get("guild_keyword"):
          
            keyword_id = await db.get_search_keyword_id(tpl_data["guild_keyword"])
            
        else:
            keyword_id = "-1"

        if tpl_data.get("product_type") == "a" or tpl_data.get("product_type") == "album":
            #TODO 找不到
            results = await db.get_album_list(content_id, lz_var.bot_username)
            if(results == []):
                await sync_album_items(content_id)
               
                
                results = await db.get_album_list(content_id, lz_var.bot_username)
            
            print(f"{results}", flush=True)
            if results:
                list_text = await Tplate.list_template(results)
                print(f"{list_text}", flush=True)
                tpl_data["album_cont_list_text"] = list_text['opt_text']

        await db.disconnect()

        content = await Tplate.pure_text_tpl(tpl_data)


        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👀 看看先",
                    url=f"https://t.me/{bot_username}?start=f_{keyword_id}_{content_id_str}"
                ),
                InlineKeyboardButton(
                    text="🐥 上传鲁馆",
                    url=f"https://t.me/{UPLOADER_BOT_NAME}?start=upload"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🏷️ 标签筛选",
                    url=f"https://t.me/{bot_username}?start=search_tag"
                )
            ],          
        ])






        review_status = None
        
        # 发送到 guild 频道
        if content and tpl_data.get("guild_chat_id"):
            print(f"🏄 准备发送到贤师楼(讨论)频道 {tpl_data['guild_chat_id']}", flush=True)
            retGuild = await _bot.send_message(
                chat_id=tpl_data["guild_chat_id"],
                message_thread_id=tpl_data.get("guild_thread_id"),
                text=content,
                parse_mode="HTML",
                reply_markup=kb
            )
            print(f"  ✅ 发送到贤师楼(讨论)频道成功", flush=True)





    except Exception as e:
        print(f"  ❌ 发送到贤师楼(讨论)频道失败1: {e}", flush=True)


    try:
        if content and tpl_data.get("guild_chat_id") != -1001943193056:
            
            retGuild = await _bot.send_message(
                chat_id=-1001943193056,
                message_thread_id=17,
                text=content,
                parse_mode="HTML",
                reply_markup=kb
            )
            print(f"  ✅ 发送到萨莱区成功", flush=True)
    
    except Exception as e:
        print(f"  ❌ 发送到萨莱区失败: {e}", flush=True)


    await MySQLPool.init_pool()  # ✅ 初始化 MySQL 连接池
    try:
        print(f"🏄 准备发送到推播频道", flush=True)
        fee = tpl_data.get("fee", lz_var.default_point)


        tpl_data["text"] = content
        tpl_data["button_str"] = f"💎 兑换 ( {fee} ) - https://t.me/{bot_username}?start=f_{keyword_id}_{content_id_str}"
        tpl_data["bot_name"] = 'luzai06bot'
        tpl_data["business_type"] = 'salai'
        tpl_data["content_id"] = tpl_data.get("id")
        r = await MySQLPool.upsert_news_content(tpl_data)
        print(f"  ✅ 发送到推播频道 {r}", flush=True)
    except Exception as e:
        print(f"  ❌ 发送资源失败0: {e}", flush=True)
    finally:
        await MySQLPool.close()


    try:
        # 发送到资源频道
        if tpl_data.get("guild_resource_chat_id"):
           
            print(
                f"🏄 准备发送到(撸馆)资源频道 C={tpl_data['guild_resource_chat_id']} "
                f"T={tpl_data.get('guild_resource_thread_id')}",
                flush=True
            )

            retResource = await _bot.send_message(
                chat_id=tpl_data["guild_resource_chat_id"],
                message_thread_id=tpl_data.get("guild_resource_thread_id"),
                text=content,
                parse_mode="HTML",
                reply_markup=kb
            )
            review_status = 9
            
            
            print(f"  ✅ 准备发送到(撸馆)资源频道成功", flush=True)
            
            # //g.guild_resource_chat_id, g.guild_resource_thread_id, g.guild_chat_id, g.guild_thread_id 
        
    except Exception as e:
        print(f"  ❌ 准备发送到(撸馆)资源频道失败2: {e}", flush=True)
    
    return {'review_status': review_status , 'result_send': retGuild}

async def get_product_material(content_id: int):
    from lz_db import db  # 延迟导入避免循环依赖
        # ✅ 统一在这里连一次
    # await db.connect()
    rows = await db.get_album_list(content_id=int(content_id), bot_name=lz_var.bot_username)    

    if rows:
        print(f"✅ get_product_material: found rows {rows}", flush=True)
        result = await build_product_material(rows)
       
        return result
    else:
        # print(f"❌ get_product_material: no rows, try sync for content_id={content_id}", flush=True)
        await sync_album_items(content_id)
        # print(f"❌ get_product_material: no rows for content_id={content_id}", flush=True)
        return await get_product_material(content_id)
        

# == 找到文件里已有的占位 ==
async def sync_album_items(content_id: int):
    """
    单向同步：以 MySQL 为源，将 album_items 同步到 PostgreSQL。
    规则：
      - MySQL 存在 → PG upsert（存在更新，不存在插入）
      - MySQL 不存在但 PG 存在 → 从 PG 删除
    """
    # 确保两端连接池已就绪（main() 里已经 connect 过的话，这里是幂等调用）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()  # ✅ 初始化 MySQL 连接池
    await PGPool.ensure_pool()  # ✅ 初始化 PostgreSQL 连接池

    # 1) 拉 MySQL 源数据
    mysql_rows = await MySQLPool.list_album_items_by_content_id(int(content_id))
    print(f"[sync_album_items] MySQL rows = {len(mysql_rows)} for content_id={content_id}", flush=True)


    # 2.1 先确保所有 member_content_id 已写入 PG.sora_content
    member_ids = sorted({int(r["member_content_id"]) for r in mysql_rows})
    if member_ids:
        # 查询 PG 已有的
        rows_pg = await PGPool.fetch(
            "SELECT id FROM sora_content WHERE id = ANY($1::bigint[])",
            member_ids
        )
        pg_have = {int(r["id"]) for r in rows_pg}
        missing = [mid for mid in member_ids if mid not in pg_have]

        # 逐一从 MySQL 拉行并 upsert 到 PG
        for mid in missing:
            row = await MySQLPool.search_sora_content_by_id(int(mid))
            if row:
                await PGPool.upsert_sora(row)


    # 2) 先做 PG 端 UPSERT
    upsert_count = await PGPool.upsert_album_items_bulk(mysql_rows)
    print(f"[sync_album_items] Upsert to PG = {upsert_count}", flush=True)

    # 3) 差异删除（PG 有而 MySQL 没有）
    keep_ids = [int(r["member_content_id"]) for r in mysql_rows] if mysql_rows else []
    deleted = await PGPool.delete_album_items_except(int(content_id), keep_ids)
    print(f"[sync_album_items] Delete extras in PG = {deleted}", flush=True)

    # 4) 小结
    summary = {
        "content_id": int(content_id),
        "mysql_count": len(mysql_rows),
        "pg_upserted": upsert_count,
        "pg_deleted": deleted,
    }
    print(f"[sync_album_items] Done: {summary}", flush=True)
    return summary

async def sync_sora(content_id: int):
    """
    单向同步：以 MySQL 为源，将 sora_contents 同步到 PostgreSQL。
    规则：
      - MySQL 存在 → PG upsert（存在更新，不存在插入）
    """
    # 确保两端连接池已就绪（main() 里已经 connect 过的话，这里是幂等调用）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()  # ✅ 初始化 MySQL 连接池
    await PGPool.ensure_pool()  # ✅ 初始化 PostgreSQL 连接池

    # 1) 拉 MySQL 源数据
    mysql_row = await MySQLPool.search_sora_content_by_id(int(content_id))
    print(f"01[sync_sora_content] MySQL row = {mysql_row} for content_id={content_id}", flush=True)

    # 2) 先做 PG 端 UPSERT
    upsert_count = 0
    if mysql_row:
        upsert_count = await PGPool.upsert_sora(mysql_row)
    print(f"[upsert_sora] Upsert to PG = {upsert_count}", flush=True)

    # 3) Album 相关的同步
    try:
        album_sync_summary = await sync_album_items(content_id)
    except Exception as e:
        print(f"[sync_sora] sync_album_items error: {e}", flush=True)
        album_sync_summary = None
    

async def sync_product_by_user(user_id: int):
    """
    单向同步：以 MySQL 为源，将某个用户的 product 记录同步到 PostgreSQL。

    规则：
      - 仅同步 owner_user_id = user_id 的记录
      - 单次最多 500 笔
      - MySQL 存在 → PG upsert（存在更新，不存在插入）

    返回示例：
    {
        "user_id": 123456789,
        "mysql_count": 10,
        "pg_upserted": 10,
    }
    """
    # 确保两端连接池已就绪（幂等）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    # 1) 从 MySQL 拉该用户的 product 记录（最多 500 笔）
    limit = 500
    mysql_rows = await MySQLPool.list_product_for_sync(
        user_id=user_id,
        limit=limit,
    )
    count_mysql = len(mysql_rows)
    print(
        f"[sync_product_by_user] MySQL rows = {count_mysql} for user_id={user_id}",
        flush=True,
    )

    if not mysql_rows:
        summary = {
            "user_id": int(user_id),
            "mysql_count": 0,
            "pg_upserted": 0,
        }
        print(f"[sync_product_by_user] Done (no data): {summary}", flush=True)
        return summary

    # 2) 批量 upsert 到 PostgreSQL
    pg_upserted = await PGPool.upsert_product_bulk_from_mysql(mysql_rows)

    summary = {
        "user_id": int(user_id),
        "mysql_count": count_mysql,
        "pg_upserted": pg_upserted,
    }
    print(f"[sync_product_by_user] Done: {summary}", flush=True)
    return summary


async def sync_cover_change(content_id: int, thumb_file_unique_id: str, thumb_file_id: str, bot_username: str):
    """
    同时更新 MySQL 和 PostgreSQL 的 product 封面图片。
    """
    # 确保两端连接池已就绪（幂等）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    content_id = int(content_id)
    
    await MySQLPool.upsert_product_thumb(
        content_id=content_id,
        thumb_file_unique_id=thumb_file_unique_id,
        thumb_file_id=thumb_file_id,
        bot_username=bot_username,
    )

    await PGPool.upsert_product_thumb(
        content_id=content_id,
        thumb_file_unique_id=thumb_file_unique_id,
        thumb_file_id=thumb_file_id,
        bot_username=bot_username,
    )

    await MySQLPool.reset_sora_media_by_id(content_id, bot_username)
    await PGPool.reset_sora_media_by_id(content_id, bot_username)



async def sync_transactions(user_id: int):
    # 1) 先看看这个 sender 在 PG 里已经同步到哪一笔
    await PGPool.init_pool()
    last_tx_id = await PGPool.get_max_transaction_id_for_sender(user_id) or 0

    # 2) 从这个 transaction_id 之后继续同步 MySQL → PG
    summary = await sync_transactions_from_mysql(
        start_transaction_id=last_tx_id,
        sender_id=user_id,
        limit=500,
    )
    print(summary)

async def sync_transactions_from_mysql(
    start_transaction_id: int,
    sender_id: int,
    limit: int = 500,
):
    """
    单向同步：以 MySQL 为源，将 transaction 记录同步到 PostgreSQL。

    规则：
      - 仅同步 transaction_id > start_transaction_id 的记录
      - 且 sender_id = 指定用户
      - 单次最多 limit 笔（默认 500）

    用法示例：
      await sync_transactions_from_mysql(0, 123456789)
      # → 从 transaction_id > 0 开始，同步 sender_id=123456789 的前 500 笔

    建议：你可以在外层 while 调用，直到 mysql_count < limit 为止，实现增量追赶。
    """
    # 1) 确保两端连接池已就绪（幂等）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    # 2) 从 MySQL 抓需要同步的记录（最多 limit 笔）
    mysql_rows = await MySQLPool.list_transactions_for_sync(
        start_transaction_id=start_transaction_id,
        sender_id=sender_id,
        limit=limit,
    )
    count_mysql = len(mysql_rows)
    print(
        f"[sync_transactions] MySQL rows = {count_mysql} "
        f"for sender_id={sender_id}, start_transaction_id={start_transaction_id}",
        flush=True,
    )

    if not mysql_rows:
        # 没有可同步的资料，直接返回
        return {
            "sender_id": int(sender_id),
            "mysql_count": 0,
            "pg_upserted": 0,
            "last_transaction_id": int(start_transaction_id),
        }

    # 3) 批量 upsert 到 PostgreSQL
    pg_upserted = await PGPool.upsert_transactions_bulk(mysql_rows)
    max_tx_id = max(int(r["transaction_id"]) for r in mysql_rows)

    summary = {
        "sender_id": int(sender_id),
        "mysql_count": count_mysql,
        "pg_upserted": pg_upserted,
        "last_transaction_id": max_tx_id,
    }
    print(f"[sync_transactions] Done: {summary}", flush=True)
    return summary


async def check_file_record(limit:int = 100):
    '''
    从 Mysql table file_records2 中取出 limit 条记录
    (1) 用 insert/update 语句插入到 mysql 的 table file_unique_id 中 , 
    file_records2.file_unique_id 对应 file_unique_id.file_unique_id,
    file_records2.file_id 对应 file_unique_id.file_id
    file_records2.file_type 对应 file_unique_id.file_type
    file_records2.bot_id 转译后对应 file_unique_id.bot (其中 bot_id:7985482732 = bot:Queue9838bot, bot_id:7629569353 = bot:stcparkbot )
    (2) 根据 file_records2.file_type, 分别维护表 video, photo, document, animation, 并以 insert/update 语句插入/更新对应的记录
    [Tabble].file_unique_id 对应各表的 file_records2.file_unique_id
    [Table].file_size 对应各表的 file_records2.file_size
    [Table].mime_type 对应各表的 file_records2.mime_type
    [Table].file_name 对应各表的 file_records2.file_name
    (3) 将 MySQL 中 table sora_content 中 sora_content.source_id = file_records2.file_unique_id 的记录, valid_state 更新为 9, stage 更新为 pending
    (4) 将 PostgreSQL 中 table sora_content 中 sora_content.source_id = file_records2.file_unique_id 的记录, valid_state 更新为 9, stage 更新为 pending
    (5) 删除 file_records2 中已经处理过的记录


    '''



    # ---------- 0) Pools ----------
    await asyncio.gather(MySQLPool.init_pool(), PGPool.init_pool())
    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    # ---------- 1) Fetch file_records2 ----------
    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await cur.execute(
            """
            SELECT
                id,
                file_unique_id,
                file_id,
                file_type,
                bot_id,
                man_id,
                file_size,
                mime_type,
                file_name
            FROM file_records2 
            WHERE process = 0
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = await cur.fetchall()
    except Exception as e:
        print(f"⚠️ [check_file_record] MySQL 查询 file_records2 出错: {e}", flush=True)
        await MySQLPool.release(conn, cur)
        return {
            "checked": 0,
            "upsert_file_ext": 0,
            "upsert_media": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": 0,
        }
    finally:
        await MySQLPool.release(conn, cur)

    if not rows:
        print("[check_file_record] file_records2 无待处理记录。", flush=True)
        return {
            "checked": 0,
            "upsert_file_ext": 0,
            "upsert_media": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": 0,
        }

    checked = len(rows)

    # ---------- 2) Helpers ----------
    BOT_ID_MAP = {
        7985482732: "Queue9838bot",
        7629569353: "stcparkbot",
    }

    def bot_name_of(bot_id) -> str:
        try:
            bid = int(bot_id) if bot_id is not None else None
        except Exception:
            bid = None
        if bid is None:
            return "unknown"
        return BOT_ID_MAP.get(bid, str(bid))

    def normalize_ft(ft: str) -> str:
        ft = (ft or "").lower().strip()
        if ft in ("v", "video"):
            return "video"
        if ft in ("a", "animation"):
            return "animation"
        if ft in ("d", "document"):
            return "document"
        if ft in ("p", "photo"):
            return "photo"
        return ""

    def safe_sid50(fu: str) -> str:
        return str(fu)[:50]  # MySQL sora_content.source_id = varchar(50); PG 也统一用 50

    def safe_fu100(fu: str) -> str:
        return str(fu)[:100]  # file_extension.file_unique_id = varchar(100)

    # ---------- 3) Build payloads ----------
    record_ids: list[int] = []
    source_ids_50: list[str] = []

    file_ext_payload = []  # (file_type, file_unique_id(100), file_id, bot, user_id)

    media_payload_v = []  # video: (fu, file_size, duration, width, height, file_name, mime_type, caption)
    media_payload_a = []  # animation
    media_payload_d = []  # document: (fu, file_size, file_name, mime_type, caption)
    media_payload_p = []  # photo: (fu, file_size, width, height, file_name, caption, root_unique_id)

    skipped_photo = 0

    # 注意：file_records2 这张表结构里没有 duration/width/height/caption/root_unique_id
    # 因此：
    # - video/animation/document 可以写 NULL（允许）
    # - photo 因 width/height NOT NULL -> 缺失只能跳过
    for r in rows:
        rid = int(r["id"])
        fu = r.get("file_unique_id")
        fid = r.get("file_id")
        if not fu or not fid:
            continue

        record_ids.append(rid)

        sid50 = safe_sid50(fu)
        source_ids_50.append(sid50)

        bot = bot_name_of(r.get("bot_id"))
        fu100 = safe_fu100(fu)

        file_ext_payload.append((
            r.get("file_type"),
            fu100,
            fid,
            bot,
            r.get("man_id"),  # 映射到 file_extension.user_id
        ))

        ft_norm = normalize_ft(r.get("file_type"))
        file_size = r.get("file_size") or 0
        mime_type = r.get("mime_type")
        file_name = r.get("file_name")

        if ft_norm == "video":
            media_payload_v.append((
                fu100,
                int(file_size),
                None,  # duration
                None,  # width
                None,  # height
                file_name,
                mime_type or "video/mp4",
                None,  # caption
            ))
        elif ft_norm == "animation":
            media_payload_a.append((
                fu100,
                int(file_size),
                None,
                None,
                None,
                file_name,
                mime_type or "video/mp4",
                None,
            ))
        elif ft_norm == "document":
            media_payload_d.append((
                fu100,
                int(file_size),
                file_name,
                mime_type,
                None,  # caption
            ))
        elif ft_norm == "photo":
            # file_records2 缺 width/height -> 必须跳过
            skipped_photo += 1
            continue

    # 去重（保持顺序）
    source_ids_50 = list(dict.fromkeys(source_ids_50))

    if not record_ids:
        return {
            "checked": checked,
            "upsert_file_ext": 0,
            "upsert_media": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": skipped_photo,
        }

    # ---------- 4) MySQL Transaction ----------
    upsert_file_ext = 0
    upsert_media = 0
    updated_mysql = 0
    deleted = 0

    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await conn.begin()

        # 4.1 upsert file_extension（UNIQUE(file_id, bot)）
        # create_time：新插入用 NOW()；重复时不强制覆盖（保留旧值），同时更新 file_type/file_unique_id/user_id
        if file_ext_payload:
            sql_ext = """
                INSERT INTO file_extension
                    (file_type, file_unique_id, file_id, bot, user_id, create_time)
                VALUES
                    (%s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    file_type      = VALUES(file_type),
                    file_unique_id = VALUES(file_unique_id),
                    user_id        = COALESCE(VALUES(user_id), user_id)
            """
            await cur.executemany(sql_ext, file_ext_payload)
            upsert_file_ext = cur.rowcount or 0

        # 4.2 upsert video/animation/document/photo（按你 DDL）
        async def _upsert_video_like(table_name: str, payload: list) -> int:
            if not payload:
                return 0
            sql = f"""
                INSERT INTO {table_name}
                    (file_unique_id, file_size, duration, width, height, file_name, mime_type, caption, create_time, update_time)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    file_size   = VALUES(file_size),
                    duration    = VALUES(duration),
                    width       = VALUES(width),
                    height      = VALUES(height),
                    file_name   = VALUES(file_name),
                    mime_type   = VALUES(mime_type),
                    caption     = VALUES(caption),
                    update_time = NOW()
            """
            await cur.executemany(sql, payload)
            return cur.rowcount or 0

        async def _upsert_document(payload: list) -> int:
            if not payload:
                return 0
            sql = """
                INSERT INTO document
                    (file_unique_id, file_size, file_name, mime_type, caption, create_time, update_time)
                VALUES
                    (%s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    file_size   = VALUES(file_size),
                    file_name   = VALUES(file_name),
                    mime_type   = VALUES(mime_type),
                    caption     = VALUES(caption),
                    update_time = NOW()
            """
            await cur.executemany(sql, payload)
            return cur.rowcount or 0

        async def _upsert_photo(payload: list) -> int:
            # 基于你当前 file_records2 缺 width/height，这里通常不会被调用
            if not payload:
                return 0
            sql = """
                INSERT INTO photo
                    (file_unique_id, file_size, width, height, file_name, caption, root_unique_id, create_time, update_time)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    file_size   = VALUES(file_size),
                    width       = VALUES(width),
                    height      = VALUES(height),
                    file_name   = VALUES(file_name),
                    caption     = VALUES(caption),
                    root_unique_id = VALUES(root_unique_id),
                    update_time = NOW()
            """
            await cur.executemany(sql, payload)
            return cur.rowcount or 0

        upsert_media += await _upsert_video_like("video", media_payload_v)
        upsert_media += await _upsert_video_like("animation", media_payload_a)
        upsert_media += await _upsert_document(media_payload_d)
        upsert_media += await _upsert_photo(media_payload_p)

        # 4.3 UPDATE MySQL sora_content（只更新已存在；不插入新行）
        # 分批避免 IN 过长
        BATCH = 500
        if source_ids_50:
            for i in range(0, len(source_ids_50), BATCH):
                batch_sids = source_ids_50[i:i + BATCH]
                placeholders = ",".join(["%s"] * len(batch_sids))
                sql_sc = f"""
                    UPDATE sora_content
                    SET valid_state = 9,
                        stage = 'pending'
                    WHERE source_id IN ({placeholders})
                """
                await cur.execute(sql_sc, tuple(batch_sids))
                updated_mysql += cur.rowcount or 0

        # 4.4 软删除本批已处理 file_records2
        if record_ids:
            for i in range(0, len(record_ids), BATCH):
                batch_ids = record_ids[i:i + BATCH]
                placeholders = ",".join(["%s"] * len(batch_ids))
                sql_del = f"UPDATE file_records2 SET process = 1 WHERE id IN ({placeholders})"
                await cur.execute(sql_del, tuple(batch_ids))


                sql_del = f"UPDATE file_records3 SET process = 1 WHERE id IN ({placeholders})"
                await cur.execute(sql_del, tuple(batch_ids))

                deleted += cur.rowcount or 0

        await conn.commit()

    except Exception as e:
        try:
            await conn.rollback()
        except Exception:
            pass
        print(f"❌ [check_file_record] MySQL 事务失败并回滚: {e}", flush=True)
        # MySQL 失败则 PG 不做更新（避免两边状态不一致）
        return {
            "checked": checked,
            "upsert_file_ext": upsert_file_ext,
            "upsert_media": upsert_media,
            "updated_mysql": updated_mysql,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": skipped_photo,
        }
    finally:
        await MySQLPool.release(conn, cur)

    # ---------- 5) PostgreSQL UPDATE (B1 only) ----------
    updated_pg = 0
    try:
        if source_ids_50:
            pg_conn = await PGPool.acquire()
            try:
                sql_pg = """
                    UPDATE public.sora_content
                    SET valid_state = 9,
                        stage = 'pending'
                    WHERE source_id = ANY($1::text[])
                """
                async with pg_conn.transaction():
                    result = await pg_conn.execute(sql_pg, source_ids_50)

                # asyncpg: "UPDATE <n>"
                try:
                    updated_pg = int(str(result).split()[-1])
                except Exception:
                    updated_pg = 0
            finally:
                await PGPool.release(pg_conn)

    except Exception as e:
        print(f"⚠️ [check_file_record] PostgreSQL UPDATE sora_content 出错: {e}", flush=True)

    summary = {
        "checked": checked,
        "upsert_file_ext": upsert_file_ext,
        "upsert_media": upsert_media,
        "updated_mysql": updated_mysql,
        "updated_pg": updated_pg,
        "deleted": deleted,
        "skipped_photo": skipped_photo,
    }
    print(f"[check_file_record] Done: {summary}", flush=True)
    return summary



async def check_and_fix_sora_valid_state(limit: int = 1000):
    """
    检查 MySQL.sora_content 中 valid_state = 1 的记录，
    通过 LEFT JOIN file_extension(file_unique_id = source_id) 判断文件是否存在：
      - 若存在 → valid_state = 9
      - 若不存在 → valid_state = 4

    并将相同的 valid_state 同步更新到 PostgreSQL.sora_content。

    :param limit: 本次最多处理多少条，避免一次性扫太大表；可多次循环调用。
    :return: 简单统计结果 dict
    """

    # 1) 确保连接池就绪
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )
    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    # -------------------------------
    # 2) 从 MySQL 抓出待处理的记录
    # -------------------------------
    conn, cur = await MySQLPool.get_conn_cursor()
    rows = []
    try:
        sql = """
            SELECT
                sc.id,
                sc.source_id,
                CASE
                    WHEN fe.file_unique_id IS NULL THEN 4
                    ELSE 9
                END AS new_valid_state
            FROM sora_content sc
            LEFT JOIN file_extension fe
                ON fe.file_unique_id = sc.source_id
            WHERE sc.valid_state = 1
            LIMIT %s
        """
        await cur.execute(sql, (int(limit),))
        rows = await cur.fetchall()
    except Exception as e:
        print(f"⚠️ [check_and_fix_sora_valid_state] MySQL 查询出错: {e}", flush=True)
        await MySQLPool.release(conn, cur)
        return {
            "checked": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
        }
    finally:
        await MySQLPool.release(conn, cur)

    if not rows:
        print("[check_and_fix_sora_valid_state] 没有 valid_state=1 的记录需要处理。", flush=True)
        return {
            "checked": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
        }

    checked_count = len(rows)

    # ------------------------------------
    # 3) 在 MySQL 中批量更新 valid_state
    # ------------------------------------
    ids_9 = [int(r["id"]) for r in rows if int(r["new_valid_state"]) == 9]
    ids_4 = [int(r["id"]) for r in rows if int(r["new_valid_state"]) == 4]

    updated_mysql = 0
    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        if ids_9:
            placeholders = ",".join(["%s"] * len(ids_9))
            sql9 = f"UPDATE sora_content SET valid_state = 9 WHERE id IN ({placeholders})"
            await cur.execute(sql9, ids_9)
            updated_mysql += cur.rowcount or 0

        if ids_4:
            placeholders = ",".join(["%s"] * len(ids_4))
            sql4 = f"UPDATE sora_content SET valid_state = 4 WHERE id IN ({placeholders})"
            await cur.execute(sql4, ids_4)
            updated_mysql += cur.rowcount or 0

            
            sql10 = f"UPDATE product SET review_status = 10 WHERE content_id IN ({placeholders})"
            await cur.execute(sql10, ids_4)
            # print(f"{ids_4} -> set review_status=10 for {cur.rowcount or 0} products", flush=True)


    except Exception as e:
        print(f"⚠️ [check_and_fix_sora_valid_state] MySQL 更新出错: {e}", flush=True)
    finally:
        await MySQLPool.release(conn, cur)

    # ----------------------------------------
    # 4) 同步到 PostgreSQL.sora_content
    # ----------------------------------------
    updated_pg = 0
    try:
        pg_conn = await PGPool.acquire()
        try:
            payload = [(int(r["id"]), int(r["new_valid_state"])) for r in rows]
            sql_pg = "UPDATE sora_content SET valid_state = $2 WHERE id = $1"

            async with pg_conn.transaction():
                await pg_conn.executemany(sql_pg, payload)

            updated_pg = len(payload)
        finally:
            await PGPool.release(pg_conn)
    except Exception as e:
        print(f"⚠️ [check_and_fix_sora_valid_state] PostgreSQL 更新出错: {e}", flush=True)

    summary = {
        "checked": checked_count,
        "updated_mysql": updated_mysql,
        "updated_pg": updated_pg,
    }

    print(f"[check_and_fix_sora_valid_state] Done: {summary}", flush=True)
    return summary




async def check_and_fix_sora_valid_state2(limit: int = 1000) -> Dict[str, Any]:
    """
    清理 sora_content.thumb_file_unique_id，并更新 MySQL + PostgreSQL：
    1) valid_state = 9 AND thumb_file_unique_id IS NOT NULL 的记录
    2) 若 thumb 不在 file_extension → 清空 thumb_file_unique_id（MySQL + PG）
       同时清空 bid.thumbnail_uid
    3) 批次将 valid_state = 8（MySQL + PG）
    """

    conn, cur = await MySQLPool.get_conn_cursor()
    summary = {"checked": 0, "fixed_thumb": 0, "bid_cleared": 0}

    try:
        # ① 取待处理记录
        await cur.execute(
            """
            SELECT id, source_id, thumb_file_unique_id
            FROM sora_content
            WHERE valid_state = 9
              AND thumb_file_unique_id IS NOT NULL
            LIMIT %s
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        if not rows:
            return summary

        summary["checked"] = len(rows)

        thumb_ids = list({r["thumb_file_unique_id"] for r in rows if r["thumb_file_unique_id"]})

        # ② 查 file_extension 是否存在
        fmt = ",".join(["%s"] * len(thumb_ids)) if thumb_ids else "'EMPTY'"
        exist_set = set()
        if thumb_ids:
            await cur.execute(
                f"SELECT file_unique_id FROM file_extension WHERE file_unique_id IN ({fmt})",
                tuple(thumb_ids),
            )
            exist_rows = await cur.fetchall()
            exist_set = {r["file_unique_id"] for r in exist_rows}

        ids_all = []
        ids_need_clear_thumb = []
        src_need_clear_bid = []

        for r in rows:
            cid = r["id"]
            src = r["source_id"]
            fu = r["thumb_file_unique_id"]

            ids_all.append(cid)
            if fu not in exist_set:
                ids_need_clear_thumb.append(cid)
                src_need_clear_bid.append(src)

        # ③ MySQL + PG 更新
        await conn.begin()

        # -------- MySQL 部分 --------
        if ids_need_clear_thumb:
            fmt = ",".join(["%s"] * len(ids_need_clear_thumb))
            await cur.execute(
                f"UPDATE sora_content SET thumb_file_unique_id=NULL WHERE id IN ({fmt})",
                tuple(ids_need_clear_thumb),
            )
            summary["fixed_thumb"] = cur.rowcount or 0

        # 清 bid.thumbnail_uid
        if src_need_clear_bid:
            src_need_clear_bid = list(set(src_need_clear_bid))
            fmt = ",".join(["%s"] * len(src_need_clear_bid))
            await cur.execute(
                f"UPDATE bid SET thumbnail_uid=NULL WHERE file_unique_id IN ({fmt})",
                tuple(src_need_clear_bid),
            )
            summary["bid_cleared"] = cur.rowcount or 0

        # 将 valid_state = 8（MySQL）
        if ids_all:
            fmt = ",".join(["%s"] * len(ids_all))
            await cur.execute(
                f"UPDATE sora_content SET valid_state=8 WHERE id IN ({fmt})",
                tuple(ids_all),
            )

        await conn.commit()
        print(f"[check_and_fix_sora_valid_state2] MySQL done: {summary}", flush=True)
        # -------- PostgreSQL 部分 --------
        # 🔥🔥🔥 NEW: 让 PG 的 sora_content 也同步清空 thumb_file_unique_id
        if ids_need_clear_thumb:
            for cid in ids_need_clear_thumb:
                await PGPool.execute(
                    "UPDATE sora_content SET thumb_file_unique_id = NULL WHERE id = $1",
                    cid
                )

        # 🔥 PG: sync valid_state = 8
        if ids_all:
            for cid in ids_all:
                await PGPool.execute(
                    "UPDATE sora_content SET valid_state = 8 WHERE id=$1",
                    cid
                )
        print(f"[check_and_fix_sora_valid_state2] PG done: {summary}", flush=True)
    except Exception as e:
        try:
            await conn.rollback()
        except:
            pass
        print(f"[check_and_fix_sora_valid_state] error: {e}", flush=True)
    finally:
        await MySQLPool.release(conn, cur)

    return summary



async def build_product_material(rows):   
    # 遍历结果
    send_group = []
    send_sub_group=[]
    lack_file_uid_rows = []
    current = None
    ready_status = True
    for item in rows:

        

        if len(send_sub_group)>=10:
            # print(f"\r\n>>> 10 items reached, sending group", flush=True)
            send_group.append(send_sub_group)
            send_sub_group=[]
            current = None
            
    
        if item["file_id"] == None:
            ready_status = False
            lack_file_uid_rows.append(item['source_id'])  
            continue

        if item["file_type"]=="p" or item["file_type"] == "v":  # photo, video
            # print(f"file_type={item["file_type"]}\r\n", flush=True)
            if current != None and current != 'pv':
                # print(f"\r\n>>> AS-IS:{current}, TO-BE:{item["file_type"]}", flush=True)
                #寄送
                send_group.append(send_sub_group)
                send_sub_group=[]  
                
        

            current = 'pv'    
            send_sub_group.append(
                InputMediaPhoto(media=item["file_id"]) if item["file_type"] == "p"
                else InputMediaVideo(media=item["file_id"])
            )
        elif item["file_type"] == "d":
            # print(f"file_type={item["file_type"]}\r\n", flush=True)
            if current != None and current != 'd':
                # print(f"\r\n>>> AS-IS:{current}, TO-BE:{item["file_type"]}", flush=True)
                #寄送
                send_group.append(send_sub_group)
                send_sub_group=[]  
            current = "d"
            send_sub_group.append(InputMediaDocument(media=item["file_id"]))
        elif item["file_type"] == "a":
            # print(f"file_type={item["file_type"]}\r\n", flush=True)
            if current != None and current != 'a':
                # print(f"\r\n>>> AS-IS:{current}, TO-BE:{item["file_type"]}", flush=True)
                #寄送
                send_group.append(send_sub_group)
                send_sub_group=[]  
            current = "a"

            send_sub_group.append(InputMediaAudio(media=item["file_id"]))

    # print(f"\r\n>>> Fin: AS-IS:{current}, TO-BE:{item["file_type"]}", flush=True)  
    send_group.append(send_sub_group)
    send_sub_group=[]
    current = None

    # 统计信息（仅统计可发送的媒体数量）
    total = sum(len(g) for g in send_group)

    # 生成分组状态 box：1-based 索引
    box = {
        i + 1: {
            "quantity": len(group),
            "show": False if i > 0 else True  # 先默认未发送；你真实发送成功后可回写 True
        }
        for i, group in enumerate(send_group)
    }


    # print(f"send_group={send_group}", flush=True)
    return {
        "ok": ready_status,
        "rows": send_group,                 # 原有：每一组用于 send_media_group
        "lack_file_uid_rows": lack_file_uid_rows,  # 原有：缺 file_id 的 source_id
        "material_status": {                         # ✅ 新增：JSON 状态说明
            "total": total,
            "box": box
        }
    }
        

async def sync_table(
    table: str,
    pk: str,
    last_ts: int,
    *,
    update_field: str = "update_at",
    limit: int = 5000,
    chunk_size: int = 1000,
) -> Dict[str, Any]:
    """
    单向同步：以 MySQL 为源，将指定 table 的增量记录同步到 PostgreSQL。
    规则：
      - 仅同步 MySQL 中 {update_field} > last_ts 的记录
      - PG 端使用 UPSERT（新增或取代）

    返回示例：
    {
        "table": "sora_content",
        "pk": "id",
        "last_ts_in": 1700000000000,
        "mysql_count": 120,
        "pg_upserted": 120,
        "max_update_at": 1700000001234,
    }
    """

    # 1) 确保两端连接池已就绪（幂等）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    table = (table or "").strip()
    pk = (pk or "").strip()
    last_ts = int(last_ts or 0)

    # 2) 从 MySQL 拉增量
    mysql_rows = await MySQLPool.fetch_records_updated_after(
        table=table,
        timestamp=last_ts,
        update_field=update_field,
        limit=limit,
    )
    mysql_count = len(mysql_rows)

    print(
        f"[sync_table] MySQL rows = {mysql_count} "
        f"for table={table}, {update_field}>{last_ts}",
        flush=True,
    )

    if not mysql_rows:
        summary = {
            "table": table,
            "pk": pk,
            "last_ts_in": last_ts,
            "mysql_count": 0,
            "pg_upserted": 0,
            "max_update_at": last_ts,
        }
        print(f"[sync_table] Done (no data): {summary}", flush=True)
        return summary

    # 3) 批量 UPSERT 到 PostgreSQL（新增或取代）
    pg_upserted = await PGPool.upsert_records_generic(
        table=table,
        pk_field=pk,
        rows=mysql_rows,
        chunk_size=chunk_size,
    )

    # 4) 计算本次最大 update_at，用于推进水位
    max_update_at = last_ts
    try:
        max_update_at = max(int(r.get(update_field) or 0) for r in mysql_rows) or last_ts
    except Exception:
        max_update_at = last_ts

    summary = {
        "table": table,
        "pk": pk,
        "last_ts_in": last_ts,
        "mysql_count": mysql_count,
        "pg_upserted": int(pg_upserted or 0),
        "max_update_at": int(max_update_at),
    }

    print(f"[sync_table] Done: {summary}", flush=True)
    return summary



async def sync_table_by_pks(
    table: str,
    pk,
    pks: List[Any],
    *,
    chunk_size: int = 1000,
    limit: int = 5000,
) -> Dict[str, Any]:
    """
    单向同步：以 MySQL 为源，根据指定主键列表 pks 同步到 PostgreSQL。
    支持单主键 / 复合主键。

    - MySQL: SELECT * FROM table WHERE pk IN (...) / WHERE (pk1,pk2) IN ((...),(...))
    - PG: UPSERT (新增或取代)

    参数示例：

    单主键：
        pk="id"
        pks=[1,2,3]

    复合主键：
        pk=["collection_id","content_id"]
        pks=[(123,"abc"), (123,"def")]
        # 也兼容 dict：
        # pks=[{"collection_id":123,"content_id":"abc"}, ...]

    返回示例：
    {
        "table": "sora_content",
        "pk": "id",
        "pks_in": 3,
        "mysql_count": 3,
        "pg_upserted": 3,
    }
    """

    # 1) 确保两端连接池已就绪（幂等）
    await asyncio.gather(
        MySQLPool.init_pool(),
        PGPool.init_pool(),
    )

    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    table = (table or "").strip()
    pks = pks or []

    # ---------- normalize pk_fields ----------
    if isinstance(pk, (list, tuple)):
        pk_fields = [str(x).strip() for x in pk if str(x).strip()]
    else:
        pk_s = (str(pk) if pk is not None else "").strip()
        if "," in pk_s:
            pk_fields = [x.strip() for x in pk_s.split(",") if x.strip()]
        else:
            pk_fields = [pk_s] if pk_s else []

    pk_label = ",".join(pk_fields) if pk_fields else ""

    # 2) 去重 + 截断（防止一次给太多 pk）
    uniq_pks: list = []
    seen = set()

    def _to_key_tuple(x):
        if isinstance(x, dict):
            return tuple(x.get(f) for f in pk_fields)
        if isinstance(x, (list, tuple)):
            if len(pk_fields) == 1:
                return (x[0],) if len(x) > 0 else (None,)
            if len(x) != len(pk_fields):
                raise ValueError(f"composite pk expects {len(pk_fields)} values, got {len(x)}")
            return tuple(x)
        return (x,)

    for item in pks:
        kt = _to_key_tuple(item)
        if any(v is None for v in kt):
            continue
        if kt in seen:
            continue
        seen.add(kt)
        uniq_pks.append(kt[0] if len(pk_fields) == 1 else tuple(kt))
        if len(uniq_pks) >= max(1, int(limit)):
            break

    print(
        f"[sync_table_by_pks] Start table={table}, pk={pk_label}, pks={len(uniq_pks)}",
        flush=True,
    )

    if not uniq_pks:
        summary = {
            "table": table,
            "pk": pk_label,
            "pks_in": 0,
            "mysql_count": 0,
            "pg_upserted": 0,
        }
        print(f"[sync_table_by_pks] Done (no pks): {summary}", flush=True)
        return summary

    # 3) MySQL 按主键拉记录
    mysql_rows = await MySQLPool.fetch_records_by_pks(
        table=table,
        pk_field=pk_fields if len(pk_fields) > 1 else pk_fields[0],
        pks=uniq_pks,
        limit=limit,
    )
    mysql_count = len(mysql_rows)

    print(
        f"[sync_table_by_pks] MySQL rows = {mysql_count} for table={table}, pks={len(uniq_pks)}",
        flush=True,
    )

    if not mysql_rows:
        summary = {
            "table": table,
            "pk": pk_label,
            "pks_in": len(uniq_pks),
            "mysql_count": 0,
            "pg_upserted": 0,
        }
        print(f"[sync_table_by_pks] Done (no rows): {summary}", flush=True)
        return summary

    # 4) PG 批量 UPSERT（新增或取代）
    pg_upserted = await PGPool.upsert_records_generic(
        table=table,
        pk_field=pk_fields if len(pk_fields) > 1 else pk_fields[0],
        rows=mysql_rows,
        chunk_size=chunk_size,
    )

    summary = {
        "table": table,
        "pk": pk_label,
        "pks_in": len(uniq_pks),
        "mysql_count": mysql_count,
        "pg_upserted": int(pg_upserted or 0),
    }

    print(f"[sync_table_by_pks] Done: {summary}", flush=True)
    return summary

    


''''''
