from __future__ import annotations
from typing import Optional
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import InputMediaPhoto, InputMediaDocument, InputMediaVideo, InputMediaAudio
from utils.aes_crypto import AESCrypto
from utils.tpl import Tplate
from lz_mysql import MySQLPool
from lz_pgsql import PGPool
from lz_config import AES_KEY
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

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="👀 看看先",
                url=f"https://t.me/{bot_username}?start=f_{keyword_id}_{content_id_str}"
            )
        ]])

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
        if content and tpl_data.get("guild_chat_id") != -1002675021976:
            
            retGuild = await _bot.send_message(
                chat_id=-1002675021976,
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
        result = await build_product_material(rows)
        # print(f"✅ get_product_material: got rows for content_id={content_id} {result}", flush=True)
        return result
    else:
        await sync_album_items(content_id)
        print(f"❌ get_product_material: no rows for content_id={content_id}", flush=True)
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
    

async def sync_product(user_id: int):
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
        f"[sync_product] MySQL rows = {count_mysql} for user_id={user_id}",
        flush=True,
    )

    if not mysql_rows:
        summary = {
            "user_id": int(user_id),
            "mysql_count": 0,
            "pg_upserted": 0,
        }
        print(f"[sync_product] Done (no data): {summary}", flush=True)
        return summary

    # 2) 批量 upsert 到 PostgreSQL
    pg_upserted = await PGPool.upsert_product_bulk_from_mysql(mysql_rows)

    summary = {
        "user_id": int(user_id),
        "mysql_count": count_mysql,
        "pg_upserted": pg_upserted,
    }
    print(f"[sync_product] Done: {summary}", flush=True)
    return summary




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
        
        

