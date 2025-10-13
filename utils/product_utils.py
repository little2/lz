from __future__ import annotations
from typing import Optional
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import InputMediaPhoto, InputMediaDocument, InputMediaVideo, InputMediaAudio
from utils.aes_crypto import AESCrypto
from utils.tpl import Tplate
from lz_mysql import MySQLPool
from lz_config import AES_KEY
import lz_var
import json


async def submit_resource_to_chat(content_id: int, bot: Optional[Bot] = None):
    await MySQLPool.init_pool()  # ✅ 初始化 MySQL 连接池
    try:
        tpl_data = await MySQLPool.search_sora_content_by_id(int(content_id))
        review_status = await submit_resource_to_chat_action(content_id,bot,tpl_data)
        
        MySQLPool.set_product_review_status(content_id, review_status)
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

    aes = AESCrypto(AES_KEY)
    content_id_str = aes.aes_encode(content_id)
    content = None
    kb = None



    try:
        
        # print(f"tpl_data: {tpl_data}", flush=True)

        if tpl_data.get("guild_keyword"):
            from lz_db import db  # 延迟导入避免循环依赖
             # ✅ 统一在这里连一次
            await db.connect()
            keyword_id = await db.get_search_keyword_id(tpl_data["guild_keyword"])
            await db.disconnect()
        else:
            keyword_id = "-1"

        content = await Tplate.pure_text_tpl(tpl_data)

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="👀 看看先",
                url=f"https://t.me/{bot_username}?start=f_{keyword_id}_{content_id_str}"
            )
        ]])

        review_status = None
        
        # 发送到 guild 频道
        if tpl_data.get("guild_chat_id"):
            print(f"准备发送到贤师楼频道 {tpl_data['guild_chat_id']}", flush=True)
            retGuild = await _bot.send_message(
                chat_id=tpl_data["guild_chat_id"],
                message_thread_id=tpl_data.get("guild_thread_id"),
                text=content,
                parse_mode="HTML",
                reply_markup=kb
            )
            print(f"✅ 发送到公会频道", flush=True)
    except Exception as e:
        print(f"❌ 发送资源失败1: {e}", flush=True)

    await MySQLPool.init_pool()  # ✅ 初始化 MySQL 连接池
    try:
        print(f"准备发送到推播频道 {tpl_data}", flush=True)
        fee = tpl_data.get("fee", 60)

        tpl_data["text"] = content
        tpl_data["button_str"] = f"💎 兑换 ( {fee} ) - https://t.me/{bot_username}?start=f_{keyword_id}_{content_id_str}"
        tpl_data["bot_name"] = 'luzai06bot'
        tpl_data["business_type"] = 'salai'
        tpl_data["content_id"] = tpl_data.get("id")
        r = await MySQLPool.upsert_news_content(tpl_data)
        print(f"✅ 发送到推播频道 {r}", flush=True)
    except Exception as e:
        print(f"❌ 发送资源失败0: {e}", flush=True)
    finally:
        await MySQLPool.close()


    try:
        # 发送到资源频道
        if tpl_data.get("guild_resource_chat_id"):
            print(f"准备发送到资源频道 {tpl_data['guild_resource_chat_id']}", flush=True)
            retResource = await _bot.send_message(
                chat_id=tpl_data["guild_resource_chat_id"],
                message_thread_id=tpl_data.get("guild_resource_thread_id"),
                text=content,
                parse_mode="HTML",
                reply_markup=kb
            )
            review_status = 9
            
            
            # print(f"✅ 发送到资源频道 {retResource}", flush=True)
            return review_status
        
    except Exception as e:
        print(f"❌ 发送资源失败2: {e}", flush=True)
    

async def get_product_material(content_id: int):
    from lz_db import db  # 延迟导入避免循环依赖
        # ✅ 统一在这里连一次
    # await db.connect()
    rows = await db.get_album_list(content_id=int(content_id), bot_name=lz_var.bot_username)
    # print(f"album_list: {rows}", flush=True)
    
    # await db.disconnect()

   


   
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
            "show": False  # 先默认未发送；你真实发送成功后可回写 True
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
        
        

