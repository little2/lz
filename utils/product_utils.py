from __future__ import annotations
from typing import Optional
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from utils.aes_crypto import AESCrypto
from utils.tpl import Tplate
from lz_mysql import MySQLPool
from lz_config import AES_KEY
import lz_var


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

    
    

    aes = AESCrypto(AES_KEY)
    content_id_str = aes.aes_encode(content_id)

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
                url=f"https://t.me/{lz_var.bot_username}?start=f_{keyword_id}_{content_id_str}"
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
            print(f"✅ 发送到公会频道 {retGuild}", flush=True)
            

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
        print(f"❌ 发送资源失败: {e}", flush=True)
   
        
        

