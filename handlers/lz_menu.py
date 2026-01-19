import inspect
import functools
import traceback
import sys
import re
import json
from opencc import OpenCC
from typing import Any, Callable, Awaitable, Optional

from aiogram import Router, F

from aiogram.filters import Command
from aiogram.enums import ContentType
from aiogram.utils.text_decorations import markdown_decoration
from aiogram.fsm.storage.base import StorageKey
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.exceptions import TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from utils.prof import SegTimer

from aiogram.types import (
    Message,
    BufferedInputFile,
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeDefault,
    CopyTextButton,
    CallbackQuery,
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    InputMediaPhoto, 
    InputMediaVideo, 
    InputMediaDocument, 
    InputMediaAnimation
)

from aiogram.enums import ParseMode

import textwrap
from datetime import datetime, timezone, timedelta
from typing import Coroutine

import asyncio
import os
from lz_db import db
from lz_config import AES_KEY, ENVIRONMENT,META_BOT, RESULTS_PER_PAGE, KEY_USER_ID, ADMIN_IDS,UPLOADER_BOT_NAME, VALKEY_URL
import lz_var
import random
from lz_main import load_or_create_skins
import redis.asyncio as redis_async



from lz_mysql import MySQLPool
from lz_pgsql import PGPool
# from ananbot_utils import AnanBOTPool 

from utils.unit_converter import UnitConverter
from utils.aes_crypto import AESCrypto
from utils.media_utils import Media
from utils.tpl import Tplate
from utils.string_utils import LZString
from utils.product_utils import build_product_material,sync_sora,sync_product
from utils.product_utils import submit_resource_to_chat,get_product_material, MenuBase, sync_transactions
from utils.action_gate import ActionGate



from pathlib import Path

from handlers.handle_jieba_export import export_lexicon_files







router = Router()

_background_tasks: dict[str, asyncio.Task] = {}

_valkey = redis_async.from_url(VALKEY_URL, decode_responses=True)

class LZFSM(StatesGroup):
    waiting_for_title = State()
    waiting_for_description = State()
    """资源管理：直接下架原因输入"""
    waiting_unpublish_reason = State()

class RedeemFSM(StatesGroup):
    waiting_for_condition_answer = State()


async def _ensure_sora_manage_permission(callback: CallbackQuery, content_id: int) -> Optional[int]:
    """校验管理权限。

    Returns:
        owner_user_id：有权限时返回 owner_user_id；无权限则弹窗并返回 None。
    """
    try:
        record = await db.search_sora_content_by_id(int(content_id))
        owner_user_id = int(record.get("owner_user_id") or 0) if record else 0
    except Exception as e:
        print(f"❌ 读取 owner_user_id 失败: {e}", flush=True)
        await callback.answer("系统忙碌，请稍后再试。", show_alert=True)
        return None

    uid = int(callback.from_user.id)
    if uid == owner_user_id or uid in ADMIN_IDS:
        return owner_user_id

    await callback.answer("你没有权限管理这个资源。", show_alert=True)
    return None


async def _mysql_set_product_review_status_by_content_id(content_id: int, review_status: int, operator_user_id: int = 0, reason: str = "") -> None:
    """更新 MySQL product.review_status（尽量走 MySQLPool；若无专用方法则 fallback 直连执行）。"""
    await MySQLPool.ensure_pool()

    # 1) 优先走你已有的封装（若存在）
    if hasattr(MySQLPool, "set_product_review_status"):
        await getattr(MySQLPool, "set_product_review_status")(int(content_id), int(review_status), operator_user_id=operator_user_id, reason=reason)
        return


def debug(func: Callable[..., Any]):
    """
    自动捕获异常并打印出函数名、文件名、行号、错误类型、出错代码。
    同时兼容同步函数与异步函数。
    """
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def awrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                exc_type, _, exc_tb = sys.exc_info()
                tb_last = traceback.extract_tb(exc_tb)[-1]
                print("⚠️  函数执行异常捕获")
                print(f"📍 函数名：{func.__name__}")
                print(f"📄 文件：{tb_last.filename}")
                print(f"🔢 行号：{tb_last.lineno}")
                print(f"➡️ 出错代码：{tb_last.line}")
                print(f"❌ 错误类型：{exc_type.__name__}")
                print(f"💬 错误信息：{e}")
                print(f"📜 完整堆栈：\n{traceback.format_exc()}")
                # raise  # 需要外层捕获时放开
        return awrapper
    else:
        @functools.wraps(func)
        def swrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                exc_type, _, exc_tb = sys.exc_info()
                tb_last = traceback.extract_tb(exc_tb)[-1]
                print("⚠️  函数执行异常捕获")
                print(f"📍 函数名：{func.__name__}")
                print(f"📄 文件：{tb_last.filename}")
                print(f"🔢 行号：{tb_last.lineno}")
                print(f"➡️ 出错代码：{tb_last.line}")
                print(f"❌ 错误类型：{exc_type.__name__}")
                print(f"💬 错误信息：{e}")
                print(f"📜 完整堆栈：\n{traceback.format_exc()}")
                # raise
        return swrapper


def spawn_once(key: str, coro_factory: Callable[[], Awaitable[Any]]):
    """相同 key 的后台任务只跑一个；结束后自动清理。仅在需要时才创建 coroutine。"""
    task = _background_tasks.get(key)
    if task and not task.done():
        return

    async def _runner():
        try:
            # 到这里才真正创建 coroutine，避免“未 await”警告
            coro = coro_factory()
            await asyncio.wait_for(coro, timeout=60)
        except Exception:
            print(f"🔥 background task failed for key={key}", flush=True)

    t = asyncio.create_task(_runner(), name=f"backfill:{key}")
    _background_tasks[key] = t
    t.add_done_callback(lambda _: _background_tasks.pop(key, None))




# ========= 工具 =========

def _short(text: str | None, n: int = 60) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ")
    return text[:n] + ("..." if len(text) > n else "")






async def _edit_caption_or_text(
    msg: Message | None = None,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
    chat_id: int | None = None,
    message_id: int | None = None,
    photo: str | None = None,
    state: FSMContext | None = None,
):
    """
    统一编辑：
      - 若原消息有媒体：
          * 传入 photo → 用 edit_message_media 换图 + caption
          * 未传 photo → 用 edit_message_caption 仅改文字
      - 若原消息无媒体：edit_message_text
    额外规则：
      - 若要求“换媒体”但未传 photo，则尝试复用原图（仅当原媒体是 photo）
      - 若原媒体不是 photo，则回退为仅改 caption（避免类型不匹配错误）
    """
    return await Media.edit_caption_or_text(
        msg=msg,
        text=text,
        reply_markup=reply_markup,
        chat_id=chat_id,
        message_id=message_id,
        photo=photo,
        state=state,
    )
    
    # try:
    #     # print(f"text=>{text}")
    #     if msg is None and (chat_id is None or message_id is None):
    #         # 没有 msg，也没提供 chat_id/message_id，无法定位消息
    #         return

    #     if hasattr(msg, 'chat'):
    #         if chat_id is None:
    #             chat_id = msg.chat.id
    #         if message_id is None:
    #             message_id = msg.message_id
        
    #     if message_id is None:
    #         print('没有 message_id，无法定位消息', flush=True)
    #         return False


    #     # 判断是否为媒体消息（按优先顺序找出第一种存在的媒体属性）
    #     media_attr = next(
    #         (attr for attr in ["animation", "video", "photo", "document"] if getattr(msg, attr, None)),
    #         None
    #     )

    #     if media_attr:
    #         # ——————————— 有媒体的情况 ———————————
    #         if photo:
    #             # print(f"‼️ 编辑消息，换图 + caption {chat_id} {message_id}", flush=True)
    #             # 明确要换图：用传入的 photo
    #             current_message = await lz_var.bot.edit_message_media(
    #                 chat_id=chat_id,
    #                 message_id=message_id,
    #                 media=InputMediaPhoto(
    #                     media=photo,
    #                     caption=text,
    #                     parse_mode="HTML",
    #                 ),
    #                 reply_markup=reply_markup
    #             )
    #             # print(f"\n\ncurrent_message={current_message}", flush=True)
    #         else:
    #             # 未传 photo：尝试“复用原媒体”
    #             if media_attr == "photo":
    #                 print(f"‼️ 编辑消息，仅改 caption，复用原图", flush=True)
    #                 # Aiogram 的 Message.photo 是 PhotoSize 列表，取最后一项（最大尺寸）
    #                 try:
    #                     orig_photo_id = (msg.photo[-1].file_id) if getattr(msg, "photo", None) else None
    #                 except Exception:
    #                     orig_photo_id = None

    #                 if orig_photo_id:
    #                     print(f"‼️ 找到原图 ID，复用", flush=True)
    #                     # 用 edit_message_media + 原图，实现“换媒体但沿用原图 + 改 caption”
    #                     current_message =  await lz_var.bot.edit_message_media(
    #                         chat_id=chat_id,
    #                         message_id=message_id,
    #                         media=InputMediaPhoto(
    #                             media=orig_photo_id,
    #                             caption=text,
    #                             parse_mode="HTML",
    #                         ),
    #                         reply_markup=reply_markup
    #                     )
    #                 else:
    #                     print(f"⚠️ 未找到原图 ID，改为仅改 caption", flush=True)
    #                     # 兜底：拿不到原图 id，就仅改 caption
    #                     current_message = await lz_var.bot.edit_message_caption(
    #                         chat_id=chat_id,
    #                         message_id=message_id,
    #                         caption=text,
    #                         parse_mode="HTML",
    #                         reply_markup=reply_markup,
    #                     )
    #             else:
    #                 print(f"‼️ 原媒体不是 photo，仅改 caption", flush=True)
    #                 # 原媒体不是 photo（例如 animation/video/document）：
    #                 # 为避免 “can't use file of type ... as Photo” 错误，这里不强行换媒体，改为仅改 caption
    #                 current_message = await lz_var.bot.edit_message_caption(
    #                     chat_id=chat_id,
    #                     message_id=message_id,
    #                     caption=text,
    #                     parse_mode="HTML",
    #                     reply_markup=reply_markup,
    #                 )
    #     else:
    #         print(f"‼️ 编辑消息，仅改 text（无媒体）", flush=True)
    #         # ——————————— 无媒体的情况 ———————————
    #         current_message = await lz_var.bot.edit_message_text(
    #             chat_id=chat_id,
    #             message_id=message_id,
    #             text=text,
    #             reply_markup=reply_markup,
    #         )
        
    #     if state is not None:
    #         await MenuBase.set_menu_status(state, {
    #             "current_message": current_message,
    #             "current_chat_id": current_message.chat.id,
    #             "current_message_id": current_message.message_id
    #         })

    #     return current_message
    # except Exception as e:
    #     # 你也可以在这里加上 traceback 打印，或区分 TelegramBadRequest
    #     print(f"❌ 编辑消息失败a: {e}", flush=True)



async def _edit_caption_or_text2(msg : Message | None = None, *,  text: str, reply_markup: InlineKeyboardMarkup | None, chat_id: int|None = None, message_id:int|None = None, photo: str|None = None):
    """
    统一编辑：若原消息有照片 -> edit_caption；否则 -> edit_text
    """
    try:
        if chat_id is None:
            chat_id = msg.chat.id
        
        if message_id is None:
            message_id = msg.message_id

        media_attr = next(
            (attr for attr in ["animation", "video", "photo", "document"]
            if getattr(msg, attr, None)),
            None
        )

        if media_attr and photo:
            # 取出媒体对象
            media = getattr(msg, media_attr)



            await lz_var.bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(
                    media=photo,  # 或改成 media.file_id 视你的变量
                    caption=text,
                    parse_mode="HTML"
                ),
                reply_markup=reply_markup
            )

        else:
            # 没有媒体，只编辑文字
            await lz_var.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup
            )



        # if getattr(msg, "photo", None):
   

        #     if(photo!=None and lz_var.skins.get('home') and lz_var.skins['home'].get('file_id')):
        #         await lz_var.bot.edit_message_media(
        #             chat_id=chat_id,
        #             message_id=message_id,
        #             media=InputMediaPhoto(
        #                 media=photo,
        #                 caption=text,
        #                 parse_mode="HTML"
        #             ),
        #             reply_markup=reply_markup
        #         )



        #         # //CgACAgEAAxkBAAIIVmj0hnKZxG9Ti6fHNjIr5Fz5YrmHAAJwBgAC2LCpR2u0VCzFMI5PNgQ
        #     else:
        #         await lz_var.bot.edit_message_caption(
        #             chat_id=chat_id,
        #             message_id=message_id,
        #             caption=text,
        #             reply_markup=reply_markup
        #         )


        # else:
        #     await lz_var.bot.edit_message_text(
        #         chat_id=chat_id,
        #         message_id=message_id,
        #         text=text,
        #         reply_markup=reply_markup
        #     )
    except Exception as e:
        print(f"❌ 编辑消息失败b: {e}", flush=True)


@debug
async def handle_update_thumb(content_id, file_id,state):
    print(f"🏃🖼 开始取得视频的默认封面图，正在处理...{lz_var.man_bot_id}", flush=True)
    await MySQLPool.init_pool()
    await PGPool.init_pool()
    try:
        send_video_result = await lz_var.bot.send_video(chat_id=lz_var.man_bot_id, video=file_id)
        buf,pic = await Media.extract_preview_photo_buffer(send_video_result, prefer_cover=True, delete_sent=True)
        if buf and pic:
            try:
                buf.seek(0)  # ✅ 防止 read 到空

                # ✅ DB 前确保有池（双保险）
                await MySQLPool.ensure_pool()
                await PGPool.ensure_pool()

                # 上传给仓库机器人，获取新的 file_id 和 file_unique_id
                print(f"🏃🖼 上传给 {lz_var.man_bot_id} 以取得封面图的file_id", flush=True)
                newcover = await lz_var.bot.send_photo(
                    chat_id=lz_var.x_man_bot_id,
                    photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg")
                )
                    
                largest = newcover.photo[-1]
                thumb_file_id = largest.file_id
                thumb_file_unique_id = largest.file_unique_id

            
                # # 更新这个 sora_content 的 thumb_uniuque_id
                await MySQLPool.set_sora_content_by_id(content_id, {
                    "thumb_file_unique_id": thumb_file_unique_id,
                    "stage":"pending"
                })
            except Exception as e:
                print(f"⚠️🏃🖼  用缓冲图更新封面失败：{e}", flush=True)
            
            try:
                # invalidate_cached_product(content_id)
                await PGPool.upsert_product_thumb(
                    content_id, thumb_file_unique_id, thumb_file_id, lz_var.bot_username
                )

                print(f"🏃🖼 预览图更新中 {content_id} {thumb_file_unique_id} {thumb_file_id}", flush=True)
            except Exception as e:
                print(f"⚠️🏃🖼  用缓冲图更新封面失败(PostgreSQL)：{e}", flush=True)


            try:
                state_data = await MenuBase.get_menu_status(state)
     
                current_message = state_data.get("current_message")


                if current_message and hasattr(current_message, 'message_id') and hasattr(current_message, 'chat'):
                    await lz_var.bot.edit_message_media(
                            chat_id=current_message.chat.id,
                            message_id=current_message.message_id,
                            media=InputMediaPhoto(
                                media=thumb_file_id,   # 新图的 file_id
                                caption=current_message.caption,   # 保留原 caption
                                parse_mode="HTML",               # 如果原本有 HTML 格式
                            ),
                        reply_markup=current_message.reply_markup  # 保留原按钮
                    )
                    print(f"✅ [X-MEDIA] 成功更新菜单消息的缩略图", flush=True)
            except Exception as e:
                print(f"❌ [X-MEDIA] menu_message 无法更新缩略图，缺少 message_id 或 chat 信息 {current_message}", flush=True)     

        
        else:
            print(f"...⚠️🏃🖼  提取缩图失败 for content_id: {content_id}", flush=True)

    except TelegramNotFound as e:
    
        await lz_var.user_client.send_message(lz_var.bot_username, "/start")
        await lz_var.user_client.send_message(lz_var.bot_username, "[~bot~]")

        print(f"...⚠️ chat_id for content_id: {content_id}，错误：ChatNotFound", flush=True)

    except (TelegramForbiddenError) as e:
        print(f"...⚠️ TelegramForbiddenError for content_id: {content_id}，错误：{e}", flush=True)
    except (TelegramBadRequest) as e:
        await lz_var.user_client.send_message(lz_var.bot_username, "/start")
        await lz_var.user_client.send_message(lz_var.bot_username, "[~bot~]")
        print(f"...⚠️ TelegramBadRequest for content_id: {content_id}，错误：{e}", flush=True)
    except Exception as e:

        print(f"...⚠️ 失败 for content_id: {content_id}，错误：{e}", flush=True)


# == 主菜单 ==
def main_menu_keyboard():
    keyboard = [
        [
            InlineKeyboardButton(text="🔍 搜索", callback_data="search"),
            InlineKeyboardButton(text="🏆 排行", callback_data="ranking"),
        ],
    ]

    # 仅在 dev 环境显示「资源橱窗」
    if ENVIRONMENT == "dev":
        keyboard.append([
            InlineKeyboardButton(text="🪟 资源橱窗", callback_data="collection"),
            InlineKeyboardButton(text="🕑 我的历史", callback_data="my_history"),
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="🕑 我的历史", callback_data="my_history"),
        ])

    keyboard.append([
        InlineKeyboardButton(
            text="📤 上传资源",
            url=f"https://t.me/{UPLOADER_BOT_NAME}?start=upload"
        )
    ])

    keyboard.append([
        InlineKeyboardButton(
            text="🐲 小龙阳",
            url=f"https://t.me/xiaolongyang002bot?start=map"
        )
    ])



    return InlineKeyboardMarkup(inline_keyboard=keyboard)

    




    # return InlineKeyboardMarkup(inline_keyboard=[
    #     [
    #         InlineKeyboardButton(text="🔍 搜索", callback_data="search"),
    #         InlineKeyboardButton(text="🏆 排行", callback_data="ranking")
    #     ],
    #     [
    #         InlineKeyboardButton(text="🪟 资源橱窗", callback_data="collection"),
    #         InlineKeyboardButton(text="🕑 我的历史", callback_data="my_history")
    #     ],
    #     # [InlineKeyboardButton(text="🎯 猜你喜欢", callback_data="guess_you_like")],
    #     [InlineKeyboardButton(text="📤 上传资源", url=f"https://t.me/{UPLOADER_BOT_NAME}?start=upload")],
       
    # ])

# == 搜索菜单 ==
def search_menu_keyboard():
    keyboard = []

    # 仅在 dev 环境显示「关键字搜索」
    if ENVIRONMENT == "dev":
        keyboard.append(
            [InlineKeyboardButton(text="🔑 关键字搜索", callback_data="search_keyword")]
        )

    keyboard.extend([
        [InlineKeyboardButton(text="🏷️ 标签筛选", callback_data="search_tag")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# == 排行菜单 ==
def ranking_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 近期火热资源排行板", callback_data="ranking_resource")],
        [InlineKeyboardButton(text="👑 近期火热上传者排行板", callback_data="ranking_uploader")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 资源橱窗菜单 ==
def collection_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🪟 我的资源橱窗", callback_data="clt_my")],
        [InlineKeyboardButton(text="❤️ 我收藏的资源橱窗", callback_data="clt_favorite")],
        [InlineKeyboardButton(text="🛍️ 逛逛资源橱窗广场", callback_data="explore_marketplace")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])



# ========= 菜单构建 =========
def _build_clt_edit_keyboard(collection_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 资源橱窗主题", callback_data=f"clt:edit_title:{collection_id}")],
        [InlineKeyboardButton(text="📝 资源橱窗简介", callback_data=f"clt:edit_desc:{collection_id}")],
        [InlineKeyboardButton(text="👁 是否公开", callback_data=f"cc:is_public:{collection_id}")],
        [InlineKeyboardButton(text=f"🔙 返回资源橱窗信息{collection_id}", callback_data=f"clt:my:{collection_id}:0:k")]
    ])

def back_only_keyboard(back_to: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 返回上页", callback_data=back_to)]
    ])

def is_public_keyboard(collection_id: int, is_public: int | None):
    pub  = ("✔️ " if is_public == 1 else "") + "公开"
    priv = ("✔️ " if is_public == 0 else "") + "不公开"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=pub,  callback_data=f"cc:public:{collection_id}:1"),
            InlineKeyboardButton(text=priv, callback_data=f"cc:public:{collection_id}:0"),
        ],
        [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"cc:back:{collection_id}")]
    ])





# ===== 资源橱窗: 标题 =====

@router.callback_query(F.data.regexp(r"^clt:edit_title:\d+$"))
async def handle_clt_edit_title(callback: CallbackQuery, state: FSMContext):
    _, _, cid = callback.data.split(":")
    await MenuBase.set_menu_status(state, {
        "collection_id": int(cid),
        "anchor_chat_id": callback.message.chat.id,
        "anchor_msg_id": callback.message.message_id,
        "anchor_message": callback.message
    })
    # await state.update_data({
    #     "collection_id": int(cid),
    #     "anchor_chat_id": callback.message.chat.id,
    #     "anchor_msg_id": callback.message.message_id,
    #     "anchor_message": callback.message
    # })
    print(f"{callback.message.chat.id} {callback.message.message_id}")
    await state.set_state(LZFSM.waiting_for_title)
    await _edit_caption_or_text(
        callback.message,
        text="📝 请输入标题（长度 ≤ 255，可包含中文、英文或符号）：",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"clt:edit:{cid}:0:tk")]
        ]),
        state= state
    )

@router.message(LZFSM.waiting_for_title)
async def on_title_input(message: Message, state: FSMContext):
    data = await state.get_data()
    cid = int(data.get("collection_id"))
    anchor_chat_id = data.get("anchor_chat_id")
    anchor_msg_id  = data.get("anchor_msg_id")
    anchor_message = data.get("anchor_message")

    print(f"197=>{anchor_chat_id} {anchor_msg_id}")

    text = (message.text or "").strip()
    if len(text) == 0 or len(text) > 255:
        # 直接提示一条轻量回复也可以改为 alert；这里按需求删输入，所以给个轻提示再删。
        await message.reply("⚠️ 标题长度需为 1~255，请重新输入。")
        return

    # 1) 更新数据库
    await MySQLPool.update_user_collection(collection_id=cid, title=text)


    # 2) 删除用户输入的这条消息
    try:
        await lz_var.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        print(f"⚠️ 删除用户输入失败: {e}", flush=True)


    # 3) 刷新锚点消息的文本与按钮
    await _build_clt_edit(cid, anchor_message,state)
    await state.clear()

# ===== 资源橱窗 : 简介 =====

@router.callback_query(F.data.regexp(r"^clt:edit_desc:\d+$"))
async def handle_clt_edit_desc(callback: CallbackQuery, state: FSMContext):
    _, _, cid = callback.data.split(":")
    await MenuBase.set_menu_status(state, {
        "collection_id": int(cid),
        "anchor_message": callback.message,
        "anchor_chat_id": callback.message.chat.id,
        "anchor_msg_id": callback.message.message_id,
    })

    # await state.update_data({
    #     "collection_id": int(cid),
    #     "anchor_message": callback.message,
    #     "anchor_chat_id": callback.message.chat.id,
    #     "anchor_msg_id": callback.message.message_id,
    # })
    await state.set_state(LZFSM.waiting_for_description)
    await _edit_caption_or_text(
        callback.message,
        text="🧾 请输入这个资源橱窗的介绍：",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"clt:edit:{cid}:0:tk")]
        ]),
        state= state
    )



@router.message(LZFSM.waiting_for_description)
async def on_description_input(message: Message, state: FSMContext):
    data = await state.get_data()
    cid = int(data.get("collection_id"))
    anchor_chat_id = data.get("anchor_chat_id")
    anchor_msg_id  = data.get("anchor_msg_id")
    anchor_message = data.get("anchor_message")

    text = (message.text or "").strip()
    if len(text) == 0:
        await message.reply("⚠️ 介绍不能为空，请重新输入。")
        return

    # 1) 删除用户输入消息
    try:
        await lz_var.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        print(f"⚠️ 删除用户输入失败: {e}", flush=True)

    # 2) 更新数据库
    await MySQLPool.update_user_collection(collection_id=cid, description=text)

    # 3) 刷新锚点消息
    await _build_clt_edit(cid, anchor_message,state)
    await state.clear()


# ========= 资源橱窗:是否公开 =========

@router.callback_query(F.data.regexp(r"^cc:is_public:\d+$"))
async def handle_cc_is_public(callback: CallbackQuery):
    _, _, cid = callback.data.split(":")
    cid = int(cid)

    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    is_public = rec.get("is_public") if rec else None

    text = "👁 请选择这个资源橱窗是否可以公开："
    kb = is_public_keyboard(cid, is_public)  # 如果这是 async 函数，记得加 await

    # 判断是否媒体消息（photo/video/animation/document 都视为“媒体+caption”）
    is_media = bool(
        getattr(callback.message, "photo", None) or
        getattr(callback.message, "video", None) or
        getattr(callback.message, "animation", None) or
        getattr(callback.message, "document", None)
    )

    if is_media:
        await callback.message.edit_caption(text, reply_markup=kb)
    else:
        await callback.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:public:\d+:(0|1)$"))
async def handle_cc_public_set(callback: CallbackQuery, state: FSMContext   ):
    _, _, cid, val = callback.data.split(":")
    cid, is_public = int(cid), int(val)
    await MySQLPool.update_user_collection(collection_id=cid, is_public=is_public)
    
    await _build_clt_edit(cid, callback.message, state=state)

    # rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    # await callback.message.edit_reply_markup(reply_markup=is_public_keyboard(cid, rec.get("is_public")))
    await callback.answer("✅ 已更新可见性设置")

# ========= 资源橱窗:返回（从输入页回设置菜单 / 从“我的资源橱窗”回资源橱窗主菜单） =========

# 可用 clt:edit 取代 TODO
@router.callback_query(F.data.regexp(r"^cc:back:\d+$"))
async def handle_cc_back(callback: CallbackQuery,state: FSMContext):
    _, _, cid = callback.data.split(":")
    cid = int(cid)
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名资源橱窗"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    await _edit_caption_or_text(
        callback.message,
        text=f"当前设置：\n• ID：{cid}\n• 标题：{title}\n• 公开：{pub}\n• 简介：{_short(desc,120)}\n\n请选择要设置的项目：",
        reply_markup=_build_clt_edit_keyboard(cid),
        state= state
    )



# == 历史菜单 ==
def history_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton(text="📜 我的上传", callback_data="history_update:0")],
        [InlineKeyboardButton(text="💎 我的兑换", callback_data="history_redeem:0")],
    ]

    # 仅在 dev 环境显示「我的收藏资源橱窗」
    if ENVIRONMENT == "dev":
        keyboard.append(
            [InlineKeyboardButton(text="❤️ 我的收藏资源橱窗", callback_data="clt_my")]
        )

    keyboard.append(
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")]
    )

    return InlineKeyboardMarkup(inline_keyboard=keyboard)



# == 历史记录选项响应 ==
@router.callback_query(F.data.regexp(r"^(history_update|history_redeem):\d+$"))
# @router.callback_query(F.data.in_(["history_update", "history_redeem"]))
async def handle_history_update(callback: CallbackQuery, state: FSMContext):
    # print(f"handle_history_update: {callback.data}", flush=True)
    func, page_num = callback.data.split(":")
    page_num = int(page_num) or 0
    user_id = callback.from_user.id
    # page = 0

    await MenuBase.set_menu_status(state, {
        "menu_message": callback.message
    })


    # await state.update_data({
    #     "menu_message": callback.message
    # })

    if func == "history_update":
        callback_function = 'ul_pid'
        keyword_id = user_id
        photo = lz_var.skins['history_update']['file_id']
       
    elif func == "history_redeem":
        callback_function = 'fd_pid'
        keyword_id = user_id
        photo = lz_var.skins['history_redeem']['file_id']
   
   

    pg_result = await _build_pagination(callback_function, keyword_id, page_num, state=state)
    if not pg_result.get("ok"):
        await callback.answer(pg_result.get("message"), show_alert=True)
        return

    await _edit_caption_or_text(
        photo=photo,
        msg=callback.message,
        text=pg_result.get("text"), 
        reply_markup =pg_result.get("reply_markup"),
        state= state
    )

    # await callback.message.reply(
    #     text=pg_result.get("text"), parse_mode=ParseMode.HTML,
    #     reply_markup =pg_result.get("reply_markup")
    # )

    await callback.answer()


async def render_results(results: list[dict], search_key_id: int , page: int , total: int, per_page: int = 10, callback_function: str = "") -> str:
    total_pages = (total + per_page - 1) // per_page
    lines = []
    stag = "f"
    if callback_function in {"pageid"}:
        stag = "f"
        keyword = await db.get_keyword_by_id(search_key_id)
        lines = [
            f"<b>🔍 关键词：</b> <code>{keyword}</code>\r\n"
        ]
    elif callback_function in {"fd_pid"}:
        stag = "fd"
        
    elif callback_function in {"ul_pid"}:
        stag = "ul"
        
    
    
    for r in results:
        # print(r)
        content = _short(r["content"]) or r["id"]
        # 根据 r['file_type'] 进行不同的处理
        if r['file_type'] == 'v':
            icon = "🎬"
        elif r['file_type'] == 'd':
            icon = "📄"
        elif r['file_type'] == 'p':
            icon = "🖼"
        elif r['file_type'] == 'a':
            icon = "📂"
        else:
            icon = "🔹"


        aes = AESCrypto(AES_KEY)
        encoded = aes.aes_encode(r['id'])

        lines.append(
            f"{icon}<a href='https://t.me/{lz_var.bot_username}?start={stag}_{search_key_id}_{encoded}'>{content}</a>"
            # f"<b>Type:</b> {r['file_type']}\n"
            # f"<b>Source:</b> {r['source_id']}\n"
            # f"<b>内容:</b> {content}"
        )

    

    # 页码信息放到最后
    lines.append(f"\n<b>📃 第 {page + 1}/{total_pages} 页（共 {total} 项）</b>")


    return "\n".join(lines)  # ✅ 强制变成纯文字


@router.callback_query(
    F.data.regexp(r"^(ul_pid|fd_pid|pageid)\|")
)
async def handle_pagination(callback: CallbackQuery, state: FSMContext):
    callback_function, keyword_id_str, page_str = callback.data.split("|")
    keyword_id = int(keyword_id_str) 
    page = int(page_str)

    print(f"Pagination: {callback_function}, {keyword_id}, {page}", flush=True)

    if callback_function == "ul_pid":
        photo = lz_var.skins['history_update']['file_id']
    elif callback_function == "fd_pid":
        photo = lz_var.skins['history_redeem']['file_id']
    elif callback_function == "pageid":
        photo = lz_var.skins['search_keyword']['file_id']
    else:
        photo = lz_var.skins['home']['file_id']

    

    pg_result = await _build_pagination(callback_function, keyword_id, page, state=state)
    # print(f"pg_result: {pg_result}", flush=True)
    if not pg_result.get("ok"):
        await callback.answer(pg_result.get("message"), show_alert=True)
        return

    await callback.answer()

    current_message = await _edit_caption_or_text(
        photo=photo,
        msg=callback.message,
        text=pg_result.get("text"),
        reply_markup=pg_result.get("reply_markup"),
        state= state
    )

    await MenuBase.set_menu_status(state, {
        "fetch_thumb_file_unique_id": "fetch_thumb_file_unique_id",
        "fetch_file_unique_id": "fetch_file_unique_id"
    })

    


async def _build_pagination_action(callback_function:str, search_key_index:str, page:int, state: FSMContext):

    product_info = {}
    keyword_id = int(search_key_index)
    print(f"Pagination: {callback_function}, {keyword_id}, {page}", flush=True)

    if callback_function == "ul_pid":
        photo = lz_var.skins['history_update']['file_id']
    elif callback_function == "fd_pid":
        photo = lz_var.skins['history_redeem']['file_id']
    elif callback_function == "pageid":
        photo = lz_var.skins['search_keyword']['file_id']
    else:
        photo = lz_var.skins['home']['file_id']


    pg_result = await _build_pagination(callback_function, keyword_id, page, state=state)
    # print(f"pg_result: {pg_result}", flush=True)
    if not pg_result.get("ok"):
        
        return
    product_info['ok'] = "1"
    product_info['cover_file_id'] = photo
    product_info['caption'] = pg_result.get("text")
    product_info['reply_markup'] = pg_result.get("reply_markup")


    return product_info




    
async def _prefetch_sora_media_for_results(state: FSMContext, result: list[dict]):
    """
    基于整批 result 做 sora_media 预加载：
      - 使用 PGPool.cache 记录：
          1) 每个 content_id 的 sora_media 状态
          2) 每个 file_unique_id 是否已经发起过 fetch
      - 整个函数会被 spawn_once 包装在后台执行，不阻塞主流程。
    """
    if state is None or not result:
        return

    id_to_fuid: dict[int, str] = {}
    id_to_tfuid: dict[int, str] = {}


    try:
        # 1) 从 result 收集 content_id → file_unique_id 映射
        id_to_fuid: dict[int, str] = {}
        for sc in result:
            cid = sc.get("id") or sc.get("content_id")
            fuid = sc.get("source_id") or sc.get("file_unique_id")
            tfuid = sc.get("thumb_file_unique_id")

            if not cid or not fuid:
                continue

            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue

            if cid_int not in id_to_fuid:
                id_to_fuid[cid_int] = fuid
                if tfuid:
                    id_to_tfuid[cid_int] = tfuid

        if not id_to_fuid:
            return

        bot_name = getattr(lz_var, "bot_username", "unknown_bot")

        # 2) 找出哪些 content_id 缓存里还没有，需要打 PG
        to_query: list[int] = []
        if PGPool.cache:
            for cid_int in id_to_fuid.keys():
                cache_key = f"pg:sora_media:{bot_name}:{cid_int}"
                entry = PGPool.cache.get(cache_key)
                if entry is None:
                    to_query.append(cid_int)
        else:
            # 没有 cache 可用，就全部查一次
            to_query = list(id_to_fuid.keys())

        # 3) 对 to_query 打一次 PG，查询 sora_media
        if to_query:
            await PGPool.init_pool()
            await PGPool.ensure_pool()

            rows = await PGPool.fetch(
                """
                SELECT content_id, file_id, thumb_file_id
                FROM sora_media
                WHERE source_bot_name = $1
                  AND content_id = ANY($2::bigint[])
                """,
                bot_name,
                to_query,
            )

            # 先把查到的写入 cache
            seen: set[int] = set()
            for r in rows or []:
                try:
                    cid_int = int(r["content_id"])
                except (TypeError, ValueError):
                    continue

                fuid = id_to_fuid.get(cid_int)
                cache_key = f"pg:sora_media:{bot_name}:{cid_int}"

                # 已经有没有关系，后面 set 会覆盖
                entry = {
                    "file_id": r.get("file_id"),
                    "thumb_file_id": r.get("thumb_file_id"),
                    "file_unique_id": fuid,
                    "thumb_file_unique_id": id_to_tfuid.get(cid_int),
                    "requested": False,
                }

                # 如果这个 fuid 已经被标记发起过 fetch，则同步 requested 状态
                if fuid and PGPool.cache:
                    prefetch_key = f"pg:sora_prefetch_fuid:{fuid}"
                    if PGPool.cache.get(prefetch_key):
                        entry["requested"] = True

                if PGPool.cache:
                    PGPool.cache.set(cache_key, entry, ttl=1800)

                seen.add(cid_int)

            # 对于没任何 sora_media 记录的 content_id，也建一个空 entry，避免下次再查 PG
            for cid_int in to_query:
                if cid_int in seen:
                    continue
                fuid = id_to_fuid.get(cid_int)
                cache_key = f"pg:sora_media:{bot_name}:{cid_int}"
                entry = {
                    "file_id": None,
                    "thumb_file_id": None,
                    "file_unique_id": fuid,
                    "thumb_file_unique_id": id_to_tfuid.get(cid_int),
                    "requested": False,
                }
                if fuid and PGPool.cache:
                    prefetch_key = f"pg:sora_prefetch_fuid:{fuid}"
                    if PGPool.cache.get(prefetch_key):
                        entry["requested"] = True
                if PGPool.cache:
                    PGPool.cache.set(cache_key, entry, ttl=3600)

        # 4) 从 cache 里找出需要 fetch 的 candidates（最多 RESULTS_PER_PAGE 个）
        tasks: list[asyncio.Task] = []
        started = 0

        for cid_int, fuid in id_to_fuid.items():
            if not fuid:
                continue



            cache_key = f"pg:sora_media:{bot_name}:{cid_int}"
            entry = PGPool.cache.get(cache_key) if PGPool.cache else None

            if entry is None:
                # 理论上不会发生（前面已经写入），但保险处理
                entry = {
                    "file_id": None,
                    "thumb_file_id": None,
                    "file_unique_id": fuid,
                    "requested": False,
                }

            # 已经有完整的 file_id + thumb_file_id → 不需要 fetch
            if entry.get("file_id") and entry.get("thumb_file_id"):
                # 重新写回，刷新 TTL 即可
                if PGPool.cache:
                    PGPool.cache.set(cache_key, entry, ttl=1800)
                continue

            # 已经发起过 fetch（不论成功与否），避免重复请求
            prefetch_key = f"pg:sora_prefetch_fuid:{fuid}"
            already_prefetched = PGPool.cache.get(prefetch_key) if PGPool.cache else None
            if already_prefetched or entry.get("requested"):
                # 刷新一下缓存 TTL
                if PGPool.cache:
                    PGPool.cache.set(cache_key, entry, ttl=1800)
                continue

            # 还没请求过 → 标记并发起 fetch
            entry["file_unique_id"] = fuid
            entry["requested"] = True
            if PGPool.cache:
                PGPool.cache.set(cache_key, entry, ttl=1800)
                PGPool.cache.set(prefetch_key, True, ttl=3600)

            async def _one_fetch(fuid: str = fuid):
                try:
                    # _thumb_file_unique_id = already_prefetched['thumb_file_unique_id'] 
                    thumb_fuid = (entry or {}).get("thumb_file_unique_id")
                    # 暂停 1 秒
                    await asyncio.sleep(1.0)
                   
                    await Media.fetch_file_by_file_uid_from_x(state=None, ask_file_unique_id=fuid, timeout_sec=10.0)
                    if thumb_fuid:
                        await Media.fetch_file_by_file_uid_from_x(state=None, ask_file_unique_id=thumb_fuid, timeout_sec=10.0)
                    # 成功后，真正的 file_id / thumb_file_id 会被写回 sora_media。
                    # 以后再触发预加载时，PG 查询 + cache 会拿到最新状态。
                except Exception as e:
                    print(f"[prefetch] fetch_file_by_file_uid_from_x failed for {fuid}: {e}", flush=True)

            tasks.append(asyncio.create_task(_one_fetch()))
            started += 1

            if started >= (int(RESULTS_PER_PAGE/2)-1):
                break

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        print(f"[prefetch] _prefetch_sora_media_for_results error: {e}", flush=True)



async def _build_pagination(
    callback_function,
    keyword_id: int | None = -1,
    page: int | None = 0,
    state: FSMContext | None = None,
):
    keyword = ""
    if callback_function in {"pageid"}:
        # 用 keyword_id 查回 keyword 文本
        
        
        keyword = await db.get_keyword_by_id(keyword_id)
      
        if not keyword:
            return {"ok": False, "message": "⚠️ 无法找到对应关键词"}
            
        result = await db.search_keyword_page_plain(keyword)
        if not result:
            return {"ok": False, "message": "⚠️ 没有找到任何结果"}
    elif callback_function in {"fd_pid"}:
       
        spawn_once(
            f"sync_transactions:{keyword_id}",
            lambda: sync_transactions(keyword_id)
        )
        result = await PGPool.search_history_redeem(keyword_id)
        if not result:
            return {"ok": False, "message": "⚠️ 同步正在进行中，或是您目前还没有任何兑换纪录"}
    elif callback_function in {"ul_pid"}:
        spawn_once(
            f"sync_product:{keyword_id}",
            lambda: sync_product(keyword_id)
        )

        result = await PGPool.search_history_upload(keyword_id)
        if not result:
            return {"ok": False, "message": "⚠️ 同步正在进行中，或是您目前还没有任何上传纪录"}            


    # === 正常分页 ===# === 背景进行文件的同步 (预加载) ===
    # 使用「整批 result」而不只是当前页：
    # 1) 从 result 提取 (content_id, source_id=file_unique_id)
    # 2) 用 content_id 列表去查 sora_media 里现状
    #    - 条件：source_bot_name = 当前 bot
    # 3) 找出下列这些 content_id：
    #    - 没有 sora_media 记录，或
    #    - 有记录但 file_id 为空，或
    #    - 有记录但 thumb_file_id 为空
    # 4) 从这些候选中，最多挑 RESULTS_PER_PAGE 个，
    #    用 spawn_once + Media.fetch_file_by_file_uid_from_x(state, file_unique_id, 10)
    #    并用 file_unique_id 做 key 标记，避免重复请求
        # === 背景进行文件的同步（预加载） ===
    # 把整块预加载逻辑丢到 spawn_once，让主线程只负责分页与渲染，不被 PG / X 仓库拖慢。
    print(f"Prefetch sora_media for pagination: {callback_function}, {keyword_id}", flush=True)
    if state is not None and result:
        print(f"Starting prefetch task...", flush=True)
        # 用 callback_function + keyword_id 当 key，避免同一批结果被重复开启预加载任务
        key = f"prefetch_sora_media:{callback_function}:{keyword_id}"
        # 注意要把 result 拷贝成 list，避免外面后续修改它
        snapshot = list(result)

        #预加载任务
        spawn_once(
            key,
            lambda state=state, snapshot=snapshot: _prefetch_sora_media_for_results(state, snapshot),
        )

    # === 正常分页 ===


    start = page * RESULTS_PER_PAGE
    end = start + RESULTS_PER_PAGE
    sliced = result[start:end]
    has_next = end < len(result)
    has_prev = page > 0
    
    text = await render_results(sliced, keyword_id, page, total=len(result), per_page=RESULTS_PER_PAGE, callback_function=callback_function)

    reply_markup=build_pagination_keyboard(keyword_id, page, has_next, has_prev, callback_function)

    return {"ok": True, "text": text, "reply_markup": reply_markup}



def build_pagination_keyboard(keyword_id: int, page: int, has_next: bool, has_prev: bool,
                              callback_function: str | None = "pageid") -> InlineKeyboardMarkup:
    """
    分页键盘（两行）
    第一行：上一页 / 下一页
    第二行：返回、刷新
    """
    keyboard: list[list[InlineKeyboardButton]] = []

    # 第一行：分页按钮
    page_buttons: list[InlineKeyboardButton] = []
    if has_prev:
        page_buttons.append(InlineKeyboardButton(text=f"⬅️ 上一页", callback_data=f"{callback_function}|{keyword_id}|{page - 1}"))
    if has_next:
        page_buttons.append(InlineKeyboardButton(text=f"➡️ 下一页", callback_data=f"{callback_function}|{keyword_id}|{page + 1}"))
    if page_buttons:
        keyboard.append(page_buttons)


    
    # 第二行：自定义按钮（随意扩展）
    if callback_function in {"ul_pid", "fd_pid"}:
        page_buttons: list[InlineKeyboardButton] = []
        page_buttons.append(InlineKeyboardButton(text="🔙 返回我的历史", callback_data=f"my_history"))
        keyboard.append(page_buttons)
    elif callback_function in {"pageid"}:
        page_buttons: list[InlineKeyboardButton] = []
        page_buttons.append(InlineKeyboardButton(text="🔙 返回搜寻", callback_data=f"search"))
        keyboard.append(page_buttons)

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# == 猜你喜欢菜单 ==
def guess_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 查看推荐资源", callback_data="view_recommendations")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 资源上传菜单 ==
def upload_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 上传资源", url=f"https://t.me/{UPLOADER_BOT_NAME}?start=upload")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])


# == 启动指令 == # /id 360242
@router.message(Command("id"))
async def handle_search_by_id(message: Message, state: FSMContext, command: Command = Command("id")):
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        # ✅ 调用并解包返回的三个值
        result = await load_sora_content_by_id(int(args[1]), state)
        

        ret_content, file_info, purchase_info = result
        source_id = file_info[0] if len(file_info) > 0 else None
        file_type = file_info[1] if len(file_info) > 1 else None
        file_id = file_info[2] if len(file_info) > 2 else None
        thumb_file_id = file_info[3] if len(file_info) > 3 else None
        owner_user_id = purchase_info[0] if purchase_info[0] else None
        fee = purchase_info[1] if purchase_info[1] else None


        # ✅ 检查是否找不到资源（根据返回第一个值）
        if ret_content.startswith("⚠️"):
            await message.answer(ret_content, parse_mode="HTML")
            return

        # ✅ 发送带封面图的消息
        await message.answer_photo(
            photo=thumb_file_id,
            caption=ret_content,
            parse_mode="HTML"
            
        )

        print(f"🔍 完成，file_id: {file_id}, thumb_file_id: {thumb_file_id}, owner_user_id: {owner_user_id}",flush=True)
        if not file_id and source_id:
            print("❌ 没有找到 file_id",flush=True)
            await MySQLPool.fetch_file_by_file_uid(source_id)
            print(f"🔍 完成",flush=True)


@router.message(Command("reload"))
async def handle_reload(message: Message, state: FSMContext, command: Command = Command("reload")):
    lz_var.skins = await load_or_create_skins(if_del=True)
    await message.answer("🔄 皮肤配置已重新加载。")


@router.message(Command("s"))
async def handle_search_s(message: Message, state: FSMContext, command: Command = Command("s")):
    # 删除 /s 这个消息
    try:
        await message.delete()
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter) as e:
        print(f"❌ 删除 /s 消息失败: {e}", flush=True)
    pass

    if ENVIRONMENT != "dev":
        print("🔍 搜索指令已禁用（仅限开发环境）", flush=True)
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("请输入关键词： /s 正太 钢琴")
        return
    
    keyword = parts[1]

    # 太短的直接挡掉（避免搜一堆 “的/在/是”）
    if len(keyword) < 2:
        await message.answer("关键词再具体一点会更好哦（至少 2 个字）")
        return

    # 限制最大长度，避免恶意灌长字串
    if len(keyword) > 50:
        keyword = keyword[:50]

    await db.insert_search_log(message.from_user.id, keyword)
    result = await db.upsert_search_keyword_stat(keyword)

    tw2s = OpenCC('tw2s')
    keyword = tw2s.convert(keyword)
    print(f"🔍 搜索关键词: {keyword}", flush=True)

    await handle_search_component(message, state, keyword)
    
    # keyword_id = await db.get_search_keyword_id(keyword)

    # list_info = await _build_pagination(callback_function="pageid", keyword_id=keyword_id, state=state)
    # if not list_info.get("ok"):
    #     msg = await message.answer(list_info.get("message"))
    #     # ⏳ 延迟 5 秒后自动删除
    #     await asyncio.sleep(5)
    #     try:
    #         await msg.delete()
    #     except Exception as e:
    #         print(f"❌ 删除提示消息失败: {e}", flush=True)
    #     return

    # date = await MenuBase.get_menu_status(state)


    # # print(f"handle_message={handle_message}",flush=True)

    # if date and date.get("current_message"):
    #     try:
    #         await _edit_caption_or_text(
    #             photo=lz_var.skins['search_keyword']['file_id'],
    #             msg=date.get("current_message"),
    #             text=list_info.get("text"),
    #             reply_markup=list_info.get("reply_markup"),
    #             state= state
    #         )
    #         return
    #     except Exception as e:
    #         print(f"❌ 编辑消息失败c: {e}", flush=True)
            
    # menu_message = await message.answer_photo(
    #     photo=lz_var.skins['search_keyword']['file_id'],
    #     caption=list_info.get("text"),
    #     parse_mode="HTML",
    #     reply_markup=list_info.get("reply_markup"),
    # )

    # await MenuBase.set_menu_status(state, {
    #     "current_chat_id": menu_message.chat.id,
    #     "current_message_id": menu_message.message_id,
    #     "current_message": menu_message,
    #     "return_function": "search_list",
    #     "return_chat_id": menu_message.chat.id,
    #     "return_message_id": menu_message.message_id,
    # })


@router.message(Command("setcommand"))
async def handle_set_comment_command(message: Message, state: FSMContext):

    await lz_var.bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
    await lz_var.bot.delete_my_commands(scope=BotCommandScopeAllPrivateChats())
    await lz_var.bot.delete_my_commands(scope=BotCommandScopeDefault())
    await lz_var.bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="首页菜单"),
            # BotCommand(command="s", description="使用搜索"),
            BotCommand(command="search_tag", description="标签筛选"),
            # BotCommand(command="post", description="创建资源夹(一个投稿多个资源)"),
            # BotCommand(command="sub", description="订阅通知"),
            # BotCommand(command="me", description="查看积分"),
            BotCommand(command="rank", description="排行"),
            # BotCommand(command="all", description="所有文件"),
            # BotCommand(command="like", description="收藏文件"),
            # BotCommand(command="migrate_code", description="获取迁移码")
        ],
        scope=BotCommandScopeAllPrivateChats()
    )
    print("✅ 已设置命令列表", flush=True)
   
async def handle_search_component(message: Message, state: FSMContext, keyword:str):  
    keyword_id = await db.get_search_keyword_id(keyword)
    list_info = await _build_pagination(callback_function="pageid", keyword_id=keyword_id, state=state)
    if not list_info.get("ok"):
        msg = await message.answer(list_info.get("message"))
        # ⏳ 延迟 5 秒后自动删除
        await asyncio.sleep(5)
        try:
            await msg.delete()
        except Exception as e:
            print(f"❌ 删除提示消息失败: {e}", flush=True)
        return

    date = await MenuBase.get_menu_status(state)


    # print(f"handle_message={handle_message}",flush=True)

    if date and date.get("current_message"):
        try:
            await _edit_caption_or_text(
                photo=lz_var.skins['search_keyword']['file_id'],
                msg=date.get("current_message"),
                text=list_info.get("text"),
                reply_markup=list_info.get("reply_markup"),
                state= state
            )
            return
        except Exception as e:
            print(f"❌ 编辑消息失败c: {e}", flush=True)
            
    menu_message = await message.answer_photo(
        photo=lz_var.skins['search_keyword']['file_id'],
        caption=list_info.get("text"),
        parse_mode="HTML",
        reply_markup=list_info.get("reply_markup"),
    )

    await MenuBase.set_menu_status(state, {
        "current_chat_id": menu_message.chat.id,
        "current_message_id": menu_message.message_id,
        "current_message": menu_message,
        "return_function": "search_list",
        "return_chat_id": menu_message.chat.id,
        "return_message_id": menu_message.message_id,
    })

# == 启动指令 ==
@debug
@router.message(Command("start"))
async def handle_start(message: Message, state: FSMContext, command: Command = Command("start")):
    # 删除 /start 这个消息
    try:
        if message.text and message.text == "/start":
            pass
        else:
            await message.delete()
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter) as e:
        print(f"❌ 删除 /start 消息失败: {e}", flush=True)


    user_id = message.from_user.id


    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        param = args[1].strip()
        parts = param.split("_")
        if args[1] == "search_tag":
            await handle_search_tag_command(message, state)
        elif parts[0] == "rci":    #remove_collect_item
            date = await state.get_data()
            clt_id = date.get("collection_id")
            handle_message = date.get("message")
  
            print(f"State data: {date}", flush=True)
            
            content_id = parts[1]
            page = int(parts[2]) or 0
            await MySQLPool.remove_content_from_user_collection(int(clt_id), int(content_id))

            result = await _get_clti_list(clt_id,page,user_id,"list")
               
            if result.get("success") is False:
                await message.answer("这个资源橱窗暂时没有收录文件", show_alert=True)
                return

            await _edit_caption_or_text(
                handle_message,
                text=result.get("caption"),
                reply_markup=result.get("reply_markup"),
                state= state
            )
            print(f"删除资源橱窗项目 ID: {content_id} {page} {clt_id}")
            pass
        elif parts[0] == "clt":    #remove_collection_item
            collection_id = parts[1]
            print(f"437>{collection_id}")
            collection_info  = await _build_clt_info(cid=collection_id, user_id=user_id, mode='view', ops='handle_clt_fav')
            print(f"439>{collection_info}")

            # await message.answer_photo(
            #     photo=product_info['cover_file_id'],
            #     caption=product_info['caption'],
            #     parse_mode="HTML",
            #     reply_markup=product_info['reply_markup'])


            if collection_info.get("success") is False:
                await message.answer(collection_info.get("message"), show_alert=True)
                return
            elif collection_info.get("photo"):
                # await callback.message.edit_media(media=collection_info.get("photo"), caption=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))
                

                await lz_var.bot.send_photo(
                    chat_id=user_id,
                    caption = collection_info.get("caption"),
                    photo=collection_info.get("photo"),
                    reply_markup=collection_info.get("reply_markup"),
                    parse_mode="HTML")
                

    
                
                return
            else:
                await message.edit_text(text=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))


            pass
        elif (parts[0] in ["f", "fd", "ul", "cm", "cf"]):
            content_id = 0
            search_key_index = parts[1]
            encoded = "_".join(parts[2:])  # 剩下的部分重新用 _ 拼接
            
            if encoded: 
                print(f"🔍 搜索关键字索引: {search_key_index}, 编码内容: {encoded}")
                # encoded = param[2:]  # 取第三位开始的内容
                try:
                    aes = AESCrypto(AES_KEY)
                    content_id_str = aes.aes_decode(encoded) or None
                    if content_id_str:
                        content_id = int(content_id_str)  # ✅ 关键修正

                    # date = await state.get_data()
                    # clti_message = date.get("menu_message")
                    state_data = await MenuBase.get_menu_status(state)
                    # current_message = state_data.get("current_message") if state_data else None
                    current_message = None
                except Exception as e:
                    # tb = traceback.format_exc()
                    notify_msg=await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                    # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                    spawn_once(f"notify_msg:{notify_msg.message_id}",lambda: Media.auto_self_delete(notify_msg, 7))
                    print(f"❌ 解密失败A：{e}", flush=True)



            try:
                caption_txt = "🔍 正在从院长的硬盘搜索这个资源，请稍等片刻...ㅤㅤㅤㅤㅤㅤㅤ." 
                if parts[0]!="f" and current_message and hasattr(current_message, 'message_id') and hasattr(current_message, 'chat'):
                    try:
                        
                        # print(f"clti_message={current_message}",flush=True)
                        current_message = await lz_var.bot.edit_message_media(
                            chat_id=current_message.chat.id,
                            message_id=current_message.message_id,
                            media=InputMediaAnimation(
                                media=lz_var.skins["loading"]["file_id"],
                                caption=caption_txt,
                                parse_mode="HTML"
                            )
                        )
                        
                            
                        # return
                    except Exception as e:
                        print(f"❌ 编辑消息失败d: {e}", flush=True)
                        current_message = await message.answer_animation(
                            animation=lz_var.skins["loading"]["file_id"],  # 你的 GIF file_id 或 URL
                            caption=caption_txt,
                            parse_mode="HTML",
                            protect_content=True
                        )
                else:   
                    current_message = await message.answer_animation(
                        animation=lz_var.skins["loading"]["file_id"],  # 你的 GIF file_id 或 URL
                        caption=caption_txt,
                        parse_mode="HTML",
                        protect_content=True
                    )

                    # print(f"clti_message={clti_message}",flush=True)
                
                await MenuBase.set_menu_status(state, {
                    "current_message": current_message,
                    "current_chat_id": current_message.chat.id,
                    "current_message_id": current_message.message_id
                })

            except Exception as e:
                # tb = traceback.format_exc()
                notify_msg=await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                spawn_once(f"notify_msg:{notify_msg.message_id}",lambda: Media.auto_self_delete(notify_msg, 7))
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                print(f"❌ 解密失败B：{e}", flush=True)


                # //
  

           
            
            try:
                if (parts[0] in ["f","fd", "ul", "cm", "cf"]):
                    
                    if parts[0] == "f" and content_id == 0:
                        print(f"encoded==>{encoded} {content_id}")
                        product_info = await _build_pagination_action('pageid', search_key_index, 0, state)
                        
                    else:
                        viewer_user_id=int(message.from_user.id)
                        product_info = await _build_product_info(content_id, search_key_index, state=state, message=message, search_from=parts[0], viewer_user_id=viewer_user_id)
                   
            except Exception as e:
               
                # tb = traceback.format_exc()
                notify_msg=await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                spawn_once(f"notify_msg:{notify_msg.message_id}",lambda: Media.auto_self_delete(notify_msg, 7))
                print(f"❌ 解密失败C：{e}", flush=True)
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                try:
                    if content_id > 0:
                        await sync_sora(content_id)
                except Exception as e2:
                    print(f"❌ 解密失败D：{e2}", flush=True)
                
                return

            try:
                print(f"688:Product Info", flush=True)
                if product_info and product_info['ok']:
                    if (parts[0] in ["f","fd", "ul", "cm", "cf"]):
                        # date = await state.get_data()
                        # clti_message = date.get("menu_message")
                        try:
                            if current_message and hasattr(current_message, 'message_id') and hasattr(current_message, 'chat'):
                               
                                current_message =  await _edit_caption_or_text(
                                    photo=product_info['cover_file_id'],
                                    msg =current_message,
                                    text=product_info['caption'],
                                    reply_markup=product_info['reply_markup'],
                                    state= state
                                )



                                return
                            else:
                                print(f"⚠️ 无法编辑消息，clti_message 不存在或无效", flush=True)
                               
                        except Exception as e:
                            print(f"❌ 编辑消息失败e: {e}", flush=True)
                    

                    print(f"‼️ 不应该执行到这段，要查",flush=True)
                    product_message = await message.answer_photo(
                        photo=product_info['cover_file_id'],
                        caption=product_info['caption'],
                        parse_mode="HTML",
                        reply_markup=product_info['reply_markup'])
                
                    # storage = state.storage
                    # key = StorageKey(bot_id=lz_var.bot.id, chat_id=lz_var.x_man_bot_id , user_id=lz_var.x_man_bot_id )
                    # storage_data = await storage.get_data(key)
                    # storage_data["menu_message"] = product_message
                    # await storage.set_data(key, storage_data)

                    await MenuBase.set_menu_status(state, {
                        "current_message": product_message,
                        "current_chat_id": product_message.chat.id,
                        "current_message_id": product_message.message_id
                    })


                else:
                    await message.answer(product_info['msg'], parse_mode="HTML")
                    return
            except Exception as e:
                # tb = traceback.format_exc()
                notify_msg=await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                spawn_once(f"notify_msg:{notify_msg.message_id}",lambda: Media.auto_self_delete(notify_msg, 7))
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                print(f"❌ 解密失败D：{e}", flush=True)

        elif parts[0] == "post":
            await _submit_to_lg()
        elif parts[0] == "upload":
            await message.answer(f"📦 请直接上传图片/视频/文件", parse_mode="HTML")
        else:
            await message.answer(f"📦 你提供的参数是：`{param}`", parse_mode="HTML")
    else:

        current_message = await message.answer_photo(
                photo=lz_var.skins['home']['file_id'],
                caption="👋 欢迎使用 LZ 机器人！请选择操作：",
                parse_mode="HTML",
                reply_markup=main_menu_keyboard()
        )              
        # await message.answer("👋 欢迎使用 LZ 机器人！请选择操作：", reply_markup=main_menu_keyboard())
        await MenuBase.set_menu_status(state, {
            "current_chat_id": current_message.chat.id,
            "current_message_id": current_message.message_id,
            "current_message": current_message,
            "menu_message": current_message
        })

        # await state.update_data({
        #     "menu_message": menu_message
        # })


def get_index_by_source_id(search_result: list[dict], source_id: str, one_based: bool = False) -> int:
    """返回该 source_id 在 search_result 中的下标；找不到返回 -1。
       one_based=True 时返回排名从 1 开始。"""
    idx = next((i for i, r in enumerate(search_result) if r.get("source_id") == source_id), -1)
    return (idx + 1) if (one_based and idx != -1) else idx

async def _build_product_info(content_id :int , search_key_index: str, state: FSMContext, message: Message, search_from : str = 'search', current_pos:int = 0, viewer_user_id: int | None = None):
    aes = AESCrypto(AES_KEY)
    encoded = aes.aes_encode(content_id)

    stag = "f"
    if search_from == 'cm':   
        stag = "cm"
    elif search_from == 'cf':   
        stag = "cf"
    elif search_from == 'fd':   
        stag = "fd"
    elif search_from == 'ul':   
        stag = "ul"
    else:
        stag = "f"
    
    shared_url = f"https://t.me/{lz_var.bot_username}?start={stag}_{search_key_index}_{encoded}"

    # print(f"message_id: {message.message_id}")

    await MenuBase.set_menu_status(state, {
        "collection_id": int(content_id),
        "search_key_index": search_key_index,
        "search_from": search_from,
        "current_pos": int(current_pos),
        "current_content_id": int(content_id),
        'message': message,
        'action':'_build_product_info'
    })

    # await state.update_data({
    #     "collection_id": int(content_id),
    #     "search_key_index": search_key_index,
    #     'message': message,
    #     'action':'_build_product_info'
    # })
    # print(f"_build_product_info: {content_id}, {search_key_index}, {search_from}, {current_pos}", flush=True)
    # ✅ 调用并解包返回的三个值
    
    result_sora = await load_sora_content_by_id(content_id, state, search_key_index, search_from)
    
    print(f"result_sora==>{result_sora}", flush=True)

    ret_content, file_info, purchase_info = result_sora
    source_id = file_info[0] if len(file_info) > 0 else None
    file_type = file_info[1] if len(file_info) > 1 else None
    file_id = file_info[2] if len(file_info) > 2 else None
    thumb_file_id = file_info[3] if len(file_info) > 3 else None

    owner_user_id = purchase_info[0] if purchase_info[0] else None
    fee = purchase_info[1] if purchase_info[1] else 0
    search_result = []
    
    # print(f"thumb_file_id:{thumb_file_id}")
    # ✅ 检查是否找不到资源（根据返回第一个值）
    if ret_content.startswith("⚠️"):
        return {"ok": False, "msg": ret_content}
    
    
    print(f"current_pos1={current_pos}")
    if int(search_key_index)>0:
        
        if stag == "f":
        # 尝试从搜索结果中定位当前位置
            
            keyword = await db.get_keyword_by_id(int(search_key_index))
            if keyword:
                print(f"🔍 取得搜索结果以定位当前位置: {keyword}", flush=True)
                search_result = await db.search_keyword_page_plain(keyword)
        elif stag == "cm" or stag == 'cf':  
            search_result = await MySQLPool.get_clt_files_by_clt_id(search_key_index)
        elif stag == 'fd':
            # 我的兑换   
            search_result = await PGPool.search_history_redeem(search_key_index)
        elif stag == 'ul':   
            search_result = await PGPool.search_history_upload(search_key_index)
            
        else:
            stag = "f"
    
            
            
        if search_result and current_pos<=0:
            try:
                current_pos = get_index_by_source_id(search_result, source_id) 
                print(f"搜索结果总数: {len(search_result)}", flush=True)
            except Exception as e:
                print(f"❌ 取得索引失败：{e}", flush=True)
    
    
   
    if file_id or file_type in ['album', 'a']:
        resource_icon = "💎"
    else:
        resource_icon = "🔄"

    print(f"current_pos2={current_pos}")

    discount_amount = int(fee * lz_var.xlj_discount_rate)
    xlj_final_price = fee - discount_amount
    

    # ==== 形成翻页按钮 ====

    # 取得总数（你已经有 search_result）
    total = len(search_result) if search_result else 0
    has_prev = current_pos > 0
    has_next = current_pos < total - 1

    print(f"{current_pos} / {total} | has_prev={has_prev} | has_next={has_next}", flush=True)

    nav_row = []

    if has_prev:
        nav_row.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"sora_page:{search_key_index}:{current_pos}:-1:{search_from}"
            )
        )

    nav_row.append(
        InlineKeyboardButton(
            text=f"{resource_icon} {fee}",
            callback_data=f"sora_redeem:{content_id}"
        )
    )

    if has_next:
        nav_row.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"sora_page:{search_key_index}:{current_pos}:1:{search_from}"
            )
        )

    reply_markup = InlineKeyboardMarkup(inline_keyboard=[nav_row])


    reply_markup.inline_keyboard.append(
        [
            InlineKeyboardButton(
                text=f"{resource_icon} {xlj_final_price} (小懒觉会员)",
                callback_data=f"sora_redeem:{content_id}:xlj"
            )
        ]
    ) 


    page_num = int(int(current_pos) / RESULTS_PER_PAGE) or 0

    if search_from == "cm":
        reply_markup.inline_keyboard.append(
            [
                InlineKeyboardButton(text="🔙 回资源橱窗", callback_data=f"clti:list:{search_key_index}:0"),
            ]
        )
    elif search_from == "cf":
        reply_markup.inline_keyboard.append(
            [
                InlineKeyboardButton(text="🔙 回资源橱窗", callback_data=f"clti:flist:{search_key_index}:0"),
            ]
        )    
    elif search_from == "ul":
        reply_markup.inline_keyboard.append(
            [
                InlineKeyboardButton(text="📂 回我的上传", callback_data=f"history_update:{page_num}"),
            ]
        )    
    elif search_from == "fd":
        reply_markup.inline_keyboard.append(
            [
                InlineKeyboardButton(text="💠 回我的兑换", callback_data=f"history_redeem:{page_num}"),
            ]
        )    
    else:

        if int(search_key_index) > 0:
            reply_markup.inline_keyboard.append(
                [
                    InlineKeyboardButton(text="🔙 返回搜索结果", callback_data=f"pageid|{search_key_index}|{page_num}"),
                ]
            )
        else:
            if ENVIRONMENT == "dev":
                reply_markup.inline_keyboard.append(
                    [
                        InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                    ]
                )
    
    bottom_row = []
    bottom_row.append(
        InlineKeyboardButton(text="🔗 复制资源链结", copy_text=CopyTextButton(text=shared_url))
    )

    if ENVIRONMENT == "dev":
        bottom_row.append(
            InlineKeyboardButton(text="➕ 加入资源橱窗", callback_data=f"add_to_collection:{content_id}:0")
        ) 

    reply_markup.inline_keyboard.append(bottom_row)
    

    viewer_id = (
        int(viewer_user_id)
        if viewer_user_id is not None
        else int(message.from_user.id)
    )


    if ((viewer_id == owner_user_id) or (viewer_id in ADMIN_IDS)):
        # 如果是资源拥有者，添加编辑按钮
        if viewer_id == owner_user_id:
            role_tag = "（你是拥有者）"
        else:
            role_tag = "（你是管理员）"

        reply_markup.inline_keyboard.append(
            [
                InlineKeyboardButton(text=f"⚙️ 管理 {role_tag}", callback_data=f"sora_operation:{content_id}")
            ]
        )
    


    return {'ok': True, 'caption': ret_content, 'file_type':'photo','cover_file_id': thumb_file_id, 'reply_markup': reply_markup}



@router.callback_query(F.data.startswith("sora_operation:"))
async def handle_sora_operation_entry(callback: CallbackQuery, state: FSMContext):
    # 解析 content_id
    try:
        _, content_id_str = callback.data.split(":", 1)
        content_id = int(content_id_str)
        db.cache.delete(f"sora_content_id:{content_id}")

       
        spawn_once(
            f"sync_sora:{content_id}",
            lambda: sync_sora(content_id)
        )

        
    except Exception as e:
        print(f"❌ sora_operation entry failed: {e}", flush=True)
        await callback.answer("参数错误", show_alert=True)
        return

    

    # 权限校验（owner 或 ADMIN）
    owner_user_id = await _ensure_sora_manage_permission(callback, content_id)
    if owner_user_id is None or not owner_user_id:
        return



    # 记录返回所需信息（可选：用于返回上一页时重建）
    data = await state.get_data()
    search_key_index = data.get("search_key_index")  # 你现有搜索流程会写这个
    await state.update_data(
        sora_op_content_id=content_id,
        sora_op_owner_user_id=owner_user_id,
        sora_op_search_key_index=search_key_index,
    )

    record = await db.search_sora_content_by_id(int(content_id))
    print(f"{record}",flush=True)

    text = (
        "请选择你要处理的方式:\n\n"
        "<b>📝 退回编辑:</b>\n"
        "当你需要修改内容说明或积分设定时，请选择此项。\n"
        "资源将被切换到编辑模式，完成修改后需重新提交审核，审核通过后才能再次上架。\n\n"
        "<b>🛑 直接下架:</b>\n"
        "资源将立即下架，所有用户将无法存取。\n"
        "若日后需要重新上架，需先将状态改为「退回编辑」，并完成编辑与审核流程。\n"
    )

    rows_kb: list[list[InlineKeyboardButton]] = []

    if record.get("review_status") in [1]:
        aes = AESCrypto(AES_KEY)
        encoded = aes.aes_encode(content_id)
    
        rows_kb.append([
            InlineKeyboardButton(text="📝 编辑", url=f"https://t.me/{UPLOADER_BOT_NAME}?start=r_{encoded}")
        ])
    else:
        rows_kb.append([
            InlineKeyboardButton(text="📝 退回编辑", callback_data=f"sora_op:return_edit:{content_id}")
        ])

    rows_kb.append([
        InlineKeyboardButton(text="🛑 直接下架", callback_data=f"sora_op:unpublish:{content_id}")
    ])

    rows_kb.append([
        InlineKeyboardButton(text="💪 强制更新", callback_data=f"force_update:{content_id}")
    ])

    rows_kb.append([
        InlineKeyboardButton(text="🔙 返回", callback_data=f"sora_op:back:{content_id}")
    ])


    kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)



    await _edit_caption_or_text(callback.message, text=text, reply_markup=kb, state=state)
    await callback.answer()


@router.callback_query(F.data.startswith("sora_op:return_edit:"))
async def handle_sora_op_return_edit(callback: CallbackQuery, state: FSMContext):
    try:
        _, _, content_id_str = callback.data.split(":", 2)
        content_id = int(content_id_str)
        
        aes = AESCrypto(AES_KEY)
        encoded = aes.aes_encode(content_id)
    except Exception:
        await callback.answer("参数错误", show_alert=True)
        return

    owner_user_id = await _ensure_sora_manage_permission(callback, content_id)
    if owner_user_id is None or not owner_user_id:
        return

    try:
        await _mysql_set_product_review_status_by_content_id(content_id, 1, operator_user_id=callback.from_user.id, reason="退回编辑")
        await sync_sora(content_id)
        db.cache.delete(f"sora_content_id:{content_id}")
        
    except Exception as e:
        print(f"❌ sora_op return_edit failed: {e}", flush=True)
        await callback.answer("操作失败，请稍后再试", show_alert=True)
        return

    

    rows_kb: list[list[InlineKeyboardButton]] = []

    rows_kb.append([
        InlineKeyboardButton(text="📝 编辑", url=f"https://t.me/{UPLOADER_BOT_NAME}?start=r_{encoded}")
    ])

    kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)

    tip = f"✅ 已退回编辑。\n请使用 @{UPLOADER_BOT_NAME} 修改资料，修改完成后需重新审核才能上架。"

    # 退回后仍停留在管理页，或你也可以直接返回上一页（这里先停留）
    await _edit_caption_or_text(callback.message, text=tip, reply_markup=kb, state=state)
    await callback.answer("已退回编辑", show_alert=False)

@router.callback_query(F.data.startswith("sora_op:unpublish:"))
async def handle_sora_op_unpublish_prompt(callback: CallbackQuery, state: FSMContext):
    try:
        _, _, content_id_str = callback.data.split(":", 2)
        content_id = int(content_id_str)
    except Exception:
        await callback.answer("参数错误", show_alert=True)
        return

    owner_user_id = await _ensure_sora_manage_permission(callback, content_id)
    if owner_user_id is None or not owner_user_id:
        return

    # 发一条“输入原因”的提示消息（你要求：有取消按钮、取消则删除该消息）
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ 取消", callback_data="sora_op:cancel_unpublish")]
    ])
    prompt = await callback.message.answer("请输入下架原因（可输入一句话）：", reply_markup=kb)

    # 进入等待原因状态，并记录 prompt_message_id 用于删除
    await state.update_data(
        sora_op_content_id=content_id,
        sora_op_unpublish_prompt_chat_id=prompt.chat.id,
        sora_op_unpublish_prompt_message_id=prompt.message_id,
    )
    await state.set_state(LZFSM.waiting_unpublish_reason)
    await callback.answer()


@router.callback_query(F.data == "force_update")
async def handle_sora_op_force_update(callback: CallbackQuery, state: FSMContext):
    try:
        _, content_id_str = callback.data.split(":", 1)
        content_id = int(content_id_str)
    except Exception:
        await callback.answer("参数错误", show_alert=True)
        return

    # 不强制权限也行（但管理页回上一页通常仍在 owner/admin 手里）
    await sync_sora(int(content_id))
    await callback.answer("更新同步中，请在 1 分钟后再试", show_alert=False)

@router.callback_query(F.data == "sora_op:cancel_unpublish")
async def handle_sora_op_cancel_unpublish(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    msg_id = data.get("sora_op_unpublish_prompt_message_id")
    chat_id = data.get("sora_op_unpublish_prompt_chat_id")

    # 删除提示消息（按你要求）
    try:
        if chat_id and msg_id:
            await lz_var.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception as e:
        print(f"❌ delete unpublish prompt failed: {e}", flush=True)

    # 清状态
    data = await state.get_data()
    # 删除提示消息略...
    await state.update_data(
        sora_op_unpublish_prompt_chat_id=None,
        sora_op_unpublish_prompt_message_id=None,
    )
    await state.set_state(None)   # 或者 state.clear() 但你要先把需要的 key 保存回去
    await callback.answer("已取消", show_alert=False)


@router.message(LZFSM.waiting_unpublish_reason)
async def handle_sora_op_unpublish_reason(message: Message, state: FSMContext):
    reason = (message.text or "").strip()
    if not reason:
        await message.reply("原因不能为空，请重新输入；或点取消。")
        return

    data = await state.get_data()
    content_id = int(data.get("sora_op_content_id") or 0)
    if not content_id:
        await message.reply("找不到 content_id，已取消本次操作。")
        await state.clear()
        return

    # 再做一次权限校验（message 进来时也要挡）
    try:
        record = await db.search_sora_content_by_id(int(content_id))
        owner_user_id = int(record.get("owner_user_id") or 0) if record else 0
    except Exception as e:
        print(f"❌ 读取 owner_user_id 失败: {e}", flush=True)
        await message.reply("系统忙碌，请稍后再试。")
        return

    uid = int(message.from_user.id)
    if not (uid == owner_user_id or uid in ADMIN_IDS):
        await message.reply("你没有权限执行此操作。")
        await state.clear()
        return

    # 写 review_status=20 并同步
    try:
        
        db.cache.delete(f"sora_content_id:{content_id}")
        print(f"🛑 用户 {uid} 为 content_id={content_id} 直接下架，原因：{reason}", flush=True)

        await _mysql_set_product_review_status_by_content_id(content_id, 20, operator_user_id=uid, reason=reason)
        await sync_sora(content_id)
        db.cache.delete(f"sora_content_id:{content_id}")
        
    except Exception as e:
        print(f"❌ sora_op unpublish failed: {e}", flush=True)
        await message.reply("下架失败，请稍后再试。")
        return

    # 删除“输入原因”提示消息（按你要求）
    try:
        chat_id = data.get("sora_op_unpublish_prompt_chat_id")
        msg_id = data.get("sora_op_unpublish_prompt_message_id")
        if chat_id and msg_id:
            await lz_var.bot.delete_message(chat_id=int(chat_id), message_id=int(msg_id))
    except Exception as e:
        print(f"❌ delete unpublish prompt failed: {e}", flush=True)

    await state.clear()

    await message.reply(
        "✅ 已直接下架。\n"
        f"下架原因：{reason}\n"
        "若要重新上架，需要将状态改成退回编辑。"
    )

@router.callback_query(F.data.startswith("sora_op:back:"))
async def handle_sora_op_back(callback: CallbackQuery, state: FSMContext):
    try:
        _, _, content_id_str = callback.data.split(":", 2)
        content_id = int(content_id_str)
    except Exception:
        await callback.answer("参数错误", show_alert=True)
        return

    # 不强制权限也行（但管理页回上一页通常仍在 owner/admin 手里）
    data = await state.get_data()
    search_key_index = data.get("sora_op_search_key_index") or 0

    try:
        product_info = await _build_product_info(
            content_id=content_id,
            search_key_index=search_key_index,
            state=state,
            message=callback.message,
            viewer_user_id=int(callback.from_user.id),
        )
        await _edit_caption_or_text(
            msg=callback.message,
            text=product_info.get("caption"),
            reply_markup=product_info.get("reply_markup"),
            photo=product_info.get("cover_file_id"),
            state=state
        )
    except Exception as e:
        print(f"❌ sora_op back failed: {e}", flush=True)
        await callback.answer("返回失败", show_alert=True)
        return

    await callback.answer()





@router.message(Command("post"))
async def handle_post(message: Message, state: FSMContext, command: Command = Command("post")):
    # 删除 /post 这个消息
    try:
        await message.delete()
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter) as e:
        print(f"❌ 删除 /post 消息失败: {e}", flush=True)

    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        content_id = args[1].strip()

        await submit_resource_to_chat(int(content_id))
        

    else:
        await message.answer("👋 欢迎使用 LZ 机器人！请选择操作：", reply_markup=main_menu_keyboard())
        pass


async def _submit_to_lg():
    try:
        product_rows = await MySQLPool.get_pending_product()
        if not product_rows:
            print("📭 没有找到待送审的 product", flush=True)
            return

        for row in product_rows:
            content_id = row.get("content_id")
            if not content_id:
                continue
            print(f"🚀 提交 content_id={content_id} 到 LG", flush=True)
            await submit_resource_to_chat(int(content_id))

    except Exception as e:
        print(f"❌ _submit_to_lg 执行失败: {e}", flush=True)


async def build_add_to_collection_keyboard(user_id: int, content_id: int, page: int) -> InlineKeyboardMarkup:
    # 复用你现成的 _load_collections_rows()
    rows, has_next = await _load_collections_rows(user_id=user_id, page=page, mode="mine")
    kb_rows: list[list[InlineKeyboardButton]] = []

    if not rows:
        # 没有资源橱窗就引导创建
        kb_rows.append([InlineKeyboardButton(text="➕ 创建资源橱窗", callback_data="clt:create")])
        kb_rows.append([InlineKeyboardButton(text="🔙 返回资源橱窗菜单", callback_data="clt_my")])
        return InlineKeyboardMarkup(inline_keyboard=kb_rows)

    # 本页 6 条资源橱窗选择按钮
    for r in rows:
        cid = r.get("id")
        title = (r.get("title") or "未命名资源橱窗")[:30]
        kb_rows.append([
            InlineKeyboardButton(
                text=f"🪟 {title}  #ID{cid}",
                callback_data=f"choose_collection:{cid}:{content_id}:{page}"
            )
        ])

    # 翻页
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"add_to_collection:{content_id}:{page-1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="下一页 ➡️", callback_data=f"add_to_collection:{content_id}:{page+1}"))
    if nav:
        kb_rows.append(nav)

    # 返回
    kb_rows.append([InlineKeyboardButton(text="🔙 返回资源橱窗菜单", callback_data="clt_my")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)



@router.callback_query(F.data.regexp(r"^add_to_collection:(\d+):(\d+)$"))
async def handle_add_to_collection(callback: CallbackQuery, state: FSMContext):
    _, content_id_str, page_str = callback.data.split(":")
    content_id = int(content_id_str)
    page = int(page_str)
    user_id = callback.from_user.id

    # 统计用户资源橱窗数量 & 取第一个资源橱窗ID
    count, first_id = await MySQLPool.get_user_collections_count_and_first(user_id=user_id)

    if count == 0:
        # 自动创建一个默认资源橱窗并加入
        new_id = await MySQLPool.create_default_collection(user_id=user_id, title="未命名资源橱窗")
        if not new_id:
            await callback.answer("创建资源橱窗失败，请稍后再试", show_alert=True)
            return

        ok = await MySQLPool.add_content_to_user_collection(collection_id=new_id, content_id=content_id)
        tip = "✅ 已为你创建资源橱窗并加入" if ok else "资源橱窗已创建，但加入失败"
        await callback.answer(tip, show_alert=False)
        # 也可以顺手把按钮切到“我的资源橱窗”：
        # kb = await build_add_to_collection_keyboard(user_id=user_id, content_id=content_id, page=0)
        # await _edit_caption_or_text(callback.message, text="你的资源橱窗：", reply_markup=kb)
        return

    if count == 1 and first_id:
        # 直接加入唯一资源橱窗
        ok = await MySQLPool.add_content_to_user_collection(collection_id=first_id, content_id=content_id)
        tip = "✅ 已加入你的唯一资源橱窗" if ok else "⚠️ 已在该资源橱窗里或加入失败"
        await callback.answer(tip, show_alert=False)
        return

    # 多个资源橱窗 → 弹出分页选择
    kb = await build_add_to_collection_keyboard(user_id=user_id, content_id=content_id, page=page)
    await _edit_caption_or_text(
        callback.message,
        text=f"请选择要加入的资源橱窗（第 {page+1} 页）：",
        reply_markup=kb,
        state= state
    )
    await callback.answer()


# 选择某个资源橱窗 → 写入 user_collection_file（去重）
@router.callback_query(F.data.regexp(r"^choose_collection:(\d+):(\d+):(\d+)$"))
async def handle_choose_collection(callback: CallbackQuery, state: FSMContext):
    _, cid_str, content_id_str, page_str = callback.data.split(":")
    collection_id = int(cid_str)
    content_id = int(content_id_str)
    page = int(page_str)
    user_id = callback.from_user.id
    data = await state.get_data()
    search_key_index = data.get('search_key_index')

    ok = await MySQLPool.add_content_to_user_collection(collection_id=collection_id, content_id=content_id)
    if ok:
        tip = "✅ 已加入该资源橱窗"
    else:
        tip = "⚠️ 已在该资源橱窗里或加入失败"

    viewer_user_id = int(callback.from_user.id)
    product_info = await _build_product_info(content_id=content_id, search_key_index=search_key_index, state=state, message=callback.message, viewer_user_id=viewer_user_id)
    # 保持在选择页，方便继续加入其他资源橱窗
    # kb = await build_add_to_collection_keyboard(user_id=user_id, content_id=content_id, page=page)
    try:
        await callback.message.edit_reply_markup(reply_markup=product_info['reply_markup'])
    except Exception as e:
        print(f"❌ 刷新加入资源橱窗页失败: {e}", flush=True)

    await callback.answer(tip, show_alert=False)


# == 主菜单选项响应 ==
@router.callback_query(F.data == "search")
async def handle_search(callback: CallbackQuery,state: FSMContext):
    await _edit_caption_or_text(
        photo=lz_var.skins['search']['file_id'],
        msg=callback.message,
        text="👋 请选择操作：", 
        reply_markup=search_menu_keyboard(),
        state= state
    )

def back_search_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 返回搜索", callback_data="search")],
    ])

    
@router.callback_query(F.data == "ranking")
async def handle_ranking(callback: CallbackQuery,state: FSMContext):

    await _edit_caption_or_text(
        photo=lz_var.skins['ranking']['file_id'],
        msg=callback.message,
        text="排行榜", 
        reply_markup=ranking_menu_keyboard(),
        state= state
    )  

@router.message(Command("rank"))
async def handle_ranking_command(message: Message, state: FSMContext, command: Command = Command("rank")):
    product_message = await lz_var.bot.send_photo(
        chat_id=message.chat.id,
        photo=lz_var.skins['ranking']['file_id'],
        caption="排行榜", 
        parse_mode="HTML",
        reply_markup=ranking_menu_keyboard()
    )

    await MenuBase.set_menu_status(state, {
        "current_message": product_message,
        "current_chat_id": product_message.chat.id,
        "current_message_id": product_message.message_id
    })


@router.callback_query(F.data == "collection")
async def handle_collection(callback: CallbackQuery):


    await lz_var.bot.edit_message_media(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        media=InputMediaPhoto(
            media=lz_var.skins['clt_menu']['file_id'],
            caption="👋 资源橱窗菜单！请选择操作：",
            parse_mode="HTML"
        ),
        reply_markup=collection_menu_keyboard()
    )


    # await callback.message.answer_photo(
    #     photo=lz_var.skins['clt_menu']['file_id'],
    #     caption="👋 资源橱窗菜单！请选择操作：",
    #     parse_mode="HTML",
    #     reply_markup=collection_menu_keyboard())   


    # await callback.message.edit_reply_markup(reply_markup=collection_menu_keyboard())

@router.callback_query(F.data == "my_history")
async def handle_my_history(callback: CallbackQuery,state: FSMContext):
    
    await _edit_caption_or_text(
        photo=lz_var.skins['history']['file_id'],
        msg=callback.message,
        text="👋 这是你的历史记录菜单！请选择操作：", 
        reply_markup=history_menu_keyboard(),
        state= state
    )


   

@router.callback_query(F.data == "guess_you_like")
async def handle_guess_you_like(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=guess_menu_keyboard())

@router.callback_query(F.data == "upload_resource")
async def handle_upload_resource(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=upload_menu_keyboard())

# == 搜索选项响应 ==
@router.callback_query(F.data == "search_keyword")
async def handle_search_keyword(callback: CallbackQuery,state: FSMContext):
    await MenuBase.set_menu_status(state, {
        "current_chat_id": callback.message.chat.id,
        "current_message_id": callback.message.message_id,
        "menu_message": callback.message
    })
    # await state.update_data({
    #     "menu_message": callback.message
    # })
    text = textwrap.dedent('''\
        <b>搜索使用方法</b>
        /s + 关键词1 + 关键词2

        <b>⚠️ 注意</b>:
        • /s 与关键词之间需要空格
        • 多个关键词之间需要空格
        • 最多支持10个字符
    ''')
    await _edit_caption_or_text(
        photo=lz_var.skins['search_keyword']['file_id'],
        msg=callback.message,
        text=text, 
        reply_markup=back_search_menu_keyboard(),
        state= state
    )

async def check_valid_key(message) -> bool:
    user_id = message.from_user.id
    action = "search_tag"
    key = f"{action}:{user_id}"
    msg_time_local = message.date + timedelta(hours=8)
    yymmdd = msg_time_local.strftime("%y%m%d")

    confirm_val = await _valkey.get(key)
    print(f"[valkey] get: {key}={confirm_val}", flush=True)

    if confirm_val != yymmdd:
        TAG_FILTER_QUOTES = [
            "新出的标签筛选，直接赢麻了",
            "标签筛选刚上线，yyds实锤",
            "谁懂啊！新功能标签筛选太神了",
            "刚发现标签筛选，搜东西开挂了",
            "标签筛选是新出的吧？怎么这么爽",
            "新功能标签筛选，撸管人救命稻草",
            "偷偷更新了标签筛选？这也太顶了",
            "标签筛选刚出就上头，回不去了",
            "新出的标签筛选，找东西不用猜",
            "标签筛选新功能，懒人直接封神",
            "刚上线的标签筛选，精准到哭",
            "新功能标签筛选，比AI还懂我",
            "标签筛选是新出的外挂吧？",
            "最近超火的标签筛选，真香警告",
            "新出标签筛选，搜不到算我输",
            "标签筛选刚推就用上，爽翻",
            "刚试了新出的标签筛选，绝了",
            "标签筛选新上线，关键词退退退",
            "新出的标签筛选，撸管党狂喜",
            "标签筛选是新功能？怎么没人早说",
            "刚更新就有标签筛选，爱了爱了",
            "新功能标签筛选，专治找不到",
            "标签筛选新出的？这也太会了吧",
            "新上线标签筛选，直接拿捏需求",
            "刚挖到新功能：标签筛选YYDS",
            "标签筛选新出没多久，但已离不开了",
            "新功能标签筛选，省流天花板",
            "标签筛选刚上线，我就焊在手上了",
            "新出的标签筛选，CPU不烧了",
            "标签筛选是最近的新功能吧？神了",
            "刚用新功能标签筛选，秒出结果",
            "新出标签筛选，搜东西像开了天眼",
            "标签筛选新功能，真的会谢（太好用）",
            "新上线的标签筛选，建议锁死",
            "标签筛选新功能，让我告别试词",
            "刚上的标签筛选，精准得离谱",
            "新功能标签筛选，当代搜索基本操作",
            "标签筛选新出的？怎么像为我定制的",
            "刚发现这新功能：标签筛选太顶了",
            "标签筛选新上线，找啥都丝滑",
            "标签筛选是新功能？怎么像早就该有",
            "新功能标签筛选，一用就上瘾",
            "刚推的标签筛选，直接改变搜索习惯",
            "新出标签筛选，再也不用猜关键词",
            "刚出的标签筛选，已经成我刚需了",
            "新出的标签筛选，直接赢麻了",
            "标签筛选刚上线，yyds实锤",
            "谁懂啊！新功能标签筛选太神了",
            "刚发现标签筛选，搜东西开挂了",
            "标签筛选是新出的吧？怎么这么爽",
            "偷偷更新了标签筛选？这也太顶了",
            "标签筛选刚出就上头，回不去了",
            "新出的标签筛选，找东西不用猜",
            "标签筛选新功能，懒人直接封神",
            "新功能标签筛选，比AI还懂我",
            "标签筛选是新出的外挂吧？",
            "最近超火的标签筛选，真香警告",
            "新出标签筛选，搜不到算我输",
            "标签筛选刚推就用上，爽翻",
            "新功能标签筛选，效率拉满",
            "刚试了新出的标签筛选，绝了",
            "标签筛选新上线，关键词退退退",
            "标签筛选是新功能？怎么没人早说",
            "刚更新就有标签筛选，爱了爱了",
            "标签筛选新出的？这也太会了吧",
            "新上线标签筛选，直接拿捏需求",
            "刚挖到新功能：标签筛选YYDS",
            "标签筛选新出没多久，但已离不开了",
            "新功能标签筛选，省流天花板",
            "新出的标签筛选，直接起飞",
            "标签筛选新功能，体验原地起飞",
            "想找点对味的？标签筛选带你飞",
            "标签筛选刚推，我就冲了，起飞！",   
            "深夜用标签筛选，快乐原地起飞",
            "标签筛选一勾，爽感瞬间起飞",
            "这新功能绝了，标签筛选起飞中",
            "标签筛选新出，快到灵魂出窍",
            "不用翻半天，标签筛选直接冲",
            "新出标签筛选，精准到起飞",    
            "标签筛选新上线，体验直接拉满",
            "想找点刺激的？标签筛选冲就完事",
            "标签筛选一用，快乐值爆表起飞",
            "新功能标签筛选，省时又上头",
            "标签筛选刚出，我已经飞了",
            "深夜专属：标签筛选一键起飞",    
            "标签筛选一勾，今晚稳了，起飞！",
            "新标签筛选，快得离谱还上头",
            "标签筛选新功能，私密体验起飞",
            "想找点心动的？标签筛选冲了",
            "标签筛选刚上线，爽感原地爆炸",
            "新出标签筛选，三秒进入状态",
            "标签筛选一用，效率爽感双起飞",           
            "标签筛选=你的快乐加速器",
            "新功能标签筛选，体验直接升天",
            "标签筛选一开，精准到心巴起飞",
            "刚上的标签筛选，爽到循环使用",
            "标签筛选新上线，快准还不尴尬",
            "新标签筛选，懂你没说出口的爽",
            "想找点特别的？标签筛选带你冲",
            "标签筛选刚出，快乐值已拉满",
            "新出的标签筛选，快得刚刚好起飞",
            "新功能标签筛选，爽感无缝衔接"            
        ]


        option_buttons = []
        option_buttons.append(
            [
            InlineKeyboardButton(
                text=f"🏷️ 标签筛选",
                url=f"https://t.me/{lz_var.bot_username}?start=search_tag"
            )]
        )
        option_buttons.append(
            [InlineKeyboardButton(
                text=f"📩 教务处小助手",
                url=f"https://t.me/{lz_var.helper_bot_name}?start=nothing"
            )
            ]
        )

        
        await message.answer(
            text="⚠️ 小提示：你正在使用 /search_tag，已进入标签筛选界面\n\n"
           
            "• 这个功能目前还在 内测中，需要先手动开启才能使用\n"
            "• 启用后，大约 60 秒内即可生效，有效期为 1 天\n"
            "• 因为还在持续优化，个别内容可能会有点不太准\n"
            "• 使用过程中如果遇到任何问题，欢迎随时找下面的 教务处小助手机器人 跟我们说一声\n\n"
            "感谢你的理解与支持 ❤️\n\n"
            "🔓 怎么开启？\n\n"
            "在任意群组里 公开发送 下面这行文字即可（复制粘贴就行）：\n"
            f"<code>{random.choice(TAG_FILTER_QUOTES)}</code> 👈 (点字可复制)\n\n"
            ,

            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=option_buttons)
        )
        return False
    return True


@router.callback_query(F.data == "search_tag")
async def handle_search_tag(callback: CallbackQuery,state: FSMContext):
   
    if not await check_valid_key(callback.message):
        return



    keyboard = await get_filter_tag_keyboard(callback_query=callback,  state=state)

    await _edit_caption_or_text(
        photo=lz_var.skins['search_tag']['file_id'],
        msg=callback.message,
        text="🏷️ 请选择标签进行筛选...", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )

@router.message(Command("search_tag"))
async def handle_search_tag_command(message: Message, state: FSMContext, command: Command = Command("search_tag")):

     
    if not await check_valid_key(message):
        return

    keyboard = await get_filter_tag_keyboard(callback_query=message,  state=state)

    product_message = await lz_var.bot.send_photo(
        chat_id=message.chat.id,
        caption = "🏷️ 请选择标签进行筛选...", 
        photo=lz_var.skins['search_tag']['file_id'],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        parse_mode="HTML")


    await MenuBase.set_menu_status(state, {
        "current_message": product_message,
        "current_chat_id": product_message.chat.id,
        "current_message_id": product_message.message_id
    })




    # await callback.message.answer("🏷️ 请选择标签进行筛选...")

# 全局缓存延迟刷新任务
tag_refresh_tasks: dict[tuple[int, str], asyncio.Task] = {}
TAG_REFRESH_DELAY = 0.7

@router.callback_query(F.data.startswith("toggle_tag:"))
async def handle_toggle_tag(callback_query: CallbackQuery, state: FSMContext):
    parts = callback_query.data.split(":")
    tag = parts[1]
    tag_type = parts[2]
    user_id = callback_query.from_user.id


    # FSM 中缓存的打勾 tag 列表 key
    fsm_key = f"selected_tags:{user_id}"

    data = await state.get_data()
    selected_tags = set(data.get(fsm_key, []))
    
    if tag in selected_tags:
        selected_tags.remove(tag)
        await callback_query.answer("☑️ 已移除标签，你可以一次性勾选，系统会稍后刷新")
    else:
        selected_tags.add(tag)
        await callback_query.answer("✅ 已添加标签，你可以一次性勾选，系统会稍后刷新")

    # 更新 FSM 中缓存
    await state.update_data({fsm_key: list(selected_tags)})



    # 生成刷新任务 key
    task_key = (int(user_id), str(tag_type))

    # 如果已有延迟任务，取消旧的
    old_task = tag_refresh_tasks.get(task_key)
    if old_task and not old_task.done():
        old_task.cancel()

    # 创建新的延迟刷新任务
    async def delayed_refresh():
        try:
            await asyncio.sleep(TAG_REFRESH_DELAY)
            keyboard = await get_filter_tag_keyboard(callback_query, current_tag_type=tag_type, state=state)
            await _edit_caption_or_text(
                photo=lz_var.skins['search_tag']['file_id'],
                msg=callback_query.message,
                text="🏷️ 请选择标签进行筛选...", 
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
            )


            tag_refresh_tasks.pop(task_key, None)
            print(f"\n\n---\n")
        except asyncio.CancelledError:
            pass  # 被取消时忽略

    tag_refresh_tasks[task_key] = asyncio.create_task(delayed_refresh())

@router.callback_query(F.data.startswith("set_tag_type:"))
async def handle_set_tag_type(callback_query: CallbackQuery, state: FSMContext):
    parts = callback_query.data.split(":")
    type_code = parts[1]
    keyboard = await get_filter_tag_keyboard(callback_query, state, current_tag_type=type_code)
    await _edit_caption_or_text(
        photo=lz_var.skins['search_tag']['file_id'],
        msg=callback_query.message,
        text="🏷️ 请选择标签进行筛选...", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )



async def get_filter_tag_keyboard(callback_query: CallbackQuery,  state: FSMContext, current_tag_type: str = None):
    keyboard = []

    all_tags_by_type = await MySQLPool.get_all_tags_grouped()
    all_tag_rows = all_tags_by_type['all_tag_rows']
    all_tag_types = all_tags_by_type['all_tag_types']
    
    
    user_id = callback_query.from_user.id     
    fsm_key = f"selected_tags:{user_id}"
    data = await state.get_data()
    selected_tags = set(data.get(fsm_key, []))

    print(f"2254:当前已选标签: {fsm_key} : {selected_tags}", flush=True)




    # ===== 新增：按规格分组展开 =====
    GROUPS = [
        ['age','par','eth', 'face'], 
        ['act', 'nudity','fetish'],
        ['att','feedback','play'],
        ['pro', 'position', 'hardcore']
    ]

    is_open = False
    if current_tag_type is None:
        is_open = True
    

    for subgroup in GROUPS:
        

        for _tag_type in subgroup:
            

            tag_rows = all_tag_rows.get(_tag_type, [])
            tag_codes = [i["tag"] for i in tag_rows]
            selected_count = len(set(tag_codes) & set(selected_tags))
            total_count = len(tag_codes)

            current_type_cn = (all_tag_types.get(_tag_type) or {}).get("type_cn") or _tag_type
            display_cn = f"{current_type_cn} ( {selected_count}/{total_count} )"
            
            
            if current_tag_type in subgroup or is_open :
                
                header = f"━━━ {display_cn} ━━━ " 
                keyboard.append([InlineKeyboardButton(text=header, callback_data="noop")])

                

                row = []
                for tag_row in tag_rows:
                   
                    tag_text = tag_row["tag_cn"] or tag_row["tag"]
                    tag_code = tag_row["tag"]
                    display = f"☑️ {tag_text}" if tag_code in selected_tags else tag_text

                    # print(f":::selected_tags={selected_tags}, tag_code={tag_code}, display={display}", flush=True)
                    row.append(InlineKeyboardButton(
                        text=f"{display}",
                        callback_data=f"toggle_tag:{tag_code}:{_tag_type}"
                    ))

                    if len(row) == 3:
                        keyboard.append(row)
                        row = []
                if row:
                    keyboard.append(row)
            else:
                keyboard.append([
                    InlineKeyboardButton(
                        text=f"――― {display_cn} ――― ",
                        callback_data=f"set_tag_type:{_tag_type}"
                    )
                ])
        is_open = False  # 重置展开标志

    keyboard.append([
        InlineKeyboardButton(
            text="🔍 开始搜索",
            callback_data=f"search_tag_start"
        )
    ])

    keyboard.append([
        InlineKeyboardButton(
            text="🔙 返回搜索",
            callback_data=f"search"
        )
    ])

    return keyboard


@router.callback_query(F.data.startswith("search_tag_start"))
async def handle_toggle_tag(callback_query: CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    all_tags_by_type = await MySQLPool.get_all_tags_grouped()
    all_tag_rows = all_tags_by_type['all_tag_rows']
    
    fsm_key = f"selected_tags:{user_id}"
    data = await state.get_data()
    selected_tags = set(data.get(fsm_key, []))

    # selected_tags: set[tag]
    tag2cn = {r["tag"]: (r.get("tag_cn") or r["tag"])
            for rows in all_tag_rows.values() for r in rows}

    keyword = " ".join(tag2cn.get(t, t) for t in selected_tags)
    
    await handle_search_component(callback_query.message, state, keyword)

async def get_html_content(file_path: str, title: str) -> str:
    FILE_PATH = file_path
    MAX_AGE = timedelta(hours=24, minutes=30)

    def file_is_fresh(path: str) -> bool:
        try:
            mtime = os.path.getmtime(path)
        except FileNotFoundError:
            return False
        except Exception:
            return False
        # 用已载入的 datetime 计算年龄
        now = datetime.now(timezone.utc)
        mdt = datetime.fromtimestamp(mtime, tz=timezone.utc)
        return (now - mdt) <= MAX_AGE

    html_text: str | None = None
    if not file_is_fresh(FILE_PATH):
        # 过期或不存在 -> DB 拉最新快照
        html_text = await MySQLPool.fetch_task_value_by_title(title)
        if html_text:
            try:
                folder = os.path.dirname(FILE_PATH) or "."
                os.makedirs(folder, exist_ok=True)
                with open(FILE_PATH, "w", encoding="utf-8") as f:
                    f.write(html_text)
            except Exception as e:
                print(f"⚠️ 写入 {FILE_PATH} 失败: {e}", flush=True)

    # 若没拉到（或原本就新鲜），则从文件读
    if html_text is None:
        try:
            with open(FILE_PATH, "r", encoding="utf-8") as f:
                html_text = f.read()
        except Exception as e:
            print(f"⚠️ 读取 {FILE_PATH} 失败: {e}", flush=True)
            html_text = "<b>暂时没有可显示的排行榜内容。</b>"
    return html_text
   

# == 排行选项响应 ==

# == 排行选项响应 ==
@router.callback_query(F.data == "ranking_resource")
async def handle_ranking_resource(callback: CallbackQuery,state: FSMContext):
    """
    - 若 ranking_resource.html 不存在或 mtime > 24.5h：从 MySQL task_rec 读取 task_title='salai_hot' 的 task_value 并覆盖写入
    - 否则直接读取文件
    - 最终以 HTML 方式发送
    """
    FILE_PATH = getattr(lz_var, "HOT_RANKING_HTML_PATH", "ranking_resource.html")
    html_text = await get_html_content(FILE_PATH, "salai_hot")
    

    await _edit_caption_or_text(
        photo=lz_var.skins['ranking_resource']['file_id'],
        msg=callback.message,
        text=html_text, 
        reply_markup=ranking_menu_keyboard(),

    )

    # await callback.message.answer(
    #     html_text,
    #     parse_mode=ParseMode.HTML,
    #     disable_web_page_preview=True
    # )
    await callback.answer()

@router.callback_query(F.data == "ranking_uploader")
async def handle_ranking_uploader(callback: CallbackQuery,state: FSMContext):
    FILE_PATH = getattr(lz_var, "RANKING_UPLOADER_HTML_PATH", "ranking_uploader.html")
    html_text = await get_html_content(FILE_PATH, "salai_reward")
    
    # await callback.message.answer(
    #     html_text,
    #     parse_mode=ParseMode.HTML,
    #     disable_web_page_preview=True
    # )

    await _edit_caption_or_text(
        photo=lz_var.skins['ranking_uploader']['file_id'],
        msg=callback.message,
        text=html_text, 
        reply_markup=ranking_menu_keyboard(),
        state= state
    )    
    await callback.answer()




# ====== 通用：分页列表键盘（mine / fav）======

async def _load_collections_rows(user_id: int, page: int, mode: str):
    PAGE_SIZE = 6
    offset = page * PAGE_SIZE
    if mode == "mine":
        rows = await MySQLPool.list_user_collections(user_id=user_id, limit=PAGE_SIZE + 1, offset=offset)
    elif mode == "fav":
        rows = await MySQLPool.list_user_favorite_collections(user_id=user_id, limit=PAGE_SIZE + 1, offset=offset)
    else:
        rows = []
    has_next = len(rows) > PAGE_SIZE
    return rows[:PAGE_SIZE], has_next

def _collection_btn_text(row: dict) -> str:
    cid   = row.get("id")
    title = (row.get("title") or "未命名资源橱窗")[:30]
    pub   = "公开" if (row.get("is_public") == 1) else "不公开"
    return f"〈{title}〉（{pub}）#ID{cid}"

async def build_collections_keyboard(user_id: int, page: int, mode: str) -> InlineKeyboardMarkup:
    """
    mode: 'mine'（我的资源橱窗）| 'fav'（我收藏的资源橱窗）
    每页 6 行资源橱窗按钮；第 7 行：上一页 | [创建，仅 mine] | 下一页（上一/下一按需显示）
    最后一行：🔙 返回上页
    """
    PAGE_SIZE = 6
    display, has_next = await _load_collections_rows(user_id, page, mode)

    list_prefix = "cc:mlist" if mode == "mine" else "cc:flist" #上下页按钮
    edit_prefix = "clt:my"  if mode == "mine" else "clt:fav" #列表按钮

    kb_rows: list[list[InlineKeyboardButton]] = []

    if not display:
        # 空列表：mine 显示创建，fav 不显示创建
        if mode == "mine":
            kb_rows.append([InlineKeyboardButton(text="➕ 创建资源橱窗", callback_data="clt:create")])
        kb_rows.append([InlineKeyboardButton(text="🔙 返回上页", callback_data="collection")])
        return InlineKeyboardMarkup(inline_keyboard=kb_rows)

    # 6 行资源橱窗按钮
    for r in display:
        cid = r.get("id")
        btn_text = _collection_btn_text(r)
        kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"{edit_prefix}:{cid}:{page}:tk")])

    # 第 7 行：上一页 | [创建，仅 mine] | 下一页
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"{list_prefix}:{page-1}"))

    if mode == "mine":
        nav_row.append(InlineKeyboardButton(text="➕ 创建资源橱窗", callback_data="clt:create"))

    if has_next:
        nav_row.append(InlineKeyboardButton(text="下一页 ➡️", callback_data=f"{list_prefix}:{page+1}"))

    # 有可能出现只有“上一页/下一页”而中间没有“创建”的情况（fav 模式）
    if nav_row:
        kb_rows.append(nav_row)

    # 返回上页

    kb_rows.append([InlineKeyboardButton(text="🔙 返回资源橱窗菜单", callback_data="collection")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)



# ====== “我的资源橱窗”入口用通用键盘（保持既有行为）======

@router.callback_query(F.data == "clt_my")
async def handle_clt_my(callback: CallbackQuery,state: FSMContext):
    user_id = callback.from_user.id
    # “我的资源橱窗”之前是只换按钮；为了统一体验，也可以换 text，但你要求按钮呈现，因此只换按钮：

    text = f'这是你的资源橱窗'

    await _edit_caption_or_text(
        photo=lz_var.skins['clt_my']['file_id'],
        msg=callback.message,
        text=text, 
        reply_markup=await build_collections_keyboard(user_id=user_id, page=0, mode="mine"),
        state= state
    )





    # await callback.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:mlist:\d+$"))
async def handle_clt_my_pager(callback: CallbackQuery):
    _, _, page_str = callback.data.split(":")
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=int(page_str), mode="mine")
    await callback.message.edit_reply_markup(reply_markup=kb)

#查看资源橱窗
@router.callback_query(F.data.regexp(r"^clt:my:(\d+)(?::(\d+)(?::([A-Za-z0-9]+))?)?$"))
async def handle_clt_my_detail(callback: CallbackQuery,state: FSMContext):
    # ====== “我的资源橱窗”入口用通用键盘（保持既有行为）======
    print(f"handle_clt_my_detail: {callback.data}")
    # _, _, cid_str, page_str,refresh_mode = callback.data.split(":")
    # cid = int(cid_str)

    parts = callback.data.split(":")
    # 例：clt:my:{cid}:{page}:{mode}
    cid = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    refresh_mode = parts[4] if len(parts) > 4 else "tk"


    user_id = callback.from_user.id

    new_message = callback.message    

    if refresh_mode == 'k':
        kb = _build_clt_info_keyboard(cid, is_fav=False, mode='edit', ops='handle_clt_my')
        await callback.message.edit_reply_markup(reply_markup=kb)
    elif refresh_mode == 't':
        pass
    elif refresh_mode == 'tk':
        collection_info  = await _build_clt_info(cid=cid, user_id=user_id, mode='edit', ops='handle_clt_my')
        if collection_info.get("success") is False:
            await callback.answer(collection_info.get("message"), show_alert=True)
            return
        elif collection_info.get("photo"):
            # await callback.message.edit_media(media=collection_info.get("photo"), caption=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))
            
            new_message = await callback.message.edit_media(
                media=InputMediaPhoto(media=collection_info.get("photo"), 
                caption=collection_info.get("caption"), 
                parse_mode="HTML"),
                reply_markup=collection_info.get("reply_markup")
            )
            


        else:
            new_message = await callback.message.edit_text(text=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))

    await MenuBase.set_menu_status(state, {
        "current_message": new_message,
        "menu_message": new_message,
        "current_chat_id": new_message.chat.id,
        "current_message_id": new_message.message_id
    })

    # await state.update_data({
    #     "menu_message": new_message
    # })

#编辑资源橱窗详情
@router.callback_query(F.data.regexp(r"^clt:edit:\d+:\d+(?::([A-Za-z]+))?$"))
async def handle_clt_edit(callback: CallbackQuery,state: FSMContext):
    # ====== “我的资源橱窗”入口用通用键盘（保持既有行为）======
    print(f"handle_clt_edit: {callback.data}")
    _, _, cid_str, page_str, refresh_mode = callback.data.split(":")
    cid = int(cid_str)
    print(f"{callback.message.chat.id} {callback.message.message_id}")
    if refresh_mode == 'k':
        kb = _build_clt_edit_keyboard(cid)
        await callback.message.edit_reply_markup(reply_markup=kb)
    elif refresh_mode == 't':
        pass
    elif refresh_mode == 'tk':
        caption = await _build_clt_edit_caption(cid)
        kb = _build_clt_edit_keyboard(cid)

        await _edit_caption_or_text(
            callback.message,
            text=caption, 
            reply_markup=kb,
            state= state
        )
        user_id = callback.from_user.id
        await MySQLPool.delete_cache(f"user:clt:{user_id}:")

async def _build_clt_edit(cid: int, anchor_message: Message,state: FSMContext):
    caption = await _build_clt_edit_caption(cid)
    kb = _build_clt_edit_keyboard(cid)
    await _edit_caption_or_text(
        msg=anchor_message,
        text=caption, 
        reply_markup=kb,
        state= state
    )

async def _build_clt_edit_caption(cid: int ):
    
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名资源橱窗"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    text = (
        f"当前设置：\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc, 120)}\n\n"
        f"请选择要设置的项目："
    )

   
    return text


# ====== “我收藏的资源橱窗”入口（复用通用键盘，mode='fav'）======

#创建资源橱窗
@router.callback_query(F.data == "clt:create")
async def handle_clt_create(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    ret = await MySQLPool.create_user_collection(user_id=user_id)  # 默认：未命名资源橱窗、公开
    cid = ret.get("id")


    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名资源橱窗"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    text = (
        f"🆕 已创建资源橱窗：\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc, 120)}\n\n"
        f"请选择要设置的项目："
    )
    await _edit_caption_or_text(
        callback.message,
        text=text,
        reply_markup=_build_clt_edit_keyboard(cid),
        state= state
    )
    await MySQLPool.delete_cache(f"collection_info_{cid}")




@router.callback_query(F.data == "clt_favorite")
async def handle_clt_favorite(callback: CallbackQuery,state: FSMContext):
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=0, mode="fav")
    await _edit_caption_or_text(
        photo=lz_var.skins['clt_fav']['file_id'],
        msg=callback.message,
        text="这是你收藏的资源橱窗", 
        reply_markup=kb,
        state= state
    )


    # await callback.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:flist:\d+$"))
async def handle_clt_favorite_pager(callback: CallbackQuery):
    _, _, page_str = callback.data.split(":")
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=int(page_str), mode="fav")
    await callback.message.edit_reply_markup(reply_markup=kb)

# 收藏列表点击 → 详情（只读，无“标题/简介/公开”按钮）
def favorite_detail_keyboard(page: int):
    # 只提供返回收藏列表与回资源橱窗主菜单
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 返回收藏列表", callback_data=f"cc:flist:{page}")],
        [InlineKeyboardButton(text="🪟 回资源橱窗菜单", callback_data="collection")],
    ])


@router.callback_query(F.data.regexp(r"^clt:fav:(\d+)(?::(\d+)(?::([A-Za-z0-9]+))?)?$"))
async def handle_clt_fav(callback: CallbackQuery):
    # ====== “我收藏的资源橱窗”入口（复用通用键盘，mode='fav'）======
    print(f"handle_clt_fav: {callback.data}")
    _, _, cid_str, page_str,refresh_mode = callback.data.split(":")
    cid = int(cid_str)
    user_id = callback.from_user.id

    if refresh_mode == 'k':
        kb = _build_clt_info_keyboard(cid, is_fav=False, mode='view', ops='handle_clt_fav')
        await callback.message.edit_reply_markup(reply_markup=kb)
    elif refresh_mode == 't':
        pass
    elif refresh_mode == 'tk':
        collection_info  = await _build_clt_info(cid=cid, user_id=user_id, mode='view', ops='handle_clt_fav')
        if collection_info.get("success") is False:
            await callback.answer(collection_info.get("message"), show_alert=True)
            return
        elif collection_info.get("photo"):
            # await callback.message.edit_media(media=collection_info.get("photo"), caption=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))
            
            await callback.message.edit_media(
                media=InputMediaPhoto(media=collection_info.get("photo"), 
                caption=collection_info.get("caption"), 
                parse_mode="HTML"),
                reply_markup=collection_info.get("reply_markup")
            )
            
            return
        else:
            await callback.message.edit_text(text=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))



    # collection_info  = await _build_clt_info(cid=cid, user_id=user_id, mode='view', ops='handle_clt_fav')
    # if collection_info.get("success") is False:
    #     await callback.answer(collection_info.get("message"), show_alert=True)
    #     return
    # elif collection_info.get("photo"):
    #     # await callback.message.edit_media(media=collection_info.get("photo"), caption=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))
        
    #     await callback.message.edit_media(
    #         media=InputMediaPhoto(media=collection_info.get("photo"), caption=collection_info.get("caption"), parse_mode="HTML"),
    #         reply_markup=collection_info.get("reply_markup")
    #     )
        
    #     return
    # else:
    #     await callback.message.edit_text(text=collection_info.get("caption"), reply_markup=collection_info.get("reply_markup"))



# ============ /set [id]：资源橱窗信息/列表/收藏切换 ============



def _build_clt_info_caption(rec: dict) -> str:
    return (
        f"<blockquote>{rec.get('title') or '未命名资源橱窗'}</blockquote>\n"
        f"{rec.get('description') or ''}\n\n"
        f"🆔 {rec.get('id')}     👤 {rec.get('user_id')}\n"
    )


#collection > 资源橱窗 Partal > 资源橱窗列表 CollectionList > [单一资源橱窗页 CollectionDetail] > 显示资源橱窗内容 CollectItemList 或 编辑资源橱窗 CollectionEdit
def _build_clt_info_keyboard(cid: int, is_fav: bool, mode: str = 'view', ops: str = 'handle_clt_fav') -> InlineKeyboardMarkup:
    kb_rows: list[list[InlineKeyboardButton]] = []

    print(f"ops={ops}")

    callback_function = ''
    if ops == 'handle_clt_my':
        callback_function = 'clti:list'
    elif ops == 'handle_clt_fav':
        callback_function = 'clti:flist' 

    nav_row: list[InlineKeyboardButton] = []
    nav_row.append(InlineKeyboardButton(text="🪟 显示资源橱窗内容", callback_data=f"{callback_function}:{cid}:0"))

    if mode == 'edit':
        nav_row.append(InlineKeyboardButton(text="🔧 编辑资源橱窗", callback_data=f"clt:edit:{cid}:0:k"))
    else:
        fav_text = "❌ 取消收藏" if is_fav else "🩶 收藏"
        nav_row.append(InlineKeyboardButton(text=fav_text, callback_data=f"uc:fav:{cid}"))
    
    if nav_row:
        kb_rows.append(nav_row)  

    shared_url = f"https://t.me/{lz_var.bot_username}?start=clt_{cid}"
    kb_rows.append([InlineKeyboardButton(text="🔗 复制资源橱窗链结", copy_text=CopyTextButton(text=shared_url))])


    if ops == 'handle_clt_my':
        kb_rows.append([InlineKeyboardButton(text="🔙 返回我的资源橱窗", callback_data="clt_my")])
    elif ops == 'handle_clt_fav':
        kb_rows.append([InlineKeyboardButton(text="🔙 返回收藏的资源橱窗", callback_data="clt_favorite")])
    else:
        kb_rows.append([InlineKeyboardButton(text="🔙 返回", callback_data="clt_my")])


    # 关键点：需要二维数组（每个子列表是一行按钮）
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# 查看资源橱窗的按钮
def _clti_list_keyboard(cid: int, page: int, has_prev: bool, has_next: bool, is_fav: bool, mode: str = 'view') -> InlineKeyboardMarkup:
    nav_row: list[InlineKeyboardButton] = []
    rows = []
    if mode == 'list':
        callback_function = 'my'
        title = "🔙 返回我的资源橱窗主页"
    elif mode == 'flist':
        callback_function = 'fav' 
        title = "🔙 返回收藏的资源橱窗主页"


    if has_prev:
        nav_row.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"clti:{mode}:{cid}:{page-1}"))



    if has_next:
        nav_row.append(InlineKeyboardButton(text="➡️ 下一页", callback_data=f"clti:{mode}:{cid}:{page+1}"))

    
    if nav_row: rows.append(nav_row)



    print(f"callback_function={callback_function}")

    rows.append([InlineKeyboardButton(text=title, callback_data=f"clt:{callback_function}:{cid}:0:tk")])

   


    return InlineKeyboardMarkup(inline_keyboard=rows)


# /set [数字]
@router.message(Command("set"))
async def handle_set_collection(message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.answer("用法：/set [资源橱窗ID]")
        return

    cid = int(args[1].strip())
    user_id = message.from_user.id
    retCollect = await _build_clt_info(cid=cid, user_id=user_id, ops='handle_set_collection')

# 查看资源橱窗
async def _build_clt_info( cid: int, user_id: int, mode: str = 'view', ops:str ='set') -> dict:
    bot_name = getattr(lz_var, "bot_username", None) or "luzaitestbot"
    # 查询资源橱窗 + 封面 file_id（遵循你给的 SQL）
    rec = await MySQLPool.get_collection_detail_with_cover(collection_id=cid, bot_name=bot_name)
    if not rec:
        # return await message.answer("⚠️ 未找到该收藏")
        return {"success": False, "message": "未找到该收藏"}
        
    
    # 是否已收藏（用于按钮文案）
   
    is_fav = await MySQLPool.is_collection_favorited(user_id=user_id, collection_id=cid)

    caption = _build_clt_info_caption(rec)

    # 有封面 -> sendPhoto；无封面 -> sendMessage
    cover_file_id = rec.get("cover_file_id") or lz_var.skins['clt_cover']['file_id']
    # cover_file_id = "AgACAgEAAxkBAAICXWjSrgfWzDY2mgnFdUCKY4MVkwSaAAI-C2sblpeYRiQXZv8N-OgzAQADAgADeQADNgQ" #TODO
   
    kb = _build_clt_info_keyboard(cid, is_fav, mode, ops)
    try:
       
       
      
        # if ops == 'handle_clt_my':
        #     cover_file_id = lz_var.skins['clt_my']['file_id']
        # elif ops == 'handle_clt_fav':
        #     cover_file_id = lz_var.skins['clt_fav']['file_id']

        if cover_file_id:
            # return await message.answer_photo(photo=cover_file_id, caption=caption, reply_markup=kb)
            return {"success": True, "photo": cover_file_id, "caption": caption, "reply_markup": kb}
           
        else:
            # return await message.answer(text=caption, reply_markup=kb)
            return {"success": True,  "caption": caption, "reply_markup": kb}
           
    except Exception as e:
        print(f"❌ 发送资源橱窗信息失败: {e}", flush=True)
        # return await message.answer("⚠️ 发送资源橱窗信息失败")
        return {"success": False,  "caption": caption, "reply_markup": kb}
        


# 「显示列表」：分页展示前 6 个文件的 file_id
@router.callback_query(F.data.regexp(r"^clti:(flist|list):\d+:\d+$"))
async def handle_clti_list(callback: CallbackQuery, state: FSMContext):
    _, mode, clt_id_str, page_str = callback.data.split(":")
    clt_id, page = int(clt_id_str), int(page_str)
    user_id = callback.from_user.id
    
    # print(f"--->{mode}_message")
    await MenuBase.set_menu_status(state, {
        "menu_message": callback.message,
        "collection_id": clt_id,
        'action':mode,
        "current_chat_id": callback.message.chat.id,
        "current_message_id": callback.message.message_id,
    })
    # await state.update_data({
    #     "menu_message": callback.message,
    #     "collection_id": clt_id,
    #     'action':mode
    #     })

    # print(f"✅ Clti Message {callback.message.message_id} in chat {callback.message.chat.id}", flush=True)

    result = await _get_clti_list(clt_id,page,user_id,mode)

    if result.get("success") is False:
        await callback.answer("这个资源橱窗暂时没有收录文件", show_alert=True)
        return

    await _edit_caption_or_text(
        callback.message,
        text=result.get("caption"),
        reply_markup=result.get("reply_markup"),
        state= state
    )
    await callback.answer()



async def _get_clti_list(cid,page,user_id,mode):
    # 拉取本页数据（返回 file_id list 与 has_next）
    files, has_next = await MySQLPool.list_collection_files_file_id(collection_id=cid, limit=RESULTS_PER_PAGE+1, offset=page*RESULTS_PER_PAGE)
    display = files[:RESULTS_PER_PAGE]
    has_prev = page > 0
    is_fav = await MySQLPool.is_collection_favorited(user_id=user_id, collection_id=cid)

    if not display:
        return {"success": False, "message": "这个资源橱窗暂时没有收录文件"}
   
    # 组装列表 caption：仅列 file_id
    lines = [f"资源橱窗 #{cid} 文件列表（第 {page+1} 页）", ""]
    for idx, f in enumerate(display, start=1):
        # print(f"f{f}",flush=True)
        content = _short(f.get("content"))
        # 根据 r['file_type'] 进行不同的处理
        if f.get('file_type') == 'v':
            icon = "🎬"
        elif f.get('file_type') == 'd':
            icon = "📄"
        elif f.get('file_type') == 'p':
            icon = "🖼"
        elif r['file_type'] == 'a':
            icon = "📂"
        else:
            icon = "🔹"

        fid = _short(f.get("content"),30) or "(无 file_id)"
        aes = AESCrypto(AES_KEY)
        encoded = aes.aes_encode(f.get("id"))

        stag = "cm"
        print(f"mode={mode}")
        if mode == 'list':
            fix_href = f'<a href="https://t.me/{lz_var.bot_username}?start=rci_{f.get("id")}_{page}">❌</a> '
            stag = "cm"
        elif mode == 'flist':
            fix_href = ''
            stag = "cf"
        lines.append(
            f"{icon}<a href='https://t.me/{lz_var.bot_username}?start={stag}_{cid}_{encoded}'>{content}</a> {fix_href}"
        )

        # lines.append(f"{icon}<a href='https://t.me/{lz_var.bot_username}?start={stag}_{cid}_{encoded}'>{f.get("id")} {content}</a> {fix_href}")
    caption = "\n".join(lines)



    reply_markup = _clti_list_keyboard(cid, page, has_prev, has_next, is_fav, mode)

    return {"success": True, "caption": caption, "reply_markup": reply_markup}
    





# 「资源橱窗信息」：恢复信息视图
@router.callback_query(F.data.regexp(r"^uc:info:\d+$"))
async def handle_uc_info(callback: CallbackQuery,state: FSMContext):
    _, _, cid_str = callback.data.split(":")
    cid = int(cid_str)
    bot_name = getattr(lz_var, "bot_username", None) or "luzaitestbot"
    rec = await MySQLPool.get_collection_detail_with_cover(collection_id=cid, bot_name=bot_name)
    if not rec:
        await callback.answer("未找到该收藏", show_alert=True); return
    is_fav = await MySQLPool.is_collection_favorited(user_id=callback.from_user.id, collection_id=cid)
    await _edit_caption_or_text(callback.message, text=_build_clt_info_caption(rec), reply_markup=_build_clt_info_keyboard(cid, is_fav),state= state)
    await callback.answer()

# 「收藏 / 取消收藏」：落 DB 并刷新按钮
@router.callback_query(F.data.regexp(r"^uc:fav:\d+$"))
async def handle_uc_fav(callback: CallbackQuery):
    _, _, cid_str = callback.data.split(":")
    cid = int(cid_str)
    user_id = callback.from_user.id

    print(f"➡️ 用户 {user_id} 切换资源橱窗 {cid} 收藏状态", flush=True)

    is_fav = False
    already = await MySQLPool.is_collection_favorited(user_id=user_id, collection_id=cid)
    if already:
        ok = await MySQLPool.remove_collection_favorite(user_id=user_id, collection_id=cid)
        tip = "已取消收藏" if ok else "取消收藏失败"
        is_fav = False
    else:
        ok = await MySQLPool.add_collection_favorite(user_id=user_id, collection_id=cid)
        tip = "已加入收藏" if ok else "收藏失败"
        is_fav = True

    print(f"➡️ 用户 {user_id} 资源橱窗 {cid} 收藏状态切换结果: {tip}", flush=True)

    # 判断当前是否列表视图：看 caption 文本是否包含“文件列表”
    is_list_view = False
    try:
        cur_caption = (callback.message.caption or callback.message.text or "") if callback.message else ""
        is_list_view = "文件列表" in cur_caption
    except Exception:
        pass

    if is_list_view:
        # 提取当前页号（从 callback.data 不好取，尝试从 caption 无法拿页码则回 0）
        page = 0
        if cur_caption:
            # 简单解析 “第 X 页”
            import re
            m = re.search(r"第\s*(\d+)\s*页", cur_caption)
            if m:
                page = max(int(m.group(1)) - 1, 0)

        # 检查是否还有翻页（重新查一次以确保 nav 正确）
        files, has_next = await MySQLPool.list_collection_files_file_id(collection_id=cid, limit=RESULTS_PER_PAGE+1, offset=page*RESULTS_PER_PAGE)
        has_prev = page > 0
        kb = _clti_list_keyboard(cid, page, has_prev, has_next, is_fav)
    else:
        kb = _build_clt_info_keyboard(cid, is_fav)

    try:
        ret= await callback.message.edit_reply_markup(reply_markup=kb)
    except Exception as e:
        print(f"❌ 刷新收藏按钮失败: {e}", flush=True)

    await callback.answer(tip, show_alert=False)



@router.callback_query(F.data == "explore_marketplace")
async def handle_explore_marketplace(callback: CallbackQuery):
    await callback.message.answer("🛍️ 欢迎来到资源橱窗展场，看看其他人都在收藏什么吧！")



# == 猜你喜欢选项响应 ==
@router.callback_query(F.data == "view_recommendations")
async def handle_view_recommendations(callback: CallbackQuery):
    await callback.message.answer("🎯 根据你的兴趣推荐：...")

# == 资源上传选项响应 ==
@router.callback_query(F.data == "do_upload_resource")
async def handle_do_upload_resource(callback: CallbackQuery):
    await callback.message.answer("📤 请上传你要分享的资源：...")

# == 通用返回首页 ==
@router.callback_query(F.data == "go_home")
async def handle_go_home(callback: CallbackQuery):
    # await callback.answer_photo(
    #     photo=lz_var.skins['home']['file_id'],
    #     caption="👋 欢迎使用 LZ 机器人！请选择操作：",
    #     parse_mode="HTML",
    #     reply_markup=main_menu_keyboard())

    await lz_var.bot.edit_message_media(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        media=InputMediaPhoto(
            media=lz_var.skins['home']['file_id'],
            caption="👋 欢迎使用 LZ 机器人！请选择操作：",
            parse_mode="HTML"
        ),
        reply_markup=main_menu_keyboard()
    )

    # await callback.message.edit_reply_markup(reply_markup=main_menu_keyboard())





@router.callback_query(F.data.startswith("sora_page:"))
async def handle_sora_page(callback: CallbackQuery, state: FSMContext):
    try:
        # 新 callback_data 结构: sora_page:<search_key_index>:<current_pos>:<offset>
        _, search_key_index_str, current_pos_str, offset_str, search_from = callback.data.split(":")
        # print(f"➡️ handle_sora_page: {callback.data}")
        search_key_index = int(search_key_index_str)
        current_pos = int(current_pos_str)
        offset = int(offset_str)
        search_from = str(search_from) or "search"
        if search_from == "search" or search_from == "f":
            # 查回 keyword
            keyword = await db.get_keyword_by_id(search_key_index)
            if not keyword:
                await callback.answer("⚠️ 无法找到对应关键词", show_alert=True)
                return

            # 拉取搜索结果 (用 MemoryCache 非常快)
            result = await db.search_keyword_page_plain(keyword)
            if not result:
                await callback.answer("⚠️ 搜索结果为空", show_alert=True)
                return
           
        elif search_from == "cm" or search_from == "cf":
            # print(f"🔍 搜索资源橱窗 ID {search_key_index} 的内容")
            # 拉取收藏夹内容
            result = await MySQLPool.get_clt_files_by_clt_id(search_key_index)
            if not result:
                await callback.answer("⚠️ 资源橱窗为空", show_alert=True)
                return
        elif search_from == "fd":
            # print(f"🔍 搜索资源橱窗 ID {search_key_index} 的内容")
            result = await PGPool.search_history_redeem(search_key_index)
            if not result:
                await callback.answer("⚠️ 兑换纪录为空", show_alert=True)
                return    
        elif search_from == "ul":
            # print(f"🔍 搜索资源橱窗 ID {search_key_index} 的内容")
            result = await PGPool.search_history_upload(search_key_index)
            if not result:
                await callback.answer("⚠️ 上传纪录为空", show_alert=True)
                return   


        print(f"Prefetch sora_media for pagination: {search_from}", flush=True)
        if state is not None and result:
            print(f"Starting prefetch task...", flush=True)
            # 用 callback_function + keyword_id 当 key，避免同一批结果被重复开启预加载任务
            key = f"prefetch_sora_media:{search_from}"
            # 注意要把 result 拷贝成 list，避免外面后续修改它
            snapshot = list(result)

            spawn_once(
                key,
                lambda state=state, snapshot=snapshot: _prefetch_sora_media_for_results(state, snapshot),
            )

        # 计算新的 pos
        new_pos = current_pos + offset
        if new_pos < 0 or new_pos >= len(result):
            await callback.answer("⚠️ 没有上一项 / 下一项", show_alert=True)
            return

        # 取对应 content_id
        next_record = result[new_pos]
        # print(f"next_record={next_record}")
        next_content_id = next_record["id"]
        # print(f"➡️ 翻页请求: current_pos={current_pos}, offset={offset}, new_pos={new_pos}, next_content_id={next_content_id}")

    
        await MenuBase.set_menu_status(state, {
            "current_message": callback.message,
            "menu_message": callback.message,
            "current_chat_id": callback.message.chat.id,
            "current_message_id": callback.message.message_id
        })

        
        viewer_id = callback.from_user.id
        product_info = await _build_product_info(content_id=next_content_id, search_key_index=search_key_index,  state=state,  message= callback.message, search_from=search_from , current_pos=new_pos, viewer_user_id=viewer_id)

        if product_info.get("ok") is False:
            print(f"❌ _build_product_info failed: {product_info}")
            await callback.answer(product_info.get("msg"), show_alert=True)
            return


    

        # print(f"product_info={product_info}")
        ret_content = product_info.get("caption")
        thumb_file_id = product_info.get("cover_file_id")
        reply_markup = product_info.get("reply_markup")

       
        try:    
            result_edit_media=await callback.message.edit_media(
                media={
                    "type": "photo",
                    "media": thumb_file_id,
                    "caption": ret_content,
                    "parse_mode": "HTML"
                },
                reply_markup=reply_markup
            )

            await MenuBase.set_menu_status(state, {
                "current_message": result_edit_media,
                "menu_message": result_edit_media,
                "current_chat_id": result_edit_media.chat.id,
                "current_message_id": result_edit_media.message_id
            })

          
            
        except Exception as e:
            print(f"❌ edit_media failed: {e}, try edit_text")

        await callback.answer()

    except Exception as e:
        print(f"❌ handle_sora_page error: {e}")
        await callback.answer("⚠️ 翻页失败", show_alert=True)




@router.callback_query(F.data.startswith("keyframe:"))
async def handle_keyframe_redeem(callback: CallbackQuery, state: FSMContext):
    content_id = callback.data.split(":")[1]
    result = await load_sora_content_by_id(int(content_id), state)
    ret_content, file_info, purchase_info = result
    source_id = file_info[0] if len(file_info) > 0 else None
    file_type = file_info[1] if len(file_info) > 1 else None
    file_id = file_info[2] if len(file_info) > 2 else None
    thumb_file_id = file_info[3] if len(file_info) > 3 else None

    owner_user_id = purchase_info[0] if purchase_info[0] else None
    fee = purchase_info[1] if purchase_info[1] else 0
    purchase_condition = purchase_info[2] if len(purchase_info) > 2 else None
    reply_text = ''
    answer_text = ''

    rows_kb: list[list[InlineKeyboardButton]] = []
    aes = AESCrypto(AES_KEY)
    encoded = aes.aes_encode(content_id)

    shared_url = f"https://t.me/{lz_var.bot_username}?start=f_-1_{encoded}"
    # # rows_kb.append([
    # #     InlineKeyboardButton(
    # #         text="⚠️ 我要打假",
    # #         url=f"https://t.me/{lz_var.UPLOADER_BOT_NAME}?start=s_{source_id}"
    # #     )
    # # ])

    rows_kb.append([
        InlineKeyboardButton(text="🔗 复制资源链结", copy_text=CopyTextButton(text=shared_url))
    ])

    feedback_kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)


    from_user_id = callback.from_user.id
    send_content_kwargs = dict(chat_id=from_user_id, video=file_id, caption=ret_content, parse_mode="HTML", protect_content=True, reply_markup=feedback_kb)
    sr = await lz_var.bot.send_video(**send_content_kwargs)
    await callback.answer("已开启亮点模式，点选介绍文字中的时间轴，可以直接跳转视频中到对应时间。", show_alert=True)


TZ_UTC8 = timezone(timedelta(hours=8))

def _today_ymd() -> str:
    return datetime.now(TZ_UTC8).strftime("%Y-%m-%d")









@router.callback_query(F.data.startswith("sora_redeem:"))
async def handle_redeem(callback: CallbackQuery, state: FSMContext):

    content_id = callback.data.split(":")[1]
    redeem_type = callback.data.split(":")[2] if len(callback.data.split(":")) > 2 else None #小懒觉会员
    extra_enc = callback.data.split(":")[3] if len(callback.data.split(":")) > 3 else None #额外条件

    # 先取用户 id：后面所有门槛判断都会用到
    from_user_id = callback.from_user.id

    # 默认值：避免 purchase_condition 缺失时 UnboundLocalError
    condition: dict = {}
    is_protect_content = False

    timer = SegTimer("handle_redeem", content_id=f"{content_id}")
    print(f"开始交易记录")


    timer.lap("2634 load_sora_content_by_id")
    result = await load_sora_content_by_id(int(content_id), state)
    # print("Returned==>:", result)

    ret_content, file_info, purchase_info = result
    source_id = file_info[0] if len(file_info) > 0 else None
    file_type = file_info[1] if len(file_info) > 1 else None
    file_id = file_info[2] if len(file_info) > 2 else None
    thumb_file_id = file_info[3] if len(file_info) > 3 else None

    owner_user_id = purchase_info[0] if purchase_info[0] else None
    fee = purchase_info[1] if purchase_info[1] else 0
    purchase_condition = purchase_info[2] if len(purchase_info) > 2 else None
    reply_text = ''
    answer_text = ''
    
    
    if ret_content.startswith("⚠️"):
        await callback.answer(f"⚠️ 当前无法兑换。{ret_content}", show_alert=True)
        # await lz_var.bot.send_message(chat_id=from_user_id, text=ret_content, parse_mode="HTML")
        return

    if purchase_condition:
        # 1) parse json
        try:
            if isinstance(purchase_condition, (dict, list)):
                # 有些链路可能已经是 dict（防御性处理）
                condition = purchase_condition if isinstance(purchase_condition, dict) else {}
            else:
                condition = json.loads(str(purchase_condition))
                if not isinstance(condition, dict):
                    condition = {}
        except Exception:
            condition = {}

        # 2) protect 标记（后面发货要用）
        is_protect_content = str(condition.get("protect", "")).strip() == "1"
        print(f"购买条件解析结果: {condition}, is_protect_content={is_protect_content}", flush=True)

    # 3) message_count 门槛（contribute_today）
    required_msg_count = int(condition.get("message_count") or 0)
    if required_msg_count > 0:
        today = _today_ymd()
        ui_count = await MySQLPool.get_contribute_today_count(from_user_id, today)
        if ui_count < required_msg_count:
            text = (
                f"你<u>今天</u>的<u>-发言数</u>需要超过 {required_msg_count} 点后，才可以兑换\n"
                f"目前你有 <b>{ui_count}</b> 活跃值 ( 今天内 )\n\n"
                "🎈 温馨小提醒:\n"
                "・发言可得活跃值，但一分钟只计一次\n"
                "・学院群所有版块都适用\n\n"
                "・兑换后会扣除本日活跃值\n\n"
                "‼️轻松聊天没问题，但若被判定为『恶意刷句数』，将会被取消资格或拉黑。\n"
                "🧠 什么是恶意刷句数？例如：\n"
                "・把一句话故意拆成多句发\n"
                "・狂刷 emoji、数字、标点\n"
                "・贴小说、歌词、复制文当发言\n"
                "・「现在要限制发言数了吗」「我要讲第几句」这类空气话\n\n"
                "✅ 系统允许自然聊天，但我们重视的是交流，而不是骗系统积分\n"
            )
            await lz_var.bot.send_message(chat_id=from_user_id, text=text, parse_mode=ParseMode.HTML)
            await callback.answer()
            return       

    # 4) stone 门槛（必须是某群成员）
    stone_chat_id = condition.get("stone")
    if stone_chat_id not in (None, "", 0, "0"):
        try:
            stone_chat_id_int = int(stone_chat_id)
        except Exception:
            stone_chat_id_int = 0

        if stone_chat_id_int:
            ok = await MySQLPool.is_chatgroup_member(from_user_id, stone_chat_id_int)
            if not ok:
                await lz_var.bot.send_message(chat_id=from_user_id, text="你需要参加且通过本期石汤头后，才可以兑换。")
                await callback.answer()
                return

    # 5) talking_task 门槛（咒语开局 + 累计发言数）
    required_task = int(condition.get("talking_task") or 0)
    if required_task > 0:
        word_pool = [
            "现在AI圈要看东西，基本都是在龙阳小馆。",
            "不夸张，混AI的十个有九个在龙阳小馆。",
            "有些作品一看就知道，是在龙阳小馆出来的。",
            "我现在刷AI，第一反应就是在龙阳小馆翻。",
            "讲真，在龙阳小馆看到的东西都比较稳。",
        ]
        spell_word = random.choice(word_pool)

        today = _today_ymd()
        task_count = await MySQLPool.get_talking_task_count(from_user_id, today)

        if task_count < required_task:
            # 用 CopyTextButton 提升“点一下复制咒语”的可用性
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="点击复制咒语", copy_text=CopyTextButton(text=spell_word))],
            ])

            text = (
                "🕵️‍♂️ 神秘资源任务 · 今日限定开启！\n\n"
                "这东西不容易拿，要靠智慧 + 嘴皮子 + 一点仪式感。\n\n"
                "🎯 任务启动条件如下：\n\n"
                "1️⃣ 先在群里任何角落大声宣布一句（点击下方按钮复制）：\n"
                f"「<code>{spell_word}</code>」\n"
                "这句话是通关咒语，不讲不算，讲了才开局！(重复讲会重置喔!)\n\n"
                "2️⃣ 从你说完这句起，接下来只要今天正常聊天，累计满 "
                f"{required_task} 发言，宝藏就向你敞开大门！\n\n"
                "💡 记住：\n"
                "一分钟内只能获得一次发言数，别刷屏，靠耐力。\n"
                "咒语之后的发言才会被记入，别走错顺序啦！\n"
                "重复讲咒语会重置发言数喔！一次只能做一个任务！\n\n"
                "📦 等你说满了，再来申请，神秘资源就会飞向你。\n\n"
                f"目前你有 <b>{task_count}</b> 次神秘任务的发言数 ( 今天内，每天午夜会重置 )"
            )
            await lz_var.bot.send_message(chat_id=from_user_id, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
            await callback.answer()
            return

    
    if not file_id and (file_type != 'a' and file_type !='album') :
        timer.lap("没有找到匹配记录")
        print("❌ 没有找到匹配记录 source_id")
        await callback.answer(f"👻 我们正偷偷的从院长的硬盘把这个资源搬出来，这段时间先看看别的资源吧。{file_type}", show_alert=True)
        # await callback.message.reply("👻 我们正偷偷的从院长的硬盘把这个资源搬出来，这段时间先看看别的资源吧。")
        # await lz_var.bot.delete_message(
        #     chat_id=callback.message.chat.id,
        #     message_id=callback.message.message_id
        # )
        return
    


    # ===== 小懒觉会员判断（SQL 已移至 lz_db.py）=====
    def _fmt_ts(ts: int | None) -> str:
        if not ts:
            return "未开通"
        tz = timezone(timedelta(hours=8))  # Asia/Singapore/UTC+8
        try:
            return datetime.fromtimestamp(int(ts), tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(ts)
    


    timer.lap("2687 get_latest_membership_expire")
    expire_ts_raw = await db.get_latest_membership_expire(from_user_id)
    now_utc = int(datetime.now(timezone.utc).timestamp())

    try:
        expire_ts_int = int(expire_ts_raw or 0)
    except (TypeError, ValueError):
        expire_ts_int = 0

    # 统一在会员判断之后再计算费用
    sender_fee = int(fee) * (-1)
    receiver_fee = int(int(fee) * (0.6))
    receiver_id = owner_user_id or 0

    if expire_ts_int >= now_utc:
        timer.lap("2753 是小懒觉会员")
        discount_amount = int(fee * lz_var.xlj_discount_rate)
        xlj_final_price = fee - discount_amount
        sender_fee = xlj_final_price * (-1)


    
    
    

    # 6) credit / author 这些“购买前门槛”建议也在交易前挡住（对齐你 PHP 逻辑）
    #    （只有当资源有价格/会扣分时才做）
    user_info = await MySQLPool.get_user_point_credit(from_user_id)
    try:
        user_point = int(user_info.get('point') or 0)
    except (TypeError, ValueError):
        user_point = 0

    user_credit = int(user_info.get("credit") or 0)

    required_credit = int(condition.get("credit") or 0) if condition else 0
    if required_credit > 0 and user_credit < required_credit:
        await lz_var.bot.send_message(
            chat_id=from_user_id,
            text=f"此资源门槛为 {required_credit} 分信用积分，你目前仅有 {user_credit} 分。还差一点点积累，暂时无法兑换。"
        )
        await callback.answer()
        return

    author_gate = condition.get("author") if condition else ""
    author_gate = str(author_gate).strip()
    print(f"author_gate={author_gate} {condition}", flush=True)
    if author_gate == "1":
        await lz_var.bot.send_message(
            chat_id=from_user_id,
            text="作者设定为需经过他的同意才能兑换，目前已通知作者，请耐心等待回应。\n\n如果作者同意兑换，你将会收到通知。"
        )
        await callback.answer()
        return



    if( user_point + sender_fee < 0) and (int(owner_user_id or 0) != int(from_user_id)):
        await callback.answer("⚠️ 你的积分余额不足，无法兑换此资源，请先赚取或充值积分。", show_alert=True)
        return


    # 7) 额外问答门槛（在真正扣分/发货前拦截）
    #    - condition.question 存在：要求用户回答
    #    - 回答 == condition.answer：继续兑换
    #    - 回答错误：不扣分、不发货
    if condition and str(condition.get("question") or "").strip():
        question = str(condition.get("question") or "").strip()
        answer = str(condition.get("answer") or "").strip()
        # extra_type = 
        print(f"extra_enc={extra_enc}, verify={ActionGate.verify_extra(extra_enc, callback.from_user.id, content_id)}", flush=True)
        if not extra_enc or not ActionGate.verify_extra(extra_enc, callback.from_user.id, content_id):
            await state.set_state(RedeemFSM.waiting_for_condition_answer)
            await state.update_data({
                "redeem_condition": {"question": question, "answer": answer},
                "redeem_context": {"content_id": content_id, "redeem_type": redeem_type},
            })

            await lz_var.bot.send_message(
                chat_id=from_user_id,
                text=f"<blockquote>❓ 兑换验证</blockquote>\n\n{question}\n\n<i>请直接回复答案：</i>",
                parse_mode="HTML"
            )
            await callback.answer()
            return
    
    if not expire_ts_int or  expire_ts_int < now_utc:
        # 未开通/找不到记录 → 用原价，提示并给两个按钮，直接返回
        human_ts = _fmt_ts(expire_ts_int)
        if not expire_ts_int:
            text = (
                f"你目前不是小懒觉会员，或是会员已过期。将以原价 {fee} 兑换此资源\r\n\r\n"
                f"目前你的小懒觉会员期有效期为 {human_ts}，可点选下方按钮更新或兑换小懒觉会员"
            )
        else:
            text = (
                "你的小懒觉会员过期或未更新会员限期(会有时间差)。\r\n\r\n"
                f"目前你的小懒觉会员期有效期为 {human_ts}，可点选下方按钮更新或兑换小懒觉会员"
            )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="更新小懒觉会员期", callback_data="xlj:update")],
            [InlineKeyboardButton(
                text="兑换小懒觉会员 ( 💎 800 )",
                url="https://t.me/xljdd013bot?start=join_xiaolanjiao_act"
            )],
        ])
        timer.lap("不是小懒觉会员")
        notify_msg = await callback.message.reply(text, reply_markup=kb)
        
        spawn_once(
            f"notify_msg:{notify_msg.message_id}",
            lambda: Media.auto_self_delete(notify_msg, 7)
        )

        if( redeem_type == 'xlj'):
            await callback.answer()
            return
    elif int(expire_ts_int) >= now_utc:
        timer.lap("2753 是小懒觉会员")
        
        try:
            reply_text = f"你是小懒觉会员，此资源优惠 {discount_amount} 积分，只需要支付 {xlj_final_price} 积分。\r\n\r\n目前你的小懒觉会员期有效期为 {_fmt_ts(expire_ts)}"
        except Exception:
            pass



    timer.lap("2771 开始交易记录")
    result = await MySQLPool.transaction_log({
        'sender_id': from_user_id,
        'receiver_id': receiver_id,
        'transaction_type': 'confirm_buy',
        'transaction_description': source_id,
        'sender_fee': sender_fee,
        'receiver_fee': receiver_fee
    })
    timer.lap("2780 结束")





    # print(f"🔍 交易记录结果: {result}", flush=True)

    
    # ✅ 兜底：确保 result & user_info 可用
    if not isinstance(result, dict):
        await callback.answer("⚠️ 交易服务暂不可用，请稍后再试。", show_alert=True)
        return





    # print(f"💰 交易结果: {result}, 交易后用户积分余额: {user_point}", flush=True)
    timer.lap(f"判断交易结果{result.get('status')}")
    if result.get('status') == 'exist' or result.get('status') == 'insert' or result.get('status') == 'reward_self':

        if result.get('status') == 'exist':
            reply_text += f"✅ 你已经兑换过此资源，不需要扣除积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {user_point}。"

            # print(f"💬 回复内容: {reply_text}", flush=True)
        elif result.get('status') == 'insert':
            
            reply_text += f"✅ 兑换成功，已扣除 {sender_fee} 积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {(user_point+sender_fee)}。"

            available_content_length = 20
            content_preview = ret_content[:available_content_length]
            if len(ret_content) > available_content_length:
                content_preview += "..."

            aes = AESCrypto(AES_KEY)
            encoded = aes.aes_encode(content_id)

            try:
                timer.lap("交易通知")

                #  $group_text = "<a href='tg://user?id=" . $user_info['id'] . "'>" . $user_title . "</a>";
                receiver_fullname = await MySQLPool.get_user_name(receiver_id)
                sender_fullname = await MySQLPool.get_user_name(from_user_id)
                share_url = f"https://t.me/{lz_var.bot_username}?start=f_-1_{encoded}"
                owner_html = f"<a href='tg://user?id={receiver_id}'>{receiver_fullname}</a>"
                sender_html = f"<a href='tg://user?id={from_user_id}'>{sender_fullname}</a>"
                notice_text_author = f"🔔 你分享的资源<a href='{share_url}'>「{content_preview}」</a> 已被用户兑换，获得 {receiver_fee} 积分分成！"
                notice_text_manager = f"🔔 {owner_html} 分享的资源<a href='{share_url}'>「{content_preview}」</a> 已被用户 {sender_html} 兑换，获得 {receiver_fee} 积分分成！"

               

                if receiver_id != KEY_USER_ID :
                    timer.lap(f"传送给管理员")
                    await lz_var.bot.send_message(
                        parse_mode="HTML",
                        chat_id=KEY_USER_ID,
                        text=notice_text_manager,
                        disable_web_page_preview=True
                    )   
                
                if receiver_id != 0 and receiver_id != 666666:
                    try:
                        timer.lap(f"传送给资源拥有者 {receiver_id}")
                        ret = await lz_var.bot.send_message(
                            parse_mode="HTML",
                            chat_id=receiver_id,
                            text=notice_text_author,
                            disable_web_page_preview=True
                        )

                        print(f"ret={ret}")
                    except Exception as e:
                        print(f"❌ 发送兑换通知给资源拥有者失败: {e}", flush=True)



               
            except Exception as e:
                print(f"❌ 发送兑换通知失败: {e}", flush=True)

       
        elif result.get('status') == 'reward_self':
            
            reply_text += f"✅ 这是你自己的资源"
            if user_point > 0:
                reply_text += f"，当前积分余额: {(user_point+sender_fee)}。"

        feedback_kb = None
        if UPLOADER_BOT_NAME and source_id:

           
            rows_kb: list[list[InlineKeyboardButton]] = []

            rows_kb.append([
                InlineKeyboardButton(
                    text="⚠️ 我要打假",
                    url=f"https://t.me/{UPLOADER_BOT_NAME}?start=s_{source_id}"
                )
            ])

            if file_type == "video" or file_type == "v":
                #只有视频有亮点模式
                pattern = r"\b\d{2}:\d{2}\b"
                matches = re.findall(pattern, ret_content)
                print(f"{matches} {len(matches)}", flush=True)
                if len(matches) >= 3:
                    rows_kb.append([
                        InlineKeyboardButton(
                            text="⚡️ 亮点模式",
                            callback_data=f"keyframe:{content_id}"
                        )
                    ])


            feedback_kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)

       


        try:
            send_content_kwargs = dict(chat_id=from_user_id, reply_markup=feedback_kb, protect_content=is_protect_content)
            if callback.message.message_id is not None:
                send_content_kwargs["reply_to_message_id"] = callback.message.message_id

            timer.lap(f"资源类型{file_type}处理")

            if file_type == "album" or file_type == "a":
               
                productInfomation = await get_product_material(content_id)
                if not productInfomation:
                     await callback.answer(f"资源同步中，请稍等一下再试，请先看看别的资源吧 {content_id}", show_alert=True)
                     return   

                result = await Media.send_media_group(callback, productInfomation, 1, content_id, source_id, protect_content=is_protect_content)
                
                if result and not result.get('ok'):
                    await callback.answer(result.get('message'), show_alert=True)
                    return
            elif file_type == "photo" or file_type == "p":  
                send_content_kwargs["photo"] = file_id
                sr = await lz_var.bot.send_photo(**send_content_kwargs)
            elif file_type == "video" or file_type == "v":
                send_content_kwargs["video"] = file_id
                sr = await lz_var.bot.send_video(**send_content_kwargs)
            elif file_type == "document" or file_type == "d":
                send_content_kwargs["document"] = file_id
                sr = await lz_var.bot.send_document(**send_content_kwargs)
        except Exception as e:
            print(f"❌ (2886): {e}")
            # 发出是哪一行错了
            print(traceback.format_exc(), flush=True)
          
            return  


        timer.lap(f"结束全部流程 {reply_text}")
        await callback.answer(reply_text, show_alert=True)
        
        # TODO : 删除兑换消息，改为复制一条新的消息
        # new_message = await lz_var.bot.copy_message(
        #     chat_id=callback.message.chat.id,
        #     from_chat_id=callback.message.chat.id,
        #     message_id=callback.message.message_id,
        #     reply_markup=callback.message.reply_markup
        # )

        # print(f"new_message:{new_message}", flush=True)
        # NewMessage = {
        #     "chat": {"id":callback.message.chat.id},
        #     "message_id": new_message.message_id
        # }
        # await MenuBase.set_menu_status(state, {
        #     "menu_message": NewMessage,
        #     "current_message": new_message,
        #     "current_chat_id": callback.message.chat.id,
        #     "current_message_id": new_message.message_id
        # })

        # await lz_var.bot.delete_message(
        #     chat_id=callback.message.chat.id,
        #     message_id=callback.message.message_id
        # )

        return
    elif result.get('status') == 'insufficient_funds':
       
        reply_text = f"❌ 你的积分不足 ( {user_point} ) ，无法兑换此资源 ( {abs(sender_fee)} )。"
        await callback.answer(reply_text, show_alert=True)
        # await callback.message.reply(reply_text, parse_mode="HTML")
        return


@router.message(RedeemFSM.waiting_for_condition_answer)
async def handle_redeem_condition_answer(message: Message, state: FSMContext):
    """
    处理兑换问答门槛。
    """
    data = await state.get_data()
    cond = data.get("redeem_condition") or {}
    ctx = data.get("redeem_context") or {}
    # ✅ 验证通过：给一个“继续兑换”按钮，让用户重新点一次
    content_id = ctx.get("content_id")
    redeem_type = ctx.get("redeem_type")
    user_id = message.from_user.id
    

    expected = str(cond.get("answer") or "").strip()
    got = (message.text or "").strip()

    # 先退出等待态，避免重复触发
    await state.clear()

    if not expected:
        await message.answer("⚠️ 兑换验证配置异常（缺少答案），请稍后再试。")
        return

    if got != expected:
        await message.answer("❌ 回答不正确，本次兑换已取消（未扣分）。")
        return



    try:
        if not content_id:
            await message.answer("⚠️ 系统忙碌（缺少兑换上下文），请回到资源页重新点击兑换。")
            return
        
        # callback_data 设计：加一个 gate 参数，避免再次弹出问题
        # 形式：sora_redeem:{content_id}:{redeem_type}:gate:{token}
        parts = [f"sora_redeem:{int(content_id)}"]
        if redeem_type:
            parts.append(str(redeem_type))
        else:
            parts.append("none")
        # gate 标记
        
        extra_str = ActionGate.make_extra(user_id, content_id)
        parts.append(f"{extra_str}")
        
        cb = ":".join(parts)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"验证通过，继续兑换", callback_data=cb)],
        ])

        await message.answer("✅ 回答正确。请点击下方按钮继续兑换：", reply_markup=kb)

        
    except Exception as e:
        print(f"❌ continue redeem after condition failed: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        await message.answer("⚠️ 系统忙碌，请稍后再试。")

async def _build_mediagroup_box(page,source_id,content_id,material_status):
   
    if material_status:
        total_quantity = material_status.get("total", 0)
        box_dict = material_status.get("box", {})  # dict: {1:{...}, 2:{...}}
        # 盒子数量（组数）
        box_quantity = len(box_dict)  

        # 生成 1..N 号按钮；每行 5 个
        rows_kb: list[list[InlineKeyboardButton]] = []
        current_row: list[InlineKeyboardButton] = []

        # 若想按序号排序，确保顺序一致
        for box_id, meta in sorted(box_dict.items(), key=lambda kv: kv[0]):
            if box_id == page:
                show_tag = "✅ "
            else:
                show_tag = "✅ " if meta.get("show") else ""
            quantity = int(meta.get("quantity", 0))
            current_row.append(
                InlineKeyboardButton(
                    text=f"{show_tag}{box_id}",
                    callback_data=f"media_box:{content_id}:{box_id}:{quantity}"  # 带上组号
                )
            )
            if len(current_row) == 5:
                rows_kb.append(current_row)
                current_row = []

        # 收尾：剩余不足 5 个的一行
        if current_row:
            rows_kb.append(current_row)

        # 追加反馈按钮（单独一行）
        rows_kb.append([
            InlineKeyboardButton(
                text="⚠️ 我要打假",
                url=f"https://t.me/{UPLOADER_BOT_NAME}?start=s_{source_id}"
            )
        ])

        feedback_kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)

        # 计算页数：每页 10 个（与你 send_media_group 的分组一致）
        # 避免整除时多 +1，用 (total+9)//10 或 math.ceil
        pages = (total_quantity + 9) // 10 if total_quantity else 0
        text = f"💡当前 {box_quantity}/{total_quantity} 个，第 1/{max(pages,1)} 页"
        return { "feedback_kb": feedback_kb, "text": text}


@router.callback_query(F.data.startswith("media_box:"))
async def handle_media_box(callback: CallbackQuery, state: FSMContext):
    print(f"{callback.data}", flush=True)
    _, content_id, box_id, quantity = callback.data.split(":")
    product_row = await db.search_sora_content_by_id(int(content_id))
    # product_info = product_row.get("product_info") or {}
    source_id = product_row.get("source_id") or ""

    # sora_content = AnanBOTPool.search_sora_content_by_id(content_id)
    # source_id = sora_content.get("source_id") if sora_content else ""
    # source_id = get_content_id_by_file_unique_id(content_id)
    # ===== 你原本的业务逻辑（保留） =====
    # rows = await AnanBOTPool.get_album_list(content_id=int(content_id), bot_name=lz_var.bot_username)
               
    productInfomation = await get_product_material(content_id)

    await Media.send_media_group(callback, productInfomation, box_id, content_id,source_id)
    await callback.answer()

@router.callback_query(F.data.startswith("media_box_old:"))
async def handle_media_box(callback: CallbackQuery, state: FSMContext):
    # reply_to_message_id = callback.message.reply_to_message.message_id
    _, content_id, box_id, quantity = callback.data.split(":")
    from_user_id = callback.from_user.id

    # ===== 你原本的业务逻辑（保留） =====
    source_id = None
    productInfomation = await get_product_material(content_id)
    material_status = productInfomation.get("material_status")
    total_quantity = material_status.get("total", 0)
    box_dict = material_status.get("box", {})  # dict: {1:{...}, 2:{...}}
    # 盒子数量（组数）
    box_quantity = len(box_dict)  


    return_media = await _build_mediagroup_box(box_id, source_id, content_id, material_status)
    feedback_kb = return_media.get("feedback_kb")
    text = return_media.get("text")
    quantity = int(quantity) if quantity else 0

    rows = productInfomation.get("rows", [])
    await lz_var.bot.send_media_group(
        chat_id=from_user_id,
        media=rows[(int(box_id)-1)],
        # reply_to_message_id=reply_to_message_id
    )
    # ==================================

    msg = callback.message
    original_text = msg.text or msg.caption or ""

    # ✅ 2) 取出所有按钮，找到 text 等于 box_id（或 callback_data 末段等于 box_id）的按钮，把文字加上 "[V]"
    kb = msg.reply_markup
    new_rows: list[list[InlineKeyboardButton]] = []

    if kb and kb.inline_keyboard:
        for row in kb.inline_keyboard:
            new_row = []
            for btn in row:
                # 去掉已有的 "[V]"，避免重复标记
                base_text = btn.text.lstrip()
                if base_text.startswith("✅"):
                    _, _, _, btn_quantity = btn.callback_data.split(":")
                    quantity = quantity+ int(btn_quantity)
                   
                    print(f"✅{btn}") 
                    # base_text_pure = base_text[3:].lstrip()
                    # sent_quantity = len(material_status.get("box",{}).get(int(base_text_pure),{}).get("file_ids",[])) if material_status else 0

                # 判断是否为目标按钮（文字等于 box_id 或 callback_data 的最后一段等于 box_id）
                is_target = (base_text == box_id)
                if not is_target and btn.callback_data:
                    try:
                        is_target = (btn.callback_data.split(":")[-1] == box_id)
                    except Exception:
                        is_target = False

                # 目标按钮加上 "[V]" 前缀，其他按钮保持/移除多余的前缀
                new_btn_text = f"✅ {base_text}" if is_target else base_text

                # 用 pydantic v2 的 model_copy 复制按钮，仅更新文字，其他字段（url、callback_data 等）保持不变
                new_btn = btn.model_copy(update={"text": new_btn_text})
                new_row.append(new_btn)
            new_rows.append(new_row)

    new_markup = InlineKeyboardMarkup(inline_keyboard=new_rows) if new_rows else kb


    # ✅ 1) 取出 callback 内原消息文字，并在后面加 "123"
    
    
    new_text = f"💡当前 {quantity}/{total_quantity} 个，第 {box_id}/{box_quantity} 页"

    # ✅ 3) 编辑这条原消息（有文字用 edit_text；若是带 caption 的媒体则用 edit_caption）
    try:
        if(total_quantity > quantity):
            await lz_var.bot.send_message(
                chat_id=from_user_id,
                text=new_text,
                reply_markup=new_markup,
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id
            )

        await msg.delete()

        # if msg.text is not None:
           
        #     await msg.edit_text(new_text, reply_markup=new_markup)
        # else:
        #     await msg.edit_caption(new_text, reply_markup=new_markup)
    except Exception as e:
        # 可选：记录一下，避免因“内容未变更”等报错中断流程
        print(f"[media_box] edit message failed: {e}", flush=True)

    # 可选：给个轻量反馈，去掉“加载中”状态
    await callback.answer()



@router.callback_query(F.data == "xlj:update")
async def handle_update_xlj(callback: CallbackQuery, state: FSMContext):
    """
    同步当前用户在 MySQL 的 xlj 会员记录到 PostgreSQL：
      1) MySQL: membership where course_code='xlj' AND user_id=? AND expire_timestamp > now
      2) 写入 PG：ON CONFLICT (membership_id) UPSERT
    """
    user_id = callback.from_user.id
    tz = timezone(timedelta(hours=8))
    now_ts = int(datetime.now(timezone.utc).timestamp())
    now_human = datetime.fromtimestamp(now_ts, tz).strftime("%Y-%m-%d %H:%M:%S")

    # 1) 从 MySQL 取数据（仅在 lz_mysql.py 内使用 MySQLPool）
    try:
        rows = await MySQLPool.fetch_valid_xlj_memberships()
    except Exception as e:
        await callback.answer(f"同步碰到问题，请稍后再试 [错误代码 1314 ]", show_alert=True)
        print(f"Error1314:{e}")
        return

    if not rows:
        print(
            f"目前在 MySQL 没有可同步的有效『小懒觉会员』记录（xlj）。\n"
            f"当前时间：{now_human}\n\n"
            f"如已完成兑换，请稍候片刻再尝试更新。"
        )
        return

    # 2) 批量写入 PG（仅按 membership_id 冲突）
    sync_ret = await db.upsert_membership_bulk(rows)
    if not sync_ret.get("ok"):
        await callback.answer(f"同步数据库碰到问题，请稍后再试 [错误代码 1329 ]", show_alert=True)
        print(f"Error1329:写入 PostgreSQL 失败：{sync_ret.get('error')}")
        return 

    # 3) 只取当前用户的最大 expire_timestamp
    user_rows = [r for r in rows if str(r.get("user_id")) == str(user_id)]
    if not user_rows:
        await callback.message.reply("✅ 已同步，但你目前没有有效的小懒觉会员记录。")
        return

    max_expire = max(int(r["expire_timestamp"]) for r in user_rows if r.get("expire_timestamp"))
    human_expire = datetime.fromtimestamp(max_expire, tz).strftime("%Y-%m-%d %H:%M:%S")

    await callback.message.reply(
        f"✅ 会员信息已更新。\n"
        f"你的小懒觉会员有效期截至：{human_expire}"
    )

   

# 📌 功能函数：根据 sora_content id 载入资源
async def load_sora_content_by_id(content_id: int, state: FSMContext, search_key_index=None, search_from : str = '') -> str:
    convert = UnitConverter()  # ✅ 实例化转换器
    # print(f"content_id = {content_id}, search_key_index={search_key_index}, search_from={search_from}")
    record = await db.search_sora_content_by_id(content_id)

    print(f"\n\n\n🔍 载入 ID: {content_id}, Record: {record}", flush=True)
    if record:
       
         # 取出字段，并做基本安全处理
        fee = record.get('fee', lz_var.default_point)
        if fee is None or fee < 0:
            fee = lz_var.default_point
            
        owner_user_id = record.get('owner_user_id', 0)

        valid_state = record.get('valid_state', '')
        review_status = record.get('review_status', '')
        record_id = record.get('id', '')
        tag = record.get('tag', '')
        file_size = record.get('file_size', '')
        duration = record.get('duration', '')
        source_id = record.get('source_id', '')
        file_type = record.get('file_type', '')
        content = record.get('content', '')
        file_id = record.get('file_id', '')
        thumb_file_unique_id = record.get('thumb_file_unique_id', '')
        thumb_file_id = record.get('thumb_file_id', '')
        product_type = record.get('product_type')  # free, paid, vip
        file_password = record.get('file_password', '')
        if product_type is None:
            product_type = file_type  # 默认付费

        purchase_condition = record.get('purchase_condition', '')  
        # print(f"{record}")

        # print(f"🔍 载入 ID: {record_id}, Source ID: {source_id}, thumb_file_id:{thumb_file_id}, File Type: {file_type}\r\n")

        # ✅ 若 thumb_file_id 为空，则给默认值
        if not thumb_file_id and thumb_file_unique_id != None:
            print(f"🔍 没有找到 thumb_file_id，背景尝试从 thumb_file_unique_id( {thumb_file_unique_id} )获取")
            spawn_once(
                f"thumb_file_id:{thumb_file_unique_id}",
                lambda: Media.fetch_file_by_file_uid_from_x(state, thumb_file_unique_id, 10)
            )

           
            # 设置当下要获取的 thumb 是什么,若从背景取得图片时，可以直接更新 (fetch_thumb_file_unique_id 且 menu_message 存在)
            # state_data = await state.get_data()
            # menu_message = state_data.get("menu_message")
            state_data = await MenuBase.get_menu_status(state)
     
            if state_data.get("current_message"):
                print(f"🔍 设置 fetch_thumb_file_unique_id: {thumb_file_unique_id}，并丢到后台获取")
               
                await MenuBase.set_menu_status(state, {
                    "fetch_thumb_file_unique_id": f"{thumb_file_unique_id}"
                })
            else:
                print("❌ menu_message 不存在，无法设置 fetch_thumb_file_unique_id")
            
    
        if not thumb_file_id:
            print("❌ 在延展库没有封面图，先用预设图")
            



            if file_id and not thumb_file_unique_id and (file_type == "video" or file_type == "v"):
                print(f"这是视频，有file_id，试著取它的默认封面图")
                spawn_once(
                    f"create_thumb_file_id:{file_id}",
                    lambda: handle_update_thumb(content_id, file_id ,state)
                )

            # default_thumb_file_id: list[str] | None = None  # Python 3.10+
            if lz_var.default_thumb_file_id:
                # 令 thumb_file_id = lz_var.default_thumb_file_id 中的随机值
                thumb_file_id = random.choice(lz_var.default_thumb_file_id)
              
                # 这里可以选择是否要从数据库中查找
            else:
              
                file_id_list = await db.get_file_id_by_file_unique_id(lz_var.default_thumb_unique_file_ids)
                # 令 lz_var.thumb_file_id = file_id_row
                if file_id_list:
                    lz_var.default_thumb_file_id = file_id_list
                    thumb_file_id = random.choice(file_id_list)
                else:
                    print("❌ 没有找到 default_thumb_unique_file_ids,增加扩展库中")
                    # 遍历 lz_var.default_thumb_unique_file_ids
                    for unique_id in lz_var.default_thumb_unique_file_ids:
                        
                        # 进入等待态（最多 10 秒）
                        thumb_file_id = await Media.fetch_file_by_file_uid_from_x(state, unique_id, 10)
                        print(f"✅ 取到的 thumb_file_id: {thumb_file_id}")
                    # 处理找不到的情况
                    
       
        list_text = ''

        if content:
            content = LZString.dedupe_cn_sentences(content)
            content = LZString.clean_text(content)

        if product_type == "album" or product_type == "a":
            
            try:
                results = await db.get_album_list(content_id, lz_var.bot_username)
                
                list_text = await Tplate.list_template(results)
                content = content +  "\r\n" + list_text['opt_text'] 
            except Exception as e:
                print(f"❌ 载入相册列表内容失败: {e}")
                #列出出错的行数
                traceback.print_exc()
                content = content +  "\r\n" + "⚠️ 相册内容加载失败，请稍后再试。"
            
            
        # print("🔍 处理标签和内容长度")          


        ret_content = ""
        tag_length = 0
        max_total_length = 1000  # 预留一点安全余地，不用满 1024
               
        if tag:
            ret_content += f"{record['tag']}\n\n"


        if(file_password  and file_password.strip() != ''):
            ret_content += f"🔐 密码: <code>{file_password}</code>  (点选复制)\n\n"

        profile = ""
        if file_size and (product_type != "album" and product_type != "a"):
            # print(f"🔍 资源大小: {file_size}")
            label_size = convert.byte_to_human_readable(file_size)
            ret_content += f"📄 {label_size}  "
            profile += f"📄 {label_size}  "

        if duration and (product_type != "album" and product_type != "a"):
            label_duration = convert.seconds_to_hms(duration)
            ret_content += f"🕙 {label_duration}  "
            profile += f"🕙 {label_duration}  "

        # space = ""
        # meta_line = profile or ""
        # meta_len = len(meta_line)
        # target_len = 10  # 你可以设目标行长度，比如 55 字符
        # if meta_len < target_len:
        #     pad_len = target_len - meta_len
        #     space += "ㅤ" * pad_len  # 用中点撑宽（最通用，Telegram 不会过滤）
        # ret_content += f"{space}"


        if search_key_index:
            # print(f"🔍 载入搜索附加信息: {search_key_index} from {search_from}")
            if search_from == "cm" or search_from == "cf":
                
                clt_info = await MySQLPool.get_user_collection_by_id(collection_id=int(search_key_index))
                
                ret_content += f"\r\n🪟 资源橱窗: {clt_info.get('title')}\n\n"
            else:
                keyword = await db.get_keyword_by_id(int(search_key_index))
                if keyword:
                    ret_content += f"\r\n🔑 关键字: {keyword}\n\n"

        # print(f"ret_content before length {len(ret_content)}")

        if ret_content:
            tag_length = len(ret_content)
    
        # print(F"标签长度 {tag_length}", flush=True)
        if not file_id and source_id and (file_type != 'a' and file_type !='album') :
            # 不阻塞：丢到后台做补拉
       
            spawn_once(
                f"fild_id:{source_id}",
                lambda: Media.fetch_file_by_file_uid_from_x(state, source_id, 10 )
            ) 
            await MenuBase.set_menu_status(state, {
                "fetch_file_unique_id": f"{source_id}"
            })
        

        
        # print(f"tag_length {tag_length}")

        # 计算可用空间
        available_content_length = max_total_length - tag_length - 50  # 预留额外描述字符
        
        # print(f"长度 {available_content_length}", flush=True)
        # print(f"长度 {available_content_length}")


        # 裁切内容
        if content is None:
            content_preview = ""
        else:


            # print(f"原始内容长度 {len(content)}，可用长度 {available_content_length}") 
            content_preview = content[:available_content_length]
            if len(content) > available_content_length:
                content_preview += "..."
            # print(f"裁切后内容长度 {len(content_preview)}")

        if ret_content:
            ret_content = content_preview+"\r\n\r\n"+ret_content
        else:
            ret_content = content_preview
        
        # print(f"1847:🔍 载入 ID: {record_id}, Source ID: {source_id}, thumb_file_id:{thumb_file_id}, File Type: {file_type}\r\n")
        # ✅ 返回三个值

      
        '''
    
        审核状态
        0   编辑中(投稿者) o
        1   未通过审核(投稿者) v
        2   初审进行中 o
        3   通过初审,复审进行中 o
        4   经检举,初审进行中 v
        6   通过终核,上架进行中 o
        7   上架失败
        9   成功上架 
        10  文件死忙
        11  文件同步失败
        12 投稿重复-给出原投稿的跳转链接
        20  上传者自行下架




        '''
      
        print(f"valid_state:{valid_state}, review_status:{review_status}")
        
        if review_status == 4:
            ret_content = f"<b>⚠️ 这个资源已被举报，正在审核中，请慎重兑换 ⚠️ </b>\n\n{ret_content}"
        if review_status == 2:
            ret_content = f"<b>⚠️ 这个资源尚未审核通过，若要兑换请谨慎考虑 ⚠️ </b>\n\n{ret_content}"

        elif valid_state==20 or review_status in [1,20]:
            if review_status==1:
                ret_content = f"<b>😭 作者还没有正式发布，无法兑换或查看内容。 </b>"
            else:
                ret_content = "😭 该资源已被下架，无法兑换或查看内容。"    # 如果开头是 ⚠️ 会停
            
            source_id = None
            product_type = None
            file_id = None
            thumb_file_id = None

        
        return ret_content, [source_id, product_type, file_id, thumb_file_id], [owner_user_id, fee, purchase_condition]
        
    else:
        await sync_sora(content_id)
        warn = f"⚠️ 正在同步中，请稍后再试一次 ( ID : {content_id} )"
        empty_file_info = [None, None, None, None]
        empty_purchase_info = [None, 0, None]
        return warn, empty_file_info, empty_purchase_info
        
    

@router.message(Command("lexicon"))
async def handle_jieba_export(message: Message | None = None):
    await export_lexicon_files(message=message, output_dir=".", force=True)



   