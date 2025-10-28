import inspect
import functools
import traceback
import sys
from typing import Any, Callable
from typing import Callable, Awaitable, Any
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, CopyTextButton
from aiogram.filters import Command
from aiogram.enums import ContentType
from aiogram.utils.text_decorations import markdown_decoration
from aiogram.fsm.storage.base import StorageKey
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.exceptions import TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

 
from aiogram.types import (
    Message,
    BufferedInputFile,
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    InputMediaPhoto, 
    InputMediaVideo, 
    InputMediaDocument, 
    InputMediaAnimation
)


from aiogram.enums import ParseMode
from utils.unit_converter import UnitConverter
from utils.aes_crypto import AESCrypto
from utils.media_utils import Media
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Coroutine

import asyncio
import os
from lz_db import db
from lz_config import AES_KEY, ENVIRONMENT,META_BOT, RESULTS_PER_PAGE
import lz_var
import traceback
import random
from lz_main import load_or_create_skins
from handlers.lz_search_highlighted import build_pagination_keyboard

from utils.media_utils import Media
from utils.tpl import Tplate

from lz_mysql import MySQLPool
from ananbot_utils import AnanBOTPool 

from utils.product_utils import submit_resource_to_chat,get_product_material

import functools
import traceback
import sys








router = Router()

_background_tasks: dict[str, asyncio.Task] = {}

class LZFSM(StatesGroup):
    waiting_for_title = State()
    waiting_for_description = State()

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
            await asyncio.wait_for(coro, timeout=15)
        except Exception:
            print(f"🔥 background task failed for key={key}", flush=True)

    t = asyncio.create_task(_runner(), name=f"backfill:{key}")
    _background_tasks[key] = t
    t.add_done_callback(lambda _: _background_tasks.pop(key, None))


def spawn_once1(key: str, coro: "Coroutine"):
    """相同 key 的后台任务只跑一个；结束后自动清理。"""
    task = _background_tasks.get(key)
    if task and not task.done():
        return

    async def _runner():
        try:
            # 可按需加超时
            await asyncio.wait_for(coro, timeout=15)
        except Exception:
            print(f"🔥 background task failed for key={key}", flush=True)

    t = asyncio.create_task(_runner(), name=f"backfill:{key}")
    _background_tasks[key] = t
    t.add_done_callback(lambda _: _background_tasks.pop(key, None))

async def set_global_state(state: FSMContext, thumb_file_unique_id: str | None = None, menu_message: Message | None = None):
    storage = state.storage
    key = StorageKey(bot_id=lz_var.bot.id, chat_id=lz_var.x_man_bot_id , user_id=lz_var.x_man_bot_id )
    storage_data = await storage.get_data(key)
    if thumb_file_unique_id:
        storage_data["fetch_thumb_file_unique_id"] = f"{thumb_file_unique_id}"
    if menu_message:
        storage_data["menu_message"] = menu_message
        await state.update_data({
            "menu_message": menu_message
        })
    await storage.set_data(key, storage_data)

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
    photo: str | None = None
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
    try:
        if msg is None and (chat_id is None or message_id is None):
            # 没有 msg，也没提供 chat_id/message_id，无法定位消息
            return

        if chat_id is None:
            chat_id = msg.chat.id
        if message_id is None:
            message_id = msg.message_id

        # 判断是否为媒体消息（按优先顺序找出第一种存在的媒体属性）
        media_attr = next(
            (attr for attr in ["animation", "video", "photo", "document"] if getattr(msg, attr, None)),
            None
        )

        if media_attr:
            # ——————————— 有媒体的情况 ———————————
            if photo:
                # 明确要换图：用传入的 photo
                await lz_var.bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=message_id,
                    media=InputMediaPhoto(
                        media=photo,
                        caption=text,
                        parse_mode="HTML",
                    ),
                    reply_markup=reply_markup,
                )
            else:
                # 未传 photo：尝试“复用原媒体”
                if media_attr == "photo":
                    # Aiogram 的 Message.photo 是 PhotoSize 列表，取最后一项（最大尺寸）
                    try:
                        orig_photo_id = (msg.photo[-1].file_id) if getattr(msg, "photo", None) else None
                    except Exception:
                        orig_photo_id = None

                    if orig_photo_id:
                        # 用 edit_message_media + 原图，实现“换媒体但沿用原图 + 改 caption”
                        await lz_var.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=message_id,
                            media=InputMediaPhoto(
                                media=orig_photo_id,
                                caption=text,
                                parse_mode="HTML",
                            ),
                            reply_markup=reply_markup,
                        )
                    else:
                        # 兜底：拿不到原图 id，就仅改 caption
                        await lz_var.bot.edit_message_caption(
                            chat_id=chat_id,
                            message_id=message_id,
                            caption=text,
                            parse_mode="HTML",
                            reply_markup=reply_markup,
                        )
                else:
                    # 原媒体不是 photo（例如 animation/video/document）：
                    # 为避免 “can't use file of type ... as Photo” 错误，这里不强行换媒体，改为仅改 caption
                    await lz_var.bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=message_id,
                        caption=text,
                        parse_mode="HTML",
                        reply_markup=reply_markup,
                    )
        else:
            # ——————————— 无媒体的情况 ———————————
            await lz_var.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
            )

    except Exception as e:
        # 你也可以在这里加上 traceback 打印，或区分 TelegramBadRequest
        print(f"❌ 编辑消息失败: {e}", flush=True)



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
        print(f"❌ 编辑消息失败: {e}", flush=True)


@debug
async def handle_update_thumb(content_id, file_id):
    print(f"✅ [X-MEDIA] 需要为视频创建缩略图，正在处理...{lz_var.man_bot_id}", flush=True)
    await MySQLPool.init_pool()
    try:
        send_video_result = await lz_var.bot.send_video(chat_id=lz_var.man_bot_id, video=file_id)
        buf,pic = await Media.extract_preview_photo_buffer(send_video_result, prefer_cover=True, delete_sent=True)
        if buf and pic:
            try:
                buf.seek(0)  # ✅ 防止 read 到空

                # ✅ DB 前确保有池（双保险）
                await MySQLPool.ensure_pool()

                # 上传给仓库机器人，获取新的 file_id 和 file_unique_id
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

                # invalidate_cached_product(content_id)
                await AnanBOTPool.upsert_product_thumb(
                    content_id, thumb_file_unique_id, thumb_file_id, lz_var.bot_username
                )

                print("预览图更新中", flush=True)
            except Exception as e:
                print(f"⚠️ 用缓冲图更新封面失败：{e}", flush=True)
                    
        
        else:
            print(f"...⚠️ 提取缩图失败 for content_id: {content_id}", flush=True)

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
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔍 搜索", callback_data="search"),
            InlineKeyboardButton(text="🏆 排行", callback_data="ranking")
        ],
        [
            InlineKeyboardButton(text="📂 合集", callback_data="collection"),
            InlineKeyboardButton(text="🕑 我的历史", callback_data="my_history")
        ],
        # [InlineKeyboardButton(text="🎯 猜你喜欢", callback_data="guess_you_like")],
        [InlineKeyboardButton(text="📤 上传资源", url=f"https://t.me/{META_BOT}?start=upload")],
       
    ])

# == 搜索菜单 ==
def search_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 关键字搜索", callback_data="search_keyword")],
        [InlineKeyboardButton(text="🏷️ 标签筛选", callback_data="search_tag")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 排行菜单 ==
def ranking_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 近期火热资源排行板", callback_data="ranking_resource")],
        [InlineKeyboardButton(text="👑 近期火热上传者排行板", callback_data="ranking_uploader")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 合集菜单 ==
def collection_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 我的合集", callback_data="clt_my")],
        [InlineKeyboardButton(text="❤️ 我收藏的合集", callback_data="clt_favorite")],
        [InlineKeyboardButton(text="🛍️ 逛逛合集市场", callback_data="explore_marketplace")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])



# ========= 菜单构建 =========
def _build_clt_edit_keyboard(collection_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 合集主题", callback_data=f"clt:edit_title:{collection_id}")],
        [InlineKeyboardButton(text="📝 合集简介", callback_data=f"clt:edit_desc:{collection_id}")],
        [InlineKeyboardButton(text="👁 是否公开", callback_data=f"cc:is_public:{collection_id}")],
        [InlineKeyboardButton(text=f"🔙 返回合集信息{collection_id}", callback_data=f"clt:my:{collection_id}:0:k")]
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





# ===== 合集: 标题 =====

@router.callback_query(F.data.regexp(r"^clt:edit_title:\d+$"))
async def handle_clt_edit_title(callback: CallbackQuery, state: FSMContext):
    _, _, cid = callback.data.split(":")
    await state.update_data({
        "collection_id": int(cid),
        "anchor_chat_id": callback.message.chat.id,
        "anchor_msg_id": callback.message.message_id,
        "anchor_message": callback.message
    })
    print(f"{callback.message.chat.id} {callback.message.message_id}")
    await state.set_state(LZFSM.waiting_for_title)
    await _edit_caption_or_text(
        callback.message,
        text="📝 请输入标题（长度 ≤ 255，可包含中文、英文或符号）：",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"clt:edit:{cid}:0:tk")]
        ])
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
    await _build_clt_edit(cid, anchor_message)
    await state.clear()

# ===== 合集 : 简介 =====

@router.callback_query(F.data.regexp(r"^clt:edit_desc:\d+$"))
async def handle_clt_edit_desc(callback: CallbackQuery, state: FSMContext):
    _, _, cid = callback.data.split(":")
    await state.update_data({
        "collection_id": int(cid),
        "anchor_message": callback.message,
        "anchor_chat_id": callback.message.chat.id,
        "anchor_msg_id": callback.message.message_id,
    })
    await state.set_state(LZFSM.waiting_for_description)
    await _edit_caption_or_text(
        callback.message,
        text="🧾 请输入这个合集的介绍：",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"clt:edit:{cid}:0:tk")]
        ])
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
    await _build_clt_edit(cid, anchor_message)
    await state.clear()


# ========= 合集:是否公开 =========

@router.callback_query(F.data.regexp(r"^cc:is_public:\d+$"))
async def handle_cc_is_public(callback: CallbackQuery):
    _, _, cid = callback.data.split(":")
    cid = int(cid)

    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    is_public = rec.get("is_public") if rec else None

    text = "👁 请选择这个合集是否可以公开："
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
async def handle_cc_public_set(callback: CallbackQuery):
    _, _, cid, val = callback.data.split(":")
    cid, is_public = int(cid), int(val)
    await MySQLPool.update_user_collection(collection_id=cid, is_public=is_public)
    
    await _build_clt_edit(cid, callback.message)

    # rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    # await callback.message.edit_reply_markup(reply_markup=is_public_keyboard(cid, rec.get("is_public")))
    await callback.answer("✅ 已更新可见性设置")

# ========= 合集:返回（从输入页回设置菜单 / 从“我的合集”回合集主菜单） =========

# 可用 clt:edit 取代 TODO
@router.callback_query(F.data.regexp(r"^cc:back:\d+$"))
async def handle_cc_back(callback: CallbackQuery):
    _, _, cid = callback.data.split(":")
    cid = int(cid)
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名合集"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    await _edit_caption_or_text(
        callback.message,
        text=f"当前设置：\n• ID：{cid}\n• 标题：{title}\n• 公开：{pub}\n• 简介：{_short(desc,120)}\n\n请选择要设置的项目：",
        reply_markup=_build_clt_edit_keyboard(cid)
    )



# == 历史菜单 ==
def history_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 我的上传", callback_data="history_update")],
        [InlineKeyboardButton(text="💎 我的兑换", callback_data="history_redeem")],
        [InlineKeyboardButton(text="🗑️ 收藏合集", callback_data="clt_my")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])



# == 历史记录选项响应 ==
@router.callback_query(F.data.in_(["history_update", "history_redeem"]))
async def handle_history_update(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    page = 0

    await state.update_data({
        "menu_message": callback.message
    })

    if callback.data == "history_update":
        callback_function = 'ul_pid'
        keyword_id = user_id
        photo = lz_var.skins['history_update']['file_id']
       
    elif callback.data == "history_redeem":
        callback_function = 'fd_pid'
        keyword_id = user_id
        photo = lz_var.skins['history_redeem']['file_id']
   
   

    pg_result = await _build_pagination(callback_function, keyword_id, page)
    if not pg_result.get("ok"):
        await callback.answer(pg_result.get("message"), show_alert=True)
        return

    await _edit_caption_or_text(
        photo=photo,
        msg=callback.message,
        text=pg_result.get("text"), 
        reply_markup =pg_result.get("reply_markup")
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
async def handle_pagination(callback: CallbackQuery):
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


    pg_result = await _build_pagination(callback_function, keyword_id, page)
    # print(f"pg_result: {pg_result}", flush=True)
    if not pg_result.get("ok"):
        await callback.answer(pg_result.get("message"), show_alert=True)
        return

    await _edit_caption_or_text(
        photo=photo,
        msg=callback.message,
        text=pg_result.get("text"),
        reply_markup=pg_result.get("reply_markup")
    )


    await callback.answer()

    

async def _build_pagination(callback_function, keyword_id:int | None = -1, page:int | None = 0):
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
        result = await MySQLPool.search_history_redeem(keyword_id)
        if not result:
            return {"ok": False, "message": "⚠️ 没有找到任何兑换纪录"}
    elif callback_function in {"ul_pid"}:
        result = await MySQLPool.search_history_upload(keyword_id)
        if not result:
            return {"ok": False, "message": "⚠️ 没有找到任何上传纪录"}            

    start = page * RESULTS_PER_PAGE
    end = start + RESULTS_PER_PAGE
    sliced = result[start:end]
    has_next = end < len(result)
    has_prev = page > 0
    
    text = await render_results(sliced, keyword_id, page, total=len(result), per_page=RESULTS_PER_PAGE, callback_function=callback_function)

    reply_markup=build_pagination_keyboard(keyword_id, page, has_next, has_prev, callback_function)

    return {"ok": True, "text": text, "reply_markup": reply_markup}



# == 猜你喜欢菜单 ==
def guess_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 查看推荐资源", callback_data="view_recommendations")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 资源上传菜单 ==
def upload_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 上传资源", url=f"https://t.me/{META_BOT}?start=upload")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])


# == 启动指令 == # /id 360242
@router.message(Command("id"))
async def handle_search_by_id(message: Message, state: FSMContext, command: Command = Command("id")):
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        # ✅ 调用并解包返回的三个值
        result = await load_sora_content_by_id(int(args[1]), state)
        print("Returned==>:", result)

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
async def handle_search_s(message: Message, state: FSMContext, command: Command = Command("ss")):
    # 删除 /ss 这个消息
    try:
        await message.delete()
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter) as e:
        print(f"❌ 删除 /s 消息失败: {e}", flush=True)
    pass

    if ENVIRONMENT != "dev":
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("请输入关键词： /s 正太 钢琴")
        return
    
    keyword = parts[1]

    print(f"🔍 搜索关键词: {keyword}", flush=True)

    await db.insert_search_log(message.from_user.id, keyword)
    result = await db.upsert_search_keyword_stat(keyword)
    
    keyword_id = await db.get_search_keyword_id(keyword)

    list_info = await _build_pagination(callback_function="pageid", keyword_id=keyword_id)
    if not list_info.get("ok"):
        await message.answer(list_info.get("message"), show_alert=True)
        return

    date = await state.get_data()
    handle_message = date.get("menu_message")

    # print(f"handle_message={handle_message}",flush=True)

    if handle_message:
        try:
            await _edit_caption_or_text(
                photo=lz_var.skins['search_keyword']['file_id'],
                msg=handle_message,
                text=list_info.get("text"),
                reply_markup=list_info.get("reply_markup"),
            )
            return
        except Exception as e:
            print(f"❌ 编辑消息失败: {e}", flush=True)
            
    menu_message = await message.answer_photo(
        photo=lz_var.skins['search_keyword']['file_id'],
        caption=list_info.get("text"),
        parse_mode="HTML",
        reply_markup=list_info.get("reply_markup"),
    )

    await set_global_state(state, menu_message=menu_message)
    


# == 启动指令 ==
@debug
@router.message(Command("start"))
async def handle_start(message: Message, state: FSMContext, command: Command = Command("start")):
    # 删除 /start 这个消息
    try:
        await message.delete()
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter) as e:
        print(f"❌ 删除 /start 消息失败: {e}", flush=True)


    user_id = message.from_user.id


    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        param = args[1].strip()
        parts = param.split("_")
        if parts[0] == "rci":    #remove_collect_item
            date = await state.get_data()
            clt_id = date.get("collection_id")
            handle_message = date.get("message")
  
            print(f"State data: {date}", flush=True)
            
            content_id = parts[1]
            page = int(parts[2]) or 0
            await MySQLPool.remove_content_from_user_collection(int(clt_id), int(content_id))

            result = await _get_clti_list(clt_id,page,user_id,"list")
               
            if result.get("success") is False:
                await message.answer("这个合集暂时没有收录文件", show_alert=True)
                return

            await _edit_caption_or_text(
                handle_message,
                text=result.get("caption"),
                reply_markup=result.get("reply_markup")
            )
            print(f"🔍 删除合集项目 ID: {content_id} {page} {clt_id}")
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

            search_key_index = parts[1]
            encoded = "_".join(parts[2:])  # 剩下的部分重新用 _ 拼接
            # print(f"🔍 搜索关键字索引: {search_key_index}, 编码内容: {encoded}")
            # encoded = param[2:]  # 取第三位开始的内容
            try:
                aes = AESCrypto(AES_KEY)
                content_id_str = aes.aes_decode(encoded)
                

                date = await state.get_data()
                clti_message = date.get("menu_message")

                caption_txt = "🔍 正在从院长的硬盘搜索这个资源，请稍等片刻...ㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤ." 
                if clti_message:
                    try:
                        # print(f"clti_message={clti_message}",flush=True)
                        ret_message = await lz_var.bot.edit_message_media(
                            chat_id=clti_message.chat.id,
                            message_id=clti_message.message_id,
                            media=InputMediaAnimation(
                                media=lz_var.skins["loading"]["file_id"],
                                caption=caption_txt,
                                parse_mode="HTML"
                            )
                        )
                        # return
                    except Exception as e:
                        print(f"❌ 编辑消息失败: {e}", flush=True)
                        clti_message = await message.answer_animation(
                            animation=lz_var.skins["loading"]["file_id"],  # 你的 GIF file_id 或 URL
                            caption=caption_txt,
                            parse_mode="HTML"
                        )
                   



                else:   
                    clti_message = await message.answer_animation(
                        animation=lz_var.skins["loading"]["file_id"],  # 你的 GIF file_id 或 URL
                        caption=caption_txt,
                        parse_mode="HTML"
                    )

                    # print(f"clti_message={clti_message}",flush=True)

                
                
                # //
  


                content_id = int(content_id_str)  # ✅ 关键修正
                if parts[0] == "f":
                    product_info = await _build_product_info(content_id, search_key_index, state, message)
                elif (parts[0] in ["fd", "ul", "cm", "cf"]):
                    product_info = await _build_product_info(content_id, search_key_index, state=state, message=message, search_from=parts[0])


                print(f"688:Product Info", flush=True)
                if product_info['ok']:
                    if (parts[0] in ["f","fd", "ul", "cm", "cf"]):
                        # date = await state.get_data()
                        # clti_message = date.get("menu_message")
                        try:
                            if clti_message:
                                await _edit_caption_or_text(
                                    photo=product_info['cover_file_id'],
                                    msg=clti_message,
                                    text=product_info['caption'],
                                    reply_markup=product_info['reply_markup']
                                )
                                return
                        except Exception as e:
                            print(f"⚠️ 删除失败: {e}", flush=True)
                    
                    product_message = await message.answer_photo(
                        photo=product_info['cover_file_id'],
                        caption=product_info['caption'],
                        parse_mode="HTML",
                        reply_markup=product_info['reply_markup'])
                
                    storage = state.storage
                    key = StorageKey(bot_id=lz_var.bot.id, chat_id=lz_var.x_man_bot_id , user_id=lz_var.x_man_bot_id )
                    storage_data = await storage.get_data(key)
                    storage_data["menu_message"] = product_message
                    await storage.set_data(key, storage_data)


                else:
                    await message.answer(product_info['msg'], parse_mode="HTML")
                    return
            except Exception as e:
                # tb = traceback.format_exc()
                await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                print(f"❌ 解密失败：{e}", flush=True)
        elif parts[0] == "post":
            await _submit_to_lg()
        elif parts[0] == "upload":
            await message.answer(f"📦 请直接上传图片/视频/文件", parse_mode="HTML")
        else:
            await message.answer(f"📦 你提供的参数是：`{param}`", parse_mode="HTML")
    else:
        if ENVIRONMENT != "dev":
            return

        menu_message = await message.answer_photo(
                photo=lz_var.skins['home']['file_id'],
                caption="👋 欢迎使用 LZ 机器人！请选择操作：",
                parse_mode="HTML",
                reply_markup=main_menu_keyboard())   
        # await message.answer("👋 欢迎使用 LZ 机器人！请选择操作：", reply_markup=main_menu_keyboard())
        await state.update_data({
            "menu_message": menu_message
        })


async def _build_product_info(content_id :int , search_key_index: str, state: FSMContext, message: Message, search_from : str = 'search', current_pos:int = 0):
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

    await state.update_data({
        "collection_id": int(content_id),
        "search_key_index": search_key_index,
        'message': message,
        'action':'_build_product_info'
    })

    # ✅ 调用并解包返回的三个值
    result_sora = await load_sora_content_by_id(content_id, state, search_key_index, search_from)
    
    ret_content, file_info, purchase_info = result_sora
    source_id = file_info[0] if len(file_info) > 0 else None
    file_type = file_info[1] if len(file_info) > 1 else None
    file_id = file_info[2] if len(file_info) > 2 else None
    thumb_file_id = file_info[3] if len(file_info) > 3 else None

    owner_user_id = purchase_info[0] if purchase_info[0] else None
    fee = purchase_info[1] if purchase_info[1] else 0
    
    
    # print(f"thumb_file_id:{thumb_file_id}")
    # ✅ 检查是否找不到资源（根据返回第一个值）
    if ret_content.startswith("⚠️"):
        return {"ok": False, "msg": ret_content}
        

    if ENVIRONMENT == "dev":
        reply_markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️", callback_data=f"sora_page:{search_key_index}:{current_pos}:-1:{search_from}"),
                InlineKeyboardButton(text=f"💎 {fee}", callback_data=f"sora_redeem:{content_id}"),
                InlineKeyboardButton(text="➡️", callback_data=f"sora_page:{search_key_index}:{current_pos}:1:{search_from}"),
            ],
            [
                InlineKeyboardButton(text=f"💎 {lz_var.xlj_fee} (小懒觉会员)", callback_data=f"sora_redeem:{content_id}:xlj")
            ],
        ])

    
        if search_from == "cm":
            reply_markup.inline_keyboard.append(
                [
                    InlineKeyboardButton(text="📂 回合集", callback_data=f"clti:list:{search_key_index}:0"),
                ]
            )
        elif search_from == "cf":
            reply_markup.inline_keyboard.append(
                [
                    InlineKeyboardButton(text="📂 回合集", callback_data=f"clti:flist:{search_key_index}:0"),
                ]
            )    
        elif search_from == "ul":
            reply_markup.inline_keyboard.append(
                [
                    InlineKeyboardButton(text="📂 回我的上传", callback_data=f"history_update"),
                ]
            )    
        elif search_from == "fd":
            reply_markup.inline_keyboard.append(
                [
                    InlineKeyboardButton(text="📂 回我的兑换", callback_data=f"history_redeem"),
                ]
            )    
        else:
            reply_markup.inline_keyboard.append(
                [
                    InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                ]
            )
        
        reply_markup.inline_keyboard.append(
            [
                InlineKeyboardButton(text="🔗 复制资源连结", copy_text=CopyTextButton(text=shared_url)),
                InlineKeyboardButton(text="➕ 加入合集", callback_data=f"add_to_collection:{content_id}:0")
            ]
        )


    else:
        reply_markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=f"💎 {fee}", callback_data=f"sora_redeem:{content_id}")
            ],
            [
                InlineKeyboardButton(text=f"💎 {lz_var.xlj_fee} (小懒觉会员)", callback_data=f"sora_redeem:{content_id}:xlj")
            ],
            [
                InlineKeyboardButton(text="🔗 复制资源链结", copy_text=CopyTextButton(text=shared_url))
            ]
        ])

    return {'ok': True, 'caption': ret_content, 'file_type':'photo','cover_file_id': thumb_file_id, 'reply_markup': reply_markup}




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
        # 没有合集就引导创建
        kb_rows.append([InlineKeyboardButton(text="➕ 创建合集", callback_data="clt:create")])
        kb_rows.append([InlineKeyboardButton(text="🔙 返回合集菜单", callback_data="clt_my")])
        return InlineKeyboardMarkup(inline_keyboard=kb_rows)

    # 本页 6 条合集选择按钮
    for r in rows:
        cid = r.get("id")
        title = (r.get("title") or "未命名合集")[:30]
        kb_rows.append([
            InlineKeyboardButton(
                text=f"📦 {title}  #ID{cid}",
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
    kb_rows.append([InlineKeyboardButton(text="🔙 返回合集菜单", callback_data="clt_my")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)



@router.callback_query(F.data.regexp(r"^add_to_collection:(\d+):(\d+)$"))
async def handle_add_to_collection(callback: CallbackQuery):
    _, content_id_str, page_str = callback.data.split(":")
    content_id = int(content_id_str)
    page = int(page_str)
    user_id = callback.from_user.id

    # 统计用户合集数量 & 取第一个合集ID
    count, first_id = await MySQLPool.get_user_collections_count_and_first(user_id=user_id)

    if count == 0:
        # 自动创建一个默认合集并加入
        new_id = await MySQLPool.create_default_collection(user_id=user_id, title="未命名合集")
        if not new_id:
            await callback.answer("创建合集失败，请稍后再试", show_alert=True)
            return

        ok = await MySQLPool.add_content_to_user_collection(collection_id=new_id, content_id=content_id)
        tip = "✅ 已为你创建合集并加入" if ok else "合集已创建，但加入失败"
        await callback.answer(tip, show_alert=False)
        # 也可以顺手把按钮切到“我的合集”：
        # kb = await build_add_to_collection_keyboard(user_id=user_id, content_id=content_id, page=0)
        # await _edit_caption_or_text(callback.message, text="你的合集：", reply_markup=kb)
        return

    if count == 1 and first_id:
        # 直接加入唯一合集
        ok = await MySQLPool.add_content_to_user_collection(collection_id=first_id, content_id=content_id)
        tip = "✅ 已加入你的唯一合集" if ok else "⚠️ 已在该合集里或加入失败"
        await callback.answer(tip, show_alert=False)
        return

    # 多个合集 → 弹出分页选择
    kb = await build_add_to_collection_keyboard(user_id=user_id, content_id=content_id, page=page)
    await _edit_caption_or_text(
        callback.message,
        text=f"请选择要加入的合集（第 {page+1} 页）：",
        reply_markup=kb
    )
    await callback.answer()


# 选择某个合集 → 写入 user_collection_file（去重）
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
        tip = "✅ 已加入该合集"
    else:
        tip = "⚠️ 已在该合集里或加入失败"

    product_info = await _build_product_info(content_id=content_id, search_key_index=search_key_index, state=state, message=callback.message)
    # 保持在选择页，方便继续加入其他合集
    # kb = await build_add_to_collection_keyboard(user_id=user_id, content_id=content_id, page=page)
    try:
        await callback.message.edit_reply_markup(reply_markup=product_info['reply_markup'])
    except Exception as e:
        print(f"❌ 刷新加入合集页失败: {e}", flush=True)

    await callback.answer(tip, show_alert=False)


# == 主菜单选项响应 ==
@router.callback_query(F.data == "search")
async def handle_search(callback: CallbackQuery):
    await _edit_caption_or_text(
        photo=lz_var.skins['search']['file_id'],
        msg=callback.message,
        text="👋 请选择操作：", 
        reply_markup=search_menu_keyboard()
    )

def back_search_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 返回搜索", callback_data="search")],
    ])

    
@router.callback_query(F.data == "ranking")
async def handle_ranking(callback: CallbackQuery):

    await _edit_caption_or_text(
        photo=lz_var.skins['ranking']['file_id'],
        msg=callback.message,
        text="排行榜", 
        reply_markup=ranking_menu_keyboard()
    )  


@router.callback_query(F.data == "collection")
async def handle_collection(callback: CallbackQuery):


    await lz_var.bot.edit_message_media(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        media=InputMediaPhoto(
            media=lz_var.skins['clt_menu']['file_id'],
            caption="👋 合集菜单！请选择操作：",
            parse_mode="HTML"
        ),
        reply_markup=collection_menu_keyboard()
    )


    # await callback.message.answer_photo(
    #     photo=lz_var.skins['clt_menu']['file_id'],
    #     caption="👋 合集菜单！请选择操作：",
    #     parse_mode="HTML",
    #     reply_markup=collection_menu_keyboard())   


    # await callback.message.edit_reply_markup(reply_markup=collection_menu_keyboard())

@router.callback_query(F.data == "my_history")
async def handle_my_history(callback: CallbackQuery):
    
    await _edit_caption_or_text(
        photo=lz_var.skins['history']['file_id'],
        msg=callback.message,
        text="👋 这是你的历史记录菜单！请选择操作：", 
        reply_markup=history_menu_keyboard()
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
    await state.update_data({
        "menu_message": callback.message
    })
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
        reply_markup=back_search_menu_keyboard()
    )

   

@router.callback_query(F.data == "search_tag")
async def handle_search_tag(callback: CallbackQuery):
    await _edit_caption_or_text(
        photo=lz_var.skins['search_tag']['file_id'],
        msg=callback.message,
        text="🏷️ 请选择标签进行筛选...", 
        reply_markup=back_search_menu_keyboard()
    )


    # await callback.message.answer("🏷️ 请选择标签进行筛选...")


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
async def handle_ranking_resource(callback: CallbackQuery):
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
        reply_markup=ranking_menu_keyboard()
    )

    # await callback.message.answer(
    #     html_text,
    #     parse_mode=ParseMode.HTML,
    #     disable_web_page_preview=True
    # )
    await callback.answer()

@router.callback_query(F.data == "ranking_uploader")
async def handle_ranking_uploader(callback: CallbackQuery):
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
        reply_markup=ranking_menu_keyboard()
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
    title = (row.get("title") or "未命名合集")[:30]
    pub   = "公开" if (row.get("is_public") == 1) else "不公开"
    return f"〈{title}〉（{pub}）#ID{cid}"

async def build_collections_keyboard(user_id: int, page: int, mode: str) -> InlineKeyboardMarkup:
    """
    mode: 'mine'（我的合集）| 'fav'（我收藏的合集）
    每页 6 行合集按钮；第 7 行：上一页 | [创建，仅 mine] | 下一页（上一/下一按需显示）
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
            kb_rows.append([InlineKeyboardButton(text="➕ 创建合集", callback_data="clt:create")])
        kb_rows.append([InlineKeyboardButton(text="🔙 返回上页", callback_data="collection")])
        return InlineKeyboardMarkup(inline_keyboard=kb_rows)

    # 6 行合集按钮
    for r in display:
        cid = r.get("id")
        btn_text = _collection_btn_text(r)
        kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"{edit_prefix}:{cid}:{page}:tk")])

    # 第 7 行：上一页 | [创建，仅 mine] | 下一页
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"{list_prefix}:{page-1}"))

    if mode == "mine":
        nav_row.append(InlineKeyboardButton(text="➕ 创建合集", callback_data="clt:create"))

    if has_next:
        nav_row.append(InlineKeyboardButton(text="下一页 ➡️", callback_data=f"{list_prefix}:{page+1}"))

    # 有可能出现只有“上一页/下一页”而中间没有“创建”的情况（fav 模式）
    if nav_row:
        kb_rows.append(nav_row)

    # 返回上页

    kb_rows.append([InlineKeyboardButton(text="🔙 返回合集菜单", callback_data="collection")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)



# ====== “我的合集”入口用通用键盘（保持既有行为）======

@router.callback_query(F.data == "clt_my")
async def handle_clt_my(callback: CallbackQuery):
    user_id = callback.from_user.id
    # “我的合集”之前是只换按钮；为了统一体验，也可以换 text，但你要求按钮呈现，因此只换按钮：

    text = f'这是你的合集'

    await _edit_caption_or_text(
        photo=lz_var.skins['clt_my']['file_id'],
        msg=callback.message,
        text=text, 
        reply_markup=await build_collections_keyboard(user_id=user_id, page=0, mode="mine")
    )





    # await callback.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:mlist:\d+$"))
async def handle_clt_my_pager(callback: CallbackQuery):
    _, _, page_str = callback.data.split(":")
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=int(page_str), mode="mine")
    await callback.message.edit_reply_markup(reply_markup=kb)

#查看合集
@router.callback_query(F.data.regexp(r"^clt:my:(\d+)(?::(\d+)(?::([A-Za-z0-9]+))?)?$"))
async def handle_clt_my_detail(callback: CallbackQuery,state: FSMContext):
    # ====== “我的合集”入口用通用键盘（保持既有行为）======
    print(f"handle_clt_my_detail: {callback.data}")
    _, _, cid_str, page_str,refresh_mode = callback.data.split(":")
    cid = int(cid_str)
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


    await state.update_data({
        "menu_message": new_message
    })

#编辑合集详情
@router.callback_query(F.data.regexp(r"^clt:edit:\d+:\d+(?::([A-Za-z]+))?$"))
async def handle_clt_edit(callback: CallbackQuery):
    # ====== “我的合集”入口用通用键盘（保持既有行为）======
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
            reply_markup=kb
        )
        user_id = callback.from_user.id
        await MySQLPool.delete_cache(f"user:clt:{user_id}:")

async def _build_clt_edit(cid: int, anchor_message: Message):
    caption = await _build_clt_edit_caption(cid)
    kb = _build_clt_edit_keyboard(cid)
    await _edit_caption_or_text(
        msg=anchor_message,
        text=caption, 
        reply_markup=kb
    )

async def _build_clt_edit_caption(cid: int ):
    
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名合集"
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


# ====== “我收藏的合集”入口（复用通用键盘，mode='fav'）======

#创建合集
@router.callback_query(F.data == "clt:create")
async def handle_clt_create(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    ret = await MySQLPool.create_user_collection(user_id=user_id)  # 默认：未命名合集、公开
    cid = ret.get("id")


    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名合集"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    text = (
        f"🆕 已创建合集：\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc, 120)}\n\n"
        f"请选择要设置的项目："
    )
    await _edit_caption_or_text(
        callback.message,
        text=text,
        reply_markup=_build_clt_edit_keyboard(cid)
    )
    cache_key = f"collection_info_{cid}"
    MySQLPool.cache.delete(cache_key)


@router.callback_query(F.data == "clt_favorite")
async def handle_clt_favorite(callback: CallbackQuery):
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=0, mode="fav")
    await _edit_caption_or_text(
        photo=lz_var.skins['clt_fav']['file_id'],
        msg=callback.message,
        text="这是你收藏的合集", 
        reply_markup=kb
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
    # 只提供返回收藏列表与回合集主菜单
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 返回收藏列表", callback_data=f"cc:flist:{page}")],
        [InlineKeyboardButton(text="📦 回合集菜单", callback_data="collection")],
    ])


@router.callback_query(F.data.regexp(r"^clt:fav:(\d+)(?::(\d+)(?::([A-Za-z0-9]+))?)?$"))
async def handle_clt_fav(callback: CallbackQuery):
    # ====== “我收藏的合集”入口（复用通用键盘，mode='fav'）======
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



# ============ /set [id]：合集信息/列表/收藏切换 ============



def _build_clt_info_caption(rec: dict) -> str:
    return (
        f"<blockquote>{rec.get('title') or '未命名合集'}</blockquote>\n"
        f"{rec.get('description') or ''}\n\n"
        f"🆔 {rec.get('id')}     👤 {rec.get('user_id')}\n"
    )


#collection > 合集 Partal > 合集列表 CollectionList > [单一合集页 CollectionDetail] > 显示合集内容 CollectItemList 或 编辑合集 CollectionEdit
def _build_clt_info_keyboard(cid: int, is_fav: bool, mode: str = 'view', ops: str = 'handle_clt_fav') -> InlineKeyboardMarkup:
    kb_rows: list[list[InlineKeyboardButton]] = []

    print(f"ops={ops}")

    callback_function = ''
    if ops == 'handle_clt_my':
        callback_function = 'clti:list'
    elif ops == 'handle_clt_fav':
        callback_function = 'clti:flist' 

    nav_row: list[InlineKeyboardButton] = []
    nav_row.append(InlineKeyboardButton(text="🗂️ 显示合集内容", callback_data=f"{callback_function}:{cid}:0"))

    if mode == 'edit':
        nav_row.append(InlineKeyboardButton(text="🔧 编辑合集", callback_data=f"clt:edit:{cid}:0:k"))
    else:
        fav_text = "❌ 取消收藏" if is_fav else "🩶 收藏"
        nav_row.append(InlineKeyboardButton(text=fav_text, callback_data=f"uc:fav:{cid}"))
    
    if nav_row:
        kb_rows.append(nav_row)  

    shared_url = f"https://t.me/{lz_var.bot_username}?start=clt_{cid}"
    kb_rows.append([InlineKeyboardButton(text="🔗 复制合集链结", copy_text=CopyTextButton(text=shared_url))])


    if ops == 'handle_clt_my':
        kb_rows.append([InlineKeyboardButton(text="🔙 返回我的合集", callback_data="clt_my")])
    elif ops == 'handle_clt_fav':
        kb_rows.append([InlineKeyboardButton(text="🔙 返回收藏的合集", callback_data="clt_favorite")])
    else:
        kb_rows.append([InlineKeyboardButton(text="🔙 返回", callback_data="clt_my")])


    # 关键点：需要二维数组（每个子列表是一行按钮）
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# 查看合集的按钮
def _clti_list_keyboard(cid: int, page: int, has_prev: bool, has_next: bool, is_fav: bool, mode: str = 'view') -> InlineKeyboardMarkup:
    nav_row: list[InlineKeyboardButton] = []
    rows = []
    if mode == 'list':
        callback_function = 'my'
        title = "🔙 返回我的合集主页"
    elif mode == 'flist':
        callback_function = 'fav' 
        title = "🔙 返回收藏的合集主页"


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
        await message.answer("用法：/set [合集ID]")
        return

    cid = int(args[1].strip())
    user_id = message.from_user.id
    retCollect = await _build_clt_info(cid=cid, user_id=user_id, ops='handle_set_collection')

# 查看合集
async def _build_clt_info( cid: int, user_id: int, mode: str = 'view', ops:str ='set') -> dict:
    bot_name = getattr(lz_var, "bot_username", None) or "luzaitestbot"
    # 查询合集 + 封面 file_id（遵循你给的 SQL）
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
        print(f"❌ 发送合集信息失败: {e}", flush=True)
        # return await message.answer("⚠️ 发送合集信息失败")
        return {"success": False,  "caption": caption, "reply_markup": kb}
        


# 「显示列表」：分页展示前 6 个文件的 file_id
@router.callback_query(F.data.regexp(r"^clti:(flist|list):\d+:\d+$"))
async def handle_clti_list(callback: CallbackQuery, state: FSMContext):
    _, mode, clt_id_str, page_str = callback.data.split(":")
    clt_id, page = int(clt_id_str), int(page_str)
    user_id = callback.from_user.id
    
    print(f"--->{mode}_message")
    await state.update_data({
        "menu_message": callback.message,
        "collection_id": clt_id,
        'action':mode
        })

    print(f"✅ Clti Message {callback.message.message_id} in chat {callback.message.chat.id}", flush=True)

    result = await _get_clti_list(clt_id,page,user_id,mode)

    if result.get("success") is False:
        await callback.answer("这个合集暂时没有收录文件", show_alert=True)
        return

    await _edit_caption_or_text(
        callback.message,
        text=result.get("caption")+f"\n\n {callback.message.message_id}",
        reply_markup=result.get("reply_markup")
    )
    await callback.answer()



async def _get_clti_list(cid,page,user_id,mode):
    # 拉取本页数据（返回 file_id list 与 has_next）
    files, has_next = await MySQLPool.list_collection_files_file_id(collection_id=cid, limit=RESULTS_PER_PAGE+1, offset=page*RESULTS_PER_PAGE)
    display = files[:RESULTS_PER_PAGE]
    has_prev = page > 0
    is_fav = await MySQLPool.is_collection_favorited(user_id=user_id, collection_id=cid)

    if not display:
        return {"success": False, "message": "这个合集暂时没有收录文件"}
   
    # 组装列表 caption：仅列 file_id
    lines = [f"合集 #{cid} 文件列表（第 {page+1} 页）", ""]
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
            f"{icon}<a href='https://t.me/{lz_var.bot_username}?start={stag}_{cid}_{encoded}'>{f.get('id')} {content}</a> {fix_href}"
        )

        # lines.append(f"{icon}<a href='https://t.me/{lz_var.bot_username}?start={stag}_{cid}_{encoded}'>{f.get("id")} {content}</a> {fix_href}")
    caption = "\n".join(lines)



    reply_markup = _clti_list_keyboard(cid, page, has_prev, has_next, is_fav, mode)

    return {"success": True, "caption": caption, "reply_markup": reply_markup}
    

    await _edit_caption_or_text(
        callback.message,
        text=caption,
        reply_markup=_clti_list_keyboard(cid, page, has_prev, has_next, is_fav)
    )
    pass



# 「合集信息」：恢复信息视图
@router.callback_query(F.data.regexp(r"^uc:info:\d+$"))
async def handle_uc_info(callback: CallbackQuery):
    _, _, cid_str = callback.data.split(":")
    cid = int(cid_str)
    bot_name = getattr(lz_var, "bot_username", None) or "luzaitestbot"
    rec = await MySQLPool.get_collection_detail_with_cover(collection_id=cid, bot_name=bot_name)
    if not rec:
        await callback.answer("未找到该收藏", show_alert=True); return
    is_fav = await MySQLPool.is_collection_favorited(user_id=callback.from_user.id, collection_id=cid)
    await _edit_caption_or_text(callback.message, text=_build_clt_info_caption(rec), reply_markup=_build_clt_info_keyboard(cid, is_fav))
    await callback.answer()

# 「收藏 / 取消收藏」：落 DB 并刷新按钮
@router.callback_query(F.data.regexp(r"^uc:fav:\d+$"))
async def handle_uc_fav(callback: CallbackQuery):
    _, _, cid_str = callback.data.split(":")
    cid = int(cid_str)
    user_id = callback.from_user.id

    print(f"➡️ 用户 {user_id} 切换合集 {cid} 收藏状态", flush=True)

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

    print(f"➡️ 用户 {user_id} 合集 {cid} 收藏状态切换结果: {tip}", flush=True)

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
    await callback.message.answer("🛍️ 欢迎来到合集市场，看看其他人都在收藏什么吧！")



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

@debug
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
        if search_from == "search":
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
            # print(f"🔍 搜索合集 ID {search_key_index} 的内容")
            # 拉取收藏夹内容
            result = await MySQLPool.get_clt_files_by_clt_id(search_key_index)
            if not result:
                await callback.answer("⚠️ 合集为空", show_alert=True)
                return
        elif search_from == "fd":
            # print(f"🔍 搜索合集 ID {search_key_index} 的内容")
            result = await MySQLPool.search_history_redeem(search_key_index)
            if not result:
                await callback.answer("⚠️ 兑换纪录为空", show_alert=True)
                return    
        elif search_from == "ul":
            # print(f"🔍 搜索合集 ID {search_key_index} 的内容")
            result = await MySQLPool.search_history_upload(search_key_index)
            if not result:
                await callback.answer("⚠️ 上传纪录为空", show_alert=True)
                return   

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

    


        

        product_info = await _build_product_info(content_id=next_content_id, search_key_index=search_key_index,  state=state,  message= callback.message, search_from=search_from , current_pos=new_pos)

        if product_info.get("ok") is False:
            print(f"❌ _build_product_info failed: {product_info}")
            await callback.answer(product_info.get("msg"), show_alert=True)
            return


    

        # print(f"product_info={product_info}")
        ret_content = product_info.get("caption")
        thumb_file_id = product_info.get("cover_file_id")
        reply_markup = product_info.get("reply_markup")

       
        try:    
            r=await callback.message.edit_media(
                media={
                    "type": "photo",
                    "media": thumb_file_id,
                    "caption": ret_content,
                    "parse_mode": "HTML"
                },
                reply_markup=reply_markup
            )

            await set_global_state(state=state, menu_message=r)
            
        except Exception as e:
            print(f"❌ edit_media failed: {e}, try edit_text")

        await callback.answer()

    except Exception as e:
        print(f"❌ handle_sora_page error: {e}")
        await callback.answer("⚠️ 翻页失败", show_alert=True)

@router.callback_query(F.data.startswith("sora_redeem:"))
async def handle_redeem(callback: CallbackQuery, state: FSMContext):
    content_id = callback.data.split(":")[1]
    redeem_type = callback.data.split(":")[2] if len(callback.data.split(":")) > 2 else None

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

    answer_text = ''
    
    if purchase_condition is not None:
        await callback.answer(f"⚠️ 该资源请到专属的机器人兑换", show_alert=True)
        return


    if not file_id:
        print("❌ 没有找到匹配记录 source_id")
        await callback.answer("👻 我们正偷偷的从院长的硬盘把这个资源搬出来，这段时间先看看别的资源吧。", show_alert=True)
        # await callback.message.reply("👻 我们正偷偷的从院长的硬盘把这个资源搬出来，这段时间先看看别的资源吧。")
        # await lz_var.bot.delete_message(
        #     chat_id=callback.message.chat.id,
        #     message_id=callback.message.message_id
        # )
        return
    
    # 若有,则回覆消息
    from_user_id = callback.from_user.id

    # ===== 小懒觉会员判断（SQL 已移至 lz_db.py）=====
    def _fmt_ts(ts: int | None) -> str:
        if not ts:
            return "未开通"
        tz = timezone(timedelta(hours=8))  # Asia/Singapore/UTC+8
        try:
            return datetime.fromtimestamp(int(ts), tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(ts)

    expire_ts = await db.get_latest_membership_expire(from_user_id)
    now_utc = int(datetime.now(timezone.utc).timestamp())

    if not expire_ts:
        # 未开通/找不到记录 → 用原价，提示并给两个按钮，直接返回
        human_ts = _fmt_ts(None)
        text = (
            f"你目前不是小懒觉会员，或是会员已过期。将以原价 {fee} 兑换此资源\r\n\r\n"
            f"目前你的小懒觉会员期有效期为 {human_ts}，可点选下方按钮更新或兑换小懒觉会员"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="更新小懒觉会员期", callback_data="xlj:update")],
            [InlineKeyboardButton(
                text="兑换小懒觉会员 ( 💎 800 )",
                url="https://t.me/xljdd013bot?start=join_xiaolanjiao_act"
            )],
        ])
        await callback.message.reply(text, reply_markup=kb)
        
        if( redeem_type == 'xlj'):
            await callback.answer()
            return

    elif int(expire_ts) < now_utc:
        # 已开通但过期 → 用原价，提示并给两个按钮，直接返回
        human_ts = _fmt_ts(expire_ts)
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
        await callback.message.reply(text, reply_markup=kb)
        if( redeem_type == 'xlj'):
            await callback.answer()
            return


    elif int(expire_ts) >= now_utc:
        fee = lz_var.xlj_fee
        
        try:
            reply_text = f"你是小懒觉会员，在活动期间，享有最最最超值优惠价，每个资源只要 {fee} 积分。\r\n\r\n目前你的小懒觉会员期有效期为 {_fmt_ts(expire_ts)}"
            # await callback.answer(
            #     f"你是小懒觉会员，在活动期间，享有最最最超值优惠价，每个资源只要 {fee} 积分。\r\n\r\n"
            #     f"目前你的小懒觉会员期有效期为 {_fmt_ts(expire_ts)}",
            #     show_alert=True
            # )
        except Exception:
            pass
    # 会员有效 → 本次兑换价改为 10，弹轻提示后继续扣分发货
    

    # 统一在会员判断之后再计算费用
    sender_fee = int(fee) * (-1)
    receiver_fee = int(int(fee) * (0.4))

    result = await MySQLPool.transaction_log({
        'sender_id': from_user_id,
        'receiver_id': owner_user_id or 0,
        'transaction_type': 'confirm_buy',
        'transaction_description': source_id,
        'sender_fee': sender_fee,
        'receiver_fee': receiver_fee
    })





    # print(f"🔍 交易记录结果: {result}", flush=True)

    
    # ✅ 兜底：确保 result & user_info 可用
    if not isinstance(result, dict):
        await callback.answer("⚠️ 交易服务暂不可用，请稍后再试。", show_alert=True)
        return

    user_info = result.get('user_info') or {}
    try:
        user_point = int(user_info.get('point') or 0)
    except (TypeError, ValueError):
        user_point = 0

    print(f"💰 交易结果: {result}, 交易后用户积分余额: {user_point}", flush=True)

    if result.get('status') == 'exist' or result.get('status') == 'insert' or result.get('status') == 'reward_self':

        if result.get('status') == 'exist':
            reply_text += f"✅ 你已经兑换过此资源，不需要扣除积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {user_point}。"

            print(f"💬 回复内容: {reply_text}", flush=True)
        elif result.get('status') == 'insert':
            
            reply_text += f"✅ 兑换成功，已扣除 {sender_fee} 积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {(user_point+sender_fee)}。"
       
        elif result.get('status') == 'reward_self':
            
            reply_text += f"✅ 这是你自己的资源"
            if user_point > 0:
                reply_text += f"，当前积分余额: {(user_point+sender_fee)}。"

        feedback_kb = None
        if lz_var.UPLOADER_BOT_NAME and source_id:
            feedback_kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="⚠️ 反馈内容",
                    url=f"https://t.me/{lz_var.UPLOADER_BOT_NAME}?start=s_{source_id}"
                )
            ]])


        try:
           
            if file_type == "album" or file_type == "a":
                
                productInfomation = await get_product_material(content_id)
                if not productInfomation:
                     await callback.answer(f"资源同步中，请稍等一下再试，请看看别的资源吧 {content_id}", show_alert=True)
                     return   
                # else:
                #     print(f"1892=>{productInfomation}")
                await Media.send_media_group(callback, productInfomation, 1, content_id, source_id)

                # if productInfomation.get("ok") is False and productInfomation.get("lack_file_uid_rows"):
                #     await callback.answer("资源同步中，请稍后再试，请看看别的资源吧", show_alert=True) 
                #     lack_file_uid_rows = productInfomation.get("lack_file_uid_rows")
                #     for fuid in lack_file_uid_rows:
                    
                #         await lz_var.bot.send_message(
                #             chat_id=lz_var.x_man_bot_id,
                #             text=f"{fuid}"
                #         )
                #         await asyncio.sleep(0.7)
                        
                        
                #     return
                # # print(f"1896=>{productInfomation}")
                # rows = productInfomation.get("rows", [])
                # if rows:


                #     material_status = productInfomation.get("material_status")
                #     if material_status:
                #         return_media = await _build_mediagroup_box(1, source_id,content_id, material_status)
                #         feedback_kb = return_media.get("feedback_kb")
                #         text = return_media.get("text")
                        
                #         await lz_var.bot.send_message(
                #             parse_mode="HTML",
                #             reply_markup=feedback_kb,
                #             chat_id=from_user_id,
                #             text=text,
                #             # reply_to_message_id=callback.message.message_id
                #         )



                    
            elif file_type == "photo" or file_type == "p":
                await lz_var.bot.send_photo(
                    chat_id=from_user_id,
                    photo=file_id,
                    reply_to_message_id=callback.message.message_id,
                    reply_markup=feedback_kb
                )
            elif file_type == "video" or file_type == "v":
                await lz_var.bot.send_video(
                    chat_id=from_user_id,
                    video=file_id,
                    reply_to_message_id=callback.message.message_id,
                    reply_markup=feedback_kb
                )
            elif file_type == "document" or file_type == "d":
                await lz_var.bot.send_document(
                    chat_id=from_user_id,
                    document=file_id,
                    reply_to_message_id=callback.message.message_id,
                    reply_markup=feedback_kb
                )
        except Exception as e:
            print(f"❌ 目标 chat 不存在或无法访问: {e}")

        await callback.answer(reply_text, show_alert=True)
        new_message = await lz_var.bot.copy_message(
            chat_id=callback.message.chat.id,
            from_chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            reply_markup=callback.message.reply_markup
        )
        await set_global_state(state, menu_message=new_message)

        await lz_var.bot.delete_message(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id
        )

        return
    elif result.get('status') == 'insufficient_funds':
       
        reply_text = f"❌ 你的积分不足 ( {user_point} ) ，无法兑换此资源 ( {abs(sender_fee)} )。"
        await callback.answer(reply_text, show_alert=True)
        # await callback.message.reply(reply_text, parse_mode="HTML")
        return


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
                text="⚠️ 反馈内容",
                url=f"https://t.me/{lz_var.UPLOADER_BOT_NAME}?start=s_{source_id}"
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
    # print(f"🔍 载入 ID: {content_id}, Record: {record}", flush=True)
    if record:
        



         # 取出字段，并做基本安全处理
        fee = record.get('fee', 60)
        if fee is None or fee < 0:
            fee = 60
            
        owner_user_id = record.get('owner_user_id', 0)

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
        if product_type is None:
            product_type = file_type  # 默认付费

        purchase_condition = record.get('purchase_condition', '')  
        # print(f"{record}")

        # print(f"🔍 载入 ID: {record_id}, Source ID: {source_id}, thumb_file_id:{thumb_file_id}, File Type: {file_type}\r\n")

        # ✅ 若 thumb_file_id 为空，则给默认值
        if not thumb_file_id and thumb_file_unique_id != None:
            print(f"🔍 没有找到 thumb_file_id，背景尝试从 thumb_file_unique_id {thumb_file_unique_id} 获取")
            spawn_once(
                f"thumb_file_id:{thumb_file_unique_id}",
                lambda: Media.fetch_file_by_file_uid_from_x(state, thumb_file_unique_id, 10)
            )


            # thumb_file_id = await Media.fetch_file_by_file_uid_from_x(state, thumb_file_unique_id, 10)
            # 设置当下要获取的 thumb 是什么,若从背景取得图片时，可以直接更新 (fetch_thumb_file_unique_id 且 menu_message 存在)
            state_data = await state.get_data()
            menu_message = state_data.get("menu_message")
     
            if menu_message:
                print(f"🔍 设置 fetch_thumb_file_unique_id: {thumb_file_unique_id}，并丢到后台获取")
                await set_global_state(state, thumb_file_unique_id=f"{thumb_file_unique_id}", menu_message=menu_message)
            else:
                print("❌ menu_message 不存在，无法设置 fetch_thumb_file_unique_id")
            
            # print(f"🔍 设置 fetch_thumb_file_unique_id: {thumb_file_unique_id}，并丢到后台获取")
            # spawn_once(f"thumb_file_unique_id:{thumb_file_unique_id}", Media.fetch_file_by_file_uid_from_x(state, thumb_file_unique_id, 10))
            
        if not thumb_file_id:
            print("❌ 在延展库没有，用预设图")
            
            if file_id and not thumb_file_unique_id and (file_type == "video" or file_type == "v"):
                spawn_once(
                    f"create_thumb_file_id:{file_id}",
                    lambda: handle_update_thumb(content_id, file_id )
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
                    
                    


        ret_content = ""
        tag_length = 0
        max_total_length = 1000  # 预留一点安全余地，不用满 1024
               
        if tag:
            ret_content += f"{record['tag']}\n\n"

        profile = ""
        if file_size:
            # print(f"🔍 资源大小: {file_size}")
            label_size = convert.byte_to_human_readable(file_size)
            ret_content += f"📄 {label_size}  "
            profile += f"📄 {label_size}  "

        if duration:
            label_duration = convert.seconds_to_hms(duration)
            ret_content += f"🕙 {label_duration}  "
            profile += f"🕙 {label_duration}  "

        space = ""
        meta_line = profile or ""
        meta_len = len(meta_line)
        target_len = 55  # 你可以设目标行长度，比如 55 字符
        if meta_len < target_len:
            pad_len = target_len - meta_len
            space += "ㅤ" * pad_len  # 用中点撑宽（最通用，Telegram 不会过滤）
        ret_content += f"{space}"


        if search_key_index:
            
            if search_from == "cm" or search_from == "cf":
                
                clt_info = await MySQLPool.get_user_collection_by_id(collection_id=int(search_key_index))
                
                ret_content += f"\r\n📂 合集: {clt_info.get('title')}\n\n"
            else:
                keyword = await db.get_keyword_by_id(int(search_key_index))
                if keyword:
                    ret_content += f"\r\n🔑 关键字: {keyword}\n\n"

        # print(f"ret_content before length {len(ret_content)}")

        if ret_content:
            tag_length = len(ret_content)
    

        if not file_id and source_id:
            # 不阻塞：丢到后台做补拉
            # spawn_once(f"fild_id:{source_id}", Media.fetch_file_by_file_uid_from_x(state, source_id, 10))
            spawn_once(
                f"fild_id:{source_id}",
                lambda: Media.fetch_file_by_file_uid_from_x(state, source_id, 10 )
            )
        
        # print(f"tag_length {tag_length}")

        # 计算可用空间
        available_content_length = max_total_length - tag_length - 50  # 预留额外描述字符
        
       
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
        return ret_content, [source_id, product_type, file_id, thumb_file_id], [owner_user_id, fee, purchase_condition]
        
    else:
        return f"⚠️ 没有找到 ID 为 {content_id} 的 Sora 内容记录"
    


