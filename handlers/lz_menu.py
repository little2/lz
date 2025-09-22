from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, CopyTextButton
from aiogram.filters import Command
from aiogram.enums import ContentType
from aiogram.utils.text_decorations import markdown_decoration

from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.exceptions import TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from utils.unit_converter import UnitConverter
from utils.aes_crypto import AESCrypto
from utils.media_utils import Media



import asyncio

from lz_db import db
from lz_config import AES_KEY, ENVIRONMENT
import lz_var
import traceback
import random

from utils.media_utils import Media
from utils.tpl import Tplate

from lz_mysql import MySQLPool

from utils.product_utils import submit_resource_to_chat

router = Router()

_background_tasks: dict[str, asyncio.Task] = {}

class LZFSM(StatesGroup):
    waiting_for_title = State()
    waiting_for_description = State()


def spawn_once(key: str, coro: "Coroutine"):
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
        [InlineKeyboardButton(text="🎯 猜你喜欢", callback_data="guess_you_like")],
        [InlineKeyboardButton(text="📤 资源上传", callback_data="upload_resource")],
    ])

# == 搜索菜单 ==
def search_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 关键字搜索", callback_data="keyword_search")],
        [InlineKeyboardButton(text="🏷️ 标签筛选", callback_data="tag_filter")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 排行菜单 ==
def ranking_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 近期火热资源排行板", callback_data="hot_resource_ranking")],
        [InlineKeyboardButton(text="👑 近期火热上传者排行板", callback_data="hot_uploader_ranking")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 合集菜单 ==
def collection_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 我的合集", callback_data="my_collections")],
        [InlineKeyboardButton(text="❤️ 我收藏的合集", callback_data="my_favorite_collections")],
        [InlineKeyboardButton(text="🛍️ 逛逛合集市场", callback_data="explore_marketplace")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# ========= 菜单构建 =========



def create_collection_menu_keyboard(collection_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 合集主题（title）", callback_data=f"cc:title:{collection_id}")],
        [InlineKeyboardButton(text="📝 合集简介（description）", callback_data=f"cc:description:{collection_id}")],
        [InlineKeyboardButton(text="👁 是否公开（is_public）", callback_data=f"cc:is_public:{collection_id}")],
        [InlineKeyboardButton(text="🔙 返回我的合集", callback_data="my_collections")],
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


# ========= 工具 =========

def _short(text: str | None, n: int = 60) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ")
    return text[:n] + ("..." if len(text) > n else "")

# ========= 创建合集：先新建记录，进入设置菜单 =========

# ====== 我的合集：按钮+分页 ======









@router.callback_query(F.data.regexp(r"^cc:title:\d+$"))
async def handle_cc_title(callback: CallbackQuery, state: FSMContext):
    _, _, cid = callback.data.split(":")
    await state.update_data({
        "collection_id": int(cid),
        "anchor_chat_id": callback.message.chat.id,
        "anchor_msg_id": callback.message.message_id,
    })
    await state.set_state(LZFSM.waiting_for_title)
    await callback.message.edit_text(
        "📝 请输入标题（长度 ≤ 255，可包含中文、英文或符号）：",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"cc:back:{cid}")]
        ])
    )

@router.message(LZFSM.waiting_for_title)
async def on_title_input(message: Message, state: FSMContext):
    data = await state.get_data()
    cid = int(data.get("collection_id"))
    anchor_chat_id = data.get("anchor_chat_id")
    anchor_msg_id  = data.get("anchor_msg_id")

    text = (message.text or "").strip()
    if len(text) == 0 or len(text) > 255:
        # 直接提示一条轻量回复也可以改为 alert；这里按需求删输入，所以给个轻提示再删。
        await message.reply("⚠️ 标题长度需为 1~255，请重新输入。")
        return

    # 1) 删除用户输入的这条消息
    try:
        await lz_var.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        print(f"⚠️ 删除用户输入失败: {e}", flush=True)

    # 2) 更新数据库
    await MySQLPool.update_user_collection(collection_id=cid, title=text)

    # 3) 刷新锚点消息的文本与按钮
    rec = await MySQLPool.get_user_collection_by_id(cid)
    title = rec.get("title") if rec else text
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    new_text = (
        f"当前设置：\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc,120)}\n\n"
        f"请选择要设置的项目："
    )
    try:
        await lz_var.bot.edit_message_text(
            chat_id=anchor_chat_id,
            message_id=anchor_msg_id,
            text=new_text,
            reply_markup=create_collection_menu_keyboard(cid)
        )
    except Exception as e:
        print(f"❌ 编辑锚点消息失败: {e}", flush=True)

    await state.clear()

# ===== 简介 =====

@router.callback_query(F.data.regexp(r"^cc:description:\d+$"))
async def handle_cc_description(callback: CallbackQuery, state: FSMContext):
    _, _, cid = callback.data.split(":")
    await state.update_data({
        "collection_id": int(cid),
        "anchor_chat_id": callback.message.chat.id,
        "anchor_msg_id": callback.message.message_id,
    })
    await state.set_state(LZFSM.waiting_for_description)
    await callback.message.edit_text(
        "🧾 请输入这个合集的介绍：",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 返回上页", callback_data=f"cc:back:{cid}")]
        ])
    )

@router.message(LZFSM.waiting_for_description)
async def on_description_input(message: Message, state: FSMContext):
    data = await state.get_data()
    cid = int(data.get("collection_id"))
    anchor_chat_id = data.get("anchor_chat_id")
    anchor_msg_id  = data.get("anchor_msg_id")

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
    rec = await MySQLPool.get_user_collection_by_id(cid)
    title = rec.get("title") if rec else "未命名合集"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    new_text = (
        f"当前设置：\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc,120)}\n\n"
        f"请选择要设置的项目："
    )
    try:
        await lz_var.bot.edit_message_text(
            chat_id=anchor_chat_id,
            message_id=anchor_msg_id,
            text=new_text,
            reply_markup=create_collection_menu_keyboard(cid)
        )
    except Exception as e:
        print(f"❌ 编辑锚点消息失败: {e}", flush=True)

    await state.clear()


# ========= 是否公开 =========

@router.callback_query(F.data.regexp(r"^cc:is_public:\d+$"))
async def handle_cc_is_public(callback: CallbackQuery):
    _, _, cid = callback.data.split(":")
    cid = int(cid)
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    is_public = rec.get("is_public") if rec else None
    await callback.message.edit_text(
        "👁 请选择这个合集是否可以公开：",
        reply_markup=is_public_keyboard(cid, is_public)
    )

@router.callback_query(F.data.regexp(r"^cc:public:\d+:(0|1)$"))
async def handle_cc_public_set(callback: CallbackQuery):
    _, _, cid, val = callback.data.split(":")
    cid, is_public = int(cid), int(val)
    await MySQLPool.update_user_collection(collection_id=cid, is_public=is_public)
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    await callback.message.edit_reply_markup(reply_markup=is_public_keyboard(cid, rec.get("is_public")))
    await callback.answer("✅ 已更新可见性设置")

# ========= 返回（从输入页回设置菜单 / 从“我的合集”回合集主菜单） =========

@router.callback_query(F.data.regexp(r"^cc:back:\d+$"))
async def handle_cc_back(callback: CallbackQuery):
    _, _, cid = callback.data.split(":")
    cid = int(cid)
    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    title = rec.get("title") if rec else "未命名合集"
    desc  = rec.get("description") if rec else ""
    pub   = "公开" if (rec and rec.get("is_public") == 1) else "不公开"

    await callback.message.edit_text(
        f"当前设置：\n• ID：{cid}\n• 标题：{title}\n• 公开：{pub}\n• 简介：{_short(desc,120)}\n\n请选择要设置的项目：",
        reply_markup=create_collection_menu_keyboard(cid)
    )



# == 历史菜单 ==
def history_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 查看我的历史记录", callback_data="view_my_history")],
        [InlineKeyboardButton(text="🗑️ 清除我的历史记录", callback_data="clear_my_history")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 猜你喜欢菜单 ==
def guess_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 查看推荐资源", callback_data="view_recommendations")],
        [InlineKeyboardButton(text="🔙 返回首页", callback_data="go_home")],
    ])

# == 资源上传菜单 ==
def upload_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 上传资源", callback_data="do_upload_resource")],
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

        ret_content, file_info, user_info = result
        source_id = file_info[0] if len(file_info) > 0 else None
        file_type = file_info[1] if len(file_info) > 1 else None
        file_id = file_info[2] if len(file_info) > 2 else None
        thumb_file_id = file_info[3] if len(file_info) > 3 else None
        owner_user_id = user_info[0] if user_info[0] else None
        fee = user_info[1] if user_info[1] else None


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
        if not file_id:
            print("❌ 没有找到 file_id",flush=True)
            await MySQLPool.fetch_file_by_file_id(file_id)
            print(f"🔍 完成",flush=True)


# == 启动指令 ==
@router.message(Command("start"))
async def handle_start(message: Message, state: FSMContext, command: Command = Command("start")):
    # 删除 /start 这个消息
    try:
        await message.delete()
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter) as e:
        print(f"❌ 删除 /start 消息失败: {e}", flush=True)

    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        param = args[1].strip()
        parts = param.split("_")

        if parts[0] == "f":

            search_key_index = parts[1]
            encoded = "_".join(parts[2:])  # 剩下的部分重新用 _ 拼接
            print(f"🔍 搜索关键字索引: {search_key_index}, 编码内容: {encoded}")
            # encoded = param[2:]  # 取第三位开始的内容
            try:
                aes = AESCrypto(AES_KEY)
                content_id_str = aes.aes_decode(encoded)
                
                content_id = int(content_id_str)  # ✅ 关键修正

                shared_url = f"https://t.me/{lz_var.bot_username}?start=f_-1_{encoded}"

               
          
                # ✅ 调用并解包返回的三个值
                ret_content, [source_id, file_type,file_id, thumb_file_id], [owner_user_id,fee] = await load_sora_content_by_id(content_id, state, search_key_index)
                # print(f"thumb_file_id:{thumb_file_id}")
                # ✅ 检查是否找不到资源（根据返回第一个值）
                if ret_content.startswith("⚠️"):
                    await message.answer(ret_content, parse_mode="HTML")
                    return

                if ENVIRONMENT == "dev":
                    reply_markup = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text="⬅️", callback_data=f"sora_page:{search_key_index}:0:-1"),
                            InlineKeyboardButton(text=f"💎 {fee}", callback_data=f"sora_redeem:{content_id}"),
                            InlineKeyboardButton(text="➡️", callback_data=f"sora_page:{search_key_index}:0:1"),
                        ],
                        [
                            InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                        ],
                        [
                            InlineKeyboardButton(text="🔗 复制", copy_text=CopyTextButton(text=shared_url))
                        ]
                    ])
                else:
                    reply_markup = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text=f"💎 {fee}", callback_data=f"sora_redeem:{content_id}")
                        ],
                        [
                            InlineKeyboardButton(text="🔗 复制", copy_text=CopyTextButton(text=shared_url))
                        ]
                    ])


                # ✅ 发送带封面图的消息
                # print(f"{thumb_file_id}")
                # print(f"{file_id}")
                await message.answer_photo(
                    photo=thumb_file_id,
                    caption=ret_content,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )

            except Exception as e:
                # tb = traceback.format_exc()
                await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                print(f"❌ 解密失败：{e}", flush=True)
        elif parts[0] == "post":
            await _submit_to_lg()
        else:
            await message.answer(f"📦 你提供的参数是：`{param}`", parse_mode="HTML")
    else:
        await message.answer("👋 欢迎使用 LZ 机器人！请选择操作：", reply_markup=main_menu_keyboard())
        pass


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



# == 主菜单选项响应 ==
@router.callback_query(F.data == "search")
async def handle_search(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=search_menu_keyboard())

@router.callback_query(F.data == "ranking")
async def handle_ranking(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=ranking_menu_keyboard())

@router.callback_query(F.data == "collection")
async def handle_collection(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=collection_menu_keyboard())

@router.callback_query(F.data == "my_history")
async def handle_my_history(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=history_menu_keyboard())

@router.callback_query(F.data == "guess_you_like")
async def handle_guess_you_like(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=guess_menu_keyboard())

@router.callback_query(F.data == "upload_resource")
async def handle_upload_resource(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=upload_menu_keyboard())

# == 搜索选项响应 ==
@router.callback_query(F.data == "keyword_search")
async def handle_keyword_search(callback: CallbackQuery):
    await callback.message.answer("🔑 请输入你要搜索的关键字...")

@router.callback_query(F.data == "tag_filter")
async def handle_tag_filter(callback: CallbackQuery):
    await callback.message.answer("🏷️ 请选择标签进行筛选...")

# == 排行选项响应 ==
@router.callback_query(F.data == "hot_resource_ranking")
async def handle_hot_resource_ranking(callback: CallbackQuery):
    await callback.message.answer("🔥 当前资源排行榜如下：...")

@router.callback_query(F.data == "hot_uploader_ranking")
async def handle_hot_uploader_ranking(callback: CallbackQuery):
    await callback.message.answer("👑 当前上传者排行榜如下：...")




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

    list_prefix = "cc:mlist" if mode == "mine" else "cc:flist"
    edit_prefix = "cc:edit"  if mode == "mine" else "cc:fedit"

    kb_rows: list[list[InlineKeyboardButton]] = []

    if not display:
        # 空列表：mine 显示创建，fav 不显示创建
        if mode == "mine":
            kb_rows.append([InlineKeyboardButton(text="➕ 创建合集", callback_data="create_collection")])
        kb_rows.append([InlineKeyboardButton(text="🔙 返回上页", callback_data="collection")])
        return InlineKeyboardMarkup(inline_keyboard=kb_rows)

    # 6 行合集按钮
    for r in display:
        cid = r.get("id")
        btn_text = _collection_btn_text(r)
        kb_rows.append([InlineKeyboardButton(text=btn_text, callback_data=f"{edit_prefix}:{cid}:{page}")])

    # 第 7 行：上一页 | [创建，仅 mine] | 下一页
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"{list_prefix}:{page-1}"))

    if mode == "mine":
        nav_row.append(InlineKeyboardButton(text="➕ 创建", callback_data="create_collection"))

    if has_next:
        nav_row.append(InlineKeyboardButton(text="下一页 ➡️", callback_data=f"{list_prefix}:{page+1}"))

    # 有可能出现只有“上一页/下一页”而中间没有“创建”的情况（fav 模式）
    if nav_row:
        kb_rows.append(nav_row)

    # 返回上页
    kb_rows.append([InlineKeyboardButton(text="🔙 返回上页", callback_data="collection")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)



# ====== “我的合集”入口用通用键盘（保持既有行为）======

@router.callback_query(F.data == "my_collections")
async def handle_my_collections(callback: CallbackQuery):
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=0, mode="mine")
    # “我的合集”之前是只换按钮；为了统一体验，也可以换 text，但你要求按钮呈现，因此只换按钮：
    await callback.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:mlist:\d+$"))
async def handle_my_collections_pager(callback: CallbackQuery):
    _, _, page_str = callback.data.split(":")
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=int(page_str), mode="mine")
    await callback.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:edit:\d+:\d+$"))
async def handle_my_collections_edit(callback: CallbackQuery):
    _, _, cid_str, page_str = callback.data.split(":")
    cid = int(cid_str)
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
    await callback.message.edit_text(text, reply_markup=create_collection_menu_keyboard(cid))


# ====== “我收藏的合集”入口（复用通用键盘，mode='fav'）======

@router.callback_query(F.data == "create_collection")
async def handle_create_collection(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    ret = await MySQLPool.create_user_collection(user_id=user_id)  # 默认：未命名合集、公开
    cid = ret.get("id")
    await state.update_data({"collection_id": cid})

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
    await callback.message.edit_text(text, reply_markup=create_collection_menu_keyboard(cid))


@router.callback_query(F.data == "my_favorite_collections")
async def handle_my_favorite_collections(callback: CallbackQuery):
    user_id = callback.from_user.id
    kb = await build_collections_keyboard(user_id=user_id, page=0, mode="fav")
    await callback.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.regexp(r"^cc:flist:\d+$"))
async def handle_my_favorite_collections_pager(callback: CallbackQuery):
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

@router.callback_query(F.data.regexp(r"^cc:fedit:\d+:\d+$"))
async def handle_my_favorite_collections_view(callback: CallbackQuery):
    _, _, cid_str, page_str = callback.data.split(":")
    cid = int(cid_str)
    page = int(page_str)

    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    if not rec:
        await callback.answer("⚠️ 未找到该合集", show_alert=True)
        return

    title = rec.get("title") or "未命名合集"
    desc  = rec.get("description") or ""
    pub   = "公开" if (rec.get("is_public") == 1) else "不公开"

    text = (
        f"📌 合集详情（只读）\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc, 600)}"
    )
    await callback.message.edit_text(text, reply_markup=favorite_detail_keyboard(page))



# ====== “我收藏的合集”入口（复用通用键盘，mode='fav'）======


    _, _, cid_str, page_str = callback.data.split(":")
    cid = int(cid_str)
    page = int(page_str)

    rec = await MySQLPool.get_user_collection_by_id(collection_id=cid)
    if not rec:
        await callback.answer("⚠️ 未找到该合集", show_alert=True)
        return

    title = rec.get("title") or "未命名合集"
    desc  = rec.get("description") or ""
    pub   = "公开" if (rec.get("is_public") == 1) else "不公开"

    text = (
        f"📌 合集详情（只读）\n"
        f"• ID：{cid}\n"
        f"• 标题：{title}\n"
        f"• 公开：{pub}\n"
        f"• 简介：{_short(desc, 600)}"
    )
    await callback.message.edit_text(text, reply_markup=favorite_detail_keyboard(page))

@router.callback_query(F.data == "explore_marketplace")
async def handle_explore_marketplace(callback: CallbackQuery):
    await callback.message.answer("🛍️ 欢迎来到合集市场，看看其他人都在收藏什么吧！")

# == 历史记录选项响应 ==
@router.callback_query(F.data == "view_my_history")
async def handle_view_my_history(callback: CallbackQuery):
    await callback.message.answer("📜 这是你的浏览历史：...")

@router.callback_query(F.data == "clear_my_history")
async def handle_clear_my_history(callback: CallbackQuery):
    await callback.message.answer("🗑️ 你的历史记录已清除。")

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
    await callback.message.edit_reply_markup(reply_markup=main_menu_keyboard())


@router.callback_query(F.data.startswith("sora_page:"))
async def handle_sora_page(callback: CallbackQuery, state: FSMContext):
    try:
        # 新 callback_data 结构: sora_page:<search_key_index>:<current_pos>:<offset>
        _, search_key_index_str, current_pos_str, offset_str = callback.data.split(":")
        search_key_index = int(search_key_index_str)
        current_pos = int(current_pos_str)
        offset = int(offset_str)

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

        # 计算新的 pos
        new_pos = current_pos + offset
        if new_pos < 0 or new_pos >= len(result):
            await callback.answer("⚠️ 没有上一项 / 下一项", show_alert=True)
            return

        # 取对应 content_id
        next_record = result[new_pos]
        next_content_id = next_record["id"]

        # 调用 load_sora_content_by_id
        ret_content, [source_id, file_type, file_id, thumb_file_id], [owner_user_id,fee] = await load_sora_content_by_id(next_content_id, state, search_key_index)

        if ret_content.startswith("⚠️"):
            await callback.answer(ret_content, show_alert=True)
            return

        if ENVIRONMENT == "dev":
            reply_markup = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="⬅️", callback_data=f"sora_page:{search_key_index}:{new_pos}:-1"),
                    InlineKeyboardButton(text=f"💎 {fee}", callback_data=f"sora_redeem:{next_content_id}"),
                    InlineKeyboardButton(text="➡️", callback_data=f"sora_page:{search_key_index}:{new_pos}:1"),
                ],
                [
                    InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                ]
            ])
        else:
            reply_markup = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text=f"💎 {fee}", callback_data=f"sora_redeem:{next_content_id}")
                ]
            ])


        await callback.message.edit_media(
            media={
                "type": "photo",
                "media": thumb_file_id,
                "caption": ret_content,
                "parse_mode": "HTML"
            },
            reply_markup=reply_markup
        )
        await callback.answer()

    except Exception as e:
        print(f"❌ handle_sora_page error: {e}")
        await callback.answer("⚠️ 翻页失败", show_alert=True)


@router.callback_query(F.data.startswith("sora_redeem:"))
async def handle_redeem(callback: CallbackQuery, state: FSMContext):
    source_id = callback.data.split(":")[1]

    result = await load_sora_content_by_id(int(source_id), state)
    # print("Returned==>:", result)

    ret_content, file_info, user_info = result
    source_id = file_info[0] if len(file_info) > 0 else None
    file_type = file_info[1] if len(file_info) > 1 else None
    file_id = file_info[2] if len(file_info) > 2 else None
    thumb_file_id = file_info[3] if len(file_info) > 3 else None

    owner_user_id = user_info[0] if user_info[0] else None
    fee = user_info[1] if user_info[1] else 0

    

    if not file_id:
        print("❌ 没有找到匹配记录 source_id")
        await callback.answer("👻 我们正偷偷的从院长的硬盘把这个资源搬出来，这段时间先看看别的资源吧。", show_alert=True)
        # await callback.message.reply("👻 我们正偷偷的从院长的硬盘把这个资源搬出来，这段时间先看看别的资源吧。")
        await lz_var.bot.delete_message(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id
        )
        return
    
    # 若有,则回覆消息
    from_user_id = callback.from_user.id
    sender_fee = int(fee) * (-1)  # ✅ 发送者手续费
    receiver_fee = int(fee) * (0.4)
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



    if result.get('status') == 'exist' or result.get('status') == 'insert' or result.get('status') == 'reward_self':

        if result.get('status') == 'exist':
            reply_text = f"✅ 你已经兑换过此资源，不需要扣除积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {user_point}。"
        elif result.get('status') == 'insert':
            
            reply_text = f"✅ 兑换成功，已扣除 {sender_fee} 积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {(user_point+sender_fee)}。"
       
        elif result.get('status') == 'reward_self':
            
            reply_text = f"✅ 这是你自己的资源"
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
            if file_type == "photo" or file_type == "p":
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
        
        return
    elif result.get('status') == 'insufficient_funds':
       
        reply_text = f"❌ 你的积分不足 ( {user_point} ) ，无法兑换此资源 ( {abs(sender_fee)} )。"
        await callback.answer(reply_text, show_alert=True)
        # await callback.message.reply(reply_text, parse_mode="HTML")
        return
        

   

# 📌 功能函数：根据 sora_content id 载入资源
async def load_sora_content_by_id(content_id: int, state: FSMContext, search_key_index=None) -> str:
    convert = UnitConverter()  # ✅ 实例化转换器
    record = await db.search_sora_content_by_id(content_id)
    print(f"🔍 载入 ID: {content_id}, Record: {record}", flush=True)
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
        
        # print(f"{record}")

        # print(f"🔍 载入 ID: {record_id}, Source ID: {source_id}, thumb_file_id:{thumb_file_id}, File Type: {file_type}\r\n")

        # ✅ 若 thumb_file_id 为空，则给默认值
        if not thumb_file_id and thumb_file_unique_id != None:
            print(f"🔍 没有找到 thumb_file_id，尝试从 thumb_file_unique_id {thumb_file_unique_id} 获取")


            thumb_file_id = await Media.fetch_file_by_file_id_from_x(state, thumb_file_unique_id, 10)
           

        if not thumb_file_id:
            print("❌ 在延展库没有，用预设图")
            
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
                        thumb_file_id = await Media.fetch_file_by_file_id_from_x(state, unique_id, 10)
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
            keyword = await db.get_keyword_by_id(int(search_key_index))
            if keyword:
                ret_content += f"\r\n🔑 关键字: {keyword}\n\n"

        if ret_content:
            tag_length = len(ret_content)
    

        if not file_id and source_id:
            # 不阻塞：丢到后台做补拉
            spawn_once(f"src:{source_id}", Media.fetch_file_by_file_id_from_x(state, source_id, 10))

        # 计算可用空间
        available_content_length = max_total_length - tag_length - 50  # 预留额外描述字符
        
       
        # print(f"长度 {available_content_length}")


        # 裁切内容
        
        content_preview = content[:available_content_length]
        if len(content) > available_content_length:
            content_preview += "..."

        if ret_content:
            ret_content = content_preview+"\r\n\r\n"+ret_content
        else:
            ret_content = content_preview
        

        # ✅ 返回三个值
        return ret_content, [source_id, file_type, file_id, thumb_file_id], [owner_user_id, fee]
        
    else:
        return f"⚠️ 没有找到 ID 为 {content_id} 的 Sora 内容记录"