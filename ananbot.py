import logging
from datetime import datetime
import asyncio
import time
from typing import Optional, Coroutine, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    InputMediaPhoto,
    BufferedInputFile,
)
from aiogram.enums import ChatAction,ContentType
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
import aiohttp

from ananbot_utils import AnanBOTPool  # ✅ 修改点：改为统一导入类
from utils.media_utils import Media
from ananbot_config import BOT_TOKEN, BOT_MODE, WEBHOOK_HOST, WEBHOOK_PATH, REVIEW_CHAT_ID, REVIEW_THREAD_ID,WEBAPP_HOST, WEBAPP_PORT,PUBLISH_BOT_TOKEN
import lz_var
from lz_config import AES_KEY

from utils.prof import SegTimer
from utils.aes_crypto import AESCrypto

from utils.product_utils import submit_resource_to_chat_action

bot = Bot(token=BOT_TOKEN)
lz_var.bot = bot

publish_bot = Bot(token=PUBLISH_BOT_TOKEN)

# 全局变量缓存 bot username
media_upload_tasks: dict[tuple[int, int], asyncio.Task] = {}

# 全局缓存延迟刷新任务
tag_refresh_tasks: dict[tuple[int, str], asyncio.Task] = {}

# ✅ 匿名选择的超时任务缓存
anonymous_choice_tasks: dict[tuple[int, int], asyncio.Task] = {}

# 在模块顶部添加
has_prompt_sent: dict[tuple[int, int], bool] = {}


# product_info 缓存，最多缓存 100 个，缓存时间 60 秒
product_info_cache: dict[int, dict] = {}
product_info_cache_ts: dict[int, float] = {}
PRODUCT_INFO_CACHE_TTL = 60  # 秒
PRODUCT_INFO_CACHE_MAX = 100  # 新增：最多缓存条数
product_review_url_cache: dict[int, str] = {}

DEFAULT_THUMB_FILE_ID = ""

# ===== 举报类型：全域配置 =====
REPORT_TYPES: dict[int, str] = {
    11: "预览图不符",
    12: "描述不符",
    13: "标签不符",
    21: "播放故障",
    31: "无解密密码",
    32: "密码错误",
    33: "分包",
    90: "其他",
}

INPUT_TIMEOUT = 180

COLLECTION_PROMPT_DELAY = 2
TAG_REFRESH_DELAY = 0.7
BG_TASK_TIMEOUT = 15



_background_tasks: dict[str, asyncio.Task] = {}

def spawn_once(key: str, coro: "Coroutine"):
    """相同 key 的后台任务只跑一个；结束后自动清理。"""
    task = _background_tasks.get(key)
    if task and not task.done():
        return

    async def _runner():
        try:
            # 可按需加超时
            await asyncio.wait_for(coro, timeout=BG_TASK_TIMEOUT)
        except Exception as e:
            print(f"🔥 background task failed for key={key} {e}", flush=True)

    t = asyncio.create_task(_runner(), name=f"backfill:{key}")
    _background_tasks[key] = t
    t.add_done_callback(lambda _: _background_tasks.pop(key, None))


bot_username = None
dp = Dispatcher(storage=MemoryStorage())

class ProductPreviewFSM(StatesGroup):
    waiting_for_preview_photo = State(state="product_preview:waiting_for_preview_photo")
    waiting_for_price_input = State(state="product_preview:waiting_for_price_input")
    waiting_for_collection_media = State(state="product_preview:waiting_for_collection_media")
    waiting_for_removetag_source = State(state="product_preview:waiting_for_removetag_source")  
    waiting_for_content_input = State(state="product_preview:waiting_for_content_input")  
    waiting_for_thumb_reply = State(state="product_preview:waiting_for_thumb_reply")  
    waiting_for_x_media = State()
    waiting_for_anonymous_choice = State(state="product_preview:waiting_for_anonymous_choice")
    waiting_for_report_type = State(state="report:waiting_for_type")
    waiting_for_report_reason = State(state="report:waiting_for_reason")

async def health(request):
    return web.Response(text="✅ Bot 正常运行", status=200)


@dp.message(
    (F.photo | F.video | F.document)
    & (F.from_user.id == lz_var.x_man_bot_id)
    & F.reply_to_message.as_("reply_to")
)
async def handle_x_media_when_waiting(message: Message, state: FSMContext, reply_to: Message):
    """
    仅在等待态才处理；把 file_unique_id 写到 FSM。
    """
    if await state.get_state() != ProductPreviewFSM.waiting_for_x_media.state:
        print(f"【Telethon】收到非等待态的私聊媒体，跳过处理。当前状态：{await state.get_state()}", flush=True)
        return  # 非等待态，跳过


    file_unique_id = None

    if message.photo:
        largest_photo = message.photo[-1]
        file_unique_id = largest_photo.file_unique_id
        file_id = largest_photo.file_id
        file_type = "photo"
    elif message.video:
        file_unique_id = message.video.file_unique_id
        file_id = message.video.file_id
        file_type = "video"
    elif message.document:
        file_unique_id = message.document.file_unique_id
        file_id = message.document.file_id
        file_type = "document"
    else:
        return

    print(f"✅ [X-MEDIA] 收到 {file_type}，file_unique_id={file_unique_id} {file_id}，"
          f"from={message.from_user.id}，reply_to_msg_id={reply_to.message_id}", flush=True)

    user_id = int(message.from_user.id) if message.from_user else None
    
    lz_var.bot_username = await get_bot_username()

    await AnanBOTPool.insert_file_extension(
        file_type,
        file_unique_id=file_unique_id,
        file_id=file_id,
        bot_username=lz_var.bot_username,
        user_id=user_id
    )


    # 把结果写回 FSM
    await state.update_data({"x_file_unique_id": file_unique_id})
    await state.update_data({"x_file_id": file_id})



def build_report_type_keyboard(file_unique_id: str, transaction_id: int) -> InlineKeyboardMarkup:
    """
    根据 REPORT_TYPES 全域配置，生成举报类型按钮。
    一行一个，避免被 Telegram 截断。
    """
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"report_type:{file_unique_id}:{transaction_id}:{code}")]
        for code, label in REPORT_TYPES.items()
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_largest_photo(photo_sizes):
    return max(photo_sizes, key=lambda p: p.width * p.height)


async def get_bot_username():
    global bot_username
    if not bot_username:
        bot_info = await bot.get_me()
        bot_username = bot_info.username

    if not lz_var.bot_username:
        lz_var.bot_username = bot_username
    
    return bot_username


def format_bytes(size: int) -> str:
    if size is None:
        return "0B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}PB"

def format_seconds(seconds: int) -> str:
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h:
        return f"{h}:{m:02}:{s:02}"
    elif m:
        return f"{m}:{s:02}"
    else:
        return f"0:{s:02}"

async def get_list(content_id):

    list_text = ''
    bot_username = await get_bot_username()
    results = await AnanBOTPool.get_collect_list(content_id, bot_username)
    list_text = await list_template(results)
    return list_text



async def list_template(results):
    collect_list_text = ''
    collect_cont_list_text = ''
    list_text = ''
    video_count = document_count = photo_count = 0

    for row in results:
        file_type = row["file_type"]
        file_size = row.get("file_size", 0)
        duration = row.get("duration", 0)

        if file_type == "v":
            video_count += 1
            collect_list_text += f"　🎬 {format_bytes(file_size)} | {format_seconds(duration)}\n"
        elif file_type == "d":
            document_count += 1
            collect_list_text += f"　📄 {format_bytes(file_size)}\n"
        elif file_type == "p":
            photo_count += 1
            collect_list_text += f"　🖼️ {format_bytes(file_size)}\n"

    if video_count:
        collect_cont_list_text += f"🎬 x{video_count} 　"
    if document_count:
        collect_cont_list_text += f"📄 x{document_count} 　"
    if photo_count:
        collect_cont_list_text += f"🖼️ x{photo_count}"

    if collect_list_text:
        list_text += "\n📦 文件列表：\n" + collect_list_text.rstrip()
    if collect_cont_list_text:
        list_text += "\n\n📊 本合集包含：" + collect_cont_list_text

    return list_text


@dp.callback_query(F.data.startswith("make_product:"))
async def make_product(callback_query: CallbackQuery, state: FSMContext):
    parts = callback_query.data.split(":")
    content_id, file_type, file_unique_id, user_id = parts[1], parts[2], parts[3], parts[4]

    product_id = await AnanBOTPool.get_existing_product(content_id)
    if not product_id:

        row = await AnanBOTPool.get_sora_content_by_id(content_id)
        if row.get("content"):
            content = row["content"]
        else:
            content = "请修改描述"

        await AnanBOTPool.create_product(content_id, "默认商品", content, 68, file_type, user_id)
    
    thumb_file_id,preview_text,preview_keyboard = await get_product_tpl(content_id)
    await callback_query.message.delete()
    new_msg = await callback_query.message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
    await update_product_preview(content_id, thumb_file_id, state, new_msg)


async def get_product_tpl(content_id: int | str) -> tuple[str, str, InlineKeyboardMarkup]:
    content_id = int(content_id)  # 兜底：/review 等场景传的是字符串
    product_row = await get_product_info(content_id)

    print(f"🔍 get_product_tpl for content_id={content_id}", flush=True)

    thumb_file_id = product_row.get("thumb_file_id") or ""
    preview_text = product_row.get("preview_text") or ""
    preview_keyboard = product_row.get("preview_keyboard")

    return thumb_file_id, preview_text, preview_keyboard

async def get_product_info(content_id: int):
    
    # 统一初始化，避免未赋值
    buttons: list[list[InlineKeyboardButton]] = []

    # 统一从工具函数取
    cached = get_cached_product(content_id)
    if cached is not None:
        print(f"\r\nfrom cache", flush=True)
        return cached
    else:
        print(f"\r\nnot from cache", flush=True)

    # 查询是否已有同 source_id 的 product
    # 查找缩图 file_id
    bot_username = await get_bot_username()
    product_info = await AnanBOTPool.search_sora_content_by_id(content_id, bot_username)

    thumb_file_id = product_info.get("m_thumb_file_id") or DEFAULT_THUMB_FILE_ID
    thumb_unique_id = product_info.get("thumb_file_unique_id")
    file_unique_id = product_info.get('source_id')
    file_id = product_info.get('m_file_id')
    anonymous_mode = product_info.get('anonymous_mode',1)
    owner_user_id = product_info.get('owner_user_id') or 0
    file_type = product_info.get('file_type', '')
    content = product_info.get('content', '')
    
    '''
    审核状态
    0   编辑中(投稿者)
    1   未通过审核(投稿者)
    2   初审进行中
    3   通过初审,复审进行中
    4   经检举,初审进行中
    6   通过终核,上架进行中
    7   上架失败
    9   成功上架 
    '''



    if product_info.get('fee') is None:
        product_info['fee'] = 68

    if not product_info.get('product_id'):
        await AnanBOTPool.create_product(content_id, "默认商品", content, product_info['fee'], file_type, owner_user_id)
        product_info['review_status'] = 0

    review_status = product_info.get('review_status')



    anonymous_button_text = ''
    if anonymous_mode == 1:
        anonymous_button_text = "🙈 发表模式: 匿名发表"
    elif anonymous_mode == 3:
        anonymous_button_text = "🐵 发表模式: 公开上传者"


    content_list = await get_list(content_id)

    preview_text = f"数据库ID:<code>{content_id}</code> <code>{file_unique_id}</code>"
    
    if(product_info['content']  and product_info['content'].strip() != ''):
        preview_text += f"\n\n{shorten_content(product_info['content'],300)}"

    if(product_info['tag']  and product_info['tag'].strip() != ''):
        preview_text += f"\n\n<i>{product_info['tag']}</i>"

    if(content_list  and content_list.strip() != ''):
        preview_text += f"\n\n<i>{content_list}</i>"

    # if review_status == 3 or review_status==4 or review_status==5:
    #     await AnanBOTPool.check_guild_manager(content_id)

    if review_status == 4:

        report_info = await AnanBOTPool.find_existing_report(file_unique_id)  

        if report_info:
            # 取举报类型（可能为 None）
            rtype = report_info.get('report_type')
            # 允许字符串数字；安全转 int
            try:
                rtype_int = int(rtype) if rtype is not None else None
            except Exception:
                rtype_int = None

            rtype_label = REPORT_TYPES.get(rtype_int, "其他") if rtype_int is not None else "其他"
            reason = (report_info.get('report_reason') or '').strip()

            preview_text += "\n\n"
            preview_text += f"<blockquote>举报类型：</blockquote>\n {rtype_label}"
            if reason:
                preview_text += f"\n\n<blockquote>举报原因：</blockquote>\n {reason}"

           

    
    if review_status <= 3:
    
        if review_status <= 1:
            # 按钮列表构建
            buttons = [
                [
                    InlineKeyboardButton(text="📝 内容", callback_data=f"set_content:{content_id}:0"),
                    InlineKeyboardButton(text="📷 预览", callback_data=f"set_preview:{content_id}")
                ]
            ]
        else:
            buttons = [
                [
                    InlineKeyboardButton(text="📝 内容", callback_data=f"set_content:{content_id}:1"),
                    InlineKeyboardButton(text="📷 预览", callback_data=f"set_preview:{content_id}")
                ]
            ]

        if product_info['file_type'] in ['document', 'collection']:
            buttons.append([
                InlineKeyboardButton(text="🔒 密码", callback_data=f"set_password:{content_id}")
            ])

        if review_status == 0 or review_status == 1:
            buttons.extend([
                [
                    InlineKeyboardButton(text="🏷️ 标签", callback_data=f"tag_full:{content_id}"),
                    InlineKeyboardButton(text="🧩 系列", callback_data=f"series:{content_id}")
                ],
                [InlineKeyboardButton(text=f"💎 积分 ({product_info['fee']})", callback_data=f"set_price:{content_id}")],
                [InlineKeyboardButton(text=f"{anonymous_button_text}", callback_data=f"toggle_anonymous:{content_id}")],
                [InlineKeyboardButton(text="➕ 添加资源", callback_data=f"add_items:{content_id}")],
                [
                    InlineKeyboardButton(text="📬 提交投稿", callback_data=f"submit_product:{content_id}"),
                    InlineKeyboardButton(text="❌ 取消投稿", callback_data=f"cancel_publish:{content_id}")
                ]
            ])

        elif review_status == 2:
            # 初审
            buttons.extend([
                [
                    InlineKeyboardButton(text="🏷️ 标签", callback_data=f"tag_full:{content_id}"),
                    InlineKeyboardButton(text="🧩 系列", callback_data=f"series:{content_id}")
                ],
                [
                    InlineKeyboardButton(text="✅ 通过审核并写入", callback_data=f"approve_product:{content_id}:6"),
                    InlineKeyboardButton(text="❌ 拒绝投稿", callback_data=f"approve_product:{content_id}:1")
                ]




            ])
            # 待审核
        elif review_status == 3:
            
            buttons.extend([
                [
                    InlineKeyboardButton(text="🏷️ 标签", callback_data=f"tag_full:{content_id}")
                ],
                [
                    InlineKeyboardButton(text="✅ 通过且写入", callback_data=f"approve_product:{content_id}:6"),
                    InlineKeyboardButton(text="❌ 拒绝投稿", callback_data=f"approve_product:{content_id}:1")
                ]
            ])
            # 待审核            
    elif review_status == 4:
        # 按钮列表构建
        buttons = [
            [
                InlineKeyboardButton(text="📝 内容", callback_data=f"set_content:{content_id}"),
                InlineKeyboardButton(text="📷 预览", callback_data=f"set_preview:{content_id}")
            ]
        ]

        buttons.extend([
            [
                InlineKeyboardButton(text="🏷️ 标签", callback_data=f"tag_full:{content_id}"),
                InlineKeyboardButton(text="🧩 系列", callback_data=f"series:{content_id}")
            ],
            [
                InlineKeyboardButton(text="✅ 认可举报", callback_data=f"judge_suggest:{content_id}:'Y'"),
                InlineKeyboardButton(text="❌ 不认可举报", callback_data=f"judge_suggest:{content_id}:'N'")
            ]
        ])
    elif review_status == 6:
        buttons = [[InlineKeyboardButton(text="通过审核,等待上架", callback_data=f"none")]]
    elif review_status == 7:
        buttons = [[InlineKeyboardButton(text="通过审核,但上架失败", callback_data=f"none")]]
    elif review_status == 9:
        buttons = [[InlineKeyboardButton(text="通过审核,已上架", callback_data=f"none")]]
    elif review_status == 10:
        buttons = [[InlineKeyboardButton(text="资源已失效", callback_data=f"none")]]
    elif review_status == 11:
        buttons = [[InlineKeyboardButton(text="资源已失效(同步)", callback_data=f"none")]]

    return_url = product_review_url_cache.get(content_id)
    if return_url:
        buttons.extend([
            [
                InlineKeyboardButton(text="🔙 返回审核", url=f"{return_url}")
            ]
        ])


    product_info['buttons'] = buttons
    preview_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)


    # 写入缓存
    set_cached_product(content_id, {
        "thumb_file_id": thumb_file_id,
        "thumb_unique_id": thumb_unique_id,
        "preview_text": preview_text,
        "preview_keyboard": preview_keyboard,
        "product_info": product_info
    })

    # return thumb_file_id, preview_text, preview_keyboard
    return {
        "thumb_file_id": thumb_file_id,
        "preview_text": preview_text,
        "preview_keyboard": preview_keyboard,
        "product_info": product_info
    }


def shorten_content(text: str, max_length: int = 30) -> str:
    if not text:
        return ""
    text = text.replace('\n', '').replace('\r', '')
    return text[:max_length] + "..." if len(text) > max_length else text


############
#  tag     
############

async def refresh_tag_keyboard(callback_query: CallbackQuery, content_id: str, type_code: str, state: FSMContext):
    # 一次查出所有 tag_type（保持原有排序）
    tag_types = await AnanBOTPool.get_all_tag_types()

    # ✅ 一次性查询所有标签并按 type_code 分组
    all_tags_by_type = await AnanBOTPool.get_all_tags_grouped()

    # 查询该资源的 file_unique_id
    sora_content = await AnanBOTPool.get_sora_content_by_id(content_id)
    file_unique_id = sora_content['source_id']

    # print(f"🔍 查询到 file_unique_id: {file_unique_id} for content_id: {content_id}")
    fsm_key = f"selected_tags:{file_unique_id}"
    data = await state.get_data()
    selected_tags = set(data.get(fsm_key, []))

    # 如果 FSM 中没有缓存，就从数据库查一次
    if selected_tags is None or not selected_tags or selected_tags == []:
        selected_tags = await AnanBOTPool.get_tags_for_file(file_unique_id)
        print(f"🔍 从数据库查询到选中的标签: {selected_tags} for file_unique_id: {file_unique_id},并更新到FSM")
        await state.update_data({fsm_key: list(selected_tags)})
    else:
        print(f"🔍 从 FSM 缓存中获取选中的标签: {selected_tags} for file_unique_id: {file_unique_id}")

    keyboard = []

    for tag_type in tag_types:
        current_code = tag_type["type_code"]
        current_cn = tag_type["type_cn"]

        tag_rows = all_tags_by_type.get(current_code, [])
        tag_codes = [tag["tag"] for tag in tag_rows]

        # 勾选统计
        selected_count = len(set(tag_codes) & set(selected_tags))
        total_count = len(tag_codes)
        # display_cn = f"{current_cn} ({selected_count}/{total_count})"

        # 需要显示已选标签名的 type_code
        SPECIAL_DISPLAY_TYPES = {'age', 'eth', 'face', 'feedback', 'nudity','par'}

        if current_code in SPECIAL_DISPLAY_TYPES:
            # 获取该类型下已选标签名
            selected_tag_names = [
                (tag["tag_cn"] or tag["tag"])
                for tag in tag_rows
                if tag["tag"] in selected_tags
            ]
            if selected_tag_names:
                display_cn = f"{current_cn} ( {'、'.join(selected_tag_names)} )"
            else:
                display_cn = f"{current_cn} (未选择)"
        else:
            display_cn = f"{current_cn} ( {selected_count}/{total_count} )"


        if current_code == type_code:
            # 当前展开的类型
            keyboard.append([
                InlineKeyboardButton(text=f"━━━ ▶️ {display_cn} ━━━ ", callback_data="noop")
            ])

            row = []
            for tag in tag_rows:
                tag_text = tag["tag_cn"] or tag["tag"]
                tag_code = tag["tag"]
                display = f"☑️ {tag_text}" if tag_code in selected_tags else tag_text

                row.append(InlineKeyboardButton(
                    text=display,
                    callback_data=f"add_tag:{content_id}:{tag_code}"
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
                    callback_data=f"set_tag_type:{content_id}:{current_code}"
                )
            ])

    # 添加「完成」按钮
    keyboard.append([
        InlineKeyboardButton(
            text="✅ 设置完成并返回",
            callback_data=f"back_to_product_from_tag:{content_id}"
        )
    ])

    await callback_query.message.edit_reply_markup(
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )

@dp.callback_query(F.data.startswith("add_tag:"))
async def handle_toggle_tag(callback_query: CallbackQuery, state: FSMContext):
    parts = callback_query.data.split(":")
    content_id = parts[1]
    tag = parts[2]
    user_id = callback_query.from_user.id

    # 获取资源 ID
    sora_content = await AnanBOTPool.get_sora_content_by_id(content_id)
    file_unique_id = sora_content['source_id']

    # # 是否已存在该标签
    # tag_exists = await AnanBOTPool.is_tag_exist(file_unique_id, tag)

    # if tag_exists:
    #     await AnanBOTPool.remove_tag(file_unique_id, tag)
    #     await callback_query.answer("☑️ 已移除标签，你可以一次性勾选，系统会稍后刷新", show_alert=False)
    # else:
    #     await AnanBOTPool.add_tag(file_unique_id, tag)
    #     await callback_query.answer("✅ 已添加标签，你可以一次性勾选，系统会稍后刷新", show_alert=False)

    # FSM 中缓存的打勾 tag 列表 key
    fsm_key = f"selected_tags:{file_unique_id}"

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

    # 获取该 tag 所属类型（用于后续刷新 keyboard）
    tag_info = await AnanBOTPool.get_tag_info(tag)
    if not tag_info:
        return
    type_code = tag_info["tag_type"]

    # 生成刷新任务 key
    task_key = (int(user_id), int(content_id))

    # 如果已有延迟任务，取消旧的
    old_task = tag_refresh_tasks.get(task_key)
    if old_task and not old_task.done():
        old_task.cancel()

    # 创建新的延迟刷新任务
    async def delayed_refresh():
        try:
            await asyncio.sleep(TAG_REFRESH_DELAY)
            await refresh_tag_keyboard(callback_query, content_id, type_code, state)
            tag_refresh_tasks.pop(task_key, None)
        except asyncio.CancelledError:
            pass  # 被取消时忽略

    tag_refresh_tasks[task_key] = asyncio.create_task(delayed_refresh())

@dp.callback_query(F.data.startswith("tag_full:"))
async def handle_tag_full(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]

    # 查询所有标签类型
    tag_types = await AnanBOTPool.get_all_tag_types()  # 你需要在 AnanBOTPool 中实现这个方法

    keyboard = []
    for tag in tag_types:
        type_code = tag["type_code"]
        type_cn = tag["type_cn"]
        keyboard.append([
            InlineKeyboardButton(
                text=f"[{type_cn}]",
                callback_data=f"set_tag_type:{content_id}:{type_code}"
            )
        ])

    # 添加「设置完成并返回」按钮
    keyboard.append([
        InlineKeyboardButton(
            text="✅ 设置完成并返回",
            callback_data=f"back_to_product:{content_id}"
        )
    ])

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    try:
        # await callback_query.message.edit_reply_markup(reply_markup=reply_markup)
        await refresh_tag_keyboard(callback_query, content_id, 'age', state)
    except Exception as e:
        print(f"⚠️ 编辑一页标签按钮失败: {e}", flush=True)
    
@dp.callback_query(F.data.startswith("set_tag_type:"))
async def handle_set_tag_type(callback_query: CallbackQuery, state: FSMContext):
    parts = callback_query.data.split(":")
    content_id = parts[1]
    type_code = parts[2]
    await refresh_tag_keyboard(callback_query, content_id, type_code, state)


@dp.callback_query(F.data.startswith("back_to_product_from_tag:"))
async def handle_back_to_product_from_tag(callback_query: CallbackQuery, state: FSMContext):
    content_id = int(callback_query.data.split(":")[1])
    user_id = callback_query.from_user.id

    # 1) 取 file_unique_id 与 FSM 的最终选择
   
    sora_content = await AnanBOTPool.get_sora_content_by_id(content_id)
    file_unique_id = sora_content['source_id']
    fsm_key = f"selected_tags:{file_unique_id}"
    data = await state.get_data()
    selected_tags = set(data.get(fsm_key, []))

    # 2) 一次性同步 file_tag（增删）
    try:
        summary = await AnanBOTPool.sync_file_tags(file_unique_id, selected_tags, actor_user_id=user_id)
    except Exception as e:
        logging.exception(f"落库标签失败: {e}")
        summary = {"added": 0, "removed": 0, "unchanged": 0}
        await callback_query.answer("⚠️ 标签保存失败，但已返回卡片", show_alert=False)

    # 3) 生成 hashtag 串并写回 sora_content.tag + stage='pending'
    try:
        # 根据 code 批量取中文名（无中文则回退 code）
        print(f"🔍 正在批量获取标签中文名: {selected_tags}", flush=True)
        tag_map = await AnanBOTPool.get_tag_cn_batch(list(selected_tags))
        print(f"🔍 获取标签中文名完成: {tag_map}", flush=True)
        tag_names = [tag_map[t] for t in selected_tags]  # 无序集合；如需稳定可按中文名排序
        print(f"🔍 生成 hashtag 串: {tag_names}", flush=True)
        # 可选：按中文名排序，稳定显示（建议）
        tag_names.sort()
        
        hashtag_str = Media.build_hashtag_string(tag_names, max_len=200)
        await AnanBOTPool.update_sora_content_tag_and_stage(content_id, hashtag_str)
    except Exception as e:
        logging.exception(f"更新 sora_content.tag 失败: {e}")

    # 4) 清理 FSM 里该资源的选择缓存 + 取消延时任务
    try:
        await state.update_data({fsm_key: []})
    except Exception:
        pass
    task_key = (int(user_id), int(content_id))
    old_task = tag_refresh_tasks.pop(task_key, None)
    if old_task and not old_task.done():
        old_task.cancel()

    # ✅ 重置缓存（删除）
    invalidate_cached_product(content_id)

    # 5) 回到商品卡片
    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    try:
        await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard
            
        )
    except Exception as e:
        logging.exception(f"返回商品卡片失败: {e}")

    # 6) 轻提示
    await callback_query.answer(
        f"✅ 标签已保存 (+{summary.get('added',0)}/-{summary.get('removed',0)})，内容待处理",
        show_alert=False
    )





############
#  add_items     
############



@dp.callback_query(F.data.startswith("add_items:"))
async def handle_add_items(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    chat_id = callback_query.message.chat.id
    message_id = callback_query.message.message_id
    content_list = await get_list(content_id)  # 获取合集列表，更新状态
    caption_text = f"{content_list}\n\n📥 请直接传送资源"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤添加完成并回设定页", callback_data=f"done_add_items:{content_id}")]
    ])

    try:
        await callback_query.message.edit_caption(caption=caption_text, reply_markup=keyboard)
    except Exception as e:
        print(f"⚠️ 编辑添加资源 caption 失败: {e}", flush=True)

    await state.clear()  # 👈 强制清除旧的 preview 状态
    await state.set_state(ProductPreviewFSM.waiting_for_collection_media)
    await state.set_data({
        "content_id": content_id,
        "chat_id": chat_id,
        "message_id": message_id,
        "last_button_ts": datetime.now().timestamp()
    })


async def _ensure_placeholder_once(message: Message, state: FSMContext):
    # 再读一次 FSM，已有就直接返回
    data = await state.get_data() or {}
    if data.get("placeholder_msg_id"):
        return
    # 创建并写回
    placeholder = await message.reply("⏳ 正在处理，请稍候...")
    await state.update_data({
        "placeholder_msg_id": placeholder.message_id,
        "chat_id": message.chat.id
    })


@dp.message(F.chat.type == "private", F.content_type.in_({
    ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT
}), ProductPreviewFSM.waiting_for_collection_media)
async def receive_collection_media(message: Message, state: FSMContext):
    data = await state.get_data()
    content_id = int(data["content_id"])
    chat_id = data["chat_id"]
    # message_id = data["message_id"]
    # placeholder_msg_id = data["placeholder_msg_id"] if "placeholder_msg_id" in data else None
    placeholder_msg_id = data.get("placeholder_msg_id") or None
    
    # 立即反馈：占位消息
    # await bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    # if not placeholder_msg_id:
    #     placeholder = await message.answer("⏳ 正在处理，请稍候...")
    #     placeholder_msg_id = placeholder.message_id
    #     await state.update_data({"placeholder_msg_id": placeholder_msg_id})

    # 先触发一次性“占位创建”，不等待完成（spawn_once 自带并发去重）
    spawn_once(
        ("placeholder", message.chat.id, content_id),
        _ensure_placeholder_once(message, state)
    )


    # 识别媒体属性（共通）
    if message.content_type == ContentType.PHOTO:
        file = get_largest_photo(message.photo)
        file_type = "photo"
        type_code = "p"
    elif message.content_type == ContentType.VIDEO:
        file = message.video
        file_type = "video"
        type_code = "v"
    else:
        file = message.document
        file_type = "document"
        type_code = "d"

    file_unique_id = file.file_unique_id
    file_id = file.file_id
    user_id = int(message.from_user.id)
    
    file_size = getattr(file, "file_size", 0)
    duration = getattr(file, "duration", 0)
    width = getattr(file, "width", 0)
    height = getattr(file, "height", 0)


    meta = {
        "content_id": content_id,
        "file_type":file_type,
        "file_size": file_size,
        "duration": duration,
        "width": width,
        "height": height,
        "file_unique_id": file_unique_id,
        "file_id": file_id
    }

    spawn_once(
        f"copy_message:{message.message_id}",
        lz_var.bot.copy_message(
            chat_id=lz_var.x_man_bot_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )
    )

    spawn_once(
        f"process_add_item_async:{message.message_id}",
        _process_add_item_async(message, state, meta, placeholder_msg_id)
    )

    print(f"添加资源：{file_type} {file_unique_id} {file_id}", flush=True)

    # --- 管理提示任务 ---
    key = (user_id, int(content_id))
    has_prompt_sent[key] = False

    # 若已有旧任务，取消
    old_task = media_upload_tasks.get(key)
    if old_task and not old_task.done():
        old_task.cancel()

    # await message.delete()

    # 创建新任务（3秒内无动作才触发）
    async def delayed_finish_prompt():
        try:
            await asyncio.sleep(COLLECTION_PROMPT_DELAY)
            current_state = await state.get_state()
            if current_state == ProductPreviewFSM.waiting_for_collection_media and not has_prompt_sent.get(key, False):
                has_prompt_sent[key] = True  # ✅ 设置为已发送，防止重复

                try:
                    list_text = await get_list(content_id)
                    caption_text = f"{list_text}\n\n📥 请直接传送资源"
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📤添加完成并回设定页", callback_data=f"done_add_items:{content_id}")]
                    ])
                    send_message = await bot.send_message(chat_id=chat_id, text=caption_text, reply_markup=keyboard)
                except Exception as e:
                    logging.exception(f"发送提示失败: {e}")

                try:
                    data = await state.get_data() or {}
                    placeholder_msg_id = data.get("placeholder_msg_id")
                    r=await bot.delete_message(chat_id, placeholder_msg_id)
                    print(f"删除占位消息结果: {r} {placeholder_msg_id}", flush=True)

                    await state.clear()  # 👈 强制清除旧的 preview 状态
                    await state.set_state(ProductPreviewFSM.waiting_for_collection_media)
                    await state.set_data({
                        "content_id": content_id,
                        "chat_id": send_message.chat.id,
                        "placeholder_msg_id": send_message.message_id,
                        "last_button_ts": datetime.now().timestamp()
                    })


                except Exception:
                    pass

                
        except asyncio.CancelledError:
            pass


    # 存入新的 task
    media_upload_tasks[key] = asyncio.create_task(delayed_finish_prompt())



@dp.callback_query(F.data.startswith("done_add_items:"))
async def done_add_items(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    
    user_id = int(callback_query.from_user.id)

    data = await state.get_data()
    chat_id = data["chat_id"]
    message_id = data["placeholder_msg_id"]

    try:
        await state.clear()
    except Exception:
        pass



    # 清除任务
   
    key = (user_id, int(content_id))
    task = media_upload_tasks.pop(key, None)
    if task and not task.done():
        task.cancel()
    has_prompt_sent.pop(key, None)  # ✅ 清除标记

    # 返回商品菜单

    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
        reply_markup=preview_keyboard
    )


async def _process_add_item_async(message: Message, state: FSMContext, meta: dict, placeholder_msg_id: int):
    bot_username = await get_bot_username()
    user_id = str(message.from_user.id)
    file_type = meta.get("file_type")
    file_size = meta.get("file_size", 0)
    duration = meta.get("duration", 0)
    width = meta.get("width", 0)
    height = meta.get("height", 0)
    file_unique_id = meta.get("file_unique_id")
    content_id = meta.get("content_id")
    file_id = meta.get("file_id")
    type_code = file_type[0]  # "v", "d", "p"
    await AnanBOTPool.update_product_file_type(content_id, "collection")
    await AnanBOTPool.upsert_media(file_type, {
            "file_unique_id": file_unique_id,
            "file_size": file_size,
            "duration": duration,
            "width": width,
            "height": height,
            "create_time": datetime.now()
        })
    
    await AnanBOTPool.insert_file_extension(file_type, file_unique_id, file_id, bot_username, user_id)
    member_content_row = await AnanBOTPool.insert_sora_content_media(file_unique_id, file_type, file_size, duration, user_id, file_id, bot_username)
    member_content_id = member_content_row["id"]

    # 插入到 collection_items 表
    await AnanBOTPool.insert_collection_item(
        content_id=content_id,
        member_content_id=member_content_id,
        file_unique_id=file_unique_id,
        file_type=type_code  # "v", "d", "p"
    )



############
#  set_price     
############
@dp.callback_query(F.data.startswith("set_price:"))
async def handle_set_price(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    if not content_id:
        return await callback_query.answer("⚠️ 找不到内容 ID", show_alert=True)


    product_info = await AnanBOTPool.get_existing_product(content_id)       
    cur_price = product_info.get('price')
    try:
        cur_price = int(cur_price) if cur_price is not None else 68
    except Exception:
        cur_price = 68

    caption = f"当前价格为 {cur_price}\n\n请在 3 分钟内输入商品价格(1-99)"
    cancel_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="取消", callback_data=f"cancel_set_price:{content_id}")]
    ])

    try:
       
        await callback_query.message.edit_caption(caption=caption, reply_markup=cancel_keyboard)
    except Exception as e:
        print(f"⚠️ 设置价格 edit_caption 失败: {e}", flush=True)

    await state.set_state(ProductPreviewFSM.waiting_for_price_input)
    await state.set_data({
        "content_id": content_id,
        "chat_id": callback_query.message.chat.id,
        "message_id": callback_query.message.message_id,
        "callback_id": callback_query.id   # 👈 保存弹窗 ID
    })

    asyncio.create_task(clear_price_request_after_timeout(state, content_id, callback_query.message.chat.id, callback_query.message.message_id))

async def clear_price_request_after_timeout(state: FSMContext, content_id: str, chat_id: int, message_id: int):
    await asyncio.sleep(INPUT_TIMEOUT)
    current_state = await state.get_state()
    if current_state == ProductPreviewFSM.waiting_for_price_input:
        await state.clear()
        thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text,parse_mode="HTML"),
                reply_markup=preview_keyboard
                
            )
        except Exception as e:
            print(f"⚠️ 超时恢复菜单失败: {e}", flush=True)

@dp.message(F.chat.type == "private", ProductPreviewFSM.waiting_for_price_input, F.text)
async def receive_price_input(message: Message, state: FSMContext):
    
    data = await state.get_data()
    content_id = data.get("content_id")
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    
    price_str = message.text.strip()
    if not price_str.isdigit() or not (34 <= int(price_str) <= 102):
        # await message.answer("❌ 请输入 34~102 的整数作为价格")
        # 回到菜单
        
        callback_id = data.get("callback_id")
        if callback_id:
            await bot.answer_callback_query(callback_query_id=callback_id, text=f"❌ 请输入 34~102 的整数作为价格", show_alert=True)
        else:
            await state.clear()
            thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)

            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text,parse_mode="HTML"),
                reply_markup=preview_keyboard
                
            )

            # await bot.edit_message_media(
            #     chat_id=chat_id,
            #     message_id=message_id,
            #     media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
            #     reply_markup=preview_keyboard,
            #     parse_mode="HTML"
            # )
        await message.delete()  
        return

    price = int(price_str)
    


    await AnanBOTPool.update_product_price(content_id=content_id, price=price)


    # await message.answer(f"✅ 已更新价格为 {price} 积分")

    # 回到菜单

    await state.clear()
    await message.delete()

    # ✅ 重置缓存（删除）
    invalidate_cached_product(content_id)



    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    await bot.edit_message_media(
        chat_id=chat_id,
        message_id=message_id,
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
        reply_markup=preview_keyboard
    )


@dp.callback_query(F.data.startswith("cancel_set_price:"))
async def cancel_set_price(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    await state.clear()
    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text,parse_mode="HTML"),
        reply_markup=preview_keyboard
        
    )


############
#  设置预览图 Thumb     
############

@dp.callback_query(F.data.startswith("set_preview:"))
async def handle_set_preview(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    user_id = str(callback_query.from_user.id)
    
    thumb_file_id = None
    bot_username = await get_bot_username()

    thumb_file_id, thumb_unique_file_id = await AnanBOTPool.get_preview_thumb_file_id(bot_username, content_id)

    if not thumb_file_id:
        # 如果没有缩略图，传送 
        thumb_file_id = DEFAULT_THUMB_FILE_ID


    # 更新原消息内容（图片不变，仅改文字+按钮）
    caption_text = "📸 请在 3 分钟内发送预览图（图片格式）"
    cancel_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🪄 自动更新预览图", callback_data=f"auto_update_thumb:{content_id}")
        ],
        [InlineKeyboardButton(text="取消", callback_data=f"cancel_set_preview:{content_id}")]

    ])

    try:
        await callback_query.message.edit_caption(caption=caption_text, reply_markup=cancel_keyboard)
    except Exception as e:
        print(f"⚠️ edit_caption 失败：{e}", flush=True)

    # 设置 FSM 状态
    await state.set_state(ProductPreviewFSM.waiting_for_preview_photo)
    await state.set_data({
        "content_id": content_id,
        "chat_id": callback_query.message.chat.id,
        "message_id": callback_query.message.message_id,
        "callback_id": callback_query.id   # 👈 保存弹窗 ID
    })

    asyncio.create_task(clear_preview_request_after_timeout(state, user_id, callback_query.message.message_id, callback_query.message.chat.id, content_id))

async def clear_preview_request_after_timeout(state: FSMContext, user_id: str, message_id: int, chat_id: int, content_id):
    await asyncio.sleep(INPUT_TIMEOUT)
    current_state = await state.get_state()
    if current_state == ProductPreviewFSM.waiting_for_preview_photo:
        try:
            await state.clear()
        except Exception as e:
            print(f"⚠️ 清除状态失败：{e}", flush=True)

        try:
            thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=preview_keyboard,
                media=InputMediaPhoto(
                    media=thumb_file_id,
                    caption=preview_text,
                    parse_mode="HTML"
                )
                
            )
        except Exception as e:
            print(f"⚠️ 超时编辑失败：{e}", flush=True)

@dp.callback_query(F.data.startswith("cancel_set_preview:"))
async def cancel_set_preview(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    try:
        await state.clear()
    except Exception as e:
        print(f"⚠️ 清除状态失败：{e}", flush=True)
    print(f"取消设置预览图：{content_id}", flush=True)

    message_id = callback_query.message.message_id
    chat_id = callback_query.message.chat.id
    try:
        thumb_file_id,preview_text,preview_keyboard = await get_product_tpl(content_id)
        await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            
            reply_markup=preview_keyboard,
            media=InputMediaPhoto(
                media=thumb_file_id,
                caption=preview_text,
                parse_mode="HTML"
            )
            
        )
    except Exception as e:
        print(f"⚠️ 超时编辑失败：{e}", flush=True)
        

@dp.message(F.chat.type == "private", F.content_type == ContentType.PHOTO, ProductPreviewFSM.waiting_for_preview_photo)
async def receive_preview_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    content_id = data["content_id"]
    chat_id = data["chat_id"]
    message_id = data["message_id"]

    # print(f"📸 1开始处理预览图：content_id={content_id}, chat_id={chat_id}, message_id={message_id}", flush=True)
    

    photo = get_largest_photo(message.photo)
    file_unique_id = photo.file_unique_id
    file_id = photo.file_id
    width = photo.width
    height = photo.height
    file_size = photo.file_size or 0
    user_id = int(message.from_user.id)
    photo_message = message

    # print(f"📸 2收到预览图：{file_unique_id}", flush=True)

    spawn_once(f"copy:{photo_message.message_id}", lz_var.bot.copy_message(
        chat_id=lz_var.x_man_bot_id,
        from_chat_id=message.chat.id,
        message_id=photo_message.message_id
    ))

    # await lz_var.bot.copy_message(
    #     chat_id=lz_var.x_man_bot_id,
    #     from_chat_id=message.chat.id,
    #     message_id=photo_message.message_id
    # )

    # print(f"📸 3预览图已成功设置：{file_unique_id}", flush=True)
    await AnanBOTPool.upsert_media( "photo", {
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "duration": 0,
        "width": width,
        "height": height,
        "create_time": datetime.now()
    })
    bot_username = await get_bot_username()
    await AnanBOTPool.insert_file_extension("photo", file_unique_id, file_id, bot_username, user_id)
    await AnanBOTPool.insert_sora_content_media(file_unique_id, "photo", file_size, 0, user_id, file_id, bot_username)
    await AnanBOTPool.upsert_product_thumb(content_id, file_unique_id,file_id, bot_username)
    # Step 4: 更新 update_bid_thumbnail

    # print(f"📸 4更新预览图数据库记录：{file_unique_id}", flush=True)
    row = await AnanBOTPool.get_sora_content_by_id(content_id)
    if row and row.get("source_id"):
        source_id = row["source_id"]
        await AnanBOTPool.update_bid_thumbnail(source_id, file_unique_id, file_id, bot_username)




   
    # print(f"📸 6预览图更新中，正在返回菜单：{file_unique_id}",flush=True)
    # 编辑原消息，更新为商品卡片

   
    

    thumb_file_id = file_id
    _, preview_text, preview_keyboard = await get_product_tpl(content_id)
    try:
        # print(f"\r\nTPL: thumb={thumb_file_id[:10]}..., caption_len={len(preview_text)}, kb_type={type(preview_keyboard)}", flush=True)
       
        # print(f"\r\nmessage_id = {message_id} {chat_id}")
        

        edit_result=await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text,parse_mode="HTML"),
            reply_markup=preview_keyboard,     
        )
        # print(f"Edit result: {edit_result}", flush=True)
        print(f"📸 7预览图更新完成，返回菜单中：{file_unique_id}", flush=True)
        await photo_message.delete()
    except Exception as e:
        print(f"⚠️ 8更新预览图失败B：{e}", flush=True)

    # await message.answer("✅ 预览图已成功设置！")
    
    try:
        await state.clear()
    except Exception as e:
        print(f"⚠️ 清除状态失败：{e}", flush=True)
    invalidate_cached_product(content_id)
    print(f"📸 9预览图更新完成，返回菜单中：{file_unique_id}", flush=True)


@dp.callback_query(F.data.startswith("auto_update_thumb:"))
async def handle_auto_update_thumb(callback_query: CallbackQuery, state: FSMContext):
    bot_username = await get_bot_username()
    content_id = int(callback_query.data.split(":")[1])
    print(f"▶️ 开始自动处理预览图", flush=True)
    try:
        # Step 1: 取得 sora_content.source_id
        row = await AnanBOTPool.search_sora_content_by_id(content_id,lz_var.bot_username)
        if not row or not row.get("source_id"):
            return await callback_query.answer("...⚠️ 无法取得 source_id", show_alert=True)

        source_id = row["source_id"]
        print(f"...🔍 取得 source_id: {source_id} for content_id: {content_id}", flush=True)
        bot_username = await get_bot_username()
        
        thumb_file_unique_id = None
        thumb_file_id = None

        # Step 2: 取得 thumb_file_unique_id
        print(f"...🔍 查询缩图信息 for source_id: {source_id}", flush=True)
        thumb_row = await AnanBOTPool.get_bid_thumbnail_by_source_id(source_id)
        print(f"...🔍 取得缩图记录: {thumb_row} for source_id: {source_id}", flush=True)
        
        # 遍寻 thumb_row
        if thumb_row:
            print(f"...🔍 取得缩图信息: {thumb_row} for source_id: {source_id}", flush=True)
            for sub_row in thumb_row:
                thumb_file_unique_id = sub_row["thumb_file_unique_id"]
                print(f"...🔍 取得缩图 unique_id: {thumb_file_unique_id} for source_id: {source_id}", flush=True)
                if sub_row['bot_name'] == bot_username:   
                    thumb_file_id = sub_row["thumb_file_id"]

        if thumb_file_unique_id is None and thumb_file_id is None:
            # print(f"{row.get("file_type")} {row.get("m_file_id")}", flush=True)
            if (row.get("file_type") == 'video' or row.get("file_type") == 'v') and row.get("m_file_id"):
                send_video_result = await lz_var.bot.send_video(chat_id=callback_query.message.chat.id, video=row.get("m_file_id"))
                
                # 记录临时消息 id，便于无论成功/失败都删除
                _tmp_chat_id = send_video_result.chat.id
                _tmp_msg_id = send_video_result.message_id
                
                print(f"送出的视频信息{send_video_result}")
                buf,pic = await Media.extract_preview_photo_buffer(send_video_result, prefer_cover=True, delete_sent=True)
                
                if buf and pic:
                    try:
                        newcover = await callback_query.message.edit_media(
                            media=InputMediaPhoto(
                                media=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"),
                                caption=callback_query.message.caption,
                                caption_entities=callback_query.message.caption_entities
                            ),
                            reply_markup=callback_query.message.reply_markup
                        )
                        largest = newcover.photo[-1]
                        thumb_file_id = largest.file_id
                        thumb_file_unique_id = largest.file_unique_id

                        invalidate_cached_product(content_id)
                        await AnanBOTPool.upsert_product_thumb(
                            content_id, thumb_file_unique_id, thumb_file_id, await get_bot_username()
                        )

                        await callback_query.answer("预览图更新中", show_alert=False)
                    except Exception as e:
                        if(str(e).find("message is not modified")>=0):
                            await callback_query.answer("⚠️ 这就是这个资源的默认预览图（无修改）", show_alert=True)
                        else:
                            print(f"⚠️ 用缓冲图更新封面失败：{e}", flush=True)
                            await callback_query.answer("⚠️ 用缓冲图更新封面失败，需要手动上传或是机器人排程生成", show_alert=True)
                
                else:
                    print(f"...⚠️ 提取缩图失败 for source_id: {source_id}", flush=True)
                    await callback_query.answer("⚠️ 目前还没有这个资源的缩略图，也没预设的预览图，需要手动上传或是机器人排程生成", show_alert=True)
                
                # 3) 不论成败，尽力删除临时视频（如果之前已删，会静默忽略异常）
                try:
                    await lz_var.bot.delete_message(chat_id=_tmp_chat_id, message_id=_tmp_msg_id)
                    
                except Exception as _e_del:
                    print(f"ℹ️ 临时视频可能已被删除: {_e_del}", flush=True)
                
                
                return
            else:
                print(f"...⚠️ 找不到对应的分镜缩图 for source_id: {source_id}", flush=True)
                await callback_query.answer("⚠️ 目前还没有这个资源的缩略图，需要手动上传或是机器人排程生成", show_alert=True)
                return

        elif thumb_file_unique_id and thumb_file_id is None:
        # Step 4: 通知处理 bot 生成缩图（或触发缓存）
            storage = state.storage  # 与全局 Dispatcher 共享的同一个 storage

            x_uid = lz_var.x_man_bot_id          # = 7793315433
            x_chat_id = x_uid                     # 私聊里 chat_id == user_id
            me = await bot.get_me()
            key = StorageKey(bot_id=me.id, chat_id=x_chat_id, user_id=x_uid)


            await storage.set_state(key, ProductPreviewFSM.waiting_for_x_media.state)
            await storage.set_data(key, {})  # 清空

            await bot.send_message(chat_id=lz_var.x_man_bot_id, text=f"{thumb_file_unique_id}")
            # await callback_query.answer("...已通知其他机器人更新，请稍后自动刷新", show_alert=True)
            timeout_sec = 10
            max_loop = int((timeout_sec / 0.5) + 0.5)
            for _ in range(max_loop):
                data = await storage.get_data(key)
                x_file_id = data.get("x_file_id")
                if x_file_id:
                    thumb_file_id = x_file_id
                    # 清掉对方上下文的等待态
                    await storage.set_state(key, None)
                    await storage.set_data(key, {})
                    print(f"  ✅ [X-MEDIA] 收到 file_id={thumb_file_id}", flush=True)
                    break

                await asyncio.sleep(0.5)


        if thumb_file_unique_id and thumb_file_id:
            
            try:
                # thumb_file_unique_id = thumb_row["thumb_file_unique_id"]
                # thumb_file_id = thumb_row["thumb_file_id"]
                print(f"...🔍 取得分镜图信息: {thumb_file_unique_id}, {thumb_file_id} for source_id: {source_id}", flush=True)

                # Step 3: 更新 sora_content 缩图字段 (也重置萨莱)
                await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unique_id,thumb_file_id, bot_username)
                await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unique_id,None, 'salai001bot')

                # Step 4: 更新 update_bid_thumbnail
                await AnanBOTPool.update_bid_thumbnail(source_id, thumb_file_unique_id, thumb_file_id, bot_username)

                cache = get_cached_product(content_id)
                if cache is None:
                    # 强制重建缓存
                    fresh_thumb, fresh_text, fresh_kb = await get_product_tpl(content_id)
                    caption = fresh_text
                    kb = fresh_kb
                else:
                    cache["thumb_unique_id"] = thumb_file_unique_id
                    cache["thumb_file_id"] = thumb_file_id
                    set_cached_product(content_id, cache)
                    caption = cache["preview_text"]
                    kb = cache["preview_keyboard"]

                print(f"...✅ 更新 content_id: {content_id} 的缩图为 {thumb_file_unique_id}", flush=True)

                await callback_query.message.edit_media(
                    media=InputMediaPhoto(media=thumb_file_id, caption=caption, parse_mode="HTML"),
                    reply_markup=kb
                )
                await callback_query.answer("✅ 已自动更新预览图", show_alert=True)
            except Exception as e:
                print(f"...⚠️ 更新预览图失败A: {e}", flush=True)
        else:
            print(f"...⚠️ 找不到对应的缩图2 for source_id: {source_id} {thumb_file_unique_id} {thumb_file_id}", flush=True)
            return await callback_query.answer("⚠️ 找不到对应的缩图", show_alert=True)

    except Exception as e:
        logging.exception(f"⚠️ 自动更新预览图失败: {e}")
        await callback_query.answer("⚠️ 自动更新失败", show_alert=True)


############
#  投稿     
############
@dp.callback_query(F.data.startswith("submit_product:"))
async def handle_submit_product(callback_query: CallbackQuery, state: FSMContext):
    try:
        content_id = int(callback_query.data.split(":")[1])
    except Exception:
        return await callback_query.answer("⚠️ 提交失败：content_id 异常", show_alert=True)

    # 取当前商品信息（含预览图、内容等）
    product_row = await get_product_info(content_id)
    product_info = product_row.get("product_info", {}) or {}
    thumb_file_id = product_row.get("thumb_file_id") or ""
    content_text = (product_info.get("content") or "").strip()
    tag_string = product_info.get("tag", "")


    has_tag_string = bool(tag_string and tag_string.strip())

    # 预览图校验：必须不是默认图
    has_custom_thumb = bool(thumb_file_id and thumb_file_id != DEFAULT_THUMB_FILE_ID)

    # 标签数量校验：基于 file_unique_id 去取绑定的标签
    try:
        sora_row = await AnanBOTPool.get_sora_content_by_id(content_id)
        source_id = sora_row.get("source_id") if sora_row else None
        tag_set = await AnanBOTPool.get_tags_for_file(source_id) if source_id else set()
        tag_count = len(tag_set or [])
    except Exception:
        tag_set = set()
        tag_count = 0

    # 内容长度校验（“超过30字”→ 严格 > 30）
    content_ok = len(content_text) > 30
    tags_ok = tag_count >= 5
    thumb_ok = has_custom_thumb
    has_tag_ok = has_tag_string

    # 如果有缺项，给出可操作的引导并阻止送审
    if not (content_ok and tags_ok and thumb_ok and has_tag_ok):
        missing_parts = []
        if not content_ok:
            missing_parts.append("📝 内容需 > 30 字")
        if not thumb_ok:
            missing_parts.append("📷 需要设置预览图（不是默认图）")

        if not has_tag_ok:
            missing_parts.append(f"🏷️ 请检查标签是否正确")
        elif not tags_ok :
            missing_parts.append(f"🏷️ 标签需 ≥ 5 个（当前 {tag_count} 个）")
        

        tips = "⚠️ 送审前需补全：\n• " + "\n• ".join(missing_parts)

        return await callback_query.answer(tips, show_alert=True)

    # tips = f"tag_count={tag_count}, len = {len(content_text)},has_custom_thumb={has_custom_thumb}"

    # return await callback_query.answer(tips, show_alert=True)

    # return

    # # 和原来的内容合并,  改到 send_to_review_group 一起做
    # spawn_once(f"refine:{content_id}", AnanBOTPool.refine_product_content(content_id))

    

    # 1) 更新 bid_status=1
    try:
        affected = await AnanBOTPool.set_product_review_status(content_id, 2)
        if affected == 0:
            return await callback_query.answer("⚠️ 未找到对应商品，提交失败", show_alert=True)
    except Exception as e:
        logging.exception(f"提交送审失败: {e}")
        return await callback_query.answer("⚠️ 提交失败，请稍后重试", show_alert=True)

    # 2) 隐藏按钮并显示“已送审请耐心等候”
    try:
        # 清理缓存，确保后续重新渲染
        invalidate_cached_product(content_id)
    except Exception:
        pass

    thumb_file_id, preview_text, _ = await get_product_tpl(content_id)
    submitted_caption = f"{preview_text}\n\n📮 <b>已送审，请耐心等候</b>"

    try:
        await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=submitted_caption, parse_mode="HTML"),
            reply_markup=None  # 👈 关键：隐藏所有按钮
        )
    except Exception as e:
        logging.exception(f"编辑媒体失败: {e}")
        # 兜底：至少把按钮清掉
        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

    result , error = await send_to_review_group(content_id, state)
    if result:
        await callback_query.answer("✅ 已提交审核", show_alert=False)
    else:
        if error:
            await callback_query.answer(f"⚠️ 发送失败：{error}", show_alert=True)
        else:
            await callback_query.answer("⚠️ 发送失败：未知错误", show_alert=True)



@dp.callback_query(F.data.startswith("cancel_publish:"))
async def handle_cancel_publish(callback_query: CallbackQuery, state: FSMContext):
    try:
        content_id = int(callback_query.data.split(":")[1])
    except Exception:
        return await callback_query.answer("⚠️ 操作失败：content_id 异常", show_alert=True)

    # （可选）如果你想把 bid_status 复位，可解开下面一行
    # await AnanBOTPool.set_product_bid_status(content_id, 0)

    # 清缓存，确保重新渲染
    try:
        invalidate_cached_product(content_id)
    except Exception:
        pass

    # 重新取卡片内容并追加“已取消投稿”
    thumb_file_id, preview_text, _ = await get_product_tpl(content_id)
    cancelled_caption = f"{preview_text}\n\n⛔ <b>已取消投稿</b>"

    try:
        await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=cancelled_caption, parse_mode="HTML"),
            reply_markup=None  # 👈 清空所有按钮
        )
    except Exception as e:
        logging.exception(f"编辑媒体失败: {e}")
        # 兜底：至少把按钮清掉
        try:
            await callback_query.message.edit_caption(caption=cancelled_caption, parse_mode="HTML")
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

    await callback_query.answer("已取消投稿", show_alert=False)


from typing import Optional, Tuple

def find_return_review_url(markup: InlineKeyboardMarkup) -> Optional[str]:
    """在 InlineKeyboardMarkup 里寻找『🔙 返回审核』按钮，找到则返回其 URL"""
    if not markup or not getattr(markup, "inline_keyboard", None):
        return None
    for row in markup.inline_keyboard:
        for btn in row:
            if btn.text == "🔙 返回审核" and btn.url:
                return btn.url
    return None


from urllib.parse import urlparse

def parse_tme_c_url(url: str) -> Optional[Tuple[int, Optional[int], int]]:
    """
    支持两种形式：
      1) https://t.me/c/<internal>/<msgId>
      2) https://t.me/c/<internal>/<threadId>/<msgId>

    返回:
      (chat_id, message_thread_id, message_id)
      其中 chat_id = int(f"-100{internal}")
    """
    try:
        parsed = urlparse(url)
        # path like: /c/2989536306/2/5  or /c/2989536306/12345
        parts = [p for p in parsed.path.split("/") if p]  # 去掉空字符串
        # 期望 parts = ["c", "<internal>", "<msgId>"] 或 ["c", "<internal>", "<threadId>", "<msgId>"]
        if len(parts) < 3 or parts[0] != "c":
            return None

        internal = parts[1]
        if len(parts) == 3:
            # 无话题
            msg_id = int(parts[2])
            thread_id = None
        elif len(parts) == 4:
            # 有话题
            thread_id = int(parts[2])
            msg_id = int(parts[3])
        else:
            return None

        if not internal.isdigit():
            return None

        chat_id = int(f"-100{internal}")
        return chat_id, thread_id, msg_id
    except Exception:
        return None


@dp.callback_query(F.data.startswith("approve_product:"))
async def handle_approve_product(callback_query: CallbackQuery, state: FSMContext):
    judge_string = ''
    try:
        print(f"callback_query={callback_query.data=}", flush=True)
        content_id = int(callback_query.data.split(":")[1])
        print(f"content_id={content_id=}", flush=True)
        if callback_query.data.split(":")[2] in ("'Y'", "'N'"):
            judge_string = callback_query.data.split(":")[2]
            review_status = 6 
        else:
            review_status = int(callback_query.data.split(":")[2])
    except Exception as e:
        logging.exception(f"解析回调数据失败: {e}")
        return await callback_query.answer("⚠️ 提交失败：content_id 异常", show_alert=True)

   
    
    reviewer = callback_query.from_user.username or callback_query.from_user.full_name


    # === 先尝试从当前卡片上找到『🔙 返回审核』的 URL，并解析出 chat/thread/message ===
    ret_chat = ret_thread = ret_msg = None
    try:
        ret_url = find_return_review_url(callback_query.message.reply_markup)
        print(f"🔍 返回审核 URL: {ret_url}", flush=True)
        if ret_url:
            parsed = parse_tme_c_url(ret_url)
            if parsed:
                ret_chat, ret_thread, ret_msg = parsed
    except Exception as e:
        logging.exception(f"解析返回审核 URL 失败: {e}")
    print(f"🔍 返回审核定位: chat={ret_chat} thread={ret_thread} msg={ret_msg}", flush=True)

    product_row = await get_product_info(content_id)
    product_info = product_row.get("product_info") or {}
    print(f"🔍 product_info = {product_info}", flush=True)
    check_result,check_error_message =  await _check_product_policy(product_row)
    if check_result is not True:
        return await callback_query.answer(check_error_message, show_alert=True)
    
    # 1) 更新 bid_status=1
    try:
        if review_status == 2:
            review_status = 1

        affected = await AnanBOTPool.set_product_review_status(content_id, review_status)
        if affected == 0:
            return await callback_query.answer("⚠️ 未找到对应商品，审核失败", show_alert=True)
        
        # if review_status == 2:
        #     affected2 = await AnanBOTPool.set_product_review_status(content_id, 1)
        #     print(f"🔍 审核拒绝，重置 bid_status =1 : {affected2}", flush=True)
    except Exception as e:
        logging.exception(f"审核失败: {e}")
        return await callback_query.answer("⚠️ 审核失败，请稍后重试", show_alert=True)

    # 2) 隐藏按钮并显示“已送审请耐心等候”
    try:
        # 清理缓存，确保后续重新渲染
        invalidate_cached_product(content_id)
    except Exception:
        pass

    '''
    审核状态
    0   编辑中(投稿者)
    1   未通过审核(投稿者)
    2   初审进行中 (审核员)
    3   通过初审,复审进行中 (审椄员)
    4   经检举,初审进行中 (审核员)
    6   通过终核,上架进行中
    7   上架失败
    9   成功上架 
    '''

    button_str = ""

    if review_status == 6:
        await callback_query.answer(f"✅ 已审核{judge_string}，审核人 +3 活跃值", show_alert=True)
        

        if judge_string == "'N'":
            button_str = f"❌ {reviewer} 不认可举报"
        elif judge_string == "'Y'":
            button_str = f"✅ {reviewer} 认可举报"
        else:
            button_str = f"✅ {reviewer} 已审核{judge_string}"
   

       
        

      

    elif review_status == 3:
        await callback_query.answer("✅ 已通过审核，审核人 +3 活跃值", show_alert=True)
        button_str = f"✅ {reviewer} 已通过审核"
        

    elif review_status == 1:
        button_str = f"❌ {reviewer} 已拒绝审核"
        await callback_query.answer("❌ 已拒绝审核，审核人 +3 活跃值", show_alert=True)
        



    if review_status == 6:
        spawn_once(f"_send_to_topic:{content_id}", _send_to_topic(content_id))
        # ⬇️ 改为后台执行，不阻塞当前回调
        spawn_once(f"refine:{content_id}", AnanBOTPool.refine_product_content(content_id))
        # print(f"🔍 审核通过，准备发送到发布频道: content_id={content_id}", flush=True)

        # await _send_to_topic(content_id)
    # await _reset_review_bot_button(callback_query,content_id,button_str)
    
    spawn_once(f"_reset_review_bot_button:{content_id}",_reset_review_bot_button(callback_query,content_id,button_str) )
    spawn_once(f"update_today_contribute:{content_id}", AnanBOTPool.update_today_contribute(callback_query.from_user.id, 3))
     # 处理审核区的按钮  
    # await _reset_review_zone_button(button_str,ret_chat,ret_msg) 
    spawn_once(f"_reset_review_zone_button:{content_id}", _reset_review_zone_button(button_str,ret_chat,ret_msg) )

    spawn_once(f"_review_next_product:{content_id}",_review_next_product(state) )

# 后台处理下一个待审核的
async def _review_next_product(state: Optional[FSMContext] = None):
    ids = await AnanBOTPool.fetch_review_status_content_ids(2,1)
    if not ids:
       return
    for content_id in ids:
        try:
            result, error = await send_to_review_group(int(content_id), state)
        except Exception as e:
            result, error = False, str(e)
        await asyncio.sleep(1)




async def _reset_review_bot_button(callback_query: CallbackQuery,content_id:int,button_str:str):  
    buttons = [[InlineKeyboardButton(text=button_str, callback_data=f"none")]]

    message = callback_query.message

    # 图片 file_id
    if message.photo:
        thumb_file_id = message.photo[-1].file_id
    else:
        thumb_file_id = None

    # 文本（caption 或 text）
    preview_text = message.caption or message.text or ""

    # thumb_file_id, preview_text, _ = await get_product_tpl(content_id)
    preview_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    try:
        # 处理当下的按钮
        ret = await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard  
        )
    except Exception as e:
        logging.exception(f"编辑媒体失败: {e}")
        # 兜底：至少把按钮清掉
        try:
            await callback_query.message.edit_reply_markup(reply_markup=preview_keyboard)
        except Exception:
            pass

async def _reset_review_zone_button(button_str,ret_chat,ret_msg):
    # # === 构造『审核结果』只读按钮，并把它写回到原审核消息（由 🔙 返回审核 指向） ===
    try:
       
        result_kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=f"{button_str}", callback_data="a=nothing")]]
        )

        # 只有当刚才解析到了返回审核的定位信息，才去编辑那条消息
        if ret_chat is not None and ret_msg is not None:
            # 注意：编辑 reply_markup 不需要 thread_id；thread_id 仅发送消息时常用
            await bot.edit_message_reply_markup(
                chat_id=ret_chat,
                message_id=ret_msg,
                reply_markup=result_kb
            )
            
            print(f"🔍 已更新原审核消息按钮: chat={ret_chat} msg={ret_msg} btn={button_str}", flush=True)

    except Exception as e:
        logging.exception(f"更新原审核消息按钮失败: {e}")

async def _send_to_topic(content_id:int):
    global publish_bot
    guild_id = await AnanBOTPool.set_product_guild(content_id) 
    print(f"send to guild_id={guild_id}")
    if guild_id is not None and guild_id > 0:       
        
        me = await publish_bot.get_me()
        publish_bot_username = me.username
        try:
            tpl_data = await AnanBOTPool.search_sora_content_by_id(int(content_id),publish_bot_username)
            review_status = await submit_resource_to_chat_action(content_id,publish_bot,tpl_data)
            if review_status is not None:
                await AnanBOTPool.set_product_review_status(content_id, review_status)
        except Exception as e:
            logging.exception(f"发送到发布频道失败: {e}")
        
    return
    

############
#  content     
############

@dp.callback_query(F.data.startswith("set_content:"))
async def handle_set_content(callback_query: CallbackQuery, state: FSMContext):
    parts = callback_query.data.split(":")
    # 兼容两种格式：set_content:{content_id}  /  set_content:{content_id}:{overwrite}
    try:
        content_id = parts[1]
        overwrite = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    except Exception:
        return await callback_query.answer("⚠️ 参数错误", show_alert=True)

    # product_info = await AnanBOTPool.get_existing_product(content_id)
    product_info = await AnanBOTPool.search_sora_content_by_id(content_id, lz_var.bot_username)
    
    print(f"🔍 取商品信息: {product_info}", flush=True)
    caption = f"<code>{product_info.get('content','')}</code>  (点选复制) \r\n\r\n📘 请输入完整的内容介绍（文本形式）"
    cancel_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="取消，不修改", callback_data=f"cancel_set_content:{content_id}")]
    ])

    try:
        await callback_query.message.edit_caption(caption=caption, reply_markup=cancel_keyboard, parse_mode="HTML")
    except Exception as e:
        print(f"⚠️ 设置内容 edit_caption 失败: {e}", flush=True)

    await state.set_state(ProductPreviewFSM.waiting_for_content_input)
    await state.set_data({
        "content_id": content_id,
        "chat_id": callback_query.message.chat.id,
        "message_id": callback_query.message.message_id,
        "overwrite": overwrite,  # 存为 int
    })

    asyncio.create_task(clear_content_input_timeout(state, content_id, callback_query.message.chat.id, callback_query.message.message_id))


async def clear_content_input_timeout(state: FSMContext, content_id: str, chat_id: int, message_id: int):
    await asyncio.sleep(INPUT_TIMEOUT)
    if await state.get_state() == ProductPreviewFSM.waiting_for_content_input:
        await state.clear()
        thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
                reply_markup=preview_keyboard
                
            )
        except Exception as e:
            print(f"⚠️ 设置内容超时恢复失败: {e}", flush=True)



@dp.message(F.chat.type == "private", ProductPreviewFSM.waiting_for_content_input, F.text)
async def receive_content_input(message: Message, state: FSMContext):
    timer = SegTimer("receive_content_input", content_id="unknown")
    try:
        content_text = message.text.strip()
        data = await state.get_data()
        content_id = data["content_id"]
        chat_id = data["chat_id"]
        message_id = data["message_id"]
        overwrite = int(data.get("overwrite", 0))
        user_id = message.from_user.id
        timer.ctx["content_id"] = content_id

        timer.lap("state.get_data")

        # 1) DB 更新（高概率慢点）
        await AnanBOTPool.update_product_content(content_id, content_text, user_id, overwrite)
        timer.lap("update_product_content")

        # 2) 清理消息（网络调用）
        try:
            await message.delete()
        except Exception:
            pass
        timer.lap("message.delete")

        # 3) 清状态（一般很快）
        await state.clear()
        timer.lap("state.clear")

        # 4) 缓存失效（看实现，可能快）
        invalidate_cached_product(content_id)
        timer.lap("invalidate_cached_product")

        # 5) 取模板（通常含 DB/IO）
        thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
        timer.lap("get_product_tpl")

        # 6) 编辑媒体（网络调用，常见瓶颈）
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
                reply_markup=preview_keyboard
            )
        except Exception as e:
            print(f"⚠️ 更新内容失败：{e}", flush=True)
        timer.lap("edit_message_media")


        # ✅ 7) 弹出字数
        length = len(content_text)
        await message.answer(f"📏 内容字数：{length}")

    finally:
        timer.end()


@dp.callback_query(F.data.startswith("cancel_set_content:"))
async def cancel_set_content(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    await state.clear()
    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
        reply_markup=preview_keyboard
    )


############
#  发表模式    
############

@dp.callback_query(F.data.startswith("toggle_anonymous:"))
async def handle_toggle_anonymous(callback_query: CallbackQuery, state: FSMContext):
    """显示匿名/公开/取消设定的选择页，并启动 60 秒超时"""
    try:
        content_id = int(callback_query.data.split(":")[1])
    except Exception:
        return await callback_query.answer("⚠️ 操作失败：content_id 异常", show_alert=True)

    # 取当前匿名状态，用于在按钮前显示 ☑️
    product_row = await AnanBOTPool.get_existing_product(content_id)
    print(f"🔍 {product_row}", flush=True)
    current_mode = int(product_row.get("anonymous_mode", 1)) if product_row else 1

    def with_check(name: str, hit: bool) -> str:
        return f"☑️ {name}" if hit else name

    btn1 = InlineKeyboardButton(
        text=with_check("🙈 匿名发表", current_mode == 1),
        callback_data=f"anon_mode:{content_id}:1"
    )
    btn2 = InlineKeyboardButton(
        text=with_check("🐵 公开发表", current_mode == 3),
        callback_data=f"anon_mode:{content_id}:3"
    )
    btn3 = InlineKeyboardButton(
        text="取消设定",
        callback_data=f"anon_cancel:{content_id}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn1],
        [btn2],
        [btn3]
    ])

    # 替换成选择说明
    desc = (
        "请选择你的发表模式:\n\n"
        "🙈 匿名发表 : 这个作品将不会显示上传者\n"
        "🐵 公开发表 : 这个作品将显示上传者"
    )

    try:
        await callback_query.message.edit_caption(caption=desc, reply_markup=kb)
    except Exception as e:
        print(f"⚠️ toggle_anonymous edit_caption 失败: {e}", flush=True)

    # 进入等待选择状态
    await state.set_state(ProductPreviewFSM.waiting_for_anonymous_choice)
    await state.set_data({
        "content_id": content_id,
        "chat_id": callback_query.message.chat.id,
        "message_id": callback_query.message.message_id
    })

    # 如果已有超时任务，先取消
    key = (callback_query.from_user.id, content_id)
    old_task = anonymous_choice_tasks.get(key)
    if old_task and not old_task.done():
        old_task.cancel()

    # 启动 60 秒超时任务
    async def timeout_back():
        try:
            await asyncio.sleep(INPUT_TIMEOUT)
            if await state.get_state() == ProductPreviewFSM.waiting_for_anonymous_choice:
                await state.clear()
                # 返回商品页
                thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
                try:
                    await bot.edit_message_media(
                        chat_id=callback_query.message.chat.id,
                        message_id=callback_query.message.message_id,
                        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
                        reply_markup=preview_keyboard
                    )
                except Exception as e:
                    print(f"⚠️ 匿名选择超时返回失败: {e}", flush=True)
        except asyncio.CancelledError:
            pass

    anonymous_choice_tasks[key] = asyncio.create_task(timeout_back())
    await callback_query.answer()  # 轻提示即可


@dp.callback_query(F.data.startswith("anon_mode:"))
async def handle_choose_anonymous_mode(callback_query: CallbackQuery, state: FSMContext):
    """用户点选 匿名/公开，更新 DB 并返回商品页"""
    try:
        _, content_id_s, mode_s = callback_query.data.split(":")
        content_id = int(content_id_s)
        mode = int(mode_s)
        if mode not in (1, 3):
            raise ValueError
    except Exception:
        return await callback_query.answer("⚠️ 选择无效", show_alert=True)

    # 更新数据库匿名模式；你需要在 AnanBOTPool 中实现该方法
    #   async def update_product_anonymous_mode(content_id: int, mode: int) -> int:  # 返回受影响行数
    affected = await AnanBOTPool.update_product_anonymous_mode(content_id, mode)
    if affected == 0:
        return await callback_query.answer("⚠️ 未找到对应商品", show_alert=True)

    # 清理任务与状态
    try:
        await state.clear()
    except Exception:
        pass
    key = (callback_query.from_user.id, content_id)
    task = anonymous_choice_tasks.pop(key, None)
    if task and not task.done():
        task.cancel()

    # 失效缓存，返回商品页
    invalidate_cached_product(content_id)

    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    try:
        await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard
        )
    except Exception as e:
        print(f"⚠️ 匿名选择返回卡片失败: {e}", flush=True)

    await callback_query.answer("✅ 已更新发表模式", show_alert=False)


@dp.callback_query(F.data.startswith("anon_cancel:"))
async def handle_cancel_anonymous_choice(callback_query: CallbackQuery, state: FSMContext):
    """用户点选取消设定，直接返回商品页"""
    try:
        content_id = int(callback_query.data.split(":")[1])
    except Exception:
        return await callback_query.answer("⚠️ 操作失败", show_alert=True)

    try:
        await state.clear()
    except Exception:
        pass
    key = (callback_query.from_user.id, content_id)
    task = anonymous_choice_tasks.pop(key, None)
    if task and not task.done():
        task.cancel()

    # 返回商品页（不改任何值）
    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    try:
        await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard
        )
    except Exception as e:
        print(f"⚠️ 取消设定返回卡片失败: {e}", flush=True)

    await callback_query.answer("已取消设定", show_alert=False)



# —— /review 指令 —— 
@dp.message(F.chat.type == "private", F.text.startswith("/review"))
async def handle_review_command(message: Message):
    """
    用法: /review [content_id]
    行为: 回覆 content_id 本身
    """
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("❌ 使用格式: /review [content_id]")
    
    content_id = parts[1]
    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    
    newsend = await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
    # await message.answer(content_id)


@dp.message(Command("start"))
async def handle_search(message: Message, state: FSMContext):
    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        return  # 或者给出引导文案后 return

    if len(args) > 1:
        param = args[1].strip()
        parts = param.split("_")   
        if not parts:  # 空串情况
            return await message.answer("❌ 无效的参数")

    if parts[0] == "f" or parts[0] == "fix":
        try:
            aes = AESCrypto(AES_KEY)
            kind_index = parts[1]
            if(kind_index != 'r'):
                return await message.answer("❌ 无效的参数")
            
            encoded = "_".join(parts[2:])  # 剩下的部分重新用 _ 拼接
            content_id_str = aes.aes_decode(encoded)
            decode_row = content_id_str.split("|")
            
            content_id = int(decode_row[1])
            # print(f"解码内容: {content_id}", flush=True)
            await fix_suggest_content(message,content_id,state)            
        except Exception as e:
            print(f"⚠️ 解码失败: {e}", flush=True)
            pass
    elif parts[0] == "s" or parts[0] == "suggest":
        try:
            await report_content(message.from_user.id, parts[1], state)
          
        except Exception as e:
            print(f"⚠️ 解码失败: {e}", flush=True)
    elif parts[0] == "a" or parts[0] == "admin":
        try:
            await report_content(message.from_user.id, parts[1], state, "admin")
          
        except Exception as e:
            print(f"⚠️ 解码失败: {e}", flush=True)            
    elif parts[0] == "p":
        try:
            aes = AESCrypto(AES_KEY)
            kind_index = parts[1]
            if(kind_index != '9'):
                
                print(f"⚠️ 无效的参数: {kind_index}", flush=True)
                return await message.answer("❌ 无效的参数")
            
            encoded = "_".join(parts[2:])  # 剩下的部分重新用 _ 拼接
            source_id_str = aes.aes_decode(encoded)
            decode_row = source_id_str.split("|")

            if(decode_row[0]!='p'):
                return await message.answer("❌ 无效的参数2")

            content_id = int(decode_row[1])
            
            row = await AnanBOTPool.get_sora_content_by_id(content_id)
            if row and row.get("source_id"):
                await report_content(message.from_user.id, row["source_id"])
        except Exception as e:
            print(f"⚠️ 解码失败: {e}", flush=True)
            pass
   


from aiogram.filters import CommandObject


############
#  审核功能  
############



@dp.message(Command("next"))
async def handle_get_next_report_to_judge(message: Message, state: FSMContext):
    report_row = await AnanBOTPool.get_next_report_to_judge()
    print(f"下一个待裁定 {report_row}", flush=True)
    next_file_unique_id = report_row['file_unique_id'] if report_row else None
    report_id = report_row['report_id']
    print(f"file_unique_id {next_file_unique_id}", flush=True)
    if next_file_unique_id:
        next_content_id = await AnanBOTPool.get_content_id_by_file_unique_id(next_file_unique_id)
        if next_content_id:
            result, error = await send_to_review_group(next_content_id, state)
            await AnanBOTPool.set_product_review_status(next_content_id, 4)  # 更新为经检举,初审进行中
            await AnanBOTPool.update_report_status(report_id, "published")
        else:
            await message.answer(f"❌ 未找到对应的 content_id: {next_file_unique_id}")  
            

# ====== ③ 指令处理器：/postreview 依序发送，每个间隔 15 秒 ======
@dp.message(Command("postreview"))
async def cmd_postreview(message: Message, command: CommandObject, state: FSMContext):

    bot_username = await get_bot_username()  # 👈 增加这一行
    args = (command.args or "").strip().split()
    if len(args) != 1 or not args[0].isdigit():
        ids = await AnanBOTPool.fetch_review_status_content_ids(2,5)
        if not ids:
            await message.answer("目前没有待复审的商品（review_status = 2）。")
            return

        success, failed = 0, 0
        await message.answer(f"开始批量发送到审核群组，共 {len(ids)} 个内容。每个间隔 15 秒。")

        for content_id in ids:
            try:
                result, error = await send_to_review_group(int(content_id), state)
                
            except Exception as e:
                result, error = False, str(e)

            if result:
                success += 1
                await message.answer("✅ 已发送到审核群组")
            else:
                failed += 1
                if error:
                    await message.answer(f"⚠️ 发送失败：{error}")
                else:
                    await message.answer("⚠️ 发送失败：未知错误")

            # 间隔 15 秒
            await asyncio.sleep(15)

        await message.answer(f"完成：成功 {success}，失败 {failed}，总计 {len(ids)}。")
    else:
        content_id = int(args[0])
        result , error = await send_to_review_group(content_id, state)
        if result:
            await message.answer("✅ 已发送到审核群组")
        else:
            if error:
                await message.answer(f"⚠️ 发送失败：{error}")
            else:
                await message.answer("⚠️ 发送失败：未知错误")





@dp.message(F.chat.type == "private", Command("post"))
async def cmd_post(message: Message, command: CommandObject, state: FSMContext):
    """
    用法: /post [content_id]
    行为: 去到指定群组(含话题ID)贴一则“请审核”文字并附带按钮
    """
    # 解析参数
    bot_username = await get_bot_username()  # 👈 增加这一行
    args = (command.args or "").strip().split()
    if len(args) != 1 or not args[0].isdigit():
        return await message.answer("❌ 使用格式: /post [content_id]")
    content_id = int(args[0])
    await _send_to_topic(content_id)
    


async def send_to_review_group(content_id: int, state: FSMContext):
    product_row = await get_product_info(content_id)
    preview_text = product_row.get("preview_text") or ""
    bot_url = f"https://t.me/{(await get_bot_username())}"
    product_info = product_row.get("product_info") or {}
    file_id = product_info.get("m_file_id") or ""
    thumb_file_id = product_info.get("m_thumb_file_id") or ""
    source_id = product_info.get("source_id") or ""
    thumb_file_unqiue_id = product_info.get("thumb_file_unique_id") or ""

    if not thumb_file_unqiue_id and thumb_file_id:
        print(f"背景搬运缩略图 {source_id} for content_id: {content_id}", flush=True)
        # 不阻塞：丢到后台做补拉
        spawn_once(f"thumb_file_unqiue_id:{thumb_file_unqiue_id}", Media.fetch_file_by_file_id_from_x(state, thumb_file_unqiue_id, 10))
    
    if not file_id and source_id and thumb_file_id:
        print(f"背景搬运 {source_id} for content_id: {content_id}", flush=True)
        # 不阻塞：丢到后台做补拉
        spawn_once(f"src:{source_id}", Media.fetch_file_by_file_id_from_x(state, source_id, 10))

        print(f"创建或更新sora_media {thumb_file_unqiue_id} for content_id: {content_id}", flush=True)
        await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unqiue_id, thumb_file_id, bot_username)

    # 发送到指定群组/话题
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 审核", callback_data=f"review:{content_id}")],
        [InlineKeyboardButton(text="🤖 机器人", url=f"{bot_url}")]
    ])

    # 合并更新 product content
    spawn_once(f"refine:{content_id}", AnanBOTPool.refine_product_content(content_id))

    # [InlineKeyboardButton(text="🆖 回报同步失败", callback_data=f"reportfail:{content_id}")]

    try:
        await bot.send_message(
            chat_id=REVIEW_CHAT_ID,
            text=preview_text,
            reply_markup=kb,
            message_thread_id=REVIEW_THREAD_ID,  # 指定话题
            parse_mode="HTML"
        )
        invalidate_cached_product(content_id)
        
        return True, None
        
    except Exception as e:
        return False,e
        

async def _rename_review_button_to_in_progress(callback_query: CallbackQuery, content_id: int) -> None:
    """
    将当前消息里的 '🔎 审核' 按钮，改成 '{username} 🔎 审核中'
    - 仅改这条被点击的消息
    - 仅改 callback_data == f"review:{content_id}" 的按钮
    - 其它按钮（例如 🔙 返回审核）保持不变
    """
    msg = callback_query.message
    if not msg or not msg.reply_markup:
        return

    # 取展示用名字：优先 username，加 @；否则用 full_name
    u = callback_query.from_user
    if u.username:
        reviewer = f"@{u.username}"
    else:
        reviewer = (u.full_name or str(u.id)).strip()

    # Telegram 按钮文字最大 64 字，做个保险截断
    new_text_prefix = f"{reviewer} 🔎 审核中"
    new_text = new_text_prefix[:64]

    kb = msg.reply_markup
    changed = False
    new_rows: list[list[InlineKeyboardButton]] = []



    


    for row in kb.inline_keyboard:
        new_row: list[InlineKeyboardButton] = []
        for btn in row:
            # 只处理 callback 按钮，且 callback_data 精确匹配 review:{content_id}
            if getattr(btn, "callback_data", None) == f"review:{content_id}":
                # 避免重复改名（已是“审核中”就不再改）
                if btn.text != new_text:
                    new_row.append(
                        InlineKeyboardButton(text=new_text, callback_data=btn.callback_data)
                    )
                    changed = True
                else:
                    new_row.append(btn)
            elif getattr(btn, "callback_data", None) == f"reportfail:{content_id}":
                pass
            else:
                new_row.append(btn)
        new_rows.append(new_row)


    if changed:
        try:
            await callback_query.bot.edit_message_reply_markup(
                chat_id=msg.chat.id,
                message_id=msg.message_id,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=new_rows),
            )
        except Exception as e:
            # 静默失败即可，不影响后续流程
            print(f"⚠️ 更新审核按钮文字失败: {e}", flush=True)



@dp.callback_query(F.data.startswith("reportfail:"))
async def handle_reportfail_button(callback_query: CallbackQuery, state: FSMContext):
    """
    群内有人点击“回报同步失效”按钮后，将对应 content_id 的商品卡片贴到当前群/话题
    """
    try:
        _, cid = callback_query.data.split(":")
        content_id = int(cid)
    except Exception:
        return await callback_query.answer("⚠️ 参数错误", show_alert=True)
    user_id = callback_query.from_user.id
    bot_username = await get_bot_username()

    # 取得预览卡片（沿用你现成的函数）
    product_row = await get_product_info(content_id)
    product_info = product_row.get("product_info") or {}
    file_id = product_info.get("m_file_id") or ""

    if file_id:
        return await callback_query.answer(f"⚠️ 请点选审核", show_alert=True)
    
    if product_info.get("review_status") in (2,4):
        guild_row = await AnanBOTPool.check_guild_role(user_id,'manager')
        if not guild_row:
            return await callback_query.answer(f"⚠️ 这个资源正在审核状态(需要撸馆社团干部权限才能审核)", show_alert=True)
    elif product_info.get("review_status") in (3, 5):
        guild_row = await AnanBOTPool.check_guild_role(user_id,'owner')
        if not guild_row:
            return await callback_query.answer(f"⚠️ 这个资源正在上架中(需要撸馆社长权限才能审核)", show_alert=True)
    else:
        pass

    await AnanBOTPool.set_product_review_status(content_id, 11)  # 11 同步失败

    spawn_once(f"_review_next_product:{content_id}",_review_next_product(state) )

    result_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"🆖 同步失效", callback_data="a=nothing")]]
    )
                
    await bot.edit_message_reply_markup(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        reply_markup=result_kb
    )

    return await callback_query.answer(
        f"🆖 这个资源已经回报为同步失效 {product_info.get('review_status')}",
        show_alert=True
    )



def has_reportfail(m: InlineKeyboardMarkup | None) -> bool:
    if not m or not m.inline_keyboard:
        return False
    for row in m.inline_keyboard:
        for btn in row:
            # 有些按钮可能是 url 按钮（没有 callback_data），做个防御
            if getattr(btn, "callback_data", None) == target_cb:
                return True
    return False


@dp.callback_query(F.data.startswith("review:"))
async def handle_review_button(callback_query: CallbackQuery, state: FSMContext):
    """
    群内有人点击“查看/审核”按钮后，将对应 content_id 的商品卡片贴到当前群/话题
    """
    try:
        _, cid = callback_query.data.split(":")
        content_id = int(cid)
    except Exception:
        return await callback_query.answer("⚠️ 参数错误", show_alert=True)

    

    user_id = callback_query.from_user.id
    bot_username = await get_bot_username()


    # 取得预览卡片（沿用你现成的函数）
    product_row = await get_product_info(content_id)

    product_info = product_row.get("product_info") or {}
    print(f"{content_id} -> {product_info['review_status']}", flush=True)
    # thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    '''
    审核状态
    0   编辑中(投稿者)
    1   未通过审核(投稿者)
    2   初审进行中
    3   通过初审,复审进行中
    4   经检举,初审进行中
    6   通过终核,上架进行中
    7   上架失败
    9   成功上架 
    '''


    if product_info.get("review_status") in (2,4):
        guild_row = await AnanBOTPool.check_guild_role(user_id,'manager')
        if not guild_row:
            return await callback_query.answer(f"⚠️ 这个资源正在审核状态(需要撸馆社团干部权限才能审核)", show_alert=True)
    elif product_info.get("review_status") in (3, 5):
        guild_row = await AnanBOTPool.check_guild_role(user_id,'owner')
        if not guild_row:
            return await callback_query.answer(f"⚠️ 这个资源正在上架中(需要撸馆社长权限才能审核)", show_alert=True)
    else:
        result_kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=f"✅ Checked", callback_data="a=nothing")]]
        )
                    
        await bot.edit_message_reply_markup(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            reply_markup=result_kb
        )
        return await callback_query.answer(
            f"⚠️ 这个资源已经不是审核的状态 {product_info.get('review_status')}",
            show_alert=True
        )

    
    # 群/话题定位：沿用当前消息所在的 chat & thread（若存在）
    thumb_file_id = product_row.get("thumb_file_id") or ""
    thumb_file_unique_id = product_row.get("thumb_file_unique_id") or ""
    preview_text = product_row.get("preview_text") or ""
    source_id = product_info.get("source_id") or ""
    file_id = product_info.get("m_file_id") or ""
    file_type = product_info.get("file_type") or ""

    buttons = product_info.get("buttons") or [] 

    if not product_review_url_cache.get(content_id):
        # ===== 构造“返回审核”的链接（指向当前这条群消息）=====
        src_chat_id   = callback_query.message.chat.id
        src_msg_id    = callback_query.message.message_id
        src_thread_id = getattr(callback_query.message, "message_thread_id", None)

        # -100xxxxxxxxxx → xxxxxxxxxx
        chat_for_link = str(src_chat_id)
        if chat_for_link.startswith("-100"):
            chat_for_link = chat_for_link[4:]
        else:
            chat_for_link = str(abs(src_chat_id))

        if src_thread_id:
            return_url = f"https://t.me/c/{chat_for_link}/{src_thread_id}/{src_msg_id}"
        else:
            return_url = f"https://t.me/c/{chat_for_link}/{src_msg_id}"


        product_review_url_cache[content_id] = return_url
        if return_url:
            buttons.extend([
                [
                    InlineKeyboardButton(text="🔙 返回审核", url=f"{return_url}")
                ]
            ])




    preview_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    msg = callback_query.message
    target_cb = f"reportfail:{content_id}"
    markup = msg.reply_markup  # InlineKeyboardMarkup 或 None


    # #先发资源
    if not file_id:
        invalidate_cached_product(content_id)
        await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unique_id, thumb_file_id, bot_username)
        await Media.fetch_file_by_file_id_from_x(state, source_id, 10)
        #TODO: 应该在发送到审核区时就会做一次了
        spawn_once(f"refine:{content_id}", AnanBOTPool.refine_product_content(content_id))
        
        # 2) 检查并补上“🆖 回报同步失败”按钮
        try:
          



            if not has_reportfail(markup):
                new_rows = []
                if markup and markup.inline_keyboard:
                    # 复制原有按钮，不破坏现有的“审核/机器人/…”布局
                    for row in markup.inline_keyboard:
                        new_rows.append(list(row))
                # 追加一行“回报同步失败”按钮
                new_rows.append([
                    InlineKeyboardButton(text="🆖 回报同步失败", callback_data=target_cb)
                ])
                await bot.edit_message_reply_markup(
                    chat_id=msg.chat.id,
                    message_id=msg.message_id,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=new_rows),
                )
        except Exception as e:
            # 不要影响主流程；这里仅记日志即可
            logging.exception(f"补回报按钮失败: content_id={content_id}, err={e}")

        # 3) 给用户弹窗提示
        return await callback_query.answer(f"👉 资源正在同步中，请1分钟后再试 \r\n\r\n(若一直无法同步，请点击🆖无法同步按钮)", show_alert=True)
    


    # TODO

    spawn_once(f"refine:{content_id}", AnanBOTPool.sync_bid_product())


    if file_id :
        try:
            if file_type == "photo" or file_type == "p":
                await bot.send_photo(chat_id=user_id, photo=file_id)
            elif file_type == "video" or file_type == "v":
                await bot.send_video(chat_id=user_id, video=file_id)
            elif file_type == "document" or file_type == "d":
                await bot.send_document(chat_id=user_id, document=file_id)
        except Exception as e:
            print(f"❌ 目标 chat 不存在或无法访问: {e}")


    try:
        # 发送图文卡片（带同样的操作按钮）
        newsend = await bot.send_photo(
            chat_id=user_id,
            photo=thumb_file_id,
            caption=preview_text,
            reply_markup=preview_keyboard,
            parse_mode="HTML"
        )

        await update_product_preview(content_id, thumb_file_id, state , newsend)
        
        await callback_query.answer(f"👉 机器人(@{bot_username})已将审核内容传送给你", show_alert=False)

        # ✅ 立刻把按钮文字改成「{username} 🔎 审核中」
        await _rename_review_button_to_in_progress(callback_query, content_id)

    except Exception as e:

        error_text = str(e).lower()

        # 定义可能代表文件 ID 无效的关键字
        file_invalid_keywords = [
            "wrong file identifier",
            "can't use file of type"
        ]

        if any(keyword in error_text for keyword in file_invalid_keywords):
            await AnanBOTPool.upsert_product_thumb(int(content_id), thumb_file_unique_id, '', bot_username)
            invalidate_cached_product(content_id)
            print(
                f"🔄 无效的文件 ID，已清理缓存，准备重新拉取 {source_id} for content_id: {content_id}, thumb_file_id: {thumb_file_id}",
                flush=True
            )
            await callback_query.answer("⚠️ 发送的文件无效，正在自动修复中，请稍候再试", show_alert=True)
        else:
            await callback_query.answer(
                f"⚠️ 请先启用机器人 (@{bot_username}) 私信 (私信机器人按 /start )",
                show_alert=True
            )

        print(f"⚠️ 发送审核卡片失败: {e}", flush=True)







############
#  举报功能   
############
async def fix_suggest_content(message:Message, content_id: int, state) -> bool:
    """
    修复建议内容（新版）：
    """
    try:
        bot_username = await get_bot_username()
        await message.delete()
       
        product_row = await get_product_info(content_id)

     

        thumb_file_id = product_row.get("thumb_file_id") or ""
        preview_text = product_row.get("preview_text") or ""
        preview_keyboard = product_row.get("preview_keyboard") or ""

        product_info = product_row.get("product_info") or {}
        file_id = product_info.get("m_file_id") or ""
        thumb_file_unqiue_id = product_info.get("thumb_file_unique_id") or ""
        source_id = product_info.get("source_id") or ""
        file_type = product_info.get("file_type") or ""
        review_status = product_info.get("review_status") or 0
        
        if(review_status!=4):
            return await message.answer("🤠 该资源已纠错审核完成")


        from_user_id = message.from_user.id


        if not file_id and source_id and thumb_file_id:
            print(f"背景搬运 {source_id} for content_id: {content_id}", flush=True)
            # 不阻塞：丢到后台做补拉
            spawn_once(f"src:{source_id}", Media.fetch_file_by_file_id_from_x(state, source_id, 10))

            print(f"创建或更新sora_media {thumb_file_unqiue_id} for content_id: {content_id}", flush=True)
            await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unqiue_id, thumb_file_id, bot_username)



        # #先发资源
        if file_id :
            try:
                if file_type == "photo" or file_type == "p":
                    await lz_var.bot.send_photo(chat_id=from_user_id, photo=file_id)
                elif file_type == "video" or file_type == "v":
                    await lz_var.bot.send_video(chat_id=from_user_id, video=file_id)
                elif file_type == "document" or file_type == "d":
                    await lz_var.bot.send_document(chat_id=from_user_id, document=file_id)
            except Exception as e:
                print(f"❌ 目标 chat 不存在或无法访问: {e}")

        #再发设置按钮
        try:
            print(f"🔄 重新发送设置按钮")
            new_msg = await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
            print(f"{new_msg}", flush=True)
            await update_product_preview(content_id, thumb_file_id, state, new_msg)
        except Exception as e:
            err_text = str(e)

            # 特殊处理：如果是 video 当作 photo 的错误，就删除 sora_media.thumb_file_id
            if "can't use file of type" in err_text:
                try:
                    await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unqiue_id, None, await get_bot_username())
                    print(f"🗑 已删除错误的 thumb_file_id for content_id={content_id}", flush=True)
                    invalidate_cached_product(content_id)

                except Exception as db_err:
                    logging.exception(f"⚠️ 删除 thumb_file_id 失败 content_id={content_id}: {db_err}")



            print(f"❌ 发送预览图失败: {e}", flush=True)
            return False
        
        




        
        return True
    except Exception as e:
        logging.exception(f"[fix_suggest] 失败 content_id={content_id}: {e}")
        return False


async def report_content(user_id: int, file_unique_id: str, state: FSMContext, model: str ="normal") -> bool:
    """
    举报流程（新版）：
    1) 校验用户是否对该资源有可举报的交易
    2) 校验是否已有举报在处理
    3) 弹出举报类型按钮（report_type）
       若能拿到缩图，则以 send_photo 展示；否则 send_message。
    """
    try:
        trade_url = await AnanBOTPool.get_trade_url(file_unique_id)

        # Step 1: 交易记录校验
        tx = await AnanBOTPool.find_user_reportable_transaction(user_id, file_unique_id)
        if not tx or not tx.get("transaction_id"):
            await bot.send_message(
                chat_id=user_id,
                text=f"<a href='{trade_url}'>{file_unique_id}</a> 需要有兑换纪录才能举报",
                parse_mode="HTML"
            )

            if model != "admin":
                return False
           

        # Step 2: 是否已有举报在处理中
        existing = await AnanBOTPool.find_existing_report(file_unique_id)
        if existing and existing.get("report_id"):
            await bot.send_message(
                chat_id=user_id,
                text=f"<a href='{trade_url}'>{file_unique_id}</a> 已有人先行反馈",
                parse_mode="HTML"
            )

            #送出审核 TODO
            if model == "admin":
                content_id = await AnanBOTPool.get_content_id_by_file_unique_id(file_unique_id)
                await AnanBOTPool.set_product_review_status(content_id, 4)  # 更新为经检举,初审进行中
                result , error = await send_to_review_group(content_id, state)
            else:
                return False

        # Step 3: 举报类型按钮（短文案，防止 TG 截断）
        kb = build_report_type_keyboard(file_unique_id, tx['transaction_id'])

        
        content_id = await AnanBOTPool.get_content_id_by_file_unique_id(file_unique_id)

        product_row = await get_product_info(content_id)

        thumb_file_id = product_row.get("thumb_file_id") or ""
        preview_text = product_row.get("preview_text") or ""
       
        prompt = f"{preview_text}\r\n\r\n请选择对 <a href='{trade_url}'>{file_unique_id}</a> 的反馈类型："

        if thumb_file_id:
            # 用图片 + caption
            new_msg=await bot.send_photo(
                chat_id=user_id,
                photo=thumb_file_id,
                caption=prompt,
                parse_mode="HTML",
                reply_markup=kb
            )
            await update_product_preview(content_id, thumb_file_id, state, new_msg)
        else:
            # 纯文本
            await bot.send_message(
                chat_id=user_id,
                text=prompt,
                parse_mode="HTML",
                reply_markup=kb
            )

        return True

    except Exception as e:
        logging.exception(f"[report] 失败 user_id={user_id} file_unique_id={file_unique_id}: {e}")
        try:
            await bot.send_message(chat_id=user_id, text="⚠️ 反馈处理失败，请稍后重试。")
        except Exception:
            pass
        return False

@dp.callback_query(F.data.startswith("report_type:"))
async def handle_choose_report_type(callback_query: CallbackQuery, state: FSMContext):
    """
    用户点举报类型按钮后，进入 FSM 等待说明文字
    回调格式：report_type:<file_unique_id>:<transaction_id>:<report_type>
    """
    try:
        _, file_unique_id, tx_id_s, rtype_s = callback_query.data.split(":")
        transaction_id = int(tx_id_s)
        report_type = int(rtype_s)
    except Exception:
        return await callback_query.answer("⚠️ 参数错误", show_alert=True)

    # 记录到 FSM
    await state.set_state(ProductPreviewFSM.waiting_for_report_reason)
    await state.set_data({
        "report_file_unique_id": file_unique_id,
        "report_transaction_id": transaction_id,
        "report_type": report_type,
        "report_user_id": callback_query.from_user.id
    })

    # 在同一条消息上改文案 + 增加「放弃举报」按钮
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="放弃举报", callback_data=f"cancel_report:{file_unique_id}:{transaction_id}")]
    ])

    try:
        if callback_query.message.photo:
            # 首屏是 send_photo → 用 edit_caption
            await callback_query.message.edit_caption(
                caption="请填写举报说明（文字），发送后即提交。",
                reply_markup=cancel_kb
            )
        else:
            await callback_query.message.edit_text(
                text="请填写举报说明（文字），发送后即提交。",
                reply_markup=cancel_kb
            )
    except Exception:
        # 兜底：如果无法 edit（例如消息已不存在），就另发一条
        await bot.send_message(
            chat_id=callback_query.from_user.id,
            text="请填写举报说明（文字），发送后即提交。",
            reply_markup=cancel_kb
        )

    await callback_query.answer()

@dp.message(F.chat.type == "private", ProductPreviewFSM.waiting_for_report_reason, F.text)
async def handle_report_reason_text(message: Message, state: FSMContext):
    data = await state.get_data()
    file_unique_id = data.get("report_file_unique_id")
    transaction_id = data.get("report_transaction_id")
    report_type = data.get("report_type")
    user_id = data.get("report_user_id")

    reason = message.text.strip()
    if not file_unique_id or not transaction_id or not report_type:
        await state.clear()
        return await message.answer("⚠️ 缺少举报信息，请重试。")

    # 入库：调用你自家的 DB 封装方法（需在 AnanBOTPool 中实现）
    # 建议方法签名：
    #   async def create_report(file_unique_id: str, transaction_id: int, report_type: int, report_reason: str) -> int
    try:
        report_id = await AnanBOTPool.create_report(
            file_unique_id=file_unique_id,
            transaction_id=transaction_id,
            report_type=report_type,
            report_reason=reason
        )
        
        content_id = await AnanBOTPool.get_content_id_by_file_unique_id(file_unique_id)
        await AnanBOTPool.set_product_review_status(content_id, 4)  # 更新为经检举,初审进行中
        result , error = await send_to_review_group(content_id, state)
        if result:
            await message.answer(f"✅ 举报已提交（编号：{report_id}）。我们会尽快处理。")
            
        else:
            if error:
                await message.answer(f"⚠️ 发送失败：{error}")
            else:
                await message.answer("⚠️ 发送失败：未知错误")

    except Exception as e:
        logging.exception(f"create_report 失败: {e}")
        await message.answer("⚠️ 提交失败，请稍后重试。")

    try:
        await state.clear()
    except Exception:
        pass

@dp.callback_query(F.data.startswith("cancel_report:"))
async def handle_cancel_report(callback_query: CallbackQuery, state: FSMContext):
    """
    用户点击『放弃举报』，清理举报相关的 FSM。
    - 若首屏是 send_photo：删除该图片消息，并另外发送一条文本“已放弃举报。”
    - 若首屏是文本：直接 edit_text 为“已放弃举报。”
    回调格式：cancel_report:<file_unique_id>:<transaction_id>
    """
    try:
        _, _file_unique_id, _tx_id_s = callback_query.data.split(":")
    except Exception:
        return await callback_query.answer("⚠️ 参数错误", show_alert=True)

    # 清理举报相关的 FSM（仅当正处于填写举报说明时）
    try:
        cur_state = await state.get_state()
        if cur_state == ProductPreviewFSM.waiting_for_report_reason.state:
            await state.clear()
    except Exception:
        pass

    chat_id = callback_query.message.chat.id
    msg_id = callback_query.message.message_id

    try:
        if getattr(callback_query.message, "photo", None):
            # 是图片消息：删除图片，另发一条文本确认
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                # 兜底：删失败就改 caption 并移除按钮
                try:
                    await callback_query.message.edit_caption("已放弃举报。")
                except Exception:
                    pass
            finally:
                try:
                    await bot.send_message(chat_id=chat_id, text="已放弃举报。")
                except Exception:
                    pass
        else:
            # 纯文本消息：直接改文案并移除按钮
            try:
                await callback_query.message.edit_text("已放弃举报。")
            except Exception:
                # 兜底：发新消息
                await bot.send_message(chat_id=chat_id, text="已放弃举报。")
    except Exception:
        # 最终兜底
        try:
            await bot.send_message(chat_id=chat_id, text="已放弃举报。")
        except Exception:
            pass

    await callback_query.answer("已取消")

@dp.callback_query(F.data.startswith("judge_suggest:"))
async def handle_judge_suggest(callback_query: CallbackQuery, state: FSMContext):
    """
    管理员对举报进行裁定：认可举报 (Y) 或 不认可举报 (N)
    回调格式：judge_suggest:<content_id>:'Y' 或 'N'
    """
    try:
        _, content_id_s, decision = callback_query.data.split(":")
        content_id = int(content_id_s)
        decision = decision.strip("'")  # Y 或 N
    except Exception:
        return await callback_query.answer("⚠️ 参数错误", show_alert=True)

    try:
        # 从 DB 查举报详情 (交易 + 举报 + bid)
        product_row = await get_product_info(content_id)
        product_info = product_row.get("product_info") or {}
        file_unique_id = product_info.get('source_id')

        # try:
            
        #     if source_id is None:
        #         sora_content = await AnanBOTPool.search_sora_content_by_id(content_id,lz_var.bot_username)  # 确保 content_id 存在
        #         file_unique_id = source_id = sora_content.get("source_id") if sora_content else None
        #     tag_set = await AnanBOTPool.get_tags_for_file(source_id) if source_id else set()
        #     tag_count = len(tag_set or [])
        # except Exception:
        #     tag_set = set()
        #     tag_count = 0

        # # 内容长度校验（“超过30字”→ 严格 > 30）
        # content_ok = len(content_text) > 30
        # tags_ok = tag_count >= 5
        # thumb_ok = has_custom_thumb
        # has_tag_ok = has_tag_string


        # # 如果有缺项，给出可操作的引导并阻止送审
        # if not (content_ok and tags_ok and thumb_ok and has_tag_ok):
        #     missing_parts = []
        #     if not content_ok:
        #         missing_parts.append("📝 内容需 > 30 字")
        #     if not thumb_ok:
        #         missing_parts.append("📷 需要设置预览图（不是默认图）")

        #     if not has_tag_ok:
        #         missing_parts.append(f"🏷️ 请检查标签是否正确")
        #     elif not tags_ok :
        #         missing_parts.append(f"🏷️ 标签需 ≥ 5 个（当前 {tag_count} 个）")
            

        #     tips = "⚠️ 送审前需补全：\n• " + "\n• ".join(missing_parts)

        #     return await callback_query.answer(tips, show_alert=True)


        check_result,check_error_message =  await _check_product_policy(product_row)
        if check_result is not True:
            return await callback_query.answer(check_error_message, show_alert=True)
        
        report_info = await AnanBOTPool.find_existing_report(file_unique_id)
        if not report_info:
            await AnanBOTPool.set_product_review_status(content_id, 6) #进入复审阶段
            invalidate_cached_product(content_id)
            return await callback_query.answer("⚠️ 找不到举报信息", show_alert=True)

        # 期望字段（见 get_report_detail_by_content 的 SELECT）
        report_id     = report_info.get("report_id")
        sender_id     = report_info.get("sender_id")       # 举报人 (发起交易的人)
        owner_user_id = report_info.get("owner_user_id")   # 上传者 (bid.owner_user_id)
        sender_fee    = int(report_info.get("sender_fee") or 0)
        receiver_fee  = int(report_info.get("receiver_fee") or 0)
        report_reason = report_info.get("report_reason") or ""

        reply_msg = (
            f"你所举报的资源 <a href='https://t.me/{lz_var.bot_username}?start={file_unique_id}'>{file_unique_id}</a>\n"
            f"检举理由: {report_reason}\n"
        )

        option_buttons = []

        if decision == "Y":  # 认可举报
            reply_msg += f"举报内容成立，将退还 {sender_fee} 积分"

            print(f"sender_id = {sender_id}")
            # 1) 记录退费交易
            ret_refund = await AnanBOTPool.transaction_log({
                "sender_id": owner_user_id,
                "sender_fee": -1 * receiver_fee,
                "receiver_id": sender_id,
                "receiver_fee": -1 * sender_fee,
                "transaction_type": "refund",
                "transaction_description": str(report_id)
            })

            print(f"{ret_refund}")

            if ret_refund['status'] == 'insert':
                # 2) 通知举报人
                print(f"✅ 已记录退费交易，退还 {sender_fee} 积分给举报人 {sender_id}")
                try:
                    await bot.send_message(
                        chat_id=sender_id,
                        text=reply_msg,
                        parse_mode="HTML"
                    )

                    # 3) 通知上传者
                    await bot.send_message(
                        chat_id=owner_user_id,
                        text=(
                            f"你所上传的资源 <a href='https://t.me/{lz_var.bot_username}?start={file_unique_id}'>{file_unique_id}</a> "
                            f"被举报，将回收之前的积分分成。\n检举理由: {report_reason}"
                        ),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"❌ 目标 chat 不存在或无法访问: {e}")

            # 4) 更新 bid 表（owner_user_id 交回给系统或指定 ID）
            await AnanBOTPool.update_bid_owner(file_unique_id, new_owner_id="6874579736")

            # 5) 更新 report 状态
            await AnanBOTPool.update_report_status(report_id, "approved")
            await handle_approve_product(callback_query, state)
            # await AnanBOTPool.set_product_review_status(content_id, 3) #进入复审阶段

            # option_buttons.append([
            #     InlineKeyboardButton(
            #         text=f"✅ 确认举报属实 ({sender_id})",
            #         callback_data="a=nothing"
            #     )
            # ])

        elif decision == "N":  # 不认可举报
            reply_msg += (
                "举报内容不成立。\n若密文失效，请在获取密文的消息点击 '❌ 失效' 即会更换新的密文。\n"
                "若仍无法更换，请等待资源持有者重新上传，再重新兑换一次即可获得新密文或连结（免积分）。"
            )

            
            try:
                # 通知举报人
                await bot.send_message(
                    chat_id=sender_id,
                    text=reply_msg,
                    parse_mode="HTML"
                )
            except Exception as e:
                print(f"❌ 目标 chat 不存在或无法访问: {e}")


            # 更新 report 状态
            await AnanBOTPool.update_report_status(report_id, "rejected")
            # await AnanBOTPool.set_product_review_status(content_id, 3) #进入复审阶段
            await handle_approve_product(callback_query, state)
            # option_buttons.append([
            #     InlineKeyboardButton(
            #         text=f"❌ 不认可举报 ({sender_id})",
            #         callback_data="a=nothing"
            #     )
            # ])

        # 编辑原消息按钮，替换为结果
        # try:
        #     await callback_query.message.edit_reply_markup(
        #         reply_markup=InlineKeyboardMarkup(inline_keyboard=option_buttons)
        #     )
        # except Exception as e:
        #     logging.exception(f"编辑举报裁定按钮失败: {e}")

        await callback_query.answer("✅ 已处理举报", show_alert=False)
        invalidate_cached_product(content_id)

        # 找下一个
        
        report_row = await AnanBOTPool.get_next_report_to_judge()
        print(f"下一个待裁定 {report_row}", flush=True)
        next_file_unique_id = report_row['file_unique_id'] if report_row else None
        report_id = report_row['report_id']
        print(f"下一个待裁定 {next_file_unique_id}", flush=True)
        if next_file_unique_id:
            next_content_id = await AnanBOTPool.get_content_id_by_file_unique_id(next_file_unique_id)
            result , error = await send_to_review_group(next_content_id, state)
            await AnanBOTPool.set_product_review_status(next_content_id, 4)  # 更新为经检举,初审进行中
            await AnanBOTPool.update_report_status(report_id, "published")


    except Exception as e:
        logging.exception(f"[judge_suggest] 裁定失败 content_id={content_id}: {e}")
        await callback_query.answer("⚠️ 裁定失败，请稍后重试", show_alert=True)



async def _check_product_policy(product_row):
    # product_row = await get_product_info(content_id)
    product_info = product_row.get("product_info") or {}
    source_id = file_unique_id = product_info.get('source_id')
    content_text = (product_info.get("content") or "").strip()
    tag_string = product_info.get("tag", "")
    thumb_file_id = product_row.get("thumb_file_id") or ""
    has_custom_thumb = bool(thumb_file_id and thumb_file_id != DEFAULT_THUMB_FILE_ID)
    has_tag_string = bool(tag_string and tag_string.strip())

    try:
        tag_set = await AnanBOTPool.get_tags_for_file(source_id) if source_id else set()
        tag_count = len(tag_set or [])
    except Exception:
        tag_set = set()
        tag_count = 0

    # 内容长度校验（“超过30字”→ 严格 > 30）
    content_ok = len(content_text) > 30
    tags_ok = tag_count >= 5
    thumb_ok = has_custom_thumb
    has_tag_ok = has_tag_string

    check_result = True
    check_error_message = None

    # 如果有缺项，给出可操作的引导并阻止送审
    if not (content_ok and tags_ok and thumb_ok and has_tag_ok):
        missing_parts = []
        if not content_ok:
            missing_parts.append("📝 内容需 > 30 字")
        if not thumb_ok:
            missing_parts.append("📷 需要设置预览图（不是默认图）")

        if not has_tag_ok:
            missing_parts.append(f"🏷️ 请检查标签是否正确")
        elif not tags_ok :
            missing_parts.append(f"🏷️ 标签需 ≥ 5 个（当前 {tag_count} 个）")
        

        tips = "⚠️ 送审前需补全：\n• " + "\n• ".join(missing_parts)
        check_result = False
        check_error_message = tips
        
    return check_result,check_error_message

############
#  系列
############
SERIES_CTX = "series_ctx"  # 保存“原始 caption/按钮”的上下文



def build_series_keyboard(all_series: list[dict], selected_ids: set[int], content_id: int, per_row: int = 2) -> InlineKeyboardMarkup:
    btns = []
    for s in all_series:
        sid = int(s["id"] if isinstance(s, dict) else s[0])
        name = s["name"] if isinstance(s, dict) else s[1]
        checked = sid in selected_ids
        text = f"{'✅' if checked else '⬜'} {name}"
        btns.append(InlineKeyboardButton(text=text, callback_data=f"series_toggle:{content_id}:{sid}"))
    rows = [btns[i:i+per_row] for i in range(0, len(btns), per_row)]
    rows.append([InlineKeyboardButton(text="✅ 设置完成并返回", callback_data=f"series_close:{content_id}")])
    rows.append([InlineKeyboardButton(text="取消", callback_data=f"series_cancel:{content_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.startswith("series:"))
async def open_series_panel(cb: CallbackQuery, state: FSMContext):
    try:
        _, cid = cb.data.split(":")
        content_id = int(cid)
    except Exception:
        return await cb.answer("⚠️ 参数错误", show_alert=True)

    row = await AnanBOTPool.get_sora_content_by_id(content_id)
    if not row or not row.get("source_id"):
        return await cb.answer("⚠️ 找不到该资源的 source_id", show_alert=True)
    file_unique_id = row["source_id"]

    # 读全量系列与已选
    all_series = await AnanBOTPool.get_all_series()
    selected_ids_db = await AnanBOTPool.get_series_ids_for_file(file_unique_id)

    # FSM：缓存“原始 caption + 按钮”与“当前选择”
    data = await state.get_data()
    ctx = data.get(SERIES_CTX, {})
    key = f"{cb.message.chat.id}:{cb.message.message_id}"
    if key not in ctx:
        ctx[key] = {
            "orig_caption": cb.message.caption or "",
            "orig_markup": cb.message.reply_markup  # 直接存对象，关闭时重用
        }
        await state.update_data(**{SERIES_CTX: ctx})
    await state.update_data({f"selected_series:{file_unique_id}": list(selected_ids_db)})

    # 生成面板 caption（附统计）
    selected_names = [s["name"] for s in all_series if s["id"] in selected_ids_db]
    unselected_names = [s["name"] for s in all_series if s["id"] not in selected_ids_db]
    panel = (
        "\n\n📚 系列（点击切换）\n"
        f"已选（{len(selected_names)}）：{', '.join(selected_names) if selected_names else '无'}\n"
        f"未选（{len(unselected_names)}）：{', '.join(unselected_names) if unselected_names else '无'}"
    )
    new_caption = (ctx[key]["orig_caption"] or "").rstrip() + panel

    kb = build_series_keyboard(all_series, selected_ids_db, content_id)
    try:
        await cb.message.edit_caption(caption=new_caption, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await cb.message.edit_text(text=new_caption, reply_markup=kb, parse_mode="HTML")
    finally:
        await cb.answer()

@dp.callback_query(F.data.startswith("series_toggle:"))
async def toggle_series_item(cb: CallbackQuery, state: FSMContext):
    try:
        _, cid, sid = cb.data.split(":")
        content_id = int(cid)
        series_id = int(sid)
    except Exception:
        return await cb.answer("⚠️ 参数错误", show_alert=True)

    row = await AnanBOTPool.get_sora_content_by_id(content_id)
    if not row or not row.get("source_id"):
        return await cb.answer("⚠️ 找不到该资源的 source_id", show_alert=True)
    file_unique_id = row["source_id"]

    # FSM 中读取并更新“当前选择”
    data = await state.get_data()
    fsm_key = f"selected_series:{file_unique_id}"
    selected_ids = set(data.get(fsm_key, []))
    if series_id in selected_ids:
        selected_ids.remove(series_id)
        tip = "❎ 已取消"
    else:
        selected_ids.add(series_id)
        tip = "✅ 已选中"
    await state.update_data({fsm_key: list(selected_ids)})

    # 重渲染 caption + 键盘
    all_series = await AnanBOTPool.get_all_series()
    selected_names = [s["name"] for s in all_series if s["id"] in selected_ids]
    unselected_names = [s["name"] for s in all_series if s["id"] not in selected_ids]

    # 取原 caption
    ctx = data.get(SERIES_CTX, {})
    key = f"{cb.message.chat.id}:{cb.message.message_id}"
    base_caption = (ctx.get(key) or {}).get("orig_caption", cb.message.caption or "")
    panel = (
        "\n\n📚 系列（点击切换）\n"
        f"已选（{len(selected_names)}）：{', '.join(selected_names) if selected_names else '无'}\n"
        f"未选（{len(unselected_names)}）：{', '.join(unselected_names) if unselected_names else '无'}\n"
        f"{tip}"
    )
    new_caption = (base_caption or "").rstrip() + panel
    kb = build_series_keyboard(all_series, selected_ids, content_id)

    try:
        await cb.message.edit_caption(caption=new_caption, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await cb.message.edit_text(text=new_caption, reply_markup=kb, parse_mode="HTML")
    finally:
        await cb.answer()


@dp.callback_query(F.data.startswith("series_close:"))
async def close_series_panel(cb: CallbackQuery, state: FSMContext):
    try:
        _, cid = cb.data.split(":")
        content_id = int(cid)
    except Exception:
        return await cb.answer("⚠️ 参数错误", show_alert=True)

    # 定位 file_unique_id
    sora = await AnanBOTPool.get_sora_content_by_id(content_id)
    if not sora or not sora.get("source_id"):
        return await cb.answer("⚠️ 找不到该资源的 source_id", show_alert=True)
    file_unique_id = sora["source_id"]

    # 取 FSM 最终选择并落库
    data = await state.get_data()
    fsm_key = f"selected_series:{file_unique_id}"
    selected_ids = set(map(int, data.get(fsm_key, [])))
    try:
        summary = await AnanBOTPool.sync_file_series(file_unique_id, selected_ids)
    except Exception as e:
        logging.exception(f"落库系列失败: {e}")
        summary = {"added": 0, "removed": 0, "unchanged": 0}

    # 清理 FSM
    try:
        await state.update_data({fsm_key: []})
    except Exception:
        pass
    ctx = data.get(SERIES_CTX, {})
    key = f"{cb.message.chat.id}:{cb.message.message_id}"
    if key in ctx:
        del ctx[key]
        await state.update_data(**{SERIES_CTX: ctx})

    # 失效缓存并重绘商品卡片
    try:
        invalidate_cached_product(content_id)
    except Exception:
        pass

    thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
    try:
        await cb.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard
        )
    except Exception as e:
        logging.exception(f"返回商品卡片失败: {e}")
        # 兜底：至少把按钮恢复
        try:
            await cb.message.edit_reply_markup(reply_markup=preview_keyboard)
        except Exception:
            pass

    await cb.answer(f"✅ 系列已保存 (+{summary.get('added',0)}/-{summary.get('removed',0)})", show_alert=False)


@dp.callback_query(F.data.startswith("series_cancel:"))
async def cancel_series_panel(cb: CallbackQuery, state: FSMContext):
    try:
        _, cid = cb.data.split(":")
        content_id = int(cid)
    except Exception:
        return await cb.answer("⚠️ 参数错误", show_alert=True)

    # 清理和系列相关的 FSM 缓存（不落库）
    try:
        # 取得当前资源的 file_unique_id，清除选择缓存
        sora = await AnanBOTPool.get_sora_content_by_id(content_id)
        if sora and sora.get("source_id"):
            fsm_key = f"selected_series:{sora['source_id']}"
            data = await state.get_data()
            if fsm_key in data:
                await state.update_data({fsm_key: []})

        # 清掉保存的原始 caption/markup（如果存过）
        data = await state.get_data()
        ctx = data.get("series_ctx", {})
        key = f"{cb.message.chat.id}:{cb.message.message_id}"
        if key in ctx:
            del ctx[key]
            await state.update_data(**{"series_ctx": ctx})
    except Exception:
        pass

    # 直接回到商品卡片（不保存任何变更）
    try:
        thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
        await cb.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard
        )
    except Exception:
        # 兜底：至少恢复按钮
        try:
            _, preview_text, preview_keyboard = await get_product_tpl(content_id)
            await cb.message.edit_caption(caption=preview_text, parse_mode="HTML")
            await cb.message.edit_reply_markup(reply_markup=preview_keyboard)
        except Exception:
            pass

    await cb.answer("已取消，不做修改")


############
#  共用   
############

@dp.message(F.chat.type == "private", F.text.startswith("/removetag"))
async def handle_start_remove_tag(message: Message, state: FSMContext):
    parts = message.text.strip().split(" ", 1)
    if len(parts) != 2:
        return await message.answer("❌ 使用格式: /removetag [tag]")

    tag = parts[1].strip()
    await state.set_state(ProductPreviewFSM.waiting_for_removetag_source)
    await state.set_data({"tag": tag})
    await message.answer(f"🔍 请发送要移除该 tag 的 source_id")

@dp.message(F.chat.type == "private", ProductPreviewFSM.waiting_for_removetag_source, F.text)
async def handle_removetag_source_input(message: Message, state: FSMContext):
    source_id = message.text.strip()
    data = await state.get_data()
    tag = data.get("tag")

    if not tag or not source_id:
        await message.answer("⚠️ 缺少 tag 或 source_id")
        return

    receiver_row = await AnanBOTPool.find_rebate_receiver_id(source_id, tag)

    if receiver_row is not None and 'receiver_id' in receiver_row and receiver_row['receiver_id']:
    # do something

        await message.answer(f"✅ 找到关联用户 receiver_id: {receiver_row['receiver_id']}")

        result = await AnanBOTPool.transaction_log({
            'sender_id': receiver_row['receiver_id'],
            'receiver_id': 0,
            'transaction_type': 'penalty',
            'transaction_description': source_id,
            'sender_fee': (receiver_row['receiver_fee'])*(-2),
            'receiver_fee': 0,
            'memo': tag
        })
        if result['status'] == 'insert':

            dt = datetime.fromtimestamp(receiver_row['transaction_timestamp'])
          

            await message.answer(f"✅ 已记录惩罚交易，扣除 {receiver_row['receiver_fee'] * 2} 积分")

            await AnanBOTPool.update_credit_score(receiver_row['receiver_id'], -1)

            await AnanBOTPool.media_auto_send({
                'chat_id': receiver_row['receiver_id'],
                'bot': 'salai001bot',
                'text': f"你在{dt.strftime('%Y-%m-%d %H:%M:%S')}贴的标签不对，已被扣信用分"
            })


        else:
            print(f"⚠️ 记录惩罚交易失败: {result}", flush=True)
    else:
        await message.answer("⚠️ 没有找到匹配的 rebate 记录")

    print(f"删除 tag `{tag}` from source_id `{source_id}`", flush=True)
    deleted = await AnanBOTPool.delete_file_tag(source_id, tag)
    print(f"删除 tag `{tag}` from source_id `{source_id}`: {deleted}", flush=True)

    if deleted:
        await message.answer(f"🗑️ 已移除 tag `{tag}` 从 source_id `{source_id}`")
    else:
        await message.answer("⚠️ file_tag 表中未找到对应记录")

    # asyncio.create_task(clear_removetag_timeout(state, message.chat.id))

async def clear_removetag_timeout(state: FSMContext, chat_id: int):
    await asyncio.sleep(300)
    current_state = await state.get_state()
    if current_state == ProductPreviewFSM.waiting_for_removetag_source:
        await state.clear()
        await bot.send_message(chat_id, "⏳ 已超时，取消移除标签操作。")

@dp.message(F.chat.type == "private", F.text)
async def handle_text(message: Message):
    await message.answer("hi")




@dp.message(F.chat.type == "private", F.content_type.in_({ContentType.VIDEO, ContentType.DOCUMENT, ContentType.PHOTO}))
async def handle_media(message: Message, state: FSMContext):

    timer = SegTimer(
        "handle_media",
        msg_id=message.message_id,
        from_user=message.from_user.id if message.from_user else None,
        chat_id=message.chat.id
    )

    # 立即反馈：占位消息
    await bot.send_chat_action(message.chat.id, ChatAction.TYPING)
    placeholder = await message.reply(
        "🏃‍♂️  正在处理，请稍候..."
    )
    timer.lap("send_placeholder")

   
    

    file_type = message.content_type
    bot_username = await get_bot_username()
    user_id = str(message.from_user.id)

    try:
        if file_type == ContentType.PHOTO:
            photo = get_largest_photo(message.photo)
            meta = {
                "file_type": "photo",
                "file_unique_id": photo.file_unique_id,
                "file_id": photo.file_id,
                "file_size": photo.file_size or 0,
                "duration": 0,
                "width": photo.width,
                "height": photo.height,
                "file_name": ""
            }
        elif file_type == ContentType.VIDEO:
            meta = {
                "file_type": "video",
                "file_unique_id": message.video.file_unique_id,
                "file_id": message.video.file_id,
                "file_size": message.video.file_size or 0,
                "duration": message.video.duration or 0,
                "width": message.video.width,
                "height": message.video.height,
                "file_name": message.video.file_name or ""
            }
        elif file_type == ContentType.DOCUMENT:
            meta = {
                "file_type": "document",
                "file_unique_id": message.document.file_unique_id,
                "file_id": message.document.file_id,
                "file_size": message.document.file_size or 0,
                "duration": 0,
                "width": 0,
                "height": 0,
                "file_name": message.document.file_name or ""
            }
        else:
            print(f"⚠️ 不支持的媒体类型: {file_type}", flush=True)
            return

        
    except Exception as e:
        print(f"❌ 处理媒体信息失败: {e}", flush=True)
        return await message.answer(f"⚠️ 处理媒体信息失败，请稍后重试。")
    



    timer.lap("Noe")
    product_i = await AnanBOTPool.get_product_info_by_fuid(meta['file_unique_id'])
    content_id = product_i.get("content_id") if product_i else None
    owner_user_id = str(product_i.get("owner_user_id")) if product_i else None
    timer.lap("product_i")
    if not content_id:
        spawn_once(
            f"_process_create_content_async:{message.chat.id}:{message.message_id}",
            _process_create_content_async(
                message=message,
                state=state,
                meta=meta,
                placeholder_msg_id=placeholder.message_id
            )
        )
        content_id = None
    else:
        content_id = product_i.get("content_id") if product_i else None
        product_id = product_i.get("product_id") if product_i else None
        print(f"{product_i}", flush=True)
    
        if product_id:
            if(owner_user_id!=user_id):
               await placeholder.edit_text(f"⚠️ 这个资源已经被其他用户投稿 ")
               return

           
            if product_i.get("review_status") == 2:
                guild_row = await AnanBOTPool.check_guild_role(user_id,'manager')
                timer.lap("check_guild_role")
                if not guild_row:
                    return await placeholder.edit_text(f"⚠️ 这个资源正在审核状态")
            elif product_i.get("review_status") in (3, 4, 5):
                guild_row = await AnanBOTPool.check_guild_role(user_id,'owner')
                timer.lap("check_guild_role")
                if not guild_row:
                    return await placeholder.edit_text(f"⚠️ 这个资源正在上架中")   
                #    return await message.answer(f"⚠️ 这个资源已经有人投稿 ")
            else:
                print(f"{product_i.get("review_status")} ", flush=True)

            thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
            timer.lap("get_product_tpl")

            if product_i.get("thumb_file_unique_id") is None and file_type == ContentType.VIDEO:
                buf,pic = await Media.extract_preview_photo_buffer(message, prefer_cover=True, delete_sent=True)
                timer.lap("extract_preview")


                newsend = await lz_var.bot.edit_message_media(
                    chat_id=message.chat.id,
                    message_id=placeholder.message_id,
                    media=InputMediaPhoto(media=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=preview_text, parse_mode="HTML"),
                    reply_markup=preview_keyboard
                )

                spawn_once(
                    f"_process_update_default_preview_async:{message.message_id}",
                    _process_update_default_preview_async(newsend, user_id=user_id, content_id=content_id)
                )

                # newsend = await message.answer_photo(photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
                timer.lap("send_preview_photo")
            else:
                newsend = await lz_var.bot.edit_message_media(
                    chat_id=message.chat.id,
                    message_id=placeholder.message_id,
                    media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
                    reply_markup=preview_keyboard
                )
                # newsend = await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
                timer.lap("answer_preview")

                await update_product_preview(content_id, thumb_file_id, state , newsend)
                timer.lap("update_product_preview")


        else:
            # 用 spawn_once 投递后台任务（同一消息只跑一次）
            meta['content_id'] = content_id
            meta['thumb_file_unique_id'] = product_i.get("thumb_file_unique_id")
            key = f"media_process:{message.chat.id}:{message.message_id}"
            spawn_once(
                key,
                _process_create_product_async(
                    message=message,
                    state=state,
                    meta=meta,
                    placeholder_msg_id=placeholder.message_id
                )
            )


            timer.lap("answer_create_prompt")
            pass


    '''    # ---------- 异步触发） ----------'''
    spawn_once(
        f"upsert_media:{meta['file_type']}",
        AnanBOTPool.upsert_media(meta['file_type'], {
            "file_unique_id": meta['file_unique_id'],
            "file_size": meta['file_size'],
            "duration": meta['duration'],
            "width": meta['width'],
            "height": meta['height'],
            "file_name": meta['file_name'],
            "create_time": datetime.now()
        })
    )
    timer.lap("db_upsert_media")

    spawn_once(f"insert_file_extension:{meta['file_type']}:{meta['file_unique_id']}",
    AnanBOTPool.insert_file_extension(meta['file_type'], meta['file_unique_id'], meta['file_id'], bot_username, user_id))
    timer.lap("insert_file_extension")


    # ---------- 2) 归档复制（异步触发） ----------
    spawn_once(
        f"copy_message:{message.message_id}",
        lz_var.bot.copy_message(
            chat_id=lz_var.x_man_bot_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )
    )

async def _process_create_content_async(message: Message, state: FSMContext, meta: dict, placeholder_msg_id: int):
    bot_username = await get_bot_username()
    user_id = str(message.from_user.id)
    row = await AnanBOTPool.insert_sora_content_media(meta['file_unique_id'], meta['file_type'], meta['file_size'], meta['duration'], user_id, meta['file_id'], bot_username)
    content_id = row["id"]
    meta['content_id'] = content_id
    meta['thumb_file_unique_id'] = None
    await _process_create_product_async(message, state, meta, placeholder_msg_id)
    return
    

async def _process_create_product_async(message: Message, state: FSMContext, meta: dict, placeholder_msg_id: int):
    try:
        user_id = str(message.from_user.id)
        print(f"🏃‍♂️ 异步处理媒体: {meta}", flush=True)
        content_id = meta['content_id']
        table = meta['file_type']
        file_unique_id = meta['file_unique_id']
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="创建", callback_data=f"make_product:{content_id}:{table}:{file_unique_id}:{user_id}"),
                InlineKeyboardButton(text="取消", callback_data="cancel_product")
            ]
        ])
        caption_text = "检测到文件，是否需要创建为投稿？"

        if meta['thumb_file_unique_id'] is None and table == "video":
            
            print(f"✅ 没有缩略图，尝试提取预览图", flush=True)
            buf,pic = await Media.extract_preview_photo_buffer(message, prefer_cover=True, delete_sent=True)
            # photo_msg = await message.answer_photo(photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=caption_text, reply_markup=markup, parse_mode="HTML")
            
            photo_msg = await lz_var.bot.edit_message_media(
                chat_id=message.chat.id,
                message_id=placeholder_msg_id,
                media=InputMediaPhoto(media=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=caption_text, parse_mode="HTML"),
                reply_markup=markup
            )

          
            # ---------- 2) 归档复制（异步触发） ----------
            spawn_once(
                f"_process_update_default_preview_async:{message.message_id}",
                _process_update_default_preview_async(photo_msg,  user_id = user_id, content_id = content_id)
            )

            


            # await AnanBOTPool.update_sora_content_media_thumbnail(file_unique_id, thumb_file_unique_id, thumb_file_id, thumb_file_size, thumb_width, thumb_height)
            
        else:
            await lz_var.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=placeholder_msg_id,
                text=caption_text,
                reply_markup=markup
            )
        

    except Exception as e:
        print(f"❌ 异步处理媒体失败: {e}", flush=True)
  


async def _process_update_default_preview_async(message: Message, user_id: str, content_id: int):
    bot_username = await get_bot_username()
    photo_obj = message.photo[-1]
    thumb_file_id = photo_obj.file_id
    thumb_file_unique_id = photo_obj.file_unique_id
    thumb_file_size = photo_obj.file_size
    thumb_width = photo_obj.width
    thumb_height = photo_obj.height

    await AnanBOTPool.upsert_media( "photo", {
                "file_unique_id": thumb_file_unique_id,
                "file_size": thumb_file_size,
                "duration": 0,
                "width": thumb_width,
                "height": thumb_height,
                "create_time": datetime.now()
            })
   

    await AnanBOTPool.insert_file_extension("photo", thumb_file_unique_id, thumb_file_id, bot_username, user_id)


    await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unique_id, thumb_file_id, bot_username)
   

    print(f"{message}", flush=True)
    await lz_var.bot.copy_message(
        chat_id=lz_var.x_man_bot_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )
    pass


async def handle_media_bk(message: Message, state: FSMContext):
    file_type = message.content_type
    bot_username = await get_bot_username()
    print(f"收到媒体消息: {file_type} from {message.from_user.id if message.from_user else 'unknown'}", flush=True)
    user_id = str(message.from_user.id)

    timer = SegTimer(
        "handle_media",
        msg_id=message.message_id,
        from_user=message.from_user.id if message.from_user else None,
        chat_id=message.chat.id
    )

    try:
        if file_type == ContentType.PHOTO:
            photo = get_largest_photo(message.photo)
            
            file_name =  ""
            file_unique_id = photo.file_unique_id
            file_id = photo.file_id
            file_size = photo.file_size or 0
            duration = 0
            width = photo.width
            height = photo.height
            table = "photo"
        elif file_type == ContentType.VIDEO:
            file_unique_id = message.video.file_unique_id
            file_name = message.video.file_name or ""
            file_id = message.video.file_id
            file_size = message.video.file_size
            
            duration = message.video.duration
            width = message.video.width
            height = message.video.height
            table = "video"
        elif file_type == ContentType.DOCUMENT:
            file_unique_id = message.document.file_unique_id
            file_name = message.document.file_name or ""
            file_id = message.document.file_id
            file_size = message.document.file_size
            duration = 0
            width = 0
            height = 0
            table = "document"
        else:
            print(f"⚠️ 不支持的媒体类型: {file_type}", flush=True)
            return

        
    except Exception as e:
        print(f"❌ 处理媒体信息失败: {e}", flush=True)
        return await message.answer(f"⚠️ 处理媒体信息失败，请稍后重试。")

    product_i = await AnanBOTPool.get_product_info_by_fuid(file_unique_id)
    content_id = product_i.get("id") if product_i else None
    if content_id:



        pass
    else:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="创建", callback_data=f"make_product:{content_id}:{table}:{file_unique_id}:{user_id}"),
                InlineKeyboardButton(text="取消", callback_data="cancel_product")
            ]
        ])
        caption_text = "检测到文件，是否需要创建为投稿？"
        await message.answer(caption_text, reply_markup=markup)
        timer.lap("answer_create_prompt")

        row = await AnanBOTPool.insert_sora_content_media(file_unique_id, table, file_size, duration, user_id, file_id,bot_username)
        content_id = row["id"]
        timer.lap("insert_sora_content")

        product_info = await AnanBOTPool.get_existing_product(content_id)
        owner_user_id = str(product_info.get("owner_user_id")) if product_info else None
        timer.lap("get_existing_product")
    



    if product_info:
        if(owner_user_id!=user_id):
            timer.finish(ok=True, note="owner_mismatch")
            return await message.answer(f"⚠️ 这个资源已经有人投稿 ")
        
        if product_info.get("review_status") == 2:
            guild_row = await AnanBOTPool.check_guild_role(user_id,'manager')
            timer.lap("check_guild_role")
            if not guild_row:
                return await message.answer(f"⚠️ 这个资源正在审核状态")
        elif product_info.get("review_status") in (3, 4, 5):
            guild_row = await AnanBOTPool.check_guild_role(user_id,'owner')
            timer.lap("check_guild_role")
            if not guild_row:
                return await message.answer(f"⚠️ 这个资源正在上架中")

        print(f"✅ 已找到现有商品信息：{product_info}", flush=True)
        thumb_file_id, preview_text, preview_keyboard = await get_product_tpl(content_id)
        timer.lap("get_product_tpl")

        if row['thumb_file_unique_id'] is None and file_type == ContentType.VIDEO:
            print(f"✅ 没有缩略图，尝试提取预览图", flush=True)
            buf,pic = await Media.extract_preview_photo_buffer(message, prefer_cover=True, delete_sent=True)
            timer.lap("extract_preview")

            
            
            newsend = await message.answer_photo(photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
            timer.lap("send_preview_photo")

            photo_obj = newsend.photo[-1]
            thumb_file_id = photo_obj.file_id
            thumb_file_unique_id = photo_obj.file_unique_id
            thumb_file_size = photo_obj.file_size
            thumb_width = photo_obj.width
            thumb_height = photo_obj.height

            await AnanBOTPool.upsert_media( "photo", {
                "file_unique_id": thumb_file_unique_id,
                "file_size": thumb_file_size,
                "duration": 0,
                "width": thumb_width,
                "height": thumb_height,
                "create_time": datetime.now()
            })
            timer.lap("upsert_thumb_media")

            await AnanBOTPool.insert_file_extension("photo", thumb_file_unique_id, thumb_file_id, bot_username, user_id)
            timer.lap("insert_thumb_extension")

            await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unique_id, thumb_file_id, bot_username)
            timer.lap("upsert_product_thumb")

            print(f"{newsend}", flush=True)
            await lz_var.bot.copy_message(
                chat_id=lz_var.x_man_bot_id,
                from_chat_id=newsend.chat.id,
                message_id=newsend.message_id
            )
            timer.lap("archive_copy_preview")
        else:
            newsend = await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
            timer.lap("answer_preview")

            await update_product_preview(content_id, thumb_file_id, state , newsend)
            timer.lap("update_product_preview")

    else:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="创建", callback_data=f"make_product:{content_id}:{table}:{file_unique_id}:{user_id}"),
                InlineKeyboardButton(text="取消", callback_data="cancel_product")
            ]
        ])
        caption_text = "检测到文件，是否需要创建为投稿？"

        if row['thumb_file_unique_id'] is None and file_type == ContentType.VIDEO:
            buf,pic = await Media.extract_preview_photo_buffer(message, prefer_cover=True, delete_sent=True)
            timer.lap("extract_preview_new")

            sent = await message.answer_photo(photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=caption_text, reply_markup=markup, parse_mode="HTML")
            timer.lap("send_preview_photo_new")

            photo_obj = sent.photo[-1]
            thumb_file_id = photo_obj.file_id
            thumb_file_unique_id = photo_obj.file_unique_id
            thumb_file_size = photo_obj.file_size
            thumb_width = photo_obj.width
            thumb_height = photo_obj.height

            await AnanBOTPool.upsert_media( "photo", {
                "file_unique_id": thumb_file_unique_id,
                "file_size": thumb_file_size,
                "duration": 0,
                "width": thumb_width,
                "height": thumb_height,
                "create_time": datetime.now()
            })
            timer.lap("upsert_thumb_media_new")

            await AnanBOTPool.insert_file_extension("photo", thumb_file_unique_id, thumb_file_id, bot_username, user_id)
            timer.lap("insert_thumb_extension_new")

            await AnanBOTPool.upsert_product_thumb(content_id, thumb_file_unique_id, thumb_file_id, bot_username)
            timer.lap("upsert_product_thumb_new")

            print(f"✅ 已取得预览图\nt_file_id = {file_id}\nt_file_unique_id = {thumb_file_unique_id}\nchat_id = {sent.chat.id}", flush=True)

            await lz_var.bot.copy_message(
                chat_id=lz_var.x_man_bot_id,
                from_chat_id=sent.chat.id,
                message_id=sent.message_id
            )
            timer.lap("archive_copy_preview_new", to=lz_var.x_man_bot_id)

        else:
            await message.answer(caption_text, reply_markup=markup)
            timer.lap("answer_create_prompt")




    '''    # ---------- 异步触发） ----------'''
    spawn_once(
        f"upsert_media:{table}",
        AnanBOTPool.upsert_media(table, {
            "file_unique_id": file_unique_id,
            "file_size": file_size,
            "duration": duration,
            "width": width,
            "height": height,
            "file_name": file_name,
            "create_time": datetime.now()
        })
    )
    timer.lap("db_upsert_media")

    spawn_once(f"insert_file_extension:{table}:{file_unique_id}",
    AnanBOTPool.insert_file_extension(table, file_unique_id, file_id, bot_username, user_id))
    timer.lap("insert_file_extension")


    # ---------- 2) 归档复制（异步触发） ----------
    spawn_once(
        f"copy_message:{message.message_id}",
        lz_var.bot.copy_message(
            chat_id=lz_var.x_man_bot_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )
    )



@dp.callback_query(F.data == "cancel_product")
async def handle_cancel_product(callback: CallbackQuery):
    try:
        await bot.delete_message(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id
        )
    except Exception as e:
        # 如果消息已经不存在 / 没权限删，就忽略
        print(f"⚠️ 删除消息失败: {e}", flush=True)

    # 可选：给个反馈（不会冒泡出错）
    try:
        await callback.answer("❎ 已取消", show_alert=False)
    except Exception:
        pass



async def update_product_preview(content_id, thumb_file_id, state, message: Message | None = None, *,
                                 chat_id: int | None = None, message_id: int | None = None):
    # 允许两种调用方式：传 message 或显式传 chat_id/message_id
    if message:
        chat_id = message.chat.id
        message_id = message.message_id
    if chat_id is None or message_id is None:
        print("⚠️ update_product_preview 缺少 chat_id/message_id，跳过")
        return

    cached = get_cached_product(content_id) or {}
    
    cached_thumb_unique = cached.get('thumb_unique_id', "")

    print(f"thumb_file_id={thumb_file_id}, cached_thumb_unique={cached_thumb_unique}", flush=True)

    # 只有在用默认图且我们已知 thumb_unique_id 时，才尝试异步更新真实图
    if thumb_file_id == DEFAULT_THUMB_FILE_ID and cached_thumb_unique:
        async def update_preview_if_arrived():
            try:
                new_file_id = await Media.fetch_file_by_file_id_from_x(state, cached_thumb_unique, 30)
                if new_file_id:
                    print(f"[预览图更新] 已获取 thumb_file_id: {new_file_id} - {cached_thumb_unique}")
                    bot_uname = await get_bot_username()
                    await AnanBOTPool.upsert_product_thumb(int(content_id), cached_thumb_unique, new_file_id, bot_uname)

                    # 失效缓存
                    invalidate_cached_product(content_id)



                    # 重新渲染并编辑“同一条消息”
                    fresh_thumb, fresh_text, fresh_kb = await get_product_tpl(content_id)
                    fresh_text = fresh_text + "\n\n（预览图已更新）"

                    orig_kb = getattr(message, "reply_markup", None) if message else None
                    use_kb = orig_kb or fresh_kb

                    try:
                        await lz_var.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=message_id,
                            media=InputMediaPhoto(media=fresh_thumb, caption=fresh_text, parse_mode="HTML"),
                            reply_markup=use_kb
                        )
                    except Exception as e:
                        print(f"⚠️ 更新预览图失败：{e}", flush=True)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"⚠️ 异步更新预览图异常：{e}", flush=True)

        asyncio.create_task(update_preview_if_arrived())

import time
from typing import Optional

def _now() -> float:
    return time.time()

def _prune_expired() -> None:
    """Remove expired items (cheap O(n)); safe to call opportunistically."""
    now = _now()
    expired = [cid for cid, ts in product_info_cache_ts.items()
               if now - ts > PRODUCT_INFO_CACHE_TTL]
    if not expired:
        return
    for cid in expired:
        product_info_cache.pop(cid, None)
        product_info_cache_ts.pop(cid, None)

def get_cached_product(content_id: int | str) -> Optional[dict]:
    """Read with TTL; return None if missing/expired."""
    try:
        cid = int(content_id)
    except Exception:
        return None
    _prune_expired()
    ts = product_info_cache_ts.get(cid)
    if ts is None:
        return None
    # (If prune wasn’t called, double-check TTL here)
    if _now() - ts > PRODUCT_INFO_CACHE_TTL:
        product_info_cache.pop(cid, None)
        product_info_cache_ts.pop(cid, None)
        return None
    return product_info_cache.get(cid)

def set_cached_product(content_id: int | str, payload: dict) -> None:
    """Write & enforce size limit; payload must contain render fields."""
    try:
        cid = int(content_id)
    except Exception:
        return
    if not isinstance(payload, dict):
        return

    # Opportunistically prune expired first to free space
    _prune_expired()

    # Enforce MAX size (loop in case MAX shrank at runtime)
    while len(product_info_cache) >= PRODUCT_INFO_CACHE_MAX and product_info_cache_ts:
        # Evict the oldest by timestamp
        oldest_cid = min(product_info_cache_ts, key=product_info_cache_ts.get)
        product_info_cache.pop(oldest_cid, None)
        product_info_cache_ts.pop(oldest_cid, None)

    product_info_cache[cid] = payload
    product_info_cache_ts[cid] = _now()

def invalidate_cached_product(content_id: int | str) -> None:
    """Invalidate entry (safe conversion + dual-map delete)."""
    try:
        cid = int(content_id)
    except Exception:
        print("⚠️ invalidate_cached_product 参数错误", flush=True)
        return
    product_info_cache.pop(cid, None)
    product_info_cache_ts.pop(cid, None)

async def get_bot_username():
    if lz_var.bot_username:
        return lz_var.bot_username
    else:
        bot_info = await bot.get_me()
        bot_username = bot_info.username
        lz_var.bot_username = bot_username
        return lz_var.bot_username
        

async def set_default_thumb_file_id():
    global DEFAULT_THUMB_FILE_ID
    first = lz_var.default_thumb_unique_file_ids[0] if lz_var.default_thumb_unique_file_ids else None
    if first:

        bot_username = await get_bot_username()
       
        DEFAULT_THUMB_FILE_ID = await AnanBOTPool.get_default_preview_thumb_file_id(bot_username, first)
       
    else:
        print("⚠️ 未配置任何默认缩略图", flush=True)

async def keep_alive_ping():
    url = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if BOT_MODE == "webhook" else f"{WEBHOOK_HOST}/"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    print(f"🌐 Keep-alive ping {url} status {resp.status}")
        except Exception as e:
            print(f"⚠️ Keep-alive ping failed: {e}")
        await asyncio.sleep(300)  # 每 5 分鐘 ping 一次

async def main():
    logging.basicConfig(level=logging.INFO)
    global bot_username
    bot_username = await get_bot_username()
    print(f"🤖 当前 bot 用户名：@{bot_username}")
    

   # ✅ 初始化 MySQL 连接池
    await AnanBOTPool.init_pool()

    await AnanBOTPool.sync_bid_product()

    await set_default_thumb_file_id()
    print(f"✅ 默认缩略图 file_id：{DEFAULT_THUMB_FILE_ID}")


    if BOT_MODE == "webhook":
        # dp.startup.register(on_startup)
        print("🚀 啟動 Webhook 模式")

        app = web.Application()
        app.router.add_get("/", health)  # ✅ 健康检查路由

        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/")
        setup_application(app, dp, bot=bot)

        task_keep_alive = asyncio.create_task(keep_alive_ping())

        # ✅ Render 环境用 PORT，否则本地用 8080
        await web._run_app(app, host="0.0.0.0", port=8080)
    else:
        print("【Aiogram】Bot（纯 Bot-API） 已启动，监听私聊＋群组媒体。",flush=True)
        await dp.start_polling(bot)  # Aiogram 轮询


   

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
