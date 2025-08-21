import os
import logging
from datetime import datetime, timedelta
import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto, PhotoSize
from aiogram.enums import ContentType
from aiogram.filters import Command
from aiomysql import create_pool
from dotenv import load_dotenv
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, PhotoSize

from ananbot_utils import AnanBOTPool  # ✅ 修改点：改为统一导入类
from utils.media_utils import Media  
from ananbot_config import BOT_TOKEN
import lz_var
from lz_config import AES_KEY, ENVIRONMENT

from utils.aes_crypto import AESCrypto

bot = Bot(token=BOT_TOKEN)
lz_var.bot = bot

# 全局变量缓存 bot username
media_upload_tasks: dict[tuple[int, int], asyncio.Task] = {}

# 全局缓存延迟刷新任务
tag_refresh_tasks: dict[tuple[int, str], asyncio.Task] = {}

# ✅ 匿名选择的超时任务缓存
anonymous_choice_tasks: dict[tuple[int, int], asyncio.Task] = {}

# 在模块顶部添加
has_prompt_sent: dict[tuple[int, int], bool] = {}


# product_info 缓存，最多缓存 100 个，缓存时间 30 秒
product_info_cache: dict[int, dict] = {}
product_info_cache_ts: dict[int, float] = {}
PRODUCT_INFO_CACHE_TTL = 60  # 秒


DEFAULT_THUMB_FILE_ID = "AgACAgEAAxkBAAPIaHHqdjJqYXWcWVoNoAJFGFBwBnUAAjGtMRuIOEBF8t8-OXqk4uwBAAMCAAN5AAM2BA"




bot_username = None
dp = Dispatcher(storage=MemoryStorage())

class ProductPreviewFSM(StatesGroup):
    waiting_for_preview_photo = State(state="product_preview:waiting_for_preview_photo")
    waiting_for_price_input = State(state="product_preview:waiting_for_price_input")
    waiting_for_collection_media = State(state="product_preview:waiting_for_collection_media")
    waiting_for_removetag_source = State(state="product_preview:waiting_for_removetag_source")  # ✅ 新增
    waiting_for_content_input = State(state="product_preview:waiting_for_content_input")  # ✅ 新增
    waiting_for_thumb_reply = State(state="product_preview:waiting_for_thumb_reply")  # ✅ 新增
    waiting_for_x_media = State()
    waiting_for_anonymous_choice = State(state="product_preview:waiting_for_anonymous_choice")

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

    user_id = str(message.from_user.id) if message.from_user else None
    
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



def get_largest_photo(photo_sizes):
    return max(photo_sizes, key=lambda p: p.width * p.height)


async def get_bot_username():
    global bot_username
    if not bot_username:
        bot_info = await bot.get_me()
        bot_username = bot_info.username
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
    collect_list_text = ''
    collect_cont_list_text = ''
    list_text = ''
    bot_username = await get_bot_username()
    results = []

    results = await AnanBOTPool.get_collect_list(content_id, bot_username)
    video_count = 0
    document_count = 0
    photo_count = 0
    

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

    


    if video_count > 0:
        collect_cont_list_text += f"🎬 x{video_count} 　"
    if document_count > 0:
        collect_cont_list_text += f"📄 x{document_count} 　"
    if photo_count > 0:
        collect_cont_list_text += f"🖼️ x{photo_count} \n"

    if collect_list_text:
        list_text += "\n📦 文件列表：\n" + collect_list_text
        list_text += "\n📊 本合集包含：" + collect_cont_list_text


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
    
    thumb_file_id,preview_text,preview_keyboard = await get_product_info(content_id)
    await callback_query.message.delete()
    new_msg = await callback_query.message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
    await update_product_preview(content_id, thumb_file_id, state, new_msg)

async def get_product_info(content_id: int) -> tuple[str, str, InlineKeyboardMarkup]:
    now = datetime.now().timestamp()
    cached = product_info_cache.get(content_id)
    cached_ts = product_info_cache_ts.get(content_id, 0)
    
    
    if cached is not None and cached and (now - cached_ts) < PRODUCT_INFO_CACHE_TTL:
        return cached["thumb_file_id"], cached["preview_text"], cached["preview_keyboard"]

   

    # 没有缓存或过期，调用原函数重新生成
    thumb_file_id, preview_text, preview_keyboard = await get_product_info_action(content_id)

    return thumb_file_id, preview_text, preview_keyboard


async def get_product_info_action(content_id):
    
    # 查询是否已有同 source_id 的 product
    # 查找缩图 file_id
    
    bot_username = await get_bot_username()
    product_info = await AnanBOTPool.search_sora_content_by_id(content_id, bot_username)
    thumb_file_id = product_info.get("m_thumb_file_id")
    thumb_unique_id = product_info.get("thumb_file_unique_id")
    file_unqiue_id = product_info.get('source_id')
    bid_status = product_info.get('bid_status')
    review_status = product_info.get('review_status')
    anonymous_mode = product_info.get('anonymous_mode',1)
    
    if anonymous_mode == 1:
        anonymous_button_text = "🙈 发表模式: 匿名发表"
    elif anonymous_mode == 3:
        anonymous_button_text = "🐵 发表模式: 公开上传者"

    if not thumb_file_id:
        #默认缩略图
        thumb_file_id = DEFAULT_THUMB_FILE_ID
        # 以下程序异步处理
            # 如果没有缩略图，传送 thumb_unique_id 给 @p_14707422896
            # 等待 @p_14707422896 的回应，应回复媒体
            # 若有回覆媒体，则回传其 file_id

    if product_info['fee'] is None:
        product_info['fee'] = 68


    content_list = await get_list(content_id)

    preview_text = f"""文件商品
- 数据库ID:{content_id} {file_unqiue_id}

{shorten_content(product_info['content'],300)}

<i>{product_info['tag']}</i>

{content_list}
"""

    print(f"{review_status}")
    if review_status < 3:
    
        # 按钮列表构建
        buttons = [
            [
                InlineKeyboardButton(text="📝 设置内容", callback_data=f"set_content:{content_id}"),
                InlineKeyboardButton(text="📷 设置预览", callback_data=f"set_preview:{content_id}")
            ]
        ]

        if product_info['file_type'] in ['document', 'collection']:
            buttons.append([
                InlineKeyboardButton(text="🔒 设置密码", callback_data=f"set_password:{content_id}")
            ])

        if review_status == 0 or review_status == 1:
            buttons.extend([
                [
                    InlineKeyboardButton(text="🏷️ 设置标签", callback_data=f"tag_full:{content_id}"),
                    InlineKeyboardButton(text=f"💎 设置积分 ({product_info['fee']})", callback_data=f"set_price:{content_id}")
                ],
                [InlineKeyboardButton(text=f"{anonymous_button_text}", callback_data=f"toggle_anonymous:{content_id}")],
                [InlineKeyboardButton(text="➕ 添加资源", callback_data=f"add_items:{content_id}")],
                [
                    InlineKeyboardButton(text="📬 提交投稿", callback_data=f"submit_product:{content_id}"),
                    InlineKeyboardButton(text="❌ 取消投稿", callback_data=f"cancel_publish:{content_id}")
                ]
            ])

        elif review_status == 2:
            
            buttons.extend([
                [
                    InlineKeyboardButton(text="🏷️ 设置标签", callback_data=f"tag_full:{content_id}")
                ],
                [
                    InlineKeyboardButton(text="✅ 通过审核", callback_data=f"approve_product:{content_id}:3"),
                    InlineKeyboardButton(text="❌ 拒绝投稿", callback_data=f"approve_product:{content_id}:1")
                ]
            ])
            # 待审核
    if review_status == 3:
        buttons = [[InlineKeyboardButton(text="通过审核,等待上架", callback_data=f"none")]]
    if review_status == 4:
        buttons = [[InlineKeyboardButton(text="通过审核,但上架失败", callback_data=f"none")]]
    if review_status == 9:
        buttons = [[InlineKeyboardButton(text="通过审核,已上架", callback_data=f"none")]]
        
       

    preview_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)


    # 写入缓存
    product_info_cache[content_id] = {
        "thumb_file_id": thumb_file_id,
        "thumb_unique_id": thumb_unique_id,
        "preview_text": preview_text,
        "preview_keyboard": preview_keyboard
    }
    product_info_cache_ts[content_id] = datetime.now().timestamp()

    


    return thumb_file_id, preview_text, preview_keyboard


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
    file_unique_id = await AnanBOTPool.get_file_unique_id_by_content_id(content_id)

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
    file_unique_id = await AnanBOTPool.get_file_unique_id_by_content_id(content_id)

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
    task_key = (user_id, content_id)

    # 如果已有延迟任务，取消旧的
    old_task = tag_refresh_tasks.get(task_key)
    if old_task and not old_task.done():
        old_task.cancel()

    # 创建新的延迟刷新任务
    async def delayed_refresh():
        try:
            await asyncio.sleep(0.7)
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
    file_unique_id = await AnanBOTPool.get_file_unique_id_by_content_id(content_id)
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
    task_key = (user_id, str(content_id))
    old_task = tag_refresh_tasks.pop(task_key, None)
    if old_task and not old_task.done():
        old_task.cancel()

    # ✅ 重置缓存（删除）
    product_info_cache.pop(content_id, None)
    product_info_cache_ts.pop(content_id, None)

    # 5) 回到商品卡片
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    list = await get_list(content_id)  # 获取合集列表，更新状态
    caption_text = f"{list}\n\n📥 请直接传送资源"
    
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

@dp.message(F.chat.type == "private", F.content_type.in_({
    ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT
}), ProductPreviewFSM.waiting_for_collection_media)
async def receive_collection_media(message: Message, state: FSMContext):
    data = await state.get_data()
    content_id = int(data["content_id"])
    chat_id = data["chat_id"]
    message_id = data["message_id"]


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
    user_id = str(message.from_user.id)
    file_size = getattr(file, "file_size", 0)
    duration = getattr(file, "duration", 0)
    width = getattr(file, "width", 0)
    height = getattr(file, "height", 0)

    await lz_var.bot.copy_message(
        chat_id=lz_var.x_man_bot_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )


    await AnanBOTPool.upsert_media(file_type, {
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "duration": duration,
        "width": width,
        "height": height,
        "create_time": datetime.now()
    })
    bot_username = await get_bot_username()
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

    await AnanBOTPool.update_product_file_type(content_id, "collection")

    print(f"添加资源：{file_type} {file_unique_id} {file_id}", flush=True)

    # --- 管理提示任务 ---
    key = (user_id, content_id)
    has_prompt_sent[key] = False

    # 若已有旧任务，取消
    old_task = media_upload_tasks.get(key)
    if old_task and not old_task.done():
        old_task.cancel()

    # await message.delete()

    # 创建新任务（3秒内无动作才触发）
    async def delayed_finish_prompt():
        try:
            await asyncio.sleep(3)
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
                    await state.clear()  # 👈 强制清除旧的 preview 状态
                    await state.set_state(ProductPreviewFSM.waiting_for_collection_media)
                    await state.set_data({
                        "content_id": content_id,
                        "chat_id": send_message.chat.id,
                        "message_id": send_message.message_id,
                        "last_button_ts": datetime.now().timestamp()
                    })
                    await bot.delete_message(chat_id, message_id)

                except Exception:
                    pass

                
        except asyncio.CancelledError:
            pass


    # 存入新的 task
    media_upload_tasks[key] = await asyncio.create_task(delayed_finish_prompt())



@dp.callback_query(F.data.startswith("done_add_items:"))
async def done_add_items(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id

    data = await state.get_data()
    chat_id = data["chat_id"]
    message_id = data["message_id"]

    try:
        await state.clear()
    except Exception:
        pass



    # 清除任务
    key = (user_id, content_id)
    task = media_upload_tasks.pop(key, None)
    if task and not task.done():
        task.cancel()
    has_prompt_sent.pop(key, None)  # ✅ 清除标记

    # 返回商品菜单

    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
        reply_markup=preview_keyboard
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

    caption = f"当前价格为 {product_info['price']}\n\n请在 1 分钟内输入商品价格(1-99)"
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
    await asyncio.sleep(60)
    current_state = await state.get_state()
    if current_state == ProductPreviewFSM.waiting_for_price_input:
        await state.clear()
        thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    if not price_str.isdigit() or not (1 <= int(price_str) <= 99):
        # await message.answer("❌ 请输入 1~99 的整数作为价格")
        # 回到菜单
        
        callback_id = data.get("callback_id")
        if callback_id:
            await bot.answer_callback_query(callback_query_id=callback_id, text=f"❌ 请输入 1~99 的整数作为价格", show_alert=True)
        else:
            state.clear()
            thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)

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
    product_info_cache.pop(content_id, None)
    product_info_cache_ts.pop(content_id, None)

    # thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    # await bot.edit_message_media(
    #     chat_id=chat_id,
    #     message_id=message_id,
    #     media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
    #     reply_markup=preview_keyboard
    # )

    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text,parse_mode="HTML"),
        reply_markup=preview_keyboard
        
    )


#
# 设置预览图
#
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
    caption_text = "📸 请在 1 分钟内发送预览图（图片格式）"
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
    await asyncio.sleep(60)
    current_state = await state.get_state()
    if current_state == ProductPreviewFSM.waiting_for_preview_photo:
        try:
            await state.clear()
        except Exception as e:
            print(f"⚠️ 清除状态失败：{e}", flush=True)

        try:
            thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
        thumb_file_id,preview_text,preview_keyboard = await get_product_info(content_id)
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
        

@dp.callback_query(F.data.startswith("auto_update_thumb:"))
async def handle_auto_update_thumb(callback_query: CallbackQuery, state: FSMContext):
    content_id = int(callback_query.data.split(":")[1])
    print(f"▶️ 开始自动处理预览图", flush=True)
    try:
        # Step 1: 取得 sora_content.source_id
        row = await AnanBOTPool.get_sora_content_by_id(content_id)
        if not row or not row.get("source_id"):
            return await callback_query.answer("...⚠️ 无法取得 source_id", show_alert=True)

        source_id = row["source_id"]
        print(f"...🔍 取得 source_id: {source_id} for content_id: {content_id}", flush=True)
        bot_username = await get_bot_username()
        
        # Step 2: 取得 thumb_file_unique_id
        thumb_row = await AnanBOTPool.get_bid_thumbnail_by_source_id(source_id)
        print(f"...🔍 取得缩图信息: {thumb_row} for source_id: {source_id}", flush=True)
        # 遍寻 thumb_row
        thumb_file_unique_id = None
        thumb_file_id = None
        for row in thumb_row:
            thumb_file_unique_id = row["thumb_file_unique_id"]
            print(f"...🔍 取得缩图 unique_id: {thumb_file_unique_id} for source_id: {source_id}", flush=True)
            if row['bot_name'] == bot_username:   
                thumb_file_id = row["thumb_file_id"]

        if thumb_file_unique_id is None and thumb_file_id is None:
            print(f"...⚠️ 找不到对应的缩图 for source_id: {source_id}", flush=True)
            return await callback_query.answer("⚠️ 目前还没有这个资源的缩略图", show_alert=True)

        elif thumb_file_unique_id and thumb_file_id is None:
        # Step 4: 通知处理 bot 生成缩图（或触发缓存）
            storage = state.storage  # 与全局 Dispatcher 共享的同一个 storage

            x_uid = lz_var.x_man_bot_id          # = 7793315433
            x_chat_id = x_uid                     # 私聊里 chat_id == user_id
            key = StorageKey(bot_id=lz_var.bot.id, chat_id=x_chat_id, user_id=x_uid)

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
                    # fresh_thumb, fresh_text, fresh_kb = await get_product_info(content_id)
                    # await lz_var.bot.edit_message_media(
                    #     chat_id=callback_query.message.chat.id,
                    #     message_id=callback_query.message.message_id,
                    #     media=InputMediaPhoto(media=x_file_id, caption=fresh_text, parse_mode="HTML"),
                    #     reply_markup=fresh_kb,
                    # )
                await asyncio.sleep(0.5)


        if thumb_file_unique_id and thumb_file_id:
            
            try:
                # thumb_file_unique_id = thumb_row["thumb_file_unique_id"]
                # thumb_file_id = thumb_row["thumb_file_id"]
                print(f"...🔍 取得分镜图信息: {thumb_file_unique_id}, {thumb_file_id} for source_id: {source_id}", flush=True)

                # Step 3: 更新 sora_content 缩图字段
                await AnanBOTPool.update_product_thumb(content_id, thumb_file_unique_id,thumb_file_id, bot_username)

                # Step 4: 更新 update_bid_thumbnail
                await AnanBOTPool.update_bid_thumbnail(source_id, thumb_file_unique_id, thumb_file_id, bot_username)

                # 确保缓存存在
                if content_id in product_info_cache:
                    product_info_cache[content_id]["thumb_unique_id"] = thumb_file_unique_id
                    product_info_cache[content_id]["thumb_file_id"] = thumb_file_id
                else:
                    # 若没缓存，则重新生成一次缓存
                    await get_product_info(content_id)
                print(f"...✅ 更新 content_id: {content_id} 的缩图为 {thumb_file_unique_id}", flush=True)



                await callback_query.message.edit_media(
                    media=InputMediaPhoto(media=thumb_file_id, caption=product_info_cache[content_id]["preview_text"], parse_mode="HTML"),
                    reply_markup=product_info_cache[content_id]["preview_keyboard"]
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
        product_info_cache.pop(content_id, None)
        product_info_cache_ts.pop(content_id, None)
    except Exception:
        pass

    thumb_file_id, preview_text, _ = await get_product_info(content_id)
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

    await callback_query.answer("✅ 已提交审核", show_alert=False)

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
        product_info_cache.pop(content_id, None)
        product_info_cache_ts.pop(content_id, None)
    except Exception:
        pass

    # 重新取卡片内容并追加“已取消投稿”
    thumb_file_id, preview_text, _ = await get_product_info(content_id)
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

@dp.callback_query(F.data.startswith("approve_product:"))
async def handle_approve_product(callback_query: CallbackQuery, state: FSMContext):
    try:
        content_id = int(callback_query.data.split(":")[1])
        review_status = int(callback_query.data.split(":")[2])
    except Exception:
        return await callback_query.answer("⚠️ 提交失败：content_id 异常", show_alert=True)

    # 1) 更新 bid_status=1
    try:
        affected = await AnanBOTPool.set_product_review_status(content_id, review_status)
        if affected == 0:
            return await callback_query.answer("⚠️ 未找到对应商品，审核失败", show_alert=True)
        if review_status == 2:
            affected2 = await AnanBOTPool.set_product_review_status(content_id, 1)
            print(f"🔍 审核拒绝，重置 bid_status =1 : {affected2}", flush=True)
    except Exception as e:
        logging.exception(f"审核失败: {e}")
        return await callback_query.answer("⚠️ 审核失败，请稍后重试", show_alert=True)

    # 2) 隐藏按钮并显示“已送审请耐心等候”
    try:
        # 清理缓存，确保后续重新渲染
        product_info_cache.pop(content_id, None)
        product_info_cache_ts.pop(content_id, None)
    except Exception:
        pass

    if review_status == 3:
        await callback_query.answer("✅ 已通过审核", show_alert=True)
        await AnanBOTPool.refine_product_content(content_id)
        buttons = [[InlineKeyboardButton(text="✅ 已通过审核", callback_data=f"none")]]
        await AnanBOTPool.set_product_guild(content_id)
    elif review_status == 1:
        await callback_query.answer("❌ 已拒绝审核", show_alert=True)
        buttons = [[InlineKeyboardButton(text="❌ 已拒绝审核", callback_data=f"none")]]

    thumb_file_id, preview_text, _ = await get_product_info(content_id)
    
    preview_keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)


    try:
        await callback_query.message.edit_media(
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard  # 👈 关键：隐藏所有按钮
        )
    except Exception as e:
        logging.exception(f"编辑媒体失败: {e}")
        # 兜底：至少把按钮清掉
        try:
            await callback_query.message.edit_reply_markup(reply_markup=preview_keyboard)
        except Exception:
            pass



@dp.message(F.chat.type == "private", F.content_type == ContentType.PHOTO, ProductPreviewFSM.waiting_for_preview_photo)
async def receive_preview_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    content_id = data["content_id"]
    chat_id = data["chat_id"]
    message_id = data["message_id"]

    photo = get_largest_photo(message.photo)
    file_unique_id = photo.file_unique_id
    file_id = photo.file_id
    width = photo.width
    height = photo.height
    file_size = photo.file_size or 0
    user_id = str(message.from_user.id)

    await lz_var.bot.copy_message(
        chat_id=lz_var.x_man_bot_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )
    
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
    await AnanBOTPool.update_product_thumb(content_id, file_unique_id,file_id, bot_username)
    # Step 4: 更新 update_bid_thumbnail

    row = await AnanBOTPool.get_sora_content_by_id(content_id)
    if row and row.get("source_id"):
        source_id = row["source_id"]
        await AnanBOTPool.update_bid_thumbnail(source_id, file_unique_id, file_id, bot_username)

    product_info_cache[content_id]["thumb_unique_id"] = file_unique_id
    product_info_cache[content_id]["thumb_file_id"] = file_id


    # 编辑原消息，更新为商品卡片
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    try:
        await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text,parse_mode="HTML"),
            reply_markup=preview_keyboard,
            
        )
    except Exception as e:
        print(f"⚠️ 更新预览图失败B：{e}", flush=True)

    # await message.answer("✅ 预览图已成功设置！")
    await message.delete()
    try:
        await state.clear()
    except Exception as e:
        print(f"⚠️ 清除状态失败：{e}", flush=True)

############
#  content     
############
@dp.callback_query(F.data.startswith("set_content:"))
async def handle_set_content(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]

    product_info = await AnanBOTPool.get_existing_product(content_id)

    caption = f"<code>{product_info['content']}</code>  (点选复制) \r\n\r\n📘 请输入完整的内容介绍（文本形式）"
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
        "message_id": callback_query.message.message_id
    })

    # 60秒超时处理
    asyncio.create_task(clear_content_input_timeout(state, content_id, callback_query.message.chat.id, callback_query.message.message_id))

async def clear_content_input_timeout(state: FSMContext, content_id: str, chat_id: int, message_id: int):
    await asyncio.sleep(60)
    if await state.get_state() == ProductPreviewFSM.waiting_for_content_input:
        await state.clear()
        thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    content_text = message.text.strip()
    data = await state.get_data()
    content_id = data["content_id"]
    chat_id = data["chat_id"]
    message_id = data["message_id"]

    await AnanBOTPool.update_product_content(content_id, content_text)
    await message.delete()
    await state.clear()

    if content_id in product_info_cache:
        # 清除旧的缓存
        product_info_cache[content_id]=None
        


    print(f"✅ 已更新内容为: {content_text}", flush=True)
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    try:
        await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text, parse_mode="HTML"),
            reply_markup=preview_keyboard
        )
    except Exception as e:
        print(f"⚠️ 更新内容失败：{e}", flush=True)

@dp.callback_query(F.data.startswith("cancel_set_content:"))
async def cancel_set_content(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    await state.clear()
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
            await asyncio.sleep(60)
            if await state.get_state() == ProductPreviewFSM.waiting_for_anonymous_choice:
                await state.clear()
                # 返回商品页
                thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    product_info_cache.pop(content_id, None)
    product_info_cache_ts.pop(content_id, None)

    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
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
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    
    newsend = await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
    # await message.answer(content_id)


@dp.message(Command("start"))
async def handle_search(message: Message):
    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        param = args[1].strip()
        parts = param.split("_")    

    if parts[0] == "r":
        try:
            aes = AESCrypto(AES_KEY)
            kind_index = parts[1]
            encoded = "_".join(parts[2:])  # 剩下的部分重新用 _ 拼接
            content_id_str = aes.aes_decode(encoded)
            decode_row = content_id_str.split("|")
            
            content_id = int(decode_row[1])
            print(f"解码内容: {content_id}", flush=True)
            thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
            await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
            


        except Exception as e:
            print(f"⚠️ 解码失败: {e}", flush=True)
            pass


    


#
# 共用
#
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
                'text': f'你在{dt.strftime('%Y-%m-%d %H:%M:%S')}贴的标签不对，已被扣信用分'
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
    file_type = message.content_type
    user_id = str(message.from_user.id)

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
        return
    
    await lz_var.bot.copy_message(
        chat_id=lz_var.x_man_bot_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id
    )


    # 正常媒体接收处理
   
#     published, kc_id = await AnanBOTPool.is_media_published(table, file_unique_id)
#     if published:
#         await message.answer(f"""此媒体已发布过
# file_unique_id: {file_unique_id}
# kc_id: {kc_id or '无'}""")
#     else:
#         await message.answer("此媒体未发布过")


    # ids = await Media.extract_preview_photo_ids(message, prefer_cover=True, delete_sent=True)
    # if not ids:
    #     await message.answer("⚠️ 这个媒体没有可用的缩略图/封面")
    #     return
    # file_id, file_unique_id = ids
    # # 这里你可以直接入库 / 复用
    # await message.answer(f"✅ 已取得预览图\nfile_id = {file_id}\nfile_unique_id = {file_unique_id}")
    print(f"接收到媒体：{file_type} {file_unique_id} {file_id}", flush=True)

    await AnanBOTPool.upsert_media(table, {
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "duration": duration,
        "width": width,
        "height": height,
        "file_name": file_name,
        "create_time": datetime.now()
    })
    bot_username = await get_bot_username()
    
    print(f"✅ 已入库媒体信息：{table} {file_unique_id} {file_id}", flush=True)
    await AnanBOTPool.insert_file_extension(table, file_unique_id, file_id, bot_username, user_id)

    print(f"✅ 已入库文件扩展信息：{table} {file_unique_id} {file_id}", flush=True)
    row = await AnanBOTPool.insert_sora_content_media(file_unique_id, table, file_size, duration, user_id, file_id,bot_username)
    content_id = row["id"]

    print(f"✅ 已入库Sora内容信息：{content_id}", flush=True)
    product_info = await AnanBOTPool.get_existing_product(content_id)
    if product_info:
        print(f"✅ 已找到现有商品信息：{product_info}", flush=True)
        thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)

        if row['thumb_file_unique_id'] is None and file_type == ContentType.VIDEO:
            print(f"✅ 没有缩略图，尝试提取预览图", flush=True)
            buf,pic = await Media.extract_preview_photo_buffer(message, prefer_cover=True, delete_sent=True)
            newsend = await message.answer_photo(photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
            
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

            await AnanBOTPool.insert_file_extension("photo", thumb_file_unique_id, thumb_file_id, bot_username, user_id)

            await AnanBOTPool.update_product_thumb(content_id, thumb_file_unique_id, thumb_file_id, bot_username)

            print(f"{newsend}", flush=True)
            await lz_var.bot.copy_message(
                chat_id=lz_var.x_man_bot_id,
                from_chat_id=newsend.chat.id,
                message_id=newsend.message_id
            )
            

           
        else:
            newsend = await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard, parse_mode="HTML")
            await update_product_preview(content_id, thumb_file_id, state , newsend)

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
            sent = await message.answer_photo(photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"), caption=caption_text, reply_markup=markup, parse_mode="HTML")

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

            await AnanBOTPool.insert_file_extension("photo", thumb_file_unique_id, thumb_file_id, bot_username, user_id)

            await AnanBOTPool.update_product_thumb(content_id, thumb_file_unique_id, thumb_file_id, bot_username)

            print(f"✅ 已取得预览图\nt_file_id = {file_id}\nt_file_unique_id = {thumb_file_unique_id}\nchat_id = {sent.chat.id}", flush=True)

            await lz_var.bot.copy_message(
                chat_id=lz_var.x_man_bot_id,
                from_chat_id=sent.chat.id,
                message_id=sent.message_id
            )

        else:
            await message.answer(caption_text, reply_markup=markup)





    
       

async def update_product_preview(content_id, thumb_file_id, state, message: Message | None = None, *,
                                 chat_id: int | None = None, message_id: int | None = None):
    # 允许两种调用方式：传 message 或显式传 chat_id/message_id
    if message:
        chat_id = message.chat.id
        message_id = message.message_id
    if chat_id is None or message_id is None:
        # 没有可编辑的目标就直接返回
        return

    cached = product_info_cache.get(content_id) or {}
    cached_thumb_unique = cached.get('thumb_unique_id', "")

    # 只有在用默认图且我们已知 thumb_unique_id 时，才尝试异步更新真实图
    if thumb_file_id == DEFAULT_THUMB_FILE_ID and cached_thumb_unique:
        async def update_preview_if_arrived():
            try:
                new_file_id = await Media.fetch_file_by_file_id_from_x(state, cached_thumb_unique, 30)
                if new_file_id:
                    print(f"[预览图更新] 已获取 thumb_file_id: {new_file_id}")
                    bot_username = lz_var.bot_username
                    await AnanBOTPool.update_product_thumb(content_id, cached_thumb_unique, new_file_id, bot_username)

                    # 失效缓存
                    product_info_cache.pop(content_id, None)
                    product_info_cache_ts.pop(content_id, None)

                    # 重新渲染并编辑“同一条消息”
                    fresh_thumb, fresh_text, fresh_kb = await get_product_info(content_id)
                    try:
                        await lz_var.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=message_id,
                            media=InputMediaPhoto(media=fresh_thumb, caption=fresh_text, parse_mode="HTML"),
                            reply_markup=fresh_kb,
                        )
                    except Exception as e:
                        print(f"⚠️ 更新预览图失败C：{e}", flush=True)
            except Exception as e:
                print(f"[预览图更新失败x] {e}")
        asyncio.create_task(update_preview_if_arrived())

 
    




async def main():
    logging.basicConfig(level=logging.INFO)
    global bot_username
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    lz_var.bot_username = bot_username
    print(f"🤖 当前 bot 用户名：@{bot_username}")

   # ✅ 初始化 MySQL 连接池
    await AnanBOTPool.init_pool()

    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
