import os
import logging
from datetime import datetime, timedelta
import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from aiogram.enums import ContentType
from aiomysql import create_pool
from dotenv import load_dotenv
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage


from ananbot_utils import AnanBOTPool  # ✅ 修改点：改为统一导入类
from ananbot_config import BOT_TOKEN

bot = Bot(token=BOT_TOKEN)

# 全局变量缓存 bot username
media_upload_tasks: dict[tuple[int, int], asyncio.Task] = {}

# 全局缓存延迟刷新任务
tag_refresh_tasks: dict[tuple[int, str], asyncio.Task] = {}

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
async def make_product(callback_query: CallbackQuery):
    parts = callback_query.data.split(":")
    content_id, file_type, file_unique_id, user_id = parts[1], parts[2], parts[3], parts[4]

    product_id = await AnanBOTPool.get_existing_product(content_id)
    if not product_id:

        row = await AnanBOTPool.get_sora_content_by_id(content_id)
        if row.get("content"):
            content = row["content"]
        else:
            content = "请修改描述"

        await AnanBOTPool.create_product(content_id, "默认商品", content, 100, file_type, user_id)
    
    thumb_file_id,preview_text,preview_keyboard = await get_product_info(content_id)
    await callback_query.message.delete()
    await callback_query.message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard)
    await update_product_preview(content_id, thumb_file_id)

async def get_product_info(content_id: int) -> tuple[str, str, InlineKeyboardMarkup]:
    now = datetime.now().timestamp()
    cached = product_info_cache.get(content_id)
    cached_ts = product_info_cache_ts.get(content_id, 0)

    if cached and (now - cached_ts) < PRODUCT_INFO_CACHE_TTL:
        return cached["thumb_file_id"], cached["preview_text"], cached["preview_keyboard"]

    # 没有缓存或过期，调用原函数重新生成
    thumb_file_id, preview_text, preview_keyboard = await get_product_info_action(content_id)

   

    return thumb_file_id, preview_text, preview_keyboard


async def get_product_info_action(content_id):
    
   
    product_info = await AnanBOTPool.get_existing_product(content_id)

        # 查询是否已有同 source_id 的 product
        # 查找缩图 file_id
    
    bot_username = await get_bot_username()
    thumb_file_id, thumb_unique_id = await AnanBOTPool.get_preview_thumb_file_id(bot_username, content_id)
    if not thumb_file_id:
        #默认缩略图
        thumb_file_id = DEFAULT_THUMB_FILE_ID
        # 以下程序异步处理
            # 如果没有缩略图，传送 thumb_unique_id 给 @p_14707422896
            # 等待 @p_14707422896 的回应，应回复媒体
            # 若有回覆媒体，则回传其 file_id



    content_list = await get_list(content_id)

    preview_text = f"""文件商品
- 数据库ID:{content_id}

- 商品价格:{product_info['price']} 积分
- 商店链接:🈚️

{shorten_content(product_info['content'],300)}

- 密码: 🈚️
- 标签:
- 状态:处理中

😶‍🌫️是否匿名:是

{content_list}

"""

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

    buttons.extend([
        [
            InlineKeyboardButton(text="🏷️ 设置标签", callback_data=f"tag_full:{content_id}"),
            InlineKeyboardButton(text="💎 设置积分", callback_data=f"set_price:{content_id}")
        ],
        [InlineKeyboardButton(text="🙈 取消匿名", callback_data=f"toggle_anonymous:{content_id}")],
        [InlineKeyboardButton(text="➕ 添加资源", callback_data=f"add_items:{content_id}")],
        [
            InlineKeyboardButton(text="📬 提交投稿", callback_data=f"submit_product:{content_id}"),
            InlineKeyboardButton(text="❌ 取消投稿", callback_data=f"cancel_publish:{content_id}")
        ]
    ])

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
        print(f"🔍 从数据库查询到选中的标签: {selected_tags} for file_unique_id: {file_unique_id}")
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
        display_cn = f"{current_cn} ({selected_count}/{total_count})"

        if current_code == type_code:
            # 当前展开的类型
            keyboard.append([
                InlineKeyboardButton(text=f"------- ▶️ [{display_cn}] ------- ", callback_data="noop")
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
                    text=f"------- [{display_cn}] -------",
                    callback_data=f"set_tag_type:{content_id}:{current_code}"
                )
            ])

    # 添加「完成」按钮
    keyboard.append([
        InlineKeyboardButton(
            text="✅ 设置完成并返回",
            callback_data=f"back_to_product:{content_id}"
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
            await asyncio.sleep(1)
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








@dp.callback_query(F.data.startswith("back_to_product:"))
async def handle_back_to_product(callback_query: CallbackQuery):
    content_id = callback_query.data.split(":")[1]
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
        reply_markup=preview_keyboard
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
    member_content_id = await AnanBOTPool.insert_sora_content_media(file_unique_id, file_type, file_size, duration, user_id, file_id, bot_username)



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
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
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
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
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
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
                reply_markup=preview_keyboard
            )
        await message.delete()  
        return

    price = int(price_str)
    


    await AnanBOTPool.update_product_price(content_id=content_id, price=price)


    # await message.answer(f"✅ 已更新价格为 {price} 积分")

    # 回到菜单

    await state.clear()
    await message.delete()
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    await bot.edit_message_media(
        chat_id=chat_id,
        message_id=message_id,
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
        reply_markup=preview_keyboard
    )


@dp.callback_query(F.data.startswith("cancel_set_price:"))
async def cancel_set_price(callback_query: CallbackQuery, state: FSMContext):
    content_id = callback_query.data.split(":")[1]
    await state.clear()
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    await callback_query.message.edit_media(
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
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
        thumb_file_id = "AgACAgEAAxkBAAPIaHHqdjJqYXWcWVoNoAJFGFBwBnUAAjGtMRuIOEBF8t8-OXqk4uwBAAMCAAN5AAM2BA"


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
                    caption=preview_text
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
                caption=preview_text
            )
            
        )
    except Exception as e:
        print(f"⚠️ 超时编辑失败：{e}", flush=True)
        

@dp.callback_query(F.data.startswith("auto_update_thumb:"))
async def handle_auto_update_thumb(callback_query: CallbackQuery, state: FSMContext):
    content_id = int(callback_query.data.split(":")[1])

    try:
        # Step 1: 取得 sora_content.source_id
        row = await AnanBOTPool.get_sora_content_by_id(content_id)
        if not row or not row.get("source_id"):
            return await callback_query.answer("⚠️ 无法取得 source_id", show_alert=True)

        source_id = row["source_id"]
        print(f"🔍 取得 source_id: {source_id} for content_id: {content_id}", flush=True)
        bot_username = await get_bot_username()
        # Step 2: 取得 thumb_file_unique_id
        thumb_row = await AnanBOTPool.get_bid_thumbnail_by_source_id(source_id, bot_username)
        if not thumb_row or not thumb_row.get("thumb_file_unique_id"):
            return await callback_query.answer("⚠️ 找不到对应的缩图", show_alert=True)

        thumb_file_unique_id = thumb_row["thumb_file_unique_id"]
        thumb_file_id = thumb_row["thumb_file_id"]
        print(f"🔍 取得 thumb_file_unique_id: {thumb_file_unique_id}, {thumb_file_id} for source_id: {source_id}", flush=True)

        # Step 3: 更新 sora_content 缩图字段
        await AnanBOTPool.update_product_thumb(content_id, thumb_file_unique_id,thumb_file_id, bot_username)
       # 确保缓存存在
        if content_id in product_info_cache:
            product_info_cache[content_id]["thumb_unique_id"] = thumb_file_unique_id
            product_info_cache[content_id]["thumb_file_id"] = thumb_file_id
        else:
            # 若没缓存，则重新生成一次缓存
            await get_product_info(content_id)
        print(f"✅ 更新 content_id: {content_id} 的缩图为 {thumb_file_unique_id}", flush=True)

        if thumb_file_id is None:
        # Step 4: 通知处理 bot 生成缩图（或触发缓存）
            await bot.send_message(chat_id=7793315433, text=f"{thumb_file_unique_id}")
            await callback_query.answer("已通知其他机器人更新，请稍后自动刷新", show_alert=True)
        else:
            
            try:
                await callback_query.message.edit_media(
                    media=InputMediaPhoto(media=thumb_file_id, caption=product_info_cache[content_id]["preview_text"]),
                    reply_markup=product_info_cache[content_id]["preview_keyboard"]
                )
            except Exception as e:
                print(f"⚠️ 更新预览图失败: {e}", flush=True)
            await callback_query.answer("✅ 已自动更新预览图", show_alert=True)

    except Exception as e:
        logging.exception(f"⚠️ 自动更新预览图失败: {e}")
        await callback_query.answer("⚠️ 自动更新失败", show_alert=True)




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
    product_info_cache[content_id]["thumb_unique_id"] = file_unique_id
    product_info_cache[content_id]["thumb_file_id"] = file_id

    # 编辑原消息，更新为商品卡片
    thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
    try:
        await bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
            reply_markup=preview_keyboard
        )
    except Exception as e:
        print(f"⚠️ 更新预览图失败：{e}", flush=True)

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
                media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
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
            media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
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
        media=InputMediaPhoto(media=thumb_file_id, caption=preview_text),
        reply_markup=preview_keyboard
    )



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
async def handle_media(message: Message):
    file_type = message.content_type
    user_id = str(message.from_user.id)

    if file_type == ContentType.PHOTO:
        photo = get_largest_photo(message.photo)
        file_unique_id = photo.file_unique_id
        file_id = photo.file_id
        file_size = photo.file_size or 0
        duration = 0
        width = photo.width
        height = photo.height
        table = "photo"
    elif file_type == ContentType.VIDEO:
        file_unique_id = message.video.file_unique_id
        file_id = message.video.file_id
        file_size = message.video.file_size
        duration = message.video.duration
        width = message.video.width
        height = message.video.height
        table = "video"
    elif file_type == ContentType.DOCUMENT:
        file_unique_id = message.document.file_unique_id
        file_id = message.document.file_id
        file_size = message.document.file_size
        duration = 0
        width = 0
        height = 0
        table = "document"
    else:
        return
    
    # 正常媒体接收处理
   
    published, kc_id = await AnanBOTPool.is_media_published(table, file_unique_id)
    if published:
        await message.answer(f"""此媒体已发布过
file_unique_id: {file_unique_id}
kc_id: {kc_id or '无'}""")
    else:
        await message.answer("此媒体未发布过")

    await AnanBOTPool.upsert_media(table, {
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "duration": duration,
        "width": width,
        "height": height,
        "create_time": datetime.now()
    })
    bot_username = await get_bot_username()
    await AnanBOTPool.insert_file_extension(table, file_unique_id, file_id, bot_username, user_id)
    content_id = await AnanBOTPool.insert_sora_content_media(file_unique_id, table, file_size, duration, user_id, file_id,bot_username)

    product_info = await AnanBOTPool.get_existing_product(content_id)
    if product_info:
        thumb_file_id, preview_text, preview_keyboard = await get_product_info(content_id)
        await message.answer_photo(photo=thumb_file_id, caption=preview_text, reply_markup=preview_keyboard)
        await update_product_preview(content_id, thumb_file_id)
       
        

    else:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="创建", callback_data=f"make_product:{content_id}:{table}:{file_unique_id}:{user_id}"),
                InlineKeyboardButton(text="取消", callback_data="cancel_product")
            ]
        ])
        await message.answer("检测到文件，是否需要创建为投稿？", reply_markup=markup)


async def update_product_preview(content_id, thumb_file_id):
    now = datetime.now().timestamp()
    cached = product_info_cache.get(content_id)

    if thumb_file_id ==  DEFAULT_THUMB_FILE_ID and cached['thumb_unique_id'] != "":
        async def update_preview_if_arrived():
            try:
                #传送信息给用户p_14707422896
                await bot.send_message(chat_id=7793315433, text=f"{cached['thumb_unique_id']}")
                # 等待 chat_id 的回应

                print(f"[通知更新成功] {cached['thumb_unique_id']}")
            except Exception as e:
                print(f"[预览图更新失败] {e}")

        asyncio.create_task(update_preview_if_arrived())
 
    




async def main():
    logging.basicConfig(level=logging.INFO)
    global bot_username
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    print(f"🤖 当前 bot 用户名：@{bot_username}")

   # ✅ 初始化 MySQL 连接池
    await AnanBOTPool.init_pool()

    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
