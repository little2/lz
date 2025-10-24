from aiogram import Router, F
from aiogram.types import Message

from aiogram.types import  InputMediaPhoto

from aiogram.fsm.context import FSMContext
from utils.media_utils import ProductPreviewFSM  # ⬅️ 新增
import json
from lz_db import db
import lz_var
router = Router()

def parse_caption_json(caption: str):
    try:
        data = json.loads(caption)
        return data if isinstance(data, dict) else False
    except (json.JSONDecodeError, TypeError):
        return False


from aiogram.fsm.context import FSMContext
from utils.media_utils import ProductPreviewFSM  # ⬅️ 新增
import lz_var

router = Router()

# ...（你原本的 handle_photo_message / handle_video / handle_document 保持不动）

@router.message(
    (F.photo | F.video | F.document)
    & (F.from_user.id == lz_var.x_man_bot_id)
    & F.reply_to_message.as_("reply_to")
)
async def handle_x_media_when_waiting(message: Message, state: FSMContext, reply_to: Message):
    """
    仅在等待态才处理；把 file_unique_id 写到 FSM。
    """
    # if await state.get_state() != ProductPreviewFSM.waiting_for_x_media.state:
        # return  # 非等待态，跳过


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

    store_data = await state.get_data()
    menu_message = store_data.get("menu_message")
    fetch_thumb_file_unique_id = store_data.get("fetch_thumb_file_unique_id")

    

    if fetch_thumb_file_unique_id == file_unique_id:
        print(f"✅ [X-MEDIA] 发现匹配的 file_unique_id，准备更新缩略图", flush=True)
        try:
            await lz_var.bot.edit_message_media(
                chat_id=menu_message.chat.id,
                message_id=menu_message.message_id,
                media=InputMediaPhoto(
                    media=file_id,   # 新图的 file_id
                    caption=menu_message.caption,   # 保留原 caption
                    parse_mode="HTML",               # 如果原本有 HTML 格式
                ),
                reply_markup=menu_message.reply_markup  # 保留原按钮
            )
        

            print(f"✅ [X-MEDIA] 成功更新菜单消息的缩略图", flush=True)
        except Exception as e:
            print(f"❌ [X-MEDIA] 更新菜单消息缩略图失败: {e}", flush=True)

    user_id = str(message.from_user.id) if message.from_user else None
    
    await db.upsert_file_extension(
        file_type,
        file_unique_id=file_unique_id,
        file_id=file_id,
        bot=lz_var.bot_username,
        user_id=user_id
    )


    # 把结果写回 FSM
    await state.update_data({"x_file_unique_id": file_unique_id, "x_file_id": file_id})
    

@router.message(F.photo)
async def handle_photo_message(message: Message):
    print(f"Received photo message: {message.photo}")
    largest_photo = message.photo[-1]
    file_id = largest_photo.file_id
    file_unique_id = largest_photo.file_unique_id



    # await message.reply(
    #     f"🖼️ 这是你上传的图片最大尺寸：\n\n"
    #     f"<b>file_id:</b> <code>{file_id}</code>\n"
    #     f"<b>file_unique_id:</b> <code>{file_unique_id}</code>",
    #     parse_mode="HTML"
    # )


    # caption = message.caption or ""
    # result = parse_caption_json(caption)




    # if result is False:
    #     pass
    #     # await message.reply("⚠️ Caption 不是合法的 JSON。")
    #     return

    # await message.reply(f"✅ 解析成功：{result}")

    largest_photo = message.photo[-1]
    file_id = largest_photo.file_id
    file_unique_id = largest_photo.file_unique_id
    user_id = str(message.from_user.id) if message.from_user else None
    print(f"{lz_var.bot_username}")
    await db.upsert_file_extension(
        file_type='photo',
        file_unique_id=file_unique_id,
        file_id=file_id,
        bot=lz_var.bot_username,
        user_id=user_id
    )

@router.message(F.video)
async def handle_video(message: Message):
    print(f"Received video message: {message.video}")
    file_id = message.video.file_id
    file_unique_id = message.video.file_unique_id
    user_id = str(message.from_user.id) if message.from_user else None


    await db.upsert_file_extension(
        file_type='video',
        file_unique_id=file_unique_id,
        file_id=file_id,
        bot=lz_var.bot_username,
        user_id=user_id
    )

@router.message(F.document)
async def handle_document(message: Message):
    print(f"Received document message: {message.document}")
    file_id = message.document.file_id
    file_unique_id = message.document.file_unique_id
    user_id = str(message.from_user.id) if message.from_user else None

    await db.upsert_file_extension(
        file_type='document',
        file_unique_id=file_unique_id,
        file_id=file_id,
        bot=lz_var.bot_username,
        user_id=user_id
    )