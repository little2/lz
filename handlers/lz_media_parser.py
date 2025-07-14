from aiogram import Router, F
from aiogram.types import Message
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


@router.message(F.photo)
async def handle_photo_message(message: Message):

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