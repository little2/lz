from aiogram import Router, F
from aiogram.types import Message
from aiogram.dispatcher.event.bases import SkipHandler


from aiogram.types import (
    Message,
    InputMediaPhoto
)

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from aiogram.fsm.context import FSMContext
from utils.media_utils import ProductPreviewFSM  # ⬅️ 新增
import json
from lz_db import db
from lz_config import UPLOADER_BOT_NAME

import lz_var
router = Router()

from utils.media_utils import Media

def parse_caption_json(caption: str):
    try:
        data = json.loads(caption)
        return data if isinstance(data, dict) else False
    except (json.JSONDecodeError, TypeError):
        return False



from aiogram.fsm.context import FSMContext
from utils.media_utils import ProductPreviewFSM  # ⬅️ 新增
from utils.product_utils import MenuBase
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

    print(f"✅ [01 X-MEDIA] 收到 {file_type}，file_unique_id={file_unique_id} {file_id}，"
          f"from={message.from_user.id}，reply_to_msg_id={reply_to.message_id}", flush=True)



    # store_data = await state.get_data()
    # menu_message = store_data.get("menu_message")
    store_data = await MenuBase.get_menu_status(state)
    fetch_thumb_file_unique_id = store_data.get("fetch_thumb_file_unique_id")
    fetch_file_unique_id = store_data.get("fetch_file_unique_id")
    current_message = store_data.get("current_message")

    print(f"✅ [02 X-MEDIA] fetch_thumb_file_unique_id={fetch_thumb_file_unique_id} fetch_file_unique_id={fetch_file_unique_id}, ")


    
    if fetch_file_unique_id == file_unique_id:
        print(f"✅ [06 X-MEDIA] 发现匹配的 file_unique_id，准备继续处理", flush=True)

        # 状态里不一定已经有 current_message（例如：异步预加载/其它 handler 清理了状态）。
        # 直接访问 current_message.reply_markup 会触发 NoneType 异常。
        if current_message is None:
            print(
                f"❌ [07 X-MEDIA] current_message is None，无法替换按钮。"
                f" file_unique_id={file_unique_id} fetch_file_unique_id={fetch_file_unique_id}",
                flush=True,
            )
            
        else:
            old_kb = getattr(current_message, "reply_markup", None)

            if not old_kb:
                print(f"❌ [08 X-MEDIA] 菜单消息没有回复键盘，无法替换按钮 current_message={current_message}", flush=True)
               
            else:
                # 若已经不是“加载中”状态（没有 🔄），就不要重复编辑，避免无意义请求。
                try:
                    has_loading = any(
                        "🔄" in (btn.text or "")
                        for row in (old_kb.inline_keyboard or [])
                        for btn in (row or [])
                    )
                except Exception:
                    has_loading = False

                if not has_loading:
                    print("ℹ️ [09 X-MEDIA] 已是最终按钮状态，跳过替换", flush=True)
                    return

            
                # === 🔄→💎 替换逻辑（克隆保留所有字段）===
                new_inline_keyboard = []
                for row in (old_kb.inline_keyboard or []):
                    new_row = []
                    for btn in row:
                        new_text = (btn.text or "").replace("🔄", "💎")
                        # 克隆按钮并仅更新 text
                        cloned_btn = btn.model_copy(update={"text": new_text})
                        new_row.append(cloned_btn)
                    new_inline_keyboard.append(new_row)
                    print("ℹ️ [10 X-MEDIA] 更换兑换按钮", flush=True)
                

                new_kb = InlineKeyboardMarkup(inline_keyboard=new_inline_keyboard)

                product_message = await Media.safe_edit_reply_markup(current_message, new_kb)
       
                print(f"✅ [11 X-MEDIA] 成功替换菜单消息按钮", flush=True)

    if fetch_thumb_file_unique_id == file_unique_id:
        print(f"✅ [03 X-MEDIA] 发现匹配的 file_unique_id，准备更新缩略图", flush=True)
        try:
            # 判断 menu_message 是否有 message_id 和 chat.id
            if current_message and hasattr(current_message, 'message_id') and hasattr(current_message, 'chat'):
                await lz_var.bot.edit_message_media(
                        chat_id=current_message.chat.id,
                        message_id=current_message.message_id,
                        media=InputMediaPhoto(
                            media=file_id,   # 新图的 file_id
                            caption=current_message.caption,   # 保留原 caption
                            parse_mode="HTML",               # 如果原本有 HTML 格式
                        ),
                    reply_markup=current_message.reply_markup  # 保留原按钮
                )
                print(f"✅ [04 X-MEDIA] 成功更新菜单消息的缩略图", flush=True)
            else:
                print(f"❌ [04 X-MEDIA] menu_message 无法更新缩略图，缺少 message_id 或 chat 信息 {current_message}", flush=True)
        except Exception as e:
            print(f"❌ [05 X-MEDIA] 更新菜单消息缩略图失败: {e}", flush=True)
               
       



    user_id = str(message.from_user.id) if message.from_user else None
    
    print(f"✅ [12 X-MEDIA] 准备写入数据库，file_type={file_type} file_unique_id={file_unique_id} file_id={file_id} user_id={user_id}")

    result = await db.upsert_file_extension(
        file_type,
        file_unique_id=file_unique_id,
        file_id=file_id,
        bot=lz_var.bot_username,
        user_id=user_id
    )

    print(f"✅ [13 X-MEDIA] 数据库写入完成，result={result}")


    # 把结果写回 FSM
    await MenuBase.set_menu_status(state, {
        "x_file_unique_id": file_unique_id,
        "x_file_id": file_id
    })
    # await state.update_data({"x_file_unique_id": file_unique_id, "x_file_id": file_id})
    

@router.message(F.photo | F.video | F.document)
async def handle_media_message(message: Message, state: FSMContext):

    meta = await Media.extract_metadata_from_message(message)

    if not meta:
        return

    print(f"Received {meta['file_type']} message: {meta}", flush=True)

    user_id = str(message.from_user.id) if message.from_user else None

    current_state = await state.get_state()
    
    # 更新 Postgresql
    await db.upsert_file_extension(
        file_type=meta["file_type"],
        file_unique_id=meta["file_unique_id"],
        file_id=meta["file_id"],
        bot=lz_var.bot_username,
        user_id=user_id
    )

    # 若目前正在等待封面图输入，交给 on_clt_cover_input 处理
    from handlers.lz_menu import LZFSM
    if current_state == LZFSM.waiting_for_clt_cover.state:
        raise SkipHandler()


    if current_state is None and user_id != lz_var.x_man_bot_id and message.chat.type == "private":

        kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🤖 鲁仔三号", url=f"https://t.me/{UPLOADER_BOT_NAME}?start=upload")]
            ])

        await message.reply(f"为了让机器人的效率最高，上传资源请找鲁仔三号 @{UPLOADER_BOT_NAME}",parse_mode="HTML", reply_markup=kb)
        print(
            f"[MEDIA] 普通媒体进入 handle_media_message | "
            f"user={user_id} state={current_state}",
            flush=True
        )

