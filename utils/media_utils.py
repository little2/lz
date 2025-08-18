# utils/media_utils.py
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram import Bot
import lz_var
import asyncio
# --- 顶部新增 import ---
from io import BytesIO
from typing import Optional, Tuple
from aiogram.types import BufferedInputFile, PhotoSize



class ProductPreviewFSM(StatesGroup):
    waiting_for_x_media = State()


class Media:

    @classmethod
    async def fetch_file_by_file_id_from_x(cls, state: FSMContext, ask_file_unique_id: str | None = None, timeout_sec: float = 10.0):
        """
        进入等待态 -> 清空状态数据 -> 轮询每 0.5 秒看是否收到 file_unique_id
        - 要求：由 7793315433 以「回覆」的方式把媒体回到你发出的那条 ask 消息
        - lz_media_parser.py 会在满足条件时把 FSM 内容写入 {"x_media_unique_id": "..."} 并打印
        - 不负责发送“召唤消息”，仅负责等待与取值；召唤逻辑由业务处发送后再调用本函数
        """



        bot = lz_var.bot
        storage = state.storage  # 与全局 Dispatcher 共享的同一个 storage

        x_uid = lz_var.x_man_bot_id          # = 7793315433
        x_chat_id = x_uid                     # 私聊里 chat_id == user_id
        key = StorageKey(bot_id=lz_var.bot.id, chat_id=x_chat_id, user_id=x_uid)

        await storage.set_state(key, ProductPreviewFSM.waiting_for_x_media.state)
        await storage.set_data(key, {})  # 清空

        # 可选：你想把召唤语塞给状态，方便对方检查 reply_to（不必须）
        if ask_file_unique_id:
            try:
                # 直接发文字请求
                await lz_var.bot.send_message(
                    chat_id=lz_var.x_man_bot_id,
                    text=f"{ask_file_unique_id}"
                )

                # 或者如果你要发的是具体文件，可以这样：
                # file = FSInputFile(path_to_file)
                # await lz_var.bot.send_document(chat_id=target_user_id, document=file)

                print(f"✅ 已向 {lz_var.x_man_bot_id} 请求文件 {ask_file_unique_id}", flush=True)

            except Exception as e:
                print(f"❌ 发送 ask_file_unique_id 给用户失败: {e}", flush=True)
        max_loop = int((timeout_sec / 0.5) + 0.5)
        for _ in range(max_loop):
            data = await storage.get_data(key)
            x_file_id = data.get("x_file_id")
            if x_file_id:
                # 清掉对方上下文的等待态
                await storage.set_state(key, None)
                await storage.set_data(key, {})
                print(f"  ✅ [X-MEDIA] 收到 file_id={x_file_id}", flush=True)
                return x_file_id
            await asyncio.sleep(0.5)

        if not x_file_id:
            print(f"❌ [X-MEDIA] 超时未收到 file_id，等待 {timeout_sec} 秒后清理状态", flush=True)

        # 超时清理
        await storage.set_state(key, None)
        await storage.set_data(key, {})
        return None


    @classmethod
    async def extract_video_metadata_from_aiogram(cls,message):
        if message.photo:
            largest = message.photo[-1]
            file_id = largest.file_id
            file_unique_id = largest.file_unique_id
            mime_type = 'image/jpeg'
            file_type = 'photo'
            file_size = largest.file_size
            file_name = None
            # 用 Bot API 发到目标群组
      

        elif message.document:
            file_id = message.document.file_id
            file_unique_id = message.document.file_unique_id
            mime_type = message.document.mime_type
            file_type = 'document'
            file_size = message.document.file_size
            file_name = message.document.file_name
       

        else:  # 视频
            file_id = message.video.file_id
            file_unique_id = message.video.file_unique_id
            mime_type = message.video.mime_type or 'video/mp4'
            file_type = 'video'
            file_size = message.video.file_size
            file_name = getattr(message.video, 'file_name', None)
        
        return file_id, file_unique_id, mime_type, file_type, file_size, file_name
    
    @classmethod
    def build_hashtag_string(cls, tag_names: list[str], max_len: int = 200) -> str:
        """
        把 ['可爱','少年'] -> '#可爱 #少年 '，并保证不超过 max_len。
        尽量按顺序加入，超长则停止。
        末尾保留一个空格便于后续拼接。
        """
        out = []
        length = 0
        for name in tag_names:
            piece = f"#{name} "
            if length + len(piece) > max_len:
                break
            out.append(piece)
            length += len(piece)
        return "".join(out)
    


    @classmethod
    async def extract_preview_photo_buffer(
        cls,
        message,
        *,
        bot: Optional[Bot] = None,
        prefer_cover: bool = True,
        delete_sent: bool = True
    ) -> Optional[Tuple[str, str]]:
        """
        从 video/document/animation 的封面/缩略图提取为“照片”，并返回新的 (file_id, file_unique_id)。
        - 仅用内存缓冲，不落盘
        - video: 优先 cover（多尺寸列表），否则 thumbnail
        - document/animation: 仅 thumbnail
        - delete_sent=True 时，会在拿到 ID 后把临时发出的照片消息删掉

        :return: (file_id, file_unique_id)；无可用预览时返回 None
        """

        """
        提取 video/document/animation 的封面/缩略图，内存下载→重新上传为照片，
        返回新的 (file_id, file_unique_id)。
        优先使用传入的 bot，其次尝试 message.bot，最后才用 lz_var.bot。
        """
        # 1) 解析可用的 bot
        tg_bot = bot or getattr(message, "bot", None) or getattr(lz_var, "bot", None)
        if tg_bot is None:
            raise RuntimeError("extract_preview_photo_ids 需要有效的 Bot 实例：请传入 bot= 或确保 message.bot / lz_var.bot 可用。")
        pic: Optional[PhotoSize] = None
        if getattr(message, "video", None):
            v = message.video
            if prefer_cover and getattr(v, "cover", None):
                pic = v.cover[0] if v.cover else None
            if not pic:
                pic = v.thumbnail
        elif getattr(message, "document", None):
            pic = message.document.thumbnail
        elif getattr(message, "animation", None):
            pic = message.animation.thumbnail

        if not pic:
            return None

        # 1) 下载到内存
        buf = BytesIO()
        await tg_bot.download(pic, destination=buf)
        buf.seek(0)

        return buf,pic

        # 2) 以照片重新上传（仅用于获得新的 file_id / file_unique_id）
        sent = await tg_bot.send_photo(
            chat_id=message.chat.id,
            photo=BufferedInputFile(buf.read(), filename=f"{pic.file_unique_id}.jpg"),
            disable_notification=True
        )

        # 3) 取最大尺寸的 photo 对象
        photo_obj = sent.photo[-1]
        file_id = photo_obj.file_id
        file_unique_id = photo_obj.file_unique_id
        file_size = photo_obj.file_size
        width = photo_obj.width
        height = photo_obj.height

        # 4) 可选：删除临时消息
        if delete_sent:
            try:
                await tg_bot.delete_message(chat_id=message.chat.id, message_id=sent.message_id)
            except Exception:
                pass

        return file_id, file_unique_id, file_size, width, height
