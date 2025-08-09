# utils/media_utils.py
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
import lz_var
import asyncio

class WaitingForXMedia(StatesGroup):
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
        key = StorageKey(bot_id=bot.id, chat_id=x_chat_id, user_id=x_uid)

        await storage.set_state(key, WaitingForXMedia.waiting_for_x_media.state)
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
    