# utils/media_utils.py
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import Message, InputMediaPhoto
from aiogram.exceptions import TelegramBadRequest
from typing import Any

from aiogram import Bot
import lz_var
import asyncio
# --- 顶部新增 import ---
from io import BytesIO
from typing import Optional, Tuple
from aiogram.types import BufferedInputFile, PhotoSize
from lz_config import UPLOADER_BOT_NAME

class ProductPreviewFSM(StatesGroup):
    waiting_for_x_media = State()


class Media:

    @classmethod
    async def _notify_lack_file_uid_rows(cls, lack_file_uid_rows: list[str], max_count: int = 100):
        sent_count = 0
        for fuid in lack_file_uid_rows:
            sent_count += 1
            try:
                await lz_var.bot.send_message(
                    chat_id=lz_var.x_man_bot_id,
                    text=f"{fuid}"
                )
            except Exception as e:
                print(f"⚠️ 背景通知缺少资源失败 {fuid}: {e}", flush=True)

            print(f"缺少 {fuid}")

            if sent_count >= max_count:
                break

            await asyncio.sleep(0.7)

    @classmethod
    async def safe_callback_answer(
        cls,
        callback,
        *,
        text: str | None = None,
        show_alert: bool = False,
    ) -> bool:
        """Safely answer callback query; ignore expiry/invalid query errors."""
        try:
            await callback.answer(text=text, show_alert=show_alert)
            return True
        except TelegramBadRequest as e:
            msg = str(e).lower()
            if "query is too old" in msg or "query id is invalid" in msg:
                return False
            raise

    @classmethod
    def _dump_reply_markup(cls, markup: InlineKeyboardMarkup | None) -> Any:
        if not markup:
            return None
        if hasattr(markup, "model_dump"):
            return markup.model_dump()
        if hasattr(markup, "dict"):
            return markup.dict()
        return str(markup)

    @classmethod
    async def safe_edit_reply_markup(cls, msg: Message | None, reply_markup: InlineKeyboardMarkup | None):
        """
        安全编辑 reply_markup：
        - 若新旧 markup 完全一致，则跳过，避免 Telegram 'message is not modified'
        - 若仍遇到 'message is not modified'，吞掉该异常，保证流程不中断
        """
        if msg is None:
            return None

        try:
            old_dump = cls._dump_reply_markup(getattr(msg, "reply_markup", None))
            new_dump = cls._dump_reply_markup(reply_markup)
            if old_dump == new_dump:
                return msg
            return await msg.edit_reply_markup(reply_markup=reply_markup)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                return msg
            raise

    @classmethod
    async def edit_caption_or_text(
        cls,
        msg: Message | None = None,
        *,
        text: str,
        reply_markup: InlineKeyboardMarkup | None,
        chat_id: int | None = None,
        message_id: int | None = None,
        photo: str | None = None,
        state: FSMContext | None = None,
        mode: str = "edit"
    ):
        """
        统一编辑（从 lz_menu 抽离）：
          - 若原消息有媒体：
              * 传入 photo → 用 edit_message_media 换图 + caption
              * 未传 photo → 尝试复用原图（仅当原媒体是 photo），否则仅改 caption
          - 若原消息无媒体：edit_message_text
        额外：
          - 遇到 Telegram 'message is not modified' 时不报错（直接返回原 msg）
          - 若传入 state，则会更新 MenuBase 的 current_message/current_message_id 等缓存
        """
        try:
            if msg is None and (chat_id is None or message_id is None):
                return None

            if msg is not None and hasattr(msg, "chat"):
                if chat_id is None:
                    chat_id = msg.chat.id
                if message_id is None:
                    message_id = msg.message_id

            if message_id is None:
                print("没有 message_id，无法定位消息", flush=True)
                return None

            if mode == "edit":
                media_attr = None
                if msg is not None:
                    media_attr = next(
                        (attr for attr in ["animation", "video", "photo", "document"] if getattr(msg, attr, None)),
                        None,
                    )
                else:
                    print(f"{msg}",flush=True)
            else:
                media_attr = mode

            current_message = None

            if media_attr:
                print(f"编辑含媒体消息(media_attr={media_attr})...", flush=True)
                if photo:
                    if mode == "edit":
                        current_message = await lz_var.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=message_id,
                            media=InputMediaPhoto(media=photo, caption=text, parse_mode="HTML"),
                            reply_markup=reply_markup
                            
                        )
                    else:
                        current_message = await lz_var.bot.send_photo(
                            chat_id=chat_id,
                            photo=photo,
                            caption=text,
                            parse_mode="HTML",
                            reply_markup=reply_markup
                            
                        )
                else:
                    if media_attr == "photo":
                        orig_photo_id = None
                        try:
                            orig_photo_id = (msg.photo[-1].file_id) if getattr(msg, "photo", None) else None
                        except Exception:
                            orig_photo_id = None

                        if orig_photo_id:
                            if mode == "edit":
                                current_message = await lz_var.bot.edit_message_media(
                                    chat_id=chat_id,
                                    message_id=message_id,
                                    media=InputMediaPhoto(media=orig_photo_id, caption=text, parse_mode="HTML"),
                                    reply_markup=reply_markup
                                
                                )
                            else:
                                current_message = await lz_var.bot.send_photo(
                                    chat_id=chat_id,
                                    photo=orig_photo_id,
                                    caption=text,
                                    parse_mode="HTML",
                                    reply_markup=reply_markup
                                   
                                )
                        else:
                            if mode == "edit":
                                current_message = await lz_var.bot.edit_message_caption(
                                    chat_id=chat_id,
                                    message_id=message_id,
                                    caption=text,
                                    parse_mode="HTML",
                                    reply_markup=reply_markup
                                )
                            else:
                                current_message = await lz_var.bot.send_message(
                                    chat_id=chat_id,
                                    text=text,
                                    parse_mode="HTML",
                                    reply_markup=reply_markup
                                )
                    else:
                        if mode == "edit":
                            current_message = await lz_var.bot.edit_message_caption(
                                chat_id=chat_id,
                                message_id=message_id,
                                caption=text,
                                parse_mode="HTML",
                                reply_markup=reply_markup
                                
                            )
                        else:
                            current_message = await lz_var.bot.send_message(
                                chat_id=chat_id,
                                text=text,
                                parse_mode="HTML",
                                reply_markup=reply_markup
                                
                            )
            else:
                print(f"编辑无媒体消息...", flush=True)
                if mode == "edit":
                     current_message = await lz_var.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        reply_markup=reply_markup
                        
                    )
                else:
                    current_message = await lz_var.bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        reply_markup=reply_markup
                    )

            if state is not None and current_message is not None:
                try:
                    from utils.product_utils import MenuBase  # 延迟 import，避免循环依赖
                    await MenuBase.set_menu_status(
                        state,
                        {
                            "current_message": current_message,
                            "current_chat_id": current_message.chat.id,
                            "current_message_id": current_message.message_id,
                        },
                    )
                except Exception as e:
                    print(f"⚠️ MenuBase.set_menu_status failed: {e}", flush=True)

            return current_message

        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                return msg
            raise
        except Exception as e:
            print(f"❌ 编辑消息失败(edit_caption_or_text): {e}", flush=True)
            return msg


    @classmethod
    async def fetch_file_by_file_uid_from_x(cls, state: FSMContext, ask_file_unique_id: str | None = None, timeout_sec: float = 10.0):
        """
        进入等待态 -> 清空状态数据 -> 轮询每 0.5 秒看是否收到 file_unique_id
        - 要求：由 仓库人 以「回覆」的方式把媒体回到你发出的那条 ask 消息
        - lz_media_parser.py 会在满足条件时把 FSM 内容写入 {"x_media_unique_id": "..."} 并打印
        - 不负责发送“召唤消息”，仅负责等待与取值；召唤逻辑由业务处发送后再调用本函数
        """

        bot = lz_var.bot
        if state:
            storage = state.storage  # 与全局 Dispatcher 共享的同一个 storage

            x_uid = lz_var.x_man_bot_id          
            x_chat_id = x_uid                     # 私聊里 chat_id == user_id
            key = StorageKey(bot_id=lz_var.bot.id, chat_id=x_chat_id, user_id=x_uid)

            await storage.set_state(key, ProductPreviewFSM.waiting_for_x_media.state)
        # await storage.set_data(key, {})  # 清空

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

                print(f"--->  🏚 已向仓库 ( {lz_var.x_man_bot_id} ) 请求文件 {ask_file_unique_id}", flush=True)

            except Exception as e:
                if "Bad Request: chat not found" in str(e):
                    print(f"❌ 发送 ask_file_unique_id 给用户失败：Bot 未与用户建立对话，请先让 {lz_var.x_man_bot_id} 给 {lz_var.bot_username} 发一条消息再试。 |_kick_|{lz_var.bot_username}", flush=True)
                else:
                    print(f"❌ 发送 ask_file_unique_id 给用户失败: {e}", flush=True)
        
        if state is None:
            return None
        
        max_loop = int((timeout_sec / 0.5) + 0.5)
        for _ in range(max_loop):
            data = await storage.get_data(key)
            x_file_id = data.get("x_file_id")
            x_file_unique_id = data.get("x_file_unique_id")
            if x_file_id:
                # 清掉对方上下文的等待态 
                await storage.set_state(key, None)
                data["x_file_id"] = None
                data["x_file_unique_id"] = None
                await storage.set_data(key, data)
                # print(f"  ✅ (fetch_file_by_file_uid_from_x) [X-MEDIA] 收到 file_id={x_file_id} | {ask_file_unique_id}", flush=True)
                return x_file_id
            await asyncio.sleep(0.5)

        # if not x_file_id:
            # print(f"❌ (fetch_file_by_file_uid_from_x)[X-MEDIA] 超时未收到 x_file_unique_id {ask_file_unique_id} ，已等待 {timeout_sec} 秒后清理状态", flush=True)

        # 超时清理 
        
        await storage.set_state(key, None)
        data = await storage.get_data(key)
        data["x_file_id"] = None
        data["x_file_unique_id"] = None
        await storage.set_data(key, data)
        
        return None


    @classmethod
    async def extract_metadata_from_message(cls,message):
        file_type = None
        mediamessage = None
        file_unique_id = ""
        file_id = ""
        file_size = 0
        height = 0
        width = 0
        if message.photo:
            largest = message.photo[-1]
            file_id = largest.file_id
            file_unique_id = largest.file_unique_id
            height = largest.height
            width = largest.width
            file_size = largest.file_size
            mediamessage = message.photo
            file_type = 'photo'

          

        elif message.document:
            file_type = 'document'
            mediamessage = message.document
        elif message.animation:
            file_type = "animation"     
            mediamessage = message.animation     
        elif message.video:       
            file_type = "video"
            mediamessage = message.video
       
        

        if file_type: 
            meta = {
                "file_type": file_type,
                "file_unique_id": file_unique_id or getattr(mediamessage, "file_unique_id", None),
                "file_id": file_id or getattr(mediamessage, "file_id", None),
                "file_size": file_size or getattr(mediamessage, "file_size", 0) or 0,
                "duration": getattr(mediamessage, "duration", 0) or 0,
                "width": width or getattr(mediamessage, "width", 0) or 0,
                "height": height or getattr(mediamessage, "height", 0) or 0,
                "file_name": getattr(mediamessage, "file_name", "") or "",
                "mime_type": getattr(mediamessage, "mime_type", "") or "",
            }
            
            # print(f"metameta==>{meta}")
            return meta
        
        print(f"{meta}")

        return False



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

    @classmethod
    async def send_media_group(cls, callback, productInfomation, box_id:int=1, content_id:int|None=0, source_id:str|None=None, protect_content:bool=False):
        from_user_id = callback.from_user.id
        quantity = 0
        material_status = productInfomation.get("material_status", {})
        total_quantity = material_status.get("total",0)
        box_dict = material_status.get("box",0)
        box_quantity = len(box_dict)  

        if productInfomation.get("ok") is False and productInfomation.get("lack_file_uid_rows"):
            lack_file_uid_rows = productInfomation.get("lack_file_uid_rows")
            asyncio.create_task(cls._notify_lack_file_uid_rows(lack_file_uid_rows, max_count=100))

            print(f"资源同步中，请稍后再试，请看看别的资源吧(-{len(lack_file_uid_rows)})", flush=True)        
            return {'ok':False,'message':f'资源同步中，请稍后再试，可以先看看别的资源吧( 请 {len(lack_file_uid_rows)} 秒后重试)'}
        # print(f"1896=>{productInfomation}")
        rows = productInfomation.get("rows", [])
        # print(f"rows={rows}", flush=True)
        if rows:

            material_status = productInfomation.get("material_status")
            # print(f"material_status={material_status}", flush=True)
            if material_status:
               
                # print(f"{rows}---{box_id}", flush=True)

                # 先只在有值时注入 reply_to_message_id
                send_media_group_kwargs = dict(chat_id=from_user_id, media=rows[(int(box_id)-1)], protect_content=protect_content)
                try:
                   

                    # if reply_to_message_id is not None:
                        # send_media_group_kwargs["reply_to_message_id"] = reply_to_message_id
                    sr = await lz_var.bot.send_media_group(**send_media_group_kwargs)
                    
                except Exception as e:
                    print(f"❌ 发送媒体组失败: {e}", flush=True)
                    return {'ok':False,'message':'发送媒体组失败，请稍后再试'}

                
                box_dict = material_status.get("box", {})  # dict: {1:{...}, 2:{...}}
                # 盒子数量（组数）
                box_quantity = len(box_dict)  
                if ( box_quantity <= 1):
                    await cls.safe_callback_answer(callback)
                    return


                msg = callback.message
                kb = msg.reply_markup
                new_rows: list[list[InlineKeyboardButton]] = []
                if int(box_id)==1:
                    return_media = await cls._build_mediagroup_box(box_id, source_id, content_id, material_status)
                    feedback_kb = return_media.get("feedback_kb")
                    text = return_media.get("text")


                elif int(box_id)>1 and kb and kb.inline_keyboard:
                    
                    quantity = 0
                    for row in kb.inline_keyboard:
                        new_row = []
                        for btn in row:
                            # 去掉已有的 "[V]"，避免重复标记
                            base_text = btn.text.lstrip()
                            base_text_pure = base_text.replace("✅","").lstrip()
                            if base_text_pure == "⚠️ 我要打假":
                                continue
                            btn_quantity = box_dict.get(int(base_text_pure),{}).get("quantity",0)
                            print(f"btn_quantity={btn_quantity}")

                            if base_text.startswith("✅"):
                                new_btn = btn.model_copy()
                                quantity = quantity+ int(btn_quantity)
                            elif base_text_pure == box_id:
                                quantity = quantity+ int(btn_quantity)
                                new_btn_text = f"✅ {base_text_pure}"
                                new_btn = btn.model_copy(update={"text": new_btn_text})
                            else:
                                new_btn = btn.model_copy()
                            
                                # print(f"😂{btn}") 
                                # base_text_pure = base_text[3:].lstrip()
                                # sent_quantity = len(material_status.get("box",{}).get(int(base_text_pure),{}).get("file_ids",[])) if material_status else 0

                            # 判断是否为目标按钮（文字等于 box_id 或 callback_data 的最后一段等于 box_id）
                            # # is_target = (base_text == box_id)
                            # if not is_target and btn.callback_data:
                            #     try:
                            #         is_target = (btn.callback_data.split(":")[-1] == box_id)
                            #     except Exception:
                            #         is_target = False

                            # 目标按钮加上 "[V]" 前缀，其他按钮保持/移除多余的前缀
                            # new_btn_text = f"✅ {base_text}" if is_target else base_text

                            # 用 pydantic v2 的 model_copy 复制按钮，仅更新文字，其他字段（url、callback_data 等）保持不变
                            # new_btn = btn.model_copy(update={"text": new_btn_text})
                            new_row.append(new_btn)
                        new_rows.append(new_row)
                    feedback_kb = InlineKeyboardMarkup(inline_keyboard=new_rows) if new_rows else kb
                    text = f"💡当前 {quantity}/{total_quantity} 个，第 {box_id} / {box_quantity} 页"
                    await msg.delete()
                
                


                # ✅ 1) 取出 callback 内原消息文字，并在后面加 "123"
  
                


                try:
                    if(total_quantity > quantity):

                        send_media_menu = dict(chat_id=from_user_id, text=text,reply_markup=feedback_kb,parse_mode="HTML")
                        try:
                            # if reply_to_message_id is not None:
                                # send_media_menu["reply_to_message_id"] = reply_to_message_id
                            sr = await lz_var.bot.send_message(**send_media_menu)
                            # print(f"sr={sr}")
                        except Exception as e:
                            print(f"❌ 发送媒体组失败: {e}", flush=True)
                            return {'ok':False,'message':'发送媒体组失败，请稍后再试'}
                    



                    # if msg.text is not None:
                    
                    #     await msg.edit_text(new_text, reply_markup=new_markup)
                    # else:
                    #     await msg.edit_caption(new_text, reply_markup=new_markup)
                except Exception as e:
                    # 可选：记录一下，避免因“内容未变更”等报错中断流程
                    print(f"[media_box] edit message failed: {e}", flush=True)

                # 可选：给个轻量反馈，去掉“加载中”状态
                await cls.safe_callback_answer(callback)




    @classmethod
    async def auto_self_delete(cls, message, delay_seconds: int = 5):
        try:
            await asyncio.sleep(delay_seconds)
            await message.delete()
        except Exception:
            pass
        


    @classmethod
    async def _build_mediagroup_box(cls, page, source_id,content_id,material_status):
        show_quantity = 0
        if material_status:
            total_quantity = material_status.get("total", 0)
            box_dict = material_status.get("box", {})  # dict: {1:{...}, 2:{...}}
            # 盒子数量（组数）
            box_quantity = len(box_dict)  

            print(f"box={box_dict}")

            # 生成 1..N 号按钮；每行 5 个
            rows_kb: list[list[InlineKeyboardButton]] = []
            current_row: list[InlineKeyboardButton] = []

            # 若想按序号排序，确保顺序一致
            for box_id, meta in sorted(box_dict.items(), key=lambda kv: kv[0]):
                quantity = int(meta.get("quantity", 0))

                # if box_id == page:
                #     show_tag = "✅ "
                # else:
                #     show_tag = "✅ " if meta.get("show") else ""
                
                if (meta.get("show")) or (box_id == page):
                    show_quantity += quantity
                    show_tag = "✅ "
                    current_row.append(
                        InlineKeyboardButton(
                            text=f"{show_tag}{box_id}",
                            callback_data=f"nothing:{content_id}:{box_id}:{quantity}"  # 带上组号
                        )
                    )
                else:
                    current_row.append(
                        InlineKeyboardButton(
                            text=f"{box_id}",
                            callback_data=f"media_box:{content_id}:{box_id}:{quantity}"  # 带上组号
                        )
                    )

                # current_row.append(
                #     InlineKeyboardButton(
                #         text=f"{show_tag}{box_id}",
                #         callback_data=f"media_box:{content_id}:{box_id}:{quantity}"  # 带上组号
                #     )
                # )
                if len(current_row) == 5:
                    rows_kb.append(current_row)
                    current_row = []

            # 收尾：剩余不足 5 个的一行
            if current_row:
                rows_kb.append(current_row)

            # 追加反馈按钮（单独一行）
            rows_kb.append([
                InlineKeyboardButton(
                    text="⚠️ 我要打假",
                    url=f"https://t.me/{UPLOADER_BOT_NAME}?start=s_{source_id}"
                )
            ])

            feedback_kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)

            # 计算页数：每页 10 个（与你 send_media_group 的分组一致）
            # 避免整除时多 +1，用 (total+9)//10 或 math.ceil
           
            text = f"💡当前 {show_quantity}/{total_quantity} 个，第 {box_id} / {box_quantity} 页"
            return { "feedback_kb": feedback_kb, "text": text}
        
    

        

''''''