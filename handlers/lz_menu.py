from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.utils.text_decorations import markdown_decoration

from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.exceptions import TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter


from utils.unit_converter import UnitConverter

from utils.aes_crypto import AESCrypto
from lz_db import db
from lz_config import AES_KEY
import lz_var
import traceback
import random

from lz_mysql import MySQLPool

router = Router()

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
async def handle_search_by_id(message: Message, command: Command = Command("id")):
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        # ✅ 调用并解包返回的三个值
        # ret_content, [file_id, thumb_file_id], [owner_user_id] = await load_sora_content_by_id(int(args[1]))

        result = await load_sora_content_by_id(int(args[1]))
        print("Returned==>:", result)

        ret_content, file_info, user_info = result
        source_id = file_info[0] if len(file_info) > 0 else None
        file_type = file_info[1] if len(file_info) > 1 else None
        file_id = file_info[2] if len(file_info) > 2 else None
        thumb_file_id = file_info[3] if len(file_info) > 3 else None
        owner_user_id = user_info[0] if user_info else None


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
async def handle_start(message: Message, command: Command = Command("start")):
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
               
               
               
                # ✅ 调用并解包返回的三个值
                ret_content, [source_id, file_type,file_id, thumb_file_id], [owner_user_id] = await load_sora_content_by_id(content_id, search_key_index)
                print(f"thumb_file_id:{thumb_file_id}")
                # ✅ 检查是否找不到资源（根据返回第一个值）
                if ret_content.startswith("⚠️"):
                    await message.answer(ret_content, parse_mode="HTML")
                    return

                # ✅ 发送带封面图的消息
                print(f"{thumb_file_id}")
                # print(f"{file_id}")
                await message.answer_photo(
                    photo=thumb_file_id,
                    caption=ret_content,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [
                          
                            # InlineKeyboardButton(text="⬅️", callback_data=f"sora_page:{search_key_index}:0:-1"),
                            InlineKeyboardButton(text="💎 60", callback_data=f"sora_redeem:{content_id}"),
                            # InlineKeyboardButton(text="➡️", callback_data=f"sora_page:{search_key_index}:0:1"),

                        ]
                        # ,
                        # [
                        #     InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                        # ]
                    ])
                )

            except Exception as e:
                # tb = traceback.format_exc()
                await message.answer("😼 正在从院长的硬盘把这个资源上传上来，这段时间还是先看看别的资源吧")
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                print(f"❌ 解密失败：{e}", flush=True)
        else:
            await message.answer(f"📦 你提供的参数是：`{param}`", parse_mode="HTML")
    else:
        #await message.answer("👋 欢迎使用 LZ 机器人！请选择操作：", reply_markup=main_menu_keyboard())
        pass


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

# == 合集选项响应 ==
@router.callback_query(F.data == "my_collections")
async def handle_my_collections(callback: CallbackQuery):
    await callback.message.answer("📦 这里是你创建的合集：...")

@router.callback_query(F.data == "my_favorite_collections")
async def handle_my_favorite_collections(callback: CallbackQuery):
    await callback.message.answer("❤️ 这里是你收藏的他人合集：...")

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
async def handle_sora_page(callback: CallbackQuery):
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
        ret_content, [source_id, file_type, file_id, thumb_file_id], [owner_user_id] = await load_sora_content_by_id(next_content_id, search_key_index)

        if ret_content.startswith("⚠️"):
            await callback.answer(ret_content, show_alert=True)
            return

        await callback.message.edit_media(
            media={
                "type": "photo",
                "media": thumb_file_id,
                "caption": ret_content,
                "parse_mode": "HTML"
            },
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    # InlineKeyboardButton(text="⬅️", callback_data=f"sora_page:{search_key_index}:{new_pos}:-1"),
                    InlineKeyboardButton(text="💎 60", callback_data=f"sora_redeem:{next_content_id}"),
                    # InlineKeyboardButton(text="➡️", callback_data=f"sora_page:{search_key_index}:{new_pos}:1"),
                ],
                # [
                #     InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                # ]
            ])
        )
        await callback.answer()

    except Exception as e:
        print(f"❌ handle_sora_page error: {e}")
        await callback.answer("⚠️ 翻页失败", show_alert=True)





@router.callback_query(F.data.startswith("sora_redeem:"))
async def handle_redeem(callback: CallbackQuery):
    source_id = callback.data.split(":")[1]
    


    result = await load_sora_content_by_id(int(source_id))
    # print("Returned==>:", result)

    ret_content, file_info, user_info = result
    source_id = file_info[0] if len(file_info) > 0 else None
    file_type = file_info[1] if len(file_info) > 1 else None
    file_id = file_info[2] if len(file_info) > 2 else None
    thumb_file_id = file_info[3] if len(file_info) > 3 else None

    owner_user_id = user_info[0] if user_info else None

    

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
    sender_fee = -60  # ✅ 发送者手续费
    result = await MySQLPool.transaction_log({
        'sender_id': from_user_id,
        'receiver_id': owner_user_id or 0,
        'transaction_type': 'confirm_buy',
        'transaction_description': source_id,
        'sender_fee': sender_fee,
        'receiver_fee': 6
    })

    user_info = result.get('user_info', {})
    user_point = int(user_info.get('point', 0))

    if result.get('status') == 'exist' or result.get('status') == 'insert':

        if result.get('status') == 'exist':
            reply_text = f"✅ 你已经兑换过此资源，不需要扣除积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {user_point}。"
        elif result.get('status') == 'insert':
            
            reply_text = f"✅ 兑换成功，已扣除 {sender_fee} 积分"
            if user_point > 0:
                reply_text += f"，当前积分余额: {(user_point-sender_fee)}。"

        try:
            if file_type == "photo" or file_type == "p":
                await lz_var.bot.send_photo(chat_id=from_user_id, photo=file_id,reply_to_message_id=callback.message.message_id)
            elif file_type == "video" or file_type == "v":
                await lz_var.bot.send_video(chat_id=from_user_id, video=file_id, reply_to_message_id=callback.message.message_id)
            elif file_type == "document" or file_type == "d":
                await lz_var.bot.send_document(chat_id=from_user_id, document=file_id, reply_to_message_id=callback.message.message_id)
        except Exception as e:
            print(f"❌ 目标 chat 不存在或无法访问: {e}")

        await callback.answer(reply_text, show_alert=True)
        
        return
    elif result.get('status') == 'insufficient_funds':
       
        reply_text = f"❌ 你的积分不足 ({user_point}) ，无法兑换此资源 ({sender_fee})。"
        await callback.answer(reply_text, show_alert=True)
        # await callback.message.reply(reply_text, parse_mode="HTML")
        return
        

   
    




    

# 📌 功能函数：根据 sora_content id 载入资源
async def load_sora_content_by_id(content_id: int, search_key_index=None) -> str:
    convert = UnitConverter()  # ✅ 实例化转换器
    record = await db.search_sora_content_by_id(content_id)
    if record:
        
         # 取出字段，并做基本安全处理
       
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
        if not thumb_file_id:
            print(f"🔍 没有找到 thumb_file_id，尝试从 thumb_file_unique_id {thumb_file_unique_id} 获取")
            retSend = await MySQLPool.fetch_file_by_file_id(thumb_file_unique_id)
           
            if retSend !=None and retSend.photo:
                largest_photo = max(retSend.photo, key=lambda p: p.file_size or 0)
                thumb_file_id = largest_photo.file_id
                thumb_file_unique_id = largest_photo.file_unique_id
                print("✅ file_id:", thumb_file_id)
                try:
                    await lz_var.bot.send_message(
                        chat_id=lz_var.sungfeng,
                        text=f"|_ask_|{record_id}@{lz_var.bot_username}"
                    )
                except Exception as e:
                    pass


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
                    # 遍历 lz_var.default_thumb_unique_file_ids
                    for unique_id in lz_var.default_thumb_unique_file_ids:
                        await MySQLPool.fetch_file_by_file_id(unique_id)
                    # 处理找不到的情况
                    print("❌ 没有找到 default_thumb_unique_file_ids,增加扩展库中")


        ret_content = ""
        tag_length = 0
        max_total_length = 1000  # 预留一点安全余地，不用满 1024
               
        if tag:
            ret_content += f"{record['tag']}\n\n"

        profile = ""
        if file_size:
            print(f"🔍 资源大小: {file_size}")
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
    



        if not file_id:
            print(f"🔍 没有找到 file_id，尝试从 source_id {source_id} 获取")
            await MySQLPool.fetch_file_by_file_id(source_id)

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
        return ret_content, [source_id, file_type, file_id, thumb_file_id], [None]
        
    else:
        return f"⚠️ 没有找到 ID 为 {content_id} 的 Sora 内容记录"