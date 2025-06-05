from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.utils.text_decorations import markdown_decoration

from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.exceptions import TelegramNotFound, TelegramMigrateToChat, TelegramRetryAfter


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
        print("Returned:", result)

        ret_content, file_info, user_info = result
        file_id = file_info[0] if len(file_info) > 0 else None
        thumb_file_id = file_info[1] if len(file_info) > 1 else None
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
            await fetch_by_file_id(file_id)
            print(f"🔍 完成",flush=True)


# == 启动指令 ==
@router.message(Command("start"))
async def handle_start(message: Message, command: Command = Command("start")):
    # 获取 start 后面的参数（如果有）
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        param = args[1].strip()
        if param.startswith("f_"):
            encoded = param[2:]  # 取第三位开始的内容
            try:
                aes = AESCrypto(AES_KEY)
                content_id_str = aes.aes_decode(encoded)
                
                content_id = int(content_id_str)  # ✅ 关键修正
               
               
               
                # ✅ 调用并解包返回的三个值
                ret_content, [file_id, thumb_file_id], [owner_user_id] = await load_sora_content_by_id(content_id)

                # ✅ 检查是否找不到资源（根据返回第一个值）
                if ret_content.startswith("⚠️"):
                    await message.answer(ret_content, parse_mode="HTML")
                    return

                # ✅ 发送带封面图的消息
                await message.answer_photo(
                    photo=thumb_file_id,
                    caption=ret_content,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text="⬅️", callback_data="sora_prev"),
                            InlineKeyboardButton(text="🎁 兑换", callback_data=f"sora_redeem:{file_id}"),
                            InlineKeyboardButton(text="➡️", callback_data="sora_next"),
                        ],
                        [
                            InlineKeyboardButton(text="🏠 回主目录", callback_data="go_home"),
                        ]
                    ])
                )

            except Exception as e:
                # tb = traceback.format_exc()
                await message.answer("这个资源正在上传到服务器，请先看看别的资源。")
                # await message.answer(f"⚠️ 解密失败：\n{e}\n\n详细错误:\n<pre>{tb}</pre>", parse_mode="HTML")
                print(f"❌ 解密失败：{e}", flush=True)
        else:
            await message.answer(f"📦 你提供的参数是：`{param}`", parse_mode="HTML")
    else:
        await message.answer("👋 欢迎使用 LZ 机器人！请选择操作：", reply_markup=main_menu_keyboard())


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

@router.callback_query(F.data == "sora_prev")
async def handle_prev(callback: CallbackQuery):
    await callback.answer("👈 上一页功能开发中...")

@router.callback_query(F.data == "sora_next")
async def handle_next(callback: CallbackQuery):
    await callback.answer("👉 下一页功能开发中...")

@router.callback_query(F.data.startswith("sora_redeem:"))
async def handle_redeem(callback: CallbackQuery):
    file_id = callback.data.split(":")[1]
    await callback.answer(f"🎁 你尝试兑换资源：{file_id}")

async def fetch_by_file_id(source_id: str):
    conn, cursor = await MySQLPool.get_conn_cursor()
    try:
        await cursor.execute("""
            SELECT f.file_type, f.file_id, f.bot, b.bot_id, b.bot_token
            FROM file_extension f
            LEFT JOIN bot b ON f.bot = b.bot_name
            WHERE f.file_unique_id = %s
            LIMIT 0, 1
        """, (source_id,))
        row = await cursor.fetchone()
    except Exception as e:
        print(f"⚠️ 数据库执行出错: {e}")
        row = None
    finally:
        await MySQLPool.release(conn, cursor)

    if not row:
        print("❌ 没有找到匹配记录 file_id")
        return

    try:
        await lz_var.user_client.send_message(row['bot_id'], "/start")
    except Exception as e:
        pass
        

   
    
    chat_id = lz_var.man_bot_id

    if chat_id:
        from aiogram import Bot
        mybot = Bot(token=f"{row['bot_id']}:{row['bot_token']}")
        try:
            if row["file_type"] == "photo":
                await mybot.send_photo(chat_id=chat_id, photo=row["file_id"])
            elif row["file_type"] == "video":
                await mybot.send_video(chat_id=chat_id, video=row["file_id"])
            elif row["file_type"] == "document":
                await mybot.send_document(chat_id=chat_id, document=row["file_id"])
        except Exception as e:
            print(f"❌ 目标 chat 不存在或无法访问: {e}")
        finally:
            await mybot.session.close()  # ✅ 释放连接资源

    

# 📌 功能函数：根据 sora_content id 载入资源
async def load_sora_content_by_id(content_id: int) -> str:
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
            print(f"❌ 没有找到 thumb_file_id，尝试从 thumb_file_unique_id {thumb_file_unique_id} 获取")
            await fetch_by_file_id(thumb_file_unique_id)

            # 传送消息给 @ztdthumb011bot
            result_send = None
            try:
                result_send = await lz_var.bot.send_message(
                    chat_id=lz_var.sungfeng,
                    text=f"|_ask_|{record_id}@{lz_var.bot_username}"
                )
            except TelegramNotFound as e:
                print(f"❌ 目标 chat 不存在或无法访问: {e}")
            except TelegramForbiddenError as e:
                print(f"❌ 被禁或没权限: {e}")
            except TelegramBadRequest as e:
                print(f"⚠️ BadRequest 错误: {e}")
            except TelegramAPIError as e:
                print(f"❗ 通用 Telegram 错误: {e}")
            except Exception as e:
                print(f"🔥 未知错误: {e}")

            # print(f"{result_send}")
            # print(f"🔍 发送消息给 @ztdthumb011bot: |_ask_|{record_id}@{lz_var.bot_username}")

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
                    # 处理找不到的情况
                    print("❌ 没有找到 file_id")


        ret_content = ""
        tag_length = 0
        max_total_length = 10000  # 预留一点安全余地，不用满 1024
               
        if tag:
            ret_content += f"{record['tag']}\n\n"

        if file_size:
            ret_content += f"📄 {record['file_size']}  "

        if duration:
            ret_content += f"🕙 {record['duration']}  "

        if ret_content:
            tag_length = len(ret_content)
    
        if not file_id:
            print(f"❌ 没有找到 file_id，尝试从 source_id {source_id} 获取")
            await fetch_by_file_id(source_id)

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
        return ret_content, [file_id, thumb_file_id], [None]
        
    else:
        return f"⚠️ 没有找到 ID 为 {content_id} 的 Sora 内容记录"