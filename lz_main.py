

import asyncio
import os
import time
import aiogram
import json
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.filters import Command  # ✅ v3 filter 写法

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from lz_config import BOT_TOKEN, BOT_MODE, WEBHOOK_PATH, WEBHOOK_HOST,AES_KEY,SESSION_STRING,USER_SESSION, API_ID, API_HASH, PHONE_NUMBER
from lz_db import db
from lz_mysql import MySQLPool

from handlers import lz_media_parser, lz_search_highlighted
from handlers import lz_menu

import lz_var
import re


from lz_redis import RedisManager
lz_var.redis_manager = RedisManager()
#
from telethon.sessions import StringSession
from telethon import TelegramClient, events


from telethon.tl.functions.photos import DeletePhotosRequest
from telethon.tl.types import InputPhoto
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.account import UpdateUsernameRequest


lz_var.start_time = time.time()
lz_var.cold_start_flag = True

class LzFSM(StatesGroup):
    waiting_for_x_media = State(state="lz:waiting_for_x_media")



if SESSION_STRING:
    print("【Telethon】使用 StringSession 登录。",flush=True)
    user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    
else:
    print("【Telethon】使用 USER_SESSION 登录。",flush=True)
    user_client = TelegramClient(USER_SESSION, API_ID, API_HASH)

lz_var.user_client = user_client  # ✅ 赋值给 lz_var 让其他模块能引用
lz_var.skins = {}  # 皮肤配置


# ================= 9. 私聊媒体处理：人类账号 =================
@user_client.on(events.NewMessage(incoming=True))
async def handle_user_private_media(event):
    # print(f"【Telethon】收到私聊媒体：{event.message.media}，来自 {event.message.from_id}",flush=True)
    
    msg = event.message
    if not msg.is_private:
        return
    
    file_type =''
    media = None
    if msg.document:
        media = msg.document
        file_type = 'document'
    elif msg.video:
        media = msg.video
        file_type = 'video'
    elif msg.photo:
        media = msg.photo
        file_type = 'photo'
    # elif msg.text:
    #     media = msg.text
    #     file_type = 'text'        
    #     pass


    print(f"【Telethon】收到私聊消息 {event.message.text} {file_type}",flush=True)

    # 转发到群组，并删除私聊
    if media:
        print(f"{lz_var.bot_id} {media}")
        ret = await user_client.send_file(lz_var.bot_username, media)
        
    elif msg.text:
        try:
            match = re.search(r'\|_kick_\|\s*(.*?)\s*(bot)', msg.text, re.IGNORECASE)
            if match:
                botname = match.group(1) + match.group(2)
                await user_client.send_message(botname, "/start")
                # await user_client.send_message(botname, "[~bot~]")
                
        except Exception as e:
                print(f"Error kicking bot: {e} {botname}", flush=True)
    
    
    await event.delete()





FILE_ID_REGEX = re.compile(
    r'(?:file_id\s*[:=]\s*)?([A-Za-z0-9_-]{30,})'
)

from aiogram import Router, F
router = Router()


async def load_or_create_skins(if_del: bool = False, config_path: str = "skins.json") -> dict:
    """
    启动时加载皮肤配置（不依赖 YAML）。
    - 若文件存在则载入。
    - 若不存在则从 default_skins 生成。
    - 若有 file_unique_id 但 file_id 为空，会调用 get_file_id_by_file_unique_id() 取得。
    """
    import lz_var
    from lz_db import db


    default_skins = {
        "home":    {"file_id": "", "file_unique_id": "AQADHwtrG8puoUd-"},  # Luzai02bot 的默认封面
        "loading": {"file_id": "", "file_unique_id": "AgADcAYAAtiwqUc"},
        "clt_menu":     {"file_id": "", "file_unique_id": "AQAD2wtrG-sSiUd-"},  # Luzai01bot 的默认封面
        "clt_my":  {"file_id": "", "file_unique_id": "AQADzAtrG-sSiUd-"},
        "clt_fav": {"file_id": "", "file_unique_id": "AQAD1wtrG-sSiUd-"},
        "clt_cover": {"file_id": "", "file_unique_id": "AQADHgtrG8puoUd-"},
        "clt_market": {"file_id": "", "file_unique_id": "AQAD2AtrG-sSiUd-"},  # Luzai03bot 的默认封面
        "history": {"file_id": "", "file_unique_id": "AQAD6AtrG-sSiUd-"},
        "history_update": {"file_id": "", "file_unique_id": "AQAD4wtrG-sSiUd-"},
        "history_redeem": {"file_id": "", "file_unique_id": "AQAD5wtrG-sSiUd-"},
        "search": {"file_id": "", "file_unique_id": "AQADGgtrG8puoUd-"},
        "search_keyword": {"file_id": "", "file_unique_id": "AQADHAtrG8puoUd-"},
        "search_tag": {"file_id": "", "file_unique_id": "AQADHQtrG8puoUd-"},
        "ranking": {"file_id": "", "file_unique_id": "AQADCwtrG9iwoUd-"},
        "ranking_resource": {"file_id": "", "file_unique_id": "AQADDQtrG9iwoUd-"},
        "ranking_uploader": {"file_id": "", "file_unique_id": "AQADDgtrG9iwoUd-"},
        "product_cover1": {"file_id": "", "file_unique_id": "AQADMK0xG4g4QEV-"},
        "product_cover2": {"file_id": "", "file_unique_id": "AQADMq0xG4g4QEV-"},
        "product_cover3": {"file_id": "", "file_unique_id": "AQADMa0xG4g4QEV-"}
    }

    # 移除文件 config_path
    if os.path.exists(config_path) and if_del:
        os.remove(config_path)

    # --- 若已有文件，直接载入 ---
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                skins = json.load(f)
            # print(f"✅ 已载入 {config_path}（共 {len(skins)} 项）")
        except Exception as e:
            print(f"⚠️ 无法读取 {config_path}，将重新生成：{e}")
            skins = default_skins.copy()
    else:
        skins = default_skins.copy()

    # --- 若 file_id 为空，尝试用数据库补齐 ---
    for name, obj in skins.items():
        if not obj.get("file_id") and obj.get("file_unique_id"):
            fu = obj["file_unique_id"]
            print(f"🔍 {name}: file_id 为空，尝试从数据库查询…（{fu}）")
            try:
                file_ids = await db.get_file_id_by_file_unique_id([fu])
                if file_ids:
                    obj["file_id"] = file_ids[0]
                    print(f"✅ 已从数据库补齐 {name}: {obj['file_id']}")
                else:
                    print(f"⚠️ 数据库未找到 {fu} 对应的 file_id")
            except Exception as e:
                print(f"❌ 查询 file_id 出错：{e}")

    # --- 若仍有 file_id 为空，尝试向 x-man 询问 ---
    need_fix = [(k, v) for k, v in skins.items() if not v.get("file_id") and v.get("file_unique_id")]
    for name, obj in need_fix:
        fu = obj["file_unique_id"]
        print(f"🧾 {name}: 向 x-man {lz_var.x_man_bot_id} 请求 file_id…（{fu}）")
        try:
            msg = await lz_var.bot.send_message(chat_id=lz_var.x_man_bot_id, text=f"{fu}")
            print(f"📨 已请求 {fu}，并已接收返回",flush=True)
        except Exception as e:
            print(f"⚠️ 向 x-man 请求失败：{e}",flush=True)

    # --- 写入文件（即便有缺） ---
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(skins, f, ensure_ascii=False, indent=4)
    # print(f"💾 已写入 {config_path}")

    lz_var.default_thumb_file_id = [
       skins.get("product_cover1", {}).get("file_id", ""),
       skins.get("product_cover2", {}).get("file_id", ""),
       skins.get("product_cover3", {}).get("file_id", ""), 
    ]
    
    return skins

async def on_startup(bot: Bot):
    webhook_url = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
    print(f"🔗 設定 Telegram webhook 為：{webhook_url}")
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(webhook_url)
    
    
    lz_var.cold_start_flag = False  # 启动完成

async def health(request):
    uptime = time.time() - lz_var.start_time
    if lz_var.cold_start_flag or uptime < 10:
        return web.Response(text="⏳ Bot 正在唤醒，请稍候...", status=503)
    return web.Response(text="✅ Bot 正常运行", status=200)

async def delete_my_profile_photos(client):
    photos = await client.get_profile_photos('me')

    if not photos:
        print("你没有设置头像。")
        return

    input_photos = []
    for photo in photos:
        if hasattr(photo, 'id') and hasattr(photo, 'access_hash') and hasattr(photo, 'file_reference'):
            input_photos.append(InputPhoto(
                id=photo.id,
                access_hash=photo.access_hash,
                file_reference=photo.file_reference
            ))

    await client(DeletePhotosRequest(id=input_photos))
    print("头像已删除。")

async def update_my_name(client, first_name, last_name=''):
    await client(UpdateProfileRequest(first_name=first_name, last_name=last_name))
    print(f"已更新用户姓名为：{first_name} {last_name}")

async def update_username(client,username):
    try:
        await client(UpdateUsernameRequest(username))  # 设置空字符串即为移除
        print("用户名已成功变更。")
    except Exception as e:
        print(f"变更失败：{e}")


async def main():
    # 10.2 并行运行 Telethon 与 Aiogram
    await user_client.start(PHONE_NUMBER)
    task_telethon = asyncio.create_task(user_client.run_until_disconnected())
   
    # await delete_my_profile_photos(user_client)
    # await update_my_name(user_client,'Luzai', 'Man')
    # await update_username(user_client,"luzai09man")

    # import aiohttp
    # from aiogram import Bot
    
    # # 禁用 SSL 验证（仅开发环境调试时使用）
    # connector = aiohttp.TCPConnector(ssl=False)

    # # 增加超时到 60 秒
    # timeout = aiohttp.ClientTimeout(total=60)

    # session = aiohttp.ClientSession(connector=connector, timeout=timeout)
   
    # bot = Bot(token=BOT_TOKEN, session=session)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    

    
     # ✅ 赋值给 lz_var 让其他模块能引用
    lz_var.bot = bot
    try:
        me = await bot.get_me()
        lz_var.bot_username = me.username
        lz_var.bot_id = me.id
        print(f"✅ Bot {me.id} - {me.username} 已启动", flush=True)
    except Exception as e:
        print(f"❌ 无法获取 Bot 信息：{e}", flush=True)
        # 记得把 Telethon 停掉
        await user_client.disconnect()
        await bot.session.close()
        return

    try:
        man_me = await user_client.get_me()
        lz_var.man_bot_id = man_me.id
        print(f"✅ 【Telethon】人类账号 {man_me.id} {man_me.username} 已启动。", flush=True)
    except Exception as e:
        print(f"❌ 无法获取人类账号信息：{e}", flush=True)

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(lz_search_highlighted.router)
    dp.include_router(lz_media_parser.router)  # ✅ 注册你的新功能模块
    dp.include_router(lz_menu.router)

    # ✅ 统一在这里连一次
    # await db.connect()
    # await MySQLPool.init_pool()  # ✅ 初始化 MySQL 连接池

    await asyncio.gather(
        db.connect(),            # PostgreSQL
        MySQLPool.init_pool(),   # MySQL
    )

   

    # ✅ 注册 shutdown 钩子：无论 webhook/polling，退出时都能清理
    @dp.shutdown()
    async def _on_shutdown():
        try:
            await db.disconnect()
        except Exception as e:
            print(f"[shutdown] PG disconnect error: {e}")
        try:
            await MySQLPool.close()
        except Exception as e:
            print(f"[shutdown] MySQL close error: {e}")
        try:
            await bot.session.close()
        except Exception as e:
            print(f"[shutdown] Bot session close error: {e}")
        try:
            await user_client.disconnect()
        except Exception as e:
            print(f"[shutdown] Telethon disconnect error: {e}")


    # ✅ Telegram /ping 指令（aiogram v3 正确写法）
    @dp.message(Command(commands=["ping", "status"]))
    async def check_status(message: types.Message):
        uptime = int(time.time() - lz_var.start_time)
        await message.reply(f"✅ Bot 已运行 {uptime} 秒，目前状态良好。")
    try:
        if BOT_MODE == "webhook":
            dp.startup.register(on_startup)
            print("🚀 啟動 Webhook 模式")

            app = web.Application()
            app.router.add_get("/", health)  # ✅ 健康检查路由

            SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
            setup_application(app, dp, bot=bot)

            # ✅ Render 环境用 PORT，否则本地用 8080
            lz_var.skins = await load_or_create_skins()
            # print(f"Skin {lz_var.skins}")
            port = int(os.environ.get("PORT", 8080))
            await web._run_app(app, host="0.0.0.0", port=port)
            
        else:
            print("🚀 啟動 Polling 模式")
            lz_var.skins = await load_or_create_skins()
            # print(f"Skin {lz_var.skins}")
            await dp.start_polling(bot, polling_timeout=10.0)
        
        
    finally:
         # 双保险：若没走到 @dp.shutdown（例如异常中断），也清理资源
        try:
            await db.disconnect()
        except Exception:
            pass
        try:
            await MySQLPool.close()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        try:
            await user_client.disconnect()
        except Exception:
            pass
        # 如果你还留着 task_telethon：
        if not task_telethon.done():
            task_telethon.cancel()       


    # 理论上 Aiogram 轮询不会退出，若退出则让 Telethon 同样停止
    # task_telethon.cancel()

if __name__ == "__main__":
    print("🟡 Cold start in progress...")
    asyncio.run(main())
    print(f"✅ Bot cold started in {int(time.time() - lz_var.start_time)} 秒")

