import asyncio
import os
import time
import aiogram
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.filters import Command  # ✅ v3 filter 写法



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
from telethon.tl.types import InputDocument
from telethon import events


from telethon.tl.functions.photos import DeletePhotosRequest
from telethon.tl.types import InputPhoto
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.account import UpdateUsernameRequest
from telethon.tl.functions.channels import InviteToChannelRequest, TogglePreHistoryHiddenRequest,LeaveChannelRequest
from telethon.errors import ChannelPrivateError




lz_var.start_time = time.time()
lz_var.cold_start_flag = True

if SESSION_STRING:
    # print("【Telethon】使用 StringSession 登录。",flush=True)
    user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    
else:
    # print("【Telethon】使用 USER_SESSION 登录。",flush=True)
    user_client = TelegramClient(USER_SESSION, API_ID, API_HASH)

lz_var.user_client = user_client  # ✅ 赋值给 lz_var 让其他模块能引用



# ================= 9. 私聊媒体处理：人类账号 =================
@user_client.on(events.NewMessage(incoming=True))
async def handle_user_private_media(event):
    # print(f"【Telethon】收到私聊媒体：{event.message.media}，来自 {event.message.from_id}",flush=True)
    print(f"【Telethon】收到私聊消息",flush=True)
    msg = event.message
    if not msg.is_private:
        return

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

   

   
    await user_client.start(PHONE_NUMBER)
    
    # 10.2 并行运行 Telethon 与 Aiogram
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
        return

    try:
        man_me = await user_client.get_me()
        lz_var.man_bot_id = man_me.id
        print(f"✅ 【Telethon】人类账号 {man_me.id} {man_me.username} 已启动。", flush=True)
    except Exception as e:
        print(f"❌ 无法获取人类账号信息：{e}", flush=True)

    dp = Dispatcher()
    dp.include_router(lz_search_highlighted.router)
    dp.include_router(lz_media_parser.router)  # ✅ 注册你的新功能模块
    dp.include_router(lz_menu.router)

    await db.connect()
    await MySQLPool.init_pool()  # ✅ 初始化 MySQL 连接池

    # ✅ Telegram /ping 指令（aiogram v3 正确写法）
    @dp.message(Command(commands=["ping", "status"]))
    async def check_status(message: types.Message):
        uptime = int(time.time() - lz_var.start_time)
        await message.reply(f"✅ Bot 已运行 {uptime} 秒，目前状态良好。")

    if BOT_MODE == "webhook":
        dp.startup.register(on_startup)
        print("🚀 啟動 Webhook 模式")

        app = web.Application()
        app.router.add_get("/", health)  # ✅ 健康检查路由

        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)

        # ✅ Render 环境用 PORT，否则本地用 8080
        port = int(os.environ.get("PORT", 8080))
        await web._run_app(app, host="0.0.0.0", port=port)
    else:
        print("🚀 啟動 Polling 模式")
        await db.connect()
        await dp.start_polling(bot, polling_timeout=10.0)


    # 理论上 Aiogram 轮询不会退出，若退出则让 Telethon 同样停止
    # task_telethon.cancel()

if __name__ == "__main__":
    print("🟡 Cold start in progress...")
    asyncio.run(main())
    print(f"✅ Bot cold started in {int(time.time() - lz_var.start_time)} 秒")

