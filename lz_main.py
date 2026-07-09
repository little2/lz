

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
import random
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram import BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.filters import Command  # ✅ v3 filter 写法
import textwrap
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from lz_config import ADMIN_IDS, BOT_TOKEN,SWITCHBOT_TOKEN, SWITCHBOT_CHAT_ID, SWITCHBOT_THREAD_ID,BOT_MODE, WEBHOOK_PATH, WEBHOOK_HOST,KEY_USER_ID
# from lz_db import db
from lz_pgsql import PGPool
from lz_mysql import MySQLPool
from utils.tpl import Tplate

from handlers import lz_media_parser
from handlers import lz_menu



import lz_var
import re


lz_var.start_time = time.time()
lz_var.cold_start_flag = True

class LzFSM(StatesGroup):
    waiting_for_x_media = State(state="lz:waiting_for_x_media")


lz_var.skins = {}  # 皮肤配置

from shared_config import SharedConfig
SharedConfig.load(True)


async def periodic_shared_config_reload(interval_seconds: int = 3600, on_reload=None):
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await asyncio.to_thread(SharedConfig.load, True)
            if on_reload:
                on_reload()
            print("✅ SharedConfig 已定时刷新", flush=True)
        except Exception as e:
            print(f"⚠️ SharedConfig 定时刷新失败: {e}", flush=True)


def create_user_client():
    from lz_config import API_HASH, API_ID, SESSION_STRING, USER_SESSION
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    if SESSION_STRING:
        print("【Telethon】使用 StringSession 登录。", flush=True)
        return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        print("【Telethon】使用 USER_SESSION 登录。", flush=True)
        return TelegramClient(USER_SESSION, API_ID, API_HASH)





async def handle_user_private_media(event):
    try:
        msg = event.message  # ✅ 先取 msg
    except Exception as e:
        print(f"❌ 读取消息失败：{e}", flush=True)
        return

    # 安全打印：text 可能为 None
    text = msg.raw_text or ""
    # print(f"【Telethon】NewMessage private={msg.is_private} text={text!r}", flush=True)

    # 1) 群消息：只在有 text 时才做关键字匹配
    if not msg.is_private:

        return

    # 2) 私聊媒体识别
    file_type = ""
    media = None
    if msg.document:
        media = msg.document
        file_type = "document"
    elif msg.video:
        media = msg.video
        file_type = "video"
    elif msg.photo:
        media = msg.photo
        file_type = "photo"

    print(f"【Telethon】私聊收到：type={file_type} text={text!r}", flush=True)

    # 2.1 私聊媒体：转发
    if media:
        if not getattr(lz_var, "bot_username", None):
            print("⚠️ bot_username 未就绪，跳过转发", flush=True)
            return
        try:
            await lz_var.user_client.send_file(lz_var.bot_username, media)
        except Exception as e:
            # 处理受保护聊天无法转发的情况
            if "ChatForwardsRestrictedError" in str(type(e).__name__):
                print(f"⚠️ 无法转发受保护聊天的消息: {e}", flush=True)
            else:
                print(f"⚠️ 转发消息失败：{e}", flush=True)
            return
        try:
            await event.delete()
        except Exception as e:
            print(f"⚠️ 删除私聊消息失败：{e}", flush=True)
        return

    # 2.2 私聊文本：kick bot（可选）
    if text:
        match = re.search(r"\|_kick_\|\s*(.*?)\s*(bot)", text, re.IGNORECASE)
        if match:
            botname = match.group(1) + match.group(2)
            await lz_var.user_client.send_message(botname, "/start")
            try:
                await event.delete()
            except Exception as e:
                print(f"⚠️ 删除私聊消息失败：{e}", flush=True)


# def register_telethon_handlers(client):
#     client.add_event_handler(handle_user_private_media, events.NewMessage(incoming=True))



FILE_ID_REGEX = re.compile(
    r'(?:file_id\s*[:=]\s*)?([A-Za-z0-9_-]{30,})'
)

class BlacklistGuardMiddleware(BaseMiddleware):
    def __init__(self, whitelist_matrix: dict[str, set[int]] | None = None):
        super().__init__()
        self.whitelist_matrix = whitelist_matrix or {}

    def _is_whitelisted(self, user_id: int) -> bool:
        for user_ids in self.whitelist_matrix.values():
            if user_id in user_ids:
                return True
        return False

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)

        # 白名单矩阵命中：直接放行，不做黑名单和今日发言校验
        if self._is_whitelisted(user.id):
            return await handler(event, data)



        # 仅在私聊场景做“今日发言”校验，避免影响群里正常发言累积。
        chat = getattr(event, "chat", None)
        if isinstance(event, types.CallbackQuery) and getattr(event, "message", None):
            chat = event.message.chat

        if chat and getattr(chat, "type", None) == "private":

            block_code =  await MySQLPool.is_user_blacklisted(user.id)
            if block_code is not None and block_code > 0:
                if block_code == 4:
                    reason = "基于社群安全考量，服务暂时暂停。"
                elif block_code == 5:
                    reason = "信用点数小于 5，暂无法使用服务。"
                elif block_code == 6:
                    reason = "已退出发言积分计划，服务同步暂停。"
                elif block_code == 7:
                    reason = "资源暂未达期待，先暂停服务以免打扰。"
                elif block_code == 8:
                    reason = "多次自删空水群信息，基于社群信任考量，服务暂时暂停。"
                else:
                    reason = "服务暂时暂停，请联系教务处小助手。"
               
                if isinstance(event, types.CallbackQuery):
                    await event.answer(reason, show_alert=True)
                elif isinstance(event, types.Message):
                    await event.answer(reason)
                return


           
            chat_cfg = SharedConfig.get("chat") or {}
            public_school = chat_cfg.get("public") or {}

            # 直接重新賦值，後續整個檔案用的 TARGET_CHAT_ID 都會是新值
            main_group_url = str(public_school.get("invite_link") or "")
            guider_bot_name = str(SharedConfig.get("guider_bot_name") or "")
        
            faq_url = f"https://t.me/{guider_bot_name}?start=faq" if guider_bot_name else ""
            rule_url = f"https://t.me/{guider_bot_name}?start=show_inst_points" if guider_bot_name else ""
           

            sgt_now = datetime.now(timezone.utc) + timedelta(hours=8)
            stat_date = sgt_now.strftime("%Y-%m-%d")
            if not await MySQLPool.has_spoken_today(user.id, stat_date,redis_url=SharedConfig.get("valkey_url")):
               
                helper_bot_url = f"https://t.me/{getattr(lz_var, 'helper_bot_name', '')}"
                keyboard = types.InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            types.InlineKeyboardButton(text="🐲 公开群🌱", url=main_group_url),
                            types.InlineKeyboardButton(text="🎓 教务处小助手", url=faq_url)
                        ],
                        [
                            types.InlineKeyboardButton(text="📐 积分规则说明🌱", url=rule_url)
                        ]
                    ]
                )

                

                slogans = [
                    "学院不是一个群名，是大家愿意回来待着的地方。",
                    "在学院待久了，多少都会有点感情。",
                    "学院的热闹，也有我参与的一份。",
                    "学院里有同学说话，气氛就会更踏实。",
                    "能一直留在学院的，多少都算自己人。",
                    "我也是学院的一份子。",
                    "学院多一点互动，气氛就会更像自己人。",
                    "在学院多聊两句，小圈圈就会多一点温度。",
                    "学院这个小圈圈能走下去，靠的是大家多少都有参与。",
                    "学院不是靠一句口号撑起来的，是靠同学们真的愿意参与。",
                    "留在学院久了，就会慢慢知道这里为什么值得留下。",
                    "龙阳能留下来的，都是愿意一起撑这个小圈圈的师兄弟。",
                    "在龙阳混久了，多少都会有一点师兄弟的感觉。",
                    "龙阳不是路过的地方，愿意留下来才会有感情。",
                    "龙阳的熟悉感，是师兄弟们一次次冒泡慢慢养出来的。",
                ]

                random_slogan = random.choice(slogans)

               
                notice_text = textwrap.dedent(f"""
                    「踢鸡大佬」越来越凶，学院/机器人也被请去「喝茶」好几轮了。

                    目前使用学院服务时，每天需在中心或学院群完成 2 句有效发言，成效不错，同学们也反应稳定多了。

                    这不是怀疑大家，而是希望同学们证明一下：自己是会互动的同学，不是只下载、不说话的潜水狼。

                    接下来还会继续优化机制，在方便与安全之间找平衡。

                    🎈 温馨提醒：

                    1. 任何建议或批评，请直接到「教务处小助手」反馈。群里消息贼多，校工们不一定会看到，不是不理你，是可能真的被刷过去了。
                    2. 请不要为了凑数发送抱怨、报数，或其他没有交流意义的内容。有效发言的重点是「交流」，不是完成每日打卡 KPI。
                    3. 详细规则可以参考「积分规则说明」 ，避免一不小心从群友变成规则题的错误示范。
                    4. 真不知道要说什么？允许你说一句「 <code>{random_slogan}</code> 」
                """).strip()


                if isinstance(event, types.CallbackQuery):
                    # await event.answer(notice_text, show_alert=True)
                    await event.message.answer(notice_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
                elif isinstance(event, types.Message):
                    await event.answer(notice_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
                return

        return await handler(event, data)

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
    from telethon.tl.functions.photos import DeletePhotosRequest
    from telethon.tl.types import InputPhoto

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
    from telethon.tl.functions.account import UpdateProfileRequest

    await client(UpdateProfileRequest(first_name=first_name, last_name=last_name))
    print(f"已更新用户姓名为：{first_name} {last_name}")

async def update_username(client,username):
    from telethon.tl.functions.account import UpdateUsernameRequest

    try:
        await client(UpdateUsernameRequest(username))  # 设置空字符串即为移除
        print("用户名已成功变更。")
    except Exception as e:
        print(f"变更失败：{e}")

async def sync():
    while False:
        from utils.product_utils import check_file_record

        summary = await check_file_record(limit=100)
        if summary["checked"] == 0:
            break


    while False:
        from utils.product_utils import check_and_fix_sora_valid_state

        summary = await check_and_fix_sora_valid_state(limit=1000)
        if summary["checked"] == 0:
            break


    while False:
        from utils.product_utils import check_and_fix_sora_valid_state2

        summary = await check_and_fix_sora_valid_state2(limit=1000)
        if summary["checked"] == 0:
            break

async def say_hello(text:str = 'Started bot!'):
    me = await lz_var.bot.get_me()
    bot_name = me.username if me and me.username else "UnknownSwitchBot"
    bot_id = me.id if me and me.id else 0
    try:
        await lz_var.switchbot.send_message(
            chat_id=f"-100{SWITCHBOT_CHAT_ID}",
            message_thread_id=SWITCHBOT_THREAD_ID,
            text=f"[{bot_name} - {bot_id}] {text}",
        )
    except Exception as e:
        print(
            f"⚠️ say_hello 发送失败: chat_id={SWITCHBOT_CHAT_ID}, "
            f"thread_id={SWITCHBOT_THREAD_ID}, error={e}",
            flush=True,
        )


async def close_bot_session(bot_instance: Bot | None, label: str):
    if not bot_instance:
        return

    session = getattr(bot_instance, "session", None)
    if session is None or getattr(session, "closed", False):
        return

    try:
        await session.close()
    except Exception as e:
        print(f"[shutdown] {label} session close error: {e}")


# async def say_hello():
#     try:
#         await lz_var.switchbot.send_message(KEY_USER_ID, f"[LZ] <code>{lz_var.bot_username}</code> 已启动！")
#     except Exception as e:
#         print(f"⚠️ say_hello 通知失败（忽略）: {e}", flush=True)
#      # 构造一个要导入的联系人
#     # try:
#     #     target = await lz_var.user_client.get_entity(KEY_USER_ID)     # 7550420493
#     #     me = await lz_var.user_client.get_me()
#     #     await lz_var.user_client.send_message(target, f"[LZ] <code>{me.id}</code> - {me.first_name} {me.last_name or ''} {me.phone or ''}。我在执行LZ任务！",parse_mode='html')   
#     #     print(f"发送消息给 KeyMan 成功。",flush=True)
#     # except Exception as e:
#     #     print(f"发送消息给 KeyMan 失败：{e}",flush=True)

#     # try:
#     #     await lz_var.user_client.send_message(SWITCHBOT_USERNAME, f"/start",parse_mode='html')   
#     #     print(f"发送消息给 {SWITCHBOT_USERNAME} 成功。",flush=True)
#     # except Exception as e:
#     #     print(f"发送消息给 {SWITCHBOT_USERNAME} 失败：{e}",flush=True)    
  
async def main():

    # 10.2 并行运行 Telethon 与 Aiogram
    config_reload_task = None
   
    my_bot_token = SharedConfig.get("my_bot_token",BOT_TOKEN)  
    switch_bot_token = SharedConfig.get("switch_bot_token",SWITCHBOT_TOKEN)

    bot = Bot(
        token=my_bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
     # ✅ 赋值给 lz_var 让其他模块能引用
    lz_var.bot = bot

    switchbot = Bot(
        token=switch_bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    lz_var.switchbot = switchbot
    


    try:
        me = await bot.get_me()
        lz_var.bot_username = me.username
        lz_var.publish_bot_name = lz_var.bot_username
        lz_var.bot_id = me.id
        print(f"✅ Bot {me.id} - {me.username} 已启动", flush=True)
    except Exception as e:
        print(f"❌ 无法获取 Bot 信息：{e} : {my_bot_token}", flush=True)
        # 记得把 Telethon 停掉
        await close_bot_session(switchbot, "SwitchBot")
        await close_bot_session(bot, "Bot")
        return


    
    # user_client = create_user_client()
    # lz_var.user_client = user_client
    # register_telethon_handlers(user_client)

    # await user_client.start(PHONE_NUMBER)
    # task_telethon = asyncio.create_task(user_client.run_until_disconnected())

    try:
        # TODO -- 人类账号可能也不再需要
        # man_me = await user_client.get_me()
        # lz_var.man_bot_id = man_me.id
        # print(f"✅ 【Telethon】人类账号 {man_me.id} {man_me.username} 已启动。", flush=True)
        pass
    except Exception as e:
        print(f"❌ 无法获取人类账号信息：{e}", flush=True)










    dp = Dispatcher(storage=MemoryStorage())

    def _to_int_set(values):
        result = set()
        for v in values or []:
            try:
                result.add(int(v))
            except Exception:
                pass
        return result

    whitelist_matrix = {
        "core": _to_int_set([KEY_USER_ID]),
        "config": _to_int_set(SharedConfig.get("whitelist_user_ids") or []),
    }

    print(f"初始白名单矩阵：{ADMIN_IDS}", flush=True)
    ''' 将  whitelist_matrix["config"] 也加到 ADMIN_IDS'''
    # whitelist_matrix["config"].update(ADMIN_IDS)
    ADMIN_IDS.update(whitelist_matrix["config"])
    print(f"合并 ADMIN_IDS 后的白名单矩阵：{ADMIN_IDS}", flush=True)

    

    def reload_whitelist_matrix():
        whitelist_matrix["core"] = _to_int_set([KEY_USER_ID])
        whitelist_matrix["config"] = _to_int_set(SharedConfig.get("whitelist_user_ids") or [])
        # 将 ADMIN_IDS 再加入  whitelist_matrix["config"]
        # whitelist_matrix["config"].update(ADMIN_IDS)
        return whitelist_matrix

   
    blacklist_guard = BlacklistGuardMiddleware(whitelist_matrix=whitelist_matrix)
    dp.message.middleware(blacklist_guard)
    dp.callback_query.middleware(blacklist_guard)

    dp.include_router(lz_media_parser.router)  # ✅ 注册你的新功能模块
    dp.include_router(lz_menu.router)

    from handlers.handle_jieba_export import ensure_lexicon_files

    # 1) 先连 MySQL
    await MySQLPool.init_pool()

    # 2) 确保本地词库文件存在（不存在就从 MySQL 导出生成）
    await ensure_lexicon_files(output_dir=".", force=False)

    # 3) 再连 PostgreSQL（此时 db.connect 内加载 jieba 就不会跳过）
    await PGPool.init_pool()
    # await db.connect()

    await sync()
    config_reload_task = asyncio.create_task(
        periodic_shared_config_reload(on_reload=reload_whitelist_matrix)
    )

    # ✅ 注册 shutdown 钩子：无论 webhook/polling，退出时都能清理
    @dp.shutdown()
    async def _on_shutdown():
        try:
            # await db.disconnect()    
            await PGPool.close()        
        except Exception as e:
            print(f"[shutdown] PG disconnect error: {e}")
        
        try:
            await MySQLPool.close()
        except Exception as e:
            print(f"[shutdown] MySQL close error: {e}")
        await close_bot_session(switchbot, "SwitchBot")
        await close_bot_session(bot, "Bot")
        if config_reload_task and not config_reload_task.done():
            config_reload_task.cancel()
            try:
                await config_reload_task
            except asyncio.CancelledError:
                pass
        # try:
        #     await user_client.disconnect()
        # except Exception as e:
        #     print(f"[shutdown] Telethon disconnect error: {e}")

    @dp.message(Command(commands=["reload_whitelist", "reloadwhite"]))
    async def reload_whitelist(message: types.Message):
        if not message.from_user or message.from_user.id != KEY_USER_ID:
            await message.reply("❌ 没有权限重新载入白名单。")
            return

        try:
            await asyncio.to_thread(SharedConfig.load, True)
            latest_matrix = reload_whitelist_matrix()
        except Exception as e:
            await message.reply(f"❌ 白名单重新载入失败：{e}")
            return

        await message.reply(
            "✅ 白名单已重新载入\n"
            f"core: {len(latest_matrix.get('core') or set())} 人\n"
            f"config: {len(latest_matrix.get('config') or set())} 人"
        )

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
           

            load_result = await Tplate.load_or_create_skins(if_del=False, get_file_ids_fn=PGPool.get_file_id_by_file_unique_id)
            if(load_result.get("ok") == 1):
                lz_var.skins = load_result.get("skins", {})
            else:
                print(f"⚠️ 加载皮肤失败: {load_result.get('handshake')}", flush=True)
                try:
                    await lz_var.switchbot.send_message(lz_var.x_man_bot_id, f"|_kick_|@{lz_var.bot_username}")
                except Exception as _kick_err:
                    print(f"⚠️ 通知 x_man 失败（忽略）: {_kick_err}", flush=True)

            await say_hello()
            # print(f"Skin {lz_var.skins}")
            port = int(os.environ.get("PORT", 8080))
            await web._run_app(app, host="0.0.0.0", port=port)
            
        else:
            print("🚀 啟動 Polling 模式")
            

            load_result = await Tplate.load_or_create_skins(if_del=False, get_file_ids_fn=PGPool.get_file_id_by_file_unique_id)
            if(load_result.get("ok") == 1):
                lz_var.skins = load_result.get("skins", {})
            else:
                print(f"⚠️ 加载皮肤失败: {load_result.get('handshake')}", flush=True)
                try:
                    await lz_var.switchbot.send_message(lz_var.x_man_bot_id, f"|_kick_|@{lz_var.bot_username}")
                except Exception as _kick_err:
                    print(f"⚠️ 通知 x_man 失败（忽略）: {_kick_err}", flush=True)

            # print(f"Skin {lz_var.skins}")
            await say_hello()
            await dp.start_polling(bot, polling_timeout=10.0)
        
        
    finally:
         # 双保险：若没走到 @dp.shutdown（例如异常中断），也清理资源
        try:
            # await db.disconnect()
            await PGPool.close()

        except Exception:
            pass
        try:
            await MySQLPool.close()
        except Exception:
            pass
        await close_bot_session(switchbot, "SwitchBot")
        await close_bot_session(bot, "Bot")
        if config_reload_task and not config_reload_task.done():
            config_reload_task.cancel()
            try:
                await config_reload_task
            except asyncio.CancelledError:
                pass
        # try:
        #     await user_client.disconnect()
        # except Exception:
        #     pass
        # # 如果你还留着 task_telethon：
        # if not task_telethon.done():
        #     task_telethon.cancel()       


    # 理论上 Aiogram 轮询不会退出，若退出则让 Telethon 同样停止
    # task_telethon.cancel()

if __name__ == "__main__":
    asyncio.run(main())
