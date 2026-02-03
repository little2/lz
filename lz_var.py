import os
from dotenv import load_dotenv
import json

bot_username: str = None  # Telegram 机器人 username
bot_id: int = None        # Telegram 机器人 ID
man_bot_id: int = None        # Telegram 机器人 ID
start_time: float = None  # 启动时间戳
cold_start_flag: bool = True  # 是否处于冷启动
default_thumb_file_id: list[str] = None  # 但不推荐，类型不完整
sungfeng: int = 7753111936  # 顺丰快递,基本废用了


x_raw = os.getenv("X_CONFIGURATION")
x_conf = json.loads(x_raw)
x_man_bot_id: int = x_conf["x_man_bot_id"]
x_man_bot_phone: str = x_conf["x_man_bot_phone"]
x_man_bot_username: str = x_conf["x_man_bot_username"]






THUMB_ADMIN_CHAT_ID: str = "ztdthumb011bot"

helper_bot_name = 'lyjwcbot'
guider_bot_name = 'xiaolongyang002bot'
publish_bot_name: str = None 
default_thumb_unique_file_ids: list[str] = [
    "AQADMK0xG4g4QEV-",
    "AQADMq0xG4g4QEV-",
    "AQADMa0xG4g4QEV-",
]
skins: dict = {}  # 皮肤配置
bot = None  # 预留 bot 全局变量
user_client = None  # Telethon 用户客户端
redis_manager = None  # Redis client
xlj_fee = 34 #TODO 可移除
xlj_discount_rate = 0.6 # 小懒觉会员补贴率
default_point = 34  # 
configuration_chat_id = -1002030683460
configuration_thread_id = 207008