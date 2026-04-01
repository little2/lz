import os
from dotenv import load_dotenv
import json
load_dotenv(dotenv_path='.lz.env')

bot_username: str = None  # Telegram 机器人 username
bot_id: int = None        # Telegram 机器人 ID
man_bot_id: int = None        # Telegram 机器人 ID
start_time: float = None  # 启动时间戳
cold_start_flag: bool = True  # 是否处于冷启动
default_thumb_file_id: list[str] = None  # 但不推荐，类型不完整
sungfeng: int = 7753111936  # 顺丰快递,基本废用了

try:
    x_raw = os.getenv("X_CONFIGURATION")
    x_conf = json.loads(x_raw)
    x_man_bot_id: int = x_conf["x_man_bot_id"]
    x_man_bot_phone: str = x_conf["x_man_bot_phone"]
    x_man_bot_username: str = x_conf["x_man_bot_username"]
    m_man_bot_id: int = int(x_conf.get("m_man_bot_id", 0) or 0)
except Exception as e:
    print(f"⚠️ X_CONFIGURATION 解析失败: {e}; 使用默认值", flush=True)
x_bk_man_bot_id = 7606450690


THUMB_ADMIN_CHAT_ID: str = "ztdthumb011bot"

helper_bot_name = 'lyjwcbot'
guider_bot_name = 'xiaolongyang003bot'
publish_bot_name: str = None 
uploader_bot_name: str = None  #

default_thumb_unique_file_ids: list[str] = [
    "AQADMK0xG4g4QEV-",
    "AQADMq0xG4g4QEV-",
    "AQADMa0xG4g4QEV-",
]
skins: dict = {"home": {"file_id": None}}  # 皮肤配置，提供 home 默认键防止 KeyError
bot = None  # 预留 bot 全局变量
switchbot = None # 预留 switchbot 全局变量
user_client = None  # Telethon 用户客户端
redis_manager = None  # Redis client
xlj_fee = 34 #TODO 可移除
xlj_discount_rate = 0.6 # 小懒觉会员补贴率
default_point = 25  # 
default_content_len = 15
configuration_chat_id = -1002030683460
configuration_thread_id = 207008
luzaivversion=1004

s_raw = os.getenv("SWITCHBOT_CONFIGURATION")
s_conf = json.loads(s_raw)
switchbot_chat_id: int = s_conf["chat_id"]
switchbot_thread_id: int = s_conf["thread_id"]
switchbot_token: str = s_conf["switchbot_token"]
switchbot_username = s_conf["switchbot_username"]