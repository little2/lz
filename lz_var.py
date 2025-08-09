bot_username: str = None  # Telegram 机器人 username
bot_id: int = None        # Telegram 机器人 ID
man_bot_id: int = None        # Telegram 机器人 ID
start_time: float = None  # 启动时间戳
cold_start_flag: bool = True  # 是否处于冷启动
default_thumb_file_id: list[str] = None  # 但不推荐，类型不完整
sungfeng: int = 7753111936
x_man_bot_id: int = 7793315433
THUMB_ADMIN_CHAT_ID: str = "ztdthumb011bot"
default_thumb_unique_file_ids: list[str] = [
    "AQADMK0xG4g4QEV-",
    "AQADMq0xG4g4QEV-",
    "AQADMa0xG4g4QEV-",
]
bot = None  # 预留 bot 全局变量
user_client = None  # Telethon 用户客户端
redis_manager = None  # Redis client