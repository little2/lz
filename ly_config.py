import os
from dotenv import load_dotenv
import json

load_dotenv(dotenv_path='.ly.env')

BOT_MODE = os.getenv("BOT_MODE", "webhook").lower()
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH")
WEBAPP_HOST = os.getenv("WEBAPP_HOST")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", 10000))

AES_KEY = os.getenv("AES_KEY", "")

ENVIRONMENT = os.getenv("ENVIRONMENT", "prd").lower()

RESULTS_PER_PAGE = 6

config = {}
# 嘗試載入 JSON 並合併參數
try:
    configuration_json = json.loads(os.getenv('CONFIGURATION', '') or '{}')
    if isinstance(configuration_json, dict):
        config.update(configuration_json)  # 將 JSON 鍵值對合併到 config 中
except Exception as e:
    print(f"⚠️ 無法解析 CONFIGURATION：{e}")

API_ID          = int(config.get('api_id', os.getenv('API_ID', 0)))
API_HASH        = config.get('api_hash', os.getenv('API_HASH', ''))
SESSION_STRING  = os.getenv("USER_SESSION_STRING")


PHONE_NUMBER    = config.get('phone_number', os.getenv('PHONE_NUMBER', ''))


MYSQL_HOST      = config.get('db_host', os.getenv('MYSQL_DB_HOST', 'localhost'))
MYSQL_USER      = config.get('db_user', os.getenv('MYSQL_DB_USER', ''))
MYSQL_PASSWORD  = config.get('db_password', os.getenv('MYSQL_DB_PASSWORD', ''))
MYSQL_DB        = config.get('db_name', os.getenv('MYSQL_DB_NAME', ''))
MYSQL_DB_PORT   = int(config.get('db_port', os.getenv('MYSQL_DB_PORT', 3306)))

META_BOT       = config.get('meta_bot', os.getenv('META_BOT', ''))


'''
ALLOWED_GROUP_IDS = {
    -1001234567890,   # 示例：学院群
    -1005566778899,   # 示例：工作群
    -1009988776655,   # 示例：测试群
}
'''

# 读取 JSON 字串
raw = os.getenv("COMMAND_RECEIVERS", "{}")
raw2 = os.getenv("ALLOWED_GROUP_IDS", "{-1001234567890, -1005566778899, -1009988776655}")
# 尝试解析为 dict



try:
    COMMAND_RECEIVERS = json.loads(raw)
    ALLOWED_GROUP_IDS = json.loads(raw2)
except json.JSONDecodeError:
    print("[ly_config] ❌ COMMAND_RECEIVERS JSON 格式错误，使用空 dict")
    COMMAND_RECEIVERS = {}
    ALLOWED_GROUP_IDS = {}



# 从 dict 中取得所有允许的 user_id（去重）
ALLOWED_PRIVATE_IDS = set(COMMAND_RECEIVERS.values())



PG_DSN = os.getenv("PG_DSN", "postgresql://user:password@127.0.0.1:5432/telebot")
STAT_FLUSH_INTERVAL = 5          # 每 5 秒刷一次库
STAT_FLUSH_BATCH_SIZE = 500      # 缓冲累计到 500 条键值就强制刷库

PG_MIN_SIZE = 1 
PG_MAX_SIZE = 5 

KEY_USER_ID = os.getenv("KEY_USER_ID")  
raw = os.getenv("BOT_INIT", "")
BOT_INIT = [x.strip() for x in raw.split(",") if x.strip()]





