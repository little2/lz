import os
from dotenv import load_dotenv
import json

load_dotenv(dotenv_path='.lz.env')
BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTGRES_DSN = os.getenv("POSTGRES_DSN")

BOT_MODE = os.getenv("BOT_MODE", "polling").lower()
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH")
WEBAPP_HOST = os.getenv("WEBAPP_HOST")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", 10000))

AES_KEY = os.getenv("AES_KEY", "")

ENVIRONMENT = os.getenv("ENVIRONMENT", "prd").lower()

# 管理员名单（逗号或分号分隔的一串 user_id）
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")

ADMIN_IDS: set[int] = set()
if ADMIN_IDS_RAW:
    for part in ADMIN_IDS_RAW.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            ADMIN_IDS.add(int(part))

RESULTS_PER_PAGE = 6
CACHE_TTL = 3000  # 緩存時間，單位秒

config = {}
# 嘗試載入 JSON 並合併參數
try:
    configuration_json = json.loads(os.getenv('CONFIGURATION', '') or '{}')
    if isinstance(configuration_json, dict):
        config.update(configuration_json)  # 將 JSON 鍵值對合併到 config 中
except Exception as e:
    print(f"⚠️ 無法解析 CONFIGURATION：{e}")

    configuration_json = json.loads(os.getenv('CONFIGURATION', '') or '{}')


API_ID          = int(config.get('api_id', os.getenv('API_ID', 0)))
API_HASH        = config.get('api_hash', os.getenv('API_HASH', ''))
SESSION_STRING  = config.get('session_string', os.getenv('SESSION_STRING', ''))
PHONE_NUMBER    = config.get('phone_number', os.getenv('PHONE_NUMBER', ''))
USER_SESSION    = str(API_ID) + 'session_name'  # 确保与上传的会话文件名匹配

MYSQL_HOST      = config.get('db_host', os.getenv('MYSQL_DB_HOST', 'localhost'))
MYSQL_USER      = config.get('db_user', os.getenv('MYSQL_DB_USER', ''))
MYSQL_PASSWORD  = config.get('db_password', os.getenv('MYSQL_DB_PASSWORD', ''))
MYSQL_DB        = config.get('db_name', os.getenv('MYSQL_DB_NAME', ''))
MYSQL_DB_PORT   = int(config.get('db_port', os.getenv('MYSQL_DB_PORT', 3306)))

VALKEY_URL      = config.get('valkey_url', os.getenv('VALKEY_URL', ''))

META_BOT        = config.get('meta_bot', os.getenv('META_BOT', ''))
UPLOADER_BOT_NAME = config.get('uploader_bot_name', os.getenv('UPLOADER_BOT_NAME', ''))
PUBLISH_BOT_NAME = config.get('publish_bot_name', os.getenv('PUBLISH_BOT_NAME', ''))
KEY_USER_ID     = int(config.get('key_user_id', os.getenv('KEY_USER_ID', 0)))

