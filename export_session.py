from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import os
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path='.lzbot.env')  # 自动从当前目录的 .env 文件读取

config = {}
# 嘗試載入 JSON 並合併參數
try:
    configuration_json = json.loads(os.getenv('CONFIGURATION', '') or '{}')
    if isinstance(configuration_json, dict):
        config.update(configuration_json)  # 將 JSON 鍵值對合併到 config 中
except Exception as e:
    print(f"⚠️ 無法解析 CONFIGURATION：{e}")



# 你的 API_ID 与 API_HASH
API_ID          = int(config.get('api_id', os.getenv('API_ID', 0)))
API_HASH        = config.get('api_hash', os.getenv('API_HASH', ''))
PHONE_NUMBER    = config.get('phone_number', os.getenv('PHONE_NUMBER', ''))

# 原本 .session 的文件名，例如 "123456session_name.session"
OLD_SESSION_FILE = str(API_ID) + "session_name"

with TelegramClient(OLD_SESSION_FILE, API_ID, API_HASH) as client:
    string = StringSession.save(client.session)
    print("\n✅ 以下是你的 StringSession（可写入 .env）\n")
    print("USER_SESSION_STRING=" + string)