


import asyncio
import os
import time
import aiogram
import json


from lz_config import BOT_TOKEN, BOT_MODE, WEBHOOK_PATH, WEBHOOK_HOST,AES_KEY,SESSION_STRING,USER_SESSION, API_ID, API_HASH, PHONE_NUMBER
from lz_db import db
from lz_mysql import MySQLPool

from handlers import lz_media_parser
from handlers import lz_menu

import lz_var
import re

from utils.product_utils import sync_sora, sync_album_items, check_and_fix_sora_valid_state,check_and_fix_sora_valid_state2,check_file_record

from lz_redis import RedisManager
lz_var.redis_manager = RedisManager()
#


lz_var.start_time = time.time()
lz_var.cold_start_flag = True







async def sync():
    while True:
        summary = await check_file_record(limit=100)
        if summary["checked"] == 0:
            break


    while False:
        summary = await check_and_fix_sora_valid_state(limit=1000)
        if summary["checked"] == 0:
            break


    while False:
        summary = await check_and_fix_sora_valid_state2(limit=1000)
        if summary["checked"] == 0:
            break

async def main():
   


    await sync()

    # 理论上 Aiogram 轮询不会退出，若退出则让 Telethon 同样停止
    # task_telethon.cancel()

if __name__ == "__main__":
    asyncio.run(main())


