import asyncio
from utils.product_utils import (
    check_file_record,
    # check_and_fix_sora_valid_state,
    # check_and_fix_sora_valid_state2,
)

async def sync():
    # 1. 同步 / 修复 file_record
    while True:
        summary = await check_file_record(limit=100)
        if summary.get("checked", 0) == 0:
            break

    # 2. 如需启用以下修复逻辑，取消注释即可
    #
    # while True:
    #     summary = await check_and_fix_sora_valid_state(limit=1000)
    #     if summary.get("checked", 0) == 0:
    #         break
    #
    # while True:
    #     summary = await check_and_fix_sora_valid_state2(limit=1000)
    #     if summary.get("checked", 0) == 0:
    #         break

async def main():
    # 初始化数据库连接
    # await asyncio.gather(
    #     MySQLPool.init_pool(),
    #     PGPool.init_pool(),
    # )

    # try:
    
    await sync()
    # finally:
    #     # 关闭数据库连接
    #     await PGPool.close()
    #     await MySQLPool.close()

if __name__ == "__main__":
    asyncio.run(main())
