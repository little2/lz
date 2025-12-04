import asyncio
import os
from datetime import datetime, timezone, timedelta, date
from io import BytesIO
from typing import Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.enums import ChatType
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    PhotoSize,
    BufferedInputFile,
    CopyTextButton,
    InputMediaPhoto
)





# ==========================
# 配置区（请改成你的实际值）
# ==========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_DSN = os.getenv(
    "DATABASE_DSN",
    "postgresql://user:password@localhost:5432/mydb"
)

# 收到媒体时要 copy 给谁（你的私聊 user_id）
OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "123456789"))

# 要贴公告 & 统计发言的「指定群组」
ANNOUNCE_CHAT_ID = int(os.getenv("ANNOUNCE_CHAT_ID", "-1001234567890"))

# 仓库存放缩略图的 chat（可以是你自己的私聊 / 一个专门的群）
STORAGE_CHAT_ID = int(os.getenv("STORAGE_CHAT_ID", "-1001234567890"))

# 你机器人的 username（不含 @）
BOT_USERNAME = os.getenv("BOT_USERNAME", "your_bot_username")


# ==========================
# 时区 & 日期工具
# ==========================
SINGAPORE_TZ = timezone(timedelta(hours=8))


def today_sgt() -> date:
    """以 UTC+8 作为 stat_date"""
    return datetime.now(SINGAPORE_TZ).date()


# ==========================
# PostgreSQL 封装
# ==========================
from lz_memory_cache import MemoryCache

class PGDB:
    pool: asyncpg.Pool | None = None
    _lock = asyncio.Lock()
    _cache_ready = False
    cache: MemoryCache | None = None  # 为了 type hint 更清楚

    @classmethod
    async def init_pool(cls, dsn: str):
        # 幂等：多处并发调用只建一次连接池
        if cls.pool is not None:
            if not cls._cache_ready:
                cls.cache = MemoryCache()
                cls._cache_ready = True
            return cls.pool

        async with cls._lock:
            if cls.pool is None:
                cls.pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
                
            if not cls._cache_ready:
                cls.cache = MemoryCache()
                cls._cache_ready = True
        return cls.pool




    @classmethod
    async def close_pool(cls):
        if cls.pool:
            await cls.pool.close()
            cls.pool = None

    # ---- 建表 ----

    @classmethod
    async def ensure_tables(cls):
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS file_stock (
                        id             BIGSERIAL PRIMARY KEY,
                        file_type      VARCHAR(30),
                        file_unique_id VARCHAR(100) NOT NULL UNIQUE,
                        file_id        VARCHAR(200) NOT NULL,
                        thumb_file_id  VARCHAR(200),
                        caption        TEXT,
                        bot            VARCHAR(50),
                        user_id        BIGINT,
                        create_time    TIMESTAMPTZ DEFAULT now()
                    );
                    """
                )

                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS talking_task (
                        talking_task_id  BIGSERIAL PRIMARY KEY,
                        user_id          BIGINT,
                        count            INTEGER NOT NULL DEFAULT 0,
                        stat_date        DATE NOT NULL,
                        update_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CONSTRAINT talking_task_user_date_uk UNIQUE (user_id, stat_date)
                    );
                    """
                )

    # ---- file_stock 操作 ----

    @classmethod
    async def get_file_stock_by_file_unique_id(cls, file_unique_id: str) -> Optional[dict] | None:
        cache_key = f"fuid:{file_unique_id}"
        cached = cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached


        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                '''
                SELECT *
                FROM file_stock
                WHERE file_unique_id = $1
                ''',
                file_unique_id,
            )


            if not row:
                return None

            data = dict(row)
            cls.cache.set(cache_key, data)
            cls.cache.set(f"id:{data['id']}", data)
            #（顺带也可设 id:{id}）
            return data

    @classmethod
    async def get_file_stock_by_id_offset(cls, id: int, offset: int) -> Optional[dict] | None:
        '''
        设计这个函式是为了配合翻页功能使用，尽可能减少数据库查询次数，优先从缓存中获取数据。
        若是往上页翻页(offset为负数)，则尝试从缓存中获取数据，若缓存中没有，且 id >=1 则查询数据库并将结果缓存起来。
        若是往下页翻页(offset为正数)，则直接查询数据库并将结果缓存起来。
        返回值为目标纪录的字典形式，若找不到则返回 None。
        '''
        # 防御：非法 id 直接返回 None
        if id is None or id < 1:
            return None        
        
        cache_key = f"id:{(id+offset)}"
        cached = cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached
        else:
            '''
            若是往上页翻页(offset为负数)，且 id >=1 则查询数据库并将结果缓存起来。
            '''
  
            async with cls.pool.acquire() as conn:
                if offset <0 and (id+offset)>=1:
                    rows = await conn.fetch(
                        '''
                        SELECT *
                        FROM file_stock
                        WHERE id <  $1 
                        ORDER BY id DESC
                        LIMIT 100
                        ''',
                        id,
                    )
                elif offset >0 and (id+offset)>=1:
                    rows = await conn.fetch(
                        '''
                        SELECT *
                        FROM file_stock
                        WHERE id > $1
                        ORDER BY id ASC
                        LIMIT 100
                        ''',
                        id,
                    )
                else:
                    return None
                    
                for r in rows:
                    cache_key1 = f"fuid:{r['file_unique_id']}"
                    cls.cache.set(cache_key1, dict(r))
                    cache_key2 = f"id:{r['id']}"
                    cls.cache.set(cache_key2, dict(r))

                if rows:
                    data = dict(rows[0])
                    return data
                else:
                    return None
                
                
                


    @classmethod
    async def get_file_stock_by_id(cls, id: int) -> Optional[dict] | None:
        cache_key = f"id:{id}"
        cached = cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached


        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(
                '''
                SELECT *
                FROM file_stock
                WHERE id >= ( $1 - 50 )
                ORDER BY id ASC
                LIMIT 100
                ''',
                id,
            )
            
            for r in rows:
                cache_key1 = f"fuid:{r['file_unique_id']}"
                cls.cache.set(cache_key1, dict(r))
                cache_key2 = f"id:{r['id']}"
                cls.cache.set(cache_key2, dict(r))

            return cls.cache.get(cache_key)
       

    @classmethod
    async def insert_file_stock_if_not_exists(
        cls,
        *,
        file_type: str,
        file_unique_id: str,
        file_id: str,
        thumb_file_id: Optional[str],
        caption: Optional[str],
        bot_username: str,
        user_id: Optional[int],
    ) -> Optional[int]:
        """
        返回该纪录的 id：
        - 新插入 → 回传新 id
        - 已存在 → 回传既有 id
        - 失败 → 回传 None
        """
        async with cls.pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO file_stock
                        (file_type, file_unique_id, file_id, thumb_file_id,
                         caption, bot, user_id, create_time)
                    VALUES
                        ($1, $2, $3, $4, $5, $6, $7, now())
                    ON CONFLICT (file_unique_id) DO NOTHING
                    """,
                    file_type,
                    file_unique_id,
                    file_id,
                    thumb_file_id,
                    caption,
                    bot_username,
                    user_id,
                )
            except Exception as e:
                print(f"[PG] insert_file_stock_if_not_exists error: {e}")
                return None

            row = await conn.fetchrow(
                "SELECT * FROM file_stock WHERE file_unique_id = $1",
                file_unique_id,
            )
            if not row:
                return None

            
            return row["id"]
    # ---- talking_task 操作 ----

    @classmethod
    async def get_talking_task(cls, user_id: int, stat_date: date) -> asyncpg.Record | None:
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM talking_task
                WHERE user_id = $1 AND stat_date = $2
                """,
                user_id,
                stat_date,
            )
        return row

    @classmethod
    async def upsert_talking_task_add_one(cls, user_id: int, stat_date: date):
        """
        收到“新视频且未重复”时调用：
        - 若不存在纪录 → 新增一笔 count = 1
        - 若存在 → count = count + 1
        """
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO talking_task (user_id, stat_date, count, update_timestamp)
                VALUES ($1, $2, 1, now())
                ON CONFLICT (user_id, stat_date)
                DO UPDATE
                    SET count = talking_task.count + 1,
                        update_timestamp = now()
                """,
                user_id,
                stat_date,
            )

    @classmethod
    async def increment_talking_task_if_exists(cls, user_id: int, stat_date: date, delta: int = 1) -> bool:
        """
        指定群发言时用：
        - 只在纪录存在时做 count + delta
        - 不存在则什么都不做
        返回 True 表示有更新，False 表示没有这条纪录
        """
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE talking_task
                SET count = talking_task.count + $3,
                    update_timestamp = now()
                WHERE user_id = $1 AND stat_date = $2
                RETURNING talking_task_id
                """,
                user_id,
                stat_date,
                delta,
            )
        return row is not None

    @classmethod
    async def consume_one_quota(cls, user_id: int, stat_date: date) -> int | None:
        """
        兑换视频时使用：
        - 若纪录存在且 count > 0 → count - 1，并返回新的 count
        - 若纪录存在但 count <= 0 → 返回 0
        - 若纪录不存在 → 返回 None
        """
        async with cls.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT talking_task_id, count
                    FROM talking_task
                    WHERE user_id = $1 AND stat_date = $2
                    FOR UPDATE
                    """,
                    user_id,
                    stat_date,
                )
                if row is None:
                    return None

                current_count = row["count"]
                if current_count <= 0:
                    await conn.execute(
                        """
                        UPDATE talking_task
                        SET update_timestamp = now()
                        WHERE talking_task_id = $1
                        """,
                        row["talking_task_id"],
                    )
                    return 0

                new_count = current_count - 1
                await conn.execute(
                    """
                    UPDATE talking_task
                    SET count = $2, update_timestamp = now()
                    WHERE talking_task_id = $1
                    """,
                    row["talking_task_id"],
                    new_count,
                )
        return new_count


# ==========================
# 缩略图稳定化工具
# ==========================
async def ensure_stable_thumb(
    message: Message,
    *,
    bot: Bot,
    storage_chat_id: int,
    prefer_cover: bool = True,
) -> Optional[Tuple[str, str]]:
    """
    下载原始 thumbnail / cover，
    重新上传到 storage_chat_id，返回 (thumb_file_id, thumb_file_unique_id)。

    若没有缩略图或失败 → 返回 None。
    """
    pic: Optional[PhotoSize] = None

    if getattr(message, "video", None):
        v = message.video
        # 预留 cover 支持（如果后续版本 / 其他客户端有 cover 字段）
        if prefer_cover and getattr(v, "cover", None):
            cover = v.cover
            if isinstance(cover, list) and cover:
                pic = cover[0]
            elif isinstance(cover, PhotoSize):
                pic = cover
        if not pic:
            pic = v.thumbnail
    elif getattr(message, "document", None):
        pic = message.document.thumbnail
    elif getattr(message, "animation", None):
        pic = message.animation.thumbnail

    if not pic:
        return None

    # 1) 下载到内存
    buf = BytesIO()
    await bot.download(pic, destination=buf)
    buf.seek(0)

    # 2) 重新上传为可复用的 photo
    try:
        filename = f"{pic.file_unique_id}.jpg"
        upload_msg = await bot.send_photo(
            chat_id=storage_chat_id,
            photo=BufferedInputFile(buf.read(), filename=filename),
        )

        largest = upload_msg.photo[-1]
        thumb_file_id = largest.file_id
        thumb_file_unique_id = largest.file_unique_id

        return thumb_file_id, thumb_file_unique_id
    except Exception as e:
        print(f"⚠️ ensure_stable_thumb 重新上传封面失败: {e}", flush=True)
        return None


# ==========================
# Aiogram 机器人逻辑
# ==========================
router = Router()


# ---- 1 & 2 收媒体 ----
@router.message(
    F.chat.type.in_({ChatType.PRIVATE, ChatType.GROUP, ChatType.SUPERGROUP})
    & (F.content_type.in_({"photo", "video", "document"}))
)
async def handle_media_message(message: Message, bot: Bot):
    """
    1. 收到任何媒体 → copy 给 OWNER_USER_ID。
    2. 若是 video，且 file_unique_id 未在 file_stock 才触发：
       - talking_task(user_id, today) +1
       - file_stock 写入（含稳定 thumb_file_id）
       - 在 ANNOUNCE_CHAT_ID 发公告 + deep-link 按钮
    """
    user = message.from_user
    if user is None or user.is_bot:
        return

    # 1) copy 给指定用户
    try:
        await bot.copy_message(
            chat_id=OWNER_USER_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
    except Exception as e:
        print(f"[Bot] copy_message error: {e}")

    # 2) 只有 video 才参与“兑换池”
    if not message.video:
        return

    video = message.video
    file_unique_id = video.file_unique_id
    file_id = video.file_id

    # 文件名 / caption 用来当展示文字
    file_name = video.file_name or ""
    caption = file_name or (message.caption or "")

    # 先检查是否已存在于 file_stock
    existed = await PGDB.get_file_stock_by_file_unique_id(file_unique_id)
    if existed:
        # 已经有这条视频了 → 不重复计数，也不公告
        return

    stat_date = today_sgt()

    # 不存在 → 先 talking_task +1
    await PGDB.upsert_talking_task_add_one(user_id=user.id, stat_date=stat_date)

    # 生成稳定缩略图（重新上传到仓库 chat）
    thumb_file_id: Optional[str] = None
    thumb_info = await ensure_stable_thumb(
        message,
        bot=bot,
        storage_chat_id=STORAGE_CHAT_ID,
        prefer_cover=True,
    )
    if thumb_info:
        thumb_file_id, _thumb_unique_id = thumb_info

    # 插入 file_stock
    new_id = await PGDB.insert_file_stock_if_not_exists(
        file_type="video",
        file_unique_id=file_unique_id,
        file_id=file_id,
        thumb_file_id=thumb_file_id,
        caption=caption,
        bot_username=BOT_USERNAME,
        user_id=user.id
    )
    if new_id is None:
        # 理论上不会发生，防御一下
        return

    # 在指定群组公告
    # title = file_name if file_name else "🍚新布施!"
    title = "🍚新布施!"
    deep_link = f"https://t.me/{BOT_USERNAME}?start={new_id}"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👀查看", url=deep_link)
            ]
        ]
    )

    try:
        await bot.send_message(
            chat_id=ANNOUNCE_CHAT_ID,
            text=title,
            reply_markup=kb,
        )
    except Exception as e:
        print(f"[Bot] send announce error: {e}")


# ---- 3 群组发言计数 ----
@router.message(
    F.chat.id == ANNOUNCE_CHAT_ID,
    F.content_type == "text",
    F.from_user.is_bot == False,
)
async def handle_group_text_message(message: Message):
    """
    3. 用户在指定群组发言：
       若 talking_task(stat_date,user_id) 存在 → count+1
       若不存在 → 不新增、不更新
    """
    user = message.from_user
    if user is None:
        return

    stat_date = today_sgt()
    await PGDB.increment_talking_task_if_exists(user.id, stat_date, delta=1)


# ---- 4 /start file_unique_id ----
@router.message(CommandStart(deep_link=True))
async def handle_start_with_param(message: Message, command: CommandStart):
    """
    4. /start [id]
       - 查 file_stock
       - 若存在 → 发缩略图 + 「兑换」按钮
    """
    user = message.from_user
    if user is None:
        return

    arg = command.args
    if not arg:
        await message.answer("🙏欢迎使用布施机器僧～")
        return
    try:
        id = int(arg)
    except ValueError:
        await message.answer("参数格式不正确，这个布施编号看起来怪怪的。")
        return
    
    stock_row = await PGDB.get_file_stock_by_id(id)
    if stock_row is None:
        await message.answer("🙏这个布施已经不存在或尚未入功德箱。")
        return

    tpl_data = tpl(stock_row)

    thumb_file_id = tpl_data["thumb_file_id"]
    caption = tpl_data["caption"]
    kb = tpl_data["kb"]
            

    # 有稳定缩略图 → 用 photo，当作预览图
    if thumb_file_id:
        try:
            await message.answer_photo(
                photo=thumb_file_id,
                caption=caption,
                reply_markup=kb,
            )
            return
        except Exception as e:
            print(f"[Bot] send preview photo error: {e}")

    # fallback：没有缩略图 / 发送失败 → 用文字
    await message.answer(
        text=caption,
        reply_markup=kb,
    )

def tpl(stock_row):
    thumb_file_id = stock_row["thumb_file_id"] 
    caption = stock_row["caption"] or "🍚"
    id = stock_row["id"]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️",callback_data=f"item:{id}:-1"),  
                InlineKeyboardButton(text="🤲 化缘",callback_data=f"redeem:{id}"),
                InlineKeyboardButton(text="▶️",callback_data=f"item:{id}:1")                  
            ],
            [                
                InlineKeyboardButton(text="📿 随喜转发", copy_text=CopyTextButton(text=f"https://t.me/{BOT_USERNAME}?start={id}"))
            ],
            [                
                InlineKeyboardButton(text="🙏 布施岛", url=f"https://t.me/{BOT_USERNAME}")
            ]
        ]
    )
    return {"thumb_file_id":thumb_file_id,"caption":caption,"kb":kb}


# ---- 5 callback：兑换视频 ----
@router.callback_query(F.data.startswith("redeem:"))
async def handle_redeem_callback(callback: CallbackQuery, bot: Bot):
    """
    5. 兑换逻辑：
       - 检查今天 talking_task(user_id, stat_date) 的 count
       - count > 0 → sendVideo，count-1
       - count <=0 → 提示「群里发言或发不重覆的视频资源可以兑换视频」
       - 没有纪录 → 提示「你今天需要上传一个视频才能开始兑换」
    """
    if not callback.from_user or callback.from_user.is_bot:
        await callback.answer()
        return

    user_id = callback.from_user.id
    stat_date = today_sgt()

    _, id_str = callback.data.split(":", 1)

    try:
        id = int(id_str)
    except ValueError:
        await callback.answer("参数错误，请稍后再试。", show_alert=True)
        return

    new_count = await PGDB.consume_one_quota(user_id, stat_date)

    if new_count is None:
        await callback.answer("🙏你今天需要布施一个视频才能开始化缘。", show_alert=True)
        return

    if new_count == 0:
        # consume_one_quota 里：count<=0 的情况不会扣，只更新时间 → 返回 0
        await callback.answer("🙏你的功德不足，需要在群里发言、布施不重覆的视频资源或是分享布施连结给新人就能获得功德。", show_alert=True)
        return
    
  

    # 有额度且成功扣 1
    row = await PGDB.get_file_stock_by_id(id)
    if row is None:
        await callback.answer("🙏找不到这个布施，可能已经被移除。", show_alert=True)
        return

    file_id = row["file_id"]
    try:
        await bot.send_video(
            chat_id=user_id,
            video=file_id,
        )
        await callback.answer("🙏化缘成功，已发送视频给你。", show_alert=False)
    except Exception as e:
        print(f"[Bot] send_video error: {e}")
        await callback.answer("🙏发送视频失败，请稍后再试。", show_alert=True)


# ---- 5 callback：翻页 ----
@router.callback_query(F.data.startswith("item:"))
async def handle_item_callback(callback: CallbackQuery, bot: Bot):
    if not callback.from_user or callback.from_user.is_bot:
        await callback.answer()
        return

    try:
        _, id_str, offset_str = callback.data.split(":", 2)
        current_id = int(id_str)
        offset = int(offset_str)
    except ValueError:
        await callback.answer("参数错误，请稍后再试。", show_alert=True)
        return


    stock_row = await PGDB.get_file_stock_by_id_offset(current_id, offset)
    if not stock_row:
        await callback.answer("🙏已经没有更多布施了。", show_alert=True)
        return

    tpl_data = tpl(stock_row)
    thumb_file_id = tpl_data["thumb_file_id"]
    caption = tpl_data["caption"]
    kb = tpl_data["kb"]

    try:
        if thumb_file_id:
            await callback.message.edit_media(
                media=InputMediaPhoto(
                    media=thumb_file_id,
                    caption=caption,
                ),
                reply_markup=kb,
            )
        else:
            await callback.message.edit_text(
                text=caption,
                reply_markup=kb,
            )
        await callback.answer()
    except Exception as e:
        print(f"[Bot] pagination edit error: {e}")
        await callback.answer("更新失败，请稍后再试。", show_alert=True)

    

# ==========================
# main
# ==========================
async def main():
    bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
    dp = Dispatcher()

    await PGDB.init_pool(DATABASE_DSN)
    await PGDB.ensure_tables()

    dp.include_router(router)

    print("Bot started...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
