import asyncio
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta, date
from io import BytesIO
from typing import Optional, Tuple
import base64
import asyncpg
from typing import Any, Callable, Awaitable, Any
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart,Command
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


load_dotenv(dotenv_path='.bs.env')

_background_tasks: dict[str, asyncio.Task] = {}

def spawn_once(key: str, coro_factory: Callable[[], Awaitable[Any]]):
    """相同 key 的后台任务只跑一个；结束后自动清理。"""
    task = _background_tasks.get(key)
    if task and not task.done():
        return

    async def _runner():
        try:
            coro = coro_factory()
            await asyncio.wait_for(coro, timeout=15)
        except Exception as e:
            print(f"🔥 background task failed for key={key}: {e}", flush=True)

    t = asyncio.create_task(_runner(), name=f"bg:{key}")
    _background_tasks[key] = t
    t.add_done_callback(lambda _: _background_tasks.pop(key, None))


# ==========================
# 配置区（请改成你的实际值）
# ==========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
# 你机器人的 username（不含 @）

PG_DSN = os.getenv("PG_DSN")

# 收到媒体时要 copy 给谁（你的私聊 user_id）
X_USER_ID = int(os.getenv("X_USER_ID"))

# 要贴公告 & 统计发言的「指定群组」
ANNOUNCE_CHAT_ID = int(os.getenv("ANNOUNCE_CHAT_ID"))


BOT_USERNAME = None # 会在 main() 里初始化



# ==========================
# 时区 & 日期工具
# ==========================
SINGAPORE_TZ = timezone(timedelta(hours=8))


WEBHOOK_HOST= os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH= os.getenv("WEBHOOK_PATH","/")
WEBAPP_HOST= os.getenv("WEBAPP_HOST","0.0.0.0")
BOT_MODE= os.getenv("BOT_MODE","polling")  # polling / webhook

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
    def ensure_cache(cls):
        if cls.cache is None:
            cls.cache = MemoryCache()
            cls._cache_ready = True



    @classmethod
    async def close_pool(cls):
        if cls.pool:
            await cls.pool.close()
            cls.pool = None
        cls.cache = None
        cls._cache_ready = False

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
                        file_size      BIGINT,        -- ✅ 新增
                        duration       INTEGER,       -- ✅ 新增
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
    async def get_file_stock_by_file_unique_id(cls, file_unique_id: str) -> Optional[dict] :
        cls.ensure_cache()
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
        cls.ensure_cache()
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
        cls.ensure_cache()
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
        file_size: Optional[int],
        duration: Optional[int],
    ) -> Optional[int]:

        async with cls.pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO file_stock
                        (file_type, file_unique_id, file_id, thumb_file_id,
                        caption, bot, user_id, file_size, duration, create_time)
                    VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
                    ON CONFLICT (file_unique_id) DO NOTHING
                    """,
                    file_type,
                    file_unique_id,
                    file_id,
                    thumb_file_id,
                    caption,
                    bot_username,
                    user_id,
                    file_size,
                    duration
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

    @classmethod
    async def update_file_stock_thumb(cls, file_unique_id: str, thumb_file_id: str):
        """
        后台生成缩略图后，更新 file_stock.thumb_file_id，
        并同步更新 MemoryCache 中的缓存。
        """
        cls.ensure_cache()

        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE file_stock
                SET thumb_file_id = $2
                WHERE file_unique_id = $1
                """,
                file_unique_id,
                thumb_file_id,
            )

        # 同步更新缓存（如果已有）
        if cls.cache:
            cache_key = f"fuid:{file_unique_id}"
            data = cls.cache.get(cache_key)
            if data:
                data["thumb_file_id"] = thumb_file_id
                cls.cache.set(cache_key, data)
                # 顺带也更新 id:{id}
                if "id" in data:
                    cls.cache.set(f"id:{data['id']}", data)



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
        - 若纪录存在且距离上次 update_timestamp ≥ 60 秒 → count + delta
        - 否则不更新
        返回 True 表示有更新，False 表示未更新
        """
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE talking_task
                SET count = talking_task.count + $3,
                    update_timestamp = now()
                WHERE user_id = $1 
                AND stat_date = $2
                AND now() - update_timestamp >= interval '60 seconds'
                RETURNING talking_task_id;
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


    @classmethod
    async def has_any_talking_task(cls, user_id: int) -> bool:
        """
        判断用户是否“老用户”：
        只要 talking_task 中存在任一笔纪录（不限定 stat_date）即可视为老用户。
        """
        cls.ensure_cache()
        cache_key = f"user_exists:{user_id}"

        # ---- MemoryCache 命中 ----
        cached = cls.cache.get(cache_key)
        if cached is not None:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached   # True 或 False

        # ---- 查询 PG ----
        async with cls.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 1
                FROM talking_task
                WHERE user_id = $1
                LIMIT 1
                """,
                user_id,
            )

        is_old = row is not None

        # ---- 存入内存 cache，用 True/False，而不是 row dict ----
        cls.cache.set(cache_key, is_old)

        return is_old


    @classmethod
    async def add_talking_task_score(cls, user_id: int, stat_date: date, delta: int):
        """
        通用加功德接口：
        - 若不存在纪录 → 新增一笔 count = delta
        - 若存在纪录 → count = count + delta（不受 60 秒限制）
        """
        async with cls.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO talking_task (user_id, stat_date, count, update_timestamp)
                VALUES ($1, $2, $3, now())
                ON CONFLICT (user_id, stat_date)
                DO UPDATE
                    SET count = talking_task.count + EXCLUDED.count,
                        update_timestamp = now()
                """,
                user_id,
                stat_date,
                delta,
            )


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

    WHITE_JPEG_BASE64 = (
        "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////"
        "//////////////////////////////////////////////////////////////////////////////////////////////"
        "//////////////2wBDAf//////////////////////////////////////////////////////////////////////////////////////"
        "//////////////////////////////////////////////////////////////////////////////////////////////"
        "//////////////wAARCAAQABADASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAf/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFAEBAAAAAAAAA"
        "AAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/AMf/AP/Z"
    )
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

    if pic:
        # 1) 下载到内存
        buf = BytesIO()
        await bot.download(pic, destination=buf)
        buf.seek(0)
        filename = f"{pic.file_unique_id}.jpg"
    else:
        print(f"[ensure_stable_thumb] no thumb available → using fallback white image")

        white_jpeg_bytes = base64.b64decode(WHITE_JPEG_BASE64)
        buf = BytesIO(white_jpeg_bytes)      
        filename = "white.jpg"

    # 2) 重新上传为可复用的 photo
    try:
        
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
    1. 收到任何媒体 → copy 给 X_USER_ID。
    2. 若是 video，且 file_unique_id 未在 file_stock 才触发：
       - talking_task(user_id, today) +1
       - file_stock 写入（thumb 先留空）
       - 在 ANNOUNCE_CHAT_ID 发公告 + deep-link 按钮
       - 缩略图由后台任务生成并回写
    """
    user = message.from_user
    if user is None or user.is_bot:
        return

    # 1) copy 给指定用户
    spawn_once(
        f"copy_message:{message.message_id}",
        lambda: bot.copy_message(
            chat_id=X_USER_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
    )


    # try:
    #     await bot.copy_message(
    #         chat_id=X_USER_ID,
    #         from_chat_id=message.chat.id,
    #         message_id=message.message_id,
    #     )
    # except Exception as e:
    #     print(f"[Bot] copy_message error: {e}")

    # 2) 只有 video 才参与“兑换池”
    if not message.video:
        await bot.send_message(
            chat_id=message.chat.id,
            text="🙏 贫僧只收视频",
            reply_to_message_id=message.message_id,
        )
        return

    video = message.video
    file_unique_id = video.file_unique_id
    file_id = video.file_id
    file_size = video.file_size           # ✅ 新增
    duration = video.duration

    # 文件名 / caption 用来当展示文字
    file_name = video.file_name or ""
    caption = file_name or (message.caption or "")

    print(f"[Bot] Received video from user_id={user.id}, file_unique_id={file_unique_id}")

    # 先检查是否已存在于 file_stock
    existed = await PGDB.get_file_stock_by_file_unique_id(file_unique_id)
    if existed:
        await bot.send_message(
            chat_id=message.chat.id,
            text="🙏 这份已有其他施主布施了",
            reply_to_message_id=message.message_id,
        )
        return

    print(f"[Bot] New video, processing for user_id={user.id}, file_unique_id={file_unique_id}")

    stat_date = today_sgt()

    # 不存在 → 先 talking_task +1
    await PGDB.upsert_talking_task_add_one(user_id=user.id, stat_date=stat_date)

    # 先插入一条记录，thumb_file_id 暂时为 None（后台再补）
    new_id = await PGDB.insert_file_stock_if_not_exists(
        file_type="video",
        file_unique_id=file_unique_id,
        file_id=file_id,
        thumb_file_id=None,
        caption=caption,
        bot_username=BOT_USERNAME,
        user_id=user.id,
        file_size=file_size,
        duration=duration,
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

        await bot.send_message(
            chat_id=message.chat.id,
            text="🙏 阿弥陀佛，施主功德 +1",
            reply_to_message_id=message.message_id,
        )
    except Exception as e:
        print(f"[Bot] send announce error: {e}")

    # 3) 丢到后台去做缩略图 + 更新 DB
    #    使用 spawn_once，确保同一个 file_unique_id 只会跑一个后台任务
    def _coro_factory():
        async def _job():
            await asyncio.sleep(0.7)  # 避免 Telegram 限流
            thumb_info = await ensure_stable_thumb(
                message,
                bot=bot,
                storage_chat_id=X_USER_ID,
                prefer_cover=True,
            )
            if not thumb_info:
                return

            thumb_file_id, _thumb_unique_id = thumb_info

            await PGDB.update_file_stock_thumb(
                file_unique_id=file_unique_id,
                thumb_file_id=thumb_file_id,
            )

        return _job()

    spawn_once(
        key=f"thumb:{file_unique_id}",
        coro_factory=_coro_factory,
    )


# ---- 3 群组发言计数 ----
@router.message(
    F.content_type == "text",
    F.chat.id == ANNOUNCE_CHAT_ID,
    F.from_user.is_bot == False,
)
async def handle_group_text_message(message: Message):
    """
    3. 用户在指定群组发言：
       若 talking_task(stat_date,user_id) 存在 → count+1
       若不存在 → 不新增、不更新
    """
    print(f"[Bot] {message.chat.id} Group message from user_id={message.from_user.id} in ANNOUNCE_CHAT_ID")
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

    print(f"[Bot] /start with arg from user_id={user.id}: {command.args}")
    arg = command.args
    if not arg:
        await message.answer("🙏欢迎使用布施机器僧～")
        return
    try:
        argrow = arg.split("_")  # 支持 deep-link 带 user_id 的格式
        id = int(argrow[0])
        from_user_id = int(argrow[1]) if len(argrow) >1 else None
    except ValueError:
        await message.answer("参数格式不正确，这个布施编号看起来怪怪的。")
        return
    
    stock_row = await PGDB.get_file_stock_by_id(id)
    if stock_row is None:
        await message.answer("🙏这个布施已经不存在或尚未入功德箱。")
        return

    tpl_data = tpl(stock_row, user.id)

    thumb_file_id = tpl_data["thumb_file_id"]
    caption = tpl_data["caption"]
    kb = tpl_data["kb"]
            

    
    if from_user_id and from_user_id != user.id:
        """
        推荐奖励逻辑：
        - 直接使用 PGDB.has_any_talking_task(user.id)
            若 False → 代表从未出现过 → 真·新用户
            若 True  → 已是老用户 → 不给奖励
        - 新用户 → 双方 +3 功德
        """

        # 判断是否老用户（含 MemoryCache，不需额外 cache）
        is_old = await PGDB.has_any_talking_task(user.id)

        if not is_old:
            # 真·新用户 → 发奖励
            stat_date = today_sgt()

            # 邀请人 +3
            await PGDB.add_talking_task_score(
                user_id=from_user_id,
                stat_date=stat_date,
                delta=3,
            )

            # 新用户 +3（此处会写入 talking_task，使其变成老用户）
            await PGDB.add_talking_task_score(
                user_id=user.id,
                stat_date=stat_date,
                delta=3,
            )

            # 通知邀请人
            try:
                await message.bot.send_message(
                    chat_id=from_user_id,
                    text=(
                        "🙏 感谢您引荐新朋友，共沾法喜！功德 +3"
                    ),
                )
            except Exception as e:
                print(f"[Referral] fail notify inviter {from_user_id}: {e}", flush=True)

        else:
            # 老用户 → 不发奖励
            print(f"[Referral] user {user.id} is old user → skip reward")



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


    
@router.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [                
                InlineKeyboardButton(text="🙏 布施岛", url=f"https://t.me/+oRTYsn1BKC5mZTA8")
            ]
        ]
    )
    
    await message.answer(
        text="🙏 贫僧已入定～\n送来视频，即是布施，即得功德。",
        reply_markup=kb,
    )

    


def format_file_size(size_in_bytes: int) -> str:
    """格式化文件大小为易读字符串"""
    if size_in_bytes < 1024:
        return f"{size_in_bytes} B"
    elif size_in_bytes < 1024 ** 2:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3:
        return f"{size_in_bytes / 1024 ** 2:.2f} MB"
    else:
        return f"{size_in_bytes / 1024 ** 3:.2f} GB"

def format_duration(duration_in_seconds: int) -> str:
    """格式化持续时间为易读字符串"""
    minutes, seconds = divmod(duration_in_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}:{minutes}:{seconds}"
    elif minutes > 0:
        return f"{minutes}:{seconds}"
    else:
        return f"{seconds}"

def tpl(stock_row,user_id):
    thumb_file_id = stock_row["thumb_file_id"] 
    caption = stock_row["caption"] or "🍚"
    if stock_row['file_size']:
        caption += f"\n\n💾{format_file_size(stock_row['file_size'])} 🕐{format_duration(stock_row['duration'])} "



    id = stock_row["id"]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️",callback_data=f"item:{id}:-1"),  
                InlineKeyboardButton(text="🤲 化缘",callback_data=f"redeem:{id}"),
                InlineKeyboardButton(text="▶️",callback_data=f"item:{id}:1")                  
            ],
            [                
                InlineKeyboardButton(text="📿 随喜转发", copy_text=CopyTextButton(text=f"https://t.me/{BOT_USERNAME}?start={id}_{user_id}"))
            ],
            [                
                InlineKeyboardButton(text="🙏 布施岛", url=f"https://t.me/+oRTYsn1BKC5mZTA8")
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
        await callback.answer("🙏你的功德不足，可在岛里发言、布施视频或是分享连结给新人就能获得功德。", show_alert=True)
        return
    
  

    # 有额度且成功扣 1
    row = await PGDB.get_file_stock_by_id(id)
    if row is None:
        await callback.answer("🙏找不到这个布施，可能已经被移除。", show_alert=True)
        return

    file_id = row["file_id"]
    try:
        caption = f"你今天的功德值为 {new_count}，还可以继续化缘。\n\nhttps://t.me/{BOT_USERNAME}?start={id}"
        await bot.send_video(
            chat_id=user_id,
            video=file_id,
            parse_mode=ParseMode.HTML,
            caption=caption,
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

    tpl_data = tpl(stock_row,callback.from_user.id)
    thumb_file_id = tpl_data["thumb_file_id"]
    caption = tpl_data["caption"]
    kb = tpl_data["kb"]

    try:
        if thumb_file_id:
            await callback.message.edit_media(
                media=InputMediaPhoto(
                    media=thumb_file_id,
                    caption=f"🍚{caption}",
                ),
                reply_markup=kb,
            )
        else:
            await callback.message.edit_text(
                text=f"🍚{caption}",
                reply_markup=kb,
            )
        await callback.answer()
        await asyncio.sleep(0.7)  # 避免 Telegram 限流
    except Exception as e:
        print(f"[Bot] pagination edit error: {e}")
        await callback.answer("更新失败，请稍后再试。", show_alert=True)

    
@router.message(Command("me"))
async def cmd_hello(message: Message):
    row = await PGDB.get_talking_task(user_id=message.from_user.id, stat_date=today_sgt())
    print(f"[Bot] /me for user_id={message.from_user.id}, row={row}")
    count = row.get("count", 0) if row else 0
    text = f"""
👤 阁下的基本信息:

🆔 用户ID: {message.from_user.id}
💎 今日功德: {count}
"""
    await message.answer(text=text)

# ==========================

from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats

@router.message(Command("setcommand"))
async def handle_set_comment_command(message: Message, bot: Bot):

    await bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
    await bot.delete_my_commands(scope=BotCommandScopeAllPrivateChats())
    await bot.delete_my_commands(scope=BotCommandScopeDefault())

    await bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="🏮 入寺主页"),
            BotCommand(command="me", description="🧘 查询我的功德")
        ],
        scope=BotCommandScopeAllPrivateChats()
    )
    print("✅ 已设置命令列表", flush=True)


async def on_startup(bot: Bot):
    webhook_url = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
    print(f"🔗 設定 Telegram webhook 為：{webhook_url}")
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(webhook_url)

# main
# ==========================
async def main():
    global BOT_USERNAME   # ← 必须加这一行
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
   
    me = await bot.get_me()
    bot.username = me.username
    BOT_USERNAME = me.username
    

    dp = Dispatcher()

    await PGDB.init_pool(PG_DSN)
    await PGDB.ensure_tables()

    dp.include_router(router)


   


    if BOT_MODE == "webhook":
        dp.startup.register(on_startup)
        print("🚀 啟動 Webhook 模式")

        app = web.Application()

        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)

        port = int(os.environ.get("PORT", 8080))
        await web._run_app(app, host="0.0.0.0", port=port)
        
    else:
        print("🚀 啟動 Polling 模式")
        
        
       
        await dp.start_polling(bot, polling_timeout=10.0)



if __name__ == "__main__":
    asyncio.run(main())
