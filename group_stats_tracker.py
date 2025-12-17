# group_stats_tracker.py
import re
import asyncio
from collections import defaultdict
from datetime import timedelta

from telethon import events
from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl

from pg_stats_db import PGStatsDB


class GroupStatsTracker:
    """
    群组发言统计功能（纯 classmethod 版本）
    统计维度：
      日期(UTC+8) + 用户 + 群 + thread + 类型 + from_bot + hour(UTC+8)
    """

    client = None
    flush_interval = 5
    flush_batch_size = 500

    _buffer = defaultdict(int)
    _lock = asyncio.Lock()
    _flusher_task = None
    _offline_replay_task = None   # ⬅ 新增：离线交易回放的后台任务

    _url_regex = re.compile(r"https?://\S+", re.IGNORECASE)

    # bot 缓存： { user_id: True/False }
    _bot_cache = {}

    _raw_buffer = []
    _raw_max_len = 5000  # 单条截断，避免极端长文本

    # ------------------------------
    # 初始化 / 绑定 Telethon
    # ------------------------------
    @classmethod
    def configure(cls, client, flush_interval=5, flush_batch_size=500):
        cls.client = client
        cls.flush_interval = flush_interval
        cls.flush_batch_size = flush_batch_size

        @client.on(events.NewMessage)
        async def _handler(event):
            await cls.on_new_message(event)

    # @classmethod
    # async def start_background_tasks(cls):
    #     if cls._flusher_task:
    #         return
    #     cls._flusher_task = asyncio.create_task(cls._periodic_flusher())


    @classmethod
    async def start_background_tasks(cls, offline_replay_coro=None, offline_interval: int = 60):
        """
        启动后台任务：
        - _periodic_flusher：每 flush_interval 秒写一次统计
        - _periodic_offline_replay：每 offline_interval 秒跑一次离线回放（如果有传）
        """
        if not cls._flusher_task:
            cls._flusher_task = asyncio.create_task(cls._periodic_flusher())

        # 如果有传 replay 协程，就再开一个后台任务
        if offline_replay_coro and not cls._offline_replay_task:
            cls._offline_replay_task = asyncio.create_task(
                cls._periodic_offline_replay(offline_replay_coro, offline_interval)
            )


    # @classmethod
    # async def stop_background_tasks(cls):
    #     await cls.flush()
    #     if cls._flusher_task:
    #         cls._flusher_task.cancel()
    #         cls._flusher_task = None

    @classmethod
    async def stop_background_tasks(cls):
        await cls.flush()
        if cls._flusher_task:
            cls._flusher_task.cancel()
            cls._flusher_task = None

        if cls._offline_replay_task:
            cls._offline_replay_task.cancel()
            cls._offline_replay_task = None




    # ------------------------------
    # 核心消息处理
    # ------------------------------
    @classmethod
    async def on_new_message(cls, event):
        msg = event.message
        if not msg:
            return

        if not (event.is_group or event.is_channel):
            return

        user_id = msg.sender_id
        if not user_id:
            return

        chat_id = event.chat_id
        thread_id = cls.get_thread_id(msg)
        msg_type = cls.classify_message(msg)

        # ================================
        # 转为 UTC+8（北京时间/新加坡时间）
        # ================================
        msg_time_local = msg.date + timedelta(hours=8)
        hour = msg_time_local.hour
        stat_date = msg_time_local.date()

        # ================================
        # bot 缓存逻辑
        # ================================
        if user_id in cls._bot_cache:
            from_bot = cls._bot_cache[user_id]
        else:
            # 不在缓存时，才 get_sender
            try:
                sender = await event.get_sender()
                from_bot = bool(getattr(sender, "bot", False))
            except Exception:
                from_bot = False

            # 写入缓存
            cls._bot_cache[user_id] = from_bot

        # 统计维度 key
        key = (
            stat_date,
            int(user_id),
            int(chat_id),
            int(thread_id),
            msg_type,
            from_bot,
            hour
        )

        async with cls._lock:
            cls._buffer[key] += 1
            need_flush = len(cls._buffer) >= cls.flush_batch_size


        text = (msg.message or "").strip()
        is_cmd = text.startswith("/")  # 过滤指令
        if (msg_type == "text") and (not from_bot) and (not is_cmd) and len(text) >= 3:
            if len(text) > cls._raw_max_len:
                text = text[:cls._raw_max_len]

            raw_row = {
                "chat_id": int(chat_id),
                "message_id": int(msg.id),
                "thread_id": int(thread_id),
                "user_id": int(user_id),
                "msg_time_utc": msg.date,          # UTC
                "stat_date": stat_date,        # UTC+8 date
                "hour": int(hour),             # UTC+8 hour
                "text": text,
                "from_bot": bool(from_bot),
            }

            async with cls._lock:
                cls._raw_buffer.append(raw_row)


        if need_flush:
            await cls.flush()

    # ------------------------------
    # Flush 写入 PG
    # ------------------------------
    @classmethod
    async def flush(cls):
        async with cls._lock:
            if not cls._buffer and not cls._raw_buffer:
                return
            items = list(cls._buffer.items())
            cls._buffer.clear()

            raw_rows = list(cls._raw_buffer)
            cls._raw_buffer.clear()

        if items:
            await PGStatsDB.upsert_daily_counts(items)

        if raw_rows:
            await PGStatsDB.upsert_raw_messages(raw_rows)


    @classmethod
    async def _periodic_flusher(cls):
        while True:
            await asyncio.sleep(cls.flush_interval)
            try:
                await cls.flush()
            except Exception as e:
                print(f"[stats flush error] {e}", flush=True)


    @classmethod
    async def _periodic_offline_replay(cls, replay_coro, interval: int):
        """
        周期性调用 replay_coro（例如 ly.py 里的 replay_offline_transactions）
        """
        while True:
            await asyncio.sleep(interval)
            try:
                await replay_coro()
            except Exception as e:
                print(f"[offline replay error] {e}", flush=True)


    # ------------------------------
    # 工具方法
    # ------------------------------
    @classmethod
    def get_thread_id(cls, msg) -> int:
        for attr in ("reply_to_top_id", "topic_id"):
            tid = getattr(msg, attr, None)
            if tid:
                return int(tid)
        try:
            if msg.reply_to and getattr(msg.reply_to, "top_msg_id", None):
                return int(msg.reply_to.top_msg_id)
        except Exception:
            pass
        return 0

    @classmethod
    def has_url(cls, msg) -> bool:
        if msg.entities:
            for e in msg.entities:
                if isinstance(e, (MessageEntityUrl, MessageEntityTextUrl)):
                    return True
        if msg.message and cls._url_regex.search(msg.message):
            return True
        return False

    @classmethod
    def classify_message(cls, msg) -> str:
        if getattr(msg, "video", None):
            return "video"
        if getattr(msg, "sticker", None):
            return "sticker"
        doc = getattr(msg, "document", None)
        if doc and getattr(doc, "mime_type", "") in ("image/webp", "application/x-tgsticker"):
            return "sticker"
        if getattr(msg, "photo", None):
            return "photo"
        if doc:
            return "document"
        if cls.has_url(msg):
            return "url"
        if msg.message and msg.message.strip():
            return "text"
        return "other"
