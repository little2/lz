# group_stats_tracker.py

import re
import asyncio
import random
from collections import defaultdict
from datetime import timedelta

from telethon import events
from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl
from telethon.tl.functions.messages import ReportRequest
from telethon.tl.types import ReportResultAddComment, ReportResultChooseOption, ReportResultReported

from pg_stats_db import PGStatsDB
import redis.asyncio as redis_async
from lz_config import VALKEY_URL

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

    _valkey = None

    # ------------------------------
    # 初始化 / 绑定 Telethon
    # ------------------------------
    @classmethod
    def configure(cls, client, flush_interval=5, flush_batch_size=500):
        cls.client = client
        cls.flush_interval = flush_interval
        cls.flush_batch_size = flush_batch_size


        cls._valkey = redis_async.from_url(VALKEY_URL, decode_responses=True)

       

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

        # KV_TRIGGERS  = {
        #     "search_tag": [
        #         "标签筛选",
        #         "龙阳学院",
        #     ],
        # }
        # # print(f"[valkey] check triggers user_id={user_id} text={text}", flush=True)
        # for action, keywords in KV_TRIGGERS.items():
        #     if any(k in text for k in keywords):
        #         print(f"[valkey] trigger action={action} user_id={user_id}", flush=True)
        #         yymmdd = msg_time_local.strftime("%y%m%d")
        #         key = f"{action}:{user_id}"
        #         if cls._valkey:
        #             try:
        #                 await cls._valkey.set(key, yymmdd, ex=86400)
        #                 print(f"[valkey] set ok: {key}={yymmdd}", flush=True)
        #                 # confirm_val = await cls._valkey.get(key)
        #                 # print(f"[valkey] set ok: {key}={confirm_val}", flush=True)
        #             except Exception as e:
        #                 print(f"[valkey] set/get failed: {key} err={e}", flush=True)
        #         else:
        #             print("[valkey] client not ready, skip set/get", flush=True)

        await cls.check_and_report(msg)

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


    @classmethod
    async def check_and_report(cls, msg):
        """
        檢查訊息內容並提交舉報。
        1. 提取「👀查看」按鈕 URL（用於舉報理由）
        2. 提取 she11shopbot 連結（用於舉報理由）
        3. 提交舉報
        """
        # ================================
        # 第一步：取得訊息文字內容
        # ================================
        message_text = getattr(msg, "message", None) or ""
        link_prefix = "https://t.me/she11shopbot"
        has_shopbot_link = link_prefix in message_text

        

        # ================================
        # 第二步：驗證訊息 ID 和聊天 ID
        # ================================
        message_id = getattr(msg, "id", None)
        if not isinstance(message_id, int):
            return

        chat = getattr(msg, "peer_id", None)
        if not chat:
            return

        # print(f"{message_text[:30]} id={message_id} chat={chat}", flush=True)

        # ================================
        # 第三步：檢查按鈕中的「👀查看」
        # ================================
        report_url = None
       
        has_view_button = False

        reply_markup = getattr(msg, "reply_markup", None)
        rows = getattr(reply_markup, "rows", None)
        if rows:
            for row in rows:
                for btn in getattr(row, "buttons", []) or []:
                    text = getattr(btn, "text", "") or ""
                    if "👀查看" in text:
                        has_view_button = True
                        report_url = getattr(btn, "url", None) or getattr(btn, "callback_data", None)
                        break
                if has_view_button:
                    break

        if not has_view_button:
            message_buttons = getattr(msg, "buttons", None)
            if message_buttons:
                for row in message_buttons:
                    for btn in row:
                        text = getattr(btn, "text", "") or ""
                        if "👀查看" in text:
                            has_view_button = True
                            report_url = getattr(btn, "url", None) or getattr(btn, "callback_data", None)
                            break
                    if has_view_button:
                        break


        # ================================
        # 第四步：檢查 she11shopbot 連結
        # ================================



        if not has_view_button:
            if has_shopbot_link:
                match = re.search(r"https://t\.me/she11shopbot\S*", message_text)
                report_url = match.group(0) if match else None
            else:
                return

        if not has_view_button and not has_shopbot_link:
            return

        if not report_url:
            return

        # ================================
        # 第五步：組裝舉報理由並提交
        # ================================
        report_reasons = [
            "This bot shares child abuse links: {url}",
            "Possible CSAM link detected here: {url}",
            "This bot distributes illegal child content: {url}",
            "Child exploitation material may be shared here: {url}",
            "This bot appears involved in CSAM distribution: {url}",
            "Please investigate this child safety violation: {url}",
            "Suspicious child abuse content linked here: {url}",
            "This bot promotes illegal child material: {url}",
            "Potential child exploitation activity: {url}",
            "Unsafe child-related content found here: {url}",
            "This bot repeatedly shares harmful links: {url}",
            "Please review this suspected CSAM source: {url}",
            "Illegal child abuse material suspected: {url}",
            "This link may contain exploitative child content: {url}",
            "Possible child safety violation detected: {url}",
            "This bot redirects users to illegal material: {url}",
            "Suspicious CSAM-related activity from this bot: {url}",
            "This bot may violate child protection laws: {url}",
            "Please check this illegal child content link: {url}",
            "Potential exploitation network activity here: {url}",
            "This bot spreads harmful child-related content: {url}",
            "Repeated suspicious child abuse links detected: {url}",
            "This bot appears connected to CSAM sharing: {url}",
            "Please investigate this exploitative content: {url}",
            "Child abuse material may be distributed here: {url}",
            "Suspicious illegal child content source: {url}",
            "This bot shares unsafe exploitation links: {url}",
            "Potential CSAM distribution through this link: {url}",
            "This bot may expose users to illegal material: {url}",
            "Reported suspicious child exploitation link: {url}",
        ]



        reason_template = random.choice(report_reasons)
        url_for_reason = report_url or message_text[:50]
        final_reason = reason_template.format(url=url_for_reason)

        # ================================
        # 提交舉報（非同步）
        # ================================
        try:
            await cls.report_message(
                chat=chat,
                message_id=message_id,
                report_reason=final_reason
            )
            # print(f"[check_and_report] 舉報完成 id={message_id} reason='{final_reason}'", flush=True)
        except Exception as e:
            print(f"[check_and_report] 舉報失敗: {e}", flush=True)


    @classmethod
    async def report_message(
        cls,
        chat,
        message_id: int,
        report_reason: str = "child",
        keyword: str = "child",
    ) -> None:
        """按 Telegram 回传的步骤提交举报。"""
        # 需先確保 cls.client 已正確 configure
        if cls.client is None:
            raise RuntimeError("GroupStatsTracker.client 尚未 configure")

        client = cls.client
        entity = await client.get_input_entity(chat)

        # print(f"开始举报 id={message_id} reason={report_reason}", flush=True)

        result = await client(
            ReportRequest(
                peer=entity,
                id=[message_id],
                option=b"",
                message="",
            )
        )

        # print(f"举报第一步结果 id={message_id}: {result.stringify()}", flush=True)

        step = 1
        result_reported = result
        while isinstance(result, ReportResultChooseOption):
            selected = None
            # 优先锁定你指定的选项：Child sexual abuse / b'21'
            for opt in result.options:
                text = (getattr(opt, "text", "") or "").strip().lower()
                if text == "child sexual abuse" or getattr(opt, "option", None) == b"21":
                    selected = opt
                    break
            # 如果该轮没有 b'21'，再走宽松匹配
            if selected is None:
                for opt in result.options:
                    text = (getattr(opt, "text", "") or "").lower()
                    if keyword in text or "child" in text or "abuse" in text:
                        selected = opt
                        break
            if selected is None:
                raise RuntimeError("没有找到合适的举报理由，请手动查看 options。")
            # print(f"选定举报理由 {selected.text} {selected.option}", flush=True)
            result = await client(
                ReportRequest(
                    peer=entity,
                    id=[message_id],
                    option=selected.option,
                    message=report_reason,
                )
            )
            result_reported = result
            step += 1
            # print(f"举报第{step}步结果 id={message_id}: {result.stringify()}", flush=True)

        if isinstance(result, ReportResultAddComment):
            # print("需要添加备注内容，正在提交", flush=True)
            result_reported = await client(
                ReportRequest(
                    peer=entity,
                    id=[message_id],
                    option=result.option,
                    message=report_reason,
                )
            )
            # print(f"举报第三步结果 id={message_id}: {result.stringify()}", flush=True)

        if isinstance(result_reported, ReportResultReported):
            print(f"👮 举报已提交 id={message_id} reason={report_reason}", flush=True)
            #休息 1~2秒避免短时间内同一账号举报过多被 Telegram 限制
            await asyncio.sleep(random.uniform(1,2))
        else:
            print(f"返回结果 id={message_id}: {result_reported.stringify()}", flush=True)
            print(type(result_reported), flush=True)
            print(result_reported.stringify(), flush=True)



    # ------------------------------
    # 工具方法
    # ------------------------------
    @classmethod
    def get_thread_id(cls, msg) -> int:
        # 1) 最标准的：Telethon forum topic root id
        for attr in ("reply_to_top_id", "topic_id"):
            tid = getattr(msg, attr, None)
            if tid:
                return int(tid)

        # 2) 你的样本命中的情况：reply header 标记 forum_topic，但没给 top_id
        rt = getattr(msg, "reply_to", None)
        if rt and getattr(rt, "forum_topic", False):
            rid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
            if rid:
                return int(rid)

        # 3) 传统回复链的 top_msg_id（有些版本/场景会走这里）
        if rt and getattr(rt, "top_msg_id", None):
            return int(rt.top_msg_id)

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
