# pg_stats_db.py
import asyncpg
import asyncio
from typing import Any, Dict, List,Optional  # ⬅ 新增
import json

class PGStatsDB:
    """
    PostgreSQL 操作层（纯 classmethod 风格）
    与 GroupStatsTracker 的 key 对齐：
      (stat_date, user_id, chat_id, thread_id, msg_type, from_bot, hour)
    """

    pool: asyncpg.Pool | None = None
    _lock = asyncio.Lock()
    _offline_tx_table_inited: bool = False 

    @classmethod
    async def init_pool(cls, dsn: str, min_size: int = 1, max_size: int = 5):
        if cls.pool is not None:
            return cls.pool

        cls.pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=min_size,
            max_size=max_size
        )
        return cls.pool

    @classmethod
    async def close_pool(cls):
        if cls.pool:
            await cls.pool.close()
            cls.pool = None

    @classmethod
    async def ensure_table(cls):
        """
        ✅ 最终版表结构：含 from_bot + hour
        """
        ddl = """
        CREATE TABLE IF NOT EXISTS tg_msg_stats_daily (
            stat_date  DATE        NOT NULL,
            user_id    BIGINT      NOT NULL,
            chat_id    BIGINT      NOT NULL,
            thread_id  BIGINT      NOT NULL DEFAULT 0,
            msg_type   TEXT        NOT NULL,
            from_bot   BOOLEAN     NOT NULL DEFAULT FALSE,
            hour       SMALLINT    NOT NULL,
            cnt        INTEGER     NOT NULL DEFAULT 0,
            PRIMARY KEY (
                stat_date, user_id, chat_id, thread_id,
                msg_type, from_bot, hour
            )
        );

        CREATE INDEX IF NOT EXISTS idx_stats_chat_date
        ON tg_msg_stats_daily (chat_id, stat_date);

        CREATE INDEX IF NOT EXISTS idx_stats_user_date
        ON tg_msg_stats_daily (user_id, stat_date);

        
        -- ============================================
        -- 2) 精简版 user 表（新增）
        -- ============================================
        CREATE TABLE IF NOT EXISTS "user" (
            user_id BIGINT PRIMARY KEY,
            credit INT DEFAULT 10,
            point INT NOT NULL DEFAULT 0,
            task_award_date DATE DEFAULT NULL,
            task_award_count INT DEFAULT 0,
            last_task_award_at TIMESTAMPTZ DEFAULT NULL
        );

        -- 话题系统用：
        -- 1) tg_group_messages_raw：保存每条文本消息（含 message_id），供 BERTopic 聚类
        -- 2) tg_group_topics_hourly：保存每小时的话题摘要 + message_ids（证据链）

        CREATE TABLE IF NOT EXISTS tg_group_messages_raw (
            chat_id      BIGINT      NOT NULL,
            thread_id    BIGINT      NOT NULL DEFAULT 0,
            message_id   BIGINT      NOT NULL,
            user_id      BIGINT      NOT NULL,
            from_bot     BOOLEAN     NOT NULL DEFAULT FALSE,
            msg_time_utc TIMESTAMPTZ NOT NULL,
            stat_date    DATE        NOT NULL,
            hour         SMALLINT    NOT NULL,
            text         TEXT        NOT NULL,

            topic_id     INTEGER     NULL,
            topic_ver    INTEGER     NOT NULL DEFAULT 1,
            topic_at     TIMESTAMPTZ NULL,

            PRIMARY KEY (chat_id, message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_raw_chat_date_hour
        ON tg_group_messages_raw (chat_id, stat_date, hour);

        CREATE INDEX IF NOT EXISTS idx_raw_topic_lookup
        ON tg_group_messages_raw (chat_id, stat_date, hour, topic_id);

        CREATE TABLE IF NOT EXISTS tg_group_topics_hourly (
            chat_id     BIGINT   NOT NULL,
            thread_id   BIGINT   NOT NULL DEFAULT 0,
            stat_date   DATE     NOT NULL,
            hour        SMALLINT NOT NULL,
            topic_id    INTEGER  NOT NULL,
            msg_count   INTEGER  NOT NULL DEFAULT 0,
            topic_words TEXT     NOT NULL DEFAULT '',
            keywords    TEXT     NOT NULL DEFAULT '',
            message_ids JSONB    NOT NULL DEFAULT '[]'::jsonb,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, thread_id, stat_date, hour, topic_id)
        );

        CREATE INDEX IF NOT EXISTS idx_topics_chat_date_hour
        ON tg_group_topics_hourly (chat_id, stat_date, hour);
        """

        async with cls._lock:
            if cls.pool is None:
                raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
            async with cls.pool.acquire() as conn:
                await conn.execute(ddl)

        





    @classmethod
    async def upsert_daily_counts(cls, items: list[tuple[tuple, int]]):
        """
        items:
          [ ((stat_date,user_id,chat_id,thread_id,msg_type,from_bot,hour), cnt), ... ]
        """
        if not items:
            return
       
        sql = """
        INSERT INTO tg_msg_stats_daily
            (stat_date, user_id, chat_id, thread_id, msg_type, from_bot, hour, cnt)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (
            stat_date, user_id, chat_id, thread_id,
            msg_type, from_bot, hour
        )
        DO UPDATE SET cnt = tg_msg_stats_daily.cnt + EXCLUDED.cnt;
        """

        async with cls._lock:
            async with cls.pool.acquire() as conn:
                async with conn.transaction():
                    for (key, c) in items:
                        (
                            stat_date, user_id, chat_id,
                            thread_id, msg_type, from_bot, hour
                        ) = key

                        await conn.execute(
                            sql,
                            stat_date, user_id, chat_id,
                            thread_id, msg_type, from_bot, hour, c
                        )

    # ================== 离线交易队列表 ==================

    @classmethod
    async def ensure_offline_tx_table(cls):
        """
        建立离线交易队列表 offline_transaction_queue
        需要在 init_pool(dsn) 之后调用一次。
        """
        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 PGStatsDB.init_pool(dsn)")

        # 避免每次都重建
        if cls._offline_tx_table_inited:
            return

        ddl = """
        CREATE TABLE IF NOT EXISTS offline_transaction_queue (
            id                      BIGSERIAL PRIMARY KEY,
            sender_id               BIGINT      NOT NULL,
            receiver_id             BIGINT      NULL,
            transaction_type        TEXT        NOT NULL,
            transaction_description TEXT        NOT NULL,
            sender_fee              INTEGER     NOT NULL,
            receiver_fee            INTEGER     NOT NULL,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed               BOOLEAN     NOT NULL DEFAULT FALSE,
            processed_at            TIMESTAMPTZ NULL,
            last_error              TEXT        NULL
        );
        """
        async with cls.pool.acquire() as conn:
            await conn.execute(ddl)

        cls._offline_tx_table_inited = True

    @classmethod
    async def record_offline_transaction(cls, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        MySQL 不可用时的降级方案：
        1. 先在 PostgreSQL 的 "user" 表中扣/加 point（以 PG 当前的 point 为准）
        2. 把整笔交易写到 offline_transaction_queue，等 MySQL 恢复后再回放

        预期 transaction_data 结构：
        {
            "sender_id": int,
            "receiver_id": int,
            "transaction_type": str,
            "transaction_description": str,
            "sender_fee": int,    # 通常是负数
            "receiver_fee": int,  # 通常是正数
            ...
        }
        """
        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 PGStatsDB.init_pool(dsn)")

        # 确保离线交易表存在
        await cls.ensure_offline_tx_table()

        sender_id = int(transaction_data["sender_id"])
        receiver_id = int(transaction_data.get("receiver_id") or 0)
        sender_fee = int(transaction_data["sender_fee"])
        receiver_fee = int(transaction_data["receiver_fee"])
        tx_type = transaction_data["transaction_type"]
        tx_desc = transaction_data["transaction_description"]

        async with cls.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # 1) 锁定 sender 的 point
                    row = await conn.fetchrow(
                        'SELECT point FROM "user" WHERE user_id = $1 FOR UPDATE',
                        sender_id,
                    )
                    if not row:
                        return {
                            "ok": "",
                            "status": "pg_user_not_found",
                            "transaction_data": transaction_data,
                        }

                    current_point = int(row["point"] or 0)
                    if current_point < abs(sender_fee):
                        return {
                            "ok": "",
                            "status": "pg_insufficient_funds",
                            "transaction_data": transaction_data,
                            "user_info": {"point": current_point},
                        }

                    # 2) 扣 sender 点数（sender_fee 一般是负数）
                    await conn.execute(
                        'UPDATE "user" SET point = point + $1 WHERE user_id = $2',
                        sender_fee,
                        sender_id,
                    )

                    # 3) 加 receiver 点数（如果有）
                    if receiver_id:
                        await conn.execute(
                            'UPDATE "user" SET point = point + $1 WHERE user_id = $2',
                            receiver_fee,
                            receiver_id,
                        )

                    # 4) 写入离线交易队列
                    row = await conn.fetchrow(
                        """
                        INSERT INTO offline_transaction_queue (
                            sender_id,
                            receiver_id,
                            transaction_type,
                            transaction_description,
                            sender_fee,
                            receiver_fee
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                        RETURNING id, created_at
                        """,
                        sender_id,
                        receiver_id if receiver_id != 0 else None,
                        tx_type,
                        tx_desc,
                        sender_fee,
                        receiver_fee,
                    )

                print(
                    f'✅ [PG offline] 已记录离线交易 id={row["id"]} type={tx_type} desc={tx_desc}',
                    flush=True,
                )
                return {
                    "ok": "1",
                    "status": "offline_queue",
                    "offline_id": int(row["id"]),
                    "transaction_data": transaction_data,
                }

            except Exception as e:
                print(f"❌ [PG offline] record_offline_transaction 出错: {e}", flush=True)
                return {
                    "ok": "",
                    "status": "pg_offline_error",
                    "error": str(e),
                    "transaction_data": transaction_data,
                }

 

    @classmethod
    async def find_transaction_by_description(cls, desc: str) -> Optional[Dict[str, Any]]:
        """
        根据 transaction_description 在 PostgreSQL 中查询一笔交易纪录。

        目前仅查询 offline_transaction_queue：
        - 用于 MySQL 挂掉、交易改记到 PG 离线队列时的“查重”/确认用途
        - 语义上等价于 MySQLPool.find_transaction_by_description，但数据来源不同

        :param desc: 例如 "chat_id message_id"
        :return: dict | None
        """
        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 PGStatsDB.init_pool(dsn)")

       

        async with cls.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    SELECT
                        id,
                        sender_id,
                        receiver_id,
                        transaction_type,
                        transaction_description,
                        sender_fee,
                        receiver_fee,
                        created_at,
                        processed,
                        processed_at,
                        last_error
                    FROM offline_transaction_queue
                    WHERE transaction_description = $1
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    desc,
                )
                # 与 MySQL 版行为对齐：查不到就回 None，查到就回 dict
                return dict(row) if row else None

            except Exception as e:
                print(f"⚠️ PGStatsDB.find_transaction_by_description 出错: {e}", flush=True)
                return None

   

    @classmethod
    async def sync_user_from_mysql(cls, max_batch: int = 1000) -> int:
        """
        单向同步：
        - 从 MySQL telebot.user 抓出按 update_time 排序的最多 max_batch 笔
        - 把 user_id / credit / point / task_award_date / task_award_count / last_task_award_at
          upsert 到 PostgreSQL 的 "user" 表

        注意：
        - 只负责 MySQL → PostgreSQL，不会反向改 MySQL
        - 需要外部先调用 MySQLPool.init_pool / PGStatsDB.init_pool / PGStatsDB.ensure_table()
        """
        # 延迟引入，避免循环依赖
        from lz_mysql import MySQLPool

        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 PGStatsDB.init_pool()")

        # 1) 从 MySQL 抓最近更新的 user 记录
        await MySQLPool.ensure_pool()
        conn_mysql, cur_mysql = await MySQLPool.get_conn_cursor()
        try:
            sql = (
                "SELECT user_id, credit, point, task_award_date, "
                "       task_award_count, last_task_award_at "
                "FROM user "
                "ORDER BY update_time DESC "
                "LIMIT %s"
            )
            await cur_mysql.execute(sql, (max_batch,))
            rows = await cur_mysql.fetchall()
        finally:
            await MySQLPool.release(conn_mysql, cur_mysql)

        if not rows:
            print("✅ sync_user_from_mysql: MySQL 无更新记录可同步。", flush=True)
            return 0

        # 2) 组装参数列表，写入 PostgreSQL
        records: List[tuple] = []
        for row in rows:
            # aiomysql 一般是 dict-like row
            user_id = int(row["user_id"])
            credit = row.get("credit", 10)
            point = row.get("point", 0)
            task_award_date = row.get("task_award_date")
            task_award_count = row.get("task_award_count", 0)
            last_task_award_at = row.get("last_task_award_at")

            records.append(
                (
                    user_id,
                    int(credit) if credit is not None else 10,
                    int(point) if point is not None else 0,
                    task_award_date,
                    int(task_award_count) if task_award_count is not None else 0,
                    last_task_award_at,
                )
            )

        async with cls.pool.acquire() as conn_pg:
            sql_pg = """
                INSERT INTO "user" (
                    user_id,
                    credit,
                    point,
                    task_award_date,
                    task_award_count,
                    last_task_award_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (user_id) DO UPDATE SET
                    credit = EXCLUDED.credit,
                    point  = EXCLUDED.point,
                    task_award_date = EXCLUDED.task_award_date,
                    task_award_count = EXCLUDED.task_award_count,
                    last_task_award_at = EXCLUDED.last_task_award_at
            """
            async with conn_pg.transaction():
                await conn_pg.executemany(sql_pg, records)

        print(f"✅ sync_user_from_mysql: 已同步 {len(records)} 笔 user 记录到 PostgreSQL。", flush=True)
        return len(records)

    # （写入原始语料，含 message_id）
    @classmethod
    async def upsert_raw_messages(cls, rows: list[dict]):
        """
        rows: [
          {
            "chat_id": int,
            "thread_id": int,
            "message_id": int,
            "user_id": int,
            "from_bot": bool,
            "msg_time_utc": datetime,   # msg.date
            "stat_date": date,          # msg.date +8h 的 date
            "hour": int,                # msg.date +8h 的 hour
            "text": str,
          }, ...
        ]
        """
        if not rows:
            return

        sql = """
        INSERT INTO tg_group_messages_raw
            (chat_id, thread_id, message_id, user_id, from_bot,
             msg_time_utc, stat_date, hour, text)
        VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (chat_id, message_id)
        DO UPDATE SET
            thread_id    = EXCLUDED.thread_id,
            user_id      = EXCLUDED.user_id,
            from_bot     = EXCLUDED.from_bot,
            msg_time_utc = EXCLUDED.msg_time_utc,
            stat_date    = EXCLUDED.stat_date,
            hour         = EXCLUDED.hour,
            text         = EXCLUDED.text;
        """

        async with cls._lock:
            if cls.pool is None:
                raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
            async with cls.pool.acquire() as conn:
                async with conn.transaction():
                    for r in rows:
                        await conn.execute(
                            sql,
                            int(r["chat_id"]),
                            int(r.get("thread_id") or 0),
                            int(r["message_id"]),
                            int(r["user_id"]),
                            bool(r.get("from_bot", False)),
                            r["msg_time_utc"],
                            r["stat_date"],
                            int(r["hour"]),
                            r["text"],
                        )


    @classmethod
    async def fetch_hour_texts(cls, chat_id: int, stat_date, hour: int, thread_id: int = 0, min_len: int = 3):
        sql = """
        SELECT message_id, text
        FROM tg_group_messages_raw
        WHERE chat_id=$1 AND stat_date=$2 AND hour=$3 AND thread_id=$4
          AND from_bot=FALSE
          AND length(text) >= $5
        ORDER BY message_id ASC
        """
        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(sql, int(chat_id), stat_date, int(hour), int(thread_id), int(min_len))
            return [{"message_id": int(r["message_id"]), "text": r["text"]} for r in rows]

    @classmethod
    async def update_message_topics(cls, chat_id: int, stat_date, hour: int, mapping: dict[int, int], thread_id: int = 0, topic_ver: int = 1):
        """
        mapping: { message_id: topic_id, ... }
        """
        if not mapping:
            return

        sql = """
        UPDATE tg_group_messages_raw
        SET topic_id=$5, topic_ver=$6, topic_at=NOW()
        WHERE chat_id=$1 AND stat_date=$2 AND hour=$3 AND thread_id=$4 AND message_id=$7
        """
        async with cls._lock:
            if cls.pool is None:
                raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
            async with cls.pool.acquire() as conn:
                async with conn.transaction():
                    for mid, tid in mapping.items():
                        await conn.execute(
                            sql,
                            int(chat_id), stat_date, int(hour), int(thread_id),
                            int(tid), int(topic_ver), int(mid)
                        )

    @classmethod
    async def upsert_topics_hourly(
        cls,
        chat_id: int,
        thread_id: int,
        stat_date,
        hour: int,
        topics: list[dict],
    ):
        """
        topics: [
          {
            "topic_id": int,
            "msg_count": int,
            "topic_words": str,     # BERTopic 的词（可选）
            "keywords": str,        # pke_zh 精炼短语（建议逗号分隔）
            "message_ids": list[int]  # 代表消息 ID（证据链）
          }, ...
        ]
        """
        if not topics:
            return

        sql = """
        INSERT INTO tg_group_topics_hourly
            (chat_id, thread_id, stat_date, hour, topic_id, msg_count, topic_words, keywords, message_ids, updated_at)
        VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
        ON CONFLICT (chat_id, thread_id, stat_date, hour, topic_id)
        DO UPDATE SET
            msg_count   = EXCLUDED.msg_count,
            topic_words = EXCLUDED.topic_words,
            keywords    = EXCLUDED.keywords,
            message_ids = EXCLUDED.message_ids,
            updated_at  = NOW();
        """

        async with cls._lock:
            if cls.pool is None:
                raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
            async with cls.pool.acquire() as conn:
                async with conn.transaction():
                    for t in topics:
                        await conn.execute(
                            sql,
                            int(chat_id),
                            int(thread_id or 0),
                            stat_date,
                            int(hour),
                            int(t["topic_id"]),
                            int(t.get("msg_count", 0)),
                            t.get("topic_words", "") or "",
                            t.get("keywords", "") or "",
                            json.dumps([int(x) for x in (t.get("message_ids") or [])], ensure_ascii=False),
                        )

    @classmethod
    async def get_topics_hourly(cls, chat_id: int, stat_date, hour: int, thread_id: int = 0, limit: int = 10):
        sql = """
        SELECT topic_id, msg_count, keywords, topic_words, message_ids
        FROM tg_group_topics_hourly
        WHERE chat_id=$1 AND stat_date=$2 AND hour=$3 AND thread_id=$4
        ORDER BY msg_count DESC
        LIMIT $5
        """
        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(sql, int(chat_id), stat_date, int(hour), int(thread_id), int(limit))
            return [dict(r) for r in rows]

    @classmethod
    async def get_topic_all_message_ids(cls, chat_id: int, stat_date, hour: int, topic_id: int, thread_id: int = 0, limit: int = 500):
        """
        反查：某小时某 topic 的所有 message_id（不仅是代表 message_ids）
        """
        sql = """
        SELECT message_id
        FROM tg_group_messages_raw
        WHERE chat_id=$1 AND stat_date=$2 AND hour=$3 AND thread_id=$4 AND topic_id=$5
        ORDER BY message_id ASC
        LIMIT $6
        """
        if cls.pool is None:
            raise RuntimeError("PGStatsDB.pool 尚未初始化，请先调用 init_pool()")
        async with cls.pool.acquire() as conn:
            rows = await conn.fetch(sql, int(chat_id), stat_date, int(hour), int(thread_id), int(topic_id), int(limit))
            return [int(r["message_id"]) for r in rows]
