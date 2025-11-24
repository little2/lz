# pg_stats_db.py
import asyncpg
import asyncio


class PGStatsDB:
    """
    PostgreSQL 操作层（纯 classmethod 风格）
    与 GroupStatsTracker 的 key 对齐：
      (stat_date, user_id, chat_id, thread_id, msg_type, from_bot, hour)
    """

    pool: asyncpg.Pool | None = None
    _lock = asyncio.Lock()

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
        """
        async with cls.pool.acquire() as conn:
            await conn.execute(ddl)

        # （可选）推荐索引：便于查报表
        await cls.ensure_indexes()

    @classmethod
    async def ensure_indexes(cls):
        """
        可选索引：不影响写入，只让查询更快
        """
        idx_sql = """
        CREATE INDEX IF NOT EXISTS idx_stats_chat_date
        ON tg_msg_stats_daily (chat_id, stat_date);

        CREATE INDEX IF NOT EXISTS idx_stats_user_date
        ON tg_msg_stats_daily (user_id, stat_date);
        """
        async with cls.pool.acquire() as conn:
            await conn.execute(idx_sql)

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
