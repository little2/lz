# lz_pgsql.py  —— @classmethod 风格，接口与 lz_mysql.py 一致
import os
import asyncio
import asyncpg
from typing import Optional, Dict, Any, List, Tuple

from lz_config import POSTGRES_DSN
from lz_memory_cache import MemoryCache
import lz_var

# ====== 连接池参数（与原文件一致，并支持环境变量覆盖）======
DEFAULT_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
DEFAULT_MAX = int(os.getenv("POSTGRES_POOL_MAX", "5"))
ACQUIRE_TIMEOUT = float(os.getenv("POSTGRES_ACQUIRE_TIMEOUT", "10"))
COMMAND_TIMEOUT = float(os.getenv("POSTGRES_COMMAND_TIMEOUT", "60"))
CONNECT_TIMEOUT = float(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"))
CONNECT_RETRIES = int(os.getenv("POSTGRES_CONNECT_RETRIES", "2"))

# （保留：若你后续在 PG 里要做中文分词/同义词替换，这里仍可复用）
SYNONYM = {
    "滑鼠": "鼠标",
    "萤幕": "显示器",
    "笔电": "笔记本",
}


class PGPool:
    """
    参考 lz_mysql.py 的 MySQLPool 设计：
    - 使用类属性持有单例连接池
    - 所有方法采用 @classmethod
    - 提供 init_pool / ensure_pool / acquire / release / close
    - 自带一个 MemoryCache 实例（与 MySQLPool.cache 对齐）
    """

    _pool: Optional[asyncpg.Pool] = None
    _lock = asyncio.Lock()
    _cache_ready = False
    cache: Optional[MemoryCache] = None

    # ========= 连接池生命周期 =========
    @classmethod
    async def init_pool(cls) -> asyncpg.Pool:
        """
        幂等：可在多处并发调用，仅初始化一次。
        失败时按照 CONNECT_RETRIES 做指数回退重试。
        """
        if cls._pool is not None:
            if not cls._cache_ready:
                cls.cache = MemoryCache()
                cls._cache_ready = True
            return cls._pool

        async with cls._lock:
            if cls._pool is None:
                last_exc = None
                app_name = getattr(lz_var, "bot_username", "lz_app")

                for attempt in range(CONNECT_RETRIES + 1):
                    try:
                        cls._pool = await asyncpg.create_pool(
                            dsn=POSTGRES_DSN,
                            min_size=DEFAULT_MIN,
                            max_size=DEFAULT_MAX,
                            max_inactive_connection_lifetime=300,
                            command_timeout=COMMAND_TIMEOUT,
                            timeout=CONNECT_TIMEOUT,
                            statement_cache_size=1024,
                            # 👉 把这些会话参数放到这里
                            server_settings={
                                "application_name": app_name,
                                "timezone": "UTC",
                            },
                        )
                       
                        print("✅ PostgreSQL 连接池初始化完成")
                        break
                    except Exception as e:
                        last_exc = e
                        if attempt < CONNECT_RETRIES:
                            await asyncio.sleep(1.0 * (attempt + 1))
                        else:
                            raise

            if not cls._cache_ready:
                cls.cache = MemoryCache()
                cls._cache_ready = True

        return cls._pool

    @classmethod
    async def ensure_pool(cls) -> asyncpg.Pool:
        if cls._pool is None:
            await cls.init_pool()
        return cls._pool

    @classmethod
    async def acquire(cls) -> asyncpg.Connection:
        """
        获取连接；保持与 MySQLPool.get_conn_cursor 的精神一致（不过 PG 无 cursor 对象）。
        """
        await cls.ensure_pool()
        return await cls._pool.acquire(timeout=ACQUIRE_TIMEOUT)

    @classmethod
    async def release(cls, conn: Optional[asyncpg.Connection]):
        if conn and cls._pool:
            await cls._pool.release(conn)

    @classmethod
    async def close(cls):
        async with cls._lock:
            if cls._pool:
                await cls._pool.close()
                cls._pool = None
                print("🛑 PostgreSQL 连接池已关闭")

    # ========= 工具 =========
    @classmethod
    def _normalize_query(cls, keyword_str: str) -> str:
        return " ".join((keyword_str or "").strip().lower().split())

    # ========= 示例：与原 PGDB 同名/同义方法 =========
    @classmethod
    async def search_keyword_page_highlighted(
        cls, keyword_str: str, last_id: int = 0, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        以 content_seg_tsv 做全文索引查询，并用 ts_headline 高亮。
        - 与原 lz_pgsql.py 的 PGDB.search_keyword_page_highlighted 等价
        - 增加了 MemoryCache，与 MySQLPool 风格统一
        """
        query = cls._normalize_query(keyword_str)
        cache_key = f"pg:highlighted:{query}:{last_id}:{limit}"

        # 内存缓存（短期，避免抖动）
        if cls.cache:
            cached = cls.cache.get(cache_key)
            if cached:
                # print(f"🔹 MemoryCache hit for {cache_key}")
                return cached

        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(
                """
                SELECT id,
                       source_id,
                       file_type,
                       ts_headline('simple', content, plainto_tsquery('simple', $1)) AS highlighted_content
                FROM sora_content
                WHERE content_seg_tsv @@ plainto_tsquery('simple', $1)
                  AND id > $2
                ORDER BY id ASC
                LIMIT $3
                """,
                query, int(last_id), int(limit)
            )
            result = [dict(r) for r in rows]
            if cls.cache:
                cls.cache.set(cache_key, result, ttl=60)
            return result
        finally:
            await cls.release(conn)

    # ========= 你可能会用到的通用执行封装（可选）=========
    @classmethod
    async def fetch(cls, sql: str, *args, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        通用查询封装：返回 List[dict]；与 asyncpg.fetch 对齐。
        """
        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(sql, *args, timeout=timeout)
            return [dict(r) for r in rows]
        finally:
            await cls.release(conn)

    @classmethod
    async def fetchrow(cls, sql: str, *args, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        通用单行查询封装：返回 dict 或 None；与 asyncpg.fetchrow 对齐。
        """
        conn = None
        try:
            conn = await cls.acquire()
            row = await conn.fetchrow(sql, *args, timeout=timeout)
            return dict(row) if row else None
        finally:
            await cls.release(conn)

    @classmethod
    async def execute(cls, sql: str, *args, timeout: Optional[float] = None) -> str:
        """
        通用执行封装：与 asyncpg.execute 对齐，返回命令标签（如 'UPDATE 3'）。
        """
        conn = None
        try:
            conn = await cls.acquire()
            return await conn.execute(sql, *args, timeout=timeout)
        finally:
            await cls.release(conn)

    @classmethod
    async def executemany(cls, sql: str, args_seq: List[tuple], timeout: Optional[float] = None) -> None:
        """
        批量执行封装：与 asyncpg.executemany 对齐。
        """
        conn = None
        try:
            conn = await cls.acquire()
            await conn.executemany(sql, args_seq, timeout=timeout)
        finally:
            await cls.release(conn)

    @classmethod
    async def upsert_product_thumb(
        cls,
        content_id: int,
        thumb_file_unique_id: str,
        thumb_file_id: str,
        bot_username: str,
    ):
        """
        更新缩图信息（PostgreSQL 版本，@classmethod）：
        - sora_content: 若传入 thumb_file_unique_id，则更新该 content_id 的缩略图字段
        - sora_media: 以 (content_id, source_bot_name) 为唯一键做 UPSERT，更新 thumb_file_id
        * 需要 sora_media 上有唯一约束：UNIQUE (content_id, source_bot_name)

        返回:
        {
            "sora_content_updated_rows": int,   # UPDATE 影响行数（0/1）
            "sora_media_upsert_action": "insert" 或 "update"
        }
        """
        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            print(
                f"[PG upsert_product_thumb] fuid={thumb_file_unique_id} fid={thumb_file_id} "
                f"content_id={content_id} bot={bot_username}",
                flush=True,
            )

            async with conn.transaction():
                # 1) 更新 sora_content（有传才更）
                content_rows = 0
                if thumb_file_unique_id:
                    sql_update_content = """
                        UPDATE sora_content
                        SET thumb_file_unique_id = $1
                        WHERE id = $2
                    """
                    tag = await conn.execute(sql_update_content, thumb_file_unique_id, content_id)
                    # asyncpg 的 execute 返回类似 'UPDATE 1'，取最后的数字即影响行数
                    try:
                        content_rows = int(tag.split()[-1])
                    except Exception:
                        content_rows = 0

                    print(f"✅ [X-MEDIA][PG] UPDATE sora_content tag: {tag}; rows={content_rows}", flush=True)

                # 2) UPSERT sora_media（依 (content_id, source_bot_name) 唯一约束）
                #    使用 RETURNING (xmax = 0) AS inserted 来判断是插入还是更新：
                #      * inserted=True 表示新插入
                #      * inserted=False 表示触发了冲突并执行了 UPDATE
                sql_upsert_media = """
                    INSERT INTO sora_media (content_id, source_bot_name, thumb_file_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (content_id, source_bot_name)
                    DO UPDATE SET thumb_file_id = EXCLUDED.thumb_file_id
                    RETURNING (xmax = 0) AS inserted
                """
                row = await conn.fetchrow(sql_upsert_media, content_id, bot_username, thumb_file_id)
                upsert_action = "insert" if (row and row.get("inserted")) else "update"

                print(f"✅ [X-MEDIA][PG] UPSERT sora_media done; action={upsert_action}", flush=True)

            print(f"✅ [X-MEDIA][PG] 事务完成: content_rows={content_rows}, media_action={upsert_action}", flush=True)
            return {
                "sora_content_updated_rows": content_rows,
                "sora_media_upsert_action": upsert_action,
            }

        except Exception as e:
            # async with conn.transaction() 失败会自动回滚，这里仅打日志
            print(f"❌ [X-MEDIA][PG] upsert_product_thumb error: {e}", flush=True)
            raise
        finally:
            await cls.release(conn)
