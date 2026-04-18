# lz_pgsql.py  —— @classmethod 风格，接口与 lz_mysql.py 一致
import os
import asyncio
import asyncpg
from typing import Optional, Dict, Any, List, Tuple
import jieba

from lz_config import POSTGRES_DSN,VALKEY_URL
from lz_memory_cache import MemoryCache
from lz_cache import TwoLevelCache
import lz_var
from opencc import OpenCC
from handlers.handle_jieba_export import ensure_and_load_lexicon_runtime


# ====== 连接池参数（与原文件一致，并支持环境变量覆盖）======
DEFAULT_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
DEFAULT_MAX = int(os.getenv("POSTGRES_POOL_MAX", "2"))
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
    _table_columns_cache: Dict[str, set] = {}


    # ========= 连接池生命周期 =========
    @classmethod
    async def init_pool(cls) -> asyncpg.Pool:
        """
        幂等：可在多处并发调用，仅初始化一次。
        失败时按照 CONNECT_RETRIES 做指数回退重试。
        """
        if cls._pool is not None:
            if not cls._cache_ready:

                # cls.cache = MemoryCache()
                cls.cache = TwoLevelCache(
                    valkey_client=VALKEY_URL,
                    namespace="lz:"
                )
                cls._cache_ready = True
            return cls._pool

        async with cls._lock:
            if cls._pool is None:
                last_exc = None
                app_name = getattr(lz_var, "bot_username", "lz_app")
                if lz_var.bot_username is None:
                    app_name = "lz_app"
                for attempt in range(CONNECT_RETRIES + 1):
                    try:
                        cls._pool = await asyncpg.create_pool(
                            dsn=POSTGRES_DSN,
                            min_size=DEFAULT_MIN,
                            max_size=DEFAULT_MAX,
                            max_inactive_connection_lifetime=300,
                            command_timeout=COMMAND_TIMEOUT,
                            timeout=CONNECT_TIMEOUT,
                            statement_cache_size=0,
                            # server_settings=None,  # ✅ 先置空
                            # 👉 把这些会话参数放到这里
                            server_settings={
                                "application_name": app_name,
                                "timezone": "UTC",
                            },
                        )
                       
                        print("✅ PostgreSQL 连接池初始化完成")
                        # ✅ 新增：启动时自动确保/导出/载入词库（全局幂等）
                        await ensure_and_load_lexicon_runtime(output_dir=".", export_if_missing=True)
                        break
                    except Exception as e:
                        last_exc = e
                        if attempt < CONNECT_RETRIES:
                            await asyncio.sleep(1.0 * (attempt + 1))
                        else:
                            raise

            if not cls._cache_ready:
                # cls.cache = MemoryCache()
                cls.cache = TwoLevelCache(
                    valkey_client=VALKEY_URL,
                    namespace="lz:"
                )
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


    @classmethod
    async def delete_cache(cls, key: str, use_prefix: bool = True):
        """
        删除缓存。

        - use_prefix=True: 删除以 key 开头的所有缓存键
        - use_prefix=False: 仅精确删除 key 本身

        - MemoryCache: 无 keys() 接口，因此从内部 _store 取 key
        - TwoLevelCache: 仅清 L1（L2 依业务需要可扩展批量删；目前保持轻量，不阻塞）
        """
        if not cls.cache:
            return

        if use_prefix and hasattr(cls.cache, "delete_prefix"):
            await cls.cache.delete_prefix(key)
            print(f"✅ 已清理 PostgreSQL 前缀缓存: {key}", flush=True)
            return

        if not use_prefix:
            try:
                if hasattr(cls.cache, "delete"):
                    cls.cache.delete(key)
                else:
                    l1 = getattr(cls.cache, "l1", None)
                    if l1 is None:
                        l1 = cls.cache
                    store = getattr(l1, "_store", None)
                    if store is not None:
                        store.pop(key, None)
                print(f"✅ 已精确清理 PostgreSQL 缓存: {key}", flush=True)
            except Exception:
                pass
            return

        # 统一拿到 L1 的 store（兼容 MemoryCache / TwoLevelCache）
        l1 = getattr(cls.cache, "l1", None)
        if l1 is None:
            l1 = cls.cache  # 可能直接是 MemoryCache

        store = getattr(l1, "_store", None)
        if not store:
            return

        keys_to_delete = [k for k in list(store.keys()) if str(k).startswith(key)]
        for k in keys_to_delete:
            try:
                # TwoLevelCache / MemoryCache 都支持 delete(key)
                if hasattr(cls.cache, "delete"):
                    cls.cache.delete(k)
                else:
                    store.pop(k, None)
            except Exception:
                pass

        print(f"✅ 已清理 PostgreSQL 前缀缓存: {key}", flush=True)

    # ========= 工具 =========
    @classmethod
    def _normalize_query(cls, keyword_str: str) -> str:
        return " ".join((keyword_str or "").strip().lower().split())

    @classmethod
    def replace_synonym(cls, text: str) -> str:
        for k, v in SYNONYM.items():
            text = text.replace(k, v)
        return text


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
            cached = await cls.cache.get(cache_key)
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
        thumb_hash: Optional[str] = None,
    ):
        """
        更新缩图信息（PostgreSQL 版本，@classmethod）：
        - sora_content: 若传入 thumb_file_unique_id / thumb_hash，则更新该 content_id 的缩略图字段
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
                f"[PG upsert_product_thumb] thumb_file_unique_id={thumb_file_unique_id} thumb_file_id={thumb_file_id} thumb_hash={thumb_hash} "
                f"content_id={content_id} bot={bot_username}",
                flush=True,
            )

            async with conn.transaction():
                # 1) 更新 sora_content（有传才更）
                content_rows = 0
                if thumb_file_unique_id or thumb_hash is not None:
                    set_clauses: List[str] = []
                    params: List[Any] = []
                    param_idx = 1

                    if thumb_file_unique_id:
                        set_clauses.append(f"thumb_file_unique_id = ${param_idx}")
                        params.append(thumb_file_unique_id)
                        param_idx += 1

                    if thumb_hash is not None:
                        set_clauses.append(f"thumb_hash = ${param_idx}")
                        params.append(thumb_hash)
                        param_idx += 1

                    params.append(content_id)
                    sql_update_content = f"""
                        UPDATE sora_content
                        SET {', '.join(set_clauses)}
                        WHERE id = ${param_idx}
                    """
                    tag = await conn.execute(sql_update_content, *params)
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

    @classmethod
    async def reset_sora_media_by_id(cls, content_id, bot_username):
        
        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            async with conn.transaction():
                sql_update_content = """
                    UPDATE sora_media
                    SET thumb_file_id = NULL
                    WHERE content_id = $1 and source_bot_name <> $2
                """
                await conn.execute(sql_update_content, int(content_id), bot_username)
                    # asyncpg 的 execute 返回类似 'UPDATE 1'，取最后的数字即影响行数
        except Exception as e:
            # async with conn.transaction() 失败会自动回滚，这里仅打日志
            print(f"❌ [X-MEDIA][PG] upsert_product_thumb error: {e}", flush=True)
            raise
        finally:
            await cls.release(conn)



    # 更新 sora_content / product 表（PostgreSQL 版）
    @classmethod
    async def upsert_sora(cls, mysql_row: Dict[str, Any]) -> int:
        """
        将 MySQL 的 sora_content 一行 upsert 到 PostgreSQL：
        1) upsert public.sora_content
        2) 若含商品信息，则 upsert public.product（以 content_id 为冲突键）
        返回：受影响的总行数（sora_content + product 的近似和）
        """
        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            async with conn.transaction():
                # ---------- 1) 准备 sora_content 字段 ----------
                content_id = int(mysql_row["id"])
                source_id = mysql_row.get("source_id")
                file_type = mysql_row.get("file_type")
                content = (mysql_row.get("content") or "").strip()
                file_size = mysql_row.get("file_size")
                duration = mysql_row.get("duration")
                tag = mysql_row.get("tag")
                thumb_file_unique_id = mysql_row.get("thumb_file_unique_id")
                owner_user_id = mysql_row.get("owner_user_id")
                stage = mysql_row.get("stage", "updated")
                plan_update_timestamp = mysql_row.get("plan_update_timestamp")
                
                thumb_hash = mysql_row.get("thumb_hash")
                valid_state = mysql_row.get("valid_state", 1)
                file_password = (mysql_row.get("file_password") or "").strip()

                # content_seg 由 MySQL 端预先生成；PG 仅负责落库。
                content_seg = (mysql_row.get("content_seg") or "").strip()




                sql_sora = """
                    INSERT INTO sora_content (
                        id, source_id, file_type, content, content_seg,
                        file_size, duration, tag,
                        thumb_file_unique_id, owner_user_id, stage,
                        plan_update_timestamp, thumb_hash, valid_state, file_password
                    ) VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8,
                        $9, $10, $11,
                        $12, $13, $14, $15
                    )
                    ON CONFLICT (id)
                    DO UPDATE SET
                        source_id            = EXCLUDED.source_id,
                        file_type            = EXCLUDED.file_type,
                        content              = EXCLUDED.content,
                        content_seg          = EXCLUDED.content_seg,
                        file_size            = EXCLUDED.file_size,
                        duration             = EXCLUDED.duration,
                        tag                  = EXCLUDED.tag,
                        thumb_file_unique_id = EXCLUDED.thumb_file_unique_id,
                        -- 仅当原纪录 owner_user_id 为 NULL 或 0 时才更新；否则保留原值
                        owner_user_id = CASE
                            WHEN sora_content.owner_user_id IS NULL OR sora_content.owner_user_id = 0
                                THEN EXCLUDED.owner_user_id
                            ELSE sora_content.owner_user_id
                        END,

                        -- 冲突更新时一律重置为 'pending'
                        stage = 'pending',
                        plan_update_timestamp= EXCLUDED.plan_update_timestamp,
                        thumb_hash           = EXCLUDED.thumb_hash,
                        valid_state          = EXCLUDED.valid_state,
                        file_password        = EXCLUDED.file_password
                        
                """
                tag_ret1 = await conn.execute(
                    sql_sora,
                    content_id, source_id, file_type, content, content_seg,
                    file_size, duration, tag,
                    thumb_file_unique_id, owner_user_id, stage,
                    plan_update_timestamp, thumb_hash, valid_state, file_password
                )
                try:
                    affected1 = int(tag_ret1.split()[-1])
                except Exception:
                    affected1 = 0

                # ---------- 2) 若含商品信息，upsert product（以 content_id 唯一） ----------
                # 你的 MySQL 查询别名：
                #   p.price  as fee
                #   p.file_type as product_type
                #   p.owner_user_id
                #   p.purchase_condition
                #   g.guild_id
                fee = mysql_row.get("fee")
                product_type = mysql_row.get("product_type")
                p_owner_user_id = mysql_row.get("owner_user_id")
                purchase_condition = mysql_row.get("purchase_condition")
                guild_id = mysql_row.get("guild_id")
                product_id = mysql_row.get("product_id")
                review_status = mysql_row.get("review_status", 0)

                affected2 = 0
                if any(v is not None for v in (fee, product_type, p_owner_user_id, purchase_condition, guild_id)):
                    # price 为 NOT NULL，确保是整数；无则 0
                    try:
                        price_int = int(fee) if fee is not None else 0
                    except Exception:
                        price_int = 0

                    sql_product = """
                        INSERT INTO product (
                            id,content_id, price, file_type, owner_user_id, purchase_condition, guild_id, review_status,
                            created_at, updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8,NOW(), NOW())
                        ON CONFLICT (content_id)
                        DO UPDATE SET
                            content_id         = EXCLUDED.content_id,
                            price              = EXCLUDED.price,
                            file_type          = EXCLUDED.file_type,
                            owner_user_id      = EXCLUDED.owner_user_id,
                            purchase_condition = EXCLUDED.purchase_condition,
                            guild_id           = EXCLUDED.guild_id,
                            review_status      = EXCLUDED.review_status,
                            updated_at         = NOW()
                    """
                    tag_ret2 = await conn.execute(
                        sql_product,
                        product_id, content_id, price_int, product_type, p_owner_user_id, purchase_condition, guild_id, review_status
                    )
                    try:
                        affected2 = int(tag_ret2.split()[-1])
                    except Exception:
                        affected2 = 0

                return affected1 + affected2
        finally:
            await cls.release(conn)


    # ========= Collect 相关 =========


    # ========= Album 相关 =========
    @classmethod
    async def get_album_list(cls, content_id: int, bot_name: str) -> List[Dict[str, Any]]:
        """
        查询某个 album 下的所有成员文件（PostgreSQL 版）
        - 对应 PHP 的 get_album_list()
        - 使用 asyncpg，占位符 $1/$2
        - 若 m.file_id 为空且从 file_extension 匹配到 ext_file_id，则回写/新增到 sora_media.file_id
        - 返回值：list[dict]
        依赖：
          - album_items(content_id, member_content_id, file_unique_id, file_type, position, stage, ...)
          - sora_content(id, source_id, file_type, content, file_size, duration, ...)
          - sora_media(content_id, source_bot_name, file_id, thumb_file_id, UNIQUE(content_id, source_bot_name))
          - file_extension(file_unique_id, bot, file_id)
        """
        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            sql = """
                SELECT
                    c.member_content_id,           -- 用于回写 sora_media.content_id
                    s.source_id,
                    c.file_type,
                    s.content,
                    s.file_size,
                    s.duration,
                    m.source_bot_name,
                    m.thumb_file_id,
                    m.file_id,
                    fe.file_id AS ext_file_id,
                    c.preview 
                FROM album_items AS c
                LEFT JOIN sora_content AS s
                    ON c.member_content_id = s.id
                LEFT JOIN sora_media   AS m
                    ON c.member_content_id = m.content_id
                   AND m.source_bot_name   = $1
                LEFT JOIN file_extension AS fe
                    ON fe.file_unique_id = s.source_id
                   AND fe.bot            = $1
                WHERE c.content_id = $2
                ORDER BY c.file_type;
            """

            rows = await conn.fetch(sql, bot_name, content_id)

            dict_rows: List[Dict[str, Any]] = []
            to_upsert: List[Tuple[int, str, str]] = []  # (content_id, bot_name, file_id)

            for rec in rows or []:
                d = dict(rec)
                if d.get("file_id") is None and d.get("ext_file_id") is not None:
                    # 用 ext_file_id 回填到返回值
                    d["file_id"] = d["ext_file_id"]
                    # 收集需要写回 sora_media 的条目
                    if d.get("member_content_id") is not None:
                        to_upsert.append((
                            int(d["member_content_id"]),
                            bot_name,
                            str(d["ext_file_id"]),
                        ))
                dict_rows.append(d)

            if to_upsert:
                upsert_sql = """
                    INSERT INTO sora_media (content_id, source_bot_name, file_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (content_id, source_bot_name)
                    DO UPDATE SET file_id = EXCLUDED.file_id
                """
                async with conn.transaction():
                    await conn.executemany(upsert_sql, to_upsert)

            return dict_rows

        except Exception as e:
            print(f"⚠️ [PG] get_album_list 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)


# lz_pgsql.py

    @classmethod
    async def upsert_album_items_bulk2(cls, rows):
        """
        将 MySQL 的 album_items 批量 upsert 到 PG.album_items
        以 id 为主键对齐（MySQL / PG 使用同一套 id）
        """
        if not rows:
            return 0

        await cls.ensure_pool()

        sql = """
        INSERT INTO album_items (
            id,
            content_id,
            member_content_id,
            file_unique_id,
            file_type,
            "position",
            stage,
            created_at,
            updated_at,
            preview
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        )
        ON CONFLICT (id) DO UPDATE SET
            content_id        = EXCLUDED.content_id,
            member_content_id = EXCLUDED.member_content_id,
            file_unique_id    = EXCLUDED.file_unique_id,
            file_type         = EXCLUDED.file_type,
            "position"        = EXCLUDED."position",
            stage             = EXCLUDED.stage,
            updated_at        = EXCLUDED.updated_at,
            preview           = EXCLUDED.preview
        ;
        """

        # 从 MySQL 记录构出 payload；如果 MySQL 里有 created_at / updated_at 就带过去，没有就用 NOW()
        from datetime import datetime

        payload = []
        now = datetime.now()
        for r in rows:
            payload.append((
                int(r["id"]),
                int(r["content_id"]),
                int(r["member_content_id"]),
                r.get("file_unique_id"),
                r.get("file_type"),
                int(r.get("position", 0)),
                r.get("stage", "pending"),
                r.get("created_at") or now,
                r.get("updated_at") or now,
                r.get("preview") or "",
            ))

        async with cls._pool.acquire() as conn:
            await conn.executemany(sql, payload)

        return len(payload)


    @classmethod
    async def upsert_album_items_bulk(cls, rows):
        if not rows:
            return 0

        await cls.ensure_pool()

        sql = """
        INSERT INTO album_items (
            id,
            content_id,
            member_content_id,
            file_unique_id,
            file_type,
            "position",
            stage,
            created_at,
            updated_at,
            preview
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        )
        ON CONFLICT (id) DO UPDATE SET
            content_id        = EXCLUDED.content_id,
            member_content_id = EXCLUDED.member_content_id,
            file_unique_id    = EXCLUDED.file_unique_id,
            file_type         = EXCLUDED.file_type,
            "position"        = EXCLUDED."position",
            stage             = EXCLUDED.stage,
            updated_at        = EXCLUDED.updated_at,
            preview           = EXCLUDED.preview
        ;
        """

        from datetime import datetime

        def _to_int(v, default=0):
            if v is None:
                return default
            if isinstance(v, str):
                v = v.strip()
                if v == "":
                    return default
            try:
                return int(v)
            except Exception:
                return default

        payload = []
        now = datetime.now()

        for r in rows:
            # 重点：preview 不能用 ""，要给 int（0/1）
            preview_val = _to_int(r.get("preview"), default=0)

            payload.append((
                _to_int(r.get("id"), default=0),
                _to_int(r.get("content_id"), default=0),
                _to_int(r.get("member_content_id"), default=0),
                r.get("file_unique_id") or None,
                r.get("file_type") or None,
                _to_int(r.get("position"), default=0),
                (r.get("stage") or "pending"),
                r.get("created_at") or now,
                r.get("updated_at") or now,
                preview_val,
            ))

        async with cls._pool.acquire() as conn:
            await conn.executemany(sql, payload)

        return len(payload)



    @classmethod
    async def delete_album_items_except(cls, content_id: int, keep_member_ids: List[int]) -> int:
        """
        删除 PG 中该 content_id 下、但不在 keep_member_ids 的 album_items
        keep_member_ids 为空时，删除该 content_id 下所有记录
        返回：删除行数
        """
        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            if keep_member_ids:
                sql = """
                    DELETE FROM album_items
                    WHERE content_id = $1
                      AND member_content_id <> ALL($2::bigint[])
                """
                tag = await conn.execute(sql, content_id, keep_member_ids)
            else:
                sql = "DELETE FROM album_items WHERE content_id = $1"
                tag = await conn.execute(sql, content_id)

            try:
                return int(tag.split()[-1])  # e.g. 'DELETE 3' → 3
            except Exception:
                return 0
        finally:
            await cls.release(conn)



    # ========= Transaction 相关 =========
    @classmethod
    async def get_max_transaction_id_for_sender(cls, sender_id: int) -> int:
        """
        查出 PostgreSQL 中指定 sender_id 的最大 transaction_id。
        若没有任何记录，回传 0。
        """
        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            # 注意表名：
            # 如果你在 PG 里建的是：
            #   CREATE TABLE "transaction" (...)
            # 就需要双引号；如果是 create table transaction (...)，就把引号拿掉。
            sql = 'SELECT max(transaction_id) FROM "transaction" WHERE sender_id = $1'
            max_id = await conn.fetchval(sql, int(sender_id))
            return int(max_id) if max_id is not None else 0
        except Exception as e:
            print(f"⚠️ [PG] get_max_transaction_id_for_sender 出错: {e}", flush=True)
            return 0
        finally:
            await cls.release(conn)


    @classmethod
    async def upsert_transactions_bulk(cls, rows: list[dict]) -> int:
        """
        将 MySQL 的 transaction 记录批量 upsert 到 PostgreSQL 的 transaction 表。
        规则：
          - 以 transaction_id 为主键
          - 冲突时更新除主键外的所有字段
        返回：受影响行数（近似：= 输入 rows 数量）
        """
        if not rows:
            return 0

        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            payload = []
            for r in rows:
                payload.append(
                    (
                        int(r["transaction_id"]),
                        int(r["sender_id"]),
                        int(r.get("sender_fee", 0)),
                        int(r.get("receiver_id", 0)),
                        int(r.get("receiver_fee", 0)),
                        r.get("transaction_type"),
                        r.get("transaction_description"),
                        int(r.get("transaction_timestamp", 0)),
                        r.get("memo"),
                    )
                )

            sql = """
                INSERT INTO transaction (
                    transaction_id,
                    sender_id,
                    sender_fee,
                    receiver_id,
                    receiver_fee,
                    transaction_type,
                    transaction_description,
                    transaction_timestamp,
                    memo
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9
                )
                ON CONFLICT (transaction_id)
                DO UPDATE SET
                    sender_id               = EXCLUDED.sender_id,
                    sender_fee              = EXCLUDED.sender_fee,
                    receiver_id             = EXCLUDED.receiver_id,
                    receiver_fee            = EXCLUDED.receiver_fee,
                    transaction_type        = EXCLUDED.transaction_type,
                    transaction_description = EXCLUDED.transaction_description,
                    transaction_timestamp   = EXCLUDED.transaction_timestamp,
                    memo                    = EXCLUDED.memo
            """

            async with conn.transaction():
                await conn.executemany(sql, payload)

            return len(payload)
        except Exception as e:
            print(f"⚠️ upsert_transactions_bulk 出错: {e}", flush=True)
            return 0
        finally:
            await cls.release(conn)



    @classmethod
    async def search_history_redeem(cls, user_id: int) -> list[dict]:
        """
        查询某个用户的所有兑换历史（PostgreSQL 版）

        对应 MySQL 版:
            SELECT sc.id, sc.source_id, sc.file_type, sc.content
            FROM transaction t
            LEFT JOIN sora_content sc ON t.transaction_description = sc.source_id
            WHERE t.sender_id = ? AND t.transaction_type='confirm_buy'
              AND sc.valid_state != 4
            ORDER BY t.transaction_id DESC
        """

        cache_key = f"pg:history:redeem:{user_id}"
        if cls.cache:
            cached = await cls.cache.get(cache_key)
            if cached:
                print(f"🔹 PG MemoryCache hit for {cache_key}")
                return cached

        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            sql = """
                SELECT
                    sc.id,
                    sc.source_id,
                    sc.file_type,
                    sc.content
                FROM "transaction" t
                LEFT JOIN sora_content sc
                    ON t.transaction_description = sc.source_id
                WHERE t.sender_id = $1
                  AND t.transaction_type = 'confirm_buy'
                  AND sc.valid_state != 4
                ORDER BY t.transaction_id DESC
            """
            rows = await conn.fetch(sql, int(user_id))
            result = [dict(r) for r in rows] if rows else []

            if cls.cache:
                cls.cache.set(cache_key, result, ttl=300)
                print(f"🔹 PG MemoryCache set for {cache_key}, {len(result)} items")

            return result
        except Exception as e:
            print(f"⚠️ [PG] search_history_redeem 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)



    @classmethod
    async def search_history_upload(cls, user_id: int) -> List[Dict[str, Any]]:
        """
        查询某个用户的所有上传历史（PostgreSQL 版本）

        对应 MySQL 版：
            SELECT sc.id, sc.source_id, sc.file_type, sc.content
            FROM product p
            LEFT JOIN sora_content sc ON p.content_id = sc.id
            WHERE p.owner_user_id = ? AND sc.valid_state != 4
            ORDER BY sc.id DESC
        """

        cache_key = f"pg:history:upload:{user_id}"

        # 内存缓存（短期，减轻 DB 压力）
        if cls.cache:
            cached = await cls.cache.get(cache_key)
            if cached:
                
                print(f"🔹 PG MemoryCache hit for {cache_key}")
                cls.cache.set(cache_key, cached, ttl=300)
                return cached

        await cls.ensure_pool()
        conn = await cls.acquire()
        try:
            sql = """
                SELECT
                    sc.id,
                    sc.source_id,
                    sc.file_type,
                    sc.content
                FROM product p
                LEFT JOIN sora_content sc
                    ON p.content_id = sc.id
                WHERE p.owner_user_id = $1
                  AND sc.valid_state != 4
                ORDER BY sc.id DESC
            """
            rows = await conn.fetch(sql, int(user_id))
            result = [dict(r) for r in rows] if rows else []

            if cls.cache:
                cls.cache.set(cache_key, result, ttl=300)
                print(f"🔹 PG MemoryCache set for {cache_key}, {len(result)} items")

            return result
        except Exception as e:
            print(f"⚠️ [PG] search_history_upload 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)


    @classmethod
    async def upsert_product_bulk_from_mysql(cls, rows: List[Dict[str, Any]]) -> int:
        """
        将 MySQL 的 product 记录批量 upsert 到 PostgreSQL 的 public.product 表。

        规则：
          - 以 content_id 为冲突键 (UNIQUE / PK)
          - 冲突时更新：price, file_type, owner_user_id, purchase_condition, guild_id
          - created_at 使用新插入时的 NOW()，更新时仅改 updated_at

        返回：受影响的行数（近似等于 rows 长度）
        """
        if not rows:
            return 0

        await cls.ensure_pool()

        # 准备批量参数
        payload: List[Tuple] = []
        for r in rows:
            content_id = int(r["content_id"])
            try:
                price = int(r.get("price") or 0)
            except Exception:
                price = 0
            id = r.get("id")
            file_type = r.get("file_type")
            owner_user_id = r.get("owner_user_id")
            owner_user_id = int(owner_user_id) if owner_user_id is not None else None
            purchase_condition = r.get("purchase_condition")
            guild_id = r.get("guild_id")

            payload.append(
                (
                    id,
                    content_id,
                    price,
                    file_type,
                    owner_user_id,
                    purchase_condition,
                    guild_id,
                )
            )

        sql = """
            INSERT INTO product (
                id,
                content_id,
                price,
                file_type,
                owner_user_id,
                purchase_condition,
                guild_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, NOW(), NOW()
            )
            ON CONFLICT (content_id) DO UPDATE SET
                content_id         = EXCLUDED.content_id,
                price              = EXCLUDED.price,
                file_type          = EXCLUDED.file_type,
                owner_user_id      = EXCLUDED.owner_user_id,
                purchase_condition = EXCLUDED.purchase_condition,
                guild_id           = EXCLUDED.guild_id,
                updated_at         = NOW()
        """

        # 和 upsert_album_items_bulk 风格保持一致
        async with cls._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, payload)

        return len(payload)


    @classmethod
    async def get_file_id_by_file_unique_id(cls, unique_ids: list[str]) -> list[str]:
        """
        根据多个 file_unique_id 取得对应的 file_id（按输入顺序对齐）

        说明：
        - 这是从 lz_db.py 移植到 PGPool 的实现
        - PostgreSQL only（file_extension 表）
        - 使用 PGPool 统一 cache / pool / acquire / release 规范
        """

        await cls.ensure_pool()

        if not unique_ids:
            return []

        # ---------- sanitize：去空、去重、保序 ----------
        conn = await cls.acquire()
        try:
            rows = await conn.fetch(
                '''
                SELECT file_id,file_unique_id
                FROM file_extension
                WHERE file_unique_id = ANY($1::text[])
                AND bot = $2
                ''',
                unique_ids, lz_var.bot_username
            )

            f_row = {}
            for r in rows or []:
                row = dict(r)
                f_row[row["file_unique_id"]] = row
            return f_row

        finally:
            await cls.release(conn)

    # ====== User Collection =====
    @classmethod
    async def get_user_collection_by_id(cls, collection_id: int) -> Optional[Dict[str, Any]]:
        """
        从 PostgreSQL 读取 user_collection 单条记录（按 id）。
        - 迁移自 lz_mysql.py 的同名函数
        - 返回 dict 或 None
        """
        await cls.ensure_pool()

        conn = None
        try:
            conn = await cls.acquire()
            row = await conn.fetchrow(
                """
                SELECT *
                FROM user_collection
                WHERE id = $1
                """,
                int(collection_id),
            )
            return dict(row) if row else None

        except Exception as e:
            print(f"⚠️ [PG] get_user_collection_by_id 出错: {e}", flush=True)
            return None

        finally:
            await cls.release(conn)

    @classmethod
    async def delete_user_collection(
        cls,
        collection_id: int,
        user_id: int,
    ) -> Dict[str, Any]:
        await cls.ensure_pool()

        conn = None
        try:
            conn = await cls.acquire()
            row = await conn.fetchrow(
                """
                SELECT id, user_id
                FROM user_collection
                WHERE id = $1
                LIMIT 1
                """,
                int(collection_id),
            )
            if not row:
                return {"ok": "", "status": "not_found", "id": collection_id}

            owner_user_id = int(row["user_id"])
            if owner_user_id != int(user_id):
                return {"ok": "", "status": "forbidden", "id": collection_id}

            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM user_collection_file WHERE collection_id = $1",
                    int(collection_id),
                )
                await conn.execute(
                    "DELETE FROM user_collection_favorite WHERE user_collection_id = $1",
                    int(collection_id),
                )
                result = await conn.execute(
                    "DELETE FROM user_collection WHERE id = $1 AND user_id = $2",
                    int(collection_id),
                    int(user_id),
                )

            try:
                deleted_rows = int(str(result).split()[-1])
            except Exception:
                deleted_rows = 0

            return {
                "ok": "1" if deleted_rows else "",
                "status": "deleted" if deleted_rows else "not_found",
                "id": collection_id,
            }
        except Exception as e:
            return {"ok": "", "status": "error", "error": str(e), "id": collection_id}
        finally:
            await cls.release(conn)
  
    @classmethod
    async def list_user_collections(
        cls, user_id: int, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        cache_key = f"user:clt:{user_id}:{limit}:{offset}"
        
        if cls.cache:
            cached = await cls.cache.get(cache_key)
            if cached:
                print(f"🔹 PG cache hit for {cache_key}")
                cls.cache.set(cache_key, cached, ttl=300)
                return cached

        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(
                """
                SELECT id, title, description, is_public, created_at, updated_at, sort_order, cover_type, cover_file_unique_id
                FROM user_collection
                WHERE user_id = $1
                ORDER BY id DESC
                LIMIT $2 OFFSET $3
                """,
                int(user_id), int(limit), int(offset),
            )
            result = [dict(r) for r in rows] if rows else []
            if cls.cache:
                cls.cache.set(cache_key, result, ttl=300)
            return result
        except Exception as e:
            print(f"⚠️ [PG] list_user_collections 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)

    @classmethod
    async def list_user_favorite_collections(
        cls, user_id: int, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        列出用户收藏的资源橱窗（基于 user_collection_favorite.user_collection_id 关联）。
        按收藏记录 id 倒序（最新收藏在前）。
        """
        cache_key = f"fav:clt:{user_id}:{limit}:{offset}"
        if cls.cache:
            cached = await cls.cache.get(cache_key)
            if cached:
                # print(f"🔹 PG MemoryCache hit for {cache_key}")
                return cached

        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(
                """
                SELECT uc.id, uc.title, uc.description, uc.is_public, uc.created_at
                FROM user_collection_favorite AS ucf
                JOIN user_collection AS uc
                  ON uc.id = ucf.user_collection_id
                WHERE ucf.user_id = $1
                ORDER BY ucf.id DESC, uc.id DESC
                LIMIT $2 OFFSET $3
                """,
                int(user_id), int(limit), int(offset),
            )
            result = [dict(r) for r in rows] if rows else []
            if cls.cache:
                cls.cache.set(cache_key, result, ttl=300)
            return result
        except Exception as e:
            print(f"⚠️ [PG] list_user_favorite_collections 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)

    @classmethod
    async def get_collection_detail_with_cover(
        cls, collection_id: int, bot_name: str = "luzaitestbot"
    ) -> Optional[Dict[str, Any]]:
        """
        返回 user_collection 全字段 + cover 对应的 file_id（若有）。
        """
        conn = None
        try:
            conn = await cls.acquire()
            row = await conn.fetchrow(
                """
                SELECT uc.*, fe.file_id AS cover_file_id
                FROM user_collection uc
                LEFT JOIN file_extension fe
                  ON uc.cover_file_unique_id = fe.file_unique_id
                 AND fe.bot = $1
                WHERE uc.id = $2
                LIMIT 1
                """,
                str(bot_name), int(collection_id),
            )
            return dict(row) if row else None
        except Exception as e:
            print(f"⚠️ [PG] get_collection_detail_with_cover 出错: {e}", flush=True)
            return None
        finally:
            await cls.release(conn)

    @classmethod
    async def is_collection_favorited(cls, user_id: int, collection_id: int) -> bool:
        conn = None
        try:
            conn = await cls.acquire()
            v = await conn.fetchval(
                """
                SELECT 1
                FROM user_collection_favorite
                WHERE user_id = $1 AND user_collection_id = $2
                LIMIT 1
                """,
                int(user_id), int(collection_id),
            )
            return bool(v)
        except Exception as e:
            print(f"⚠️ [PG] is_collection_favorited 出错: {e}", flush=True)
            return False
        finally:
            await cls.release(conn)

    @classmethod
    async def get_user_collections_count_and_first(cls, user_id: int) -> tuple[int, int | None]:
        """
        返回 (资源橱窗数量, 第一条资源橱窗ID或None)。
        只查一次：LIMIT 2 即可区分 0/1/多，并顺便拿到第一条ID。
        """
        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(
                """
                SELECT id
                FROM user_collection
                WHERE user_id = $1
                ORDER BY id ASC
                LIMIT 2
                """,
                int(user_id),
            )
            cnt = len(rows) if rows else 0
            first_id = int(rows[0]["id"]) if cnt >= 1 else None
            return cnt, first_id
        except Exception as e:
            print(f"⚠️ [PG] get_user_collections_count_and_first 出错: {e}", flush=True)
            return 0, None
        finally:
            await cls.release(conn)

    @classmethod
    async def get_clt_files_by_clt_id(cls, collection_id: int) -> List[Dict[str, Any]]:
        """
        查询某个资源橱窗的所有文件
        """
        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(
                """
                SELECT sc.id, sc.source_id, sc.file_type, sc.content
                FROM user_collection_file ucf
                LEFT JOIN sora_content sc ON ucf.content_id = sc.id
                WHERE ucf.collection_id = $1
                  AND sc.valid_state != 4
                ORDER BY ucf.sort ASC
                """,
                int(collection_id),
            )
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ [PG] get_clt_files_by_clt_id 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)

    @classmethod
    async def get_clt_by_content_id(cls, content_id: int) -> List[Dict[str, Any]]:
        """
        查询 user_collection_file：给定 content_id 的所有记录
        """
        conn = None
        try:
            conn = await cls.acquire()
            rows = await conn.fetch(
                """
                SELECT *
                FROM user_collection_file
                WHERE content_id = $1
                """,
                int(content_id),
            )
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ [PG] get_clt_by_content_id 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn)


    @classmethod
    async def list_collection_files_file_id(
        cls,
        collection_id: int,
        *,
        limit: int,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        对齐 lz_mysql.py 同名函数：
        - 返回 (items, has_next)
        - items: list[dict]，包含 sc.content, sc.id, sc.file_type
        - has_next: 外层传入 limit = PAGE_SIZE + 1 时，用于判断是否还有下一页
        """
        await cls.ensure_pool()
        conn = None
        try:
            conn = await cls.acquire()
            sql = """
                SELECT sc.content, sc.id, sc.file_type, p.content as product_content
                FROM user_collection_file ucf
                LEFT JOIN sora_content sc ON sc.id = ucf.content_id 
                LEFT JOIN product p ON p.content_id = sc.id
                WHERE ucf.collection_id = $1
                  AND sc.valid_state != 4
                ORDER BY ucf.sort ASC
                LIMIT $2 OFFSET $3
            """
            rows = await conn.fetch(sql, int(collection_id), int(limit), int(offset))
            items = [dict(r) for r in rows] if rows else []
            has_next = (len(items) > 0 and len(items) == int(limit))
            return items, has_next

        except Exception as e:
            print(f"⚠️ [PG] list_collection_files_file_id 出错: {e}", flush=True)
            return [], False

        finally:
            await cls.release(conn)


    # ========= Generic Sync Helpers =========
    @staticmethod
    def _safe_ident_pg(name: str) -> str:
        """Very small identifier sanitizer to avoid SQL injection via table/column names."""
        if not isinstance(name, str):
            raise ValueError("identifier must be str")
        name = name.strip()
        if not name:
            raise ValueError("identifier is empty")

        import re
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            raise ValueError(f"invalid identifier: {name}")
        # always double-quote to preserve case/reserved words safety
        return f'"{name}"'

    
    @classmethod
    async def upsert_records_generic(
        cls,
        table: str,
        pk_field,
        rows: list[dict],
        *,
        chunk_size: int = 1000,
    ) -> int:
        """
        通用 UPSERT：把 MySQL 拉出来的 rows 批量写入 PostgreSQL（新增或取代）。
        支持单主键 / 复合主键。

        - table: 目标表名（与 MySQL 同名）
        - pk_field:
            - 单主键: "id"
            - 复合主键: ["collection_id", "content_id"]（也兼容 "a,b" 传法）
        - rows: list[dict]，每个 dict 的 key 必须是列名
        - chunk_size: executemany 分批大小（避免 payload 太大）

        返回：写入的行数（近似 = 输入 rows 数量）。
        """
        if not rows:
            return 0

        await cls.ensure_pool()

        # ---------- normalize pk_fields ----------
        pk_fields: list[str]
        if isinstance(pk_field, (list, tuple)):
            pk_fields = [str(x).strip() for x in pk_field if str(x).strip()]
        else:
            pk_s = (str(pk_field) if pk_field is not None else "").strip()
            if "," in pk_s:
                pk_fields = [x.strip() for x in pk_s.split(",") if x.strip()]
            else:
                pk_fields = [pk_s] if pk_s else []

        if not pk_fields:
            raise ValueError("pk_field is empty")

        # ---------- identifiers ----------
        t_sql = cls._safe_ident_pg(table)
        pk_sqls = [cls._safe_ident_pg(f) for f in pk_fields]
        conflict_sql = ", ".join(pk_sqls)

        # ---------- columns ----------
        # 以第一笔为准；后续若缺字段，用 None 补齐
        columns = list(rows[0].keys())
        for f in pk_fields:
            if f not in columns:
                raise ValueError(f"pk_field '{f}' not in row keys")

        # 对齐 PG 实际表结构，忽略 MySQL 独有列（如 stage）
        pg_columns = await cls._get_table_columns(table)
        if not pg_columns:
            raise ValueError(f"table '{table}' not found or has no columns in PostgreSQL")

        original_columns = columns
        columns = [c for c in original_columns if c in pg_columns]
        dropped_columns = [c for c in original_columns if c not in pg_columns]

        if dropped_columns:
            print(
                f"[PG upsert_records_generic] table={table} 忽略 PG 不存在字段: {', '.join(dropped_columns)}",
                flush=True,
            )

        for f in pk_fields:
            if f not in columns:
                raise ValueError(
                    f"pk_field '{f}' not found in PostgreSQL table '{table}' columns"
                )

        if not columns:
            raise ValueError(f"no valid columns to upsert for table '{table}'")

        col_sql = ", ".join(cls._safe_ident_pg(c) for c in columns)
        values_sql = ", ".join(f"${i}" for i in range(1, len(columns) + 1))

        pk_set = set(pk_fields)
        update_cols = [c for c in columns if c not in pk_set]

        if update_cols:
            update_sql = ", ".join(
                f"{cls._safe_ident_pg(c)} = EXCLUDED.{cls._safe_ident_pg(c)}" for c in update_cols
            )
            sql = (
                f"INSERT INTO {t_sql} ({col_sql}) VALUES ({values_sql}) "
                f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}"
            )
        else:
            # 只有主键列：冲突就什么都不做
            sql = (
                f"INSERT INTO {t_sql} ({col_sql}) VALUES ({values_sql}) "
                f"ON CONFLICT ({conflict_sql}) DO NOTHING"
            )

        def build_payload(batch: list[dict]) -> list[tuple]:
            return [tuple(r.get(c) for c in columns) for r in batch]

        total = 0
        async with cls._pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(rows), int(chunk_size)):
                    batch = rows[i : i + int(chunk_size)]
                    await conn.executemany(sql, build_payload(batch))
                    total += len(batch)
        return total
        

    @classmethod
    async def delete_where(
        cls,
        table: str,
        conditions: dict,
    ) -> int:
        """
        通用删除函数：

            DELETE FROM table WHERE col1 = val1 AND col2 = val2 ...

        参数：
            table: 表名
            conditions: dict 形式的条件，例如：
                {
                    "user_id": 123,
                    "collection_id": 456,
                }

        返回：
            删除行数（int）

        注意：
            - conditions 不能为空
            - 所有条件使用 AND 连接
        """

        table = (table or "").strip()
        if not table:
            raise ValueError("table 不能为空")

        if not conditions or not isinstance(conditions, dict):
            raise ValueError("conditions 不能为空")

        await cls.ensure_pool()

        t_sql = cls._safe_ident_pg(table)

        where_clauses = []
        values = []
        param_index = 1

        for key, val in conditions.items():
            key = (key or "").strip()
            if not key:
                continue

            col_sql = cls._safe_ident_pg(key)
            where_clauses.append(f"{col_sql} = ${param_index}")
            values.append(val)
            param_index += 1

        if not where_clauses:
            raise ValueError("conditions 无有效字段")

        where_sql = " AND ".join(where_clauses)

        sql = f"""
            DELETE FROM {t_sql}
            WHERE {where_sql}
        """

        conn = await cls.acquire()
        try:
            tag = await conn.execute(sql, *values)

            # 返回格式类似 "DELETE 5"
            try:
                return int(tag.split()[-1])
            except Exception:
                return 0

        finally:
            await cls.release(conn)



# ========= Generic Sync Helpers =========

    @classmethod
    async def _get_table_columns(cls, table: str) -> set:
        """获取当前 schema 下某表的列名集合（带进程内缓存）。"""
        table = (table or "").strip()
        if not table:
            return set()

        cached = cls._table_columns_cache.get(table)
        if cached is not None:
            return cached

        await cls.ensure_pool()
        async with cls._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT column_name
                FROM information_schema.columns
                WHERE table_name = $1
                  AND table_schema = ANY(current_schemas(false))
                """,
                table,
            )

        cols = {str(r["column_name"]) for r in rows}
        cls._table_columns_cache[table] = cols
        return cols

    
    

''''''