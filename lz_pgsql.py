# lz_pgsql.py  —— @classmethod 风格，接口与 lz_mysql.py 一致
import os
import asyncio
import asyncpg
from typing import Optional, Dict, Any, List, Tuple
import jieba

from lz_config import POSTGRES_DSN
from lz_memory_cache import MemoryCache
import lz_var
from opencc import OpenCC

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
                            statement_cache_size=1024,
                            # server_settings=None,  # ✅ 先置空
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
                f"[PG upsert_product_thumb] thumb_file_unique_id={thumb_file_unique_id} thumb_file_id={thumb_file_id} "
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
                file_password = mysql_row.get("file_password", "").strip()

                # content_seg：同义词替换 + jieba 分词（与检索一致）
                norm = cls.replace_synonym(content)
                content_seg = " ".join(jieba.cut(norm)) if norm else ""

                if tag:
                    #将字串中的#字号全部移除
                    tag_remove_slash = tag.replace("#", "")
                    content_seg = content_seg + " " + tag_remove_slash


                tw2s = OpenCC('tw2s')
                content_seg = tw2s.convert(content_seg)




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

                affected2 = 0
                if any(v is not None for v in (fee, product_type, p_owner_user_id, purchase_condition, guild_id)):
                    # price 为 NOT NULL，确保是整数；无则 0
                    try:
                        price_int = int(fee) if fee is not None else 0
                    except Exception:
                        price_int = 0

                    sql_product = """
                        INSERT INTO product (
                            id,content_id, price, file_type, owner_user_id, purchase_condition, guild_id,
                            created_at, updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
                        ON CONFLICT (content_id)
                        DO UPDATE SET
                            content_id         = EXCLUDED.content_id,
                            price              = EXCLUDED.price,
                            file_type          = EXCLUDED.file_type,
                            owner_user_id      = EXCLUDED.owner_user_id,
                            purchase_condition = EXCLUDED.purchase_condition,
                            guild_id           = EXCLUDED.guild_id,
                            updated_at         = NOW()
                    """
                    tag_ret2 = await conn.execute(
                        sql_product,
                        product_id, content_id, price_int, product_type, p_owner_user_id, purchase_condition, guild_id
                    )
                    try:
                        affected2 = int(tag_ret2.split()[-1])
                    except Exception:
                        affected2 = 0

                return affected1 + affected2
        finally:
            await cls.release(conn)



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
    async def upsert_album_items_bulk(cls, rows):
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
            cached = cls.cache.get(cache_key)
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
            cached = cls.cache.get(cache_key)
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

