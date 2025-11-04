# lz_db.py
import asyncpg
import asyncio
import os
from lz_config import POSTGRES_DSN
from lz_memory_cache import MemoryCache
from datetime import datetime
import lz_var
import jieba

DEFAULT_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
DEFAULT_MAX = int(os.getenv("POSTGRES_POOL_MAX", "5"))
ACQUIRE_TIMEOUT = float(os.getenv("POSTGRES_ACQUIRE_TIMEOUT", "10"))
COMMAND_TIMEOUT = float(os.getenv("POSTGRES_COMMAND_TIMEOUT", "60"))
CONNECT_TIMEOUT = float(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"))  # 新增

SYNONYM = {
    "滑鼠": "鼠标",
    "萤幕": "显示器",
    "笔电": "笔记本",
}

class DB:
    def __init__(self):
        self.dsn = POSTGRES_DSN
        self.pool: asyncpg.Pool | None = None
        self.cache = MemoryCache()


    async def connect(self):
        if self.pool is not None:
            return
        # （可选）重试 2-3 次，避免临时网络波动
        retries = int(os.getenv("POSTGRES_CONNECT_RETRIES", "2"))
        last_exc = None
        for attempt in range(retries + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=DEFAULT_MIN,
                    max_size=DEFAULT_MAX,
                    max_inactive_connection_lifetime=300,
                    command_timeout=COMMAND_TIMEOUT,
                    timeout=CONNECT_TIMEOUT,            # 新增：连接级超时
                    statement_cache_size=1024,          # 稳态优化
                )
                # 预热：设置时区/应用名
                async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
                    await conn.execute("SET SESSION TIME ZONE 'UTC'")
                    await conn.execute("SET application_name = 'lz_app'")
                print("✅ PostgreSQL 连接池初始化完成")
                return
            except Exception as e:
                last_exc = e
                if attempt < retries:
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    raise

    async def connect_bk(self):
        """幂等连接 + 小连接池，避免 TooManyConnections。"""
        if self.pool is not None:
            return
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=DEFAULT_MIN,
            max_size=DEFAULT_MAX,
            max_inactive_connection_lifetime=300,
            command_timeout=COMMAND_TIMEOUT,
        )
        # 可选：预热一次连接，设置时区/应用名
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            await conn.execute("SET SESSION TIME ZONE 'UTC'")
            await conn.execute("SET application_name = 'lz_app'")
        print("✅ PostgreSQL 连接池初始化完成")

    async def disconnect(self):
        """优雅断线，给主程序 shutdown/finally 调用。"""
        pool, self.pool = self.pool, None
        if pool is not None:
            await pool.close()

    def _normalize_query(self, keyword_str: str) -> str:
        return " ".join(keyword_str.strip().lower().split())

    async def _ensure_pool(self):
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool is not connected. Call db.connect() first.")



    async def search_keyword_page_highlighted(self, keyword_str: str, last_id: int = 0, limit: int = 10):
        query = self._normalize_query(keyword_str)
        cache_key = f"highlighted:{query}:{last_id}:{limit}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            rows = await conn.fetch(
                '''
                SELECT id, source_id, file_type,
                       ts_headline('simple', content, plainto_tsquery('simple', $1)) AS highlighted_content
                FROM sora_content
                WHERE content_seg_tsv @@ plainto_tsquery('simple', $1)
                  AND id > $2
                ORDER BY id ASC
                LIMIT $3
                ''',
                query, last_id, limit
            )
            result = [dict(r) for r in rows]
            self.cache.set(cache_key, result, ttl=60)  # 缓存 60 秒
            return result


    def replace_synonym(self,text):
        for k, v in SYNONYM.items():
            text = text.replace(k, v)
        return text

    def _escape_ts_lexeme(self,s: str) -> str:
        # 简单转义，避免 to_tsquery 特殊字符影响；必要时再扩充
        return s.replace("'", "''").replace("&", " ").replace("|", " ").replace("!", " ").replace(":", " ").strip()

    def _build_tsqueries_from_tokens(self,tokens: list[str]) -> tuple[str, str]:
        toks = [self._escape_ts_lexeme(t) for t in tokens if t.strip()]
        if not toks:
            return "", ""
        phrase = " <-> ".join(toks)  # 相邻
        all_and = " & ".join(toks)   # 兜底 AND
        return phrase, all_and

    async def search_keyword_page_plain(self, keyword_str: str, last_id: int = 0, limit: int = 100):
        query = self._normalize_query(keyword_str)
        cache_key = f"searchkey:{query}:{last_id}:{limit}"
        
        cached = self.cache.get(cache_key)
        if cached:
            return cached
 
 
        # 归一 + 分词（与建索引时保持一致）
        q_norm = self.replace_synonym(keyword_str)
        tokens = list(jieba.cut(q_norm))
        phrase_q, and_q = self._build_tsqueries_from_tokens(tokens)
        if not and_q:
            return []

        limit = max(1, min(300, int(limit)))

        where_parts = []
        params = []

        # 两种匹配：相邻 或 AND
        cond = []
        if phrase_q:
            cond.append("content_seg_tsv @@ to_tsquery('simple', $1)")
            params.append(phrase_q)
        cond.append(f"content_seg_tsv @@ to_tsquery('simple', ${len(params)+1})")
        params.append(and_q)
        where_parts.append("(" + " OR ".join(cond) + ")")

        if last_id > 0:
            where_parts.append(f"id < ${len(params)+1}")
            params.append(last_id)

        sql = f"""
            SELECT
                id, source_id, file_type, content,
                GREATEST(
                    COALESCE(ts_rank_cd(content_seg_tsv, to_tsquery('simple', $1)), 0) * 1.5,
                    ts_rank_cd(content_seg_tsv, to_tsquery('simple', $2))
                ) AS rank
            FROM sora_content
            WHERE {' AND '.join(where_parts)}
            ORDER BY rank DESC, id DESC
            LIMIT ${len(params)+1}
        """
        params.append(limit)

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            rows = await conn.fetch(sql, *params)
        
        result = [dict(r) for r in rows]
        self.cache.set(cache_key, result, ttl=300)  # 缓存 60 秒
        return result




    async def upsert_file_extension(self,
        file_type: str,
        file_unique_id: str,
        file_id: str,
        bot: str,
        user_id: str = None
    ):
        now = datetime.utcnow()

        sql = """
            INSERT INTO file_extension (
                file_type, file_unique_id, file_id, bot, user_id, create_time
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (file_unique_id, bot)
            DO UPDATE SET
                file_id = EXCLUDED.file_id,
                create_time = EXCLUDED.create_time
            
        """

        # print(f"Executing SQL:\n{sql.strip()}")
        # print(f"With params: {file_type}, {file_unique_id}, {file_id}, {bot}, {user_id}, {now}")
        await self._ensure_pool()
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            result = await conn.fetchrow(sql, file_type, file_unique_id, file_id, bot, user_id, now)



        # print("DB result:", dict(result) if result else "No rows returned")


        sql2 =  """
                SELECT id FROM sora_content WHERE source_id = $1 OR thumb_file_unique_id = $2
                """

        # print(f"Executing SQL:\n{sql.strip()}")
        # print(f"With params: {file_type}, {file_unique_id}, {file_id}, {bot}, {user_id}, {now}")

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            row = await conn.fetchrow(sql2, file_unique_id, file_unique_id)
            if row:
                content_id = row["id"]
                cache_key = f"sora_content_id:{content_id}"
                # 这里原本是 db.cache.delete(...)，应为 self.cache.delete(...)
                self.cache.delete(cache_key)

    #要和 ananbot_utils.py 作整合
    async def search_sora_content_by_id(self, content_id: int):
        cache_key = f"sora_content_id:{content_id}"
        # print(f"Searching sora_content by id {content_id} with cache key {cache_key}")
        cached = self.cache.get(cache_key)
        if cached:
            print(f"\r\n\r\n173:Cache hit for {cache_key}")
            return cached
        
        # print(f"\r\n\r\nCache miss for {cache_key}, querying database...")
        await self._ensure_pool()

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            row = await conn.fetchrow(
                '''
                SELECT s.id, s.source_id, s.file_type, s.content, s.file_size, s.duration, s.tag,
                    s.thumb_file_unique_id,
                    m.file_id AS m_file_id, m.thumb_file_id AS m_thumb_file_id,
                    p.price as fee, p.file_type as product_type, p.owner_user_id, p.purchase_condition
                FROM sora_content s
                LEFT JOIN sora_media m ON s.id = m.content_id AND m.source_bot_name = $2
                LEFT JOIN product p ON s.id = p.content_id
                WHERE s.id = $1
                ''',
                content_id, lz_var.bot_username
            )
            
            if not row:
                # print(f"\r\n\r\nNo sora_content found for id {content_id}"  )
                return None  # 未找到内容
            # print(f"\r\n\r\nDatabase returned row for content_id {content_id}: {row}")

            row = dict(row)

            # 先用 m 表资料
            file_id = row.get("m_file_id")
            thumb_file_id = row.get("m_thumb_file_id")

            # 若缺其中任一，则尝试用 file_extension 查补
            if not file_id or not thumb_file_id:

                # print(f"\r\n\r\n>>> Fetching file extensions for {row.get('source_id')} and {row.get('thumb_file_unique_id')}")

                extension_rows = await conn.fetch(
                    '''
                    SELECT file_unique_id, file_id
                    FROM file_extension
                    WHERE file_unique_id = ANY($1::text[])
                    AND bot = $2
                    ''',
                    [row.get("source_id"), row.get("thumb_file_unique_id")],
                    lz_var.bot_username
                )
                ext_map = {r["file_unique_id"]: r["file_id"] for r in extension_rows}

                # print(f"Fetched {len(extension_rows)} file extensions for content_id {content_id}")
                # 如果 file_id 还没值，尝试用 source_id 和 thumb_file_unique_id 查找
                # print(f"{ext_map}")
                # print(f"{row}")
               

                if not file_id and row.get("source_id") in ext_map:
                    file_id = ext_map[row["source_id"]]

                if not thumb_file_id and row.get("thumb_file_unique_id") in ext_map:
                    thumb_file_id = ext_map[row["thumb_file_unique_id"]]

            # 如果两个都有值，就 upsert 写入 sora_media
            if file_id and thumb_file_id:
                # print(f"\r\n\r\n>>>Upserting sora_media for content_id {content_id} with file_id {file_id} and thumb_file_id {thumb_file_id}")
                await conn.execute(
                    '''
                    INSERT INTO sora_media (content_id, file_id, thumb_file_id, source_bot_name)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (content_id, source_bot_name)
                    DO UPDATE SET
                        file_id = EXCLUDED.file_id,
                        thumb_file_id = EXCLUDED.thumb_file_id
                    ''',
                    content_id, file_id, thumb_file_id, lz_var.bot_username
                )

            result = {
                "id": row["id"],
                "source_id": row["source_id"],
                "file_type": row["file_type"],
                "content": row["content"],
                "file_size": row["file_size"],
                "duration": row["duration"],
                "tag": row["tag"],
                "file_id": file_id,
                "thumb_file_id": thumb_file_id,
                "thumb_file_unique_id": row.get("thumb_file_unique_id"),
                "fee": row.get("fee"),
                "product_type": row.get("product_type"),
                "owner_user_id": row.get("owner_user_id"),
                "purchase_condition": row.get("purchase_condition")
            }

            # print(f"\r\n\r\nFinal result for content_id {content_id}: {result}")

            # self.cache.set(cache_key, result, ttl=3600)
            self.cache.set(cache_key, result, ttl=3600)
            # print(f"Cache set for {cache_key}")
            return result

            # 返回 asyncpg Record 或 None

    async def get_next_content_id(self, current_id: int, offset: int) -> int | None:
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            if offset > 0:
                row = await conn.fetchrow(
                    """
                    SELECT id FROM sora_content
                    WHERE id > $1
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    current_id
                )
            else:
                row = await conn.fetchrow(
                    """
                    SELECT id FROM sora_content
                    WHERE id < $1
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    current_id
                )
            if row:
                return row["id"]
            return None

    async def get_file_id_by_file_unique_id(self, unique_ids: list[str]) -> list[str]:
        """
        根据多个 file_unique_id 取得对应的 file_id 列表。
        """
        if not unique_ids:
            return []

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            rows = await conn.fetch(
                '''
                SELECT file_id
                FROM file_extension
                WHERE file_unique_id = ANY($1::text[])
                AND bot = $2
                ''',
                unique_ids, lz_var.bot_username
            )
            # print(f"Fetched {len(rows)} rows for unique_ids: {unique_ids} {rows}")
            return [r['file_id'] for r in rows if r['file_id']]

    async def insert_search_log(self, user_id: int, keyword: str):
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            await conn.execute(
                """
                INSERT INTO search_log (user_id, keyword, search_time)
                VALUES ($1, $2, $3)
                """,
                user_id, keyword, datetime.utcnow()
            )

    async def upsert_search_keyword_stat(self, keyword: str):
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            result = await conn.execute(
                 """
                INSERT INTO search_keyword_stat (keyword, search_count, last_search_time)
                VALUES ($1, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (keyword)
                DO UPDATE SET 
                    search_count = search_keyword_stat.search_count + 1,
                    last_search_time = CURRENT_TIMESTAMP
                """,
                keyword
            )
            return result

    async def get_search_keyword_id(self, keyword: str) -> int | None:
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            row = await conn.fetchrow(
                """
                WITH ins AS (
                    INSERT INTO search_keyword_stat (keyword)
                    VALUES ($1)
                    ON CONFLICT (keyword) DO NOTHING
                    RETURNING id
                )
                SELECT id FROM ins
                UNION ALL
                SELECT id FROM search_keyword_stat WHERE keyword = $1
                LIMIT 1
                """,
                keyword
            )
            return row["id"] if row else None

    async def get_keyword_by_id(self, keyword_id: int) -> str | None:
        
        cache_key = f"keyword:id:{keyword_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            row = await conn.fetchrow(
                """
                SELECT keyword
                FROM search_keyword_stat
                WHERE id = $1
                """,
                keyword_id
            )
            if row:
                self.cache.set(cache_key, row["keyword"], ttl=300)
                return row["keyword"]
            return None


    async def get_latest_membership_expire(self, user_id: str | int) -> int | None:
        """
        查询 membership 表中该 user_id 的最新有效期（expire_timestamp 最大值）。
        返回值:
          - int (UNIX 时间戳, 秒)
          - None (未找到记录)
        """
        await self._ensure_pool()
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            row = await conn.fetchrow(
                """
                SELECT expire_timestamp
                FROM membership
                WHERE user_id = $1
                ORDER BY expire_timestamp DESC NULLS LAST
                LIMIT 1
                """,
                str(user_id)  # membership.user_id 是 varchar
            )
            if row and row["expire_timestamp"] is not None:
                return int(row["expire_timestamp"])
            return None

    
    async def upsert_membership_bulk(self, rows: list[dict]) -> dict:
        """
        批量把 MySQL 的 membership 行同步到 PostgreSQL。
        冲突键： (membership_id)  ← 只按 membership_id 冲突
        更新策略：
          - course_code, user_id     ← 以 MySQL 行为准（EXCLUDED）
          - create_timestamp         ← 取 LEAST(现有, 新值)
          - expire_timestamp         ← 取 GREATEST(现有, 新值)

        注意：你的表里仍有 unique_course_user 约束，
        如果同一用户同一课程存在多条不同 membership_id 的历史记录，
        可能会触发唯一冲突。通常建议你保证一人一课唯一一条记录；
        若确需多条，请考虑移除或改造该唯一索引。
        """
        if not rows:
            return {"ok": "1", "count": 0}

        await self._ensure_pool()
        sql = """
        INSERT INTO membership (membership_id, course_code, user_id, create_timestamp, expire_timestamp)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (membership_id)
        DO UPDATE SET
            course_code      = EXCLUDED.course_code,
            user_id          = EXCLUDED.user_id,
            create_timestamp = LEAST(membership.create_timestamp, EXCLUDED.create_timestamp),
            expire_timestamp = GREATEST(membership.expire_timestamp, EXCLUDED.expire_timestamp)
        """
        try:
            async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
                async with conn.transaction():
                    await conn.executemany(
                        sql,
                        [
                            (
                                int(r["membership_id"]),
                                str(r["course_code"]),
                                str(r["user_id"]),
                                int(r["create_timestamp"]),
                                int(r["expire_timestamp"]),
                            )
                            for r in rows
                        ],
                    )
            return {"ok": "1", "count": len(rows)}
        except Exception as e:
            return {"ok": "", "error": str(e)}


    
    async def get_album_list(self, content_id: int, bot_name: str) -> list[dict]:
        """
        查询某个 album 下的所有成员文件（PostgreSQL 版）
        - 对应 PHP 的 get_album_list()
        - 使用 asyncpg，占位符 $1/$2
        - 若 m.file_id 为空且从 file_extension 匹配到 ext_file_id，则回写/新增到 sora_media.file_id
        - 返回值：list[dict]
        """
        await self._ensure_pool()

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
                fe.file_id AS ext_file_id
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

        upsert_sql = """
            INSERT INTO sora_media (content_id, source_bot_name, file_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (content_id, source_bot_name)
            DO UPDATE SET file_id = EXCLUDED.file_id
        """

        try:
            async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
                rows = await conn.fetch(sql, bot_name, content_id)

                # 先把记录转成可变 dict，并收集需要回写的条目
                dict_rows: list[dict] = []
                to_upsert: list[tuple[int, str, str]] = []  # (content_id, bot_name, file_id)

                for rec in rows or []:
                    d = dict(rec)
                    # 用 ext_file_id 填充返回值
                    if d.get("file_id") is None and d.get("ext_file_id") is not None:
                        d["file_id"] = d["ext_file_id"]
                        # 准备回写/新增到 sora_media
                        if d.get("member_content_id") is not None:
                            to_upsert.append((
                                int(d["member_content_id"]),
                                bot_name,
                                str(d["ext_file_id"]),
                            ))
                    dict_rows.append(d)

                # 批量 UPSERT 回写 sora_media（只处理确实需要写入的）
                if to_upsert:
                    async with conn.transaction():
                        for cid, bn, fid in to_upsert:
                            await conn.execute(upsert_sql, cid, bn, fid)

                return dict_rows
        except Exception as e:
            print(f"⚠️ get_album_list 出错: {e}", flush=True)
            return []


   
    async def upsert_album_items_bulk(self, rows: list[dict]) -> int:
        """
        批量 UPSERT 到 PostgreSQL 的 public.album_items
        冲突键： (content_id, member_content_id)
        更新字段：file_unique_id, file_type, position, updated_at, stage
        created_at 采用既有值（保持历史），若原表为空则用默认值
        返回：受影响（插入/更新）行数（近似）
        """
        if not rows:
            return 0
        await self._ensure_pool()

        # 只带 PG 有的列，注意 file_type 在 PG 是 text（已用 CHECK 约束）
        payload = []
        for r in rows:
            payload.append((
                int(r["content_id"]),
                int(r["member_content_id"]),
                (r.get("file_unique_id") or None),
                (str(r.get("file_type") or "") or ""),  # 允许空字符串
                int(r.get("position") or 0),
                str(r.get("stage") or "pending"),
            ))

        sql = """
            INSERT INTO album_items
                (content_id, member_content_id, file_unique_id, file_type, "position", stage, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
            ON CONFLICT (content_id, member_content_id)
            DO UPDATE SET
                file_unique_id = EXCLUDED.file_unique_id,
                file_type      = EXCLUDED.file_type,
                "position"     = EXCLUDED."position",
                stage          = EXCLUDED.stage,
                updated_at     = CURRENT_TIMESTAMP
        """
        affected = 0
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            async with conn.transaction():
                await conn.executemany(sql, payload)
                affected = len(payload)
        return affected


    async def delete_album_items_except(self, content_id: int, keep_member_ids: list[int]) -> int:
        """
        删除 PG 中该 content_id 下、但不在 keep_member_ids 的 album_items
        keep_member_ids 为空时，删除该 content_id 下所有记录
        返回：删除行数
        """
        await self._ensure_pool()
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            if keep_member_ids:
                sql = """
                    DELETE FROM album_items
                    WHERE content_id = $1
                    AND member_content_id <> ALL($2::bigint[])
                """
                res = await conn.execute(sql, content_id, keep_member_ids)
            else:
                sql = "DELETE FROM album_items WHERE content_id = $1"
                res = await conn.execute(sql, content_id)
            # asyncpg 的返回类似 'DELETE 3'，取数字
            try:
                return int(res.split()[-1])
            except Exception:
                return 0


db = DB()