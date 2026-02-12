# lz_db.py
import asyncpg
import asyncio
import os
from lz_config import POSTGRES_DSN,CACHE_TTL,VALKEY_URL
from lz_memory_cache import MemoryCache
from lz_cache import TwoLevelCache
from datetime import datetime
import lz_var
import jieba
from lexicon_manager import LexiconManager
from handlers.handle_jieba_export import ensure_and_load_lexicon_runtime
import redis.asyncio as redis_async

# ===================================
# jieba 字典只加载一次（全局控制）
# ===================================
_JIEBA_LOADED = False
_jieba_lock = asyncio.Lock()

def load_jieba_dict_once(path: str):
    """
    若文档存在，则 jieba 自定义词典只加载一次。
    """
    global _JIEBA_LOADED

    # 已加载过 → 不再重复加载
    if _JIEBA_LOADED:
        return

    # 文件不存在 → 跳过
    if not os.path.exists(path):
        print(f"[jieba] 未找到词典文件：{path}，跳过加载")
        return

    try:
        jieba.load_userdict(path)
        print(f"[jieba] 自定义词典已加载：{path}")
        _JIEBA_LOADED = True
    except Exception as e:
        print(f"[jieba] 加载词典失败: {e}")




DEFAULT_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
DEFAULT_MAX = int(os.getenv("POSTGRES_POOL_MAX", "2"))
ACQUIRE_TIMEOUT = float(os.getenv("POSTGRES_ACQUIRE_TIMEOUT", "10"))
COMMAND_TIMEOUT = float(os.getenv("POSTGRES_COMMAND_TIMEOUT", "60"))
CONNECT_TIMEOUT = float(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"))  # 新增




class DB:
    def __init__(self):
        self.dsn = POSTGRES_DSN
        self.pool: asyncpg.Pool | None = None
        # self.cache = MemoryCache()

        
        valkey_client = redis_async.from_url(VALKEY_URL, decode_responses=True)

        self.cache = TwoLevelCache(
            valkey_client=valkey_client,
            namespace="lz"
        )


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
                    statement_cache_size=0,          # 稳态优化
                )
                # 预热：设置时区/应用名
                async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
                    await conn.execute("SET SESSION TIME ZONE 'UTC'")
                    await conn.execute("SET application_name = 'lz_app'")
                print("✅ PostgreSQL 连接池初始化完成")
                
               

                async with _jieba_lock:
                    await ensure_and_load_lexicon_runtime(output_dir=".", export_if_missing=True)

               
                return
            except Exception as e:
                last_exc = e
                if attempt < retries:
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    print(f"PostgreSQL connect failed after {retries+1} attempts: {last_exc}")
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

    # 把关键字做「去头尾空白 + 全小写 + 合并多空格」，只用于 cache key 统一。
    def _normalize_query(self, keyword_str: str) -> str:
        return " ".join(keyword_str.strip().lower().split())


    async def _ensure_pool(self):
        if self.pool is None:
            await self.connect()

        if self.pool is None:
            raise RuntimeError("PostgreSQL pool failed to initialize")



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

    # 🔹 新增：支持同义词 OR 组的版本
    def _build_tsqueries_from_token_groups(self, token_groups: list[list[str]]) -> tuple[str, str]:
        """
        token_groups 结构示例：
        [
            ["鼠标", "滑鼠"],
            ["买"]
        ]

        生成：
        phrase_q: "(鼠标 | 滑鼠) <-> 买"
        and_q:    "(鼠标 | 滑鼠) & 买"
        """
        phrase_parts: list[str] = []
        and_parts: list[str] = []

        for group in token_groups:
            # 清洗 + 去空 + 去重
            cleaned = {
                self._escape_ts_lexeme(t)
                for t in group
                if t and t.strip()
            }
            if not cleaned:
                continue

            if len(cleaned) == 1:
                term = next(iter(cleaned))
            else:
                # 同义词 OR
                term = "(" + " | ".join(sorted(cleaned)) + ")"

            phrase_parts.append(term)
            and_parts.append(term)

        if not and_parts:
            return "", ""

        phrase_q = " <-> ".join(phrase_parts) if phrase_parts else ""
        and_q = " & ".join(and_parts)
        return phrase_q, and_q


    # BAckup
    async def search_keyword_page_plain_old(self, keyword_str: str, last_id: int = 0, limit: int = 3000):
        # 1) 归一化 + cache
        await self._ensure_pool()
        query = self._normalize_query(keyword_str)
        cache_key = f"searchkey:{query}:{last_id}:{limit}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        # 2) 替换同义词 + 分词
        tokens = list(jieba.cut(keyword_str))
        print("Tokens after jieba cut:", tokens)

        # 3) token 级同义词归一化
        tokens = LexiconManager.normalize_tokens(tokens)
        print("Tokens after synonym normalization:", tokens)

        # 4) 停用词过滤（用 search_stopwords.txt，专有名词会保留）
        tokens = LexiconManager.filter_stop_words(tokens)
        print("Tokens after stop-word filter:", tokens)


        phrase_q, and_q = self._build_tsqueries_from_tokens(tokens)
        if not and_q:
            return []

        # 4) 保护 limit
        limit = max(1, min(3000, int(limit)))

        where_parts = []
        params = []

        # ===== 先统一决定参数顺序 =====
        # current_idx 用来管理 $1, $2, $3...
        current_idx = 1
        phrase_idx = None
        and_idx = None

        cond = []

        if phrase_q:
            phrase_idx = current_idx
            params.append(phrase_q)
            cond.append(f"content_seg_tsv @@ to_tsquery('simple', ${phrase_idx})")
            current_idx += 1

        # and_q 一定存在
        and_idx = current_idx
        params.append(and_q)
        cond.append(f"content_seg_tsv @@ to_tsquery('simple', ${and_idx})")
        current_idx += 1

        where_parts.append("(" + " OR ".join(cond) + ")")

        # 分页条件：id < last_id
        if last_id > 0:
            last_id_idx = current_idx
            where_parts.append(f"id < ${last_id_idx}")
            params.append(last_id)
            current_idx += 1

        # LIMIT 的占位符
        limit_idx = current_idx
        params.append(limit)

        # ===== rank 表达式：有 phrase 就加权，没有就只用 AND rank =====
        if phrase_idx is not None:
            rank_expr = f"""
                GREATEST(
                    COALESCE(ts_rank_cd(content_seg_tsv, to_tsquery('simple', ${phrase_idx})), 0) * 1.5,
                    ts_rank_cd(content_seg_tsv, to_tsquery('simple', ${and_idx}))
                )
            """
        else:
            rank_expr = f"ts_rank_cd(content_seg_tsv, to_tsquery('simple', ${and_idx}))"

        sql = f"""
            SELECT
                id,
                source_id,
                file_type,
                content,
                {rank_expr} AS rank
            FROM sora_content
            WHERE {' AND '.join(where_parts)} AND valid_state >= 8
            ORDER BY rank DESC, id DESC
            LIMIT ${limit_idx}
        """

        print("SQL:", sql, "PARAMS:", params, flush=True)

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            rows = await conn.fetch(sql, *params)

        result = [dict(r) for r in rows]
        # ttl=300 秒（5 分钟）
        self.cache.set(cache_key, result, ttl=CACHE_TTL)
        return result

    async def search_keyword_page_plain(self, keyword_str: str, last_id: int = 0, limit: int = 10000):
        # 1) 归一化 + cache
        await self._ensure_pool()
        query = self._normalize_query(keyword_str)
        cache_key = f"searchkey:{query}"
        cached = self.cache.get(cache_key)
        print(f"Cache key: {cache_key}")
        if cached:
            print("Cache hit")
            return cached

        

        # 2) 分词
        tokens = list(jieba.cut(keyword_str))
        print("Tokens after jieba cut:", tokens)

        # 3) 停用词过滤（用 search_stopwords.txt，专有名词会保留）
        tokens = LexiconManager.filter_stop_words(tokens)
        print("Tokens after stop-word filter:", tokens)

        # 4) 同义词叠加：每个 token -> [本词 + 全部同义词]
        token_groups = LexiconManager.expand_tokens(tokens)
        print("Token groups after synonym expand:", token_groups)

        # 5) 生成 tsquery：用 OR 组构成 phrase_q / and_q
        phrase_q, and_q = self._build_tsqueries_from_token_groups(token_groups)
        if not and_q:
            return []

        # 下面的 limit / where_parts / params / SQL 构造都维持原样，不动
        # 4) 保护 limit
        limit = max(1, min(3000, int(limit)))

        where_parts = []
        params = []

        # ===== 先统一决定参数顺序 =====
        current_idx = 1
        phrase_idx = None
        and_idx = None

        cond = []

        if phrase_q:
            phrase_idx = current_idx
            params.append(phrase_q)
            cond.append(f"content_seg_tsv @@ to_tsquery('simple', ${phrase_idx})")
            current_idx += 1

        # and_q 一定存在
        and_idx = current_idx
        params.append(and_q)
        cond.append(f"content_seg_tsv @@ to_tsquery('simple', ${and_idx})")
        current_idx += 1

        where_parts.append("(" + " OR ".join(cond) + ")")

        if last_id > 0:
            last_id_idx = current_idx
            where_parts.append(f"id < ${last_id_idx}")
            params.append(last_id)
            current_idx += 1

        limit_idx = current_idx
        params.append(limit)

        if phrase_idx is not None:
            rank_expr = f"""
                GREATEST(
                    COALESCE(ts_rank_cd(content_seg_tsv, to_tsquery('simple', ${phrase_idx})), 0) * 1.5,
                    ts_rank_cd(content_seg_tsv, to_tsquery('simple', ${and_idx}))
                )
            """
        else:
            rank_expr = f"ts_rank_cd(content_seg_tsv, to_tsquery('simple', ${and_idx}))"

        sql = f"""
            SELECT
                id,
                source_id,
                file_type,
                content,
                {rank_expr} AS rank
            FROM sora_content
            WHERE {' AND '.join(where_parts)} AND valid_state >= 8
            ORDER BY rank DESC, id DESC
            LIMIT ${limit_idx}
        """

        # print("SQL:", sql, "PARAMS:", params, flush=True)

        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            rows = await conn.fetch(sql, *params)

        result = [dict(r) for r in rows]
        # print(f"{result}")
        self.cache.set(cache_key, result, ttl=CACHE_TTL, only_l2=True)
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


        await self._ensure_pool()
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            
            result = await conn.execute(sql, file_type, file_unique_id, file_id, bot, user_id, now)
            
            sql2 =  """
                    SELECT id FROM sora_content WHERE source_id = $1 OR thumb_file_unique_id = $2
                    """        
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
                    s.thumb_file_unique_id, s.valid_state, s.file_password,
                    m.file_id AS m_file_id, m.thumb_file_id AS m_thumb_file_id,
                    p.price as fee, p.file_type as product_type, p.owner_user_id, p.purchase_condition, p.review_status 
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
                "purchase_condition": row.get("purchase_condition"),
                "valid_state": row.get("valid_state"),
                "review_status": row.get("review_status"),
                "file_password": row.get("file_password")
            }

            # print(f"\r\n\r\nFinal result for content_id {content_id}: {result}")

            # self.cache.set(cache_key, result, ttl=3600)
            self.cache.set(cache_key, result, ttl=CACHE_TTL)
            # print(f"Cache set for {cache_key}")
            return result

            # 返回 asyncpg Record 或 None

    # async def get_next_content_id(self, current_id: int, offset: int) -> int | None:
    #     await self._ensure_pool()
    #     async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
    #         if offset > 0:
    #             row = await conn.fetchrow(
    #                 """
    #                 SELECT id FROM sora_content
    #                 WHERE id > $1
    #                 ORDER BY id ASC
    #                 LIMIT 1
    #                 """,
    #                 current_id
    #             )
    #         else:
    #             row = await conn.fetchrow(
    #                 """
    #                 SELECT id FROM sora_content
    #                 WHERE id < $1
    #                 ORDER BY id DESC
    #                 LIMIT 1
    #                 """,
    #                 current_id
    #             )
    #         if row:
    #             return row["id"]
    #         return None

    async def get_file_id_by_file_unique_id(self, unique_ids: list[str]) -> list[str]:
        """
        根据多个 file_unique_id 取得对应的 file_id 列表。
        """
        await self._ensure_pool()
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
        await self._ensure_pool()
        async with self.pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
            await conn.execute(
                """
                INSERT INTO search_log (user_id, keyword, search_time)
                VALUES ($1, $2, $3)
                """,
                user_id, keyword, datetime.utcnow()
            )

    async def upsert_search_keyword_stat(self, keyword: str):
        await self._ensure_pool()
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
        await self._ensure_pool()
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
        await self._ensure_pool()
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
                self.cache.set(cache_key, row["keyword"], ttl=CACHE_TTL)
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
            print(f"⚠️ 802 get_album_list 出错: {e}", flush=True)
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
                (content_id, member_content_id, file_unique_id, file_type, "position", stage, preview,updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7,CURRENT_TIMESTAMP)
            ON CONFLICT (content_id, member_content_id)
            DO UPDATE SET
                file_unique_id = EXCLUDED.file_unique_id,
                file_type      = EXCLUDED.file_type,
                "position"     = EXCLUDED."position",
                stage          = EXCLUDED.stage,
                preview        = EXCLUDED.preview,
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