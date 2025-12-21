import asyncio
import jieba
from lz_pgsql import PGPool

# sync_mysql_pool.py
import os
import aiomysql
from typing import Optional, Tuple, Any, Dict, List, Set
from lexicon_manager import LexiconManager


class MySQLPool:
    """
    最小化 MySQL 连接池：仅服务 sync()/check_file_record() 这条链路
    - init_pool()
    - ensure_pool()
    - get_conn_cursor()
    - release()
    - close()
    """

    _pool: Optional[aiomysql.Pool] = None

    @classmethod
    async def init_pool(cls) -> None:
        if cls._pool is not None:
            return

        from lz_config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_DB_PORT

        host = os.getenv("MYSQL_HOST", "127.0.0.1")
        port = int(os.getenv("MYSQL_PORT", "3306"))
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        db = os.getenv("MYSQL_DB", "telebot")
        minsize = int(os.getenv("MYSQL_POOL_MIN", "1"))
        maxsize = int(os.getenv("MYSQL_POOL_MAX", "10"))
        charset = os.getenv("MYSQL_CHARSET", "utf8mb4")

       

        cls._pool = await aiomysql.create_pool(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DB,
            port=MYSQL_DB_PORT,
            charset="utf8mb4",
            autocommit=True,
            minsize=2,
            maxsize=32,
            pool_recycle=1800,
            connect_timeout=10,
        )

        print("✅ [SyncMySQLPool] MySQL 连接池初始化完成", flush=True)

    @classmethod
    async def ensure_pool(cls) -> aiomysql.Pool:
        if cls._pool is None:
            raise RuntimeError("SyncMySQLPool not initialized. Call init_pool() first.")
        return cls._pool

    @classmethod
    async def get_conn_cursor(cls) -> Tuple[aiomysql.Connection, aiomysql.Cursor]:
        """
        返回 (conn, cur)；cur 默认 DictCursor，符合你目前代码使用 r['id'] 这种访问方式
        """
        pool = await cls.ensure_pool()
        conn = await pool.acquire()
        try:
            cur = await conn.cursor(aiomysql.DictCursor)
        except Exception:
            pool.release(conn)
            raise
        return conn, cur

    @classmethod
    async def release(cls, conn: Any, cur: Any) -> None:
        """
        与你当前代码风格一致：无论成功失败，都可以安全 release
        """
        try:
            if cur is not None:
                await cur.close()
        except Exception:
            pass

        try:
            if cls._pool is not None and conn is not None:
                cls._pool.release(conn)
        except Exception:
            pass

    @classmethod
    async def close(cls) -> None:
        if cls._pool is None:
            return
        cls._pool.close()
        try:
            await cls._pool.wait_closed()
        except Exception:
            pass
        cls._pool = None
        print("🛑 [SyncMySQLPool] MySQL 连接池已关闭", flush=True)




async def sync():

    summary = await apply_thumb_from_bid_thumbnail_t5_batched(
        batch_size=500,
        sleep_seconds=0.05,
    )

    # summary = await dedupe_bid_thumbnail_t_update4_to5_batched(
    #     batch_groups=500,
    #     sleep_seconds=0.05,
    # )
    # await sync_bid_thumbnail_t_update_batched()
    # await sync_product_mysql_to_postgres_no_json_fix()
    # await MySQLPool.init_pool()
    # await diff_bodyexam_files()

    # # 1. 同步 / 修复 file_record
    # while False:
    #     summary = await check_file_record(limit=100)
    #     if summary.get("checked", 0) == 0:
    #         break

    # await MySQLPool.init_pool()
    # while True:
    #     r = await check_and_fix_file_tag_avalible(limit=2000)
    #     print(r, flush=True)
    #     if r["checked"] == 0:
    #         break



    # 2. 如需启用以下修复逻辑，取消注释即可
    #
    # while True:
    #     summary = await check_and_fix_sora_valid_state(limit=1000)
    #     if summary.get("checked", 0) == 0:
    #         break
    #
    # while True:
    #     summary = await check_and_fix_sora_valid_state2(limit=1000)
    #     if summary.get("checked", 0) == 0:
    #         break



'''
同步 product 表
'''
async def sync_product_mysql_to_postgres_no_json_fix(
    batch_size: int = 2000,
) -> Dict[str, int]:
    """
    全量同步 MySQL.product -> PostgreSQL.public.product（PG 目前为空也可用）
    - PG product.id 显式写入 MySQL.id（不走 nextval）
    - purchase_condition 不做任何清洗/容错：原样写入，并在 PG 端强制 ::jsonb
      => 只要遇到不合法 JSON，会直接报错中断（符合“不处理 JSON 不合法”的要求）
    - 同步完成后修正 product_id_seq，避免后续 nextval 撞号
    """
    await MySQLPool.init_pool()
    await PGPool.init_pool()
    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    fetched = 0
    inserted_or_updated = 0
    last_id = 0

    while True:
        conn, cur = await MySQLPool.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT
                    id,
                    name,
                    content,
                    guild_id,
                    price,
                    content_id,
                    file_type,
                    owner_user_id,
                    anonymous_mode,
                    view_times,
                    purchase_times,
                    like_times,
                    dislike_times,
                    hot_score,
                    bid_status,
                    review_status,
                    purchase_condition,
                    created_at,
                    updated_at
                FROM product
                WHERE id > %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (int(last_id), int(batch_size)),
            )
            rows = await cur.fetchall()
        finally:
            await MySQLPool.release(conn, cur)

        if not rows:
            break

        fetched += len(rows)
        last_id = int(rows[-1]["id"])

        payload: List[Tuple[Any, ...]] = []
        for r in rows:
            payload.append((
                int(r["id"]),
                r.get("name"),
                r.get("content"),
                r.get("guild_id"),
                int(r.get("price") or 0),
                int(r["content_id"]),
                r.get("file_type"),
                r.get("owner_user_id"),
                int(r.get("anonymous_mode") or 1),
                int(r.get("view_times") or 0),
                int(r.get("purchase_times") or 0),
                int(r.get("like_times") or 0),
                int(r.get("dislike_times") or 0),
                int(r.get("hot_score") or 0),
                int(r.get("bid_status") or 0),
                int(r.get("review_status") or 0),
                r.get("purchase_condition"),  # 原样：str/None
                r.get("created_at"),
                r.get("updated_at"),
            ))

        pg_conn = await PGPool.acquire()
        try:
            sql = """
                INSERT INTO public.product (
                    id,
                    name,
                    content,
                    guild_id,
                    price,
                    content_id,
                    file_type,
                    owner_user_id,
                    anonymous_mode,
                    view_times,
                    purchase_times,
                    like_times,
                    dislike_times,
                    hot_score,
                    bid_status,
                    review_status,
                    purchase_condition,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15,$16,
                    $17,
                    $18,$19
                )
                ON CONFLICT (content_id) DO UPDATE SET
                    id = EXCLUDED.id,
                    name = EXCLUDED.name,
                    content = EXCLUDED.content,
                    guild_id = EXCLUDED.guild_id,
                    price = EXCLUDED.price,
                    file_type = EXCLUDED.file_type,
                    owner_user_id = EXCLUDED.owner_user_id,
                    anonymous_mode = EXCLUDED.anonymous_mode,
                    view_times = EXCLUDED.view_times,
                    purchase_times = EXCLUDED.purchase_times,
                    like_times = EXCLUDED.like_times,
                    dislike_times = EXCLUDED.dislike_times,
                    hot_score = EXCLUDED.hot_score,
                    bid_status = EXCLUDED.bid_status,
                    review_status = EXCLUDED.review_status,
                    purchase_condition = EXCLUDED.purchase_condition,
                    created_at = COALESCE(EXCLUDED.created_at, public.product.created_at),
                    updated_at = COALESCE(EXCLUDED.updated_at, public.product.updated_at)
            """
            async with pg_conn.transaction():
                await pg_conn.executemany(sql, payload)
                inserted_or_updated += len(payload)
        finally:
            await PGPool.release(pg_conn)

        print(f"✅ [product sync] batch done, last_id={last_id}, rows={len(rows)}", flush=True)

    # 修正 sequence
    pg_conn = await PGPool.acquire()
    try:
        async with pg_conn.transaction():
            await pg_conn.execute(
                """
                SELECT setval(
                    'product_id_seq',
                    GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.product), 1),
                    true
                )
                """
            )
    finally:
        await PGPool.release(pg_conn)

    summary = {"fetched": fetched, "inserted_or_updated": inserted_or_updated}
    print(f"🎯 [product sync] DONE: {summary}", flush=True)
    return summary

''''
'''



def _escape_ts_lexeme(s: str) -> str:
    # 简单转义，避免 to_tsquery 特殊字符影响；必要时再扩充
    return s.replace("'", "''").replace("&", " ").replace("|", " ").replace("!", " ").replace(":", " ").strip()



  # 🔹 新增：支持同义词 OR 组的版本
def _build_tsqueries_from_token_groups(token_groups: list[list[str]]) -> tuple[str, str]:
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
            _escape_ts_lexeme(t)
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

async def search(keyword_str):
   
    # 2) 分词
    jieba.load_userdict("jieba_userdict.txt")

    tokens = list(jieba.cut(keyword_str))
    print("Tokens after jieba cut:", tokens)

    # 3) 停用词过滤（用 search_stopwords.txt，专有名词会保留）
    tokens = LexiconManager.filter_stop_words(tokens)
    print("Tokens after stop-word filter:", tokens)

    # 4) 同义词叠加：每个 token -> [本词 + 全部同义词]
    token_groups = LexiconManager.expand_tokens(tokens)
    print("Token groups after synonym expand:", token_groups)

    # 5) 生成 tsquery：用 OR 组构成 phrase_q / and_q
    phrase_q, and_q = _build_tsqueries_from_token_groups(token_groups)
    if not and_q:
        return []

    # 下面的 limit / where_parts / params / SQL 构造都维持原样，不动
    # 4) 保护 limit
   

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
            source_id,
            {rank_expr} AS rank
        FROM sora_content
        WHERE {' AND '.join(where_parts)} AND valid_state >= 8
        ORDER BY rank DESC, id DESC
        
    """

    # print("SQL:", sql, "PARAMS:", params, flush=True)

    pg_conn = await PGPool.acquire()
    try:

        async with pg_conn.transaction():
            rows = await pg_conn.fetch(sql, *params)
            return rows
        # asyncpg: "UPDATE <n>"
       
    finally:
        await PGPool.release(pg_conn)

    
async def get_file_tag_bodyexam():
    await MySQLPool.ensure_pool()
    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await cur.execute("""
            SELECT file_unique_id
            FROM file_tag
            WHERE tag = 'bodyexam'
              AND avalible = 1
        """)
        rows = await cur.fetchall()
        return rows
    finally:
        await MySQLPool.release(conn, cur)
   
async def diff_bodyexam_files():
    # A rows：来自 search
    a_rows = await search("身体检查")
    a_ids = {r["source_id"] for r in a_rows if r.get("source_id")}

    print(f"[A] search 命中数量: {len(a_ids)}")

    # B rows：来自 file_tag
    b_rows = await get_file_tag_bodyexam()
    b_ids = {r["file_unique_id"] for r in b_rows if r.get("file_unique_id")}

    print(f"[B] file_tag(bodyexam, avalible=1) 数量: {len(b_ids)}")

    # C rows：B - A
    c_ids = b_ids - a_ids

    print(f"[C] 需要处理的 file_unique_id 数量: {len(c_ids)}")
    if not c_ids:
        print("✅ 无需更新 content_seg")
        return set()

    # 🔹 核心新增逻辑
    updated = await append_bodyexam_to_content_seg(c_ids)
    print(f"🩺 已更新 content_seg（身体检查）行数: {updated}")

    return c_ids


from typing import Set

async def append_bodyexam_to_content_seg(file_unique_ids: Set[str]) -> int:
    """
    给 sora_content.content_seg 追加 '身体检查'
    - 不重复追加
    - 自动触发 content_seg_tsv 重算
    """
    if not file_unique_ids:
        return 0

    await PGPool.ensure_pool()
    pg_conn = await PGPool.acquire()
    try:
        sql = """
            UPDATE sora_content
            SET content_seg =
                CASE
                    WHEN content_seg IS NULL OR content_seg = ''
                        THEN '身体检查'
                    WHEN content_seg LIKE '%身体检查%'
                        THEN content_seg
                    ELSE content_seg || ' 身体检查'
                END
            WHERE source_id = ANY($1::text[])
        """
        async with pg_conn.transaction():
            result = await pg_conn.execute(sql, list(file_unique_ids))

        # asyncpg 返回格式："UPDATE <n>"
        return int(result.split()[-1])
    finally:
        await PGPool.release(pg_conn)



async def check_and_fix_file_tag_avalible(limit: int = 2000) -> Dict[str, Any]:
    """
    修复 file_tag.avalible：
    - file_tag.avalible=0 且在 file_extension 存在相同 file_unique_id -> avalible=1
    - file_tag.avalible=0 且在 file_extension 不存在 -> avalible=2

    以批次更新方式减少长事务与锁竞争；不会锁表，只会锁本批次命中的行。
    """
   
    await MySQLPool.ensure_pool()

    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await conn.begin()

        # 1) 存在于 file_extension：置 1
        sql_exists = """
            UPDATE file_tag ft
            INNER JOIN file_extension fe
                ON fe.file_unique_id = ft.file_unique_id
            SET ft.avalible = 1
            WHERE ft.avalible = 0
            ORDER BY ft.id
            LIMIT %s
        """
        await cur.execute(sql_exists, (int(limit),))
        updated_to_1 = cur.rowcount or 0

        # 2) 不存在于 file_extension：置 2
        # 只处理仍为 avalible=0 的（避免覆盖上一步已置 1 的）
        sql_missing = """
            UPDATE file_tag ft
            LEFT JOIN file_extension fe
                ON fe.file_unique_id = ft.file_unique_id
            SET ft.avalible = 2
            WHERE ft.avalible = 0
                AND fe.file_unique_id IS NULL
            ORDER BY ft.id
            LIMIT %s
        """
        await cur.execute(sql_missing, (int(limit),))
        updated_to_2 = cur.rowcount or 0

        await conn.commit()

        return {
            "checked": updated_to_1 + updated_to_2,  # 本批次实际更新行数
            "updated_to_1": updated_to_1,
            "updated_to_2": updated_to_2,
        }

    except Exception as e:
        try:
            await conn.rollback()
        except Exception:
            pass
        raise RuntimeError(f"[check_and_fix_file_tag_avalible] failed: {e}") from e
    finally:
        await MySQLPool.release(conn, cur)



async def check_file_record(limit:int = 100):
    '''
    从 Mysql table file_records3 中取出 limit 条记录
    (1) 用 insert/update 语句插入到 mysql 的 table file_unique_id 中 , 
    file_records3.file_unique_id 对应 file_unique_id.file_unique_id,
    file_records3.file_id 对应 file_unique_id.file_id
    file_records3.file_type 对应 file_unique_id.file_type
    file_records3.bot_id 转译后对应 file_unique_id.bot (其中 bot_id:7985482732 = bot:Queue9838bot, bot_id:7629569353 = bot:stcparkbot )
    (2) 根据 file_records3.file_type, 分别维护表 video, photo, document, animation, 并以 insert/update 语句插入/更新对应的记录
    [Tabble].file_unique_id 对应各表的 file_records3.file_unique_id
    [Table].file_size 对应各表的 file_records3.file_size
    [Table].mime_type 对应各表的 file_records3.mime_type
    [Table].file_name 对应各表的 file_records3.file_name
    (3) 将 MySQL 中 table sora_content 中 sora_content.source_id = file_records3.file_unique_id 的记录, valid_state 更新为 9, stage 更新为 pending
    (4) 将 PostgreSQL 中 table sora_content 中 sora_content.source_id = file_records3.file_unique_id 的记录, valid_state 更新为 9, stage 更新为 pending
    (5) 删除 file_records3 中已经处理过的记录


    '''



    # ---------- 0) Pools ----------
    await asyncio.gather(MySQLPool.init_pool(), PGPool.init_pool())
    await MySQLPool.ensure_pool()
    await PGPool.ensure_pool()

    # ---------- 1) Fetch file_records3 ----------
    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await cur.execute(
            """
            SELECT
                id,
                file_unique_id,
                file_id,
                file_type,
                bot_id,
                man_id,
                file_size,
                mime_type,
                file_name
            FROM file_records3 
            WHERE process = 0
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = await cur.fetchall()
    except Exception as e:
        print(f"⚠️ [check_file_record] MySQL 查询 file_records3 出错: {e}", flush=True)
        await MySQLPool.release(conn, cur)
        return {
            "checked": 0,
            "upsert_file_ext": 0,
            "upsert_media": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": 0,
        }
    finally:
        await MySQLPool.release(conn, cur)

    if not rows:
        print("[check_file_record] file_records3 无待处理记录。", flush=True)
        return {
            "checked": 0,
            "upsert_file_ext": 0,
            "upsert_media": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": 0,
        }

    checked = len(rows)

    # ---------- 2) Helpers ----------
    BOT_ID_MAP = {
        7985482732: "Queue9838bot",
        7629569353: "stcparkbot",
    }

    def bot_name_of(bot_id) -> str:
        try:
            bid = int(bot_id) if bot_id is not None else None
        except Exception:
            bid = None
        if bid is None:
            return "unknown"
        return BOT_ID_MAP.get(bid, str(bid))

    def normalize_ft(ft: str) -> str:
        ft = (ft or "").lower().strip()
        if ft in ("v", "video"):
            return "video"
        if ft in ("a", "animation"):
            return "animation"
        if ft in ("d", "document"):
            return "document"
        if ft in ("p", "photo"):
            return "photo"
        return ""

    def safe_sid50(fu: str) -> str:
        return str(fu)[:50]  # MySQL sora_content.source_id = varchar(50); PG 也统一用 50

    def safe_fu100(fu: str) -> str:
        return str(fu)[:100]  # file_extension.file_unique_id = varchar(100)

    # ---------- 3) Build payloads ----------
    record_ids: list[int] = []
    source_ids_50: list[str] = []

    file_ext_payload = []  # (file_type, file_unique_id(100), file_id, bot, user_id)

    media_payload_v = []  # video: (fu, file_size, duration, width, height, file_name, mime_type, caption)
    media_payload_a = []  # animation
    media_payload_d = []  # document: (fu, file_size, file_name, mime_type, caption)
    media_payload_p = []  # photo: (fu, file_size, width, height, file_name, caption, root_unique_id)

    skipped_photo = 0

    # 注意：file_records3 这张表结构里没有 duration/width/height/caption/root_unique_id
    # 因此：
    # - video/animation/document 可以写 NULL（允许）
    # - photo 因 width/height NOT NULL -> 缺失只能跳过
    for r in rows:
        rid = int(r["id"])
        fu = r.get("file_unique_id")
        fid = r.get("file_id")
        if not fu or not fid:
            continue

        record_ids.append(rid)

        sid50 = safe_sid50(fu)
        source_ids_50.append(sid50)

        bot = bot_name_of(r.get("bot_id"))
        fu100 = safe_fu100(fu)

        file_ext_payload.append((
            r.get("file_type"),
            fu100,
            fid,
            bot,
            r.get("man_id"),  # 映射到 file_extension.user_id
        ))

        ft_norm = normalize_ft(r.get("file_type"))
        file_size = r.get("file_size") or 0
        mime_type = r.get("mime_type")
        file_name = r.get("file_name")

        if ft_norm == "video":
            media_payload_v.append((
                fu100,
                int(file_size),
                None,  # duration
                None,  # width
                None,  # height
                file_name,
                mime_type or "video/mp4",
                None,  # caption
            ))
        elif ft_norm == "animation":
            media_payload_a.append((
                fu100,
                int(file_size),
                None,
                None,
                None,
                file_name,
                mime_type or "video/mp4",
                None,
            ))
        elif ft_norm == "document":
            media_payload_d.append((
                fu100,
                int(file_size),
                file_name,
                mime_type,
                None,  # caption
            ))
        elif ft_norm == "photo":
            # file_records3 缺 width/height -> 必须跳过
            skipped_photo += 1
            continue

    # 去重（保持顺序）
    source_ids_50 = list(dict.fromkeys(source_ids_50))

    if not record_ids:
        return {
            "checked": checked,
            "upsert_file_ext": 0,
            "upsert_media": 0,
            "updated_mysql": 0,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": skipped_photo,
        }

    # ---------- 4) MySQL Transaction ----------
    upsert_file_ext = 0
    upsert_media = 0
    updated_mysql = 0
    deleted = 0

    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await conn.begin()

        # 4.1 upsert file_extension（UNIQUE(file_id, bot)）
        # create_time：新插入用 NOW()；重复时不强制覆盖（保留旧值），同时更新 file_type/file_unique_id/user_id
        if file_ext_payload:
            sql_ext = """
                INSERT INTO file_extension
                    (file_type, file_unique_id, file_id, bot, user_id, create_time)
                VALUES
                    (%s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    file_type      = VALUES(file_type),
                    file_unique_id = VALUES(file_unique_id),
                    user_id        = COALESCE(VALUES(user_id), user_id)
            """
            await cur.executemany(sql_ext, file_ext_payload)
            upsert_file_ext = cur.rowcount or 0

        # 4.2 upsert video/animation/document/photo（按你 DDL）
        async def _upsert_video_like(table_name: str, payload: list) -> int:
            if not payload:
                return 0
            sql = f"""
                INSERT INTO {table_name}
                    (file_unique_id, file_size, duration, width, height, file_name, mime_type, caption, create_time, update_time)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    file_size   = VALUES(file_size),
                    duration    = VALUES(duration),
                    width       = VALUES(width),
                    height      = VALUES(height),
                    file_name   = VALUES(file_name),
                    mime_type   = VALUES(mime_type),
                    caption     = VALUES(caption),
                    update_time = NOW()
            """
            await cur.executemany(sql, payload)
            return cur.rowcount or 0

        async def _upsert_document(payload: list) -> int:
            if not payload:
                return 0
            sql = """
                INSERT INTO document
                    (file_unique_id, file_size, file_name, mime_type, caption, create_time, update_time)
                VALUES
                    (%s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    file_size   = VALUES(file_size),
                    file_name   = VALUES(file_name),
                    mime_type   = VALUES(mime_type),
                    caption     = VALUES(caption),
                    update_time = NOW()
            """
            await cur.executemany(sql, payload)
            return cur.rowcount or 0

        async def _upsert_photo(payload: list) -> int:
            # 基于你当前 file_records3 缺 width/height，这里通常不会被调用
            if not payload:
                return 0
            sql = """
                INSERT INTO photo
                    (file_unique_id, file_size, width, height, file_name, caption, root_unique_id, create_time, update_time)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    file_size   = VALUES(file_size),
                    width       = VALUES(width),
                    height      = VALUES(height),
                    file_name   = VALUES(file_name),
                    caption     = VALUES(caption),
                    root_unique_id = VALUES(root_unique_id),
                    update_time = NOW()
            """
            await cur.executemany(sql, payload)
            return cur.rowcount or 0

        upsert_media += await _upsert_video_like("video", media_payload_v)
        upsert_media += await _upsert_video_like("animation", media_payload_a)
        upsert_media += await _upsert_document(media_payload_d)
        upsert_media += await _upsert_photo(media_payload_p)

        # 4.3 UPDATE MySQL sora_content（只更新已存在；不插入新行）
        # 分批避免 IN 过长
        BATCH = 500
        if source_ids_50:
            for i in range(0, len(source_ids_50), BATCH):
                batch_sids = source_ids_50[i:i + BATCH]
                placeholders = ",".join(["%s"] * len(batch_sids))
                sql_sc = f"""
                    UPDATE sora_content
                    SET valid_state = 9,
                        stage = 'pending'
                    WHERE source_id IN ({placeholders})
                """
                await cur.execute(sql_sc, tuple(batch_sids))
                updated_mysql += cur.rowcount or 0

        # 4.4 软删除本批已处理 file_records3
        if record_ids:
            for i in range(0, len(record_ids), BATCH):
                batch_ids = record_ids[i:i + BATCH]
                placeholders = ",".join(["%s"] * len(batch_ids))
                sql_del = f"UPDATE file_records3 SET process = 1 WHERE id IN ({placeholders})"
                await cur.execute(sql_del, tuple(batch_ids))


                sql_del = f"UPDATE file_records3 SET process = 1 WHERE id IN ({placeholders})"
                await cur.execute(sql_del, tuple(batch_ids))

                deleted += cur.rowcount or 0

        await conn.commit()

    except Exception as e:
        try:
            await conn.rollback()
        except Exception:
            pass
        print(f"❌ [check_file_record] MySQL 事务失败并回滚: {e}", flush=True)
        # MySQL 失败则 PG 不做更新（避免两边状态不一致）
        return {
            "checked": checked,
            "upsert_file_ext": upsert_file_ext,
            "upsert_media": upsert_media,
            "updated_mysql": updated_mysql,
            "updated_pg": 0,
            "deleted": 0,
            "skipped_photo": skipped_photo,
        }
    finally:
        await MySQLPool.release(conn, cur)

    # ---------- 5) PostgreSQL UPDATE (B1 only) ----------
    updated_pg = 0
    try:
        if source_ids_50:
            pg_conn = await PGPool.acquire()
            try:
                sql_pg = """
                    UPDATE public.sora_content
                    SET valid_state = 9,
                        stage = 'pending'
                    WHERE source_id = ANY($1::text[])
                """
                async with pg_conn.transaction():
                    result = await pg_conn.execute(sql_pg, source_ids_50)

                # asyncpg: "UPDATE <n>"
                try:
                    updated_pg = int(str(result).split()[-1])
                except Exception:
                    updated_pg = 0
            finally:
                await PGPool.release(pg_conn)

    except Exception as e:
        print(f"⚠️ [check_file_record] PostgreSQL UPDATE sora_content 出错: {e}", flush=True)

    summary = {
        "checked": checked,
        "upsert_file_ext": upsert_file_ext,
        "upsert_media": upsert_media,
        "updated_mysql": updated_mysql,
        "updated_pg": updated_pg,
        "deleted": deleted,
        "skipped_photo": skipped_photo,
    }
    print(f"[check_file_record] Done: {summary}", flush=True)
    return summary



async def sync_bid_thumbnail_t_update_batched(
    batch_size: int = 2000,
    sleep_seconds: float = 0.0,
    max_rounds: Optional[int] = None,
    ensure_index: bool = False,
) -> Dict[str, Any]:
    """
    分批修复 bid_thumbnail.t_update：
    - t_update=2 且 file_unique_id 存在于 file_extension -> 置 3
    - t_update=2 且不存在 -> 置 0

    特性：
    - 分批（LIMIT batch_size），降低 MyISAM 表锁影响
    - 每批打印进度
    - Ctrl+C 可中断：会在批次边界安全退出（已提交的批次不回滚）

    参数：
    - batch_size: 每批更新行数上限（建议 500~2000）
    - sleep_seconds: 每批之间 sleep（可用来进一步降低对线上影响）
    - max_rounds: 最多跑多少轮（None 表示跑到没有可更新为止）
    - ensure_index: 是否尝试创建 idx_bid_thumb_tupdate_uid(t_update, file_unique_id) 索引
                    注意：MyISAM 创建索引也会锁表，生产环境谨慎开启

    返回：
    - 统计信息 dict
    """
    await MySQLPool.init_pool()
    await MySQLPool.ensure_pool()

    total_to_3 = 0
    total_to_0 = 0
    rounds = 0

    # 可选：创建索引（建议你手动在低峰做；这里提供开关）
    if ensure_index:
        conn, cur = await MySQLPool.get_conn_cursor()
        try:
            # MySQL 8+ 可用 IF NOT EXISTS；若你不是 MySQL 8，下面会报错
            # 为兼容性，改用 SHOW INDEX 判断再建
            await cur.execute("SHOW INDEX FROM bid_thumbnail WHERE Key_name = 'idx_bid_thumb_tupdate_uid'")
            exists = await cur.fetchone()
            if not exists:
                print("🔧 Creating index idx_bid_thumb_tupdate_uid ...", flush=True)
                await cur.execute(
                    "ALTER TABLE bid_thumbnail ADD INDEX idx_bid_thumb_tupdate_uid (t_update, file_unique_id)"
                )
                print("✅ Index created.", flush=True)
            else:
                print("ℹ️ Index already exists: idx_bid_thumb_tupdate_uid", flush=True)
        finally:
            await MySQLPool.release(conn, cur)

    print(
        f"🚀 [bid_thumbnail] start batched sync: batch_size={batch_size}, sleep={sleep_seconds}, max_rounds={max_rounds}",
        flush=True,
    )

    try:
        while True:
            rounds += 1
            if max_rounds is not None and rounds > int(max_rounds):
                print(f"🛑 Reached max_rounds={max_rounds}. Stop.", flush=True)
                break

            # ========== Batch 1: EXISTS -> 3 ==========
            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                await conn.begin()
                sql_exists = f"""
                    UPDATE bid_thumbnail bt
                    INNER JOIN file_extension fe
                        ON fe.file_unique_id = bt.thumb_file_unique_id
                    SET bt.t_update = 4
                    WHERE bt.t_update = 3
                    ORDER BY bt.bid_thumbnail_id
                    LIMIT {int(batch_size)}
                """
                await cur.execute(sql_exists)
                updated_to_3 = cur.rowcount or 0
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                await MySQLPool.release(conn, cur)

            total_to_3 += updated_to_3

            # ========== Batch 2: MISSING -> 0 ==========
            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                await conn.begin()
                sql_missing = f"""
                    UPDATE bid_thumbnail bt
                    LEFT JOIN file_extension fe
                        ON fe.file_unique_id = bt.thumb_file_unique_id
                    SET bt.t_update = 0
                    WHERE bt.t_update = 3
                      AND fe.file_unique_id IS NULL
                    ORDER BY bt.bid_thumbnail_id
                    LIMIT {int(batch_size)}
                """
                await cur.execute(sql_missing)
                updated_to_0 = cur.rowcount or 0
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                await MySQLPool.release(conn, cur)

            total_to_0 += updated_to_0

            batch_total = updated_to_3 + updated_to_0
            print(
                f"✅ [bid_thumbnail] round={rounds} "
                f"updated_to_3={updated_to_3} updated_to_0={updated_to_0} "
                f"round_total={batch_total} "
                f"grand_total={total_to_3 + total_to_0}",
                flush=True,
            )

            # 这一轮两步都没有更新：结束
            if batch_total == 0:
                print("🎯 [bid_thumbnail] no more rows to update. Done.", flush=True)
                break

            if sleep_seconds and sleep_seconds > 0:
                await asyncio.sleep(float(sleep_seconds))

    except KeyboardInterrupt:
        # 可中断：不会回滚已提交批次，只是停止后续批次
        print(
            f"⛔ [bid_thumbnail] interrupted by user. "
            f"rounds={rounds} total_to_3={total_to_3} total_to_0={total_to_0}",
            flush=True,
        )

    result = {
        "rounds": rounds,
        "updated_to_3": total_to_3,
        "updated_to_0": total_to_0,
        "total": total_to_3 + total_to_0,
        "batch_size": int(batch_size),
        "sleep_seconds": float(sleep_seconds),
        "max_rounds": None if max_rounds is None else int(max_rounds),
    }
    print(f"📌 [bid_thumbnail] summary: {result}", flush=True)
    return result



async def dedupe_bid_thumbnail_t_update4_to5_batched(
    batch_groups: int = 1000,
    sleep_seconds: float = 0.0,
    max_rounds: Optional[int] = None,
) -> Dict[str, Any]:
    """
    处理 bid_thumbnail.t_update=4 的去重与胜出标记：
    - 按 file_unique_id 分组
    - 每组挑 winner：confirm_status 最大；若同分则 bid_thumbnail_id 最大
    - winner -> t_update=5；同组其他仍为 4 的 -> t_update=0

    特性：
    - 分批以“file_unique_id 分组”为单位处理，避免一次性锁表过久（MyISAM 表锁更敏感）
    - 每批打印进度
    - Ctrl+C 可中断：已提交的批次不回滚，只停止后续批次
    """

    await MySQLPool.init_pool()
    await MySQLPool.ensure_pool()

    rounds = 0
    total_groups = 0
    total_winners_set_5 = 0
    total_losers_set_0 = 0

    last_uid = ""  # 用于分页：file_unique_id > last_uid（按字典序）
    print(
        f"🚀 [bid_thumbnail] start t_update=4 dedupe: batch_groups={batch_groups}, sleep={sleep_seconds}, max_rounds={max_rounds}",
        flush=True,
    )

    try:
        while True:
            rounds += 1
            if max_rounds is not None and rounds > int(max_rounds):
                print(f"🛑 Reached max_rounds={max_rounds}. Stop.", flush=True)
                break

            # 1) 取一批 file_unique_id（仅限 t_update=4）
            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                await cur.execute(
                    """
                    SELECT file_unique_id
                    FROM bid_thumbnail
                    WHERE t_update = 4
                      AND file_unique_id > %s
                    GROUP BY file_unique_id
                    ORDER BY file_unique_id ASC
                    LIMIT %s
                    """,
                    (last_uid, int(batch_groups)),
                )
                uid_rows = await cur.fetchall()
            finally:
                await MySQLPool.release(conn, cur)

            if not uid_rows:
                print("🎯 [bid_thumbnail] no more t_update=4 groups. Done.", flush=True)
                break

            uids: List[str] = [r["file_unique_id"] for r in uid_rows if r.get("file_unique_id")]
            if not uids:
                break

            last_uid = uids[-1]
            total_groups += len(uids)

            # 2) 本批在同一连接内：建临时表 -> 算 winners -> 两步 update
            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                await conn.begin()

                # 临时表：存本批每个 file_unique_id 的 winner_id（bid_thumbnail_id）
                await cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_bt_winners")
                await cur.execute(
                    """
                    CREATE TEMPORARY TABLE tmp_bt_winners (
                        file_unique_id VARCHAR(50) NOT NULL,
                        winner_id INT UNSIGNED NOT NULL,
                        PRIMARY KEY (file_unique_id),
                        KEY idx_winner_id (winner_id)
                    ) ENGINE=MEMORY
                    """
                )

                # 以 IN 方式限定本批 file_unique_id
                placeholders = ",".join(["%s"] * len(uids))

                # 计算 winner：
                # - 先找每组 max(confirm_status)
                # - 再在 confirm_status=max 的候选里取 max(bid_thumbnail_id)
                sql_insert_winners = f"""
                    INSERT INTO tmp_bt_winners (file_unique_id, winner_id)
                    SELECT x.file_unique_id, MAX(bt.bid_thumbnail_id) AS winner_id
                    FROM (
                        SELECT file_unique_id, MAX(confirm_status) AS max_cs
                        FROM bid_thumbnail
                        WHERE t_update = 4
                          AND file_unique_id IN ({placeholders})
                        GROUP BY file_unique_id
                    ) x
                    JOIN bid_thumbnail bt
                      ON bt.file_unique_id = x.file_unique_id
                     AND bt.confirm_status = x.max_cs
                     AND bt.t_update = 4
                    GROUP BY x.file_unique_id
                """
                await cur.execute(sql_insert_winners, tuple(uids))

                # 2.1 winners -> t_update=5
                sql_set_winner_5 = """
                    UPDATE bid_thumbnail bt
                    JOIN tmp_bt_winners w
                      ON w.winner_id = bt.bid_thumbnail_id
                    SET bt.t_update = 5
                    WHERE bt.t_update = 4
                """
                await cur.execute(sql_set_winner_5)
                winners_set_5 = cur.rowcount or 0

                # 2.2 同组其余仍为 t_update=4 的 -> t_update=0
                # 只处理本批 uids（避免波及下一批）
                sql_set_loser_0 = f"""
                    UPDATE bid_thumbnail bt
                    LEFT JOIN tmp_bt_winners w
                      ON w.file_unique_id = bt.file_unique_id
                     AND w.winner_id = bt.bid_thumbnail_id
                    SET bt.t_update = 0
                    WHERE bt.t_update = 4
                      AND bt.file_unique_id IN ({placeholders})
                      AND w.winner_id IS NULL
                """
                await cur.execute(sql_set_loser_0, tuple(uids))
                losers_set_0 = cur.rowcount or 0

                await conn.commit()

            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                await MySQLPool.release(conn, cur)

            total_winners_set_5 += winners_set_5
            total_losers_set_0 += losers_set_0

            print(
                f"✅ [bid_thumbnail] round={rounds} groups={len(uids)} "
                f"winners_to_5={winners_set_5} losers_to_0={losers_set_0} "
                f"grand_groups={total_groups} grand_winners={total_winners_set_5} grand_losers={total_losers_set_0}",
                flush=True,
            )

            if sleep_seconds and sleep_seconds > 0:
                await asyncio.sleep(float(sleep_seconds))

    except KeyboardInterrupt:
        print(
            f"⛔ [bid_thumbnail] interrupted by user. rounds={rounds} "
            f"groups={total_groups} winners_to_5={total_winners_set_5} losers_to_0={total_losers_set_0}",
            flush=True,
        )

    result = {
        "rounds": rounds,
        "groups_processed": total_groups,
        "winners_set_to_5": total_winners_set_5,
        "losers_set_to_0": total_losers_set_0,
        "batch_groups": int(batch_groups),
        "sleep_seconds": float(sleep_seconds),
        "max_rounds": None if max_rounds is None else int(max_rounds),
    }
    print(f"📌 [bid_thumbnail] summary: {result}", flush=True)
    return result





async def apply_thumb_from_bid_thumbnail_t5_batched(
    batch_size: int = 500,
    sleep_seconds: float = 0.0,
    max_rounds: Optional[int] = None,
) -> Dict[str, Any]:
    """
    对 bid_thumbnail.t_update=5 执行对齐：
    - sc.source_id = bt.file_unique_id
    - 若 bt.thumb_file_unique_id == sc.thumb_file_unique_id:
        bt.t_update = 1
      否则:
        bt.t_update = 6
        sc.thumb_file_unique_id = bt.thumb_file_unique_id
        sora_media.thumb_file_id = NULL WHERE sora_media.content_id = sc.id

    说明：
    - bid_thumbnail(MyISAM) 无事务；sora_content/sora_media(InnoDB) 有事务
    - 本实现以“小批次 + InnoDB 事务”降低不一致窗口
    """
    await MySQLPool.init_pool()
    await MySQLPool.ensure_pool()

    rounds = 0
    total_scanned = 0
    total_bt_to_1 = 0
    total_bt_to_6 = 0
    total_sc_thumb_updated = 0
    total_sm_thumb_nulled = 0

    print(
        "[t5->(1/6)] start: batch_size=%s sleep=%s max_rounds=%s"
        % (batch_size, sleep_seconds, max_rounds),
        flush=True,
    )

    try:
        while True:
            rounds += 1
            if max_rounds is not None and rounds > int(max_rounds):
                print("🛑 Reached max_rounds=%s. Stop." % max_rounds, flush=True)
                break

            # 1) 拉一批需要处理的记录
            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                await cur.execute(
                    """
                    SELECT
                        bt.bid_thumbnail_id AS bt_id,
                        bt.file_unique_id AS file_unique_id,
                        bt.thumb_file_unique_id AS bt_thumb_uid,
                        sc.id AS content_id,
                        sc.thumb_file_unique_id AS sc_thumb_uid
                    FROM bid_thumbnail bt
                    JOIN sora_content sc
                      ON sc.source_id = bt.file_unique_id
                    WHERE bt.t_update = 5
                    ORDER BY bt.bid_thumbnail_id ASC
                    LIMIT %s
                    """,
                    (int(batch_size),),
                )
                rows = await cur.fetchall()
            finally:
                await MySQLPool.release(conn, cur)

            if not rows:
                print("🎯 [t5->(1/6)] no more rows. Done.", flush=True)
                break

            total_scanned += len(rows)

            equal_bt_ids: List[int] = []
            # (bt_id, content_id, new_thumb_uid)
            mismatch_items: List[Tuple[int, int, Optional[str]]] = []

            for r in rows:
                bt_id = int(r["bt_id"])
                content_id = int(r["content_id"])
                bt_thumb_uid = r.get("bt_thumb_uid")  # Optional[str]
                sc_thumb_uid = r.get("sc_thumb_uid")  # Optional[str]

                if bt_thumb_uid == sc_thumb_uid:
                    equal_bt_ids.append(bt_id)
                else:
                    mismatch_items.append((bt_id, content_id, bt_thumb_uid))

            # 2) 执行更新
            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                # 2.1 相等：bt.t_update = 1
                bt_to_1 = 0
                if equal_bt_ids:
                    placeholders = ",".join(["%s"] * len(equal_bt_ids))
                    await cur.execute(
                        """
                        UPDATE bid_thumbnail
                        SET t_update = 1
                        WHERE t_update = 5
                          AND bid_thumbnail_id IN (%s)
                        """ % placeholders,
                        tuple(equal_bt_ids),
                    )
                    bt_to_1 = cur.rowcount or 0

                # 2.2 不等：bt.t_update = 6（MyISAM）
                bt_to_6 = 0
                sc_updated = 0
                sm_nulled = 0

                if mismatch_items:
                    mismatch_bt_ids = [x[0] for x in mismatch_items]
                    mismatch_content_ids = [x[1] for x in mismatch_items]

                    placeholders = ",".join(["%s"] * len(mismatch_bt_ids))
                    await cur.execute(
                        """
                        UPDATE bid_thumbnail
                        SET t_update = 6
                        WHERE t_update = 5
                          AND bid_thumbnail_id IN (%s)
                        """ % placeholders,
                        tuple(mismatch_bt_ids),
                    )
                    bt_to_6 = cur.rowcount or 0

                    # 2.3 InnoDB 事务：更新 sora_content + sora_media
                    await conn.begin()
                    try:
                        # 2.3.1 更新 sora_content.thumb_file_unique_id
                        # executemany 的 rowcount 在不同驱动/版本下可能不可靠，所以这里以“执行成功”为主。
                        payload_sc = []
                        for _, content_id, new_thumb_uid in mismatch_items:
                            payload_sc.append((new_thumb_uid, int(content_id)))

                        await cur.executemany(
                            """
                            UPDATE sora_content
                            SET thumb_file_unique_id = %s
                            WHERE id = %s
                            """,
                            payload_sc,
                        )
                        sc_updated = cur.rowcount or 0

                        # 2.3.2 清空 sora_media.thumb_file_id（按 content_id 批量）
                        placeholders = ",".join(["%s"] * len(mismatch_content_ids))
                        await cur.execute(
                            """
                            UPDATE sora_media
                            SET thumb_file_id = NULL
                            WHERE content_id IN (%s)
                            """ % placeholders,
                            tuple(mismatch_content_ids),
                        )
                        sm_nulled = cur.rowcount or 0

                        await conn.commit()
                    except Exception:
                        try:
                            await conn.rollback()
                        except Exception:
                            pass
                        raise

                total_bt_to_1 += bt_to_1
                total_bt_to_6 += bt_to_6
                total_sc_thumb_updated += sc_updated
                total_sm_thumb_nulled += sm_nulled

            finally:
                await MySQLPool.release(conn, cur)

            print(
                "✅ [t5->(1/6)] round=%s scanned=%s "
                "bt_to_1=%s bt_to_6=%s sc_updated=%s sm_nulled=%s total_scanned=%s"
                % (
                    rounds,
                    len(rows),
                    bt_to_1,
                    bt_to_6,
                    total_sc_thumb_updated,
                    total_sm_thumb_nulled,
                    total_scanned,
                ),
                flush=True,
            )

            if sleep_seconds and sleep_seconds > 0:
                await asyncio.sleep(float(sleep_seconds))

    except KeyboardInterrupt:
        print(
            "⛔ [t5->(1/6)] interrupted. rounds=%s scanned=%s bt_to_1=%s bt_to_6=%s sc_updated=%s sm_nulled=%s"
            % (
                rounds,
                total_scanned,
                total_bt_to_1,
                total_bt_to_6,
                total_sc_thumb_updated,
                total_sm_thumb_nulled,
            ),
            flush=True,
        )

    result = {
        "rounds": rounds,
        "scanned": total_scanned,
        "bt_set_to_1": total_bt_to_1,
        "bt_set_to_6": total_bt_to_6,
        "sora_content_thumb_updated": total_sc_thumb_updated,
        "sora_media_thumb_file_id_nulled": total_sm_thumb_nulled,
        "batch_size": int(batch_size),
        "sleep_seconds": float(sleep_seconds),
        "max_rounds": None if max_rounds is None else int(max_rounds),
    }
    print("📌 [t5->(1/6)] summary: %s" % result, flush=True)
    return result



async def main():
    try:
        await sync()
    finally:
        # 先关 MySQL，再关 PG（顺序不是关键，但要确保都关）
        try:
            await MySQLPool.close()
        except Exception as e:
            print(f"⚠️ MySQLPool.close failed: {e}", flush=True)

        try:
            await PGPool.close()   # 你 lz_pgsql.PGPool 应该也有 close()/wait_closed()
        except Exception as e:
            print(f"⚠️ PGPool.close failed: {e}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
