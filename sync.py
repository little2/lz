import asyncio

from lz_pgsql import PGPool

# sync_mysql_pool.py
import os
import aiomysql
from typing import Optional, Tuple, Any

class SyncMySQLPool:
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

        host = os.getenv("MYSQL_HOST", "127.0.0.1")
        port = int(os.getenv("MYSQL_PORT", "3306"))
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        db = os.getenv("MYSQL_DB", "telebot")
        minsize = int(os.getenv("MYSQL_POOL_MIN", "1"))
        maxsize = int(os.getenv("MYSQL_POOL_MAX", "10"))
        charset = os.getenv("MYSQL_CHARSET", "utf8mb4")

        cls._pool = await aiomysql.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            minsize=minsize,
            maxsize=maxsize,
            autocommit=False,   # 你在 check_file_record 里有 begin/commit/rollback
            charset=charset,
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
# sync_mysql_pool.py
import os
import aiomysql
from typing import Optional, Tuple, Any

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
    # 1. 同步 / 修复 file_record
    while True:
        summary = await check_file_record(limit=100)
        if summary.get("checked", 0) == 0:
            break

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


async def main():
    # 初始化数据库连接
    # await asyncio.gather(
    #     MySQLPool.init_pool(),
    #     PGPool.init_pool(),
    # )

    # try:
    
    await sync()
    # finally:
    #     # 关闭数据库连接
    #     await PGPool.close()
    #     await MySQLPool.close()

if __name__ == "__main__":
    asyncio.run(main())
