from datetime import datetime
import time
import aiomysql
from ananbot_config import DB_CONFIG

from utils.lybase_utils import LYBase

class AnanBOTPool(LYBase):
    _pool = None
    _all_tags_grouped_cache = None
    _all_tags_grouped_cache_ts = 0
    _cache_ttl = 30  # 缓存有效时间（秒）



    @classmethod
    async def init_pool(cls):
        if cls._pool is None:
            cls._pool = await aiomysql.create_pool(**DB_CONFIG)
            print("✅ MySQL 连接池初始化完成")

    @classmethod
    async def get_conn_cursor(cls):
        if cls._pool is None:
            raise Exception("MySQL 连接池未初始化，请先调用 init_pool()")
        conn = await cls._pool.acquire()
        cursor = await conn.cursor(aiomysql.DictCursor)
        return conn, cursor

    @classmethod
    async def release(cls, conn, cursor):
        await cursor.close()
        cls._pool.release(conn)

    @classmethod
    async def is_media_published(cls, file_type, file_unique_id):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(f"SELECT kc_id FROM `{file_type}` WHERE file_unique_id=%s", (file_unique_id,))
            row = await cur.fetchone()
            return (row is not None, row["kc_id"] if row else None)
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def upsert_media(cls, file_type, data: dict):
        if file_type != "video":
            data.pop("duration", None)
        if file_type == "document":
            data.pop("width", None)
            data.pop("height", None)
        conn, cur = await cls.get_conn_cursor()
        try:
            keys = list(data.keys())
            placeholders = ', '.join(['%s'] * len(keys))
            columns = ', '.join(f"`{k}`" for k in keys)
            updates = ', '.join(f"`{k}`=VALUES(`{k}`)" for k in keys if k != "create_time")
            sql = f"""
                INSERT INTO `{file_type}` ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
            """
            await cur.execute(sql, [data[k] for k in keys])
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def insert_file_extension(cls, file_type, file_unique_id, file_id, bot_username, user_id):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """INSERT IGNORE INTO file_extension 
                   (file_type, file_unique_id, file_id, bot, user_id, create_time)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (file_type, file_unique_id, file_id, bot_username, user_id, datetime.now())
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def insert_sora_content_media(cls, file_unique_id, file_type, file_size, duration, user_id, file_id, bot_username):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """INSERT INTO sora_content 
                   (source_id, file_type, file_size, duration, owner_user_id)
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE 
                   file_type=VALUES(file_type), 
                   file_size=VALUES(file_size), 
                   duration=VALUES(duration), 
                   owner_user_id=VALUES(owner_user_id)""",
                (file_unique_id, file_type, file_size, duration, user_id)
            )
            await cur.execute("SELECT id FROM sora_content WHERE source_id=%s", (file_unique_id,))
            content_id = (await cur.fetchone())["id"]
            await cur.execute(
                """INSERT IGNORE INTO sora_media 
                   (content_id, source_bot_name, file_id)
                   VALUES (%s, %s, %s)""",
                (content_id, bot_username, file_id)
            )
            return content_id
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_product_thumb(cls, content_id: int, thumb_file_unique_id: str, thumb_file_id: str , bot_username: str):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "UPDATE sora_content SET thumb_file_unique_id = %s WHERE id = %s",
                (thumb_file_unique_id, content_id)
            )
            await cur.execute(
                "UPDATE sora_media SET thumb_file_id = %s WHERE content_id = %s and source_bot_name = %s",
                (thumb_file_id, content_id, bot_username)
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_product_price(cls, content_id: str, price: int):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "UPDATE product SET price=%s WHERE content_id=%s",
                (price, content_id)
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def create_product(cls, content_id, name, desc, price, file_type, user_id):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """INSERT INTO product 
                   (name, content, price, content_id, file_type, owner_user_id)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (name, desc, price, content_id, file_type, user_id)
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_existing_product(cls, content_id: str):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "SELECT id,price FROM product WHERE content_id = %s LIMIT 1",
                (content_id,)
            )
            row = await cur.fetchone()
            return {"id": row["id"], "price": row["price"]} if row else None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_preview_thumb_file_id(cls, bot_username: str, content_id: int):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT sm.thumb_file_id 
                FROM sora_media sm
                WHERE sm.source_bot_name = %s AND sm.content_id = %s
                LIMIT 1
                """,
                (bot_username, content_id)
            )
            row = await cur.fetchone()
            return row["thumb_file_id"] if row else None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def insert_collection_item(cls, content_id: int, member_content_id: int, file_unique_id: str, file_type: str, position: int = 0):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                INSERT IGNORE INTO collection_items (
                    content_id, member_content_id, file_unique_id, file_type, position, created_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (content_id, member_content_id, file_unique_id, file_type, position)
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_collect_list(cls, content_id: int, bot_name: str):
        sql = """
            SELECT s.source_id, c.file_type, s.content, s.file_size, s.duration,
                   m.source_bot_name, m.thumb_file_id, m.file_id
            FROM collection_items c
            LEFT JOIN sora_content s ON c.member_content_id = s.id
            LEFT JOIN sora_media m ON c.member_content_id = m.content_id AND m.source_bot_name = %s
            WHERE c.content_id = %s
            ORDER BY c.file_type
        """
        params = (bot_name, content_id)
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(sql, params)
            return await cur.fetchall()
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def find_rebate_receiver_id(cls, source_id: str, tag: str):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT receiver_id,receiver_fee,transaction_timestamp
                FROM transaction
                WHERE transaction_type = 'rebate'
                  AND transaction_description = %s
                  AND memo = %s
                """,
                (source_id, tag)
            )
            row = await cur.fetchone()
            return {
                'receiver_id': row["receiver_id"],
                'receiver_fee': row["receiver_fee"],
                'transaction_timestamp': row["transaction_timestamp"]
            } if row else None
            
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def delete_file_tag(cls, source_id: str, tag: str) -> int:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                DELETE FROM file_tag
                WHERE tag = %s AND file_unique_id = %s
                """,
                (tag, source_id)
            )
            return cur.rowcount
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def get_all_tag_types(cls):
        async with cls._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT type_code, type_cn FROM tag_type ")
                return await cur.fetchall()



    @classmethod
    async def get_tags_by_type(cls, type_code: str):
        async with cls._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("""
                    SELECT tag, tag_cn 
                    FROM tag 
                    WHERE tag_type = %s 
                    ORDER BY tag_cn ASC
                """, (type_code,))
                return await cur.fetchall()

    @classmethod
    async def get_file_unique_id_by_content_id(cls, content_id: str) -> str:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT source_id as file_unique_id 
                    FROM sora_content 
                    WHERE id = %s
                """, (content_id,))
                row = await cur.fetchone()
                return row[0] if row else ""


    @classmethod
    async def get_tags_for_file(cls, file_unique_id: str) -> list[str]:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT tag FROM file_tag 
                    WHERE file_unique_id = %s
                """, (file_unique_id,))
                rows = await cur.fetchall()
                return [r[0] for r in rows]

    @classmethod
    async def is_tag_exist(cls, file_unique_id: str, tag: str) -> bool:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT 1 FROM file_tag 
                    WHERE file_unique_id = %s AND tag = %s
                    LIMIT 1
                """, (file_unique_id, tag))
                return bool(await cur.fetchone())

    @classmethod
    async def add_tag(cls, file_unique_id: str, tag: str) -> bool:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO file_tag (file_unique_id, tag, count) 
                    VALUES (%s, %s, 0)
                """, (file_unique_id, tag))
                await conn.commit()
                return True

    @classmethod
    async def remove_tag(cls, file_unique_id: str, tag: str) -> bool:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    DELETE FROM file_tag 
                    WHERE file_unique_id = %s AND tag = %s
                """, (file_unique_id, tag))
                await conn.commit()
                return True

    @classmethod
    async def get_tag_info(cls, tag: str) -> dict:
        async with cls._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("""
                    SELECT tag, tag_type 
                    FROM tag 
                    WHERE tag = %s
                """, (tag,))
                return await cur.fetchone()

    @classmethod
    async def get_all_tags_grouped(cls) -> dict:
        """
        返回结构:
        {
        "style": [ {tag: "...", tag_cn: "..."}, ... ],
        "mood": [ ... ],
        ...
        }
        """
        now = time.time()
        if cls._all_tags_grouped_cache and now - cls._all_tags_grouped_cache_ts < cls._cache_ttl:
            return cls._all_tags_grouped_cache  # ✅ 命中缓存


        async with cls._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT tag, tag_cn, tag_type FROM tag")
                rows = await cur.fetchall()
        
        grouped = {}
        for row in rows:
            grouped.setdefault(row["tag_type"], []).append(row)
        # ✅ 更新缓存
        cls._all_tags_grouped_cache = grouped
        cls._all_tags_grouped_cache_ts = now
        
        return grouped
