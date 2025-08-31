from datetime import datetime
import time
import aiomysql
from ananbot_config import DB_CONFIG
from utils.lybase_utils import LYBase
from utils.string_utils import LZString
from utils.prof import SegTimer

class AnanBOTPool(LYBase):
    _pool = None
    _all_tags_grouped_cache = None
    _all_tags_grouped_cache_ts = 0
    _cache_ttl = 30  # 缓存有效时间（秒）
    _all_tags_types_cache = None
    _all_tags_types_cache_ts = 0


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
        try:
            await cursor.close()
        except Exception as e:
            print(f"⚠️ 关闭 cursor 时失败: {e}")

        try:
            # ✅ 关键防呆：避免重复释放或非法连接释放
            if hasattr(cls._pool, "_used") and conn in cls._pool._used:
                cls._pool.release(conn)
        except Exception as e:
            print(f"⚠️ 释放连接失败: {e}")



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
                """
                INSERT INTO sora_content
                    (source_id, file_type, file_size, duration, owner_user_id, stage)
                VALUES
                    (%s, %s, %s, %s, %s, 'pending')
                ON DUPLICATE KEY UPDATE
                    file_type     = VALUES(file_type),
                    file_size     = VALUES(file_size),
                    duration      = VALUES(duration),
                    owner_user_id = VALUES(owner_user_id),
                    stage         = 'pending'
                """,
                (file_unique_id, file_type, file_size, duration, user_id)
            )
            await cur.execute("SELECT * FROM sora_content WHERE source_id=%s LIMIT 1", (file_unique_id,))
            row = (await cur.fetchone())
            content_id = row["id"]
            await cur.execute(
                """
                INSERT INTO sora_media
                    (content_id, source_bot_name, file_id)
                VALUES
                    (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    file_id = VALUES(file_id)
                """,
                (content_id, bot_username, file_id)
            )
            return row
        finally:
            await cls.release(conn, cur)

    @classmethod    
    async def upsert_product_thumb(cls, content_id: int, thumb_file_unique_id: str, thumb_file_id: str, bot_username: str):
        """
        更新縮圖資訊：
        - sora_content: 更新 thumb_file_unique_id（僅當該 content 存在）
        - sora_media: 依 content_id + source_bot_name 做 UPSERT，更新 thumb_file_id

        回傳：dict，包含各步驟受影響筆數/狀態
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # 1) 嘗試更新 sora_content（若該 content_id 不存在則不會有影響）
            await cur.execute(
                """
                UPDATE sora_content
                SET thumb_file_unique_id = %s
                WHERE id = %s
                """,
                (thumb_file_unique_id, content_id)
            )
            content_rows = cur.rowcount  # 受影響筆數（0 代表該 content_id 不存在）

            # 2) 對 sora_media 做 UPSERT（不存在則插入，存在則更新 thumb_file_id）
            # 依賴唯一鍵 uniq_content_bot (content_id, source_bot_name)
            await cur.execute(
                """
                INSERT INTO sora_media (content_id, source_bot_name, thumb_file_id)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    thumb_file_id = VALUES(thumb_file_id)
                """,
                (content_id, bot_username, thumb_file_id)
            )
            # 在 MariaDB/MySQL 中，ON DUPLICATE 觸發更新時 rowcount 會是 2（1 插入、2 更新的慣例不完全一致，依版本而異）
            media_rows = cur.rowcount

            await conn.commit()

            return {
                "sora_content_updated_rows": content_rows,
                "sora_media_upsert_rowcount": media_rows
            }

        except Exception as e:
            try:
                await conn.rollback()
            except:
                pass
            # 讓上層知道發生了什麼錯
            raise
        finally:
            await cls.release(conn, cur)




    @classmethod
    async def update_bid_thumbnail(cls, file_unique_id: str, thumb_file_unique_id: str, thumb_file_id: str , bot_username: str):
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = (
                "INSERT INTO bid_thumbnail "
                "(file_unique_id, thumb_file_unique_id, bot_name, file_id, confirm_status) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE "
                "bot_name=VALUES(bot_name), "
                "file_id=VALUES(file_id), "
                "confirm_status=VALUES(confirm_status)"
            )   
            await cur.execute(
                sql,
                (
                    file_unique_id,
                    thumb_file_unique_id,
                    bot_username,
                    thumb_file_id,
                    10
                )
            )
           
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_product_price(cls, content_id: str, price: int):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "UPDATE product SET price=%s, stage='pending' WHERE content_id=%s",
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
                   (name, content, price, content_id, file_type, owner_user_id,stage)
                   VALUES (%s, %s, %s, %s, %s, %s, "pending")""",
                (name, desc, price, content_id, file_type, user_id)
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_existing_product(cls, content_id: str):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "SELECT id,price,content,file_type,bid_status,review_status,anonymous_mode,owner_user_id FROM product WHERE content_id = %s LIMIT 1",
                (content_id,)
            )
            row = await cur.fetchone()
            return {"id": row["id"], "price": row["price"],"content": row['content'],"file_type":row['file_type'],"anonymous_mode":row['anonymous_mode'],"review_status":row['review_status'],"owner_user_id":row['owner_user_id']} if row else None
        finally:
            await cls.release(conn, cur)

    #要和 lz_db.py 作整合
    @classmethod
    async def search_sora_content_by_id(cls, content_id: int,bot_username: str):
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute('''
                SELECT s.id, s.source_id, s.file_type, s.content, s.file_size, s.duration, s.tag,
                    s.thumb_file_unique_id,
                    m.file_id AS m_file_id, m.thumb_file_id AS m_thumb_file_id,
                    p.price as fee, p.file_type as product_type, p.owner_user_id, p.purchase_condition, p.review_status, p.anonymous_mode, p.id as product_id,
                    g.guild_id, g.guild_keyword, g.guild_resource_chat_id, g.guild_resource_thread_id
                FROM sora_content s
                LEFT JOIN sora_media m ON s.id = m.content_id AND m.source_bot_name = %s
                LEFT JOIN product p ON s.id = p.content_id
                LEFT JOIN guild g ON p.guild_id = g.guild_id
                WHERE s.id = %s
                '''
            , (bot_username, content_id))
            row = await cursor.fetchone()

            # 没查到就返回 None，交由上层处理默认值
            if not row:
                return None

            # 2) 先拿 m 表的缓存
            file_id = row.get("m_file_id")
            thumb_file_id = row.get("m_thumb_file_id")

        

            if(file_id == None and thumb_file_id == None):
                await cursor.execute(
                    """
                    INSERT INTO sora_media (content_id, source_bot_name, thumb_file_id, file_id)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        thumb_file_id = VALUES(thumb_file_id),
                        file_id      = VALUES(file_id)
                    """,
                    (content_id, bot_username, '', '')
                )


            # 3) 需要补的话，再去 file_extension 查
            need_lookup_keys = []
            if not file_id and row.get("source_id"):
                need_lookup_keys.append(row["source_id"])
            if not thumb_file_id and row.get("thumb_file_unique_id"):
                # 避免与 source_id 重复
                if row["thumb_file_unique_id"] not in need_lookup_keys:
                    need_lookup_keys.append(row["thumb_file_unique_id"])

            if need_lookup_keys:
                # 动态 IN 占位
                placeholders = ",".join(["%s"] * len(need_lookup_keys))
                params = [*need_lookup_keys, bot_username]

                await cursor.execute(
                    f'''
                    SELECT file_unique_id, file_id
                    FROM file_extension
                    WHERE file_unique_id IN ({placeholders})
                      AND bot = %s
                    ''',
                    params
                )
                extension_rows = await cursor.fetchall()
                ext_map = {r["file_unique_id"]: r["file_id"] for r in extension_rows}

                updated = False
                if not file_id and row.get("source_id") in ext_map:
                    file_id = ext_map[row["source_id"]]
                    row["m_file_id"] = file_id
                    updated = True

                if not thumb_file_id and row.get("thumb_file_unique_id") in ext_map:
                    thumb_file_id = ext_map[row["thumb_file_unique_id"]]
                    row["m_thumb_file_id"] = thumb_file_id
                    updated = True

                # 4) 如果补到了，就回写 sora_media（UPSERT）
                if updated:
                    await cursor.execute(
                        """
                        INSERT INTO sora_media (content_id, source_bot_name, thumb_file_id, file_id)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                          thumb_file_id = VALUES(thumb_file_id),
                          file_id      = VALUES(file_id)
                        """,
                        (content_id, bot_username, thumb_file_id, file_id)
                    )
                    # 如果你这边用手动事务，可考虑在外层统一提交

            

            return row
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
            row = None
        finally:
            await cls.release(conn, cursor)

        if not row:
            print("❌ 没有找到匹配记录 file_id")
            return None

    @classmethod
    async def get_preview_thumb_file_id(cls, bot_username: str, content_id: int):
        print(f"▶️ 正在获取缩略图 file_id for content_id: {content_id} by bot: {bot_username}", flush=True)
        conn, cur = await cls.get_conn_cursor()
        try:
            # 1. 查 sora_media
            await cur.execute(
                "SELECT thumb_file_id FROM sora_media WHERE source_bot_name = %s AND content_id = %s LIMIT 1",
                (bot_username, content_id)
            )
            row = await cur.fetchone()
            if row and row["thumb_file_id"]:
                return row["thumb_file_id"], None

            # 2. 查 sora_content
            print(f"...❌ sora_media 目前不存在  for content_id: {content_id}")
            await cur.execute(
                "SELECT thumb_file_unique_id FROM sora_content WHERE id = %s",
                (content_id,)
            )
            row = await cur.fetchone()
            if not row or not row["thumb_file_unique_id"]:
                return None, None
            thumb_uid = row["thumb_file_unique_id"]

            # 3. 查 file_extension
           
            await cur.execute(
                "SELECT file_id, bot FROM file_extension WHERE file_unique_id = %s AND bot = %s",
                (thumb_uid,bot_username)
            )
            row = await cur.fetchone()
            if row:
                thumb_file_id = row["file_id"]
                await cur.execute(
                    """
                    INSERT INTO sora_media (content_id, source_bot_name, thumb_file_id)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE thumb_file_id = VALUES(thumb_file_id)
                    """,
                    (content_id, bot_username, thumb_file_id)
                )
                print(f"...✅ 有缩略图，正在更新: {thumb_file_id} for content_id: {content_id}")
                return thumb_file_id, thumb_uid
            else:
                print(f"...❌ 其他机器人有缩略图，通知更新 file_id for thumb_uid: {thumb_uid}")
                return None,thumb_uid
                

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


        now = time.time()
        if cls._all_tags_types_cache and now - cls._all_tags_types_cache_ts < cls._cache_ttl:
            return cls._all_tags_types_cache  # ✅ 命中缓存


        async with cls._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT type_code, type_cn FROM tag_type WHERE type_code NOT IN ('xiaoliu','system','serial','gallery','gallery_set') ORDER BY FIELD(type_code, 'age', 'eth', 'face', 'feedback','nudity','par','act','pro','fetish','att','position','hardcore')")
                # return await cur.fetchall()
                rows = await cur.fetchall()
        
       
        
        # ✅ 更新缓存
        cls._all_tags_types_cache = rows
        cls._all_tags_types_cache_ts = now
        
        return rows




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
    async def get_content_id_by_file_unique_id(cls, file_unique_id: str) -> str:
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id,source_id as file_unique_id 
                    FROM sora_content 
                    WHERE source_id = %s
                """, (file_unique_id,))
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
                await cur.execute("SELECT tag, tag_cn, tag_type FROM tag ")
                rows = await cur.fetchall()
        
        grouped = {}
        for row in rows:
            grouped.setdefault(row["tag_type"], []).append(row)
        # ✅ 更新缓存
        cls._all_tags_grouped_cache = grouped
        cls._all_tags_grouped_cache_ts = now
        
        return grouped

    @classmethod
    async def sync_file_tags(cls, file_unique_id: str, selected_tags: set[str], *, actor_user_id: int | None = None) -> dict:
        """
        将 FSM 里最终选中的标签一次性落库：
        - 新增：INSERT ... ON DUPLICATE KEY UPDATE（count 默认为 1，可按需改成计数逻辑）
        - 移除：DELETE ... WHERE tag IN (...)
        返回 {added: n, removed: m, unchanged: k}
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # 取当前库里的标签
            await cur.execute(
                "SELECT tag FROM file_tag WHERE file_unique_id=%s",
                (file_unique_id,)
            )
            existing = {row["tag"] for row in await cur.fetchall()}

            to_add = list(selected_tags - existing)
            to_del = list(existing - selected_tags)
            unchanged = len(existing & selected_tags)

            # 批量新增
            if to_add:
                rows = [(file_unique_id, t, 1) for t in to_add]
                await cur.executemany(
                    """
                    INSERT INTO file_tag (file_unique_id, tag, `count`)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE `count`=VALUES(`count`)
                    """,
                    rows
                )

            # 批量删除
            if to_del:
                # 动态 IN 列表
                ph = ",".join(["%s"] * len(to_del))
                await cur.execute(
                    f"DELETE FROM file_tag WHERE file_unique_id=%s AND tag IN ({ph})",
                    (file_unique_id, *to_del)
                )

            await conn.commit()
            return {"added": len(to_add), "removed": len(to_del), "unchanged": unchanged}
        except Exception:
            await conn.rollback()
            raise
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def get_tag_cn_batch(cls, tags: list[str]) -> dict[str, str]:
        """
        批量取 tag -> tag_cn 的映射；若没有 tag_cn，则回退 tag 本身。
        返回: {tag: tag_cn_or_tag}
        """
        if not tags:
            return {}
        conn, cur = await cls.get_conn_cursor()
        try:
            ph = ",".join(["%s"] * len(tags))
            await cur.execute(
                f"SELECT tag, COALESCE(tag_cn, tag) AS tag_cn FROM tag WHERE tag IN ({ph})",
                tuple(tags)
            )
            rows = await cur.fetchall()
            mapping = {r["tag"]: r["tag_cn"] for r in rows}
            # 没查到的，用自身回填
            for t in tags:
                mapping.setdefault(t, t)
            return mapping
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_sora_content_tag_and_stage(cls, content_id: int, tag_str: str):
        """
        更新 sora_content.tag 与 stage='pending'
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            print(f"▶️ 正在更新 sora_content.tag={tag_str} for content_id: {content_id}", flush=True)
            await cur.execute(
                "UPDATE sora_content SET tag=%s, stage='pending' WHERE id=%s",
                (tag_str, content_id)
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            await cls.release(conn, cur)



    @classmethod
    async def update_product_content(cls, content_id: int, content: str, user_id: int = 0, overwrite: int = 0):
        # ✅ 用更准确的名称
        timer = SegTimer("update_product_content", content_id=content_id, overwrite=int(overwrite))
        # 允许的表名白名单（标识符不能用占位符，只能先验证再拼接）
        FT_MAP = {
            "d": "document", "document": "document",
            "v": "video",    "video": "video",
            "p": "photo",    "photo": "photo",
        }

        try:
            async with cls._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    timer.lap("acquire_conn_and_cursor")

                    # --- 覆写模式：需要先从 sora_content 反查源表与原始 caption ---
                    if int(overwrite) == 1:
                        # 1) 取 sora_content 基本信息
                        await cur.execute(
                            "SELECT source_id, file_type FROM sora_content WHERE id = %s LIMIT 1",
                            (content_id,)
                        )
                        row_sora_content = await cur.fetchone()
                        timer.lap("fetch_sora_content")

                        if row_sora_content:
                            src_id = row_sora_content[0]
                            file_type = row_sora_content[1]
                            ft_norm = FT_MAP.get(file_type)

                            if not ft_norm:
                                print(f"[update_product_content] Unsupported file_type={file_type} for id={content_id}", flush=True)
                            else:
                                # 2) 取对应媒体表 caption（表名用白名单 + 反引号）
                                await cur.execute(
                                    f"SELECT caption FROM `{ft_norm}` WHERE file_unique_id = %s LIMIT 1",
                                    (src_id,)
                                )
                                origin_content_row = await cur.fetchone()
                                origin_content = origin_content_row[0] if origin_content_row else ""
                                timer.lap("fetch_origin_caption")

                                # 3) 备份一份到 material_caption（只插入一次）
                                await cur.execute(
                                    "INSERT INTO `material_caption` (`file_unique_id`, `caption`, `user_id`) VALUES (%s, %s, %s)",
                                    (src_id, origin_content, user_id)
                                )
                                timer.lap("insert_material_caption")

                                # 4) 更新媒体表 caption 与 kc_id / kc_status
                                if ft_norm in ("video"):
                                    await cur.execute(
                                        f"UPDATE `{ft_norm}` "
                                        f"SET caption = %s, kc_id = %s, update_time = NOW(), kc_status = 'pending' "
                                        f"WHERE file_unique_id = %s",
                                        (content, content_id, src_id)
                                    )
                                else:
                                    await cur.execute(
                                        f"UPDATE `{ft_norm}` "
                                        f"SET caption = %s, kc_id = %s, kc_status = 'pending' "
                                        f"WHERE file_unique_id = %s",
                                        (content, content_id, src_id)
                                    )
                                timer.lap("update_media_table")

                    # --- 不论是否 overwrite，都需要同步 product / sora_content ---
                    await cur.execute(
                        "UPDATE product SET content = %s, stage='pending' WHERE content_id = %s",
                        (content, content_id)
                    )
                    timer.lap("update_product")

                    await cur.execute(
                        "UPDATE sora_content SET content = %s, stage='pending' WHERE id = %s",
                        (content, content_id)
                    )
                    timer.lap("update_sora_content")

                await conn.commit()
                timer.lap("commit")
        except Exception as e:
            # 失败时尝试回滚
            try:
                await conn.rollback()
            except Exception:
                pass
            print(f"[update_product_content] ERROR: {e}", flush=True)
            raise
        finally:
            timer.end()


    @classmethod
    async def update_product_file_type(cls, content_id: int, file_type: str):
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE product SET file_type = %s, stage='pending' WHERE content_id = %s",
                    (file_type, content_id)
                )
                await conn.commit()

    #search_sora_content_by_id
    @classmethod
    async def get_sora_content_by_id(cls, content_id: int):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute("SELECT * FROM sora_content WHERE id = %s", (content_id,))
            return await cur.fetchone()
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_bid_thumbnail_by_source_id(cls, source_id: str):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute("SELECT b.thumb_file_unique_id, f.file_id as thumb_file_id, b.bot_name FROM bid_thumbnail b LEFT JOIN file_extension f ON f.file_unique_id = b.thumb_file_unique_id WHERE b.file_unique_id = %s and b.confirm_status >= 10;", (source_id,))
            # await cur.execute("SELECT thumb_file_unique_id FROM bid_thumbnail LEFT JOIN file_extension f ON f.file_unique_id = bid_thumbnail.file_unique_id WHERE bid_thumbnail.file_unique_id = %s", (source_id,))
            return await cur.fetchall()
        finally:
            await cls.release(conn, cur)

    

    @classmethod
    async def get_product_review_status(cls, content_id: int) -> int | None:
        """
        读取当前 bid_status（可选的幂等检查用）。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "SELECT review_status FROM product WHERE content_id=%s",
                (content_id,)
            )
            row = await cur.fetchone()
            return row["review_status"] if row else None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def set_product_review_status(cls, content_id: int, status: int = 1) -> int:
        """
        将 product.review_status 设为指定值（默认 1），返回受影响行数。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                UPDATE product
                   SET review_status=%s,
                       updated_at=NOW(), stage='pending'
                 WHERE content_id=%s
                """,
                (status, content_id)
            )
            affected = cur.rowcount
            await conn.commit()
            return affected
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def refine_product_content(cls, content_id: int) -> bool:
        """
        精炼产品内容：合并 sora_content.content 与对应媒体表 caption，
        去重/清洗后回写两边。
        返回 True 表示成功（哪怕没有内容也算成功），False 表示未找到记录。
        """
        # 允许的表名白名单（标识符不能用占位符，只能先验证再拼接）
        FT_MAP = {
            "d": "document", "document": "document",
            "v": "video",    "video": "video",
            "p": "photo",    "photo": "photo",
        }

        conn, cur = await cls.get_conn_cursor()
        try:
            # 1) 取 sora_content 基本信息
            await cur.execute(
                "SELECT content, source_id, file_type FROM sora_content WHERE id = %s LIMIT 1",
                (content_id,)
            )
            row = await cur.fetchone()
            if not row:
                # 没有这条 content_id
                return False

            src_id = row["source_id"]
            ft_norm = FT_MAP.get(row["file_type"])
            if not ft_norm:
                # 未知类型，保守不动
                print(f"[refine_product_content] Unsupported file_type={row['file_type']!r} for id={content_id}")
                return False

            # 2) 取对应媒体表 caption（表名用白名单 + 反引号）
            await cur.execute(
                f"SELECT caption FROM `{ft_norm}` WHERE file_unique_id = %s LIMIT 1",
                (src_id,)
            )
            row2 = await cur.fetchone()

            # 3) 合并文本
            content_parts = []
            if row and row.get("content"):
                content_parts.append(row["content"])
            if row2 and row2.get("caption"):
                content_parts.append(row2["caption"])

            merged = "\n".join(p for p in content_parts if p)  # 避免 None
            if not merged.strip():
                # 没有可精炼的内容，也算流程成功
                return True

            # 4) 精炼：去重中文句子 + 清洗
            cleaned = LZString.dedupe_cn_sentences(merged)
            refined = LZString.clean_text(cleaned)

            # 5) 回写 sora_content 与媒体表
            await cur.execute(
                "UPDATE sora_content SET content = %s, stage='pending' WHERE id = %s",
                (refined, content_id)
            )
            if( ft_norm == 'video' or ft_norm == 'document'):
                await cur.execute(
                    f"UPDATE `{ft_norm}` SET caption = %s, kc_id = %s, update_time = NOW(), kc_status = 'pending' WHERE file_unique_id = %s",
                    (refined, content_id, src_id)
                )
            else:
                await cur.execute(
                    f"UPDATE `{ft_norm}` SET caption = %s, kc_id = %s, kc_status = 'pending' WHERE file_unique_id = %s",
                    (refined, content_id, src_id)
                )

            await cur.execute(
                "UPDATE product SET content = %s, stage='pending' WHERE content_id = %s",
                (refined, content_id)
            )


            await conn.commit()
            return True

        except Exception as e:
            await conn.rollback()
            print(f"[refine_product_content] ERROR id={content_id}: {e}")
            raise
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_material_caption(cls, content_id: int, content) -> bool:
        """
        更新所有的产品内容：合并 sora_content.content 与对应媒体表 caption，
        去重/清洗后回写两边。
        返回 True 表示成功（哪怕没有内容也算成功），False 表示未找到记录。
        """
        # 允许的表名白名单（标识符不能用占位符，只能先验证再拼接）
        FT_MAP = {
            "d": "document", "document": "document",
            "v": "video",    "video": "video",
            "p": "photo",    "photo": "photo",
        }

        conn, cur = await cls.get_conn_cursor()
        try:
            # 1) 取 sora_content 基本信息
            await cur.execute(
                "SELECT content, source_id, file_type FROM sora_content WHERE id = %s LIMIT 1",
                (content_id,)
            )
            row = await cur.fetchone()
            if not row:
                # 没有这条 content_id
                return False

            src_id = row["source_id"]
            ft_norm = FT_MAP.get(row["file_type"])
            if not ft_norm:
                # 未知类型，保守不动
                print(f"[refine_product_content] Unsupported file_type={row['file_type']!r} for id={content_id}")
                return False

            # 5) 回写 sora_content 与媒体表
            await cur.execute(
                "UPDATE sora_content SET content = %s, stage='pending' WHERE id = %s",
                (refined, content_id)
            )
            if( ft_norm == 'video' or ft_norm == 'document'):
                await cur.execute(
                    f"UPDATE `{ft_norm}` SET caption = %s, kc_id = %s, update_time = NOW(), kc_status = 'pending' WHERE file_unique_id = %s",
                    (refined, content_id, src_id)
                )
            else:
                await cur.execute(
                    f"UPDATE `{ft_norm}` SET caption = %s, kc_id = %s, kc_status = 'pending' WHERE file_unique_id = %s",
                    (refined, content_id, src_id)
                )

            await cur.execute(
                "UPDATE product SET content = %s, stage='pending' WHERE content_id = %s",
                (refined, content_id)
            )


            await conn.commit()
            return True

        except Exception as e:
            await conn.rollback()
            print(f"[refine_product_content] ERROR id={content_id}: {e}")
            raise
        finally:
            await cls.release(conn, cur)    


    @classmethod
    async def set_product_guild(cls, content_id: int) -> None:
        conn, cur = await cls.get_conn_cursor()
        try:
            # 1) 取 sora_content 基本信息
            await cur.execute(
                "SELECT source_id FROM sora_content WHERE id = %s LIMIT 1",
                (content_id,)
            )
            file_row = await cur.fetchone()
            if not file_row:
                # 没有这条 content_id
                return False

            # 2) 取归属的 guild_id
            await cur.execute(
                "SELECT g.guild_id FROM `file_tag` t LEFT JOIN guild g ON g.guild_tag = t.tag WHERE t.`file_unique_id` LIKE %s AND g.guild_id IS NOT NULL ORDER BY tag_count limit 1;",
                (file_row["source_id"],)
            )
            file_tag_row = await cur.fetchone()
            if not file_tag_row:
                return False

            await cur.execute(
                """
                UPDATE product
                   SET guild_id=%s,
                       updated_at=NOW(), stage='pending'
                 WHERE content_id=%s
                """,
                (file_tag_row["guild_id"], content_id)
            )
            affected = cur.rowcount
            await conn.commit()
            return affected
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def check_guild_role(cls, user_id: int, role: str) -> dict | None:
        """
        检查 user_id 是否在 guild_manager 串中 (以 ; 分隔并结尾)。
        返回 guild 记录 (dict)，不存在则返回 None。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            if role == "owner":
                # 管理员直接返回任意一条 guild 记录
                await cur.execute(
                    "SELECT * FROM guild WHERE guild_owner LIKE %s LIMIT 1;",
                    (f"%{user_id};",)
                )

            # 普通用户检查 guild_manager 字段
            elif role == "manager":
                await cur.execute(
                    "SELECT * FROM guild WHERE guild_manager LIKE %s LIMIT 1;",
                    (f"%{user_id};",)
                )
            
            row = await cur.fetchone()
            return row if row else None
        finally:
            await cls.release(conn, cur)




    # AnanBOTPool 内部
    @classmethod
    async def update_product_anonymous_mode(cls, content_id: int, mode: int) -> int:
        """
        将 product.anonymous_mode 设置为 1(匿名) 或 3(公开)
        返回受影响行数
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                UPDATE product
                SET anonymous_mode = %s
                WHERE content_id = %s
                """,
                (mode, content_id)
            )
            await conn.commit()
            return cur.rowcount or 0
        finally:
            await cls.release(conn, cur)

    
    @classmethod
    async def get_trade_url(cls, file_unique_id: str) -> str:
        """
        生成交易/资源跳转链接。
        这里给一个可运行的占位实现；你可按你的业务改成真实落地页。
        """
        # TODO: 若你有真实落地页，请改成你的域名规则
        return f"https://trade/{file_unique_id}"

    @classmethod
    async def find_user_reportable_transaction(cls, user_id: int, file_unique_id: str) -> dict | None:
        """
        按 PHP 逻辑：优先查与该 file_unique_id 相关的 'view' 或 'confirm_buy' 记录（任一即算有交易记录）。
        返回第一条命中记录；无则 None。
        """
        sql = """
            (SELECT *
             FROM `transaction`
             WHERE sender_id = %s
               AND transaction_type = %s
               AND memo = %s)
            UNION ALL
            (SELECT *
             FROM `transaction`
             WHERE sender_id = %s
               AND transaction_type = %s
               AND transaction_description = %s)
            LIMIT 1
        """
        params = (user_id, "view", file_unique_id, user_id, "confirm_buy", file_unique_id)
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(sql, params)
            row = await cur.fetchone()
            return row if row else None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def find_existing_report(cls, file_unique_id: str) -> dict | None:
        """
        查询是否已有针对该 file_unique_id 的举报（状态在 editing/pending/published/failed）。
        有则返回第一条；无则 None。
        """
        sql = """
            SELECT r.*, t.receiver_id as owner_user_id, t.sender_id, t.sender_fee, t.receiver_fee 
              FROM report r
              LEFT JOIN transaction t ON r.transaction_id = t.transaction_id
             WHERE r.process_status IN ('editing', 'pending', 'published', 'failed')
               AND r.file_unique_id = %s
             LIMIT 1
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(sql, (file_unique_id,))
            row = await cur.fetchone()
            return row if row else None
        finally:
           
            await cls.release(conn, cur)

    # ananbot_utils.py 内的 AnanBOTPool 类里新增
    @classmethod
    async def create_report(cls, file_unique_id: str, transaction_id: int, report_type: int, report_reason: str) -> int:
        """
        向 report 表插入一条记录，process_status='pending'
        返回自增 report_id
        """
        conn, cursor = await cls.get_conn_cursor()
        try:
            ts = int(datetime.now().timestamp())
            sql = """
                INSERT INTO report
                    (file_unique_id, transaction_id, create_timestamp, report_reason, report_type, process_status1, process_status)
                VALUES
                    (%s, %s, %s, %s, %s, 0, 'pending')
            """
            await cursor.execute(sql, (file_unique_id, transaction_id, ts, report_reason, report_type))
            await conn.commit()
            return cursor.lastrowid  # MyISAM/auto_increment 可用
        finally:
            await cls.release(conn, cursor)


    @classmethod
    async def update_bid_owner(cls, file_unique_id: str, new_owner_id: str | int) -> int:
        """
        将 bid.owner_user_id 更新为新的拥有者（如系统账号）
        返回受影响行数
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "UPDATE bid SET owner_user_id=%s WHERE file_unique_id=%s",
                (str(new_owner_id), file_unique_id)
            )
            affected = cur.rowcount or 0
            await conn.commit()
            return affected
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_report_status(cls, report_id: int, status: str) -> int:
        """
        更新 report.process_status = 'approved' / 'rejected' / 'pending' ...
        返回受影响行数
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "UPDATE report SET process_status=%s WHERE report_id=%s",
                (status, report_id)
            )
            affected = cur.rowcount or 0
            await conn.commit()
            return affected
        finally:
            await cls.release(conn, cur)
