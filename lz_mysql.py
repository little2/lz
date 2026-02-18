
import aiomysql
import time
from lz_config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_DB_PORT, VALKEY_URL
from typing import Optional, Dict, Any, List, Tuple
from lz_memory_cache import MemoryCache
from lz_cache import TwoLevelCache
import lz_var
import asyncio
from utils.lybase_utils import LYBase
from utils.prof import SegTimer
from functools import wraps 
from collections import defaultdict

# def reconnecting(func):
#     """
#     通用断线重连装饰器：
#     - 只针对 aiomysql.OperationalError
#     - 若错误码为 2006 / 2013 → 认为是断线，重建连接池 + 自动重试一次
#     - 第二次仍失败 / 其它错误 → 直接抛出
#     """
#     @wraps(func)
#     async def wrapper(*args, **kwargs):
#         # 对于 @classmethod 来说，args[0] 会是 cls
#         cls = args[0] if args else None

#         for attempt in (1, 2):
#             try:
#                 return await func(*args, **kwargs)
#             except aiomysql.OperationalError as e:
#                 code = e.args[0] if e.args else None
#                 msg = e.args[1] if len(e.args) > 1 else ""

#                 # 没有 cls，或不是断线错误，或已经重试过一次 → 直接抛
#                 if not cls or code not in (2006, 2013) or attempt == 2:
#                     print(f"❌ [MySQLPool] OperationalError {code}: {msg}", flush=True)
#                     raise

#                 # 第一次遇到 2006/2013 → 重建连接池，再重跑一次整个方法
#                 print(f"⚠️ [MySQLPool] 侦测到断线 {code}: {msg} → 重建连接池并重试一次", flush=True)
#                 try:
#                     await cls._rebuild_pool()
#                 except Exception as e2:
#                     print(f"❌ [MySQLPool] 重建连接池失败: {e2}", flush=True)
#                     raise
#                 # for 循环继续，进入第二轮
#     return wrapper

class MySQLPool(LYBase):
# class MySQLPool:
    _pool = None
    _lock = asyncio.Lock()
    _cache_ready = False
    cache = None

    @classmethod
    async def init_pool(cls):
        # 幂等：多处并发调用只建一次连接池
        if cls._pool is not None:
            if not cls._cache_ready:
                cls.cache = TwoLevelCache(valkey_client=VALKEY_URL, namespace='lz:')
                cls._cache_ready = True
            return cls._pool

        async with cls._lock:
            if cls._pool is None:
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
                print("✅ MySQL 连接池初始化完成")
            if not cls._cache_ready:
                cls.cache = TwoLevelCache(valkey_client=VALKEY_URL, namespace='lz:')
                cls._cache_ready = True
        return cls._pool

    @classmethod
    async def ensure_pool(cls):
        if cls._pool is None:
            await cls.init_pool()
        return cls._pool

    @classmethod
    async def get_conn_cursor(cls):
        # ✅ 不再抛“未初始化”，而是自愈
        await cls.ensure_pool()
        conn = await cls._pool.acquire()
        cursor = await conn.cursor(aiomysql.DictCursor)
        return conn, cursor

    @classmethod
    async def release(cls, conn, cursor):
        try:
            if cursor:
                await cursor.close()
        finally:
            if conn and cls._pool:
                cls._pool.release(conn)

    @classmethod
    async def close(cls):
        async with cls._lock:
            if cls._pool:
                cls._pool.close()
                await cls._pool.wait_closed()
                cls._pool = None
                print("🛑 MySQL 连接池已关闭")


    @classmethod
    async def _rebuild_pool(cls):
        """
        强制重建连接池，用于 2006/2013 等断线错误后的自愈。
        """
        async with cls._lock:
            if cls._pool:
                try:
                    cls._pool.close()
                    await cls._pool.wait_closed()
                except Exception as e:
                    print(f"⚠️ [MySQLPool] 关闭旧连接池出错: {e}", flush=True)
            cls._pool = None
            print("🔄 [MySQLPool] 重建 MySQL 连接池中…", flush=True)
            await cls.init_pool()



    # @classmethod
    # async def delete_cache(cls, prefix: str):
    #     keys_to_delete = [k for k in cls.cache.keys() if k.startswith(prefix)]
    #     for k in keys_to_delete:
    #         del cls.cache[k]
    #     pass

        
    @classmethod
    async def delete_cache(cls, prefix: str):
        """
        删除 cache 中以 prefix 开头的 key。
        - MemoryCache: 无 keys() 接口，因此从内部 _store 取 key
        - TwoLevelCache: 仅清 L1（L2 依业务需要可扩展批量删；目前保持轻量，不阻塞）
        """
        if not cls.cache:
            return

        # 统一拿到 L1 的 store（兼容 MemoryCache / TwoLevelCache）
        l1 = getattr(cls.cache, "l1", None)
        if l1 is None:
            l1 = cls.cache  # 可能直接是 MemoryCache

        store = getattr(l1, "_store", None)
        if not store:
            return

        keys_to_delete = [k for k in list(store.keys()) if str(k).startswith(prefix)]
        for k in keys_to_delete:
            try:
                # TwoLevelCache / MemoryCache 都支持 delete(key)
                if hasattr(cls.cache, "delete"):
                    cls.cache.delete(k)
                else:
                    store.pop(k, None)
            except Exception:
                pass

 

    @classmethod
    async def get_cache_by_key(
        cls, cache_key
    ) -> List[Dict[str, Any]]:
        
        cached = await cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached

        

    @classmethod
    async def set_cache_by_key(
        cls, cache_key, cache_value, ttl: int | None = 3000
    ) -> List[Dict[str, Any]]:
      
        # //set(self, key: str, value: Any, ttl: int = 1200, only_l2: bool = True):

        cls.cache.set(cache_key, cache_value, ttl=ttl, only_l2=False)
        print(f"🔹 MemoryCache set for {cache_key}, {cache_value} items")

    @classmethod
    async def find_transaction_by_description(cls, desc: str):
        """
        根据 transaction_description 查询一笔交易记录。
        :param desc: 例如 "chat_id message_id"
        :return: dict | None
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT *
                FROM transaction
                WHERE transaction_description = %s
                LIMIT 1
                """,
                (desc,),
            )
            row = await cur.fetchone()
            return row if row else None
        except Exception as e:
            print(f"⚠️ find_transaction_by_description 出错: {e}", flush=True)
            return None
        finally:
            await cls.release(conn, cur)



    @classmethod
    async def in_block_list(cls, user_id):
        # 这里可以实现 block list 检查逻辑
        # 目前直接写 False
        return False
    
   
    @classmethod
    async def search_sora_content_by_id(cls, content_id: int):
        await cls.ensure_pool()  # ✅ 新增
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute('''
                SELECT s.id, s.source_id, s.file_type, s.content, s.file_size, s.duration, s.tag,
                    s.thumb_file_unique_id, s.valid_state, s.file_password,
                    m.file_id AS m_file_id, m.thumb_file_id AS m_thumb_file_id,
                    p.price as fee, p.file_type as product_type, p.owner_user_id, p.purchase_condition, p.id as product_id,  p.review_status,
                    g.guild_id, g.guild_keyword, g.guild_resource_chat_id, g.guild_resource_thread_id, g.guild_chat_id, g.guild_thread_id  
                FROM sora_content s
                LEFT JOIN sora_media m ON s.id = m.content_id AND m.source_bot_name = %s
                LEFT JOIN product p ON s.id = p.content_id
                LEFT JOIN guild g ON p.guild_id = g.guild_id
                WHERE s.id = %s  ORDER BY s.id DESC
                '''
            , (lz_var.bot_username, content_id))
            row = await cursor.fetchone()
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
    async def get_sora_content_by_fuid(cls, file_unique_id: str) -> int | None:
        """
        通过 sora_content.source_id(file_unique_id) 取得 content_id(sora_content.id)
        """
        if not file_unique_id:
            return None

        await cls.ensure_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                SELECT id
                FROM sora_content
                WHERE source_id = %s
                LIMIT 1
            """
            await cur.execute(sql, (file_unique_id,))
            row = await cur.fetchone()
            if not row:
                return None
            # DictCursor：row["id"]；tuple cursor：row[0]
            return int(row["id"] if isinstance(row, dict) else row[0])
        except Exception as e:
            print(f"⚠️ get_content_id_by_file_unique_id 出错: {e}", flush=True)
            return None
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def set_sora_content_by_id(cls, content_id: int, update_data: dict):
        await cls.ensure_pool()   # ✅ 新增
        conn, cursor = await cls.get_conn_cursor()
        try:
            set_clause = ', '.join([f"{key} = %s" for key in update_data.keys()])
            await cursor.execute(f"""
                UPDATE sora_content SET {set_clause}
                WHERE id = %s
            """, (*update_data.values(), content_id))
           
            
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)

    @classmethod
    async def upsert_product_thumb(
        cls,
        content_id: int,
        thumb_file_unique_id: str,
        thumb_file_id: str,
        bot_username: str,
    ):
        await cls.ensure_pool()   # ✅ 新增
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute(f"""
                UPDATE sora_content SET thumb_file_unique_id = %s, stage='pending'
                WHERE id = %s 
            """, (thumb_file_unique_id, content_id))
           
            await cursor.execute(f"""
                INSERT INTO sora_media (content_id, source_bot_name, thumb_file_id)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    thumb_file_id = VALUES(thumb_file_id)
            """, (content_id, bot_username, thumb_file_id))
            
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)   

    @classmethod
    async def update_board_funds(
        cls,
        board_id : int,
        pay_funds:  int
    ):
        await cls.ensure_pool()   # ✅ 新增
        conn, cursor = await cls.get_conn_cursor()
        try:
            # pay_funds 一定是负数
            if pay_funds > 0:
                pay_funds = -pay_funds
            await cursor.execute(f"""
                UPDATE board SET funds = (funds + %s) 
                WHERE board_id = %s 
            """, (pay_funds, board_id))
           
        except Exception as e:
            print(f"⚠️ 427 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)   


    @classmethod
    async def reset_sora_media_by_id(cls, content_id, bot_username):
        await cls.ensure_pool()   # ✅ 新增
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute(f"""
                UPDATE sora_media SET thumb_file_id = NULL
                WHERE content_id = %s and source_bot_name <> %s
            """, (content_id, bot_username))
    
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)


    @classmethod
    async def reset_thumb_file_id(cls, content_id, thumb_file_id, bot_username):
        #若 thumb_file_id 有值,可能已经无效了，删除后，再试一次, 检查 file_extension.file_id 是否相同 ,若相同,也一并删除
        await cls.ensure_pool()  
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute(f"""
                UPDATE sora_media SET thumb_file_id = NULL
                WHERE content_id = %s and source_bot_name = %s and thumb_file_id = %s
            """, (content_id, bot_username,thumb_file_id))
    
            await cursor.execute(f"""
                DELETE FROM file_extension
                WHERE file_id = %s and bot = %s
            """, (thumb_file_id, bot_username))    

        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)

    @classmethod
    async def fetch_file_by_file_uid(cls, source_id: str):
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute("""
                SELECT f.file_type, f.file_id, f.bot, b.bot_id, b.bot_token
                FROM file_extension f
                LEFT JOIN bot b ON f.bot = b.bot_name
                WHERE f.file_unique_id = %s
                LIMIT 0, 1
            """, (source_id,))
            row = await cursor.fetchone()
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
            row = None
        finally:
            await cls.release(conn, cursor)

        if not row:
            print("❌ 没有找到匹配记录 file_id")
            return None

        chat_id = lz_var.man_bot_id
        if chat_id:
            retSend = None
            from aiogram import Bot
            mybot = Bot(token=f"{row['bot_id']}:{row['bot_token']}")
            try:
                if row["file_type"] == "photo":
                    retSend = await mybot.send_photo(chat_id=chat_id, photo=row["file_id"])
                elif row["file_type"] == "video":
                    retSend = await mybot.send_video(chat_id=chat_id, video=row["file_id"])
                elif row["file_type"] == "document":
                    retSend = await mybot.send_document(chat_id=chat_id, document=row["file_id"])
            except Exception as e:
                print(f"❌ 目标 chat 不存在或无法访问(288): {e}")
            finally:
                await mybot.session.close()
                return retSend

        return None

    @classmethod
    async def set_product_review_status(cls, content_id: int, review_status: int, operator_user_id: int = 0, reason: str = ""):
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute("""
                UPDATE product SET review_status = %s, stage='pending'
                WHERE content_id = %s
            """, (review_status, content_id))

            await cursor.execute("""
                UPDATE sora_content SET valid_state = %s, stage='pending' 
                WHERE id = %s
            """, (review_status, content_id))

            await cursor.execute("""
                INSERT INTO review_log (content_id, to_status, operator_user_id, note)
                VALUES (%s, %s, %s, %s)
            """, (content_id, review_status, operator_user_id, reason))

            
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)

    @classmethod
    async def get_pending_product(cls):
        """取得最多 1 笔待送审的 product (guild_id 不为空且 review_status=6)"""
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute("""
                SELECT content_id, guild_id, review_status
                FROM product
                WHERE guild_id IS NOT NULL
                  AND review_status = 6
                LIMIT 1
            """)
            rows = await cursor.fetchall()
            return rows
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
            return []
        finally:
            await cls.release(conn, cursor)

   
    '''
    Collection 内容管理相关方法
    '''

    @classmethod
    async def create_user_collection(
        cls,
        user_id: int,
        title: str = "未命名的资源橱窗",
        description: str = "",
        is_public: int = 1,
    ) -> Dict[str, Any]:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                INSERT INTO user_collection (user_id, title, description, is_public, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [user_id, (title or "")[:255], description or "", 1 if is_public == 1 else 0, time(), time()],
            )
            new_id = cur.lastrowid
            
            await conn.commit()
            return {"ok": "1", "status": "inserted", "id": new_id}
        except Exception as e:
            try: await conn.rollback()
            except Exception: pass
            return {"ok": "", "status": "error", "error": str(e)}
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_user_collection(
        cls,
        collection_id: int,
        title: Optional[str] = None,
        description: Optional[str] = None,
        is_public: Optional[int] = None,
        cover_type: Optional[str] = None,
        cover_file_unique_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        conn, cur = await cls.get_conn_cursor()
        try:
            sets, params = [], []
            if title is not None:
                sets.append("title = %s")
                params.append(title[:255].strip())
            if description is not None:
                sets.append("description = %s")
                params.append(description.strip())
            if is_public is not None:
                sets.append("is_public = %s")
                params.append(1 if int(is_public) == 1 else 0)

            if cover_type is not None:
                sets.append("cover_type = %s")
                params.append(cover_type[:255].strip())
            if cover_file_unique_id is not None:
                sets.append("cover_file_unique_id = %s")
                params.append(cover_file_unique_id[:255].strip())

            if not sets:
                return {"ok": "1", "status": "noop", "id": collection_id}

            sets.append("updated_at = %s")
            params.append(time)

            sql = f"UPDATE user_collection SET {', '.join(sets)} WHERE id = %s"
            params.append(collection_id)
            await cur.execute(sql, params)
            await conn.commit()
            return {"ok": "1", "status": "updated", "id": collection_id}
        except Exception as e:
            try: await conn.rollback()
            except Exception: pass
            return {"ok": "", "status": "error", "error": str(e)}
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_user_collection_by_id(cls, collection_id: int) -> Optional[Dict[str, Any]]:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT id, user_id, title, description, is_public, created_at, updated_at, sort_order, cover_type, cover_file_unique_id
                FROM user_collection
                WHERE id = %s
                """,
                [collection_id],
            )
            row = await cur.fetchone()
            if not row:
                return None
            if isinstance(row, dict):
                return row
            cols = ["id", "user_id", "title", "description", "is_public", "created_at", "updated_at", "sort_order", "cover_type", "cover_file_unique_id"]
            return {k: v for k, v in zip(cols, row)}
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def list_user_collections(
        cls, user_id: int, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        
        cache_key = f"user:clt:{user_id}:{limit}:{offset}"
        cached = await cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached

        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT id, title, description, is_public, created_at, updated_at, sort_order, cover_type, cover_file_unique_id
                FROM user_collection
                WHERE user_id = %s
                ORDER BY id DESC
                LIMIT %s OFFSET %s
                """,
                [user_id, int(limit), int(offset)],
            )
            rows = await cur.fetchall()
            if not rows:
                return []
            if isinstance(rows[0], dict):
                cls.cache.set(cache_key, rows, ttl=300)
                return rows
            cols = ["id", "title", "description", "is_public", "created_at", "updated_at", "sort_order", "cover_type", "cover_file_unique_id"]
            result= [{k: v for k, v in zip(cols, r)} for r in rows]
            cls.cache.set(cache_key, result, ttl=300)
            print(f"🔹 MemoryCache set for {cache_key}, {len(result)} items")
            return result
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def list_user_favorite_collections(
        cls, user_id: int, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        """
        列出用户收藏的资源橱窗（基于 user_collection_favorite.user_collection_id 关联）。
        按收藏记录 id 倒序（最新收藏在前）。
        """
        cache_key = f"fav:clt:{user_id}:{limit}:{offset}"
        cached = await cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached


        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT uc.id, uc.title, uc.description, uc.is_public, uc.created_at
                FROM user_collection_favorite AS ucf
                JOIN user_collection AS uc
                ON uc.id = ucf.user_collection_id
                WHERE ucf.user_id = %s
                ORDER BY ucf.id DESC, uc.id DESC
                LIMIT %s OFFSET %s
                """,
                [user_id, int(limit), int(offset)],
            )
            rows = await cur.fetchall()
            if not rows:
                return []
            if isinstance(rows[0], dict):
                cls.cache.set(cache_key, rows, ttl=300)
                return rows
            cols = ["id", "title", "description", "is_public", "created_at"]
            result = [{k: v for k, v in zip(cols, r)} for r in rows]
            cls.cache.set(cache_key, result, ttl=300)
            print(f"🔹 MemoryCache set for {cache_key}, {len(result)} items")
            return result
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_collection_detail_with_cover(cls, collection_id: int, bot_name: str = "luzaitestbot") -> dict | None:
        """
        返回 user_collection 全字段 + cover 对应的 file_id（若有）。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            SELECT uc.*, fe.file_id AS cover_file_id
            FROM user_collection uc
            LEFT JOIN file_extension fe
              ON uc.cover_file_unique_id = fe.file_unique_id
             AND fe.bot = %s
            WHERE uc.id = %s
            LIMIT 1
            """
            await cur.execute(sql, (bot_name, collection_id))
            row = await cur.fetchone()
            return dict(row) if row else None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def list_collection_files_file_id(cls, collection_id: int, limit: int, offset: int) -> tuple[list[dict], bool]:
        """
        列出资源橱窗里文件的 file_id 列表（按 sort 排序）。
        这里演示通过 sora_content.id = user_collection_file.content_id 来取 file_id。
        若你的 file_id 存在别的表，请据实替换 JOIN。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # 先取 limit+1 判断 has_next
            sql = """
            SELECT sc.content,sc.id,sc.file_type 
            FROM user_collection_file ucf
            LEFT JOIN sora_content sc
              ON sc.id = ucf.content_id
            WHERE ucf.collection_id = %s AND sc.valid_state != 4
            ORDER BY ucf.sort ASC
            LIMIT %s OFFSET %s
            """
            await cur.execute(sql, (collection_id, limit, offset))
            rows = await cur.fetchall()
            items = [dict(r) for r in rows]
            has_next = len(items) > 0 and len(items) == limit  # 外层调用已传入 limit=PAGE_SIZE+1
            return items, has_next
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def is_collection_favorited(cls, user_id: int, collection_id: int) -> bool:
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            SELECT 1 FROM user_collection_favorite
            WHERE user_id = %s AND user_collection_id = %s
            LIMIT 1
            """
            await cur.execute(sql, (user_id, collection_id))
            row = await cur.fetchone()
            return bool(row)
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def add_collection_favorite(cls, user_id: int, collection_id: int) -> bool:
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            INSERT INTO user_collection_favorite (user_collection_id, user_id, update_at)
            VALUES (%s, %s, %s)
            """
            await cur.execute(sql, (collection_id, user_id, int(time.time())))
            new_id = cur.lastrowid
            return new_id
        except Exception as e:
            # 可能需要唯一约束避免重复；无唯一约束时重复插入会多条，这里简单忽略异常或加逻辑
            print(f"⚠️ add_collection_favorite 失败: {e}", flush=True)
            return False
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def remove_collection_favorite(cls, user_id: int, collection_id: int) -> bool:
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            DELETE FROM user_collection_favorite
            WHERE user_id = %s AND user_collection_id = %s
            """
            await cur.execute(sql, (user_id, collection_id))
            return True
        except Exception as e:
            print(f"⚠️ remove_collection_favorite 失败: {e}", flush=True)
            return False
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_user_collections_count_and_first(cls, user_id: int) -> tuple[int, int | None]:
        """
        返回 (资源橱窗数量, 第一条资源橱窗ID或None)。
        只查一次：LIMIT 2 即可区分 0/1/多，并顺便拿到第一条ID。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            SELECT id
            FROM user_collection
            WHERE user_id = %s
            ORDER BY id ASC
            LIMIT 2
            """
            await cur.execute(sql, (user_id,))
            rows = await cur.fetchall()
            cnt = len(rows)
            first_id = rows[0]["id"] if cnt >= 1 else None
            return cnt, first_id
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_clt_files_by_clt_id(cls, collection_id: int) -> list[dict]:
        """
        查询某个资源橱窗的所有文件
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # id, source_id, file_type, content
            sql = """
            SELECT sc.id, sc.source_id, sc.file_type, sc.content
            FROM user_collection_file ucf
            LEFT JOIN sora_content sc ON ucf.content_id = sc.id
            WHERE ucf.collection_id = %s AND sc.valid_state != 4
            ORDER BY ucf.sort ASC
            """
            await cur.execute(sql, (collection_id,))
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ get_clt_files_by_clt_id 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_clt_by_content_id(cls, content_id: int) -> list[dict]:
        """
        查询某个资源橱窗的所有文件
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # id, source_id, file_type, content
            sql = """
            SELECT *
            FROM user_collection_file 
            WHERE content_id = %s 
            """
            await cur.execute(sql, (content_id,))
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ get_clt_by_content_id 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def add_content_to_user_collection(cls, collection_id: int, content_id: int | str) -> bool:
        """
        把 content_id 加入某个资源橱窗。已存在则不报错（联合主键去重）。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            INSERT INTO user_collection_file (collection_id, content_id, sort)
            VALUES (%s, %s, 0)
            ON DUPLICATE KEY UPDATE sort = VALUES(sort)
            """
            # content_id 列是 varchar(100)，统一转成字符串
            await cur.execute(sql, (int(collection_id), str(content_id)))
            new_id = cur.lastrowid
            await conn.commit()
            return new_id
        except Exception as e:
            print(f"❌ add_content_to_user_collection error: {e}", flush=True)
            return False
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def remove_content_from_user_collection(cls, collection_id: int, content_id: int | str) -> bool:
        """
        把 content_id 移出
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
            DELETE FROM user_collection_file WHERE collection_id = %s AND content_id = %s
            """
            # content_id 列是 varchar(100)，统一转成字符串
            await cur.execute(sql, (int(collection_id), str(content_id)))
            await conn.commit()
            return True
        except Exception as e:
            print(f"❌ remove_content_from_user_collection error: {e}", flush=True)
            return False
        finally:
            await cls.release(conn, cur)


    '''
    业务相关方法
    '''


    @classmethod
    async def upsert_news_content(cls, tpl_data: dict) -> dict:
        """
        插入或更新 news_content。
        - tpl_data 应包含至少: title, text, file_type, button_str,
          bot_name, business_type, content_id, thumb_file_unique_id
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                INSERT INTO news_content
                    (title, text, file_type, button_str,
                     created_at, bot_name, business_type, content_id, thumb_file_unique_id)
                VALUES
                    (%s, %s, %s, %s,
                     NOW(), %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    text = VALUES(text),
                    file_type = VALUES(file_type),
                    button_str = VALUES(button_str),
                    created_at = NOW(),
                    business_type = VALUES(business_type),
                    thumb_file_unique_id = VALUES(thumb_file_unique_id)
            """
            params = (
                tpl_data.get("title"),
                tpl_data.get("text"),
                tpl_data.get("file_type"),
                tpl_data.get("button_str"),
                tpl_data.get("bot_name", "salai"),
                tpl_data.get("business_type"),
                tpl_data.get("content_id"),
                tpl_data.get("thumb_file_unique_id"),
            )
            await cur.execute(sql, params)
            await conn.commit()

            return {"ok": "1", "status": "upserted", "content_id": tpl_data.get("content_id")}
        except Exception as e:
            try:
                await conn.rollback()
            except Exception:
                pass
            print(f"⚠️ upsert_news_content 出错: {e}", flush=True)
            return {"ok": "", "status": "error", "error": str(e)}
        finally:
            await cls.release(conn, cur)
 
    @classmethod
    async def fetch_valid_xlj_memberships(cls, user_id: int | str = None) -> list[dict]:
        """
        查询 MySQL membership 表，条件：
          - course_code = 'xlj'
          - expire_timestamp > 当前时间
          - 若传入 user_id，则限定 user_id；否则查所有用户
        返回: list[dict]
        """
        now_ts = int(time.time())
        conn, cur = await cls.get_conn_cursor()
        try:
            if user_id is not None:
                sql = """
                    SELECT membership_id, course_code, user_id, create_timestamp, expire_timestamp
                    FROM membership
                    WHERE course_code = %s
                      AND user_id = %s
                      AND expire_timestamp > %s
                    ORDER BY expire_timestamp DESC
                """
                await cur.execute(sql, ("xlj", str(user_id), now_ts))
            else:
                sql = """
                    SELECT membership_id, course_code, user_id, create_timestamp, expire_timestamp
                    FROM membership
                    WHERE course_code = %s
                      AND expire_timestamp > %s
                    ORDER BY expire_timestamp DESC
                """
                await cur.execute(sql, ("xlj", now_ts))

            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ fetch_valid_xlj_memberships 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)

 

    @classmethod
    async def search_history_upload(cls, user_id: int) -> list[dict]:
        """
        查询某个用户的所有上传历史
        """

        cache_key = f"history:upload:{user_id}"
        cached = await cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached


        conn, cur = await cls.get_conn_cursor()
        try:
            # id, source_id, file_type, content
            sql = """
            SELECT sc.id, sc.source_id, sc.file_type, sc.content
            FROM product p
            LEFT JOIN sora_content sc ON p.content_id = sc.id
            WHERE p.owner_user_id = %s AND sc.valid_state != 4
            ORDER BY sc.id DESC
            """
            await cur.execute(sql, (user_id,))
            rows = await cur.fetchall()
            result = [dict(r) for r in rows] if rows else []
            cls.cache.set(cache_key, result, ttl=300)
            print(f"🔹 MemoryCache set for {cache_key}, {len(result)} items")
            return result

        except Exception as e:
            print(f"⚠️ search_history_upload 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def search_history_redeem(cls, user_id: int) -> list[dict]:
        """
        查询某个用户的所有兑换历史
        """

        cache_key = f"history:redeem:{user_id}"
        cached = await cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached        
        
        conn, cur = await cls.get_conn_cursor()
        try:
            # id, source_id, file_type, content
            sql = """
            SELECT sc.id, sc.source_id, sc.file_type, sc.content
            FROM transaction t
            LEFT JOIN sora_content sc ON t.transaction_description = sc.source_id
            WHERE t.sender_id = %s and t.transaction_type='confirm_buy' AND sc.valid_state != 4
            ORDER BY t.transaction_id DESC
            """
            await cur.execute(sql, (user_id,))
            rows = await cur.fetchall()
            result = [dict(r) for r in rows] if rows else []
            cls.cache.set(cache_key, result, ttl=300)
            print(f"🔹 MemoryCache set for {cache_key}, {len(result)} items")
            return result
        except Exception as e:
            print(f"⚠️ search_history_upload 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)
            #

    @classmethod
    async def get_album_list(cls, content_id: int, bot_name: str) -> dict:
        """
        查询某个 album 下的所有成员文件，并生成文本列表。
        - 对应 PHP 版的 get_album_list()
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                SELECT s.source_id, c.file_type, s.content, s.file_size, s.duration,
                       m.source_bot_name, m.thumb_file_id, m.file_id, c.preview
                FROM album_items c 
                LEFT JOIN sora_content s ON c.member_content_id = s.id
                LEFT JOIN sora_media m ON c.member_content_id = m.content_id AND m.source_bot_name = %s
                WHERE c.content_id = %s AND s.valid_state != 4
                ORDER BY c.file_type;
            """
            await cur.execute(sql, (bot_name, content_id))
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ 1013 get_album_list 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def list_album_items_by_content_id(cls, content_id: int) -> list[dict]:
        """
        取出某个相簿（content_id）的所有 album_items 行。
        返回字段与 PG 目标表对齐：id, content_id, member_content_id,
        file_unique_id, file_type, position, created_at, updated_at, stage
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                SELECT
                    id,
                    content_id,
                    member_content_id,
                    file_unique_id,
                    file_type,
                    `position`,
                    created_at,
                    updated_at,
                    stage,
                    preview
                FROM album_items
                WHERE content_id = %s
                ORDER BY `position` ASC, id ASC
            """
            await cur.execute(sql, (content_id,))
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ list_album_items_by_content_id 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def fetch_task_value_by_title(cls, title: str) -> str | None:
        """
        读取 task_rec 中 task_title=title 的最新一笔 task_value
        返回: str | None
        """
        conn = cur = None
        try:
            conn, cur = await cls.get_conn_cursor()
            await cur.execute(
                """
                SELECT task_value
                FROM task_rec
                WHERE task_title = %s
                ORDER BY task_id DESC
                LIMIT 1
                """,
                (title,),
            )
            row = await cur.fetchone()
            if not row:
                return None

            # 兼容 dict cursor 与 tuple cursor
            if isinstance(row, dict):
                return row.get("task_value")
            else:
                # 假设 task_value 是第一列
                return row[0] if len(row) > 0 else None
        except Exception as e:
            print(f"[MySQLPool] fetch_task_value_by_title error: {e}", flush=True)
            return None
        finally:
            if conn and cur:
                await cls.release(conn, cur)

    @classmethod
    async def get_user_name(cls,user_id: int):

        if user_id is None or user_id == 0:
            return "未知用户"

        cache_key = f"get_user_name:{user_id}"
        cached = await cls.cache.get(cache_key)
        if cached:
            return cached

        try:
            chat = await lz_var.bot.get_chat(user_id)
            cached = chat.full_name or f"@{chat.username}" or "未知用户"
            return cached
        except Exception as e:
            # print(f"❌ 获取用户资料失败: {e}")
            return "未知用户"

    @classmethod
    async def list_transactions_for_sync(
        cls,
        start_transaction_id: int,
        sender_id: int,
        limit: int = 500,
    ) -> list[dict]:
        """
        取出需要同步到 PostgreSQL 的 transaction 记录：
          - transaction_id > start_transaction_id
          - sender_id = 指定 user
          - 最多 limit 笔（默认 500）
        结果按 transaction_id 升序，方便你后续增量推进。
        """
        await cls.ensure_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                SELECT
                    transaction_id,
                    sender_id,
                    sender_fee,
                    receiver_id,
                    receiver_fee,
                    transaction_type,
                    transaction_description,
                    transaction_timestamp,
                    memo
                FROM transaction
                WHERE transaction_id > %s
                  AND sender_id = %s
                ORDER BY transaction_id ASC
                LIMIT %s
            """
            await cur.execute(
                sql,
                (
                    int(start_transaction_id),
                    int(sender_id),
                    int(limit),
                ),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ list_transactions_for_sync 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def list_product_for_sync(
        cls,
        user_id: int,
        limit: int = 500,
    ) -> list[dict]:
        """
        取出需要同步到 PostgreSQL 的 product 记录：
          - owner_user_id = 指定 user_id
          - 最多 limit 笔（默认 500）
        结果按 content_id 升序，方便后续扩展做增量。
        """
        await cls.ensure_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                SELECT
                    id,
                    content_id,
                    price,
                    file_type,
                    owner_user_id,
                    purchase_condition,
                    guild_id
                FROM product
                WHERE owner_user_id = %s
                ORDER BY content_id ASC
                LIMIT %s
            """
            await cur.execute(
                sql,
                (
                    int(user_id),
                    int(limit),
                ),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(f"⚠️ list_product_for_sync 出错: {e}", flush=True)
            return []
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def export_jieba_dict(cls) -> str:
        """
        导出 jieba 自定义词典内容，用于生成 userdict.txt。

        数据来源：
        1) jieba_dict.word (人工维护专有名词)
        2) series.name      (系列名称)
        3) tag.tag_cn       (标签中文名)

        输出格式：
            word freq flag
        """
        await cls.ensure_pool()

        conn, cur = await cls.get_conn_cursor()
        try:
            # 一次把三类专有名词都查出来
            # 这里用 UNION ALL，在 Python 里再做去重&合并 freq
            sql = """
                SELECT word       AS word,
                       freq       AS freq,
                       flag       AS flag,
                       'dict'     AS src
                FROM jieba_dict
                WHERE enabled = 1

                UNION ALL

                SELECT name      AS word,
                       5000        AS freq,   -- 系列名给一个偏高的权重
                       'nz'      AS flag,
                       'series'  AS src
                FROM series
                WHERE name IS NOT NULL AND name <> ''

                UNION ALL

                SELECT tag_cn    AS word,
                       3000        AS freq,   -- 标签中文名权重略低于系列
                       'nz'      AS flag,
                       'tag_cn'  AS src
                FROM tag
                WHERE tag_cn IS NOT NULL AND tag_cn <> ''
            """
            await cur.execute(sql)
            rows = await cur.fetchall()
        except Exception as e:
            print(f"[jieba_dict] Export error: {e}", flush=True)
            return ""
        finally:
            await cls.release(conn, cur)

        if not rows:
            return ""

        # 用 dict 做去重：同一个词只保留一条，freq 取最大
        merged: dict[str, dict] = {}

        for row in rows:
            # aiomysql.DictCursor → row 是 dict
            raw_word = row.get("word")
            if not raw_word:
                continue

            word = str(raw_word).strip()
            if not word:
                continue

            # freq 兜底
            try:
                freq = int(row.get("freq") or 10)
            except Exception:
                freq = 10

            flag = (row.get("flag") or "").strip() or "nz"

            existed = merged.get(word)
            if not existed:
                merged[word] = {
                    "freq": freq,
                    "flag": flag,
                }
            else:
                # 若已存在，则 freq 取较大值；flag 只在原 flag 为默认 nz 时才覆盖
                if freq > existed["freq"]:
                    existed["freq"] = freq
                if existed["flag"] == "nz" and flag != "nz":
                    existed["flag"] = flag

        # 组装为 jieba userdict 文本
        lines: list[str] = []
        for word, info in merged.items():
            freq = info["freq"]
            flag = info["flag"] or "nz"
            lines.append(f"{word} {freq} {flag}")

        # 排序一下（可选）：让输出稳定一些
        lines.sort()

        return "\n".join(lines)

    @classmethod
    async def export_synonym_lexicon(cls) -> str:
        """
        从 search_synonym 表导出同义词词库文本。
        文本格式：
            # canonical synonym1 synonym2 ...
            正太 正太受 小正太
        """
        await cls.ensure_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT canonical, synonym
                FROM search_synonym
                WHERE enabled = 1
                ORDER BY canonical, synonym
                """
            )
            rows = await cur.fetchall()
        except Exception as e:
            print(f"⚠️ export_synonym_lexicon 出错: {e}", flush=True)
            rows = []
        finally:
            await cls.release(conn, cur)

        if not rows:
            return ""

        groups: dict[str, list[str]] = {}
        for r in rows:
            # DictCursor 或 tuple 都兼容
            canonical = (r["canonical"] if isinstance(r, dict) else r[0]) or ""
            synonym = (r["synonym"] if isinstance(r, dict) else r[1]) or ""
            canonical = canonical.strip()
            synonym = synonym.strip()
            if not canonical or not synonym:
                continue
            groups.setdefault(canonical, []).append(synonym)

        lines = [
            "# 导出自 search_synonym (canonical synonym1 synonym2 ...)",
        ]
        for canonical, syn_list in sorted(groups.items()):
            uniq_syn = sorted(set(syn_list))
            lines.append(" ".join([canonical] + uniq_syn))

        return "\n".join(lines) + "\n"


    @classmethod
    async def export_stopword_lexicon(cls) -> str:
        """
        从 search_stop_word 表导出停用词文本。
        文本格式：
            # stop words
            视频
            影片
        """
        await cls.ensure_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT word
                FROM search_stop_word
                WHERE enabled = 1
                ORDER BY word
                """
            )
            rows = await cur.fetchall()
        except Exception as e:
            print(f"⚠️ export_stopword_lexicon 出错: {e}", flush=True)
            rows = []
        finally:
            await cls.release(conn, cur)

        if not rows:
            return ""

        lines = [
            "# 导出自 search_stop_word (一行一个停用词)",
        ]
        for r in rows:
            w = (r["word"] if isinstance(r, dict) else r[0]) or ""
            w = w.strip()
            if w:
                lines.append(w)

        return "\n".join(lines) + "\n"
    
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
       
        cache_key = f"all_tags_grouped"
        cached = await cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached

        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute("SELECT t.tag, t.tag_cn, t.tag_type, y.type_cn  FROM tag t LEFT JOIN tag_type y ON t.tag_type = y.type_code")
            rows = await cur.fetchall()
            
            if not rows:
                return []
            
            
           
            all_tag_rows = defaultdict(list)
            all_tag_types = {}

            for r in rows:
                t = r.get("tag_type")
                if not t:
                    continue  # tag_type 为 NULL 的跳过（或你想塞到 "unknown" 也行）

                all_tag_types[t] = {"tag_type":t, "type_cn": r.get("type_cn")}
                all_tag_rows[t].append({
                    "tag": r.get("tag"),
                    "tag_cn": r.get("tag_cn"),
                    "type_cn": r.get("type_cn"),
                    "tag_type": t,   # 可要可不要（通常分组后可省略）
                })

            all_tag_rows = dict(all_tag_rows)

            result = {"all_tag_rows":all_tag_rows, "all_tag_types": all_tag_types}
            cls.cache.set(cache_key, result, ttl=30000)
            return result
        finally:
            await cls.release(conn, cur)

    
    
    

    @classmethod
    async def set_media_auto_send(cls, insert_data: dict):
        """
        media_auto_send UPSERT
        PK: media_auto_send_id
        UNIQUE KEY (chat_id, file_id)
        """

        if not isinstance(insert_data, dict):
            return {"ok": "", "status": "bad_insert_data"}

        # 1) 归一化字段别名（可按你习惯增补）
        data = dict(insert_data)

        # 常见别名容错：create_time/created_time -> create_timestamp
        if "create_timestamp" not in data:
            if "create_time" in data and data.get("create_time") is not None:
                data["create_timestamp"] = data.pop("create_time")
            elif "created_time" in data and data.get("created_time") is not None:
                data["create_timestamp"] = data.pop("created_time")

        # 2) 过滤 None
        data = {k: v for k, v in data.items() if v is not None}

        # 3) 必填字段校验 + 空串校验
        chat_id = str(data.get("chat_id", "")).strip()
        if not chat_id :
            return {"ok": "", "status": "missing_required_field", "required": ["chat_id"]}

        data["chat_id"] = chat_id


        table = "media_auto_send"

        columns = list(data.keys())
        values = list(data.values())

        col_sql = ", ".join(f"`{c}`" for c in columns)
        val_sql = ", ".join(["%s"] * len(values))

        # 4) 不参与 UPDATE 的字段（按你表结构定制）
        skip_update_cols = {
            "media_auto_send_id",
            "chat_id",
            "create_timestamp",
        }

        update_cols = [c for c in columns if c not in skip_update_cols]

        if update_cols:
            update_sql = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
            sql = f"""
                INSERT INTO `{table}` ({col_sql})
                VALUES ({val_sql})
                ON DUPLICATE KEY UPDATE
                {update_sql}
            """
        else:
            sql = f"""
                INSERT IGNORE INTO `{table}` ({col_sql})
                VALUES ({val_sql})
            """

        conn = cur = None
        try:
            conn, cur = await cls.get_conn_cursor()
            await cur.execute(sql, values)
            # 你当前 pool 是 autocommit=True；这里 commit 可留可不留，为保持一致我留着
            await conn.commit()

            return {
                "ok": "1",
                "status": "upserted",
                "media_auto_send_id": cur.lastrowid,
                "affected": cur.rowcount,
            }
        except Exception as e:
            try:
                if conn:
                    await conn.rollback()
            except Exception:
                pass
            return {"ok": "", "status": "error", "error": str(e)}
        finally:
            if conn and cur:
                await cls.release(conn, cur)


    ## User ###
    @classmethod
    async def get_user_point_credit(cls, user_id: int) -> dict:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "SELECT point, credit FROM user WHERE user_id=%s LIMIT 1",
                (int(user_id),)
            )
            row = await cur.fetchone()
            return row or {"point": 0, "credit": 0}
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_contribute_today_count(cls, user_id: int, stat_date: str) -> int:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT count
                FROM contribute_today
                WHERE user_id=%s AND stat_date=%s
                LIMIT 1
                """,
                (int(user_id), stat_date)
            )
            row = await cur.fetchone()
            return int((row or {}).get("count") or 0)
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def get_talking_task_count(cls, user_id: int, stat_date: str) -> int:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT count
                FROM talking_task
                WHERE user_id=%s AND stat_date=%s
                LIMIT 1
                """,
                (int(user_id), stat_date)
            )
            row = await cur.fetchone()
            return int((row or {}).get("count") or 0)
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def is_chatgroup_member(cls, user_id: int, chat_id: int) -> bool:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT id
                FROM chatgroup_member
                WHERE user_id=%s AND chat_id=%s
                LIMIT 1
                """,
                (int(user_id), int(chat_id))
            )
            row = await cur.fetchone()
            return bool(row)
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_file_id_by_file_unique_id(cls, unique_ids: list[str]) -> list[str]:
        if not unique_ids:
            rows = []
        else:
            placeholders = ",".join(["%s"] * len(unique_ids))
            sql = f"""
                SELECT * 
                FROM file_extension
                WHERE file_unique_id IN ({placeholders})
                AND bot = %s
            """
            params = tuple(unique_ids) + (lz_var.bot_username,)

            conn, cur = await MySQLPool.get_conn_cursor()
            try:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
                f_row = {}
                for r in rows or []:
                    row = dict(r)
                    f_row[row["file_unique_id"]] = row
                return f_row
            except Exception as e:
                print(f"⚠️ get_file_id_by_file_unique_id 出错: {e}", flush=True)
                return []
            
                
            finally:
                await MySQLPool.release(conn, cur)
                        
    


    @classmethod
    async def upsert_media(cls, metadata: dict) -> dict:
        """
            meta = {
                "file_type": file_type,
                "file_unique_id": file_unique_id or getattr(mediamessage, "file_unique_id", None),
                "file_id": file_id or getattr(mediamessage, "file_id", None),
                "file_size": file_size or getattr(mediamessage, "file_size", 0) or 0,
                "duration": getattr(mediamessage, "duration", 0) or 0,
                "width": width or getattr(mediamessage, "width", 0) or 0,
                "height": height or getattr(mediamessage, "height", 0) or 0,
                "file_name": getattr(mediamessage, "file_name", "") or "",
                "mime_type": getattr(mediamessage, "mime_type", "") or "",
            }
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            file_type = metadata.get("file_type")
            
            # photo 类型没有 duration 字段
            if file_type == "photo":
                sql2 = f"""
                    INSERT INTO {file_type}
                        (file_unique_id, file_size, width, height, file_name,  create_time, update_time) 
                    VALUES
                        (%s, %s, %s, %s, %s,  NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        file_unique_id = VALUES(file_unique_id),
                        file_size = VALUES(file_size),
                        width = VALUES(width),
                        height = VALUES(height),
                        file_name = VALUES(file_name),
                        update_time = NOW()
                """
                params2 = (
                    metadata.get("file_unique_id"),
                    metadata.get("file_size"),
                    metadata.get("width"),
                    metadata.get("height"),
                    metadata.get("file_name")
                   
                )
            else:
                # video, document 等其他类型有 duration 字段
                sql2 = f"""
                    INSERT INTO {file_type}
                        (file_unique_id, file_size, duration, width, height, file_name, mime_type, create_time) 
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        file_unique_id = VALUES(file_unique_id),
                        file_size = VALUES(file_size),
                        duration = VALUES(duration),
                        width = VALUES(width),
                        height = VALUES(height),
                        file_name = VALUES(file_name),
                        mime_type = VALUES(mime_type)
                """
                params2 = (
                    metadata.get("file_unique_id"),
                    metadata.get("file_size"),
                    metadata.get("duration"),
                    metadata.get("width"),
                    metadata.get("height"),
                    metadata.get("file_name"),
                    metadata.get("mime_type")
                )
            
            await cur.execute(sql2, params2)

            sql = """
                INSERT INTO file_extension
                    (file_type,file_unique_id,file_id,bot,user_id,create_time)   
                VALUES
                    (%s, %s, %s, %s, %s,NOW())
                ON DUPLICATE KEY UPDATE
                    file_type = VALUES(file_type),
                    file_unique_id  = VALUES(file_unique_id ),
                    file_id  = VALUES(file_id ),
                    bot  = VALUES(bot ),
                    user_id = VALUES(user_id)
            """
            params = (
                metadata.get("file_type"),
                metadata.get("file_unique_id"),
                metadata.get("file_id"),
                metadata.get("bot"),
                metadata.get("user_id")
            )
            await cur.execute(sql, params)
            await conn.commit()

            return {"ok": "1", "status": "upserted"}
        except Exception as e:
            try:
                await conn.rollback()
            except Exception:
                pass
            print(f"⚠️ upsert_media 出错: {e}", flush=True)
            return {"ok": "", "status": "error", "error": str(e)}
        finally:
            await cls.release(conn, cur)


    # ========= Generic Sync Helpers =========
    @staticmethod
    def _safe_ident_mysql(name: str) -> str:
        """Very small identifier sanitizer to avoid SQL injection via table/column names."""
        if not isinstance(name, str):
            raise ValueError("identifier must be str")
        name = name.strip()
        if not name:
            raise ValueError("identifier is empty")

        import re
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            raise ValueError(f"invalid identifier: {name}")
        return f"`{name}`"

    @classmethod
    async def fetch_records_updated_after(
        cls,
        table: str,
        timestamp: int,
        update_field: str = "update_at",
        limit: int = 5000,
    ) -> list[dict]:
        """
        通用增量查询：从 MySQL 取出 {update_field} > timestamp 的记录。

        - table: 表名（两库同名）
        - timestamp: big int 时间戳（与你表里的 update_at 对齐）
        - update_field: 默认 'update_at'
        - limit: 防止一次拉太多（默认 5000）

        返回: list[dict]
        """
        await cls.ensure_pool()

        t_sql = cls._safe_ident_mysql(table)
        u_sql = cls._safe_ident_mysql(update_field)
        sql = f"SELECT * FROM {t_sql} WHERE {u_sql} > %s ORDER BY {u_sql} ASC LIMIT %s"

        conn = cur = None
        try:
            conn, cur = await cls.get_conn_cursor()
            await cur.execute(sql, (int(timestamp), int(limit)))
            rows = await cur.fetchall()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print(
                f"⚠️ [MySQLPool] fetch_records_updated_after error: table={table} ts={timestamp} err={e}",
                flush=True,
            )
            return []
        finally:
            if conn and cur:
                await cls.release(conn, cur)
 


    @classmethod
    async def fetch_records_by_pks(
        cls,
        table: str,
        pk_field: str,
        pks: list,
        *,
        limit: int = 5000,
    ) -> list[dict]:
        """
        从 MySQL 按主键列表取记录：SELECT * FROM table WHERE pk IN (...)
        """
        table = (table or "").strip()
        pk_field = (pk_field or "").strip()
        if not pks:
            return []

        # 你的 MySQL 端建议继续沿用你已有的 ident 校验函数（若已实现则复用）
        if hasattr(cls, "_safe_ident_mysql"):
            cls._safe_ident_mysql(table)
            cls._safe_ident_mysql(pk_field)

        # 去重 + 截断，避免 IN 太大
        uniq_pks = list(dict.fromkeys(pks))[: max(1, int(limit))]

        placeholders = ",".join(["%s"] * len(uniq_pks))
        sql = f"SELECT * FROM `{table}` WHERE `{pk_field}` IN ({placeholders})"

        conn, cur = await cls.get_conn_cursor(dict_cursor=True)  # 若你已有这种封装就用
        try:
            await cur.execute(sql, uniq_pks)
            rows = await cur.fetchall()
            return rows or []
        finally:
            try:
                await cur.close()
            finally:
                conn.close()


    #** End of lz_mysql.py **#