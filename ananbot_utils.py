from datetime import datetime
import time
import aiomysql
from ananbot_config import DB_CONFIG
from utils.lybase_utils import LYBase
from utils.string_utils import LZString
from utils.prof import SegTimer
import uuid
import asyncio
import pymysql
from typing import Optional
from lz_memory_cache import MemoryCache
import lz_var
from lz_config import AES_KEY
from utils.aes_crypto import AESCrypto


class AnanBOTPool(LYBase):
    _pool = None
    _pool_lock = asyncio.Lock()  # 新增：并发安全
    _acq_sem = asyncio.Semaphore(16)      # 新增：总限流（前台+后台）
    _bg_sem  = asyncio.Semaphore(4)       # 新增：后台写入专用限流
    _all_tags_grouped_cache = None
    _all_tags_grouped_cache_ts = 0
    _cache_ttl = 300  # 缓存有效时间（秒）
    _all_tags_types_cache = None
    _all_tags_types_cache_ts = 0
    _cache_ready = False
    cache = None

    @classmethod
    async def init_pool_old(cls):
        if cls._pool is None:
            async with cls._pool_lock:
                if cls._pool is None:
                    # ✅ 建议：在 DB_CONFIG 里也可直接配这些键；这里做兜底
                    kwargs = dict(DB_CONFIG)
                    kwargs.setdefault("autocommit", True)
                    kwargs.setdefault("minsize", 1)
                    kwargs.setdefault("maxsize", 10)
                    kwargs.setdefault("connect_timeout", 10)
                    kwargs.setdefault("pool_recycle", 120)  # 关键：120 秒回收陈旧连接
                    kwargs.setdefault("charset", "utf8mb4")

                    cls._pool = await aiomysql.create_pool(**kwargs)
                    print("✅ MySQL 连接池初始化完成（autocommit, recycle=120）")
            
            if not cls._cache_ready:
                cls.cache = MemoryCache()
                cls._cache_ready = True
            

    @classmethod
    async def init_pool(cls):
        if cls._pool is None:
            async with cls._pool_lock:
                if cls._pool is None:
                    kwargs = dict(DB_CONFIG)
                    kwargs.setdefault("autocommit", True)
                    kwargs.setdefault("minsize", 2)
                    kwargs.setdefault("maxsize", 40)
                    kwargs.setdefault("connect_timeout", 10)
                    kwargs.setdefault("pool_recycle", 110)  # 建议略小于 MySQL wait_timeout
                    kwargs.setdefault("charset", "utf8mb4")

                    delay = 0.3
                    for i in range(4):
                        try:
                            cls._pool = await aiomysql.create_pool(**kwargs)
                            print("✅ MySQL 连接池初始化完成")
                            break
                        except Exception as e:
                            if i == 3:
                                raise
                            await asyncio.sleep(delay)
                            delay *= 2

            if not cls._cache_ready:
                cls.cache = MemoryCache()
                cls._cache_ready = True



    @classmethod
    async def _reset_pool_old(cls):
        if cls._pool:
            cls._pool.close()
            await cls._pool.wait_closed()
            cls._pool = None

    
    @classmethod
    async def _reset_pool(cls):
        async with cls._pool_lock:
            if cls._pool:
                try:
                    cls._pool.close()
                    # 防止被外层 wait_for 取消，且限制等待时长
                    await asyncio.wait_for(asyncio.shield(cls._pool.wait_closed()), timeout=20)
                except Exception as e:
                    print(f"⚠️ wait_closed 异常(忽略): {e}")
                finally:
                    cls._pool = None



    @classmethod
    async def get_conn_cursor_old(cls):
        # if cls._pool is None:
        #     raise Exception("MySQL 连接池未初始化，请先调用 init_pool()")
        try:
            conn = await cls._pool.acquire()
            try:
                await conn.ping()  # ✅ 轻量自检，触发底层重连
            except Exception:
                # 连接已坏：重建连接池再取
                await cls._reset_pool()
                await cls.init_pool()
                conn = await cls._pool.acquire()
                await conn.ping()
            cursor = await conn.cursor(aiomysql.DictCursor)
            return conn, cursor
        except Exception as e:
            # 最后兜底：彻底重置再试一次
            await cls._reset_pool()
            await cls.init_pool()
            conn = await cls._pool.acquire()
            cursor = await conn.cursor(aiomysql.DictCursor)
            return conn, cursor
    
    @classmethod
    async def get_conn_cursor(cls):
        if cls._pool is None:
            await cls.init_pool()
        # ☆ 关键：统一经由总信号量限流
        async with cls._acq_sem:
            try:
                conn = await asyncio.wait_for(cls._pool.acquire(), timeout=20)
                try:
                    await conn.ping()
                except Exception:
                    await cls._reset_pool()
                    await cls.init_pool()
                    conn = await asyncio.wait_for(cls._pool.acquire(), timeout=20)
                    await conn.ping()
                cursor = await conn.cursor(aiomysql.DictCursor)
                return conn, cursor
            except Exception:
                await cls._reset_pool()
                await cls.init_pool()
                conn = await asyncio.wait_for(cls._pool.acquire(), timeout=20)
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
    def _is_transient_mysql_error(cls, e: BaseException) -> bool:
        # 2006: MySQL server has gone away
        # 2013: Lost connection to MySQL server during query
        # 104 : Connection reset by peer（部分封装在 args[0]）
        codes = {2006, 2013, 104}
        try:
            code = getattr(e, "args", [None])[0]
            return code in codes
        except Exception:
            return False

    @classmethod
    async def upsert_media(cls, file_type, data: dict):
        if file_type == 'v':
            file_type = 'video'
        elif file_type == 'p':
            file_type = 'photo'
        elif file_type == 'd':
            file_type = 'document'
        elif file_type == 'n':
            file_type = 'animation'



        if file_type != "video":
            data.pop("duration", None)
        if file_type == "document":
            data.pop("width", None)
            data.pop("height", None)

        keys = list(data.keys())
        placeholders = ', '.join(['%s'] * len(keys))
        columns = ', '.join(f"`{k}`" for k in keys)
        updates = ', '.join(f"`{k}`=VALUES(`{k}`)" for k in keys if k != "create_time")
        sql = f"""
            INSERT INTO `{file_type}` ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {updates}
        """
        params = [data[k] for k in keys]

        # ✅ 带重试（指数退避：0.2, 0.4）
        delay = 0.2
        attempts = 3
        for i in range(attempts):
            conn, cur = await cls.get_conn_cursor()
            try:
                await cur.execute(sql, params)
                return
            except (pymysql.err.OperationalError, aiomysql.OperationalError, ConnectionResetError) as e:
                if not cls._is_transient_mysql_error(e) or i == attempts - 1:
                    raise
                # 连接可能已坏，重置连接池 + 退避后重试
                await cls._reset_pool()
                await asyncio.sleep(delay)
                delay *= 2
            finally:
                await cls.release(conn, cur)



    @classmethod
    async def upsert_media_bulk(cls, rows: list[dict], *, batch_size: int = 200, show_sql: bool = False):
        """
        批量 upsert 各媒体表（video/document/photo/animation）。
        rows 每项格式类似：
        {
            "file_type": "video",
            "file_unique_id": "AgAD...",
           
            "file_size": 123456,
            "duration": 5,
            "width": 640,
            "height": 360,
            "file_name": "xxx.mp4",
            "create_time": datetime.now()
        }
        """
        if not rows:
            print("⚠️ upsert_media_bulk: 空列表，跳过。")
            return 0

        await cls.init_pool()
        inserted_total = 0

        # 按 file_type 分组：video/document/photo/animation
        groups = {}
        for r in rows:
            ft = r.get("file_type")
            if ft in ("v", "video"): ft = "video"
            elif ft in ("d", "document"): ft = "document"
            elif ft in ("p", "photo"): ft = "photo"
            elif ft in ("n", "animation"): ft = "animation"
            else:
                continue
            groups.setdefault(ft, []).append(r)

        conn, cur = await cls.get_conn_cursor()
        try:
            for ft, items in groups.items():
                if not items:
                    continue

                # 清理字段：不同类型字段不同
                clean_rows = []
                for item in items:
                    d = dict(item)
                    d.pop("file_type", None)  # 表名用，不入列
                    d.pop("file_id", None)    # ❗ 关键：媒体表不存 file_id
                    if ft != "video":
                        d.pop("duration", None)
                    if ft == "document":
                        d.pop("width", None)
                        d.pop("height", None)
                    if ft == "photo":
                        d.pop("mime_type", None)
                    d['create_time'] = d.get('create_time') or datetime.now()
                    clean_rows.append(d)
                # print(f"clean_rows=>{clean_rows}")
                # 取出所有列名
                keys = sorted({k for r in clean_rows for k in r.keys()})
                # 删除 file_type (表名用，不入列)
                if "file_type" in keys:
                    keys.remove("file_type")

                placeholders = ",".join(["%s"] * len(keys))
                columns = ",".join(f"`{k}`" for k in keys)
                updates = ",".join(f"`{k}`=VALUES(`{k}`)" for k in keys if k != "create_time")

                base_sql = f"""
                    INSERT INTO `{ft}` ({columns})
                    VALUES {{values}}
                    ON DUPLICATE KEY UPDATE {updates}
                """

                # 分批
                from math import ceil
                total_batches = ceil(len(clean_rows) / batch_size)
                for b in range(total_batches):
                    chunk = clean_rows[b*batch_size:(b+1)*batch_size]
                    vals = []
                    for r in chunk:
                        vals.extend([r.get(k) for k in keys])

                    value_block = ",".join(["(" + placeholders + ")"] * len(chunk))
                    sql = base_sql.format(values=value_block)

                    # 🧩 调试模式：打印完整 SQL（含值）
                    if show_sql:
                        def fmt(v):
                            if v is None: return "NULL"
                            elif isinstance(v, (int, float)): return str(v)
                            elif isinstance(v, datetime): return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
                            else: return f"'{str(v).replace("'", "''")}'"
                        formatted_chunks = [
                            "(" + ",".join(fmt(r.get(k)) for k in keys) + ")" for r in chunk
                        ]
                        print(f"\n📦 [Table {ft}] 批次 {b+1}/{total_batches} 实际执行 SQL：")
                        print(base_sql.format(values=",".join(formatted_chunks)))

                    await cur.execute(sql, vals)
                    inserted_total += cur.rowcount
                    if show_sql:
                        print(f"✅ [{ft}] 批次 {b+1} 完成，影响行数：{cur.rowcount}")

            await conn.commit()
            print(f"✅ upsert_media_bulk 完成，总影响行数：{inserted_total}")
        except Exception as e:
            await conn.rollback()
            print(f"❌ upsert_media_bulk 失败: {e}")
            raise
        finally:
            await cls.release(conn, cur)

        return inserted_total


    @classmethod
    async def insert_file_extension(cls, file_type, file_unique_id, file_id, bot_username, user_id):
        # 后台写入路径使用更小并发，避免争抢
        async with cls._bg_sem:
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


    # in ananbot_utils.py (AnanBOTPool class)
    @classmethod
    async def insert_file_extension_bulk(cls, rows: list[dict], *, batch_size: int = 300):
        """
        rows: [{file_type, file_unique_id, file_id, bot_username|bot, user_id, create_time?}, ...]
        批量写入 file_extension；分批多值 INSERT + ON DUPLICATE KEY UPDATE
        """
        if not rows:
            return 0

        await cls.init_pool()
        inserted_total = 0

        # 预处理/清洗字段名
        normed = []
        now = datetime.now()
        for r in rows:
            ft   = r.get("file_type")
            fuid = r.get("file_unique_id")
            fid  = r.get("file_id")
            bot  = r.get("bot") or r.get("bot_username")
            uid  = r.get("user_id")
            if not (ft and fuid and fid and bot):
                continue
            normed.append((
                ft, fuid, fid, bot, uid, r.get("create_time") or now
            ))

        if not normed:
            return 0

        sql = """
            INSERT INTO file_extension
                (file_type, file_unique_id, file_id, bot, user_id, create_time)
            VALUES
                {values}
            ON DUPLICATE KEY UPDATE
                -- 若 unique 冲突（(file_unique_id,file_id) 或 (file_id,bot)）：
                -- 保持 file_id 不变，回写最新 bot/user_id/create_time
                bot        = VALUES(bot),
                user_id    = IFNULL(VALUES(user_id), user_id),
                create_time= VALUES(create_time)
        """

        # 分批执行，控制单次 SQL 体量
        from math import ceil
        total_batches = ceil(len(normed) / batch_size)

        conn, cur = await cls.get_conn_cursor()
        try:
            for i in range(total_batches):
                chunk = normed[i*batch_size:(i+1)*batch_size]
                # 组装多值占位
                placeholders = ",".join(["(%s,%s,%s,%s,%s,%s)"] * len(chunk))
                flat_params = []
                for tup in chunk:
                    flat_params.extend(tup)

                await cur.execute(sql.format(values=placeholders), flat_params)
                inserted_total += cur.rowcount  # 注意：包含“插入/更新”的受影响行计数
                # 显示执行的sql
               
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            await cls.release(conn, cur)

        return inserted_total


    @classmethod
    async def insert_file_extension_old(cls, file_type, file_unique_id, file_id, bot_username, user_id):
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
                    owner_user_id = IF(
                        sora_content.owner_user_id IS NULL OR sora_content.owner_user_id = 0,
                        VALUES(owner_user_id),
                        sora_content.owner_user_id
                    ),        
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
            content_rows = 0
            print(f"tfud={thumb_file_unique_id} tfid={thumb_file_id} cid={content_id} bot={bot_username}", flush=True)
            if thumb_file_unique_id!="":
                # 1) 嘗試更新 sora_content（若該 content_id 不存在則不會有影響）
                await cur.execute(
                    """
                    UPDATE sora_content
                    SET thumb_file_unique_id = %s
                    WHERE id = %s 
                    """,
                    (thumb_file_unique_id, content_id)
                )
                # 打印执行的SQL
                # cur_sql = cur._last_executed
                # print(f"✅ [X-MEDIA] 执行的 SQL: {cur_sql}", flush=True)

                content_rows = cur.rowcount  # 受影響筆數（0 代表該 content_id 不存在）
                print(f"✅ [X-MEDIA] 更新 sora_content 縮略圖(thumb_file_unique_id)", flush=True)

            # 2) 對 sora_media 做 UPSERT（不存在則插入，存在則更新 thumb_file_id）
            # 依賴唯一鍵 uniq_content_bot (content_id, source_bot_name)
            print(f"✅ [X-MEDIA] 正在 UPSERT sora_media 縮略圖...", flush=True)
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
            # media_rows = cur.rowcount

            # cur_sql = cur._last_executed
            # print(f"✅ [X-MEDIA] 执行的 SQL: {cur_sql}", flush=True)

            # print(f"✅ [X-MEDIA] UPSERT sora_media 縮略圖完成，受影響筆數：{media_rows}", flush=True)
            await conn.commit()

            print(f"✅ [X-MEDIA] 縮略圖更新事務完成", flush=True)

            return {
                "sora_content_updated_rows": content_rows,
                # "sora_media_upsert_rowcount": media_rows
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
    async def update_product_password(cls, content_id: int, password: str, user_id: int = 0, overwrite: int = 0):
        """
        更新 sora_content.file_password
        - password: 字符串（允许空 => 清除密码）
        - overwrite: 保留参数结构与 update_product_content 一致（暂时不需要处理媒体表）
        """
        timer = SegTimer("update_product_password", content_id=content_id, overwrite=int(overwrite))

        try:
            await cls.init_pool()
            async with cls._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    timer.lap("acquire_conn_and_cursor")

                    # 这里不需要像 update_product_content 那样处理媒体表 caption，
                    # 只需更新 sora_content.file_password。
                    await cur.execute(
                        """
                        UPDATE sora_content
                           SET file_password = %s,
                               stage         = 'pending'
                         WHERE id = %s
                        """,
                        (password, content_id)
                    )
                    timer.lap("update_sora_content_password")

                await conn.commit()
                timer.lap("commit")

        except Exception as e:
            try:
                await conn.rollback()
            except Exception:
                pass
            print(f"[update_product_password] ERROR: {e}", flush=True)
            raise
        finally:
            timer.end()


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


    @classmethod
    async def get_or_create_pending_product(cls, user_id: int) -> int:
        """
        若该 user 已有 review_status=0/1 的产品 → 直接回传 product.content_id
        若无 → 新建 sora_content(file_type='album') 再建 product，回传 content_id
        """
        await cls.init_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            # 1) 查找是否已有未完成商品（review_status=0 or 1）
            await cur.execute(
                """
                SELECT content_id
                  FROM product
                 WHERE owner_user_id=%s
                   AND review_status IN (0,1)
                 LIMIT 1
                """,
                (user_id,)
            )
            row = await cur.fetchone()
            if row:
                # 已存在未完成商品 → 直接回传
                return int(row["content_id"])

            # 2) 没有 → 新建 sora_content
            # 生成随机不重复 source_id（长度 36）
            source_id = uuid.uuid4().hex
            # 只取前28个字符，避免过长
            source_id = f"X_{source_id[:27]}"

            await cur.execute(
                """
                INSERT INTO sora_content (source_id, file_type, owner_user_id, stage)
                VALUES (%s, 'album', %s, 'pending')
                """,
                (source_id, user_id)
            )

            # 取得新 content_id
            await cur.execute("SELECT LAST_INSERT_ID() AS cid")
            row = await cur.fetchone()
            if not row:
                raise Exception("Failed to create sora_content")
            content_id = int(row["cid"])

            # 3) 建立 product（文件类型 album）
            # await cls.create_product(content_id, name='', desc='', price=34, file_type='album', user_id=user_id)
            await cur.execute(
                """
                INSERT INTO product
                    (content_id, file_type, owner_user_id, review_status, price,stage)
                VALUES
                    (%s, 'album', %s, 0, 34,'pending')
                """,
                (content_id, user_id)
            )

            await conn.commit()
            return content_id

        except Exception:
            try:
                await conn.rollback()
            except:
                pass
            raise
        finally:
            await cls.release(conn, cur)


    #要和 lz_db.py 作整合
    @classmethod
    async def search_sora_content_by_id(cls, content_id: int,bot_username: str):

        # cache_key = f"content_id:{content_id}"
        # cached = cls.cache.get(cache_key)
        # if cached:
        #     print(f"🔹 MemoryCache hit for {cache_key}")
        #     return cached
        
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute('''
                SELECT s.id, s.source_id, s.file_type, s.content, s.file_size, s.duration, s.tag,
                    s.thumb_file_unique_id, s.file_password,
                    m.file_id AS m_file_id, m.thumb_file_id AS m_thumb_file_id,
                    p.price as fee, p.file_type as product_type, p.owner_user_id, p.purchase_condition, p.review_status, p.anonymous_mode, p.id as product_id,
                    g.guild_id, g.guild_keyword, g.guild_resource_chat_id, g.guild_resource_thread_id, g.guild_chat_id, g.guild_thread_id
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

            # cls.cache.set(cache_key, row, ttl=300)

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
        print(f"▶️ 正在获取缩略图(get_preview_thumb_file_id) file_id for content_id: {content_id} by bot: {bot_username}", flush=True)
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
    async def get_default_preview_thumb_file_id(cls, bot_username: str, file_unqiue_id: str):
        print(f"▶️ 正在获取缩略图(get_default_preview_thumb_file_id) file_id for file_unqiue_id: {file_unqiue_id} by bot: {bot_username}", flush=True)
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "SELECT file_id, bot FROM file_extension WHERE file_unique_id = %s AND bot = %s",
                (file_unqiue_id,bot_username)
            )
            row = await cur.fetchone()
            
            if row and row["file_id"]:
                return row["file_id"]
            
                

        finally:
            await cls.release(conn, cur)




    @classmethod
    async def insert_album_item(cls, content_id: int, member_content_id: int, file_unique_id: str | None = None, file_type: str | None = None, position: int = 0):
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                INSERT IGNORE INTO album_items (
                    content_id, member_content_id, file_unique_id, file_type, position, created_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (content_id, member_content_id, file_unique_id, file_type, position)
            )
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_album_list(cls, content_id: int, bot_name: str) -> list[dict]:
        """
        查询某个 album 下的所有成员文件（MySQL 版，适配 aiomysql）
        - 逻辑同步 PostgreSQL 版本：
        1) 取 album_items -> sora_content -> sora_media（按 bot 过滤）；
        2) 若 m.file_id 为空且 file_extension 能匹配出 ext_file_id，则回填到返回值；
        3) 批量 UPSERT 回写 sora_media(content_id, source_bot_name) 的 file_id。
        - 返回：list[dict]
        """
        # 说明：
        # - 需要唯一键/联合索引：sora_media(content_id, source_bot_name) UNIQUE
        # - JOIN file_extension 使用 (file_unique_id, bot)
        sql = """
            SELECT
                c.member_content_id,           -- 回写 sora_media.content_id 使用
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
                AND m.source_bot_name   = %s
            LEFT JOIN file_extension AS fe
                ON fe.file_unique_id = s.source_id
                AND fe.bot            = %s
            WHERE c.content_id = %s
            ORDER BY c.file_type
        """
        params = (bot_name, bot_name, content_id)

        upsert_sql = """
            INSERT INTO sora_media (content_id, source_bot_name, file_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE file_id = VALUES(file_id)
        """

        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(sql, params)
            rows = await cur.fetchall() or []

            dict_rows: list[dict] = []
            to_upsert: list[tuple[int, str, str]] = []  # (content_id, bot_name, file_id)

            for d in rows:
                # d 已是 dict（DictCursor）
                file_id = d.get("file_id")
                ext_id  = d.get("ext_file_id")
                # 如果 m.file_id 为空且扩展表能命中，则用 ext_file_id 回填返回值并准备回写
                if not file_id and ext_id:
                    d["file_id"] = ext_id
                    cid = d.get("member_content_id")
                    if cid:
                        to_upsert.append((int(cid), bot_name, str(ext_id)))
                dict_rows.append(d)

            # 批量 UPSERT 回写 sora_media
            if to_upsert:
                await cur.executemany(upsert_sql, to_upsert)
                await conn.commit()

            return dict_rows
        except Exception:
            try:
                await conn.rollback()
            except Exception:
                pass
            raise
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def get_album_list_old(cls, content_id: int, bot_name: str):
        # TODO  写法和 lz_db.py 目前不同, 需保持一致，方便整合
        sql = """
            SELECT s.source_id, c.file_type, s.content, s.file_size, s.duration,
                   m.source_bot_name, m.thumb_file_id, m.file_id
            FROM album_items c
            LEFT JOIN sora_content s ON c.member_content_id = s.id
            LEFT JOIN sora_media m ON c.member_content_id = m.content_id AND m.source_bot_name = %s
            WHERE c.content_id = %s
            ORDER BY c.file_type
        """
        params = (bot_name, content_id )
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
                await cur.execute("SELECT type_code, type_cn FROM tag_type WHERE type_code NOT IN ('xiaoliu','system','serial','gallery','gallery_set') " \
                "ORDER BY FIELD(type_code, 'age', 'face', 'act', 'nudity','par', 'fetish','att', 'feedback', 'pro','eth', 'position', 'hardcore', 'position', 'hardcore')")
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
        cache_key = f"file_unique_id:{file_unique_id}"
        cached = cls.cache.get(cache_key)
        if cached:
            print(f"🔹 MemoryCache hit for {cache_key}")
            return cached
        
        
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id,source_id as file_unique_id 
                    FROM sora_content 
                    WHERE source_id = %s
                """, (file_unique_id,))
                row = await cur.fetchone()
                content_id = row[0] if row else ""
                cls.cache.set(cache_key, content_id, ttl=30000)
                return content_id



    @classmethod
    async def get_content_ids_by_fuids(cls, fuids: list[str]) -> dict[str, int]:
        """
        批量查询 source_id -> content_id 的映射。
        """
        if not fuids:
            return {}
        await cls.init_pool()
        # 去重避免 IN 过长
        fuids = list({f for f in fuids if f})
        placeholders = ",".join(["%s"] * len(fuids))
        sql = f"""
            SELECT id, source_id 
            FROM sora_content 
            WHERE source_id IN ({placeholders})
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(sql, fuids)
            rows = await cur.fetchall() or []
            return {r["source_id"]: int(r["id"]) for r in rows}
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def insert_sora_content_media_bulk(cls, rows: list[dict], *, batch_size: int = 200) -> list[dict]:
        """
        批量 upsert 缺失的成员到 sora_content + 初始化 sora_media（file_id 可为空）
        rows 每项：{
            "file_unique_id": "...",   # source_id
            "file_type": "v|d|p|n",    # 短码/全名均可，内部统一
            "file_size": int,
            "duration": int,
            "owner_user_id": str|int,
            "file_id": str|None,
            "bot_username": str
        }
        返回：包含每条 {file_unique_id, content_id} 的列表
        """
        if not rows:
            return []

        # 统一类型
        def norm_ft(ft: str) -> str:
            m = {"v":"video","video":"video","d":"document","document":"document",
                "p":"photo","photo":"photo","n":"animation","animation":"animation","a":"album","album":"album"}
            return m.get((ft or "").lower(), "document")

        await cls.init_pool()
        # 先对 sora_content 做批量 upsert
        from math import ceil
        # 只保留必要列（sora_content 不吃 file_id/bot）
        sc_rows = []
        for r in rows:
            sc_rows.append({
                "source_id": r.get("file_unique_id"),
                "file_type": norm_ft(r.get("file_type")),
                "file_size": r.get("file_size", 0),
                "duration": r.get("duration", 0),
                "owner_user_id": r.get("owner_user_id", 0),
            })
        # 过滤空 source_id
        sc_rows = [r for r in sc_rows if r["source_id"]]

        # 组装多值 INSERT ... ON DUPLICATE
        keys = ["source_id","file_type","file_size","duration","owner_user_id"]
        placeholders = ",".join(["%s"] * len(keys))
        updates = ",".join(f"`{k}`=VALUES(`{k}`)" for k in keys if k != "owner_user_id") + ", `stage`='pending'"
        base_sql = f"""
            INSERT INTO sora_content ({",".join(keys)}, stage)
            VALUES {{values}}
            ON DUPLICATE KEY UPDATE {updates}
        """

        conn, cur = await cls.get_conn_cursor()
        try:
            # 分批 upsert sora_content
            total_batches = ceil(len(sc_rows) / batch_size) or 1
            for i in range(total_batches):
                chunk = sc_rows[i*batch_size:(i+1)*batch_size]
                if not chunk:
                    continue
                vals, blocks = [], []
                for r in chunk:
                    vals.extend([r[k] for k in keys] + ["pending"])
                    blocks.append(f"({placeholders}, %s)")  # 多一个 stage
                sql = base_sql.format(values=",".join(blocks))
                await cur.execute(sql, vals)
            # 为了拿 content_id，再查回（一次性 IN）
            fuids = [r["source_id"] for r in sc_rows]
            mapping = await cls.get_content_ids_by_fuids(fuids)

            # 初始化 sora_media（把 file_id 与 bot_username 批量 upsert）
            sm_triplets = []
            for r in rows:
                fuid = r.get("file_unique_id")
                cid = mapping.get(fuid)
                if not cid:
                    continue
                sm_triplets.append((
                    int(cid),
                    r.get("bot_username"),
                    r.get("file_id") or "",
                ))

            if sm_triplets:
                await cur.executemany(
                    """
                    INSERT INTO sora_media (content_id, source_bot_name, file_id)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    file_id = VALUES(file_id)
                    """,
                    sm_triplets
                )

            await conn.commit()
            return [{"file_unique_id": f, "content_id": mapping.get(f)} for f in fuids if mapping.get(f)]
        except Exception:
            try:
                await conn.rollback()
            except:
                pass
            raise
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def insert_album_items_bulk(cls, content_id: int, members: list[tuple[int, str, int]]):
        """
        批量插入 album_items：
        members: [(member_content_id, file_type_short, position), ...]
        file_type_short: 'v'/'d'/'p'/'n'
        """
        if not members:
            return 0
        await cls.init_pool()
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.executemany(
                """
                INSERT IGNORE INTO album_items (content_id, member_content_id, file_type, position)
                VALUES (%s, %s, %s, %s)
                """,
                [(content_id, mid, ft, pos) for (mid, ft, pos) in members]
            )
            affected = cur.rowcount
            await conn.commit()
            return affected
        except Exception:
            try:
                await conn.rollback()
            except:
                pass
            raise
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def finalize_content_fields(cls, candidates,content_id, user_id, bot_username):
        # 5) 批量补齐所有成员 content_id
        fuids = [c.get("file_unique_id") for c in candidates if c.get("file_unique_id")]
        exist_map = await cls.get_content_ids_by_fuids(fuids)  # {fuid: content_id}

        missing_rows = []
        for c in candidates:
            fuid = c.get("file_unique_id")
            if not fuid:
                continue
            if fuid not in exist_map:
                missing_rows.append({
                    "file_unique_id": fuid,
                    "file_type": c.get("file_type"),
                    "file_size": c.get("file_size", 0),
                    "duration": c.get("duration", 0),
                    "owner_user_id": user_id,
                    "file_id": c.get("file_id"),
                    "bot_username": bot_username,
                })

        # 批量插入缺失成员（含初始化 sora_media）
        if missing_rows:
            inserted = await cls.insert_sora_content_media_bulk(missing_rows)
            for r in inserted:
                exist_map[r["file_unique_id"]] = int(r["content_id"])

        # 6) 批量写入 album_items（按 candidates 顺序设置 position）
        members = []
        pos = 1
        def to_short(ft: str) -> str:
            FT_SHORT = {"video": "v", "document": "d", "photo": "p", "animation": "n", "album": "a"}
            if not ft: return "d"
            ft = ft.lower()
            if ft in ("v","d","p","n","a"): return ft
            return FT_SHORT.get(ft, "d")

        for c in candidates:
            fuid = c.get("file_unique_id")
            member_cid = exist_map.get(fuid)
            if not member_cid:
                continue
            members.append( (int(member_cid), to_short(c.get("file_type")), pos) )
            pos += 1

        if members:
            await cls.insert_album_items_bulk(content_id, members)

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
            cls.init_pool()
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
    async def get_product_info_by_fuid(cls, file_unique_id: str):
        """
        读取当前 bid_status（可选的幂等检查用）。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                "SELECT p.id as product_id,p.owner_user_id,p.review_status,s.thumb_file_unique_id,s.id as content_id FROM sora_content s LEFT JOIN product p ON p.content_id = s.id WHERE s.source_id=%s",
                (file_unique_id,)
            )
            row = await cur.fetchone()
            return row
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
    async def sumbit_to_review_product(cls, content_id: int, review_status:int, owner_user_id: int) -> int:
        """
        将 product.review_status 设为指定值（默认 1），返回受影响行数。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                UPDATE product
                   SET review_status=%s,
                       updated_at=NOW(), 
                       	owner_user_id = %s,
                       stage='pending'
                 WHERE content_id=%s
                """,
                (review_status, owner_user_id, content_id)
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
            "p": "photo",    "photo": "photo"
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
                (content, content_id)
            )
            if( ft_norm == 'video' or ft_norm == 'document'):
                await cur.execute(
                    f"UPDATE `{ft_norm}` SET caption = %s, kc_id = %s, update_time = NOW(), kc_status = 'pending' WHERE file_unique_id = %s",
                    (content, content_id, src_id)
                )
            else:
                await cur.execute(
                    f"UPDATE `{ft_norm}` SET caption = %s, kc_id = %s, kc_status = 'pending' WHERE file_unique_id = %s",
                    (content, content_id, src_id)
                )

            await cur.execute(
                "UPDATE product SET content = %s, stage='pending' WHERE content_id = %s",
                (content, content_id)
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
        guild_id = None
        try:
            # 1) 取 sora_content 基本信息
            await cur.execute(
                "SELECT source_id FROM sora_content WHERE id = %s LIMIT 1",
                (content_id,)
            )
            file_row = await cur.fetchone()
            if not file_row:
                # 没有这条 content_id
                print(f"no content id")
                return False
 
            # 2) 取归属的 guild_id
            await cur.execute(
                "SELECT a.guild_id FROM `file_tag` t LEFT JOIN tag a ON a.tag = t.tag WHERE t.`file_unique_id` LIKE %s AND a.guild_id IS NOT NULL AND a.guild_id > 0 ORDER BY a.quantity ASC limit 1;",
                # "SELECT g.guild_id FROM `file_tag` t LEFT JOIN guild g ON g.guild_tag = t.tag WHERE t.`file_unique_id` LIKE %s AND g.guild_id IS NOT NULL AND t.quantity > 0 ORDER BY t.quantity ASC limit 1;",
                (file_row["source_id"],)
            )
            file_tag_row = await cur.fetchone()
            if not file_tag_row:
                guild_id = 16
                print(f"no tag")
            else:
                guild_id = file_tag_row["guild_id"]

            print(f"file_tag_row={file_tag_row}")

            await cur.execute(
                """
                UPDATE product
                   SET guild_id=%s,
                       updated_at=NOW(), stage='pending'
                 WHERE content_id=%s
                """,
                (guild_id, content_id)
            )
            affected = cur.rowcount
            await conn.commit()
            return guild_id or None
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
                    (f"%{user_id};%",)
                )

            # 普通用户检查 guild_manager 字段
            elif role == "manager":
                await cur.execute(
                    "SELECT * FROM guild WHERE guild_manager LIKE %s LIMIT 1;",
                    (f"%{user_id};%",)
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
        content_id = await AnanBOTPool.get_content_id_by_file_unique_id(file_unique_id)
        aes = AESCrypto(AES_KEY)
        encoded = aes.aes_encode(content_id)
        shared_url = f"https://t.me/{lz_var.bot_username}?start=f_-1_{encoded}"
        return shared_url

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

    @classmethod
    async def get_next_report_to_judge(cls) -> dict | None:
        """
        查询是否已有针对该 file_unique_id 的举报（状态在 editing/pending/published/failed）。
        有则返回第一条；无则 None。
        """
        sql = """
            SELECT * FROM report 
             WHERE process_status IN ('pending')
             LIMIT 1
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(sql)
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

    # ====== ② DB 辅助函数：取出所有 review_status = 2 的 content_id ======
    @classmethod
    async def fetch_review_status_content_ids(cls, review_status_id:int, quantity:int = 10) -> list[int]:
        """
        返回需要发送到审核群组的 content_id 列表（product.review_status = 2）
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # 这里假设 product 表字段为 content_id、review_status
            # 若你表结构不同，把列名改为实际名称即可
            await cur.execute(
                "SELECT content_id FROM product WHERE review_status = %s ORDER BY RAND() LIMIT %s",
                (review_status_id, quantity)
            )
            rows = await cur.fetchall()
            # aiomysql.DictCursor 时：row 是 dict；普通 Cursor 时：row 是 tuple
            ids = []
            for row in rows:
                if isinstance(row, dict):
                    ids.append(int(row.get("content_id")))
                else:
                    ids.append(int(row[0]))
            return ids
        finally:
            await cls.release(conn, cur)


    # =======================
    # Series 相关
    # =======================
    @classmethod
    async def get_all_series(cls) -> list[dict]:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute("SELECT id, name, description, tag FROM series ORDER BY name ASC")
            return await cur.fetchall()
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def get_series_ids_for_file(cls, file_unique_id: str) -> set[int]:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute("SELECT series_id FROM file_series WHERE file_unique_id=%s", (file_unique_id,))
            rows = await cur.fetchall()
            ids = set()
            for r in rows:
                ids.add(int(r["series_id"] if isinstance(r, dict) else r[0]))
            return ids
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def sync_file_series(cls, file_unique_id: str, selected_ids: set[int]) -> dict:
        """
        与 file_series 表做一次性同步（类似 sync_file_tags 风格）：
        - 新增：selected_ids - db_ids
        - 删除：db_ids - selected_ids
        返回 {added, removed, unchanged}
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            # 现有
            await cur.execute("SELECT series_id FROM file_series WHERE file_unique_id=%s", (file_unique_id,))
            rows = await cur.fetchall()
            db_ids = {int(r["series_id"] if isinstance(r, dict) else r[0]) for r in rows}

            to_add = list(selected_ids - db_ids)
            to_del = list(db_ids - selected_ids)

            if to_add:
                await cur.executemany(
                    "INSERT INTO file_series (file_unique_id, series_id) VALUES (%s, %s)",
                    [(file_unique_id, sid) for sid in to_add]
                )
            if to_del:
                await cur.executemany(
                    "DELETE FROM file_series WHERE file_unique_id=%s AND series_id=%s",
                    [(file_unique_id, sid) for sid in to_del]
                )
            await conn.commit()

            return {"added": len(to_add), "removed": len(to_del), "unchanged": len(selected_ids & db_ids)}
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def sync_bid_product(cls) -> dict:
        """
        同步 bid → product：
        - 取 10 笔 b.content_id IS NULL 且能在 sora_content 命中的记录
        - 以一条 INSERT ... ON DUPLICATE KEY UPDATE 融合插入/更新 product
        - product.content = [sora_content.id]（以字符串写入，与你现有风格一致）
        - product.price = lz_var.default_point
        - product.file_type = [bid.type]
        - product.stage = 'pending'
        - 回写 bid.content_id = [sora_content.id]
        """
        print("▶️ 正在同步 bid → product ...", flush=True)
        conn, cur = await cls.get_conn_cursor()
        fetched = upserted = bid_updated = 0
        try:
            # 1) 抓取最多 10 笔候选
            await cur.execute("""
                SELECT b.file_unique_id,
                    COALESCE(b.type, '') AS file_type,
                    s.id AS content_id
                FROM bid b
                LEFT JOIN sora_content s ON b.file_unique_id = s.source_id
                WHERE b.content_id IS NULL AND CHAR_LENGTH(s.content) > 30
                AND b.file_unique_id IS NOT NULL
                AND s.id IS NOT NULL
                LIMIT 10
            """)
            rows = await cur.fetchall()
            fetched = len(rows)
            if not rows:
                await conn.commit()
                return {"fetched": 0, "product_upserted": 0, "bid_updated": 0}

            # 2) 逐笔 UPSERT product 并回写 bid
            for r in rows:
                file_unique_id = r["file_unique_id"]
                file_type      = r["file_type"] or ""
                content_id     = int(r["content_id"])

                # 单条 SQL 融合插入/更新
                await cur.execute(
                    """
                    INSERT INTO product
                        ( price, content_id, file_type, review_status, stage, owner_user_id)
                    VALUES
                        (   %s,      %s,    %s,        2,          'pending', 666666)
                    ON DUPLICATE KEY UPDATE
                        price     = VALUES(price),
                        file_type = VALUES(file_type),
                        stage     = 'pending'
                    """,
                    (  lz_var.default_point, content_id, file_type, )
                )
                upserted += 1

                # 回写 bid.content_id
                await cur.execute(
                    "UPDATE bid SET content_id = %s WHERE file_unique_id = %s",
                    (content_id, file_unique_id)
                )
                bid_updated += 1

            await conn.commit()
            return {"fetched": fetched, "product_upserted": upserted, "bid_updated": bid_updated}

        except Exception as e:
            try:
                await conn.rollback()
            except Exception:
                pass
            print(f"[sync_bid_product] ERROR: {e}", flush=True)
            raise
        finally:
            await cls.release(conn, cur)


