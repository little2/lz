import aiomysql
import time
from lz_config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_DB_PORT
from typing import Optional, Dict, Any, List, Tuple

import lz_var


class MySQLPool:
    _pool = None

    @classmethod
    async def init_pool(cls):
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
                maxsize=32,       # 建议调高到 32 并配合并发限制
                pool_recycle=1800,  # 每半小时自动重连
                connect_timeout=10,
            )
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
    async def close(cls):
        if cls._pool:
            cls._pool.close()
            await cls._pool.wait_closed()
            cls._pool = None
            print("🛑 MySQL 连接池已关闭")

    #需要和 lyase_utils.py 整合
    @classmethod
    async def transaction_log(cls, transaction_data):
        conn, cur = await cls.get_conn_cursor()
        print(f"🔍 处理交易记录: {transaction_data}")

        user_info_row = None

        if transaction_data.get('transaction_description', '') == '':
            return {'ok': '', 'status': 'no_description', 'transaction_data': transaction_data}

        
        try:
            # 构造 WHERE 条件
            where_clauses = []
            params = []

            if transaction_data.get('sender_id', '') != '':
                where_clauses.append('sender_id = %s')
                params.append(transaction_data['sender_id'])

            if transaction_data.get('receiver_id', '') != '':
                where_clauses.append('receiver_id = %s')
                params.append(transaction_data['receiver_id'])

            where_clauses.append('transaction_type = %s')
            params.append(transaction_data['transaction_type'])

            where_clauses.append('transaction_description = %s')
            params.append(transaction_data['transaction_description'])

            where_sql = ' AND '.join(where_clauses)

            # 查询是否已有相同记录
            await cur.execute(f"""
                SELECT transaction_id FROM transaction
                WHERE {where_sql}
                LIMIT 1
            """, params)

            transaction_result = await cur.fetchone()

            if transaction_result and transaction_result.get('transaction_id'):
                return {'ok': '1', 'status': 'exist', 'transaction_data': transaction_result}

            # 禁止自己打赏自己
            if transaction_data.get('sender_id') == transaction_data.get('receiver_id'):
                return {'ok': '', 'status': 'reward_self', 'transaction_data': transaction_data}

            # 更新 sender point
            if transaction_data.get('sender_id', '') != '':

            
                try:
                    await cur.execute("""
                        SELECT * 
                        FROM user 
                        WHERE user_id = %s
                        LIMIT 0, 1
                    """, (transaction_data['sender_id'],))
                    user_info_row = await cur.fetchone()
                except Exception as e:
                    print(f"⚠️ 数据库执行出错: {e}")
                    user_info_row = None
            
                if not user_info_row or user_info_row['point'] < abs(transaction_data['sender_fee']):
                    return {'ok': '', 'status': 'insufficient_funds', 'transaction_data': transaction_data, 'user_info': user_info_row}
                else:
                    # 扣除 sender point
                    await cur.execute("""
                        UPDATE user
                        SET point = point + %s
                        WHERE user_id = %s
                    """, (transaction_data['sender_fee'], transaction_data['sender_id']))

               

            # 更新 receiver point，如果不在 block list
            if transaction_data.get('receiver_id', '') != '':
                if not await cls.in_block_list(transaction_data['receiver_id']):
                    await cur.execute("""
                        UPDATE user
                        SET point = point + %s
                        WHERE user_id = %s
                    """, (transaction_data['receiver_fee'], transaction_data['receiver_id']))

            # 插入 transaction 记录
            transaction_data['transaction_timestamp'] = int(time.time())

            insert_columns = ', '.join(transaction_data.keys())
            insert_placeholders = ', '.join(['%s'] * len(transaction_data))
            insert_values = list(transaction_data.values())

            await cur.execute(f"""
                INSERT INTO transaction ({insert_columns})
                VALUES ({insert_placeholders})
            """, insert_values)

            transaction_id = cur.lastrowid
            transaction_data['transaction_id'] = transaction_id

            # 可选的 transaction_cache 插入
            # if transaction_data['transaction_type'] == 'award':
            #     await cur.execute("""
            #         INSERT INTO transaction_cache (sender_id, receiver_id, transaction_type, transaction_timestamp)
            #         VALUES (%s, %s, %s, %s)
            #     """, (
            #         transaction_data['sender_id'],
            #         transaction_data['receiver_id'],
            #         transaction_data['transaction_type'],
            #         transaction_data['transaction_timestamp']
            #     ))

            return {'ok': '1', 'status': 'insert', 'transaction_data': transaction_data,'user_info': user_info_row}

        finally:
            await cls.release(conn, cur)

    @classmethod
    async def in_block_list(cls, user_id):
        # 这里可以实现 block list 检查逻辑
        # 目前直接写 False
        return False
    
   
    @classmethod
    async def search_sora_content_by_id(cls, content_id: int):
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute('''
                SELECT s.id, s.source_id, s.file_type, s.content, s.file_size, s.duration, s.tag,
                    s.thumb_file_unique_id,
                    m.file_id AS m_file_id, m.thumb_file_id AS m_thumb_file_id,
                    p.price as fee, p.file_type as product_type, p.owner_user_id, p.purchase_condition,
                    g.guild_id, g.guild_keyword, g.guild_resource_chat_id, g.guild_resource_thread_id, g.guild_chat_id, g.guild_thread_id  
                FROM sora_content s
                LEFT JOIN sora_media m ON s.id = m.content_id AND m.source_bot_name = %s
                LEFT JOIN product p ON s.id = p.content_id
                LEFT JOIN guild g ON p.guild_id = g.guild_id
                WHERE s.id = %s
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
    async def fetch_file_by_file_id(cls, source_id: str):
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
                print(f"❌ 目标 chat 不存在或无法访问: {e}")
            finally:
                await mybot.session.close()
                return retSend

        return None

    @classmethod
    async def set_product_review_status(cls, content_id: int, review_status: int):
        conn, cursor = await cls.get_conn_cursor()
        try:
            await cursor.execute("""
                UPDATE product SET review_status = %s
                WHERE content_id = %s
            """, (review_status, content_id))
            
        except Exception as e:
            print(f"⚠️ 数据库执行出错: {e}")
        finally:
            await cls.release(conn, cursor)

    @classmethod
    async def get_pending_product(cls):
        """取得最多 2 笔待送审的 product (guild_id 不为空且 review_status=6)"""
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




    @classmethod
    async def create_user_collection(
        cls,
        user_id: int,
        title: str = "未命名合集",
        description: str = "",
        is_public: int = 1,
    ) -> Dict[str, Any]:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                INSERT INTO user_collection (user_id, title, description, is_public)
                VALUES (%s, %s, %s, %s)
                """,
                [user_id, (title or "")[:255], description or "", 1 if is_public == 1 else 0],
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

            if not sets:
                return {"ok": "1", "status": "noop", "id": collection_id}

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
                SELECT id, user_id, title, description, is_public, created_at
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
            cols = ["id", "user_id", "title", "description", "is_public", "created_at"]
            return {k: v for k, v in zip(cols, row)}
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def list_user_collections(
        cls, user_id: int, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        conn, cur = await cls.get_conn_cursor()
        try:
            await cur.execute(
                """
                SELECT id, title, description, is_public, created_at
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
                return rows
            cols = ["id", "title", "description", "is_public", "created_at"]
            return [{k: v for k, v in zip(cols, r)} for r in rows]
        finally:
            await cls.release(conn, cur)


    @classmethod
    async def list_user_favorite_collections(
        cls, user_id: int, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        """
        列出用户收藏的合集（基于 user_collection_favorite.user_collection_id 关联）。
        按收藏记录 id 倒序（最新收藏在前）。
        """
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
                return rows
            cols = ["id", "title", "description", "is_public", "created_at"]
            return [{k: v for k, v in zip(cols, r)} for r in rows]
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
        列出合集里文件的 file_id 列表（按 sort 排序）。
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
            WHERE ucf.collection_id = %s
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
            INSERT INTO user_collection_favorite (user_collection_id, user_id)
            VALUES (%s, %s)
            """
            await cur.execute(sql, (collection_id, user_id))
            return True
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
    async def get_user_collections_count_and_first(cls, user_id: int) -> tuple[int, int | None]:
        """
        返回 (合集数量, 第一条合集ID或None)。
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
    async def create_default_collection(cls, user_id: int, title: str = "未命名合集") -> int | None:
        """
        创建默认合集并返回新建ID；失败返回 None。
        首选 lastrowid；极少数情况下取不到时，兜底再查一次。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            insert_sql = """
            INSERT INTO user_collection (user_id, title, is_public)
            VALUES (%s, %s, 1)
            """
            await cur.execute(insert_sql, (user_id, title))
            await conn.commit()
            new_id = cur.lastrowid
            if new_id:
                return int(new_id)

            # 兜底：再查最新一条
            await cur.execute(
                "SELECT id FROM user_collection WHERE user_id=%s ORDER BY id DESC LIMIT 1",
                (user_id,)
            )
            row = await cur.fetchone()
            return int(row["id"]) if row else None
        except Exception as e:
            print(f"❌ create_default_collection error: {e}", flush=True)
            return None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def add_content_to_user_collection(cls, collection_id: int, content_id: int | str) -> bool:
        """
        把 content_id 加入某个合集。已存在则不报错（联合主键去重）。
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
            await conn.commit()
            return True
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

    @classmethod
    async def get_user_collections_count_and_first(cls, user_id: int) -> tuple[int, int | None]:
        """
        返回 (合集数量, 第一条合集ID或None)。
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
    async def create_default_collection(cls, user_id: int, title: str = "未命名合集") -> int | None:
        """
        创建默认合集并返回新建ID；失败返回 None。
        首选 lastrowid；极少数情况下取不到时，兜底再查一次。
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            insert_sql = """
            INSERT INTO user_collection (user_id, title, is_public)
            VALUES (%s, %s, 1)
            """
            await cur.execute(insert_sql, (user_id, title))
            await conn.commit()
            new_id = cur.lastrowid
            if new_id:
                return int(new_id)

            # 兜底：再查最新一条
            await cur.execute(
                "SELECT id FROM user_collection WHERE user_id=%s ORDER BY id DESC LIMIT 1",
                (user_id,)
            )
            row = await cur.fetchone()
            return int(row["id"]) if row else None
        except Exception as e:
            print(f"❌ create_default_collection error: {e}", flush=True)
            return None
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def add_content_to_user_collection(cls, collection_id: int, content_id: int | str) -> bool:
        """
        把 content_id 加入某个合集。已存在则不报错（联合主键去重）。
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
            await conn.commit()
            return True
        except Exception as e:
            print(f"❌ add_content_to_user_collection error: {e}", flush=True)
            return False
        finally:
            await cls.release(conn, cur)

