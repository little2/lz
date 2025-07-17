import aiomysql
import time
from lz_config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_DB_PORT

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
                minsize=1,
                maxsize=5, 
                pool_recycle=3600
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

    @classmethod
    async def transaction_log(cls, transaction_data):
        conn, cur = await cls.get_conn_cursor()

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
            
                if user_info_row['point'] < transaction_data['sender_fee']:
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
