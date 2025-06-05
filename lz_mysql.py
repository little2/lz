import aiomysql
from lz_config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_DB_PORT

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
