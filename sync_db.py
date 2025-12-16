import asyncio
from typing import Sequence

from lz_db import db              # asyncpg pool 封装
from lz_mysql import MySQLPool    # aiomysql pool 封装


PG_UPSERT_SQL = """
INSERT INTO album_items (
    id, content_id, member_content_id, file_unique_id, file_type,
    position, created_at, updated_at, stage, preview
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, 'update', $9
)
ON CONFLICT (id) DO UPDATE SET
    content_id        = EXCLUDED.content_id,
    member_content_id = EXCLUDED.member_content_id,
    file_unique_id    = EXCLUDED.file_unique_id,
    file_type         = EXCLUDED.file_type,
    position          = EXCLUDED.position,
    created_at        = EXCLUDED.created_at,
    updated_at        = EXCLUDED.updated_at,
    stage             = 'update',
    preview           = EXCLUDED.preview
"""

MYSQL_FETCH_SQL = """
SELECT
    id, content_id, member_content_id, file_unique_id, file_type,
    position, created_at, updated_at, stage, preview
FROM album_items
WHERE stage = 'pending'
ORDER BY id ASC
LIMIT %s
"""

MYSQL_MARK_UPDATED_SQL = """
UPDATE album_items
SET stage = 'update'
WHERE id IN (%s)
"""


async def _fetch_pending_from_mysql(limit: int) -> list[dict]:
    """从 MySQL 抓待同步的 album_items（stage='pending'）"""
    await MySQLPool.init_pool()
    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        await cur.execute(MYSQL_FETCH_SQL, (int(limit),))
        rows = await cur.fetchall()
        return [dict(r) for r in rows] if rows else []
    finally:
        await MySQLPool.release(conn, cur)


async def _mark_mysql_updated(ids: Sequence[int]) -> None:
    """把 MySQL 中这些 id 的 stage 标记为 update"""
    if not ids:
        return
    await MySQLPool.init_pool()
    conn, cur = await MySQLPool.get_conn_cursor()
    try:
        placeholders = ",".join(["%s"] * len(ids))
        sql = MYSQL_MARK_UPDATED_SQL % placeholders
        await cur.execute(sql, list(ids))
    finally:
        await MySQLPool.release(conn, cur)


async def sync_pending_album_items(limit: int = 1000, batch_size: int = 200) -> dict:
    """
    将 MySQL album_items 中 stage='pending' 的行，同步到 PostgreSQL：
      - 按 id 主键 ON CONFLICT UPSERT
      - 成功后：PG 侧 stage 直接被写成 'update'；随后把 MySQL 同步成功的 id 批量设为 'update'
    返回值：{"ok": 1, "total": N, "success": S, "failed": F, "failed_ids": [...]}
    """
    await db.connect()  # 确保 PG 连接池就绪（来自 lz_db 封装）
    rows = await _fetch_pending_from_mysql(limit)
    if not rows:
        return {"ok": 1, "total": 0, "success": 0, "failed": 0, "failed_ids": []}

    success_ids: list[int] = []
    failed_ids: list[int] = []

    # 按批写入 PG，单批事务，出错不影响已成功的其它批次
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        try:
            async with db.pool.acquire() as conn:           # uses asyncpg pool
                async with conn.transaction():              # 单批事务
                    for r in chunk:
                        try:
                            await conn.execute(
                                PG_UPSERT_SQL,
                                int(r["id"]),
                                int(r["content_id"]),
                                int(r["member_content_id"]),
                                (r["file_unique_id"] or None),
                                (r["file_type"] or None),
                                int(r["position"] or 0),
                                r["created_at"],            # asyncpg 接受 datetime 或 str(ISO)
                                r["updated_at"],            # 允许 None
                            )
                            success_ids.append(int(r["id"]))
                        except Exception:
                            # 单条失败（多为外键不满足、类型不符等），记录下来继续跑下一条
                            failed_ids.append(int(r["id"]))
                            # 不中断整批，保证尽量多的成功
                            continue
        except Exception:
            # 极端情况：整批事务失败（例如网络/连接故障），把这一批全部记为失败
            failed_ids.extend([int(r["id"]) for r in chunk if int(r["id"]) not in success_ids])

    # 成功的 id 批量把 MySQL 的 stage 标记为 update
    if success_ids:
        await _mark_mysql_updated(success_ids)

    return {
        "ok": 1,
        "total": len(rows),
        "success": len(success_ids),
        "failed": len(failed_ids),
        "failed_ids": failed_ids,
    }


async def main():
    try:
        # 跑同步（可调参数）
        result = await sync_pending_album_items(limit=1000, batch_size=200)
        print("同步结果：", result)
    finally:
        # 关键：在事件循环结束前优雅关闭两个池
        try:
            await db.disconnect()         # 关闭 asyncpg 池（PostgreSQL）
        except Exception as e:
            print("关闭 PG 连接池出错：", e)
        try:
            await MySQLPool.close()       # 关闭 aiomysql 池（MySQL）
        except Exception as e:
            print("关闭 MySQL 连接池出错：", e)

if __name__ == "__main__":
    # 只调用一次 asyncio.run，避免嵌套/多循环问题
    asyncio.run(main())
