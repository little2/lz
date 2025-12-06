# synonym_manager.py
from __future__ import annotations
from typing import Dict, List
from lz_mysql import MySQLPool


class SynonymManager:
    _alias2canonical: Dict[str, str] = {}
    _loaded: bool = False

    @classmethod
    async def load_from_db(cls, scope: str | None = None):
        """
        从 MySQL synonym 表加载同义词到内存。
        scope: 可选，用来过滤某一类用法；传 None 则不限制。
        """
        await MySQLPool.init_pool()
        conn, cur = await MySQLPool.get_conn_cursor()
        try:
            if scope:
                await cur.execute(
                    """
                    SELECT canonical, alias
                    FROM synonym
                    WHERE enabled = 1 AND (scope = %s OR scope IS NULL)
                    """,
                    (scope,)
                )
            else:
                await cur.execute(
                    """
                    SELECT canonical, alias
                    FROM synonym
                    WHERE enabled = 1
                    """
                )
            rows = await cur.fetchall()
        finally:
            await MySQLPool.release(conn, cur)

        mapping: Dict[str, str] = {}
        for row in rows:
            canonical = (row["canonical"] or "").strip()
            alias = (row["alias"] or "").strip()
            if not canonical or not alias:
                continue
            mapping[alias] = canonical

        cls._alias2canonical = mapping
        cls._loaded = True
        print(f"[SynonymManager] loaded {len(mapping)} synonyms")

    @classmethod
    def normalize_token(cls, token: str) -> str:
        """
        单个词归一化：若 token 在 alias→canonical 映射中，则返回 canonical。
        """
        if not cls._loaded:
            # 如果你担心没加载，可以选择这里直接返回原词，
            # 真正加载逻辑在启动时显式调用 load_from_db()
            return token
        return cls._alias2canonical.get(token, token)

    @classmethod
    def normalize_tokens(cls, tokens: List[str]) -> List[str]:
        """
        token 列表归一化。
        """
        if not cls._loaded:
            return tokens
        return [cls.normalize_token(t) for t in tokens]
