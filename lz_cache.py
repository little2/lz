# lz_two_level_cache.py
import asyncio
import json
from typing import Any, Optional

import redis.asyncio as redis_async
from lz_memory_cache import MemoryCache

class TwoLevelCache:
    """
    L1: MemoryCache（同步）
    L2: Valkey（异步，但对外不暴露 await；用后台 task 执行）
    """

    def __init__(self, valkey_client, l1: Optional[MemoryCache] = None, namespace: str = "lz"):
      
        self.l1 = l1 or MemoryCache(max_items=2000)
        self.ns = namespace

        # 关键修正：允许传入 URL(str) 或 redis client
        if isinstance(valkey_client, str):
            self.r = redis_async.from_url(valkey_client, decode_responses=True)
        else:
            self.r = valkey_client

        # 额外：启动时快速验一下
        if not hasattr(self.r, "setex") or not hasattr(self.r, "get"):
            raise TypeError(f"TwoLevelCache L2 client invalid: type={type(self.r)!r}, value={self.r!r}")


    def _k(self, key: str) -> str:
        return f"{self.ns}:{key}"

    def get(self, key: str):
        # 1) L1
        v = self.l1.get(key)
        if v is not None:
            return v

        # 2) L2：后台回填（不阻塞）
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 没有 running loop（极少见于你这个 aiogram 场景）
            return None

        loop.create_task(self._fill_from_l2(key))
        return None

    async def _fill_from_l2(self, key: str):
        try:
            raw = await self.r.get(self._k(key))
            if not raw:
                return
            # 你如果存的是 json，就 decode；若直接存 bytes/str，也可按需调整
            val = json.loads(raw) if isinstance(raw, (bytes, str)) else raw
            self.l1.set(key, val, ttl=120)  # L1 TTL 建议短一点
        except Exception:
            return

    def set(self, key: str, value: Any, ttl: int = 1200):
        # 1) 先写 L1（立即生效）
        self.l1.set(key, value, ttl=ttl)

        # 2) 写 L2：后台执行
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._set_l2(key, value, ttl))

    async def _set_l2(self, key: str, value: Any, ttl: int):
        try:
            raw = json.dumps(value, ensure_ascii=False, default=str)
            await self.r.setex(self._k(key), ttl, raw)
        except Exception as e:
            print(f"TwoLevelCache: set L2 failed for key={key}, error={e}")
            return

    def delete(self, key: str):
        # 1) 先删 L1（立即生效）
        try:
            self.l1.delete(key)
        except Exception:
            pass

        # 2) 删 L2：后台执行（不阻塞）
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._del_l2(key))

    async def _del_l2(self, key: str):
        try:
            # redis/valkey async client: await r.delete(...)
            await self.r.delete(self._k(key))
        except Exception as e:
            print(f"TwoLevelCache: delete L2 failed for key={key}, error={e}")
            return

