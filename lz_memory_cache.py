import time
from collections import OrderedDict
from typing import Any, Optional


class MemoryCache:
    """
    L1 内存缓存（TTL + LRU + size limit）
    """

    def __init__(
        self,
        max_items: int = 2000,
        max_value_bytes: int = 256 * 1024,
    ):
        self.max_items = max_items
        self.max_value_bytes = max_value_bytes
        self._store = OrderedDict()

    def _now(self) -> float:
        return time.time()

    def _estimate_size(self, value: Any) -> int:
        # 轻量估算，避免 deepcopy / pickle
        try:
            if isinstance(value, str):
                return len(value.encode("utf-8"))
            if isinstance(value, (bytes, bytearray)):
                return len(value)
            if isinstance(value, (list, tuple, set, dict)):
                return len(str(value).encode("utf-8"))
        except Exception:
            pass
        return 128  # 保守兜底

    def get(self, key: str):
        item = self._store.get(key)
        if not item:
            return None

        value, expire_at = item
        if expire_at and expire_at < self._now():
            self._store.pop(key, None)
            return None

        # LRU 命中 → 移到末尾
        self._store.move_to_end(key)
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        # 大对象不进 L1
        if self._estimate_size(value) > self.max_value_bytes:
            return

        expire_at = self._now() + ttl if ttl else None
        self._store[key] = (value, expire_at)
        self._store.move_to_end(key)

        # 超量 → LRU 淘汰
        while len(self._store) > self.max_items:
            self._store.popitem(last=False)

    def delete(self, key: str):
        self._store.pop(key, None)

    def clear(self):
        self._store.clear()

    def stats(self) -> dict:
        return {
            "items": len(self._store),
            "max_items": self.max_items,
            "max_value_bytes": self.max_value_bytes,
        }
