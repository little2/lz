# lz_memory_cache.py

import time

class MemoryCache:
    def __init__(self):
        self.store = {}

    def set(self, key, value, ttl=60):
        expire_time = time.time() + ttl
        self.store[key] = (value, expire_time)

    def get(self, key):
        item = self.store.get(key)
        if not item:
            return None
        value, expire_time = item
        if time.time() > expire_time:
            del self.store[key]
            return None
        return value

    def clear(self):
        self.store.clear()
