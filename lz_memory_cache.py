# lz_memory_cache.py

import time

class MemoryCache:
    def __init__(self):
        self.store = {}

    def set(self, key, value, ttl=1200):
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

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            print(f"🔹 MemoryCache deleted key: {key}")
        else:
            print(f"🔹 MemoryCache delete skipped, key not found: {key}")
