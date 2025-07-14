# lz_redis.py
import redis.asyncio as aioredis
import redis.exceptions
import json
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

class RedisManager:
    async def get_client(self):
        # 每次 new Redis client，关闭连接池的 idle 保活，兼容 Render Redis KeyValue serverless
        return aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_keepalive=False,
            health_check_interval=0
        )

    async def set_json(self, key, value, ttl=300):
        client = await self.get_client()
        try:
            data = json.dumps(value)
            await client.set(key, data, ex=ttl)
            print(f"🔹 Redis cache set for {key}, {len(value)} items")
        except redis.exceptions.ConnectionError as e:
            print(f"⚠️ Redis SET connection error: {e} (可能是 Render Redis KeyValue cold start)")
        finally:
            await client.close()

    async def get_json(self, key):
        client = await self.get_client()
        try:
            data = await client.get(key)
            if data:
                try:
                    print(f"🔹 Redis cache hit for {key}")
                    return json.loads(data)
                except json.JSONDecodeError as e:
                    print(f"⚠️ Redis GET JSON decode error for key={key}: {e}")
                    return None
            else:
                print(f"🔹 Redis cache miss for {key}")
            return None
        except redis.exceptions.ConnectionError as e:
            print(f"⚠️ Redis GET connection error: {e} (可能是 Render Redis KeyValue cold start)")
            return None
        finally:
            await client.close()

    async def delete(self, key):
        client = await self.get_client()
        try:
            await client.delete(key)
            print(f"🔹 Redis key deleted: {key}")
        except redis.exceptions.ConnectionError as e:
            print(f"⚠️ Redis DEL connection error: {e} (可能是 Render Redis KeyValue cold start)")
        finally:
            await client.close()
