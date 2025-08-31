# utils/prof.py
import time
import functools
from typing import Callable, Awaitable

def now() -> float:
    return time.perf_counter()

class SegTimer:
    def __init__(self, tag: str, **ctx):
        self.tag = tag
        self.t0 = now()
        self.last = self.t0
        self.ctx = ctx

    def lap(self, name: str):
        t = now()
        dt = t - self.last
        self.last = t
        print(f"[PERF] {self.tag}::{name} {dt*1000:.2f} ms | ctx={self.ctx}", flush=True)

    def end(self):
        total = now() - self.t0
        print(f"[PERF] {self.tag}::TOTAL {total*1000:.2f} ms | ctx={self.ctx}", flush=True)

def async_timed(name: str):
    """装饰 async 函数，打印总耗时"""
    def deco(fn: Callable[..., Awaitable]):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            t0 = now()
            try:
                return await fn(*args, **kwargs)
            finally:
                print(f"[PERF] {name} TOTAL { (now()-t0)*1000:.2f} ms", flush=True)
        return wrapper
    return deco
