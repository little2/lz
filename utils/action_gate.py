# utils/action_gate.py

import time
import hmac
import hashlib
import base64


class ActionGate:
    """
    通用的「用户动作门禁」工具类

    用途示例：
    - 问答验证通过后的免验 gate
    - 高风险操作的二次确认
    - 临时授权按钮（仅本人、限时）
    """

    secret = b"action_gate_v1_default_secret"

    window_seconds = 600
    leeway = 1

    @classmethod
    def _base36(cls, n: int) -> str:
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        if n <= 0:
            return "0"
        s = ""
        while n:
            n, r = divmod(n, 36)
            s = chars[r] + s
        return s

    @classmethod
    def _base36_to_int(cls, s: str) -> int:
        return int(s, 36)

    @classmethod
    def make_extra(cls, user_id: int, object_id: int) -> str:
        bucket = int(time.time()) // cls.window_seconds
        t = cls._base36(bucket)

        msg = f"{user_id}:{object_id}:{t}".encode("utf-8")
        mac = hmac.new(cls.secret, msg, hashlib.sha256).digest()
        sig = base64.urlsafe_b64encode(mac[:6]).decode("ascii").rstrip("=")

        return f"q{t}{sig}"

    @classmethod
    def verify_extra(cls, extra: str, user_id: int, object_id: int) -> bool:
        if not extra or not extra.startswith("q") or len(extra) < 10:
            return False

        sig = extra[-8:]
        t = extra[1:-8]

        try:
            bucket_from_token = cls._base36_to_int(t)
        except Exception:
            return False

        now_bucket = int(time.time()) // cls.window_seconds
        if abs(now_bucket - bucket_from_token) > cls.leeway:
            return False

        msg = f"{user_id}:{object_id}:{t}".encode("utf-8")
        mac = hmac.new(cls.secret, msg, hashlib.sha256).digest()
        expected_sig = base64.urlsafe_b64encode(mac[:6]).decode("ascii").rstrip("=")

        return hmac.compare_digest(sig, expected_sig)
