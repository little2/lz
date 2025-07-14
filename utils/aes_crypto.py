import os
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

class AESCrypto:
    """
    AES-256-CBC 加解密工具，URL-safe Base64 编码输出/输入，
    与 PHP openssl_encrypt/openssl_decrypt 完全互通。
    """
    def __init__(self, key: str):
        # 将 key 转为 bytes，PKCS#7 填充或截断至 32 字节
        raw = key.encode('utf-8')
        if len(raw) < 32:
            raw = raw.ljust(32, b'\0')
        else:
            raw = raw[:32]
        self.key = raw

    def aes_encode(self, data: str) -> str:
        if not isinstance(data, str):
            data = str(data)

        """
        加密字符串，返回 URL-safe Base64（不带=填充）的结果。
        """
        iv = os.urandom(AES.block_size)  # 16 字节随机 IV
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        ciphertext = cipher.encrypt(pad(data.encode('utf-8'), AES.block_size))
        # 拼接 IV + 密文，URL-safe Base64 编码并去掉 '='
        encoded = base64.urlsafe_b64encode(iv + ciphertext).rstrip(b'=')
        return encoded.decode('utf-8')

    def aes_decode(self, url_safe_b64: str) -> str:
        """
        解密 URL-safe Base64 编码的字符串，返回原文。
        """
        # 恢复 Base64 的 padding
        padding_len = (4 - len(url_safe_b64) % 4) % 4
        b64 = url_safe_b64 + ('=' * padding_len)
        raw = base64.urlsafe_b64decode(b64.encode('utf-8'))
        iv = raw[:AES.block_size]
        ciphertext = raw[AES.block_size:]
        plain = unpad(AES.new(self.key, AES.MODE_CBC, iv).decrypt(ciphertext), AES.block_size)
        return plain.decode('utf-8')

