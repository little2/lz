import hashlib
import os
from utils.base62_converter import Base62Converter


def encode_transaction_id_to_short_key(transaction_id: int) -> str:
    suffix = int(transaction_id) % 100000
    return Base62Converter.decimal_to_base62(suffix).rjust(3, "0")


def decode_short_key_to_suffix(short_key: str) -> int:
    suffix = Base62Converter.base62_to_decimal(short_key)

    if suffix < 0 or suffix >= 100000:
        raise ValueError(f"invalid suffix: {suffix}")

    return suffix


def generate_seed_from_transaction_id(transaction_id: int) -> int:
    raw = str(int(transaction_id)).encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    return int.from_bytes(digest[:8], "big")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)