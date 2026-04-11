import os
import tempfile
import cv2
from imwatermark import WatermarkEncoder, WatermarkDecoder

from watermark.watermark_utils import (
    encode_transaction_id_to_short_key,
    decode_short_key_to_suffix,
)
from watermark.pattern_watermark import embed_pattern
from lz_mysql import MySQLPool


BASE62_CHARS = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")


class TransactionWatermarkService:

    @staticmethod
    def embed_short_key(input_path, output_path, short_key):
        img = cv2.imread(input_path)

        encoder = WatermarkEncoder()
        encoder.set_watermark("bytes", short_key.encode())

        result = encoder.encode(img, "dwtDct")
        cv2.imwrite(output_path, result)

    @staticmethod
    def decode_short_key(path):
        img = cv2.imread(path)

        decoder = WatermarkDecoder("bytes", 24)
        raw = decoder.decode(img, "dwtDct")

        if isinstance(raw, bytes):
            decoded = raw.decode("ascii", errors="ignore")
        else:
            decoded = str(raw)

        short_key = "".join(ch for ch in decoded if ch in BASE62_CHARS)[:3]

        if len(short_key) != 3:
            raise ValueError(
                f"failed to decode valid short key from {path}: {raw!r}. "
                "Legacy images generated through a JPEG intermediary may be corrupted."
            )

        return short_key

    @classmethod
    async def watermark(cls, transaction_id, input_path, output_path):

        short_key = encode_transaction_id_to_short_key(transaction_id)

        with tempfile.TemporaryDirectory() as tmp:
            p1 = os.path.join(tmp, "l1.png")

            cls.embed_short_key(input_path, p1, short_key)
            embed_pattern(p1, output_path, transaction_id)

        return {
            "transaction_id": transaction_id,
            "short_key": short_key,
            "output": output_path
        }

    @classmethod
    async def investigate(cls, image_path):

        short_key = cls.decode_short_key(image_path)
        suffix = decode_short_key_to_suffix(short_key)

        candidates = await MySQLPool.list_transactions_by_suffix(suffix)

        result = []

        from watermark.pattern_watermark import score_pattern

        for row in candidates:
            tid = row["transaction_id"]
            score = score_pattern(image_path, tid)

            result.append({
                "transaction_id": tid,
                "score": score
            })

        result.sort(key=lambda x: x["score"], reverse=True)

        return {
            "short_key": short_key,
            "suffix": suffix,
            "top": result[:5]
        }