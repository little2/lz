import os
import cv2
import numpy as np
from imwatermark import WatermarkEncoder, WatermarkDecoder

from watermark.watermark_utils import (
    encode_transaction_id_to_short_key,
    decode_short_key_to_suffix,
)
from watermark.pattern_watermark import embed_pattern_image
from lz_mysql import MySQLPool


BASE62_CHARS = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")


class TransactionWatermarkService:

    @staticmethod
    def embed_short_key_image(img: np.ndarray, short_key: str) -> np.ndarray:
        if img is None:
            raise ValueError("img is None")

        encoder = WatermarkEncoder()
        encoder.set_watermark("bytes", short_key.encode())
        return encoder.encode(img, "dwtDct")

    @staticmethod
    def embed_short_key(input_path, output_path, short_key):
        img = cv2.imread(input_path)
        result = TransactionWatermarkService.embed_short_key_image(img, short_key)
        cv2.imwrite(output_path, result)

    @staticmethod
    def decode_short_key_image(img: np.ndarray) -> str:
        if img is None:
            raise ValueError("img is None")

        decoder = WatermarkDecoder("bytes", 24)
        raw = decoder.decode(img, "dwtDct")

        if isinstance(raw, bytes):
            decoded = raw.decode("ascii", errors="ignore")
        else:
            decoded = str(raw)

        short_key = "".join(ch for ch in decoded if ch in BASE62_CHARS)[:3]

        if len(short_key) != 3:
            raise ValueError(
                f"failed to decode valid short key: {raw!r}. "
                "Legacy images generated through a JPEG intermediary may be corrupted."
            )

        return short_key

    @staticmethod
    def decode_short_key(path):
        img = cv2.imread(path)
        return TransactionWatermarkService.decode_short_key_image(img)

    @classmethod
    async def watermark(cls, transaction_id, input_path, output_path):

        short_key = encode_transaction_id_to_short_key(transaction_id)

        img = cv2.imread(input_path)
        if img is None:
            raise FileNotFoundError(input_path)

        img = cls.embed_short_key_image(img, short_key)
        img = embed_pattern_image(img, transaction_id)

        out_dir = os.path.dirname(output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        cv2.imwrite(output_path, img)

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