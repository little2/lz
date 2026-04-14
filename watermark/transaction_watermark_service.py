import os
import cv2
import numpy as np
from scipy.fftpack import dct, idct
from imwatermark import WatermarkEncoder, WatermarkDecoder

from watermark.watermark_utils import (
    encode_transaction_id_to_short_key,
    decode_short_key_to_suffix,
)
from watermark.pattern_watermark import embed_pattern_image
from lz_mysql import MySQLPool


BASE62_CHARS = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def _dct2(block: np.ndarray) -> np.ndarray:
    return dct(dct(block.T, norm="ortho").T, norm="ortho")


def _idct2(block: np.ndarray) -> np.ndarray:
    return idct(idct(block.T, norm="ortho").T, norm="ortho")


class TransactionWatermarkService:

    @staticmethod
    def _plan_block_indices(
        h8: int,
        w8: int,
        bit_count: int,
        max_repeats: int,
        fixed_top_rows: int = 24,
    ) -> tuple[np.ndarray, int]:
        bx_count = w8 // 8
        by_count = h8 // 8
        if bx_count <= 0 or by_count <= 0:
            raise ValueError("image too small for watermark blocks")

        top_rows = min(by_count, max(1, int(fixed_top_rows)))

        region_blocks = top_rows * bx_count
        if region_blocks < bit_count:
            # Fallback to whole image if top region is too small.
            top_rows = by_count
            region_blocks = top_rows * bx_count

        repeats = max(1, min(max_repeats, region_blocks // bit_count))
        use_blocks = bit_count * repeats
        block_indices = np.linspace(0, region_blocks - 1, num=use_blocks, dtype=np.int32)
        return block_indices, bx_count

    @staticmethod
    def _short_key_to_bits(short_key: str) -> list[int]:
        if len(short_key) != 3 or any(ch not in BASE62_ALPHABET for ch in short_key):
            raise ValueError(f"invalid short key: {short_key!r}")

        vals = [BASE62_ALPHABET.index(ch) for ch in short_key]
        checksum = (vals[0] * 3 + vals[1] * 5 + vals[2] * 7 + 17) % 64
        payload = vals + [checksum]

        bits: list[int] = []
        for v in payload:
            for i in range(6):
                bits.append((v >> (5 - i)) & 1)
        return bits

    @staticmethod
    def _bits_to_short_key(bits: list[int]) -> str:
        if len(bits) < 24:
            raise ValueError("insufficient bits for short key payload")

        vals = []
        for i in range(0, 24, 6):
            v = 0
            for b in bits[i:i + 6]:
                v = (v << 1) | int(b)
            vals.append(v)

        if any(v >= 62 for v in vals[:3]):
            raise ValueError("decoded value out of base62 range")

        checksum = (vals[0] * 3 + vals[1] * 5 + vals[2] * 7 + 17) % 64
        if vals[3] != checksum:
            raise ValueError("short key checksum mismatch")

        return "".join(BASE62_ALPHABET[v] for v in vals[:3])

    @staticmethod
    def _embed_short_key_dct_image(img: np.ndarray, short_key: str, delta: float = 26.0, max_repeats: int = 40) -> np.ndarray:
        bits = TransactionWatermarkService._short_key_to_bits(short_key)
        bit_count = len(bits)

        ycrcb = cv2.cvtColor(img, cv2.COLOR_BGR2YCrCb)
        y = ycrcb[:, :, 0].astype(np.float32)

        h, w = y.shape
        h8, w8 = h // 8 * 8, w // 8 * 8
        if h8 == 0 or w8 == 0:
            raise ValueError("image too small for watermark embedding")

        block_indices, bx_count = TransactionWatermarkService._plan_block_indices(
            h8=h8,
            w8=w8,
            bit_count=bit_count,
            max_repeats=max_repeats,
            fixed_top_rows=24,
        )

        for k, flat_idx in enumerate(block_indices):
            bit = bits[k % bit_count]

            by = int(flat_idx) // bx_count
            bx = int(flat_idx) % bx_count
            y0, x0 = by * 8, bx * 8

            block = y[y0:y0 + 8, x0:x0 + 8]
            coeff = _dct2(block)

            target_diff = delta if bit == 1 else -delta

            # Pair A (mid-frequency)
            a1 = float(coeff[3, 4])
            a2 = float(coeff[4, 3])
            amid = (a1 + a2) / 2.0
            coeff[3, 4] = amid + target_diff / 2.0
            coeff[4, 3] = amid - target_diff / 2.0

            # Pair B (nearby mid-frequency) as redundancy.
            b1 = float(coeff[2, 3])
            b2 = float(coeff[3, 2])
            bmid = (b1 + b2) / 2.0
            coeff[2, 3] = bmid + target_diff / 2.0
            coeff[3, 2] = bmid - target_diff / 2.0

            y[y0:y0 + 8, x0:x0 + 8] = _idct2(coeff)

        ycrcb[:, :, 0] = np.clip(y, 0, 255)
        return cv2.cvtColor(ycrcb.astype(np.uint8), cv2.COLOR_YCrCb2BGR)

    @staticmethod
    def _decode_short_key_dct_image_with_region(
        img: np.ndarray,
        max_repeats: int = 40,
        fixed_top_rows: int = 24,
    ) -> str:
        ycrcb = cv2.cvtColor(img, cv2.COLOR_BGR2YCrCb)
        y = ycrcb[:, :, 0].astype(np.float32)

        h, w = y.shape
        h8, w8 = h // 8 * 8, w // 8 * 8
        if h8 == 0 or w8 == 0:
            raise ValueError("image too small for watermark decoding")

        bit_count = 24
        block_indices, bx_count = TransactionWatermarkService._plan_block_indices(
            h8=h8,
            w8=w8,
            bit_count=bit_count,
            max_repeats=max_repeats,
            fixed_top_rows=fixed_top_rows,
        )

        votes = [[] for _ in range(bit_count)]
        for k, flat_idx in enumerate(block_indices):
            bit_idx = k % bit_count

            by = int(flat_idx) // bx_count
            bx = int(flat_idx) % bx_count
            y0, x0 = by * 8, bx * 8

            block = y[y0:y0 + 8, x0:x0 + 8]
            coeff = _dct2(block)
            diff_a = float(coeff[3, 4] - coeff[4, 3])
            diff_b = float(coeff[2, 3] - coeff[3, 2])
            diff = diff_a + diff_b
            votes[bit_idx].append(diff)

        bits = [1 if float(np.mean(v)) >= 0.0 else 0 for v in votes]
        return TransactionWatermarkService._bits_to_short_key(bits)

    @staticmethod
    def _decode_short_key_dct_image(img: np.ndarray, max_repeats: int = 40) -> str:
        # Try multiple decode windows. This improves robustness when image
        # bottom is cropped and block distribution changes.
        attempts = [24, 16, 32, 8]
        hits: list[str] = []
        last_error = None
        for top_rows in attempts:
            try:
                key = TransactionWatermarkService._decode_short_key_dct_image_with_region(
                    img,
                    max_repeats=max_repeats,
                    fixed_top_rows=top_rows,
                )
                hits.append(key)
            except Exception as e:
                last_error = e

        if hits:
            counts: dict[str, int] = {}
            for key in hits:
                counts[key] = counts.get(key, 0) + 1
            best_key, best_count = max(counts.items(), key=lambda kv: kv[1])

            if best_count >= 2 or len(counts) == 1:
                return best_key

            raise ValueError(f"inconsistent dct decode candidates: {counts}")

        if last_error is not None:
            raise last_error
        raise ValueError("dct decode failed")

    @staticmethod
    def _decode_short_key_legacy_image(img: np.ndarray) -> str:
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
    def embed_short_key_image(img: np.ndarray, short_key: str) -> np.ndarray:
        if img is None:
            raise ValueError("img is None")

        # New robust embedding path: DCT redundant payload with checksum,
        # much more resilient after Telegram JPEG recompression.
        out = TransactionWatermarkService._embed_short_key_dct_image(img, short_key)

        # Keep legacy watermark for backward compatibility with older decoders.
        encoder = WatermarkEncoder()
        encoder.set_watermark("bytes", short_key.encode())
        out = encoder.encode(out, "dwtDct")
        return out

    @staticmethod
    def embed_short_key(input_path, output_path, short_key):
        img = cv2.imread(input_path)
        result = TransactionWatermarkService.embed_short_key_image(img, short_key)
        cv2.imwrite(output_path, result)

    @staticmethod
    def decode_short_key_image(img: np.ndarray) -> str:
        if img is None:
            raise ValueError("img is None")

        errors = []

        try:
            return TransactionWatermarkService._decode_short_key_dct_image(img)
        except Exception as e:
            errors.append(f"dct={e}")

        try:
            return TransactionWatermarkService._decode_short_key_legacy_image(img)
        except Exception as e:
            errors.append(f"legacy={e}")

        raise ValueError("failed to decode valid short key; " + " | ".join(errors))

    @staticmethod
    def decode_short_key(path):
        print(f"Decoding short key from image: {path}", flush=True)
        img = cv2.imread(path)
        print(f"Image loaded for decoding: {img is not None}", flush=True)
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
        print(f"Investigating image: {image_path}", flush=True)
        short_key = cls.decode_short_key(image_path)
        print(f"Decoded short key: {short_key}", flush=True)
        suffix = decode_short_key_to_suffix(short_key)
        print(f"Derived suffix from short key: {suffix}", flush=True)

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