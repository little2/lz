import os
import cv2
import numpy as np
from scipy.fftpack import dct, idct

from watermark.watermark_utils import generate_seed_from_transaction_id


def dct2(block):
    return dct(dct(block.T, norm="ortho").T, norm="ortho")


def idct2(block):
    return idct(idct(block.T, norm="ortho").T, norm="ortho")


def load_image(path):
    img = cv2.imread(path)
    if img is None:
        raise FileNotFoundError(path)
    return img


def save_image(path, img, quality=95):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".jpg", ".jpeg"]:
        cv2.imwrite(path, img, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    else:
        cv2.imwrite(path, img)


def generate_pattern(transaction_id):
    seed = generate_seed_from_transaction_id(transaction_id)
    rng = np.random.default_rng(seed)
    return rng.choice([-1.0, 1.0], size=(8, 8)).astype(np.float32)


def embed_pattern_image(img, transaction_id, alpha=8.0):
    if img is None:
        raise ValueError("img is None")

    ycrcb = cv2.cvtColor(img, cv2.COLOR_BGR2YCrCb)
    y = ycrcb[:, :, 0].astype(np.float32)

    h, w = y.shape
    h8, w8 = h // 8 * 8, w // 8 * 8

    pattern = generate_pattern(transaction_id)

    for y0 in range(0, h8, 8):
        for x0 in range(0, w8, 8):
            block = y[y0:y0+8, x0:x0+8]
            coeff = dct2(block)
            coeff[1:4, 1:4] += alpha * pattern[1:4, 1:4]
            y[y0:y0+8, x0:x0+8] = idct2(coeff)

    ycrcb[:, :, 0] = np.clip(y, 0, 255)
    return cv2.cvtColor(ycrcb.astype(np.uint8), cv2.COLOR_YCrCb2BGR)


def embed_pattern(input_path, output_path, transaction_id, alpha=8.0):
    img = load_image(input_path)
    out = embed_pattern_image(img, transaction_id, alpha=alpha)
    save_image(output_path, out)


def score_pattern_image(img, transaction_id):
    if img is None:
        raise ValueError("img is None")

    ycrcb = cv2.cvtColor(img, cv2.COLOR_BGR2YCrCb)
    y = ycrcb[:, :, 0].astype(np.float32)

    h, w = y.shape
    h8, w8 = h // 8 * 8, w // 8 * 8

    pattern = generate_pattern(transaction_id)[1:4, 1:4]
    scores = []

    for y0 in range(0, h8, 8):
        for x0 in range(0, w8, 8):
            block = y[y0:y0+8, x0:x0+8]
            coeff = dct2(block)[1:4, 1:4]
            scores.append(float(np.mean(coeff * pattern)))

    if not scores:
        return 0.0

    return float(np.mean(scores))


def score_pattern(image_path, transaction_id):
    img = load_image(image_path)
    return score_pattern_image(img, transaction_id)