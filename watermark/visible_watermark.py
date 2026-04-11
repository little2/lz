import math
import os

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont


def _resolve_font_path(font_path: str | None) -> str:
    candidates = []

    if font_path:
        candidates.append(font_path)

    module_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(module_dir)
    candidates.extend([
        os.path.join(repo_root, "font", "msyh.ttc"),
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
    ])

    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate

    raise FileNotFoundError("找不到可用的中文字型，请提供 font_path。")


def _load_font(font_path: str | None, font_size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(_resolve_font_path(font_path), font_size)


def _measure_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> tuple[int, int]:
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return right - left, bottom - top


def _opacity_to_alpha(opacity: float) -> int:
    return max(0, min(255, int(round(opacity * 255))))


def _resolve_fullscreen_spacing(
    text_w: int,
    text_h: int,
    font_size: int,
    x_gap: int,
    y_gap: int,
) -> tuple[int, int]:
    min_x_gap = text_w + max(font_size, 24)
    min_y_gap = text_h + max(font_size * 2, 48)
    return max(x_gap, min_x_gap), max(y_gap, min_y_gap)


def _position_xy(position: str, width: int, height: int, text_w: int, text_h: int, margin: int) -> tuple[int, int]:
    if position == "bottom_right":
        return width - text_w - margin, height - text_h - margin
    if position == "bottom_left":
        return margin, height - text_h - margin
    if position == "top_right":
        return width - text_w - margin, margin
    if position == "top_left":
        return margin, margin
    if position == "center":
        return (width - text_w) // 2, (height - text_h) // 2
    raise ValueError(f"unsupported position: {position}")


def _save_rgba_image(image: Image.Image, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    image.convert("RGB").save(output_path)


def draw_visible_watermark(
    input_path: str,
    output_path: str,
    text: str,
    position: str = "bottom_right",
    opacity: float = 0.25,
    font_scale: float = 0.5,
    thickness: int = 1,
    margin: int = 15,
    font_path: str | None = None,
):
    """
    在指定位置绘制小型显性浮水印

    position:
        - bottom_right
        - bottom_left
        - top_right
        - top_left
        - center
    """
    src = cv2.imread(input_path)
    if src is None:
        raise FileNotFoundError(input_path)

    base = Image.fromarray(cv2.cvtColor(src, cv2.COLOR_BGR2RGB)).convert("RGBA")
    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    font_size = max(12, int(round(font_scale * 48)))
    font = _load_font(font_path, font_size)
    text_w, text_h = _measure_text(draw, text, font)
    x, y = _position_xy(position, base.width, base.height, text_w, text_h, margin)
    alpha = _opacity_to_alpha(opacity)

    draw.text((x + 1, y + 1), text, font=font, fill=(0, 0, 0, alpha))
    draw.text((x, y), text, font=font, fill=(255, 255, 255, alpha))

    result = Image.alpha_composite(base, layer)
    _save_rgba_image(result, output_path)


def draw_fullscreen_watermark(
    input_path: str,
    output_path: str,
    text: str,
    opacity: float = 0.12,
    font_scale: float = 0.9,
    thickness: int = 1,
    angle: float = -30,
    x_gap: int = 220,
    y_gap: int = 140,
    font_path: str | None = None,
):
    """
    生成满屏淡浮水印
    - 稀疏
    - 低透明
    - 斜向
    """
    src = cv2.imread(input_path)
    if src is None:
        raise FileNotFoundError(input_path)

    base = Image.fromarray(cv2.cvtColor(src, cv2.COLOR_BGR2RGB)).convert("RGBA")
    w, h = base.size
    font_size = max(12, int(round(font_scale * 48)))
    font = _load_font(font_path, font_size)

    probe = Image.new("RGBA", (1, 1), (0, 0, 0, 0))
    probe_draw = ImageDraw.Draw(probe)
    tw, th = _measure_text(probe_draw, text, font)
    x_gap, y_gap = _resolve_fullscreen_spacing(tw, th, font_size, x_gap, y_gap)

    # 在大画布上先排字，避免旋转后边缘空白
    canvas_w = int(math.sqrt(w * w + h * h)) + max(x_gap, y_gap) * 2
    canvas_h = canvas_w
    canvas = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)

    start_x = 50
    start_y = 80
    alpha = _opacity_to_alpha(opacity)

    y = start_y
    row_idx = 0
    while y < canvas_h:
        x_offset = 0 if row_idx % 2 == 0 else x_gap // 2
        x = start_x + x_offset
        while x < canvas_w:
            draw.text((x, y), text, font=font, fill=(255, 255, 255, alpha))
            x += x_gap
        y += y_gap
        row_idx += 1

    # 旋转
    rotated = canvas.rotate(angle, resample=Image.BICUBIC, center=(canvas_w // 2, canvas_h // 2))

    # 裁切回原图大小
    x0 = (canvas_w - w) // 2
    y0 = (canvas_h - h) // 2
    overlay = rotated.crop((x0, y0, x0 + w, y0 + h))

    result = Image.alpha_composite(base, overlay)
    _save_rgba_image(result, output_path)