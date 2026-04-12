import os
import cv2
import numpy as np
from dataclasses import dataclass
from typing import Optional, Literal

from watermark.watermark_utils import encode_transaction_id_to_short_key
from watermark.transaction_watermark_service import TransactionWatermarkService
from watermark.visible_watermark import (
    draw_visible_watermark_image,
    draw_fullscreen_watermark_image,
)


VisibleMode = Literal["none", "single", "fullscreen"]


@dataclass
class WatermarkWorkflowParams:
    # 必填
    transaction_id: int
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    input_bytes: Optional[bytes] = None
    input_ndarray: Optional[np.ndarray] = None
    output_format: str = "png"
    return_output_bytes: bool = False
    return_output_ndarray: bool = False

    # 1. 是否要隐形浮水印
    enable_invisible_watermark: bool = True
    enable_pattern_watermark: bool = True

    # 3. 是否显性浮水印
    visible_mode: VisibleMode = "none"

    # 3.1 单点显性浮水印
    visible_position: str = "bottom_right"
    visible_text: Optional[str] = None

    # 3.2 满屏显性浮水印
    fullscreen_text: Optional[str] = None

    # 通用样式参数
    visible_font_path: Optional[str] = None
    visible_opacity: float = 0.25
    visible_font_scale: float = 0.5
    visible_thickness: int = 1

    fullscreen_opacity: float = 0.12
    fullscreen_font_scale: float = 0.9
    fullscreen_thickness: int = 1
    fullscreen_angle: float = -30
    fullscreen_x_gap: int = 220
    fullscreen_y_gap: int = 140


class WatermarkWorkflow:

    @staticmethod
    def _decode_bytes_to_image(image_bytes: bytes) -> np.ndarray:
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError("input_bytes 不是有效图片")
        return img

    @staticmethod
    def _encode_image_to_bytes(img: np.ndarray, fmt: str = "png") -> bytes:
        fmt = (fmt or "png").strip().lower().lstrip(".")
        ext = f".{fmt}"
        ok, buf = cv2.imencode(ext, img)
        if not ok:
            raise RuntimeError(f"图片编码失败: {ext}")
        return buf.tobytes()

    @classmethod
    def _load_input_image(cls, params: WatermarkWorkflowParams) -> tuple[np.ndarray, str]:
        if params.input_ndarray is not None:
            return params.input_ndarray.copy(), "ndarray"

        if params.input_bytes is not None:
            return cls._decode_bytes_to_image(params.input_bytes), "bytes"

        if params.input_path:
            img = cv2.imread(params.input_path)
            if img is None:
                raise FileNotFoundError(params.input_path)
            return img, "path"

        raise ValueError("必须提供 input_ndarray / input_bytes / input_path 其中之一")

    @classmethod
    async def run(cls, params: WatermarkWorkflowParams) -> dict:
        current_img, input_source = cls._load_input_image(params)

        short_key = encode_transaction_id_to_short_key(params.transaction_id)

        # 默认显性文字：用 base62 short_key
        visible_text = params.visible_text or short_key
        fullscreen_text = params.fullscreen_text or short_key

        step_results = []

        # Step 1: invisible watermark
        if params.enable_invisible_watermark:
            current_img = TransactionWatermarkService.embed_short_key_image(
                current_img,
                short_key,
            )
            step_results.append("invisible_watermark")

        # Step 2: pattern watermark
        if params.enable_pattern_watermark:
            from watermark.pattern_watermark import embed_pattern_image

            current_img = embed_pattern_image(
                current_img,
                params.transaction_id,
            )
            step_results.append("pattern_watermark")

        # Step 3: visible watermark
        if params.visible_mode == "single":
            current_img = draw_visible_watermark_image(
                current_img,
                text=visible_text,
                position=params.visible_position,
                opacity=params.visible_opacity,
                font_scale=params.visible_font_scale,
                thickness=params.visible_thickness,
                font_path=params.visible_font_path,
            )
            step_results.append("visible_single")

        elif params.visible_mode == "fullscreen":
            current_img = draw_fullscreen_watermark_image(
                current_img,
                text=fullscreen_text,
                opacity=params.fullscreen_opacity,
                font_scale=params.fullscreen_font_scale,
                thickness=params.fullscreen_thickness,
                angle=params.fullscreen_angle,
                x_gap=params.fullscreen_x_gap,
                y_gap=params.fullscreen_y_gap,
                font_path=params.visible_font_path,
            )
            step_results.append("visible_fullscreen")

        elif params.visible_mode == "none":
            pass
        else:
            raise ValueError(f"unsupported visible_mode: {params.visible_mode}")

        if params.output_path:
            out_dir = os.path.dirname(params.output_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            cv2.imwrite(params.output_path, current_img)

        output_bytes = None
        if params.return_output_bytes:
            output_bytes = cls._encode_image_to_bytes(current_img, params.output_format)

        return {
            "transaction_id": params.transaction_id,
            "short_key": short_key,
            "input_path": params.input_path,
            "input_source": input_source,
            "output_path": params.output_path,
            "steps": step_results,
            "visible_mode": params.visible_mode,
            "visible_text": visible_text if params.visible_mode == "single" else None,
            "fullscreen_text": fullscreen_text if params.visible_mode == "fullscreen" else None,
            "output_ndarray": current_img if params.return_output_ndarray else None,
            "output_bytes": output_bytes,
        }