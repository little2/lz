import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import Optional, Literal

from watermark.watermark_utils import encode_transaction_id_to_short_key
from watermark.pattern_watermark import embed_pattern
from watermark.transaction_watermark_service import TransactionWatermarkService
from watermark.visible_watermark import (
    draw_visible_watermark,
    draw_fullscreen_watermark,
)


VisibleMode = Literal["none", "single", "fullscreen"]


@dataclass
class WatermarkWorkflowParams:
    # 必填
    transaction_id: int
    input_path: str
    output_path: str

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

    @classmethod
    async def run(cls, params: WatermarkWorkflowParams) -> dict:
        if not os.path.exists(params.input_path):
            raise FileNotFoundError(params.input_path)

        short_key = encode_transaction_id_to_short_key(params.transaction_id)

        # 默认显性文字：用 base62 short_key
        visible_text = params.visible_text or short_key
        fullscreen_text = params.fullscreen_text or short_key

        with tempfile.TemporaryDirectory() as tmpdir:
            current_path = params.input_path
            step_results = []

            # Step 1: invisible watermark
            if params.enable_invisible_watermark:
                next_path = os.path.join(tmpdir, "step_invisible.png")
                TransactionWatermarkService.embed_short_key(
                    current_path,
                    next_path,
                    short_key,
                )
                current_path = next_path
                step_results.append("invisible_watermark")

            # Step 2: pattern watermark
            if params.enable_pattern_watermark:
                next_path = os.path.join(tmpdir, "step_pattern.png")
                embed_pattern(
                    current_path,
                    next_path,
                    params.transaction_id,
                )
                current_path = next_path
                step_results.append("pattern_watermark")

            # Step 3: visible watermark
            if params.visible_mode == "single":
                next_path = os.path.join(tmpdir, "step_visible.png")
                draw_visible_watermark(
                    input_path=current_path,
                    output_path=next_path,
                    text=visible_text,
                    position=params.visible_position,
                    opacity=params.visible_opacity,
                    font_scale=params.visible_font_scale,
                    thickness=params.visible_thickness,
                    font_path=params.visible_font_path,
                )
                current_path = next_path
                step_results.append("visible_single")

            elif params.visible_mode == "fullscreen":
                next_path = os.path.join(tmpdir, "step_fullscreen.png")
                draw_fullscreen_watermark(
                    input_path=current_path,
                    output_path=next_path,
                    text=fullscreen_text,
                    opacity=params.fullscreen_opacity,
                    font_scale=params.fullscreen_font_scale,
                    thickness=params.fullscreen_thickness,
                    angle=params.fullscreen_angle,
                    x_gap=params.fullscreen_x_gap,
                    y_gap=params.fullscreen_y_gap,
                    font_path=params.visible_font_path,
                )
                current_path = next_path
                step_results.append("visible_fullscreen")

            elif params.visible_mode == "none":
                pass
            else:
                raise ValueError(f"unsupported visible_mode: {params.visible_mode}")

            shutil.copy(current_path, params.output_path)

        return {
            "transaction_id": params.transaction_id,
            "short_key": short_key,
            "input_path": params.input_path,
            "output_path": params.output_path,
            "steps": step_results,
            "visible_mode": params.visible_mode,
            "visible_text": visible_text if params.visible_mode == "single" else None,
            "fullscreen_text": fullscreen_text if params.visible_mode == "fullscreen" else None,
        }