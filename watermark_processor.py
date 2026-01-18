# -*- coding: utf-8 -*-
"""
watermark_processor.py (classmethod style)

- 全图平铺水印
- 单独水印
- rembg 抠图排除主体水印
- 全局缓存 font / rembg session
"""

import os
import shutil
import tempfile
import hashlib
import urllib.request
from typing import Tuple, Optional, Dict
from dataclasses import dataclass

from PIL import Image, ImageDraw, ImageFont, ImageChops
from rembg import remove, new_session


RGB = Tuple[int, int, int]


# =========================
# 配置对象
# =========================

@dataclass(frozen=True)
class TiledWatermarkConfig:
    text: str
    font_path: str
    font_size: int = 40
    color: RGB = (255, 255, 255)
    opacity: int = 160
    angle: int = 30
    density: str = "normal"   # dense / normal / sparse
    x_gap: Optional[int] = None
    y_gap: Optional[int] = None
    offset: Optional[int] = None


@dataclass(frozen=True)
class SingleWatermarkConfig:
    text: str
    font_path: str
    font_size: int = 16
    color: RGB = (120, 120, 255)
    opacity: int = 255
    position: str = "右下"
    margin: int = 30


@dataclass(frozen=True)
class RembgConfig:
    model_name: str = "u2net"
    model_path: str = "u2net.onnx"
    # 若 model_path 不存在，可提供 model_url 让程序在运行时自动下载（Render 等 CI/CD 环境推荐）
    model_url: str = "https://github.com/danielgatis/rembg/releases/download/v0.0.0/u2net.onnx"
    # 可选：下载后的 SHA256 校验（留空则不校验）
    model_sha256: Optional[str] = None
    # 下载超时（秒）
    download_timeout: int = 600
    u2net_home: Optional[str] = None



# =========================
# 主 Class（classmethod）
# =========================

class WatermarkProcessor:
    _FONT_CACHE: Dict[Tuple[str, int], ImageFont.FreeTypeFont] = {}
    _REMBG_SESSION_CACHE: Dict[Tuple[str, str], object] = {}

    # ---------- public API ----------

    @classmethod
    def process_file(
        cls,
        img_path: str,
        output_path: str,
        tiled_cfg: Optional[TiledWatermarkConfig] = None,
        single_cfg: Optional[SingleWatermarkConfig] = None,
        rembg_cfg: Optional[RembgConfig] = None,
        exclude_subject: bool = True,
    ) -> str:
        img = Image.open(img_path).convert("RGBA")
        out = cls.process_image(
            img,
            tiled_cfg=tiled_cfg,
            single_cfg=single_cfg,
            rembg_cfg=rembg_cfg,
            exclude_subject=exclude_subject,
        )

        if output_path.endswith("/"):
            raise ValueError("output_path 必须是文件路径，而不是目录路径。")


        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        out.save(output_path)
        return output_path

    @classmethod
    def process_image(
        cls,
        img: Image.Image,
        *,
        tiled_cfg: Optional[TiledWatermarkConfig],
        single_cfg: Optional[SingleWatermarkConfig],
        rembg_cfg: Optional[RembgConfig],
        exclude_subject: bool,
    ) -> Image.Image:

        base = img.convert("RGBA")

        # 平铺水印
        if tiled_cfg:
            tiled_layer = cls._build_tiled_layer(base.size, tiled_cfg)

            if exclude_subject:
                if not rembg_cfg:
                    raise ValueError("exclude_subject=True 时必须提供 rembg_cfg")
                subject_alpha = cls._extract_subject_alpha(base, rembg_cfg)
                tiled_layer = cls._exclude_subject_area(tiled_layer, subject_alpha)

            base = Image.alpha_composite(base, tiled_layer)

        # 单独水印
        if single_cfg:
            base = cls._add_single_watermark(base, single_cfg)

        return base

    # ---------- tiled watermark ----------

    @classmethod
    def _build_tiled_layer(cls, size: Tuple[int, int], cfg: TiledWatermarkConfig) -> Image.Image:
        width, height = size
        layer = Image.new("RGBA", size, (0, 0, 0, 0))
        font = cls._get_font(cfg.font_path, cfg.font_size)

        dummy_draw = ImageDraw.Draw(layer)
        text_w, text_h = cls._measure_text(dummy_draw, cfg.text, font)

        density_map = {"dense": 1.2, "normal": 1.7, "sparse": 2.2}
        factor = density_map.get(cfg.density, 1.7)

        x_gap = cfg.x_gap or int(text_w * factor)
        y_gap = cfg.y_gap or int(text_h * factor)
        offset = cfg.offset or (x_gap // 2)

        diag = int((width ** 2 + height ** 2) ** 0.5)
        tmp = Image.new("RGBA", (diag, diag), (0, 0, 0, 0))
        draw = ImageDraw.Draw(tmp)

        for y in range(0, diag, y_gap):
            xo = offset if (y // y_gap) % 2 else 0
            for x in range(-diag, diag, x_gap):
                draw.text(
                    (x + xo, y),
                    cfg.text,
                    font=font,
                    fill=cfg.color + (cls._clamp(cfg.opacity),),
                )

        tmp = tmp.rotate(cfg.angle, expand=True, resample=Image.BICUBIC)
        layer.paste(tmp, ((width - tmp.width) // 2, (height - tmp.height) // 2), tmp)
        return layer

    # ---------- single watermark ----------

    @classmethod
    def _add_single_watermark(cls, img: Image.Image, cfg: SingleWatermarkConfig) -> Image.Image:
        base = img.convert("RGBA")
        font = cls._get_font(cfg.font_path, cfg.font_size)
        layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(layer)

        w, h = cls._measure_text(draw, cfg.text, font)

        pos_map = {
            "左上": (cfg.margin, cfg.margin),
            "上方": ((base.width - w) // 2, cfg.margin),
            "右上": (base.width - w - cfg.margin, cfg.margin),
            "左边": (cfg.margin, (base.height - h) // 2),
            "中心": ((base.width - w) // 2, (base.height - h) // 2),
            "右边": (base.width - w - cfg.margin, (base.height - h) // 2),
            "左下": (cfg.margin, base.height - h - cfg.margin),
            "下方": ((base.width - w) // 2, base.height - h - cfg.margin),
            "右下": (base.width - w - cfg.margin, base.height - h - cfg.margin),
        }

        draw.text(
            pos_map.get(cfg.position, pos_map["右下"]),
            cfg.text,
            font=font,
            fill=cfg.color + (cls._clamp(cfg.opacity),),
        )
        return Image.alpha_composite(base, layer)


    @classmethod
    def ensure_model(cls, cfg: RembgConfig) -> str:
        """确保 rembg 所需模型文件存在；不存在则按 cfg.model_url 下载。

        设计目标：
        - 兼容 Render 等 ephemeral filesystem：每次冷启动可自动恢复模型
        - 下载采用临时文件 + 原子替换，避免中途失败留下损坏文件
        - 可选 SHA256 校验（cfg.model_sha256）
        """
        here = os.path.dirname(os.path.abspath(__file__))
        model_path = cfg.model_path if os.path.isabs(cfg.model_path) else os.path.join(here, cfg.model_path)
        model_path = os.path.abspath(model_path)

        if os.path.exists(model_path):
            return model_path

        if not cfg.model_url:
            raise FileNotFoundError(f"u2net 模型不存在且未提供 model_url: {model_path}")

        os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)

        tmp_fd, tmp_path = tempfile.mkstemp(prefix="u2net_", suffix=".onnx", dir=os.path.dirname(model_path))
        os.close(tmp_fd)

        try:
            print(f"[Model] Downloading u2net.onnx -> {model_path}", flush=True)
            with urllib.request.urlopen(cfg.model_url, timeout=cfg.download_timeout) as r, open(tmp_path, "wb") as f:
                shutil.copyfileobj(r, f)

            # 可选 SHA256 校验
            if cfg.model_sha256:
                h = hashlib.sha256()
                with open(tmp_path, "rb") as f:
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        h.update(chunk)
                digest = h.hexdigest().lower()
                if digest != cfg.model_sha256.strip().lower():
                    raise ValueError(f"模型 SHA256 校验失败: expected={cfg.model_sha256} got={digest}")

            # 原子替换（同一文件系统内）
            os.replace(tmp_path, model_path)
            print("[Model] Download complete", flush=True)
            return model_path
        finally:
            # 若失败，清理临时文件
            if os.path.exists(tmp_path) and tmp_path != model_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    # ---------- rembg ----------

    @classmethod
    def _extract_subject_alpha(cls, img: Image.Image, cfg: RembgConfig) -> Image.Image:
        sess = cls._get_rembg_session(cfg)
        fg = remove(img, session=sess).convert("RGBA")
        return fg.split()[-1]

    @classmethod
    def _get_rembg_session(cls, cfg: RembgConfig):
        here = os.path.dirname(os.path.abspath(__file__))
        model_path = cfg.model_path if os.path.isabs(cfg.model_path) else os.path.join(here, cfg.model_path)
        model_path = os.path.abspath(model_path)

        # 若模型不存在，尝试按配置自动下载（Render 等环境推荐）
        if not os.path.exists(model_path):
            model_path = cls.ensure_model(cfg)

        os.environ["U2NET_HOME"] = cfg.u2net_home or here
        key = (cfg.model_name, model_path)

        if key not in cls._REMBG_SESSION_CACHE:
            cls._REMBG_SESSION_CACHE[key] = new_session(
                model_name=cfg.model_name,
                model_path=model_path
            )
        return cls._REMBG_SESSION_CACHE[key]

    # ---------- utils ----------

    @classmethod
    def _get_font(cls, font_path: str, size: int):
        key = (font_path, size)
        if key not in cls._FONT_CACHE:
            cls._FONT_CACHE[key] = ImageFont.truetype(font_path, size)
        return cls._FONT_CACHE[key]

    @staticmethod
    def _measure_text(draw, text, font):
        try:
            box = draw.textbbox((0, 0), text, font=font)
            return box[2] - box[0], box[3] - box[1]
        except Exception:
            return font.getsize(text)

    @staticmethod
    def _exclude_subject_area(layer: Image.Image, subject_alpha: Image.Image) -> Image.Image:
        inv = ImageChops.invert(subject_alpha)
        r, g, b, a = layer.split()
        return Image.merge("RGBA", (r, g, b, ImageChops.multiply(a, inv)))

    @staticmethod
    def _clamp(v: int) -> int:
        return max(0, min(255, int(v)))
