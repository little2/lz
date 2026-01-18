from watermark_processor import (
    WatermarkProcessor,
    TiledWatermarkConfig,
    SingleWatermarkConfig,
    RembgConfig,
)

WatermarkProcessor.process_file(
    img_path="./input/487317/01.jpg",
    output_path="./output/487317/wm_01.png",
    tiled_cfg=TiledWatermarkConfig(
        text=" LYAI ",
        font_path="./font/msyh.ttc",
        opacity=170,
        angle=30,
    ),
    single_cfg=SingleWatermarkConfig(
        text="本作品由 AI 合成，仅限本课程学习使用。\n擅自公开传播者将承担全部法律责任并被依法追究。",
        font_path="./font/msyh.ttc",
        position="上方",
    ),
    rembg_cfg=RembgConfig(
        model_path="u2net.onnx",
    ),
)
