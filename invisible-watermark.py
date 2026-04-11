import os
import json
import asyncio
from pathlib import Path

from lz_mysql import MySQLPool
from watermark.watermark_workflow import WatermarkWorkflow, WatermarkWorkflowParams
from watermark.transaction_watermark_service import TransactionWatermarkService


INPUT_IMAGE = "input.png"
OUTPUT_DIR = "watermark_verify_output"
TRANSACTION_ID = 1027576


def ensure_output_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def pretty(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


async def build_cases():
    """
    生成多种水印模式
    """
    cases = [
        # (
        #     "01_invisible_only",
        #     WatermarkWorkflowParams(
        #         transaction_id=TRANSACTION_ID,
        #         input_path=INPUT_IMAGE,
        #         output_path=os.path.join(OUTPUT_DIR, "01_invisible_only.png"),
        #         enable_invisible_watermark=True,
        #         enable_pattern_watermark=True,
        #         visible_mode="none",
        #     )
        # ),
        # (
        #     "02_layer1_only",
        #     WatermarkWorkflowParams(
        #         transaction_id=TRANSACTION_ID,
        #         input_path=INPUT_IMAGE,
        #         output_path=os.path.join(OUTPUT_DIR, "02_layer1_only.png"),
        #         enable_invisible_watermark=True,
        #         enable_pattern_watermark=False,
        #         visible_mode="none",
        #     )
        # ),
        # (
        #     "03_layer2_only",
        #     WatermarkWorkflowParams(
        #         transaction_id=TRANSACTION_ID,
        #         input_path=INPUT_IMAGE,
        #         output_path=os.path.join(OUTPUT_DIR, "03_layer2_only.png"),
        #         enable_invisible_watermark=False,
        #         enable_pattern_watermark=True,
        #         visible_mode="none",
        #     )
        # ),
        # (
        #     "04_invisible_plus_visible_bottom_right",
        #     WatermarkWorkflowParams(
        #         transaction_id=TRANSACTION_ID,
        #         input_path=INPUT_IMAGE,
        #         output_path=os.path.join(OUTPUT_DIR, "04_invisible_plus_visible_bottom_right.png"),
        #         enable_invisible_watermark=True,
        #         enable_pattern_watermark=True,
        #         visible_mode="single",
        #         visible_position="bottom_right",
        #         visible_text="写入后成功追查出",
        #         visible_opacity=0.25,
        #         visible_font_scale=0.5,
        #         visible_thickness=1,
        #     )
        # ),
        # (
        #     "05_visible_only_bottom_right",
        #     WatermarkWorkflowParams(
        #         transaction_id=TRANSACTION_ID,
        #         input_path=INPUT_IMAGE,
        #         output_path=os.path.join(OUTPUT_DIR, "05_visible_only_bottom_right.png"),
        #         enable_invisible_watermark=False,
        #         enable_pattern_watermark=False,
        #         visible_mode="single",
        #         visible_position="bottom_right",
        #         visible_text="写入后成功追查出",
        #         visible_opacity=0.25,
        #         visible_font_scale=0.5,
        #         visible_thickness=1,
        #     )
        # ),
        (
            "06_invisible_plus_visible_top_left_custom_text",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "06_invisible_plus_visible_top_left_custom_text.png"),
                enable_invisible_watermark=True,
                enable_pattern_watermark=True,
                visible_mode="single",
                visible_position="top_left",
                visible_text="写入后成功追查出的水印",
                visible_opacity=0.22,
                visible_font_scale=0.55,
                visible_thickness=1,
            )
        ),
        (
            "07_invisible_plus_fullscreen",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "07_invisible_plus_fullscreen.png"),
                enable_invisible_watermark=True,
                enable_pattern_watermark=True,
                visible_mode="fullscreen",
                fullscreen_text="写入后成功追查出的水印",
                fullscreen_opacity=0.12,
                fullscreen_font_scale=0.9,
                fullscreen_thickness=1,
                fullscreen_angle=-30,
                fullscreen_x_gap=220,
                fullscreen_y_gap=140,
            )
        ),
        (
            "08_fullscreen_only",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "08_fullscreen_only.png"),
                enable_invisible_watermark=False,
                enable_pattern_watermark=False,
                visible_mode="fullscreen",
                fullscreen_text=None,
                fullscreen_opacity=0.12,
                fullscreen_font_scale=0.9,
                fullscreen_thickness=1,
                fullscreen_angle=-30,
                fullscreen_x_gap=220,
                fullscreen_y_gap=140,
            )
        ),
        (
            "09_passthrough",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "09_passthrough.png"),
                enable_invisible_watermark=False,
                enable_pattern_watermark=False,
                visible_mode="none",
            )
        ),
    ]
    return cases


async def verify_case(name: str, image_path: str, expected_transaction_id: int) -> dict:
    """
    对单一输出图做自动追查验证
    """
    try:
        report = await TransactionWatermarkService.investigate(image_path)

        top = report.get("top", []) or []
        top1 = top[0] if top else None
        top1_tid = top1["transaction_id"] if top1 else None
        hit = (top1_tid == expected_transaction_id)

        return {
            "case": name,
            "image_path": image_path,
            "status": "ok",
            "short_key": report.get("short_key"),
            "suffix": report.get("suffix"),
            "top1_transaction_id": top1_tid,
            "top1_score": top1.get("score") if top1 else None,
            "hit_expected_transaction": hit,
            "top5": top[:5],
        }

    except Exception as e:
        return {
            "case": name,
            "image_path": image_path,
            "status": "error",
            "error": str(e),
        }


async def main():
    if not os.path.exists(INPUT_IMAGE):
        raise FileNotFoundError(f"找不到测试图片: {INPUT_IMAGE}")

    ensure_output_dir(OUTPUT_DIR)

    await MySQLPool.init_pool()

    cases = await build_cases()

    generation_results = []
    verify_results = []

    print("===== STEP 1: 生成测试图片 =====", flush=True)
    for name, params in cases:
        print(f"\n[GENERATE] {name}", flush=True)
        result = await WatermarkWorkflow.run(params)
        generation_results.append({
            "case": name,
            **result,
        })
        print(pretty(result), flush=True)

    print("\n===== STEP 2: 自动追查验证 =====", flush=True)
    for item in generation_results:
        name = item["case"]
        image_path = item["output_path"]

        print(f"\n[VERIFY] {name}", flush=True)
        result = await verify_case(
            name=name,
            image_path=image_path,
            expected_transaction_id=TRANSACTION_ID,
        )
        verify_results.append(result)
        print(pretty(result), flush=True)

    summary = {
        "transaction_id": TRANSACTION_ID,
        "input_image": INPUT_IMAGE,
        "output_dir": os.path.abspath(OUTPUT_DIR),
        "generation_results": generation_results,
        "verify_results": verify_results,
    }

    summary_path = os.path.join(OUTPUT_DIR, "verify_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

    print("\n===== STEP 3: 结果摘要 =====", flush=True)
    for row in verify_results:
        if row["status"] == "ok":
            print(
                f"{row['case']}: "
                f"short_key={row.get('short_key')} | "
                f"top1={row.get('top1_transaction_id')} | "
                f"score={row.get('top1_score')} | "
                f"hit={row.get('hit_expected_transaction')}",
                flush=True
            )
        else:
            print(
                f"{row['case']}: ERROR -> {row['error']}",
                flush=True
            )

    print(f"\n验证结果已写入: {summary_path}", flush=True)

    await MySQLPool.close()


if __name__ == "__main__":
    asyncio.run(main())