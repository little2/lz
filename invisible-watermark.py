import os
import json
import asyncio
import io
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import BufferedInputFile, Message

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


async def watermark_file_id_with_aiogram(
    bot: Bot,
    source_file_id: str,
    upload_chat_id: int,
    transaction_id: int,
    *,
    enable_invisible_watermark: bool = True,
    enable_pattern_watermark: bool = True,
    visible_mode: str = "none",
    visible_position: str = "bottom_right",
    visible_text: Optional[str] = None,
    fullscreen_text: Optional[str] = None,
    visible_font_path: Optional[str] = None,
    visible_opacity: float = 0.25,
    visible_font_scale: float = 0.5,
    visible_thickness: int = 1,
    fullscreen_opacity: float = 0.12,
    fullscreen_font_scale: float = 0.9,
    fullscreen_thickness: int = 1,
    fullscreen_angle: float = -30,
    fullscreen_x_gap: int = 220,
    fullscreen_y_gap: int = 140,
) -> dict:
    """
    透过 Telegram file_id 抓图，加上水印后再上传，回传新的 file_id。
    """
    file_meta = await bot.get_file(source_file_id)
    file_path = getattr(file_meta, "file_path", None)
    if not file_path:
        raise ValueError("无法从 file_id 取得 file_path")

    input_buf = io.BytesIO()
    await bot.download_file(file_path, destination=input_buf)
    input_bytes = input_buf.getvalue()
    if not input_bytes:
        raise RuntimeError("下载原图失败：内容为空")

    params = WatermarkWorkflowParams(
        transaction_id=transaction_id,
        input_bytes=input_bytes,
        return_output_bytes=True,
        output_format="png",
        enable_invisible_watermark=enable_invisible_watermark,
        enable_pattern_watermark=enable_pattern_watermark,
        visible_mode=visible_mode,
        visible_position=visible_position,
        visible_text=visible_text,
        fullscreen_text=fullscreen_text,
        visible_font_path=visible_font_path,
        visible_opacity=visible_opacity,
        visible_font_scale=visible_font_scale,
        visible_thickness=visible_thickness,
        fullscreen_opacity=fullscreen_opacity,
        fullscreen_font_scale=fullscreen_font_scale,
        fullscreen_thickness=fullscreen_thickness,
        fullscreen_angle=fullscreen_angle,
        fullscreen_x_gap=fullscreen_x_gap,
        fullscreen_y_gap=fullscreen_y_gap,
    )

    workflow_result = await WatermarkWorkflow.run(params)
    output_bytes = workflow_result.get("output_bytes")
    if not output_bytes:
        raise RuntimeError("水印输出为空")

    sent = await bot.send_document(
        chat_id=upload_chat_id,
        document=BufferedInputFile(output_bytes, filename="watermarked.png"),
    )
    watermarked_file_id = sent.document.file_id if sent.document else None
    if not watermarked_file_id:
        raise RuntimeError("上传水印图片后未取得新的 file_id")

    return {
        "source_file_id": source_file_id,
        "watermarked_file_id": watermarked_file_id,
        "upload_chat_id": upload_chat_id,
        "workflow": workflow_result,
    }


async def run_aiogram_demo() -> None:
    """
    以环境变量示范 file_id -> 水印 -> 新 file_id。

    必填:
    - BOT_TOKEN
    - SOURCE_FILE_ID
    - UPLOAD_CHAT_ID
    可选:
    - TRANSACTION_ID
    """
    bot_token = os.getenv("BOT_TOKEN")
    source_file_id = os.getenv("SOURCE_FILE_ID")
    upload_chat_id_raw = os.getenv("UPLOAD_CHAT_ID")
    transaction_id_raw = os.getenv("TRANSACTION_ID")

    if not bot_token:
        raise ValueError("缺少 BOT_TOKEN")
    if not source_file_id:
        raise ValueError("缺少 SOURCE_FILE_ID")
    if not upload_chat_id_raw:
        raise ValueError("缺少 UPLOAD_CHAT_ID")

    upload_chat_id = int(upload_chat_id_raw)
    transaction_id = int(transaction_id_raw) if transaction_id_raw else TRANSACTION_ID

    async with Bot(token=bot_token) as bot:
        result = await watermark_file_id_with_aiogram(
            bot=bot,
            source_file_id=source_file_id,
            upload_chat_id=upload_chat_id,
            transaction_id=transaction_id,
            enable_invisible_watermark=True,
            enable_pattern_watermark=True,
            visible_mode="none",
        )
        print("===== AIOGRAM FILE_ID WATERMARK RESULT =====", flush=True)
        print(pretty(result), flush=True)


async def run_aiogram_polling() -> None:
    """
    以 polling 模式运行，收到图片后自动加水印并回传新 file_id。
    """
    bot_token = os.getenv("BOT_TOKEN")
    transaction_id_raw = os.getenv("TRANSACTION_ID")


    bot_token = "5982516833:AAFYBjWsVJ9zeLZYKx9mvEnEB2OsPgw_-ow"
    transaction_id_raw = 1027576


    if not bot_token:
        raise ValueError("缺少 BOT_TOKEN")

    transaction_id = int(transaction_id_raw) if transaction_id_raw else TRANSACTION_ID

    dp = Dispatcher()

    @dp.message(F.photo)
    async def on_photo(message: Message) -> None:
        try:
            source_file_id = message.photo[-1].file_id
            result = await watermark_file_id_with_aiogram(
                bot=message.bot,
                source_file_id=source_file_id,
                upload_chat_id=message.chat.id,
                transaction_id=transaction_id,
                enable_invisible_watermark=True,
                enable_pattern_watermark=True,
                visible_mode="single",
                visible_position="bottom_left",
                visible_text=str(transaction_id),
                visible_opacity=0.15,
                visible_font_scale=0.55,
                visible_thickness=1,
            )
            await message.reply(
                f"watermarked_file_id: {result['watermarked_file_id']}"
            )
        except Exception as e:
            await message.reply(f"处理失败: {e}")

    @dp.message(F.document)
    async def on_document(message: Message) -> None:
        doc = message.document
        if not doc:
            return

        mime_type = (doc.mime_type or "").lower()
        if not mime_type.startswith("image/"):
            return

        try:
            result = await watermark_file_id_with_aiogram(
                bot=message.bot,
                source_file_id=doc.file_id,
                upload_chat_id=message.chat.id,
                transaction_id=transaction_id,
                enable_invisible_watermark=True,
                enable_pattern_watermark=True,
                visible_mode="none",
            )
            await message.reply(
                f"watermarked_file_id: {result['watermarked_file_id']}"
            )
        except Exception as e:
            await message.reply(f"处理失败: {e}")

    async with Bot(token=bot_token) as bot:
        print("Polling started. Send photo or image document to watermark.", flush=True)
        await dp.start_polling(bot)


async def build_cases():
    """
    生成多种水印模式
    """
    cases = [
        (
            "01_invisible_only",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "01_invisible_only.png"),
                enable_invisible_watermark=True,
                enable_pattern_watermark=True,
                visible_mode="none",
            )
        ),
        (
            "02_layer1_only",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "02_layer1_only.png"),
                enable_invisible_watermark=True,
                enable_pattern_watermark=False,
                visible_mode="none",
            )
        ),
        (
            "03_layer2_only",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "03_layer2_only.png"),
                enable_invisible_watermark=False,
                enable_pattern_watermark=True,
                visible_mode="none",
            )
        ),
        (
            "04_invisible_plus_visible_bottom_right",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "04_invisible_plus_visible_bottom_right.png"),
                enable_invisible_watermark=True,
                enable_pattern_watermark=True,
                visible_mode="single",
                visible_position="bottom_right",
                visible_text="写入后成功追查出",
                visible_opacity=0.25,
                visible_font_scale=0.5,
                visible_thickness=1,
            )
        ),
        (
            "05_visible_only_bottom_right",
            WatermarkWorkflowParams(
                transaction_id=TRANSACTION_ID,
                input_path=INPUT_IMAGE,
                output_path=os.path.join(OUTPUT_DIR, "05_visible_only_bottom_right.png"),
                enable_invisible_watermark=False,
                enable_pattern_watermark=False,
                visible_mode="single",
                visible_position="bottom_right",
                visible_text="写入后成功追查出",
                visible_opacity=0.25,
                visible_font_scale=0.5,
                visible_thickness=1,
            )
        ),
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
    # RUN_MODE=aiogram: file_id demo
    # RUN_MODE=polling: 收图即加水印
    # 其他: 维持原本验证流程
    run_mode = os.getenv("RUN_MODE", "verify").strip().lower()

    run_mode = "polling"

    if run_mode == "aiogram":
        asyncio.run(run_aiogram_demo())
    elif run_mode == "polling":
        asyncio.run(run_aiogram_polling())
    else:
        asyncio.run(main())