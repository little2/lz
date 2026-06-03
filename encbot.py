"""
使用 aiogram 的 Bot API 實現一個 Telegram Bot：

1) 接收用戶發送的檔案，取得 file_unique_id，
   先透過 build_file_token 生成 token，再用 telegram_to_unicode_cjk 轉成 CJK 字串。

2) 接收用戶貼上的 CJK 字串，
   先用 unicode_cjk_to_telegram 還原 token，再用 parse_file_token 解析欄位。
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Any

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup, Message

from utils.utf_utils import UtfConverter


BOT_TOKEN = os.getenv("ENCBOT_TOKEN") or os.getenv("BOT_TOKEN", "6493080022:AAHuFPKdhh8qYFjPLWxAYt76ikgzNMxg9iQ")

if not BOT_TOKEN:
	raise RuntimeError("Missing bot token. Please set ENCBOT_TOKEN or BOT_TOKEN.")


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
ENCODER_UI_STATE: dict[tuple[int, int], dict[str, Any]] = {}


def _extract_media_info(message: Message) -> tuple[str, str]:
	"""
	從訊息中抽取 (file_type, file_id)。
	若不是支援的媒體類型，拋出 ValueError。
	"""
	if message.document:
		return "document", message.document.file_id
	if message.photo:
		# photo 為多個尺寸，取最大尺寸通常在最後一個
		return "photo", message.photo[-1].file_id
	if message.video:
		return "video", message.video.file_id
	if message.audio:
		return "audio", message.audio.file_id
	if message.voice:
		return "voice", message.voice.file_id
	if message.animation:
		return "animation", message.animation.file_id
	if message.sticker:
		return "sticker", message.sticker.file_id

	raise ValueError("Unsupported media type")


def _build_display(data: dict[str, Any], token: str, encoded: str) -> str:
	valid_until = str(data.get("valid_until", ""))
	if valid_until == "99991231235959":
		valid_until_display = "永久有效"
	elif len(valid_until) == 14 and valid_until.isdigit():
		valid_until_display = (
			f"{valid_until[0:4]}-{valid_until[4:6]}-{valid_until[6:8]} "
			f"{valid_until[8:10]}:{valid_until[10:12]}:{valid_until[12:14]}"
		)
	else:
		valid_until_display = valid_until

	return (
		# f"nonce: {data['nonce']}\n"
		# f"user_id: {data['user_id']}\n"
		# f"file_id: {data['file_id']}\n"
		# f"file_type: {data['file_type']}\n"
		f"no_forward: {data['no_forward']}\n"
		f"flash_seconds: {data['flash_seconds']}\n"
		f"有效時間: {valid_until_display}\n\n"
		# f"token:\n{token}\n\n"
		f"encoded(CJK):\n<code>{encoded}</code>"
	)


def _resolve_valid_until(mode: str) -> str:
	if mode == "perm":
		return "99991231235959"
	if mode == "10m":
		return (datetime.now() + timedelta(minutes=10)).strftime("%Y%m%d%H%M%S")
	if mode == "30m":
		return (datetime.now() + timedelta(minutes=30)).strftime("%Y%m%d%H%M%S")
	if mode == "1h":
		return (datetime.now() + timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
	raise ValueError(f"Unsupported valid mode: {mode}")


def _choice(label: str, selected: bool) -> str:
	return f"✅ {label}" if selected else f"{label}"


def _build_controls_keyboard(state: dict[str, Any], encoded: str) -> InlineKeyboardMarkup:
	no_forward = bool(state.get("no_forward", False))
	flash_seconds = int(state.get("flash_seconds", 0))
	valid_mode = str(state.get("valid_mode", "perm"))
	long_flash_seconds = int(state.get("video_flash_seconds", 60))
	long_flash_label = f"{long_flash_seconds}秒" if str(state.get("file_type", "")) == "video" else "60秒"

	return InlineKeyboardMarkup(
		inline_keyboard=[
			[
				InlineKeyboardButton(
					text="🚫 目前限制轉發" if no_forward else "🆗 目前可以轉發",
					callback_data=f"enc:fw:{0 if no_forward else 1}",
				),
			],
			[
				InlineKeyboardButton(
					text=_choice("不閃", flash_seconds == 0),
					callback_data="enc:fl:0",
				),
				InlineKeyboardButton(
					text=_choice("30秒", flash_seconds == 30),
					callback_data="enc:fl:30",
				),
				InlineKeyboardButton(
					text=_choice(long_flash_label, flash_seconds == long_flash_seconds),
					callback_data=f"enc:fl:{long_flash_seconds}",
				),
			],
			[
				InlineKeyboardButton(
					text=_choice("永久", valid_mode == "perm"),
					callback_data="enc:vu:perm",
				),
				InlineKeyboardButton(
					text=_choice("10分鐘", valid_mode == "10m"),
					callback_data="enc:vu:10m",
				),
				InlineKeyboardButton(
					text=_choice("60分鐘", valid_mode == "30m"),
					callback_data="enc:vu:30m",
				)
			],
			[
				InlineKeyboardButton(
					text="📋 複製密文",
					copy_text=CopyTextButton(text=encoded),
				)
			],
		]
	)


def _build_token_and_encoded(state: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
	valid_until = _resolve_valid_until(str(state.get("valid_mode", "perm")))
	token = UtfConverter.build_file_token(
		user_id=int(state["user_id"]),
		file_id=str(state["file_id"]),
		file_type=str(state["file_type"]),
		no_forward=bool(state.get("no_forward", False)),
		flash_seconds=int(state.get("flash_seconds", 0)),
		valid_until=valid_until,
	)
	encoded = UtfConverter.telegram_to_unicode_cjk(token)
	parsed = UtfConverter.parse_file_token(token)
	return token, encoded, parsed


def _format_duration(seconds: int) -> str:
	seconds = max(0, int(seconds))
	days, rem = divmod(seconds, 86400)
	hours, rem = divmod(rem, 3600)
	minutes, secs = divmod(rem, 60)

	parts: list[str] = []
	if days:
		parts.append(f"{days}天")
	if hours:
		parts.append(f"{hours}小時")
	if minutes:
		parts.append(f"{minutes}分鐘")
	if secs or not parts:
		parts.append(f"{secs}秒")

	return "".join(parts)


async def _send_media_by_type(message: Message, data: dict[str, Any]) -> Message:
	file_type = str(data["file_type"])
	file_id = str(data["file_id"])
	no_forward = bool(data.get("no_forward", False))

	if file_type == "document":
		return await message.answer_document(file_id, protect_content=no_forward)
	if file_type == "photo":
		return await message.answer_photo(file_id, protect_content=no_forward)
	if file_type == "video":
		return await message.answer_video(file_id, protect_content=no_forward)
	if file_type == "audio":
		return await message.answer_audio(file_id, protect_content=no_forward)
	if file_type == "voice":
		return await message.answer_voice(file_id, protect_content=no_forward)
	if file_type == "animation":
		return await message.answer_animation(file_id, protect_content=no_forward)
	if file_type == "sticker":
		return await message.answer_sticker(file_id, protect_content=no_forward)

	raise ValueError(f"Unsupported file_type: {file_type}")


async def _delete_message_later(sent_message: Message, delay_seconds: int) -> None:
	await asyncio.sleep(delay_seconds)
	try:
		await sent_message.delete()
	except Exception:
		# 可能因權限/訊息狀態無法刪除，忽略即可
		pass


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
	await message.reply(
		"👋 你好！\n\n"
		"發送檔案（photo/document/video/audio/voice...）給我，我會回覆加密後字串。\n"
		"你也可以直接貼上加密字串，我會解碼並解析欄位。"
	)


@dp.message(
	F.chat.type == "private",
	F.document | F.photo | F.video | F.audio | F.voice | F.animation | F.sticker,
)
async def on_media(message: Message) -> None:
	try:
		file_type, file_id = _extract_media_info(message)
		state = {
			"owner_user_id": message.from_user.id if message.from_user else 0,
			"user_id": message.from_user.id if message.from_user else 0,
			"file_id": file_id,
			"file_type": file_type,
			"no_forward": False,
			"flash_seconds": 0,
			"video_flash_seconds": (int(getattr(message.video, "duration", 0) or 0) + 15) if file_type == "video" else 60,
			"valid_mode": "perm",
		}

		token, encoded, parsed = _build_token_and_encoded(state)
		markup = _build_controls_keyboard(state, encoded)
		panel = await message.reply(_build_display(parsed, token, encoded), reply_markup=markup, parse_mode="HTML")
		ENCODER_UI_STATE[(message.chat.id, panel.message_id)] = state
	except Exception as exc:
		await message.reply(f"❌ 編碼失敗: {exc}")


@dp.callback_query(F.data.startswith("enc:"))
async def on_encode_controls(callback: CallbackQuery) -> None:
	if not callback.message:
		await callback.answer("無法取得訊息", show_alert=True)
		return
	if callback.message.chat.type != "private":
		await callback.answer("僅支援私信", show_alert=True)
		return

	state_key = (callback.message.chat.id, callback.message.message_id)
	state = ENCODER_UI_STATE.get(state_key)
	if not state:
		await callback.answer("此按鈕已失效，請重新傳送媒體", show_alert=True)
		return

	if (callback.from_user and callback.from_user.id) != int(state.get("owner_user_id", 0)):
		await callback.answer("只能由原發送者操作", show_alert=True)
		return

	try:
		_, group, value = str(callback.data).split(":", 2)
		if group == "fw":
			state["no_forward"] = value == "1"
		elif group == "fl":
			state["flash_seconds"] = int(value)
		elif group == "vu":
			if value not in {"perm", "10m", "30m", "1h"}:
				raise ValueError("invalid valid mode")
			state["valid_mode"] = value
		else:
			raise ValueError("unknown control group")

		long_flash_seconds = int(state.get("video_flash_seconds", 60))
		force_no_forward = (
			int(state.get("flash_seconds", 0)) in {30, long_flash_seconds}
			or str(state.get("valid_mode", "perm")) in {"10m", "30m"}
		)
		if force_no_forward:
			state["no_forward"] = True

		token, encoded, parsed = _build_token_and_encoded(state)
		markup = _build_controls_keyboard(state, encoded)
		await callback.message.edit_text(_build_display(parsed, token, encoded), reply_markup=markup, parse_mode="HTML")
		await callback.answer("已更新密文")
	except Exception as exc:
		await callback.answer(f"更新失敗: {exc}", show_alert=True)


@dp.message(F.chat.type == "private", F.text)
async def on_text(message: Message) -> None:
	text = (message.text or "").strip()
	if not text:
		return

	try:
		token = UtfConverter.unicode_cjk_to_telegram(text)
		data = UtfConverter.parse_file_token(token)

		valid_until_dt = datetime.strptime(str(data["valid_until"]), "%Y%m%d%H%M%S")
		now = datetime.now()

		if now > valid_until_dt:
			overdue_seconds = int((now - valid_until_dt).total_seconds())
			overdue_text = _format_duration(overdue_seconds)
			await message.reply(
				"❌ 此 token 已過期\n"
				f"過期時間: {valid_until_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
				f"已過期: {overdue_text}"
			)
			return

		sent_media_message = await _send_media_by_type(message, data)

		flash_seconds = int(data.get("flash_seconds", 0))
		if flash_seconds > 0:
			asyncio.create_task(_delete_message_later(sent_media_message, flash_seconds))

		'''
		await message.reply(
			"✅ 解碼成功\n\n"
			f"token:\n{token}\n\n"
			"解析欄位:\n"
			f"nonce: {data['nonce']}\n"
			f"user_id: {data['user_id']}\n"
			f"file_id: {data['file_id']}\n"
			f"file_type: {data['file_type']}\n"
			f"no_forward: {data['no_forward']}\n"
			f"flash_seconds: {data['flash_seconds']}\n"
			f"valid_until: {data['valid_until']}"
		)
		'''
	except Exception as exc:
		await message.reply(f"❌ 解碼或解析失敗: {exc}")


async def main() -> None:
	await dp.start_polling(bot)


if __name__ == "__main__":
	asyncio.run(main())
