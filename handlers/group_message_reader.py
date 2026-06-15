"""GroupMessageReader 处理器。"""

import asyncio
import json
import os
import re
from typing import Awaitable, Callable

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import ReportRequest
from telethon.tl.types import (
	ReportResultAddComment,
	ReportResultChooseOption,
	ReportResultReported,
)

from man_config import API_HASH, API_ID, SESSION_STRING
import random




def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID)

	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


async def report_message(
	client: TelegramClient,
	chat,
	message_id: int,
	report_reason: str = "child",
	keyword: str = "child",
) -> None:
	"""按 Telegram 回传的步骤提交举报。"""
	entity = await client.get_input_entity(chat)

	print(f"开始举报 id={message_id} reason={report_reason}", flush=True)

	result = await client(
		ReportRequest(
			peer=entity,
			id=[message_id],
			option=b"",
			message="",
		)
	)

	print(f"举报第一步结果 id={message_id}: {result.stringify()}", flush=True)

	step = 1
	result_reported = result
	while isinstance(result, ReportResultChooseOption):
		
		selected = None

		# 优先锁定你指定的选项：Child sexual abuse / b'21'
		for opt in result.options:
			text = (getattr(opt, "text", "") or "").strip().lower()
		
			if text == "child sexual abuse" or getattr(opt, "option", None) == b"21":
				selected = opt
				break

		# 如果该轮没有 b'21'，再走宽松匹配
		if selected is None:
			for opt in result.options:
				text = (getattr(opt, "text", "") or "").lower()
				if keyword in text or "child" in text or "abuse" in text:
					selected = opt
					break

		if selected is None:
			raise RuntimeError("没有找到合适的举报理由，请手动查看 options。")

		# print(f"选定举报理由 {selected.text} {selected.option}", flush=True)
		result = await client(
			ReportRequest(
				peer=entity,
				id=[message_id],
				option=selected.option,
				message=report_reason,
			)
		)
		result_reported = result
		step += 1
		print(f"举报第{step}步结果 id={message_id}: {result.stringify()}", flush=True)

	if isinstance(result, ReportResultAddComment):
		# print("需要添加备注内容，正在提交", flush=True)
		result_reported = await client(
			ReportRequest(
				peer=entity,
				id=[message_id],
				option=result.option,
				message=report_reason,
			)
		)
		print(f"举报第三步结果 id={message_id}: {result.stringify()}", flush=True)

	if isinstance(result_reported, ReportResultReported):
		print(f"👮 举报已提交 id={message_id} reason={report_reason}", flush=True)
		#休息 10~30秒避免短时间内同一账号举报过多被 Telegram 限制
		await asyncio.sleep(random.uniform(18.0, 137.0))
	else:
		print(f"返回结果 id={message_id}: {result_reported.stringify()}", flush=True)
		print(type(result_reported), flush=True)
		print(result_reported.stringify(), flush=True)


class GroupMessageReader:
	"""读取指定群消息，支持按间隔重复执行。"""
	_global_paras: dict | None = None
	_legacy_merged: bool = False

	def __init__(
		self,
		target_group: int | str,
		start_message_id: int = 1,
		batch_size: int = 100,
		interval_seconds: float = 10.0,
		telegram_bot: TelegramClient | None = None,
	) -> None:
		self.target_group = target_group
		self.next_message_id = max(1, int(start_message_id))
		self.batch_size = max(1, int(batch_size))
		self.interval_seconds = max(0.0, float(interval_seconds))
		self.telegram_bot = telegram_bot
		self._hydrate_state_from_global()

	@staticmethod
	def configure_global_paras(global_paras: dict | None) -> None:
		"""配置由 main() 注入的全域参数容器。"""
		GroupMessageReader._global_paras = global_paras if isinstance(global_paras, dict) else None

	@staticmethod
	def _legacy_json_path() -> str:
		file_name = str(SESSION_STRING or "")[:10]
		return f"{file_name}_group_message_reader.json"

	@staticmethod
	def _load_legacy_reader_data() -> dict:
		json_path = GroupMessageReader._legacy_json_path()
		if not os.path.exists(json_path):
			return {}
		try:
			with open(json_path, "r") as f:
				data = json.load(f)
			if isinstance(data, dict):
				return data
		except Exception as exc:
			print(f"[GroupMessageReader] 读取旧版状态文件失败: {exc}", flush=True)
		return {}

	@classmethod
	def _reader_store(cls) -> dict:
		"""获取并维护 global_paras['reader']。"""
		if not isinstance(cls._global_paras, dict):
			return {}

		reader_data = cls._global_paras.get("reader")
		if not isinstance(reader_data, dict):
			reader_data = {}
			cls._global_paras["reader"] = reader_data

		if not cls._legacy_merged:
			legacy = cls._load_legacy_reader_data()
			if legacy:
				for k, v in legacy.items():
					if isinstance(v, dict):
						legacy_next = v.get("next_message_id")
						if isinstance(legacy_next, int):
							current = reader_data.get(k)
							if not isinstance(current, dict):
								current = {}
							current_next = current.get("next_message_id")
							if not isinstance(current_next, int) or legacy_next > current_next:
								current["next_message_id"] = legacy_next
							reader_data[k] = current
					elif k == "next_message_id" and isinstance(v, int):
						default_state = reader_data.get("__default__")
						if not isinstance(default_state, dict):
							default_state = {}
						cur = default_state.get("next_message_id")
						if not isinstance(cur, int) or v > cur:
							default_state["next_message_id"] = v
						reader_data["__default__"] = default_state
			cls._legacy_merged = True

		cls._global_paras["reader"] = reader_data
		return reader_data

	def _state_key(self) -> str:
		return str(self.target_group)

	def _hydrate_state_from_global(self) -> None:
		reader_data = self._reader_store()
		if not reader_data:
			return

		state = reader_data.get(self._state_key())
		if not isinstance(state, dict):
			state = reader_data.get("__default__")
		if not isinstance(state, dict):
			return

		next_id = state.get("next_message_id")
		if isinstance(next_id, int) and next_id >= 1:
			self.next_message_id = next_id

	def _persist_state_to_global(self) -> None:
		reader_data = self._reader_store()
		if not isinstance(reader_data, dict):
			return
		reader_data[self._state_key()] = {
			"next_message_id": int(self.next_message_id),
			"batch_size": int(self.batch_size),
			"interval_seconds": float(self.interval_seconds),
		}
		if isinstance(GroupMessageReader._global_paras, dict):
			GroupMessageReader._global_paras["reader"] = reader_data

	def bind_telegram_bot(self, telegram_bot: TelegramClient | None) -> None:
		"""绑定外部传入的共用 TelegramClient。"""
		self.telegram_bot = telegram_bot

	async def _acquire_client(self) -> tuple[TelegramClient, bool]:
		"""返回 (client, own_client)。own_client=True 代表需在调用方断开连接。"""
		if self.telegram_bot is not None:
			if not self.telegram_bot.is_connected():
				await self.telegram_bot.start()
			return self.telegram_bot, False

		client = _build_client()
		await client.start()
		return client, True

	@staticmethod
	def _serialize_message(message) -> dict:
		return {
			"id": getattr(message, "id", None),
			"date": message.date.isoformat() if getattr(message, "date", None) else None,
			"sender_id": getattr(message, "sender_id", None),
			"text": getattr(message, "message", "") or "",
		}

	@staticmethod
	async def _print_view_button_url_if_needed(
		client: TelegramClient,
		source_entity,
		message,
	) -> None:
		"""若消息来自指定用户且包含“👀查看”按钮，则举报该消息。"""
		target_user_id = 7484645431
		from_peer = getattr(message, "from_id", None)
		from_user_id = getattr(from_peer, "user_id", None)
		if from_user_id is None:
			from_user_id = getattr(message, "sender_id", None)

		message_text = (getattr(message, "message", "") or "")
		link_prefix = "https://t.me/she11shopbot"
		has_shopbot_link = link_prefix in message_text

		if from_user_id != target_user_id and not has_shopbot_link:
			return

		has_view_button = False
		report_url = None

		reply_markup = getattr(message, "reply_markup", None)
		rows = getattr(reply_markup, "rows", None)
		if rows:
			for row in rows:
				for btn in getattr(row, "buttons", []) or []:
					text = getattr(btn, "text", "") or ""
					if "👀查看" in text:
						has_view_button = True
						report_url = getattr(btn, "url", None) or getattr(btn, "callback_data", None)
						break
				if has_view_button:
					break

		if not has_view_button:
			message_buttons = getattr(message, "buttons", None)
			if message_buttons:
				for row in message_buttons:
					for btn in row:
						text = getattr(btn, "text", "") or ""
						if "👀查看" in text:
							has_view_button = True
							report_url = getattr(btn, "url", None) or getattr(btn, "callback_data", None)
							break
					if has_view_button:
						break

		if not has_view_button:
			if has_shopbot_link:
				match = re.search(r"https://t\.me/she11shopbot\S*", message_text)
				report_url = match.group(0) if match else link_prefix
			else:
				return

		if not has_view_button and not has_shopbot_link:
			return
		
		message_id = getattr(message, "id", None)
		if not isinstance(message_id, int):
			return

		report_reason = [
			"This bot shares CP links: {url}",
			"This bot is distributing CP through this link: {url}",
			"This bot posts illegal CP links: {url}",
			"Please review this bot. It shares CP links: {url}",
			"This bot may be spreading CP content: {url}",
			"This bot uses this link to spread CP: {url}",
			"This bot keeps posting CP-related links: {url}",
			"Possible CP distribution through this bot: {url}",
			"This bot is sharing suspicious CP links: {url}",
			"Please investigate this bot and this link: {url}",
			"Illegal CP link shared by this bot: {url}",
			"This bot appears to post CP links: {url}",
			"Suspicious CP activity from this bot: {url}",
			"This bot sends users to CP links: {url}",
			"CP-related content found here: {url}",
			"This bot may contain illegal CP material: {url}",
			"Please check this bot for CP links: {url}",
			"This bot is posting harmful CP content: {url}",
			"This link may contain CP material: {url}",
			"Bot sharing suspicious illegal links: {url}",
			"This bot distributes unsafe CP links: {url}",
			"Please investigate possible CP content: {url}",
			"Reported CP-related link from this bot: {url}",
			"This bot may violate child safety rules: {url}",
			"Suspicious child exploitation link: {url}",
			"This bot is spreading illegal material: {url}",
			"Potential CP distribution detected: {url}",
			"Unsafe CP-related link shared here: {url}",
			"This bot repeatedly posts suspicious links: {url}",
			"Please review this suspicious bot activity: {url}",
		]

		report_reason = random.choice(report_reason).format(url=report_url)		

		try:
			await report_message(client, source_entity, message_id, report_reason=report_reason)
		except Exception as exc:
			print(f"report failed id={message_id}: {exc}", flush=True)

	async def _resolve_source_entity(self, client: TelegramClient):
		"""解析 target_group，支持 id / username / 对话补找。"""
		try:
			return await client.get_entity(self.target_group)
		except ValueError as exc:
			numeric_id = None
			if isinstance(self.target_group, int):
				numeric_id = self.target_group
			elif isinstance(self.target_group, str) and self.target_group.lstrip("-").isdigit():
				numeric_id = int(self.target_group)

			if numeric_id is not None:
				target_abs = abs(numeric_id)
				async for dialog in client.iter_dialogs():
					entity_id = getattr(dialog.entity, "id", None)
					if entity_id == target_abs:
						return dialog.entity

			raise ValueError(
				f"无法解析来源 target_group={self.target_group}。"
				"若是纯数字 user_id，请先与该对象产生会话，"
				"或改用 @username。"
			) from exc

	async def fetch_once(
		self,
		start_message_id: int | None = None,
		batch_size: int | None = None,
	) -> list[dict]:
		"""读取一批消息，并自动推进 next_message_id。"""
		start_id = max(1, int(start_message_id if start_message_id is not None else self.next_message_id))
		limit = max(1, int(batch_size if batch_size is not None else self.batch_size))

		client, own_client = await self._acquire_client()
		try:
			source_entity = await self._resolve_source_entity(client)
			rows: list[dict] = []
			last_id = start_id - 1
			async for message in client.iter_messages(
				source_entity,
				min_id=start_id - 1,
				reverse=True,
				limit=limit,
			):
				await self._print_view_button_url_if_needed(client, source_entity, message)
				rows.append(self._serialize_message(message))
				if isinstance(getattr(message, "id", None), int):
					last_id = message.id

			if rows:
				self.next_message_id = last_id + 1
				self._persist_state_to_global()
			return rows
		finally:
			if own_client:
				await client.disconnect()

	async def run_periodic(
		self,
		on_batch: Callable[[list[dict]], Awaitable[None] | None] | None = None,
		*,
		interval_seconds: float | None = None,
		stop_event: asyncio.Event | None = None,
		max_rounds: int | None = None,
	) -> None:
		"""按固定间隔重复执行 fetch_once。"""
		wait_seconds = self.interval_seconds if interval_seconds is None else max(0.0, float(interval_seconds))
		rounds = 0
		while True:
			if stop_event is not None and stop_event.is_set():
				return
			if max_rounds is not None and rounds >= max_rounds:
				return

			rows = await self.fetch_once()
			if on_batch is not None:
				result = on_batch(rows)
				if asyncio.iscoroutine(result):
					await result

			rounds += 1
			await asyncio.sleep(wait_seconds)





__all__ = ["GroupMessageReader", "report_message"]
