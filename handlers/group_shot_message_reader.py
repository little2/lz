"""GroupShotMessageReader 处理器。"""

import asyncio
import random
import time

from telethon import TelegramClient
from telethon.extensions import html as telethon_html
from telethon.sessions import StringSession
from pathlib import Path

from man_config import API_HASH, API_ID, SESSION_STRING


def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID)

	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


class GroupShotMessageReader:
	"""读取指定群（可选 topic/thread）的第一则消息。"""

	def __init__(
		self,
		target_group: int | str,
		thread_id: int | None = None,
		cooldown_minutes: int = 0,
		cooldown_state_file: Path | None = None,
		telegram_bot: TelegramClient | None = None,
	) -> None:
		self.target_group = target_group
		self.thread_id = int(thread_id) if isinstance(thread_id, int) else None
		self.cooldown_minutes = max(0, int(cooldown_minutes))
		self.cooldown_state_file = cooldown_state_file or Path(__file__).with_name(
			"group_shot_message_reader_last_run.txt"
		)
		self.telegram_bot = telegram_bot

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

	@staticmethod
	def _extract_message_text(message) -> str:
		"""提取訊息文字內容，兼容純文字與媒體 caption。"""
		for value in (
			getattr(message, "message", None),
			getattr(message, "raw_text", None),
			getattr(message, "text", None),
		):
			if isinstance(value, str) and value:
				return value
		return ""

	@staticmethod
	def _serialize_message(message) -> dict:
		text = GroupShotMessageReader._extract_message_text(message)
		entities = getattr(message, "entities", None) or []
		return {
			"id": getattr(message, "id", None),
			"date": message.date.isoformat() if getattr(message, "date", None) else None,
			"sender_id": getattr(message, "sender_id", None),
			"text": text,
			"html": telethon_html.unparse(text, entities),
		}

	@staticmethod
	def _normalize_forward_targets(forward_targets: dict | list[dict] | None) -> list[dict]:
		if forward_targets is None:
			return []

		if isinstance(forward_targets, list):
			items = forward_targets
		elif isinstance(forward_targets, dict):
			items = [value for value in forward_targets.values() if isinstance(value, dict)]
		else:
			return []

		normalized: list[dict] = []
		for item in items:
			chat_id = item.get("chat_id")
			if chat_id is None:
				continue
			normalized.append(
				{
					"chat_id": chat_id,
					"thread_id": item.get("thread_id"),
				}
			)
		return normalized

	def _load_last_run_timestamp(self) -> float | None:
		if not self.cooldown_state_file.exists():
			return None
		try:
			content = self.cooldown_state_file.read_text(encoding="utf-8").strip()
			if not content:
				return None
			return float(content)
		except Exception:
			return None

	def _save_last_run_timestamp(self, timestamp: float | None = None) -> None:
		self.cooldown_state_file.write_text(
			str(timestamp if timestamp is not None else time.time()),
			encoding="utf-8",
		)

	def _cooldown_remaining_seconds(self) -> int:
		if self.cooldown_minutes <= 0:
			return 0

		last_run_at = self._load_last_run_timestamp()
		if last_run_at is None:
			return 0

		cooldown_seconds = self.cooldown_minutes * 60
		remaining = int(cooldown_seconds - (time.time() - last_run_at))
		return max(0, remaining)

	async def _forward_message_to_targets(
		self,
		client: TelegramClient,
		message_data: dict,
		forward_targets: dict | list[dict] | None,
	) -> None:
		targets = self._normalize_forward_targets(forward_targets)
		if not targets:
			return

		payload = str(message_data.get("html", "") or message_data.get("text", "") or "")
		for target in targets:
			chat_id = target["chat_id"]
			thread_id = target.get("thread_id")
			send_kwargs = {
				"entity": chat_id,
				"message": payload,
				"parse_mode": "html",
			}
			if isinstance(thread_id, int) and thread_id > 0:
				send_kwargs["reply_to"] = thread_id
			sent_message = await client.send_message(**send_kwargs)
			print(
				f"[GroupShotMessageReader] forwarded to chat_id={chat_id}, thread_id={thread_id}",
				flush=True,
			)
			

	async def fetch_first_message(self) -> dict | None:
		"""读取第一则消息；若指定 thread_id，则读取该 topic 第二则消息。"""
		client, own_client = await self._acquire_client()
		try:
			source_entity = await self._resolve_source_entity(client)
			if isinstance(self.thread_id, int) and self.thread_id > 0:
				idx = 0
				async for message in client.iter_messages(
					source_entity,
					limit=2,
					reverse=True,
					reply_to=self.thread_id,
				):
					idx += 1
					if idx == 2:
						return self._serialize_message(message)
				return None

			async for message in client.iter_messages(source_entity, limit=1, reverse=True):
				return self._serialize_message(message)
			return None
		finally:
			if own_client:
				await client.disconnect()

	async def exec(self, forward_targets: dict | list[dict] | None = None) -> dict | None:
		"""供外部调用的执行入口。"""
		remaining_seconds = self._cooldown_remaining_seconds()
		if remaining_seconds > 0:
			remaining_minutes = max(1, (remaining_seconds + 59) // 60)
			print(
				f"[GroupShotMessageReader] 距离上次执行未满 {self.cooldown_minutes} 分钟，跳过。剩余约 {remaining_minutes} 分钟",
				flush=True,
			)
			return None

		first_message = await self.fetch_first_message()
		first_text = str(first_message.get("text", "") or "") if isinstance(first_message, dict) else ""
		if (
			first_message is not None
			and isinstance(first_message, dict)
			and "将取件码" in first_text
		):
			print(
				f"[GroupShotMessageReader] first_message contains 取件码, treating as valid: {first_message}",
				flush=True,
			)
			client, own_client = await self._acquire_client()
			try:


				randtme = random.uniform(13.0, 153.0)
				await asyncio.sleep(randtme)

				source_entity = await self._resolve_source_entity(client)
				await self._forward_message_to_targets(client, first_message, forward_targets)
				message_id = first_message.get("id")
				if isinstance(message_id, int):
					deleted = False
					try:
						source_message = await client.get_messages(source_entity, ids=message_id)
						if source_message is not None:
							deleted = bool(await source_message.delete())
					except Exception as exc:
						print(
							f"[GroupShotMessageReader] source message delete() failed, fallback to delete_messages: {exc}",
							flush=True,
						)

					if not deleted:
						result = await client.delete_messages(
							entity=source_entity,
							message_ids=[message_id],
							revoke=True,
						)
						deleted = True
						print(f"[GroupShotMessageReader] exec result: {result}", flush=True)

					print(
						f"[GroupShotMessageReader] deleted source message chat={self.target_group}, message_id={message_id}, deleted={deleted}",
						flush=True,
					)
			finally:
				if own_client:
					await client.disconnect()
		else:
			print(
				f"[GroupShotMessageReader] first_message: {first_message}",
				flush=True,
			)

		self._save_last_run_timestamp()
		return first_message


__all__ = ["GroupShotMessageReader"]
