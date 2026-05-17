"""GroupMediaForwarder 处理器。"""
import asyncio
import json
import random
import tempfile
from pathlib import Path

from telethon import TelegramClient
from telethon.errors.rpcerrorlist import ChatForwardsRestrictedError, FileReferenceExpiredError
from telethon.sessions import StringSession

from man_config import API_HASH, API_ID, SESSION_STRING


def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID)

	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)

class GroupMediaForwarder:
	"""从指定群组抓取媒体消息并转发到目标。"""
	_global_paras: dict | None = None
	_legacy_merged: bool = False

	def __init__(
		self,
		target_group: int | str,
		forward_to: str | int | dict | tuple | list,
		start_message_id: int = 1,
		caption_json_mode: bool = False,
		skip_caption_check: bool = False,
		sleep_enabled: bool = True,
		sleep_min_seconds: int = 67,
		sleep_max_seconds: int = 1153,
		state_file: Path | None = None,
		white_list_group_1: list[str] | None = None,
		white_list_group_2: list[str] | None = None,
		black_list: list[str] | None = None,
		keyword_routes: list[dict] | None = None,
		download_fallback_enabled: bool = True,
		backup_chat_id: int | str | None = None,
		backup_thread_id: int | None = None,
		telegram_bot: TelegramClient | None = None,
	) -> None:
		self.target_group = target_group
		self.forward_to = forward_to
		self.default_start_message_id = start_message_id
		self.caption_json_mode = caption_json_mode
		self.skip_caption_check = skip_caption_check
		self.sleep_enabled = sleep_enabled
		self.sleep_min_seconds = max(0, int(sleep_min_seconds))
		self.sleep_max_seconds = max(0, int(sleep_max_seconds))
		self.state_file = state_file or Path(__file__).with_name("man_last_message_id.txt")
		self.white_list_group_1 = white_list_group_1 or []
		self.white_list_group_2 = white_list_group_2 or []
		self.black_list = black_list or []
		# keyword_routes: [{"keywords": [...], "chat_id": int|str, "thread_id": int|None}, ...]
		self.keyword_routes: list[dict] = keyword_routes or []
		self.download_fallback_enabled = bool(download_fallback_enabled)
		self.backup_chat_id = backup_chat_id
		self.backup_thread_id = backup_thread_id
		self.telegram_bot = telegram_bot
		self._merge_legacy_into_global_store()

	def bind_telegram_bot(self, telegram_bot: TelegramClient | None) -> None:
		"""绑定 main() 传入的共用 TelegramClient。"""
		self.telegram_bot = telegram_bot

	@staticmethod
	def configure_global_paras(global_paras: dict | None) -> None:
		"""配置由 main() 注入的全域参数容器。"""
		GroupMediaForwarder._global_paras = global_paras if isinstance(global_paras, dict) else None

	@classmethod
	def _forwarder_store(cls) -> dict:
		if not isinstance(cls._global_paras, dict):
			return {}
		store = cls._global_paras.get("forwarder")
		if not isinstance(store, dict):
			store = {}
			cls._global_paras["forwarder"] = store
		return store

	def _merge_legacy_into_global_store(self) -> None:
		"""把旧参数/本地状态合并进 global_paras['forwarder']。"""
		store = self._forwarder_store()
		if not isinstance(store, dict):
			return

		if self.backup_chat_id is not None and store.get("backup_chat_id") is None:
			store["backup_chat_id"] = self.backup_chat_id
		if self.backup_thread_id is not None and store.get("backup_thread_id") is None:
			store["backup_thread_id"] = self.backup_thread_id

		if not GroupMediaForwarder._legacy_merged:
			legacy_state = self._load_state_data_local_only()
			if legacy_state:
				state_data = store.get("state_data")
				if not isinstance(state_data, dict):
					state_data = {}
				for k, v in legacy_state.items():
					old = state_data.get(k)
					if not isinstance(old, int) or v > old:
						state_data[k] = v
				store["state_data"] = state_data
			GroupMediaForwarder._legacy_merged = True

		if isinstance(GroupMediaForwarder._global_paras, dict):
			GroupMediaForwarder._global_paras["forwarder"] = store

	def _resolve_backup_target(self) -> tuple[int | str | None, int | None]:
		"""优先使用 global_paras['forwarder']，回退到构造参数。"""
		store = self._forwarder_store()
		if isinstance(store, dict) and store:
			chat_id = store.get("backup_chat_id", self.backup_chat_id)
			thread_id = store.get("backup_thread_id", self.backup_thread_id)
			return chat_id, (int(thread_id) if thread_id is not None else None)
		return self.backup_chat_id, self.backup_thread_id

	async def _acquire_client(self) -> tuple[TelegramClient, bool]:
		"""返回 (client, own_client)。own_client=True 代表需在调用方断开连接。"""
		if self.telegram_bot is not None:
			if not self.telegram_bot.is_connected():
				await self.telegram_bot.start()
			return self.telegram_bot, False

		client = _build_client()
		await client.start()
		return client, True

	def _resolve_forward_target(self) -> tuple[int | str, int | None]:
		"""
		解析默认 forward_to 目标。
		支持：
		- "bot_username" / @username / chat_id(int|str)
		- {"chat_id": ..., "thread_id": ...}
		- (chat_id, thread_id) / [chat_id, thread_id]
		返回：(chat_target, thread_id)
		"""
		target = self.forward_to

		if isinstance(target, dict):
			chat_id = target.get("chat_id")
			thread_id = target.get("thread_id")
			if chat_id is None:
				raise ValueError("forward_to 为 dict 时必须包含 chat_id")
			return chat_id, (int(thread_id) if thread_id is not None else None)

		if isinstance(target, (tuple, list)):
			if len(target) < 2:
				raise ValueError("forward_to 为 tuple/list 时必须为 (chat_id, thread_id)")
			return target[0], (int(target[1]) if target[1] is not None else None)

		return target, None

	# ── 工具方法 ─────────────────────────────────────────────

	@staticmethod
	def serialize_message(message) -> dict:
		return {
			"id": message.id,
			"date": message.date.isoformat() if message.date else None,
			"sender_id": getattr(message, "sender_id", None),
			"text": message.message or "",
		}

	def classify_text(self, text: str) -> str:
		for keyword in self.white_list_group_1:
			if keyword and keyword in text:
				return "group_1"
		for keyword in self.white_list_group_2:
			if keyword and keyword in text:
				return "group_2"
		return "group_3"

	def is_blacklisted(self, text: str) -> bool:
		return any(kw and kw in text for kw in self.black_list)

	def _match_route(self, text: str) -> dict | None:
		"""
		按 keyword_routes 顺序匹配第一条命中路由。
		路由格式：{"keywords": [...], "chat_id": int|str, "thread_id": int|None}
		"""
		for route in self.keyword_routes:
			for kw in route.get("keywords", []):
				if kw and kw in text:
					return route
		return None

	def _load_state_data_local_only(self) -> dict[str, int]:
		if not self.state_file.exists():
			return {}

		content = self.state_file.read_text(encoding="utf-8").strip()
		if not content:
			return {}

		# 向下兼容旧格式：文件只是一個數字
		if content.isdigit():
			return {str(self.target_group): int(content)}

		try:
			data = json.loads(content)
		except json.JSONDecodeError:
			return {}

		if not isinstance(data, dict):
			return {}

		state_data: dict[str, int] = {}
		for group_key, msg_id in data.items():
			if isinstance(group_key, str) and isinstance(msg_id, int):
				state_data[group_key] = msg_id

		return state_data

	def _load_state_data(self) -> dict[str, int]:
		store = self._forwarder_store()
		if isinstance(store, dict):
			by_group = store.get("last_message_id_by_group")
			if isinstance(by_group, dict):
				normalized_by_group: dict[str, int] = {}
				for group_key, msg_id in by_group.items():
					if isinstance(group_key, str) and isinstance(msg_id, int):
						normalized_by_group[group_key] = msg_id
				if normalized_by_group:
					return normalized_by_group

			state_data = store.get("state_data")
			if isinstance(state_data, dict):
				normalized: dict[str, int] = {}
				for group_key, msg_id in state_data.items():
					if isinstance(group_key, str) and isinstance(msg_id, int):
						normalized[group_key] = msg_id
				if normalized:
					return normalized

		local_state = self._load_state_data_local_only()
		if local_state and isinstance(store, dict):
			store["state_data"] = local_state
			if isinstance(GroupMediaForwarder._global_paras, dict):
				GroupMediaForwarder._global_paras["forwarder"] = store
		return local_state

	async def _load_state_data_from_backup(self, client: TelegramClient | None = None) -> dict[str, int]:
		backup_chat_id, backup_thread_id = self._resolve_backup_target()
		if backup_chat_id is None:
			return {}

		own_client = False
		work_client = client
		if work_client is None:
			work_client, own_client = await self._acquire_client()

		try:
			backup_entity = await self._resolve_entity_with_fallback(work_client, backup_chat_id)

			latest_msg = None
			if backup_thread_id is not None:
				try:
					async for msg in work_client.iter_messages(
						backup_entity,
						limit=1,
						reply_to=int(backup_thread_id),
					):
						latest_msg = msg
						break
				except TypeError:
					async for msg in work_client.iter_messages(backup_entity, limit=1):
						latest_msg = msg
						break
			else:
				async for msg in work_client.iter_messages(backup_entity, limit=1):
					latest_msg = msg
					break

			if latest_msg is None:
				return {}

			text = str(getattr(latest_msg, "message", "") or "").strip()
			if not text:
				return {}

			try:
				data = json.loads(text)
			except json.JSONDecodeError:
				return {}

			if not isinstance(data, dict):
				return {}

			state_data: dict[str, int] = {}
			for group_key, msg_id in data.items():
				if isinstance(group_key, str) and isinstance(msg_id, int):
					state_data[group_key] = msg_id

			return state_data
		except Exception:
			return {}
		finally:
			if own_client:
				await work_client.disconnect()

	async def _prepare_state_data(self, client: TelegramClient | None = None) -> dict[str, int]:
		# 业务启动前先准备状态：优先本地；本地缺失再从 backup 拉取并落地。
		local_state = self._load_state_data()
		if local_state:
			return local_state

		remote_state = await self._load_state_data_from_backup(client=client)
		if remote_state:
			self._write_state_data(remote_state)
		return remote_state

	def _write_state_data(self, data: dict[str, int]) -> None:
		self.state_file.write_text(
			json.dumps(data, ensure_ascii=False, indent=2),
			encoding="utf-8",
		)
		store = self._forwarder_store()
		if isinstance(store, dict):
			store["state_data"] = data
			store["last_message_id_by_group"] = data
			last_id = data.get(str(self.target_group))
			if isinstance(last_id, int):
				store["last_message_id"] = last_id
				store["last_target_group"] = str(self.target_group)
			if isinstance(GroupMediaForwarder._global_paras, dict):
				GroupMediaForwarder._global_paras["forwarder"] = store

	def resolve_start_message_id(self) -> int:
		state_data = self._load_state_data()
		last_id = state_data.get(str(self.target_group))
		if isinstance(last_id, int):
			return last_id
		return self.default_start_message_id

	async def write_last_message_id(
		self,
		message_id: int,
		*,
		client: TelegramClient | None = None,
	) -> None:
		state_data = self._load_state_data()
		state_data[str(self.target_group)] = message_id

		# 显式整合写入 global_paras['forwarder']，供主程序统一保存。
		store = self._forwarder_store()
		if isinstance(store, dict):
			by_group = store.get("last_message_id_by_group")
			if not isinstance(by_group, dict):
				by_group = {}
			by_group[str(self.target_group)] = int(message_id)
			store["last_message_id_by_group"] = by_group
			store["last_message_id"] = int(message_id)
			store["last_target_group"] = str(self.target_group)
			if isinstance(GroupMediaForwarder._global_paras, dict):
				GroupMediaForwarder._global_paras["forwarder"] = store

		self._write_state_data(state_data)
		state_json_text = json.dumps(state_data, ensure_ascii=False, indent=2)

		# 备份的是 state_data 形成的 JSON，而不是消息 caption。
		backup_chat_id, backup_thread_id = self._resolve_backup_target()
		if backup_chat_id is not None and client is not None:
			try:
				backup_entity = await self._resolve_entity_with_fallback(client, backup_chat_id)
				await self._send_text_chunks(
					client,
					backup_entity,
					state_json_text,
					reply_to=backup_thread_id,
				)
				print(f"[Backup] id={message_id} state_data JSON 已备份至 backup_chat_id={backup_chat_id}", flush=True)
			except Exception as exc:
				print(f"[Backup] id={message_id} 备份 state_data JSON 失败: {exc}", flush=True)

	@staticmethod
	def _extract_button_info(message) -> dict:
		"""解析按鈕資訊，並特別提取「复制链接」按鈕連結。"""
		buttons = []
		copy_link_targets = []

		rows = getattr(message, "buttons", None)
		if rows:
			for row in rows:
				for btn in row:
					text = getattr(btn, "text", "") or ""
					url = getattr(btn, "url", None)
					copy_text = getattr(btn, "copy_text", None)
					button_obj = getattr(btn, "button", None)
					if not url and button_obj is not None:
						url = getattr(button_obj, "url", None)
					if copy_text is None and button_obj is not None:
						copy_text = getattr(button_obj, "copy_text", None)

					# 某些封裝下 copy_text 可能是物件，實際值在 .text
					if copy_text is not None and not isinstance(copy_text, str):
						copy_text = getattr(copy_text, "text", None)

					buttons.append({
						"text": text,
						"url": url,
						"copy_text": copy_text,
					})

					if "复制链接" in text:
						target = copy_text or url
						if target:
							copy_link_targets.append(target)

		return {
			"buttons": buttons,
			"copy_link_targets": copy_link_targets,
		}

	@staticmethod
	def _extract_photo_info(message) -> dict:
		"""提取 photo 基本資訊。"""
		photo = getattr(message, "photo", None)
		if not photo:
			return {"has_photo": False}

		return {
			"has_photo": True,
			"photo_id": getattr(photo, "id", None),
		}

	def _format_caption(self, message, caption: str) -> str:
		"""根据配置决定是否将 caption 封装为 JSON。"""
		if not self.caption_json_mode:
			return caption

		media_type = "photo" if getattr(message, "photo", None) else (
			"video" if getattr(message, "video", None) else (
				"document" if getattr(message, "document", None) else "text"
			)
		)

		payload = {
			"caption": caption,
			"media_type": media_type,
			"message_id": getattr(message, "id", None),
			"sender_id": getattr(message, "sender_id", None),
			"date": message.date.isoformat() if getattr(message, "date", None) else None,
		}

		if media_type == "photo":
			payload["photo"] = self._extract_photo_info(message)
			payload["inline_buttons"] = self._extract_button_info(message)

		return json.dumps(payload, ensure_ascii=False)

	@staticmethod
	def _split_media_caption(caption: str, limit: int = 1024) -> tuple[str, str]:
		caption = str(caption or "")
		if len(caption) <= limit:
			return caption, ""
		return caption[:limit], caption[limit:]

	@staticmethod
	def _chunk_text(text: str, chunk_size: int = 4096) -> list[str]:
		text = str(text or "")
		if text == "":
			return []
		return [text[idx:idx + chunk_size] for idx in range(0, len(text), chunk_size)]

	async def _send_text_chunks(self, client: TelegramClient, forward_entity, text: str, reply_to: int | None = None) -> None:
		for chunk in self._chunk_text(text):
			send_kwargs = {"entity": forward_entity, "message": chunk}
			if reply_to is not None:
				send_kwargs["reply_to"] = reply_to
			await client.send_message(**send_kwargs)

	@staticmethod
	async def _resolve_entity_with_fallback(client: TelegramClient, chat_id: int | str):
		"""
		解析任意 chat_id 為 entity：
		1) 先用 get_entity() 直接解析
		2) 若失敗（不在 session cache），改從 iter_dialogs() 以 id 回退搜尋
		"""
		try:
			return await client.get_entity(int(chat_id))
		except Exception:
			pass

		target_abs = abs(int(chat_id))
		async for dialog in client.iter_dialogs():
			entity_id = getattr(dialog.entity, "id", None)
			if entity_id == target_abs:
				return dialog.entity

		raise ValueError(
			f"无法解析 chat_id={chat_id}，entity 不在 session cache 也不在 dialogs。"
			"请先让该账号加入该群组并发送一条消息。"
		)

	async def _resolve_source_entity(self, client: TelegramClient):
		"""
		解析來源實體：
		1) 先用 Telethon 直接解析（群組 id / username / chat id）
		2) 若是 user/bot 純數字 id 失敗，則從現有 dialogs 以 id 補找
		"""
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
				"若这是机器人 user_id，请先私聊该机器人一次，"
				"或改用 @username 作为 target_group。"
			) from exc

	async def _resend_message(
		self,
		client: TelegramClient,
		forward_entity,
		message,
		caption_override: str | None = None,
		reply_to: int | None = None,
		source_entity=None,
	) -> None:
		"""當來源聊天禁止轉傳時，改為下載並重新發送內容。reply_to 用於指定 thread_id（群组话题）。"""
		caption = caption_override if caption_override is not None else (message.message or "")

		if getattr(message, "media", None):
			media_caption, extra_text = self._split_media_caption(caption)
			send_kwargs = {
				"entity": forward_entity,
				"file": message.media,
				"caption": media_caption,
			}
			if reply_to is not None:
				send_kwargs["reply_to"] = reply_to

			# 盡量保留訊息型態；優先直接重用 Telegram 端媒體引用，避免大檔先下載到本地。
			if getattr(message, "video", None):
				send_kwargs["supports_streaming"] = True
			if getattr(message, "voice", None):
				send_kwargs["voice_note"] = True
			if getattr(message, "video_note", None):
				send_kwargs["video_note"] = True

			try:
				await client.send_file(**send_kwargs)
				if extra_text:
					await self._send_text_chunks(client, forward_entity, extra_text, reply_to=reply_to)
				return
			except Exception as exc:
				if not self.download_fallback_enabled:
					print(f"[Resend] id={getattr(message, 'id', None)} 直接重送媒體失敗，且已停用下載重傳 | error={exc}", flush=True)
					raise
				print(f"[Resend] id={getattr(message, 'id', None)} 直接重送媒體失敗，改用下載重傳 | error={exc}", flush=True)

			with tempfile.TemporaryDirectory(prefix="man_media_") as tmp_dir:
				try:
					downloaded_path = await client.download_media(message, file=tmp_dir)
				except FileReferenceExpiredError:
					# file reference 过期时，先按同一来源+message.id 重新拉取消息，再重试下载。
					if source_entity is None or getattr(message, "id", None) is None:
						raise
					refreshed_msg = await client.get_messages(source_entity, ids=message.id)
					if not refreshed_msg or not getattr(refreshed_msg, "media", None):
						raise
					downloaded_path = await client.download_media(refreshed_msg, file=tmp_dir)
				if downloaded_path:
					send_kwargs["file"] = downloaded_path
					await client.send_file(**send_kwargs)
					if extra_text:
						await self._send_text_chunks(client, forward_entity, extra_text, reply_to=reply_to)
					return

		if caption:
			await self._send_text_chunks(client, forward_entity, caption, reply_to=reply_to)
	# ── 核心异步方法 ──────────────────────────────────────────

	async def fetch_messages(self, start_message_id: int, limit: int) -> list[dict]:
		client, own_client = await self._acquire_client()
		me = await client.get_me()
		print(f"已登入 Telegram 帳號：{me.username} (id={me.id})",flush=True)


		try:
			entity = await self._resolve_source_entity(client)
			messages = []
			async for message in client.iter_messages(
				entity,
				min_id=start_message_id - 1,
				reverse=True,
				limit=limit,
			):
				messages.append(self.serialize_message(message))
			return messages
		finally:
			if own_client:
				await client.disconnect()

	async def fetch_and_forward(
		self,
		start_message_id: int,
		*,
		max_messages: int | None = None,
		respect_sleep: bool = True,
	) -> int:
		client, own_client = await self._acquire_client()
		me = await client.get_me()
		print(f"SESSION_READY 已登入 Telegram 帳號：{me.username} (id={me.id})", flush=True)
		
		try:
			source_entity = await self._resolve_source_entity(client)
			forward_target, forward_thread_id = self._resolve_forward_target()
			forward_entity = await client.get_entity(forward_target)
			last_message_id = start_message_id - 1
			iter_kwargs = {
				"min_id": start_message_id - 1,
				"reverse": True,
			}
			if isinstance(max_messages, int) and max_messages > 0:
				iter_kwargs["limit"] = int(max_messages)

			async for message in client.iter_messages(
				source_entity,
				**iter_kwargs,
			):
				last_message_id = message.id
				text = self.serialize_message(message).get("text", "")
				preview_text = (text or "").replace("\n", " ").strip()
				if len(preview_text) > 60:
					preview_text = preview_text[:60] + "..."
				print(
					f"[Msg] id={message.id} 開始處理 text={preview_text!r}",
					flush=True,
				)
				if not getattr(message, "media", None):
					
					continue

				# ── keyword_routes 优先分流 ──────────────────────────
				route = self._match_route(text)
				if route:
					route_entity = await client.get_entity(route["chat_id"])
					thread_id: int | None = route.get("thread_id")
					formatted_caption = self._format_caption(message, text)
					print(f"[Route] id={message.id} → chat_id={route['chat_id']} thread_id={thread_id}", flush=True)
					if self.caption_json_mode:
						await self._resend_message(
							client,
							route_entity,
							message,
							caption_override=formatted_caption,
							reply_to=thread_id,
							source_entity=source_entity,
						)
					else:
						try:
							if thread_id is not None:
								# 转发到话题 thread 需要 resend 方式才能指定 reply_to
								await self._resend_message(client, route_entity, message, reply_to=thread_id, source_entity=source_entity)
							else:
								await client.forward_messages(
									entity=route_entity,
									messages=[message.id],
									from_peer=source_entity,
								)
						except ChatForwardsRestrictedError:
							await self._resend_message(client, route_entity, message, reply_to=thread_id, source_entity=source_entity)

					

					if respect_sleep and self.sleep_enabled:
						sleep_seconds = random.randint(
							min(self.sleep_min_seconds, self.sleep_max_seconds),
							max(self.sleep_min_seconds, self.sleep_max_seconds),
						)
						print(f"[Sleep] id={message.id} 休眠 {sleep_seconds} 秒", flush=True)
						await asyncio.sleep(sleep_seconds)

					continue  # 路由命中后跳过白名单/默认转发逻辑

				# ── 默认白名单转发逻辑 ──────────────────────────────
				if self.skip_caption_check:
					should_forward = True
				else:
					if self.is_blacklisted(text):
						print(f"[Skip] id={message.id} 命中黑名单", flush=True)
						continue
					should_forward = self.classify_text(text) in {"group_1", "group_2"}
					if not should_forward:
						print(f"[Skip] id={message.id} 不在白名单分组", flush=True)

				if should_forward:
					formatted_caption = self._format_caption(message, text)
					print(f"[Forward] id={message.id} 準備轉發", flush=True)
					try:
						if self.caption_json_mode:
							await self._resend_message(
								client,
								forward_entity,
								message,
								caption_override=formatted_caption,
								reply_to=forward_thread_id,
								source_entity=source_entity,
							)
						else:
							try:
								if forward_thread_id is not None:
									await self._resend_message(client, forward_entity, message, reply_to=forward_thread_id, source_entity=source_entity)
								else:
									await client.forward_messages(
										entity=forward_entity,
										messages=[message.id],
										from_peer=source_entity,
									)
							except ChatForwardsRestrictedError:
								await self._resend_message(
									client,
									forward_entity,
									message,
									caption_override=formatted_caption,
									reply_to=forward_thread_id,
									source_entity=source_entity,
								)
								# 随机休眠
								print(f"[Forward] id={message.id} 转发遇到 ChatForwardsRestrictedError，已使用重发方式转发成功,随机休眠", flush=True)
								await asyncio.sleep(random.randint(17, 132))
								
					except Exception as exc:
						print(f"[Forward] id={message.id} 轉發失敗 | error={exc}", flush=True)

					if respect_sleep and self.sleep_enabled:
						sleep_min_seconds = min(self.sleep_min_seconds, self.sleep_max_seconds)
						sleep_max_seconds = max(self.sleep_min_seconds, self.sleep_max_seconds)
						sleep_seconds = random.randint(sleep_min_seconds, sleep_max_seconds)
						print(f"[Sleep] id={message.id} 休眠 {sleep_seconds} 秒", flush=True)
						await asyncio.sleep(sleep_seconds)
					else:
						print(f"[Sleep] id={message.id} 已关闭休眠", flush=True)

				# 不论是否转发，已检查过的消息都推进游标，避免重复检查旧消息
				await self.write_last_message_id(
					message.id,
					client=client,
				)
				print(f"[State] 已寫入 last_message_id={message.id}", flush=True)

			return last_message_id
		finally:
			if own_client:
				await client.disconnect()

	async def wait_for_new_message(self, last_seen_message_id: int, poll_interval_sec: int = 5) -> int:
		"""阻塞等待，直到 target_group 出现比 last_seen_message_id 更新的消息。"""
		client, own_client = await self._acquire_client()
		try:
			source_entity = await self._resolve_source_entity(client)
			while True:
				latest_id = None
				async for latest_msg in client.iter_messages(source_entity, limit=1):
					latest_id = latest_msg.id
					break

				if latest_id is not None and latest_id > last_seen_message_id:
					return latest_id

				await asyncio.sleep(poll_interval_sec)
		finally:
			if own_client:
				await client.disconnect()

	async def has_messages_in_target_group(self) -> bool:
		"""检查来源 target_group 是否至少有一条消息。"""
		client, own_client = await self._acquire_client()
		try:
			source_entity = await self._resolve_source_entity(client)
			async for _ in client.iter_messages(source_entity, limit=1):
				return True
			return False
		finally:
			if own_client:
				await client.disconnect()

	async def run(self) -> None:
		await self._prepare_state_data()
		next_start_id = self.resolve_start_message_id()
		print(f"[Run] 从 start_message_id={next_start_id} 开始检查。", flush=True)

		while True:
			last_checked_id = await self.fetch_and_forward(next_start_id)
			print(f"[Done] 已检查到 message_id={last_checked_id}。进入等待新消息...", flush=True)

			last_seen = max(last_checked_id, next_start_id - 1)
			new_latest_id = await self.wait_for_new_message(last_seen)
			next_start_id = last_seen + 1
			print(f"[Wake] 检测到新消息 latest_id={new_latest_id}，从 message_id={next_start_id} 继续检查。", flush=True)
