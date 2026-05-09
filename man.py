import asyncio
import json
import os
import random
import tempfile
from contextlib import suppress
from pathlib import Path
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import ChatForwardsRestrictedError, FloodWaitError, FileReferenceExpiredError
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights, InputPeerUser

from man_config import API_HASH, API_ID, SESSION_STRING, FORWARDER_RUN_TARGET


def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID)

	# StringSession 通常是较长 token，不应被当作 sqlite 文件路径
	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


async def _handle_healthcheck(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
	try:
		await reader.read(1024)
		body = b"ok"
		response = (
			b"HTTP/1.1 200 OK\r\n"
			b"Content-Type: text/plain; charset=utf-8\r\n"
			+ f"Content-Length: {len(body)}\r\n".encode("ascii")
			+ b"Connection: close\r\n\r\n"
			+ body
		)
		writer.write(response)
		await writer.drain()
	finally:
		writer.close()
		with suppress(Exception):
			await writer.wait_closed()


async def run_health_server() -> None:
	host = os.getenv("HOST", "0.0.0.0")
	port = int(os.getenv("PORT", "10000"))
	server = await asyncio.start_server(_handle_healthcheck, host, port)
	print(f"HEALTHCHECK server listening on {host}:{port}", flush=True)
	async with server:
		await server.serve_forever()

async def run_all_bot():
	for target in BOT_SCRIPTS:
		try:
			await run_bot_script(target)
		except Exception as e:
			print(f"[run_all_bot] {target} 执行失败: {e}", flush=True)


async def main() -> None:
	# await run_all_bot()
	# await monitor_bot("@dkeiwfBot")
	# await run_bot_script("@dkeiwfBot")
	# exit()

	forwarder_registry = {
		"1": ("forwarder_dy", forwarder_dy),
		"forwarder_dy": ("forwarder_dy", forwarder_dy),
		"2": ("forwarder_th", forwarder_th),
		"forwarder_th": ("forwarder_th", forwarder_th),
		"3": ("forwarder_move", forwarder_move),
		"forwarder_move": ("forwarder_move", forwarder_move),
		"4": ("forwarder_move2", forwarder_move2),
		"forwarder_move2": ("forwarder_move2", forwarder_move2),
	}

	# FORWARDER_RUN_TARGET = "forwarder_move2"

	selected_name, selected_forwarder = forwarder_registry.get(
		FORWARDER_RUN_TARGET,
		("forwarder_th", forwarder_th),
	)

	print(f"[Boot] selected forwarder: {selected_name}", flush=True)
	await asyncio.gather(
		selected_forwarder.run(),
		# forwarder_move.run(),
		run_health_server(),
	)


	

    # 2) 拉取群成员 id + username
	# inspector = TargetGroupInspector(target_group=-1001800096525)
	# members = await inspector.list_members()
	# print("members:", len(members))
	# print("first member:", members[0] if members else None)
	# await inspector.set_send_only_permissions_for_roles(members)


class TargetGroupInspector:
	"""抓取指定 Telegram 群组消息，并收集群成员 id/username。"""

	def __init__(self, target_group: int | str) -> None:
		self.target_group = target_group
		self.members: list[dict[str, int | str | None]] = []

	@staticmethod
	def _serialize_message(message) -> dict:
		return {
			"id": message.id,
			"date": message.date.isoformat() if message.date else None,
			"sender_id": getattr(message, "sender_id", None),
			"text": message.message or "",
		}

	@staticmethod
	def _extract_member_role(user) -> str:
		"""根据 participant 类型提取成员角色。"""
		participant = getattr(user, "participant", None)
		if participant is None:
			return "member"

		role_map = {
			"ChannelParticipantCreator": "creator",
			"ChatParticipantCreator": "creator",
			"ChannelParticipantAdmin": "admin",
			"ChatParticipantAdmin": "admin",
			"ChannelParticipantBanned": "restricted",
			"ChannelParticipantLeft": "left",
		}

		role = role_map.get(participant.__class__.__name__)
		if role:
			return role

		if getattr(participant, "admin_rights", None):
			return "admin"
		if getattr(participant, "banned_rights", None):
			return "restricted"
		if getattr(participant, "left", False):
			return "left"

		return "member"

	@staticmethod
	def _send_only_banned_rights() -> ChatBannedRights:
		"""
		仅允许发文字消息（等价 Bot API: can_send_messages=true，其他权限 false）。
		官方 ChatPermissions 字段对应：
		- can_send_messages = true
		- can_send_audios/can_send_documents/can_send_photos/can_send_videos/
		  can_send_video_notes/can_send_voice_notes/can_send_polls/
		  can_send_other_messages/can_add_web_page_previews/
		  can_change_info/can_invite_users/can_pin_messages/can_manage_topics = false
		在 Telethon 的 ChatBannedRights 里，True 表示“禁止该权限”。
		"""
		return ChatBannedRights(
			until_date=None,
			view_messages=False,
			send_messages=False,
			send_plain=False,
			send_media=True,
			send_stickers=True,
			send_gifs=True,
			send_games=True,
			send_inline=True,
			embed_links=True,
			send_polls=True,
			send_photos=True,
			send_videos=True,
			send_roundvideos=True,
			send_audios=True,
			send_voices=True,
			send_docs=True,
			change_info=True,
			invite_users=True,
			pin_messages=True,
			manage_topics=True,
		)

	@staticmethod
	def _parse_user_input(user: int | str | dict) -> tuple[int | None, str, int | None]:
		"""支持传入 user_id 或 list_members() 产出的 user dict。返回 (user_id, role, access_hash)。"""
		if isinstance(user, dict):
			user_id = user.get("id")
			role = str(user.get("role") or "").lower()
			access_hash = user.get("access_hash")
			access_hash = int(access_hash) if isinstance(access_hash, int) else None
			return (int(user_id), role, access_hash) if isinstance(user_id, int) else (None, role, None)

		if isinstance(user, int):
			return user, "member", None

		if isinstance(user, str) and user.lstrip("-").isdigit():
			return int(user), "member", None

		return None, "", None

	async def set_send_only_permissions(self, user: int | str | dict) -> dict:
		"""
		当 user 角色为 restricted/left/member 时，设置为“仅可发送消息”。
		返回结构：{"ok": bool, "status": str, ...}
		"""
		user_id, role, access_hash = self._parse_user_input(user)
		if user_id is None:
			return {"ok": False, "status": "bad_user"}

		if role not in {"restricted", "left", "member"}:
			return {"ok": False, "status": "role_skipped", "user_id": user_id, "role": role}

		client = _build_client()
		await client.start()
		try:
			source_entity = await self._resolve_source_entity(client)

			# 优先用 list_members() 保存的 access_hash 直接构造，避免 Telethon 缓存查找失败
			if access_hash is not None:
				participant_entity = InputPeerUser(user_id, access_hash)
			else:
				participant_entity = await client.get_input_entity(user_id)

			rights = self._send_only_banned_rights()
			await client(
				EditBannedRequest(
					channel=source_entity,
					participant=participant_entity,
					banned_rights=rights,
				)
			)

			return {
				"ok": True,
				"status": "updated",
				"user_id": user_id,
				"role": role,
			}
		except FloodWaitError as exc:
			return {
				"ok": False,
				"status": "flood_wait",
				"user_id": user_id,
				"role": role,
				"flood_wait_seconds": exc.seconds,
				"error": str(exc),
			}
		except Exception as exc:
			return {
				"ok": False,
				"status": "error",
				"user_id": user_id,
				"role": role,
				"error": str(exc),
			}
		finally:
			await client.disconnect()

	@staticmethod
	def _load_processed_ids(state_file: Path) -> set[int]:
		"""从本地文件读取已处理的 user_id 集合。"""
		if not state_file.exists():
			return set()
		try:
			data = json.loads(state_file.read_text(encoding="utf-8"))
			return {int(x) for x in data if str(x).lstrip("-").isdigit()}
		except (json.JSONDecodeError, ValueError):
			return set()

	@staticmethod
	def _save_processed_id(state_file: Path, processed_ids: set[int]) -> None:
		"""将已处理的 user_id 集合写回本地文件。"""
		state_file.write_text(
			json.dumps(sorted(processed_ids), ensure_ascii=False, indent=2),
			encoding="utf-8",
		)

	async def set_send_only_permissions_for_roles(
		self,
		users: list[dict[str, int | str | None]],
		sleep_seconds: float = 1.1,
		state_file: Path | None = None,
	) -> list[dict]:
		"""
		批量处理：对 restricted/left/member 成员设置仅可发送消息。
		- 每次操作固定休眠 sleep_seconds 秒（默认 1.1 秒，Telegram 官方建议同群每秒不超过 1 次写操作）。
		- 若服务器返回 FLOOD_WAIT_X（420），自动等待 X+1 秒后重试一次。
		- 已成功处理的 user_id 写入 state_file（默认 set_permissions_<group_id>.json），下次运行自动跳过。
		"""
		if state_file is None:
			state_file = Path(__file__).with_name(f"set_permissions_{self.target_group}.json")

		processed_ids = self._load_processed_ids(state_file)
		print(f"[State] 已读取进度文件：{state_file}，已处理 {len(processed_ids)} 人。", flush=True)

		results: list[dict] = []
		total = len(users)
		for idx, user in enumerate(users, start=1):
			user_id, _, _ah = self._parse_user_input(user)
			pct = idx / total * 100

			if user_id is not None and user_id in processed_ids:
				print(f"[Skip] {idx}/{total} ({pct:.1f}%) user_id={user_id} 已处理，跳过。", flush=True)
				results.append({"ok": True, "status": "already_done", "user_id": user_id})
				continue

			result = await self.set_send_only_permissions(user)
			if result.get("status") == "flood_wait":
				wait = result.get("flood_wait_seconds", 30) + 1
				print(f"[FloodWait] user_id={result.get('user_id')} 等待 {wait} 秒后重试...", flush=True)
				await asyncio.sleep(wait)
				result = await self.set_send_only_permissions(user)

			if result.get("ok") and user_id is not None:
				processed_ids.add(user_id)
				self._save_processed_id(state_file, processed_ids)

			results.append(result)
			status = result.get("status")
			log_line = f"[Progress] {idx}/{total} ({pct:.1f}%) user_id={result.get('user_id')} status={status}"
			if status == "error":
				log_line += f" error={result.get('error')}"
			print(log_line, flush=True)
			await asyncio.sleep(sleep_seconds)

		print(f"[Progress] 完成，共处理 {total} 人（其中 {len(processed_ids)} 人已记录）。", flush=True)
		return results

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

	async def fetch_messages(self, limit: int = 100, start_message_id: int = 1) -> list[dict]:
		"""从 target_group 抓取消息。"""
		client = _build_client()
		await client.start()
		try:
			source_entity = await self._resolve_source_entity(client)
			messages: list[dict] = []
			async for message in client.iter_messages(
				source_entity,
				min_id=max(0, start_message_id - 1),
				reverse=True,
				limit=max(1, int(limit)),
			):
				messages.append(self._serialize_message(message))
			return messages
		finally:
			await client.disconnect()

	async def list_members(self) -> list[dict[str, int | str | None]]:
		"""列出 target_group 所有成员，并将 id/username/role 保存到 self.members。"""
		client = _build_client()
		await client.start()
		try:
			source_entity = await self._resolve_source_entity(client)
			members: list[dict[str, int | str | None]] = []
			async for user in client.iter_participants(source_entity):
				members.append(
					{
						"id": getattr(user, "id", None),
						"username": getattr(user, "username", None),
						"role": self._extract_member_role(user),
						"access_hash": getattr(user, "access_hash", None),
					}
				)
			self.members = members
			return members
		finally:
			await client.disconnect()


class GroupMediaForwarder:
	"""从指定群组抓取媒体消息并转发到目标。"""

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

	def _load_state_data(self) -> dict[str, int]:
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

	async def _load_state_data_from_backup(self, client: TelegramClient | None = None) -> dict[str, int]:
		if self.backup_chat_id is None:
			return {}

		own_client = False
		work_client = client
		if work_client is None:
			work_client = _build_client()
			await work_client.start()
			own_client = True

		try:
			# 确保 backup_chat_id 是 int 型，处理负数情况
			backup_chat_id_int = int(self.backup_chat_id)
			backup_entity = await work_client.get_entity(backup_chat_id_int)

			latest_msg = None
			if self.backup_thread_id is not None:
				try:
					async for msg in work_client.iter_messages(
						backup_entity,
						limit=1,
						reply_to=int(self.backup_thread_id),
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
		self._write_state_data(state_data)
		state_json_text = json.dumps(state_data, ensure_ascii=False, indent=2)

		# 备份的是 state_data 形成的 JSON，而不是消息 caption。
		if (
			self.backup_chat_id is not None
			and client is not None
		):
			try:
				# 确保 backup_chat_id 是 int 型，处理负数情况
				backup_chat_id_int = int(self.backup_chat_id)
				backup_entity = await client.get_entity(backup_chat_id_int)
				await self._send_text_chunks(
					client,
					backup_entity,
					state_json_text,
					reply_to=self.backup_thread_id,
				)
				print(f"[Backup] id={message_id} state_data JSON 已备份至 backup_chat_id={self.backup_chat_id}", flush=True)
			except ValueError as ve:
				print(f"[Backup] id={message_id} backup_chat_id 转换失败: {ve}", flush=True)
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
		client = _build_client()
		await client.start()
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
			await client.disconnect()

	async def fetch_and_forward(self, start_message_id: int) -> int:
		client = _build_client()
		await client.start()
		me = await client.get_me()
		print(f"SESSION_READY 已登入 Telegram 帳號：{me.username} (id={me.id})", flush=True)
		
		try:
			source_entity = await self._resolve_source_entity(client)
			forward_target, forward_thread_id = self._resolve_forward_target()
			forward_entity = await client.get_entity(forward_target)
			last_message_id = start_message_id - 1

			async for message in client.iter_messages(
				source_entity,
				min_id=start_message_id - 1,
				reverse=True,
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
					print(f"[Skip] id={message.id} 非媒體消息", flush=True)
					await self.write_last_message_id(message.id)
					print(f"[State] 已寫入 last_message_id={message.id}", flush=True)
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

					await self.write_last_message_id(
						message.id,
						client=client,
					)
					print(f"[State] 已寫入 last_message_id={message.id}", flush=True)

					if self.sleep_enabled:
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
				except Exception as exc:
					print(f"[Forward] id={message.id} 轉發失敗 | error={exc}", flush=True)
				
				if self.sleep_enabled:
					sleep_min_seconds = min(self.sleep_min_seconds, self.sleep_max_seconds)
					sleep_max_seconds = max(self.sleep_min_seconds, self.sleep_max_seconds)
					sleep_seconds = random.randint(sleep_min_seconds, sleep_max_seconds)
				print(f"[Sleep] id={message.id} 休眠 {sleep_seconds} 秒", flush=True)
				await asyncio.sleep(sleep_seconds)
			else:
				print(f"[Sleep] id={message.id} 已关闭休眠", flush=True)

			return last_message_id
		finally:
			await client.disconnect()

	async def wait_for_new_message(self, last_seen_message_id: int, poll_interval_sec: int = 5) -> int:
		"""阻塞等待，直到 target_group 出现比 last_seen_message_id 更新的消息。"""
		client = _build_client()
		await client.start()
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
			await client.disconnect()

	async def has_messages_in_target_group(self) -> bool:
		"""检查来源 target_group 是否至少有一条消息。"""
		client = _build_client()
		await client.start()
		try:
			source_entity = await self._resolve_source_entity(client)
			async for _ in client.iter_messages(source_entity, limit=1):
				return True
			return False
		finally:
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


# ════════════════════════════════════════════════════════════════
#  BotSession — 通用对话原语（send / wait_reply / click）
# ════════════════════════════════════════════════════════════════

class BotSession:
	"""
	管理与单个 bot/群组的对话 session。
	建议使用 async with 语句，保证 client 自动断开。

	用法：
	    async with BotSession("@XXHL9Bot") as s:
	        sent = await s.send("📅 每日签到")
	        msg  = await s.wait_reply(timeout=30)
	        if msg:
	            await s.click(msg, b"do_checkin")
	"""

	def __init__(self, target: int | str) -> None:
		self.target = target
		self._client: TelegramClient | None = None
		self._entity = None

	async def __aenter__(self) -> "BotSession":
		self._client = _build_client()
		await self._client.start()
		self._entity = await self._client.get_entity(self.target)
		return self

	async def __aexit__(self, *_) -> None:
		if self._client:
			await self._client.disconnect()
			self._client = None

	# ── 原语方法 ─────────────────────────────────────────────

	async def send(self, text: str):
		"""发送文字消息，返回已发送的 Message 对象。"""
		sent = await self._client.send_message(entity=self._entity, message=text)
		print(f"[BotSession] 已发送 → {self.target} | text={text!r} | message_id={sent.id}", flush=True)
		return sent

	async def wait_reply(self, timeout: float = 30.0):
		"""
		等待对方下一条消息，返回 Message 对象；超时返回 None。
		同时打印消息文字与按钮列表。
		"""
		reply_event: asyncio.Event = asyncio.Event()
		received: list = []

		async def _handler(event) -> None:
			received.append(event.message)
			reply_event.set()

		self._client.add_event_handler(
			_handler,
			events.NewMessage(from_users=self._entity.id, incoming=True),
		)
		try:
			await asyncio.wait_for(reply_event.wait(), timeout=timeout)
		except asyncio.TimeoutError:
			print(f"[BotSession] 等待回复超时（{timeout}s）← {self.target}", flush=True)
			return None
		finally:
			self._client.remove_event_handler(_handler)

		msg = received[0]
		text = getattr(msg, "message", None) or ""
		print(f"[BotSession] 收到回复 ← {self.target} | message_id={msg.id} | text={text!r}", flush=True)

		buttons = _parse_buttons(msg)
		if buttons:
			print(f"[BotSession] 回复按钮 ← {self.target} | buttons={buttons}", flush=True)
		else:
			print(f"[BotSession] 回复无按钮", flush=True)

		return msg

	async def click(self, msg, data: bytes) -> None:
		"""点击消息中指定 callback_data 的按钮。"""
		data_str = data.decode("utf-8", errors="replace")
		try:
			result = await msg.click(data=data)
			print(f"[BotSession] 已点击 data={data_str!r} | result={result}", flush=True)
		except Exception as exc:
			print(f"[BotSession] 点击 data={data_str!r} 失败 | error={exc}", flush=True)

	async def click_by_text(self, msg, text: str) -> None:
		"""按按钮显示文字点击，适用于 data 含动态字段的情况。"""
		try:
			result = await msg.click(text=text)
			print(f"[BotSession] 已点击 text={text!r} | result={result}", flush=True)
		except Exception as exc:
			print(f"[BotSession] 点击 text={text!r} 失败 | error={exc}", flush=True)

	def prepare_wait_edit(self) -> "EditWaiter":
		"""
		先注册 edit 监听器，再去 click，避免竞态条件（bot edit 速度快于注册）。
		用法：
		    waiter = s.prepare_wait_edit()   # 先注册
		    await s.click_by_text(msg, "xxx")  # 再 click
		    msg2 = await waiter.wait(timeout=15)  # 再等待
		"""
		return EditWaiter(self._client, self._entity.id, self.target)

	async def wait_edit(self, timeout: float = 15.0):
		"""
		等待对方编辑（edit）任意一条消息，返回更新后的 Message 对象；超时返回 None。
		注意：若 bot edit 速度极快，请改用 prepare_wait_edit() 避免竞态。
		"""
		return await self.prepare_wait_edit().wait(timeout=timeout)


class EditWaiter:
	"""
	预注册 MessageEdited 监听器，解决 click 后 bot 立即 edit 的竞态问题。
	通过 BotSession.prepare_wait_edit() 创建。
	"""

	def __init__(self, client: TelegramClient, peer_id: int, target: str) -> None:
		self._client = client
		self._target = target
		self._event: asyncio.Event = asyncio.Event()
		self._received: list = []

		async def _handler(event) -> None:
			self._received.append(event.message)
			self._event.set()

		self._handler = _handler
		client.add_event_handler(
			_handler,
			events.MessageEdited(from_users=peer_id, incoming=True),
		)

	async def wait(self, timeout: float = 15.0):
		try:
			await asyncio.wait_for(self._event.wait(), timeout=timeout)
		except asyncio.TimeoutError:
			print(f"[BotSession] 等待 edit 超时（{timeout}s）← {self._target}", flush=True)
			return None
		finally:
			self._client.remove_event_handler(self._handler)

		msg = self._received[0]
		text = getattr(msg, "message", None) or ""
		print(f"[BotSession] 收到 edit ← {self._target} | message_id={msg.id} | text={text!r}", flush=True)
		buttons = _parse_buttons(msg)
		if buttons:
			print(f"[BotSession] edit 按钮 ← {self._target} | buttons={buttons}", flush=True)
		return msg


def _parse_buttons(msg) -> list[list[dict]]:
	"""解析消息按钮，返回二维列表 [row][btn] = {text, data, url}。"""
	rows = getattr(msg, "buttons", None)
	if not rows:
		return []
	result: list[list[dict]] = []
	for row in rows:
		row_btns: list[dict] = []
		for btn in row:
			btn_text = getattr(btn, "text", "") or ""
			btn_data = getattr(btn, "data", None)
			btn_url = getattr(btn, "url", None)
			btn_obj = getattr(btn, "button", None)
			if btn_obj is not None:
				if btn_data is None:
					btn_data = getattr(btn_obj, "data", None)
				if btn_url is None:
					btn_url = getattr(btn_obj, "url", None)
			if isinstance(btn_data, bytes):
				btn_data = btn_data.decode("utf-8", errors="replace")
			row_btns.append({"text": btn_text, "data": btn_data, "url": btn_url})
		if row_btns:
			result.append(row_btns)
	return result


# ════════════════════════════════════════════════════════════════
#  各 Bot 独立脚本
# ════════════════════════════════════════════════════════════════

class BotScripts:
	"""集中管理所有 script_ 机器人脚本。"""

	@staticmethod
	async def _send_only(target: str, text: str, timeout: float = 30.0) -> None:
		async with BotSession(target) as s:
			await s.send(text)
			await s.wait_reply(timeout=timeout)

	@staticmethod
	async def _find_message_with_button(session: BotSession, text: str, timeout: float = 25.0, poll: float = 1.2):
		"""轮询最近消息，找到含指定按钮文字的消息。"""
		if session._client is None:
			return None
		deadline = asyncio.get_running_loop().time() + timeout
		while asyncio.get_running_loop().time() < deadline:
			async for recent in session._client.iter_messages(session._entity, limit=12):
				rows = _parse_buttons(recent)
				if not rows:
					continue
				for row in rows:
					for btn in row:
						if btn.get("text") == text:
							print(f"[ccccc000_bot] 找到按钮 {text!r} | message_id={recent.id}", flush=True)
							return recent
			await asyncio.sleep(poll)
		print(f"[ccccc000_bot] 未找到按钮 {text!r}（{timeout}s）", flush=True)
		return None

	@staticmethod
	async def script_xxhl9bot() -> None:
		"""@XXHL9Bot — 每日签到流程"""
		async with BotSession("@XXHL9Bot") as s:
			await s.send("📅 每日签到")
			msg = await s.wait_reply(timeout=30)
			if not msg:
				return
			await s.click(msg, b"do_checkin")
			await s.wait_reply(timeout=15)

	@staticmethod
	async def script_aiyynvshen_bot() -> None:
		await BotScripts._send_only("@AiYYnvshen_bot", "⭐ 今日签到")

	@staticmethod
	async def script_ainudem2bot() -> None:
		await BotScripts._send_only("@ainudem2bot", "签到")

	@staticmethod
	async def script_AIVision1111_bot_bot() -> None:
		await BotScripts._send_only("@AIVision1111_bot", "📅 每日签到")

	@staticmethod
	async def script_huuy2024_bot() -> None:
		await BotScripts._send_only("@HuuY2024_bot", "📆 每日签到")

	@staticmethod
	async def script_quyi44bot() -> None:
		await BotScripts._send_only("@quyi44bot", "🌍 每日签到")

	@staticmethod
	async def script_tuoyi55bot() -> None:
		await BotScripts._send_only("@tuoyi55bot", "🌍 每日签到")

	@staticmethod
	async def script_menjjbot() -> None:
		await BotScripts._send_only("@menjjbot", "🌍 每日签到")

	@staticmethod
	async def script_tuoyi03bot() -> None:
		await BotScripts._send_only("@tuoyi03bot", "🌍 每日签到")

	@staticmethod
	async def script_quyi198bot() -> None:
		await BotScripts._send_only("@quyi198bot", "🌍 每日签到")

	@staticmethod
	async def script_linglongai_2bot() -> None:
		await BotScripts._send_only("@linglongai_2bot", "📅 签到")

	@staticmethod
	async def script_the1_visionarybot() -> None:
		await BotScripts._send_only("@the1_visionarybot", "🎰 每日抽奖")

	@staticmethod
	async def script_jsai1bot() -> None:
		await BotScripts._send_only("@JSai1bot", "🌍 每日签到")

	@staticmethod
	async def script_srikitibot() -> None:
		await BotScripts._send_only("@SrikitiBot", "🌍 每日签到")

	@staticmethod
	async def script_mengokbot() -> None:
		await BotScripts._send_only("@mengokbot", "🌍 每日签到")

	@staticmethod
	async def script_tangest4_bot() -> None:
		"""@tangest4_bot — 设 bio 后签到"""
		from telethon.tl.functions.account import UpdateProfileRequest
		client = _build_client()
		await client.start()
		try:
			await client(UpdateProfileRequest(about="https://t.me/tangest4_bot?start=ref_7501358629"))
			print("[tangest4_bot] bio 已设置", flush=True)
			await asyncio.sleep(5)
			entity = await client.get_entity("@tangest4_bot")
			sent = await client.send_message(entity=entity, message="📅 签到")
			print(f"[tangest4_bot] 已发送签到 | message_id={sent.id}", flush=True)
		finally:
			await client.disconnect()

	@staticmethod
	async def script_ccccc000_bot() -> None:
		"""@ccccc000_bot — 浏览作品/点赞/签到流程"""
		from telethon.tl.functions.account import UpdateProfileRequest

		async with BotSession("@ccccc000_bot") as s:
			await s.send("/start")
			menu = await s.wait_reply(timeout=30)
			if not menu:
				return

			await s.click_by_text(menu, "🏆 每日排行榜")
			rank_msg = await s.wait_reply(timeout=30)
			if not rank_msg:
				await s.send("🏆 每日排行榜")
				rank_msg = await s.wait_reply(timeout=30)
			if not rank_msg:
				rank_msg = menu

			await s.click_by_text(rank_msg, "🖼️ 浏览作品")
			browse_msg = await BotScripts._find_message_with_button(s, "❤️ 点赞", timeout=25)
			if not browse_msg:
				await s.send("🖼️ 浏览作品")
				browse_msg = await BotScripts._find_message_with_button(s, "❤️ 点赞", timeout=25)
			if not browse_msg:
				browse_msg = rank_msg

			waiter3 = s.prepare_wait_edit()
			await s.click_by_text(browse_msg, "❤️ 点赞")
			liked_msg = await waiter3.wait(timeout=8)
			if not liked_msg:
				liked_msg = await s.wait_reply(timeout=10)

			if s._client is None:
				return
			await s._client(UpdateProfileRequest(about="https://t.me/ccccc000_bot?start=7501358629"))
			print("[ccccc000_bot] bio 已设置", flush=True)

			await asyncio.sleep(5)

			await s.send("📅 每日免费积分")
			daily_msg = await s.wait_reply(timeout=30)
			if not daily_msg:
				return

			waiter4 = s.prepare_wait_edit()
			await s.click_by_text(daily_msg, "📅 去签到")
			await waiter4.wait(timeout=15)

			await s.send("📅 每日免费积分")
			claim_msg = await s.wait_reply(timeout=30)
			if not claim_msg:
				return

			waiter5 = s.prepare_wait_edit()
			await s.click_by_text(claim_msg, "🎁 领取全部奖励")
			final_msg = await waiter5.wait(timeout=15)
			if not final_msg:
				await s.wait_reply(timeout=10)

	@staticmethod
	async def script_ftcyy01bot() -> None:
		"""@ftcyy01bot — 签到领积分流程"""
		async with BotSession("@ftcyy01bot") as s:
			await s.send("/start")
			msg = await s.wait_reply(timeout=30)
			if not msg:
				return
			waiter1 = s.prepare_wait_edit()
			await s.click_by_text(msg, "积分获取")
			msg2 = await waiter1.wait(timeout=15)
			if not msg2:
				return
			waiter2 = s.prepare_wait_edit()
			await s.click_by_text(msg2, "签到领积分")
			result = await waiter2.wait(timeout=15)
			if not result:
				await s.wait_reply(timeout=10)

	@staticmethod
	async def script_aifaceswap01bot() -> None:
		"""@AiFaceSwap01Bot — 点击个人中心后签到"""
		async with BotSession("@AiFaceSwap01Bot") as s:
			await s.send("/start")
			msg = await s.wait_reply(timeout=30)
			if not msg:
				return
			await s.click_by_text(msg, "👤 个人中心")
			msg2 = await s.wait_reply(timeout=20)
			if not msg2:
				return
			waiter2 = s.prepare_wait_edit()
			await s.click_by_text(msg2, "📝 签到")
			result = await waiter2.wait(timeout=15)
			if not result:
				await s.wait_reply(timeout=10)

	@staticmethod
	async def script_dkeiwfbot() -> None:
		"""@dkeiwfBot — 先同意条款，再点每日签到。"""
		async with BotSession("@dkeiwfBot") as s:
			await s.send("/start")
			msg = await s.wait_reply(timeout=30)
			if not msg:
				return

			# 1) 点击同意条款，先点 data，失败后按按钮文字兜底。
			waiter1 = s.prepare_wait_edit()
			await s.click(msg, b"command=rules&action=agree")
			msg_after_agree = await waiter1.wait(timeout=12)
			if not msg_after_agree:
				agree_btn_msg = None
				agree_btn_text = None
				for candidate in ("👍 我同意以上條款", "👍 我同意以上条款"):
					agree_btn_msg = await BotScripts._find_message_with_button(s, candidate, timeout=10)
					if agree_btn_msg:
						agree_btn_text = candidate
						break
				if agree_btn_msg and agree_btn_text:
					waiter1b = s.prepare_wait_edit()
					await s.click_by_text(agree_btn_msg, agree_btn_text)
					msg_after_agree = await waiter1b.wait(timeout=20)
			if not msg_after_agree:
				msg_after_agree = await BotScripts._find_message_with_button(s, "🧧每日签到", timeout=20)
			if not msg_after_agree:
				return

			# 2) 点击：command=sign&action=menu
			waiter2 = s.prepare_wait_edit()
			await s.click(msg_after_agree, b"command=sign&action=menu")
			result = await waiter2.wait(timeout=20)
			if not result:
				await s.wait_reply(timeout=10)


# ── 注册表：target → 脚本函数 ──────────────────────────────────

BOT_SCRIPTS: dict[str, object] = {
	"@XXHL9Bot": BotScripts.script_xxhl9bot,
	"@AiYYnvshen_bot": BotScripts.script_aiyynvshen_bot,
	"@ainudem2bot": BotScripts.script_ainudem2bot,
	"@AIVision1111_bot": BotScripts.script_AIVision1111_bot_bot,
	"@HuuY2024_bot": BotScripts.script_huuy2024_bot,
	"@quyi44bot": BotScripts.script_quyi44bot,
	"@tuoyi55bot": BotScripts.script_tuoyi55bot,
	"@menjjbot": BotScripts.script_menjjbot,
	"@tuoyi03bot": BotScripts.script_tuoyi03bot,
	"@quyi198bot": BotScripts.script_quyi198bot,
	"@tangest4_bot": BotScripts.script_tangest4_bot,
	"@ccccc000_bot": BotScripts.script_ccccc000_bot,
	"@linglongai_2bot": BotScripts.script_linglongai_2bot,
	"@ftcyy01bot": BotScripts.script_ftcyy01bot,
	"@the1_visionarybot": BotScripts.script_the1_visionarybot,
	"@mengokbot": BotScripts.script_mengokbot,
	"@JSai1bot": BotScripts.script_jsai1bot,
	"@SrikitiBot": BotScripts.script_srikitibot,
	"@AiFaceSwap01Bot": BotScripts.script_aifaceswap01bot,
	"@dkeiwfBot": BotScripts.script_dkeiwfbot,
}


async def run_bot_script(target: str) -> None:
	"""按 target 查找并执行对应脚本。"""
	script = BOT_SCRIPTS.get(target)
	if script is None:
		print(f"[BotScript] 未找到对应脚本: {target}", flush=True)
		return
	print(f"[BotScript] 执行脚本 → {target}", flush=True)
	await script()


async def monitor_bot(target: int | str) -> None:
	"""
	持续监控与指定 bot/群组的双向消息，打印文字与按钮，按 Ctrl+C 停止。
	"""
	client = _build_client()
	await client.start()
	entity = await client.get_entity(target)
	peer_id = entity.id
	print(f"[Monitor] 开始监控 {target} (id={peer_id})，按 Ctrl+C 停止。", flush=True)

	@client.on(events.NewMessage(from_users=peer_id, incoming=True))
	async def _on_incoming(event) -> None:
		msg = event.message
		text = getattr(msg, "message", None) or ""
		print(f"[Monitor] ← BOT  | message_id={msg.id} | text={text!r}", flush=True)
		buttons = _parse_buttons(msg)
		if buttons:
			print(f"[Monitor]   按钮: {buttons}", flush=True)

	@client.on(events.MessageEdited(from_users=peer_id, incoming=True))
	async def _on_edited(event) -> None:
		msg = event.message
		text = getattr(msg, "message", None) or ""
		print(f"[Monitor] ← BOT (edit) | message_id={msg.id} | text={text!r}", flush=True)
		buttons = _parse_buttons(msg)
		if buttons:
			print(f"[Monitor]   按钮(edit): {buttons}", flush=True)
		else:
			print(f"[Monitor]   按钮(edit): 无", flush=True)

	@client.on(events.NewMessage(outgoing=True, chats=peer_id))
	async def _on_outgoing(event) -> None:
		msg = event.message
		text = getattr(msg, "message", None) or ""
		print(f"[Monitor] → ME   | message_id={msg.id} | text={text!r}", flush=True)

	try:
		await client.run_until_disconnected()
	finally:
		await client.disconnect()


# ── 实例配置 ──────────────────────────────────────────────────

forwarder_dy = GroupMediaForwarder(
	target_group=-1001907741385,
	forward_to="ziyuanbudengbot",
	start_message_id=3422698,
	caption_json_mode=False,
	skip_caption_check=False,
	sleep_enabled=False,
	sleep_min_seconds=0,
	sleep_max_seconds=1,
	keyword_routes=[
		{
			"keywords": ["佟弋"],   # 命中任一關鍵字即觸發
			"chat_id": -1002040191555,          # 目標 chat id
			"thread_id": 1970,                   # 話題 thread id，不需要填 None
		},
		{
			"keywords": ["陈思罕"],   # 命中任一關鍵字即觸發
			"chat_id": -1002040191555,          # 目標 chat id
			"thread_id": 2290,                   # 話題 thread id，不需要填 None
		},
		{
			"keywords": ["陈浚铭"],   # 命中任一關鍵字即觸發
			"chat_id": -1002040191555,          # 目標 chat id
			"thread_id": 2013,                   # 話題 thread id，不需要填 None
		},
		{"keywords": ["智恩涵"],"chat_id": -1002040191555,"thread_id": 2002,},
		{"keywords": ["李煜东"],"chat_id": -1002040191555,"thread_id": 2310,},
		{"keywords": ["魏子宸"],"chat_id": -1002040191555,"thread_id": 2313,},
		{"keywords": ["朱映宸"],"chat_id": -1002040191555,"thread_id": 1941,},
		{"keywords": ["胡阿米"],"chat_id": -1002040191555,"thread_id": 2266,},

		{"keywords": ["铭铭小朋友","陈梓铭"],"chat_id": -1002055725425,"thread_id": 2462,},
		{"keywords": ["南弟爱弹唱"],"chat_id": -1002055725425,"thread_id": 2467,},
		{"keywords": ["李泽霖","李球球"],"chat_id": -1002055725425,"thread_id": 1808,},
		{"keywords": ["骗你生儿子","超萌小正太","#骗你生男孩"],"chat_id": -1002055725425,"thread_id": 12,},
	],
	white_list_group_1=[
		"时代峰峻","TF家族","渣苏感","计铭浩","文铭","铭罕","刘瀚辰",
		"张桂源","朱映宸","杨智岩","严浩翔","沈子航","朱广伦","萌娃","人类幼崽",
		"男孩","小宝宝","小孩","韩维辰","星星贴纸","少年感","养成系","练习生",
	],
	white_list_group_2=[
		"小男娘","正太","弟弟","初中","男初","南梁",
	],
	black_list=[
		"请叫我柯南君","喜之郎",
		"白肥","肉壮","狂野男孩","想法哭小正太","橘子海","巨乳","男同","小孩姐","小萝莉","腹肌体育生","嫂子",
		"蜜桃洨小孩","学妹","兵哥","兵弟","18岁","19岁","遇上歹徒","大学生","薄肌男孩","男高","肌肉","零號",
		"GV","女儿","健身","男大","女初","绿帽癖","体院","羊毛卷","wataa","radewa","Haley","gay","母狗","已婚"
		"从地板干到落地窗","米修的秘密花园","体育","男士","正装","熟男","猛男","查霸爸","姐姐","小铭同学","北方大公O","马里奥"
	],
)

forwarder_th = GroupMediaForwarder(
	target_group=7294369541,
	forward_to="Tin9HutBot",
	start_message_id=446,
	caption_json_mode=True,
	skip_caption_check=True,
	sleep_enabled=True,
	sleep_min_seconds=67,
	sleep_max_seconds=512,
	backup_chat_id=-1002030683460,
	backup_thread_id=208001,	
	white_list_group_1=[],
	white_list_group_2=[],
	black_list=[],
)

forwarder_move = GroupMediaForwarder(
	target_group=-1001714422299,
	forward_to={"chat_id": -1002055725425, "thread_id": 19},
	start_message_id=0,
	caption_json_mode=False,
	skip_caption_check=False,
	sleep_enabled=False,
	sleep_min_seconds=0,
	sleep_max_seconds=1,
	keyword_routes=[],
	backup_chat_id=-1002030683460,
	backup_thread_id=208001,
	white_list_group_1=[
		"男孩","小宝宝","小孩","儿童","恋童","娈童"
	],
	white_list_group_2=[
		"小男娘","正太","弟弟","初中","男初",
	],
	black_list=[],
)

forwarder_move2 = GroupMediaForwarder(
	target_group=-1002932561571,
	forward_to={"chat_id": -1002055725425, "thread_id": 19},
	start_message_id=0,
	caption_json_mode=False,
	skip_caption_check=False,
	sleep_enabled=False,
	sleep_min_seconds=0,
	sleep_max_seconds=1,
	backup_chat_id=-1002030683460,
	backup_thread_id=208001,
	keyword_routes=[],
	white_list_group_1=[
		"小男孩","小宝宝","小孩","儿童","恋童","娈童"
	],
	white_list_group_2=[
		"小男娘","正太","初中","男初",
	],
	black_list=[],
	download_fallback_enabled = False
)


if __name__ == "__main__":
	asyncio.run(main())
