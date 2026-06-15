"""TargetGroupInspector 处理器。"""

import asyncio
import json
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights, InputPeerUser

try:
	from lz_mysql import MySQLPool
except ModuleNotFoundError:
	MySQLPool = None

try:
	from man_config import API_HASH, API_ID, SESSION_STRING
except ImportError:
	API_HASH = os.getenv("API_HASH", "")
	API_ID = os.getenv("API_ID", "")
	SESSION_STRING = os.getenv("SESSION_STRING", "")


def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID) if API_ID else 0

	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


class TargetGroupInspector:
	"""抓取指定 Telegram 群组消息，并收集群成员 id/username。"""

	def __init__(self, target_group: int | str, telegram_bot: TelegramClient | None = None) -> None:
		self.target_group = target_group
		self.telegram_bot = telegram_bot
		self.members: list[dict[str, int | str | None]] = []

	def bind_telegram_bot(self, telegram_bot: TelegramClient) -> None:
		"""绑定共享 Telethon 客户端。"""
		self.telegram_bot = telegram_bot

	async def _acquire_client(self) -> tuple[TelegramClient, bool]:
		"""获取可用客户端，返回 (client, own_client)。"""
		client = self.telegram_bot or _build_client()
		own_client = self.telegram_bot is None
		if not client.is_connected():
			await client.start()
		return client, own_client

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

		client, own_client = await self._acquire_client()
		try:
			source_entity = await self._resolve_source_entity(client)

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
			if own_client and client.is_connected():
				await client.disconnect()

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
		client, own_client = await self._acquire_client()
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
			if own_client and client.is_connected():
				await client.disconnect()

	async def list_members(self) -> list[dict[str, int | str | None]]:
		"""列出 target_group 所有成员，并将 id/username/role 保存到 self.members。"""
		client, own_client = await self._acquire_client()
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
			if own_client and client.is_connected():
				await client.disconnect()

	async def insert_members_to_db(self, members: list[dict[str, int | str | None]]) -> dict:
		"""将成员列表写入 MySQL pure(user_id, done)，依赖 user_id 主键去重。"""
		if MySQLPool is None:
			return {"ok": False, "error": "MySQLPool is not available"}

		user_ids = {
			int(item.get("id"))
			for item in members
			if isinstance(item, dict) and isinstance(item.get("id"), int)
		}

		if not user_ids:
			return {"ok": True, "total": 0, "existing": 0, "inserted": 0}

		conn, cur = await MySQLPool.get_conn_cursor()
		try:
			id_list = list(user_ids)
			batch_size = 100
			for start in range(0, len(id_list), batch_size):
				batch = id_list[start:start + batch_size]
				values_sql = ", ".join(["(%s, 0)"] * len(batch))
				sql = f"""
					INSERT INTO pure (user_id, done)
					VALUES {values_sql}
					ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)
				"""
				await cur.execute(sql, batch)

			return {
				"ok": True,
				"total": len(id_list),
				"upserted": len(id_list),
			}
		except Exception as exc:
			return {
				"ok": False,
				"total": len(user_ids),
				"error": str(exc),
			}
		finally:
			await MySQLPool.release(conn, cur)

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


__all__ = ["TargetGroupInspector"]
