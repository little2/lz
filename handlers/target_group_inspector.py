"""TargetGroupInspector 处理器。"""
import asyncio
import json
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights, InputPeerUser
from telethon.errors.rpcerrorlist import FloodWaitError

try:
	from man_config import API_HASH, API_ID, SESSION_STRING
except ImportError:
	API_HASH = os.getenv("API_HASH", "")
	API_ID = os.getenv("API_ID", "")
	SESSION_STRING = os.getenv("SESSION_STRING", "")


def _build_client() -> TelegramClient:
	"""兼容 StringSession 與本地 .session 文件名兩種輸入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID) if API_ID else 0

	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


class TargetGroupInspector:
	"""抓取指定 Telegram 群組消息，並收集群成員 id/username。"""

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
		"""根據 participant 類型提取成員角色。"""
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
		僅允許發文字消息（等價 Bot API: can_send_messages=true，其他權限 false）。
		在 Telethon 的 ChatBannedRights 里，True 表示"禁止該權限"。
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
		"""支持傳入 user_id 或 list_members() 產出的 user dict。返回 (user_id, role, access_hash)。"""
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
		"""設置用戶為"僅可發送消息"。返回結構：{"ok": bool, "status": str, ...}"""
		user_id, role, access_hash = self._parse_user_input(user)
		if user_id is None:
			return {"ok": False, "status": "bad_user"}

		if role not in {"restricted", "left", "member"}:
			return {"ok": False, "status": "role_skipped", "user_id": user_id, "role": role}

		client = _build_client()
		await client.start()
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
			await client.disconnect()

	async def _resolve_source_entity(self, client: TelegramClient):
		"""解析 target_group，支持 id / username / 對話補找。"""
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
				f"無法解析來源 target_group={self.target_group}。"
				"若是純數字 user_id，請先與該對象產生會話，"
				"或改用 @username。"
			) from exc

	async def fetch_messages(self, limit: int = 100, start_message_id: int = 1) -> list[dict]:
		"""從 target_group 抓取消息。"""
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
		"""列出 target_group 所有成員，並將 id/username/role 保存到 self.members。"""
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

	async def set_send_only_permissions_for_roles(
		self,
		users: list[dict[str, int | str | None]],
		sleep_seconds: float = 1.1,
		state_file: Path | None = None,
	) -> list[dict]:
		"""批量設置：對 restricted/left/member 成員設置僅可發送消息。"""
		if state_file is None:
			state_file = Path(__file__).with_name(f"set_permissions_{self.target_group}.json")

		processed_ids = self._load_processed_ids(state_file)
		print(f"[State] 已讀取進度文件：{state_file}，已處理 {len(processed_ids)} 人。", flush=True)

		results: list[dict] = []
		total = len(users)
		for idx, user in enumerate(users, start=1):
			user_id, _, _ah = self._parse_user_input(user)
			pct = idx / total * 100

			if user_id is not None and user_id in processed_ids:
				print(f"[Skip] {idx}/{total} ({pct:.1f}%) user_id={user_id} 已處理，跳過。", flush=True)
				results.append({"ok": True, "status": "already_done", "user_id": user_id})
				continue

			result = await self.set_send_only_permissions(user)
			if result.get("status") == "flood_wait":
				wait = result.get("flood_wait_seconds", 30) + 1
				print(f"[FloodWait] user_id={result.get('user_id')} 等待 {wait} 秒後重試...", flush=True)
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

		print(f"[Progress] 完成，共處理 {total} 人（其中 {len(processed_ids)} 人已記錄）。", flush=True)
		return results

	@staticmethod
	def _load_processed_ids(state_file: Path) -> set[int]:
		"""從本地文件讀取已處理的 user_id 集合。"""
		if not state_file.exists():
			return set()
		try:
			data = json.loads(state_file.read_text(encoding="utf-8"))
			return {int(x) for x in data if str(x).lstrip("-").isdigit()}
		except (json.JSONDecodeError, ValueError):
			return set()

	@staticmethod
	def _save_processed_id(state_file: Path, processed_ids: set[int]) -> None:
		"""將已處理的 user_id 集合寫回本地文件。"""
		state_file.write_text(
			json.dumps(sorted(processed_ids), ensure_ascii=False, indent=2),
			encoding="utf-8",
		)
