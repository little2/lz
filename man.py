import asyncio
import json
import os
import re
from contextlib import suppress
from pathlib import Path
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import FloodWaitError
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights, InputPeerUser

from man_config import API_HASH, API_ID, SESSION_STRING
from lz_mysql import MySQLPool
from handlers.bot_scripts import BOT_SCRIPTS, BotScripts
from handlers.group_media_forwarder import GroupMediaForwarder
from handlers.group_message_reader import GroupMessageReader
import random


GLOBAL_PARAMS_FILE = Path(__file__).with_name(f"{str(SESSION_STRING)[:20]}_global_params.json")
GLOBAL_PARAMS_CHAT_ID = int(os.getenv("GLOBAL_PARAMS_CHAT_ID", "0") or 0)
GLOBAL_PARAMS_THREAD_ID = int(os.getenv("GLOBAL_PARAMS_THREAD_ID", "0") or 0)
BOT_ROUND_INTERVAL_SECONDS = max(10, int(os.getenv("BOT_ROUND_INTERVAL_SECONDS", "600") or 600))
GLOBAL_PARAMS: dict = {}


def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID)

	# StringSession 通常是较长 token，不应被当作 sqlite 文件路径
	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


def _load_json_dict_from_file(file_path: Path) -> dict | None:
	"""从本地 JSON 文件读取 dict。读取失败时返回 None。"""
	if not file_path.exists():
		return None
	try:
		data = json.loads(file_path.read_text(encoding="utf-8"))
		if isinstance(data, dict):
			return data
		print(f"[GLOBAL_PARAMS] 文件存在但不是 JSON object: {file_path}", flush=True)
	except Exception as exc:
		print(f"[GLOBAL_PARAMS] 读取失败 {file_path}: {exc}", flush=True)
	return None


async def _save_json_dict_to_file(
	file_path: Path,
	data: dict,
	client: TelegramClient | None = None,
) -> None:
	"""将 dict 保存为本地 JSON 文件，并同步贴到指定 Telegram 主题。"""
	json_text = json.dumps(data, ensure_ascii=False, indent=2)
	file_path.write_text(json_text, encoding="utf-8")

	if GLOBAL_PARAMS_CHAT_ID == 0:
		return

	own_client = False
	work_client = client
	if work_client is None:
		work_client = _build_client()
		own_client = True

	try:
		if not work_client.is_connected():
			await work_client.start()
		entity = await work_client.get_entity(GLOBAL_PARAMS_CHAT_ID)
		await work_client.send_message(
			entity=entity,
			message=json_text,
			reply_to=GLOBAL_PARAMS_THREAD_ID if GLOBAL_PARAMS_THREAD_ID > 0 else None,
		)
		print(
			f"[GLOBAL_PARAMS] 已同步发送到 Telegram(chat_id={GLOBAL_PARAMS_CHAT_ID}, thread_id={GLOBAL_PARAMS_THREAD_ID})",
			flush=True,
		)
	except Exception as exc:
		print(f"[GLOBAL_PARAMS] 同步发送到 Telegram 失败: {exc}", flush=True)
	finally:
		if own_client and work_client.is_connected():
			await work_client.disconnect()


async def _fetch_latest_json_from_telegram(
	client: TelegramClient,
	chat_id: int,
	thread_id: int | None,
) -> dict:
	"""从指定 chat/thread 抓最后一笔消息，并解析其中 JSON。"""
	if not client.is_connected():
		await client.start()

	entity = await client.get_entity(chat_id)
	kwargs = {"limit": 1}
	if thread_id is not None and thread_id > 0:
		kwargs["reply_to"] = thread_id

	async for msg in client.iter_messages(entity, **kwargs):
		text = str(getattr(msg, "message", "") or "").strip()
		if not text:
			continue
		data = json.loads(text)
		if not isinstance(data, dict):
			raise ValueError("最后一笔消息内容不是 JSON object")
		return data

	raise ValueError("找不到可用消息（chat/thread 为空或没有文本）")


async def load_global_params(client: TelegramClient) -> dict:
	"""启动时加载全域参数：本地 JSON 优先，否则回退 Telegram。"""
	global GLOBAL_PARAMS

	local_data = _load_json_dict_from_file(GLOBAL_PARAMS_FILE)
	if local_data is not None:
		GLOBAL_PARAMS = local_data
		print(f"[GLOBAL_PARAMS] 已从本地加载: {GLOBAL_PARAMS_FILE}", flush=True)
		return GLOBAL_PARAMS

	if GLOBAL_PARAMS_CHAT_ID == 0:
		print("[GLOBAL_PARAMS] 本地文件不存在，且未设置 GLOBAL_PARAMS_CHAT_ID，使用空配置。", flush=True)
		GLOBAL_PARAMS = {}
		return GLOBAL_PARAMS

	try:
		remote_data = await _fetch_latest_json_from_telegram(
			client,
			chat_id=GLOBAL_PARAMS_CHAT_ID,
			thread_id=GLOBAL_PARAMS_THREAD_ID if GLOBAL_PARAMS_THREAD_ID > 0 else None,
		)
		GLOBAL_PARAMS = remote_data
		await _save_json_dict_to_file(GLOBAL_PARAMS_FILE, remote_data, client=client)
		print(
			f"[GLOBAL_PARAMS] 本地不存在，已从 Telegram(chat_id={GLOBAL_PARAMS_CHAT_ID}, thread_id={GLOBAL_PARAMS_THREAD_ID}) 获取并落盘。",
			flush=True,
		)
	except Exception as exc:
		print(f"[GLOBAL_PARAMS] 从 Telegram 获取失败，使用空配置: {exc}", flush=True)
		GLOBAL_PARAMS = {}

	return GLOBAL_PARAMS


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
			await BotScripts.run_bot_script(target)
		except Exception as e:
			print(f"[run_all_bot] {target} 执行失败: {e}", flush=True)


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
			if own_client and client.is_connected():
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
			await cur.executemany(
				"""
				INSERT INTO pure (user_id, done)
				VALUES (%s, 0)
				ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)
				""",
				[(uid,) for uid in id_list],
			)

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


# ── 实例配置 ──────────────────────────────────────────────────
###

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

forwarder_sm = GroupMediaForwarder(
	target_group=3851147469,
	forward_to="ziyuanbudengbot",
	start_message_id=0,
	caption_json_mode=True,
	skip_caption_check=True,
	sleep_enabled=True,
	sleep_min_seconds=10,
	sleep_max_seconds=10,
	backup_chat_id=-1002030683460,
	backup_thread_id=208001,	
	white_list_group_1=[],
	white_list_group_2=[],
	black_list=[],
)

forwarder_th = GroupMediaForwarder(
	target_group=7484645431,
	forward_to="Tin9HutBot",
	start_message_id=525,
	caption_json_mode=True,
	skip_caption_check=True,
	sleep_enabled=True,
	sleep_min_seconds=10,
	sleep_max_seconds=10,
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




async def main() -> None:
	telegram_bot = _build_client()
	global_paras = {}


    # # 2) 拉取群成员 id + username
	# inspector = TargetGroupInspector(target_group=-1001800096525, telegram_bot=telegram_bot)
	# members = await inspector.list_members()
	# print("members:", len(members))
	# # print("first member:", members[0] if members else None)
	# await inspector.insert_members_to_db(members)
	# # await inspector.set_send_only_permissions_for_roles(members)
	# exit()



	try:
		global_paras = await load_global_params(telegram_bot)
		print(f"{global_paras}")

		# await download_limewire_url("https://limewire.com/d/5o7Fs#Q9Voo6YzWL")

		# AI机器人签到 --------
		BotScripts.configure_client(telegram_bot)
		BotScripts.configure_global_paras(global_paras)
		# --------------------


		# 转发媒体 --------
		GroupMediaForwarder.configure_global_paras(global_paras)

		forwarder_th = GroupMediaForwarder(
			target_group=7484645431,
			forward_to="Tin9HutBot",
			start_message_id=0,
			caption_json_mode=True,
			skip_caption_check=True,
			sleep_enabled=True,
			sleep_min_seconds=32,
			sleep_max_seconds=420,
			white_list_group_1=[],
			white_list_group_2=[],
			black_list=[],
		)
		forwarder_th.bind_telegram_bot(telegram_bot)

		# 群组监控 --------
		GroupMessageReader.configure_global_paras(global_paras)
		reader = GroupMessageReader(
			target_group=3961484553,
			start_message_id=0,
			batch_size=300,
			interval_seconds=10,
		)
		reader.bind_telegram_bot(telegram_bot)
		

		async def _on_reader_batch(rows: list[dict]) -> None:
			await _save_json_dict_to_file(GLOBAL_PARAMS_FILE, GLOBAL_PARAMS, client=telegram_bot)
			if not rows:
				print(
					f"[GroupMessageReader] fetched=0 next_start={reader.next_message_id}",
					flush=True,
				)
				return
			first_id = rows[0].get("id")
			last_id = rows[-1].get("id")
			print(
				f"[GroupMessageReader] fetched={len(rows)} first_id={first_id} last_id={last_id} next_start={reader.next_message_id}",
				flush=True,
			)

		await forwarder_th._prepare_state_data(client=telegram_bot)
		forwarder_next_start = max(1, int(forwarder_th.resolve_start_message_id()))
		print(f"[RoundRobin] start forwarder_next_start={forwarder_next_start}", flush=True)

		async def _run_shared_round_robin() -> None:
			nonlocal forwarder_next_start
			print("[RoundRobin] task started (forwarder segment -> reader segment)", flush=True)
			last_bot_round_at = 0.0
			while True:
				# 0) BotScripts 段落：按固定间隔执行，避免每轮都跑完整批脚本。
				now = asyncio.get_running_loop().time()
				if now - last_bot_round_at >= BOT_ROUND_INTERVAL_SECONDS:
					try:
						print(
							f"[RoundRobin] bot segment started (interval={BOT_ROUND_INTERVAL_SECONDS}s)",
							flush=True,
						)
						await run_all_bot()
						await _save_json_dict_to_file(GLOBAL_PARAMS_FILE, GLOBAL_PARAMS, client=telegram_bot)
					except Exception as exc:
						print(f"[RoundRobin] bot segment crashed: {exc}", flush=True)
					finally:
						last_bot_round_at = now

				# 1) 先跑 forwarder 一段，避免长期占用事件循环。
				# try:
				# 	last_checked_id = await forwarder_th.fetch_and_forward(
				# 		forwarder_next_start,
				# 		max_messages=10005,
				# 		respect_sleep=False,
				# 	)
				# 	if isinstance(last_checked_id, int) and last_checked_id >= forwarder_next_start:
				# 		forwarder_next_start = last_checked_id + 1
				# except Exception as exc:
				# 	print(f"[RoundRobin] forwarder segment crashed: {exc}", flush=True)

				# 2) 再跑 reader 一段（单批次）。
				try:
					rows = await reader.fetch_once()
					await _on_reader_batch(rows)
				except Exception as exc:
					print(f"[RoundRobin] reader segment crashed: {exc}", flush=True)

				# 3) 小睡避免空转。
				randtme = random.uniform(37.0, 1121.0)
				print(f"[RoundRobin] sleeping for {randtme:.1f} seconds...", flush=True)
				await asyncio.sleep(randtme)
				

		await asyncio.gather(
			_run_shared_round_robin(),
			run_health_server(),
		)
		# --------------------

	finally:
		if telegram_bot.is_connected():
			await telegram_bot.disconnect()







	





if __name__ == "__main__":
	asyncio.run(main())
