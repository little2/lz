import asyncio
import json
import tempfile
from pathlib import Path
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import ChatForwardsRestrictedError

from lz_config import API_HASH, API_ID, USER_SESSION


class GroupMediaForwarder:
	"""从指定群组抓取媒体消息并转发到目标。"""

	def __init__(
		self,
		target_group: int | str,
		forward_to: str,
		start_message_id: int = 1,
		caption_json_mode: bool = False,
		state_file: Path | None = None,
		white_list_group_1: list[str] | None = None,
		white_list_group_2: list[str] | None = None,
		black_list: list[str] | None = None,
	) -> None:
		self.target_group = target_group
		self.forward_to = forward_to
		self.default_start_message_id = start_message_id
		self.caption_json_mode = caption_json_mode
		self.state_file = state_file or Path(__file__).with_name("man_last_message_id.txt")
		self.white_list_group_1 = white_list_group_1 or []
		self.white_list_group_2 = white_list_group_2 or []
		self.black_list = black_list or []

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

	def write_last_message_id(self, message_id: int) -> None:
		state_data = self._load_state_data()
		state_data[str(self.target_group)] = message_id
		self._write_state_data(state_data)

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

	async def _resend_message(self, client: TelegramClient, forward_entity, message, caption_override: str | None = None) -> None:
		"""當來源聊天禁止轉傳時，改為下載並重新發送內容。"""
		caption = caption_override if caption_override is not None else (message.message or "")

		if getattr(message, "media", None):
			with tempfile.TemporaryDirectory(prefix="man_media_") as tmp_dir:
				downloaded_path = await client.download_media(message, file=tmp_dir)
				if downloaded_path:
					send_kwargs = {
						"entity": forward_entity,
						"file": downloaded_path,
						"caption": caption,
					}

					# 盡量保留訊息型態
					if getattr(message, "video", None):
						send_kwargs["supports_streaming"] = True
					if getattr(message, "voice", None):
						send_kwargs["voice_note"] = True
					if getattr(message, "video_note", None):
						send_kwargs["video_note"] = True

					await client.send_file(**send_kwargs)
					return

		if caption:
			await client.send_message(entity=forward_entity, message=caption)

	# ── 核心异步方法 ──────────────────────────────────────────

	async def fetch_messages(self, start_message_id: int, limit: int) -> list[dict]:
		client = TelegramClient(USER_SESSION, API_ID, API_HASH)
		await client.start()
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
		client = TelegramClient(USER_SESSION, API_ID, API_HASH)
		await client.start()
		try:
			source_entity = await self._resolve_source_entity(client)
			forward_entity = await client.get_entity(self.forward_to)
			last_message_id = start_message_id

			async for message in client.iter_messages(
				source_entity,
				min_id=start_message_id - 1,
				reverse=True,
			):
				last_message_id = message.id
				text = self.serialize_message(message).get("text", "")
				if self.is_blacklisted(text):
					continue
				if self.classify_text(text) in {"group_1", "group_2"}:
					formatted_caption = self._format_caption(message, text)

					if self.caption_json_mode:
						await self._resend_message(client, forward_entity, message, caption_override=formatted_caption)
					else:
						try:
							await client.forward_messages(
								entity=forward_entity,
								messages=[message.id],
								from_peer=source_entity,
							)
						except ChatForwardsRestrictedError:
							await self._resend_message(client, forward_entity, message, caption_override=formatted_caption)
					self.write_last_message_id(message.id)
					await asyncio.sleep(1)

			return last_message_id
		finally:
			await client.disconnect()

	async def run(self) -> None:
		start_message_id = self.resolve_start_message_id()
		last_message_id = await self.fetch_and_forward(start_message_id)
		print(last_message_id)


# ── 实例配置 ──────────────────────────────────────────────────

forwarder = GroupMediaForwarder(
	target_group=-1001907741385,
	forward_to="ziyuanbudengbot",
	start_message_id=0,
	caption_json_mode=False,
	white_list_group_1=[
		"时代峰峻","TF家族","佟弋","渣苏感","计铭浩","文铭","铭罕","刘瀚辰","穆祉丞","陈浚铭",
		"陈思罕","张桂源","朱映宸","杨智岩","严浩翔","沈子航","智恩涵","朱广伦","萌娃","人类幼崽",
		"男孩","小宝宝","小孩","韩维辰","星星贴纸","少年感","养成系","练习生","骗你生儿子",
	],
	white_list_group_2=[
		"小男娘","正太","弟弟","初中","男初","南梁",
	],
	black_list=[
		"白肥","狂野男孩","想法哭小正太","橘子海","巨乳","男同","小孩姐","小萝莉","腹肌体育生",
		"蜜桃洨小孩","学妹","兵哥","18岁","19岁","遇上歹徒","大学生","薄肌男孩","男高","肌肉",
		"GV","女儿","健身","男大","女初","绿帽癖","体院","羊毛卷","wataa","radewa","Haley",
		"从地板干到落地窗",
	],
)

forwarder2 = GroupMediaForwarder(
	target_group=7294369541,
	forward_to="ziyuanbudengbot",
	start_message_id=0,
	caption_json_mode=True,
	white_list_group_1=[
		"儿子","TF家族","佟弋","渣苏感","计铭浩","文铭","铭罕","刘瀚辰","穆祉丞","陈浚铭",
		"陈思罕","张桂源","朱映宸","杨智岩","严浩翔","沈子航","智恩涵","朱广伦","萌娃","人类幼崽",
		"男孩","小宝宝","小孩","韩维辰","星星贴纸","少年感","养成系","练习生","骗你生儿子",
	],
	white_list_group_2=[
		"小男娘","正太","弟弟","初中","男初","南梁",
	],
	black_list=[
		"白肥","狂野男孩","想法哭小正太","橘子海","巨乳","男同","小孩姐","小萝莉","腹肌体育生",
		"蜜桃洨小孩","学妹","兵哥","18岁","19岁","遇上歹徒","大学生","薄肌男孩","男高","肌肉",
		"GV","女儿","健身","男大","女初","绿帽癖","体院","羊毛卷","wataa","radewa","Haley",
		"从地板干到落地窗",
	],
)

if __name__ == "__main__":
	asyncio.run(forwarder.run())
