import asyncio
from datetime import date
import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
	sys.path.insert(0, str(PROJECT_ROOT))

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from man_config import API_HASH, API_ID, SESSION_STRING


def _build_client() -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	raw = str(SESSION_STRING or "").strip()
	api_id = int(API_ID)

	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH)

	return TelegramClient(raw or "man", api_id, API_HASH)


class BotSession:
	"""
	管理与单个 bot/群组的对话 session。
	建议使用 async with 语句，保证 client 自动断开。
	"""

	def __init__(self, target: int | str, telegram_bot: TelegramClient | None = None) -> None:
		self.target = target
		self._client: TelegramClient | None = telegram_bot
		self._owns_client = telegram_bot is None
		self._entity = None

	class _EditWaiter:
		"""BotSession 内部编辑等待器，避免 click 后 edit 的竞态。"""

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
			buttons = BotSession.parse_buttons(msg)
			if buttons:
				print(f"[BotSession] edit 按钮 ← {self._target} | buttons={buttons}", flush=True)
			return msg

	async def __aenter__(self) -> "BotSession":
		if self._client is None:
			self._client = _build_client()
			self._owns_client = True
		if not self._client.is_connected():
			await self._client.start()
		self._entity = await self._client.get_entity(self.target)
		return self

	async def __aexit__(self, *_) -> None:
		if self._client and self._owns_client:
			await self._client.disconnect()
		if self._owns_client:
			self._client = None

	async def send(self, text: str):
		"""发送文字消息，返回已发送的 Message 对象。"""
		sent = await self._client.send_message(entity=self._entity, message=text)
		print(f"[BotSession] 已发送 → {self.target} | text={text!r} | message_id={sent.id}", flush=True)
		return sent

	async def wait_reply(self, timeout: float = 30.0):
		"""等待对方下一条消息，返回 Message 对象；超时返回 None。"""
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

		buttons = BotSession.parse_buttons(msg)
		if buttons:
			print(f"[BotSession] 回复按钮 ← {self.target} | buttons={buttons}", flush=True)
		else:
			print("[BotSession] 回复无按钮", flush=True)

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

	def prepare_wait_edit(self) -> "BotSession._EditWaiter":
		return BotSession._EditWaiter(self._client, self._entity.id, self.target)

	async def wait_edit(self, timeout: float = 15.0):
		return await self.prepare_wait_edit().wait(timeout=timeout)

	@staticmethod
	def parse_buttons(msg) -> list[list[dict]]:
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


class BotScripts:
	"""集中管理所有 script_ 机器人脚本。"""
	_telegram_bot: TelegramClient | None = None
	_global_paras: dict | None = None
	_user_info: dict | None = None

	@staticmethod
	def configure_client(telegram_bot: TelegramClient | None) -> None:
		"""配置由 main() 注入的共用 TelegramClient。"""
		BotScripts._telegram_bot = telegram_bot

	@staticmethod
	def configure_global_paras(global_paras: dict | None) -> None:
		"""配置由 main() 注入的全域参数容器。"""
		BotScripts._global_paras = global_paras if isinstance(global_paras, dict) else None

	@staticmethod
	def set_user_info(user_info) -> None:
		"""配置由 main() 注入的 user_info，部分脚本可能用到。"""
		BotScripts._user_info = user_info
		

	@staticmethod
	def _legacy_json_path() -> str:
		file_name = str(SESSION_STRING or "")[:10]
		return f"{file_name}_bot_script_times.json"

	@staticmethod
	def _load_legacy_data() -> dict:
		json_path = BotScripts._legacy_json_path()
		if not os.path.exists(json_path):
			return {}
		try:
			with open(json_path, "r") as f:
				data = json.load(f)
			if isinstance(data, dict):
				return data
		except Exception as exc:
			print(f"[BotScript] 读取旧版时间文件失败: {exc}", flush=True)
		return {}

	@staticmethod
	def _get_aibot_data() -> dict:
		"""获取并维护 aibot 时间数据；有旧文件时会合并到 global_paras['aibot']。"""
		global_paras = BotScripts._global_paras
		if isinstance(global_paras, dict):
			aibot = global_paras.get("aibot")
			if not isinstance(aibot, dict):
				aibot = {}
				global_paras["aibot"] = aibot
		else:
			aibot = {}

		legacy = BotScripts._load_legacy_data()
		if legacy:
			for k, v in legacy.items():
				old_v = aibot.get(k)
				if not isinstance(old_v, (int, float)) or (isinstance(v, (int, float)) and v > old_v):
					aibot[k] = v

		if isinstance(global_paras, dict):
			global_paras["aibot"] = aibot

		return aibot

	@staticmethod
	def _session(target: str) -> BotSession:
		return BotSession(target, telegram_bot=BotScripts._telegram_bot)

	@staticmethod
	async def _acquire_client() -> tuple[TelegramClient, bool]:
		"""返回 (client, own_client)。own_client=True 代表需在调用方断开连接。"""
		if BotScripts._telegram_bot is not None:
			if not BotScripts._telegram_bot.is_connected():
				await BotScripts._telegram_bot.start()
			return BotScripts._telegram_bot, False

		client = _build_client()
		await client.start()
		return client, True

	@staticmethod
	async def _send_only(target: str, text: str, timeout: float = 30.0) -> None:
		async with BotScripts._session(target) as s:
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
				rows = BotSession.parse_buttons(recent)
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
		"""@XXHL9Bot - 每日签到流程"""
		async with BotScripts._session("@XXHL9Bot") as s:
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
	async def script_linglongai_3bot() -> None:
		await BotScripts._send_only("@linglongai_3bot", "📅 签到")

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
		"""@tangest4_bot - 设 bio 后签到"""
		from telethon.tl.functions.account import UpdateProfileRequest
		client, own_client = await BotScripts._acquire_client()
		try:
			await client(UpdateProfileRequest(about="https://t.me/tangest4_bot?start=ref_7501358629"))
			print("[tangest4_bot] bio 已设置", flush=True)
			await asyncio.sleep(5)
			entity = await client.get_entity("@tangest4_bot")
			sent = await client.send_message(entity=entity, message="📅 签到")
			print(f"[tangest4_bot] 已发送签到 | message_id={sent.id}", flush=True)
		finally:
			if own_client:
				await client.disconnect()

	@staticmethod
	async def script_ccccc000_bot() -> None:
		"""@ccccc000_bot - 浏览作品/点赞/签到流程"""
		from telethon.tl.functions.account import UpdateProfileRequest

		async with BotScripts._session("@ccccc000_bot") as s:

			
	

			user_id = BotScripts._user_info.id

			await s.send("/start")
			menu = await s.wait_reply(timeout=30)
			if not menu:
				return

			# await s.click_by_text(menu, "🏆 每日排行榜")
			# rank_msg = await s.wait_reply(timeout=30)
			# if not rank_msg:
			# 	await s.send("🏆 每日排行榜")
			# 	rank_msg = await s.wait_reply(timeout=30)
			# if not rank_msg:
			# 	rank_msg = menu

			# await s.click_by_text(rank_msg, "🖼️ 浏览作品")
			# browse_msg = await BotScripts._find_message_with_button(s, "❤️ 点赞", timeout=25)
			# if not browse_msg:
			# 	await s.send("🖼️ 浏览作品")
			# 	browse_msg = await BotScripts._find_message_with_button(s, "❤️ 点赞", timeout=25)
			# if not browse_msg:
			# 	browse_msg = rank_msg

			# waiter3 = s.prepare_wait_edit()
			# await s.click_by_text(browse_msg, "❤️ 点赞")
			# liked_msg = await waiter3.wait(timeout=8)
			# if not liked_msg:
			# 	liked_msg = await s.wait_reply(timeout=10)

			if s._client is None:
				return
														 
			await s._client(UpdateProfileRequest(about=f"https://t.me/ccccc000_bot?start={user_id}"))
			print(f"[ccccc000_bot] bio 已设置 user_id = {user_id}", flush=True)
			
			await asyncio.sleep(5)
	

			await s.send("📅 每日免费积分")
			daily_msg = await s.wait_reply(timeout=30)
			if not daily_msg:
				return
			waiter4 = s.prepare_wait_edit()	
			await s.click_by_text(daily_msg, "📅 去签到")		
			await asyncio.sleep(2)
			await waiter4.wait(timeout=15)

			await s.send("📅 每日免费积分")
			claim_msg = await s.wait_reply(timeout=30)
			if not claim_msg:
				return
			waiter4 = s.prepare_wait_edit()	
			await s.click_by_text(claim_msg, "🚀 转发邀请")
			await asyncio.sleep(2)
			await waiter4.wait(timeout=15)


			await s.send("📅 每日免费积分")
			claim_msg = await s.wait_reply(timeout=30)
			if not claim_msg:
				return
			waiter4 = s.prepare_wait_edit()	
			await s.click_by_text(claim_msg, "🔄 刷新进度")
			await asyncio.sleep(2)
			await waiter4.wait(timeout=15)

			await s.send("📅 每日免费积分")
			claim_msg = await s.wait_reply(timeout=30)
			if not claim_msg:
				return
			waiter4 = s.prepare_wait_edit()	
			await s.click_by_text(claim_msg, '🎁 领取奖励')
			await asyncio.sleep(2)
			final_msg = await waiter4.wait(timeout=15)			
			if not final_msg:
				await s.wait_reply(timeout=10)



			

	@staticmethod
	async def script_ftcyy01bot() -> None:
		"""@ftcyy01bot - 签到领积分流程"""
		async with BotScripts._session("@ftcyy01bot") as s:
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
		"""@AiFaceSwap01Bot - 点击个人中心后签到"""
		async with BotScripts._session("@AiFaceSwap01Bot") as s:
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
		"""@dkeiwfBot - 先同意条款，再点每日签到。"""
		async with BotScripts._session("@dkeiwfBot") as s:
			await s.send("/start")
			msg = await s.wait_reply(timeout=30)
			if not msg:
				return

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

			waiter2 = s.prepare_wait_edit()
			await s.click(msg_after_agree, b"command=sign&action=menu")
			result = await waiter2.wait(timeout=20)
			if not result:
				await s.wait_reply(timeout=10)

	@staticmethod
	async def run_bot_script(target: str) -> None:
		"""按 target 查找并执行对应脚本。"""
		script = BOT_SCRIPTS.get(target)
		if script is None:
			print(f"[BotScript] 未找到对应脚本: {target}", flush=True)
			return

		data = BotScripts._get_aibot_data()

		current_time = time.time()
		last_time = data.get(target)
		if last_time is not None:
			same_date = time.strftime("%Y-%m-%d", time.localtime(last_time)) == time.strftime("%Y-%m-%d", time.localtime(current_time))
			within_20_hours = (current_time - last_time) < 5 * 3600
			if within_20_hours and same_date:
				print(f"[BotScript] 上次执行时间为 {time.ctime(last_time)}，距离现在不足5小时，跳过执行 → {target}", flush=True)
				return

		print(f"[BotScript] 执行脚本 → {target}", flush=True)
		await script()

		data[target] = current_time
		if isinstance(BotScripts._global_paras, dict):
			BotScripts._global_paras["aibot"] = data
		print(f"[BotScript] 更新执行时间(写入 global_paras['aibot']) → {target}", flush=True)

	@staticmethod
	async def monitor_bot(target: int | str) -> None:
		"""持续监控与指定 bot/群组的双向消息，打印文字与按钮，按 Ctrl+C 停止。"""
		client, own_client = await BotScripts._acquire_client()
		entity = await client.get_entity(target)
		peer_id = entity.id
		print(f"[Monitor] 开始监控 {target} (id={peer_id})，按 Ctrl+C 停止。", flush=True)

		@client.on(events.NewMessage(from_users=peer_id, incoming=True))
		async def _on_incoming(event) -> None:
			msg = event.message
			text = getattr(msg, "message", None) or ""
			print(f"[Monitor] ← BOT  | message_id={msg.id} | text={text!r}", flush=True)
			buttons = BotSession.parse_buttons(msg)
			if buttons:
				print(f"[Monitor]   按钮: {buttons}", flush=True)

		@client.on(events.MessageEdited(from_users=peer_id, incoming=True))
		async def _on_edited(event) -> None:
			msg = event.message
			text = getattr(msg, "message", None) or ""
			print(f"[Monitor] ← BOT (edit) | message_id={msg.id} | text={text!r}", flush=True)
			buttons = BotSession.parse_buttons(msg)
			if buttons:
				print(f"[Monitor]   按钮(edit): {buttons}", flush=True)
			else:
				print("[Monitor]   按钮(edit): 无", flush=True)

		@client.on(events.NewMessage(outgoing=True, chats=peer_id))
		async def _on_outgoing(event) -> None:
			msg = event.message
			text = getattr(msg, "message", None) or ""
			print(f"[Monitor] → ME   | message_id={msg.id} | text={text!r}", flush=True)

		try:
			await client.run_until_disconnected()
		finally:
			if own_client:
				await client.disconnect()


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
	"@linglongai_3bot": BotScripts.script_linglongai_3bot,
	"@ftcyy01bot": BotScripts.script_ftcyy01bot,
	"@mengokbot": BotScripts.script_mengokbot,
	"@JSai1bot": BotScripts.script_jsai1bot,
	"@SrikitiBot": BotScripts.script_srikitibot,
	"@AiFaceSwap01Bot": BotScripts.script_aifaceswap01bot,
	"@dkeiwfBot": BotScripts.script_dkeiwfbot,
}


__all__ = ["BotSession", "BotScripts", "BOT_SCRIPTS"]
