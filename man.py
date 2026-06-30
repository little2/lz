import asyncio
import json
import os
from contextlib import suppress
from pathlib import Path
from telethon import TelegramClient
from telethon.sessions import StringSession
import socks
from man_config import API_HASH, API_ID, SESSION_STRING,SESSION_STRINGS
# from lz_mysql import MySQLPool
from handlers.bot_scripts import BOT_SCRIPTS, BotScripts
from handlers.group_media_forwarder import GroupMediaForwarder
from handlers.group_message_reader import GroupMessageReader
from handlers.group_shot_message_reader import GroupShotMessageReader
# from handlers.target_group_inspector import TargetGroupInspector
import random


GLOBAL_PARAMS_FILE = Path(__file__).with_name(f"{str(SESSION_STRING)[:20]}_global_params.json")
GLOBAL_PARAMS_CHAT_ID = int(os.getenv("GLOBAL_PARAMS_CHAT_ID", "0") or 0)
GLOBAL_PARAMS_THREAD_ID = int(os.getenv("GLOBAL_PARAMS_THREAD_ID", "0") or 0)
BOT_ROUND_INTERVAL_SECONDS = max(10, int(os.getenv("BOT_ROUND_INTERVAL_SECONDS", "600") or 600))
SHOT_READER_COOLDOWN_MINUTES = max(0, int(os.getenv("SHOT_READER_COOLDOWN_MINUTES", "1") or 1))
GLOBAL_PARAMS: dict = {}


PROXY_TYPE = os.getenv("HARRY_PROXY_TYPE", "socks5").strip().lower()
PROXY_HOST = os.getenv("HARRY_PROXY_HOST", "127.0.0.1").strip()
PROXY_PORT = int(os.getenv("HARRY_PROXY_PORT", "3066") or 3066)
PROXY_USERNAME = os.getenv("HARRY_PROXY_USERNAME", "").strip() or None
PROXY_PASSWORD = os.getenv("HARRY_PROXY_PASSWORD", "").strip() or None




def _global_params_file_for_session(session_string: str) -> Path:
	prefix = str(session_string or "")[:20]
	return Path(__file__).with_name(f"{prefix}_global_params.json")


def build_proxy():
    if not PROXY_TYPE or PROXY_TYPE in {"none", "off", "false", "0"}:
        return None
    if not PROXY_HOST or not PROXY_PORT:
        raise RuntimeError("Set HARRY_PROXY_HOST and HARRY_PROXY_PORT when proxy is enabled")

    proxy_types = {
        "socks5": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
    }
    if PROXY_TYPE not in proxy_types:
        raise RuntimeError("HARRY_PROXY_TYPE only supports socks5, socks4, http, or none")

    return (
        proxy_types[PROXY_TYPE],
        PROXY_HOST,
        PROXY_PORT,
        True,
        PROXY_USERNAME,
        PROXY_PASSWORD,
    )

def _build_client(session_string: str = SESSION_STRING) -> TelegramClient:
	"""兼容 StringSession 与本地 .session 文件名两种输入。"""
	 
	proxy = build_proxy()


	raw = str(session_string or "").strip()
	api_id = int(API_ID)

	# StringSession 通常是较长 token，不应被当作 sqlite 文件路径
	if raw and len(raw) > 80 and not raw.endswith(".session") and "/" not in raw and "\\" not in raw:
		return TelegramClient(StringSession(raw), api_id, API_HASH, proxy=proxy)

	return TelegramClient(raw or "man", api_id, API_HASH, proxy=proxy)


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

async def move_mouse():
	import mouse
	mouse.move(random.randint(0, 1920), random.randint(0, 1080))


async def load_global_params(client: TelegramClient, file_path: Path = GLOBAL_PARAMS_FILE) -> dict:
	"""启动时加载全域参数：本地 JSON 优先，否则回退 Telegram。"""
	global GLOBAL_PARAMS

	local_data = _load_json_dict_from_file(file_path)
	if local_data is not None:
		GLOBAL_PARAMS = local_data
		print(f"[GLOBAL_PARAMS] 已从本地加载: {file_path}", flush=True)
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
		await _save_json_dict_to_file(file_path, remote_data, client=client)
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
		await move_mouse()
		try:
			await BotScripts.run_bot_script(target)
		except Exception as e:
			print(f"[run_all_bot] {target} 执行失败: {e}", flush=True)


# ── 实例配置 ──────────────────────────────────────────────────
###



async def process():
	telegram_bot = _build_client()
	try:
		global_paras = await load_global_params(telegram_bot)
		print(f"{global_paras}")

		# await download_limewire_url("https://limewire.com/d/5o7Fs#Q9Voo6YzWL")

		# 设定 - AI机器人签到 --------
		BotScripts.configure_client(telegram_bot)
		BotScripts.configure_global_paras(global_paras)
		# --------------------


		# 设定 - 转发媒体() --------
		GroupMediaForwarder.configure_global_paras(global_paras)

		forwarder_car = GroupMediaForwarder(
			target_group=-1001793842037,
			forward_to="cnyz001bot",
			start_message_id=59,
			caption_json_mode=False,
			skip_caption_check=True,
			sleep_enabled=True,
			sleep_min_seconds=2,
			sleep_max_seconds=2,
			white_list_group_1=[],
			white_list_group_2=[],
			black_list=[],
		)
		forwarder_car.bind_telegram_bot(telegram_bot)
		await forwarder_car._prepare_state_data(client=telegram_bot)
		forwarder_next_start = max(1, int(forwarder_car.resolve_start_message_id()))
		print(f"[RoundRobin] start forwarder_next_start={forwarder_next_start}", flush=True)

		

		# 设定 - 读取指定群组第一则消息 --------
		shot_reader = GroupShotMessageReader(
			target_group=-1002024635058,
			thread_id=13,
			cooldown_minutes=SHOT_READER_COOLDOWN_MINUTES,
			telegram_bot=telegram_bot,
		)
		

		


		# 设定 - 群组监控 --------
		GroupMessageReader.configure_global_paras(global_paras)
		reader = GroupMessageReader(
			target_group=2471390438,
			start_message_id=192857,
			batch_size=300,
			interval_seconds=10,
		)
		reader.bind_telegram_bot(telegram_bot)

		# 设定 - 群组监控2 --------
		GroupMessageReader.configure_global_paras(global_paras)
		reader2 = GroupMessageReader(
			target_group=3963049270,
			start_message_id=0,
			batch_size=300,
			interval_seconds=10,
		)
		reader2.bind_telegram_bot(telegram_bot)


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
				try:
					last_checked_id = await forwarder_car.fetch_and_forward(
						forwarder_next_start,
						max_messages=10,
						respect_sleep=False,
					)
					if isinstance(last_checked_id, int) and last_checked_id >= forwarder_next_start:
						forwarder_next_start = last_checked_id + 1

						"""将 dict 保存为本地 JSON 文件，并同步贴到指定 Telegram 主题。"""
					json_text = json.dumps(GLOBAL_PARAMS, ensure_ascii=False, indent=2)
					GLOBAL_PARAMS_FILE.write_text(json_text, encoding="utf-8")

				except Exception as exc:
					print(f"[RoundRobin] forwarder segment crashed: {exc}", flush=True)

				# 2) 再跑 reader 一段（单批次）。
				# try:
				# 	rows = await reader.fetch_once()
				# 	await _on_reader_batch(rows)
				# except Exception as exc:
				# 	print(f"[RoundRobin] reader segment crashed: {exc}", flush=True)

				# try:
				# 	rows2 = await reader2.fetch_once()
				# 	await _on_reader_batch(rows2)
				# except Exception as exc:
				# 	print(f"[RoundRobin] reader2 segment crashed: {exc}", flush=True)

				# 4) 额外跑一下 shot_reader，保持它的活跃度（它内部有冷却机制，不会每轮都执行）。如果它正好执行了，也顺便测试一下。
				await shot_reader.exec(
					{
						"primary": {"chat_id": -1003791545872, "thread_id": 0},
						"secondary": {"chat_id": -1003856146853, "thread_id": 0},
						"backup": {"chat_id": -1002024635058, "thread_id": 251},
					}
				)


				# 3) 小睡避免空转。
				randtme = random.uniform(5.0, 15.0)
				print(f"[RoundRobin] sleeping for {randtme:.1f} seconds...", flush=True)
				try:
					# await _save_json_dict_to_file(GLOBAL_PARAMS_FILE, GLOBAL_PARAMS, client=telegram_bot)
					print("[RoundRobin] round completed, global params persisted", flush=True)
				except Exception as exc:
					print(f"[RoundRobin] round-end persist failed: {exc}", flush=True)
				await asyncio.sleep(randtme)
				
		# 执行机器人
		await asyncio.gather(
			_run_shared_round_robin(),
			run_health_server(),
		)
		# --------------------

	finally:
		if telegram_bot.is_connected():
			await telegram_bot.disconnect()


async def process_bot():

	for session_string in SESSION_STRINGS:
		global_params_file = _global_params_file_for_session(session_string)

		telegram_bot = _build_client(session_string)
		await telegram_bot.start()

		

		user_info = await telegram_bot.get_me()
		print(f"\n\n已登录账号: {user_info.first_name}\n\n", flush=True)
		BotScripts.set_user_info(user_info)
		

		
		try:
			global_paras = await load_global_params(telegram_bot, file_path=global_params_file)
			print(f"{global_paras}")


			# await download_limewire_url("https://limewire.com/d/5o7Fs#Q9Voo6YzWL")

			# AI机器人签到 --------
			BotScripts.configure_client(telegram_bot)
			BotScripts.configure_global_paras(global_paras)
			BotScripts.set_user_info(user_info)
			# --------------------
		
			print("[process_bot] single-run started", flush=True)
			await run_all_bot()
			await _save_json_dict_to_file(global_params_file, GLOBAL_PARAMS, client=telegram_bot)
			print("[process_bot] single-run finished, stopping", flush=True)
		finally:
			if telegram_bot.is_connected():
				await telegram_bot.disconnect()


async def main() -> None:
	

	# await process()
	await process_bot()

    # 2) 拉取群成员 id + username
	# inspector = TargetGroupInspector(target_group=-1001944620376, telegram_bot=telegram_bot)
	# members = await inspector.list_members()
	# print("members:", len(members))
	# # print("first member:", members[0] if members else None)
	# await inspector.insert_members_to_db(members)
	# await inspector.set_send_only_permissions_for_roles(members)
	# exit()



	
			







	





if __name__ == "__main__":
	asyncio.run(main())
