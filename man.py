import asyncio
from pathlib import Path
from telethon import TelegramClient

from lz_config import API_HASH, API_ID, USER_SESSION


TARGET_GROUP = -1001907741385
START_MESSAGE_ID = 3144311
STATE_FILE = Path(__file__).with_name("man_last_message_id.txt")
FORWARD_BOT_USERNAME = "ziyuanbudengbot"
WHITE_LIST_GROUP_1 = [
	"时代峰峻","TF家族","佟弋","渣苏感","文铭","刘瀚辰","穆祉丞","陈浚铭","张桂源","朱映宸","杨智岩","严浩翔","沈子航","智恩涵","朱广伦","萌娃","人类幼崽","男孩","小宝宝","小孩","韩维辰","星星贴纸", "少年感", "养成系", "练习生","骗你生儿子"
]
WHITE_LIST_GROUP_2 = [
	"小男娘","正太","弟弟","初中","男初","南梁"
]
BLACK_LIST = ["18岁","遇上歹徒","大学生","薄肌男孩","肌肉","GV","女儿","健身","男大","女初","绿帽癖","体院","羊毛卷","wataa","radewa","Haley","从地板干到落地窗"
]


def serialize_message(message) -> dict:
	sender = getattr(message, "sender_id", None)
	return {
		"id": message.id,
		"date": message.date.isoformat() if message.date else None,
		"sender_id": sender,
		"text": message.message or "",
	}


def classify_text(text: str) -> str:
	for keyword in WHITE_LIST_GROUP_1:
		if keyword and keyword in text:
			return "group_1"

	for keyword in WHITE_LIST_GROUP_2:
		if keyword and keyword in text:
			return "group_2"

	return "group_3"


def is_blacklisted(text: str) -> bool:
	for keyword in BLACK_LIST:
		if keyword and keyword in text:
			return True

	return False


def resolve_start_message_id() -> int:
	if STATE_FILE.exists():
		content = STATE_FILE.read_text(encoding="utf-8").strip()
		if content.isdigit():
			return int(content)

	return START_MESSAGE_ID


def write_last_message_id(message_id: int) -> None:
	STATE_FILE.write_text(str(message_id), encoding="utf-8")


async def fetch_messages(start_message_id: int, limit: int) -> list[dict]:
	if not TARGET_GROUP:
		raise ValueError("Set TARGET_GROUP in man.py before running this script.")

	client = TelegramClient(USER_SESSION, API_ID, API_HASH)
	await client.start()

	try:
		entity = await client.get_entity(TARGET_GROUP)
		messages = []

		async for message in client.iter_messages(
			entity,
			min_id=start_message_id - 1,
			reverse=True,
			limit=limit,
		):
			messages.append(serialize_message(message))

		return messages
	finally:
		await client.disconnect()


async def fetch_messages_and_forward(start_message_id: int) -> int:
	if not TARGET_GROUP:
		raise ValueError("Set TARGET_GROUP in man.py before running this script.")

	client = TelegramClient(USER_SESSION, API_ID, API_HASH)
	await client.start()

	try:
		source_entity = await client.get_entity(TARGET_GROUP)
		forward_entity = await client.get_entity(FORWARD_BOT_USERNAME)
		last_message_id = start_message_id

		async for message in client.iter_messages(
			source_entity,
			min_id=start_message_id - 1,
			reverse=True,
		):
			last_message_id = message.id
			serialized_message = serialize_message(message)
			if is_blacklisted(serialized_message.get("text", "")):
				continue

			group_name = classify_text(serialized_message.get("text", ""))

			if group_name in {"group_1", "group_2"}:
				await client.forward_messages(
					entity=forward_entity,
					messages=[message.id],
					from_peer=source_entity,
				)
				write_last_message_id(message.id)
				await asyncio.sleep(1)

		return last_message_id
	finally:
		await client.disconnect()


async def main() -> None:
	start_message_id = resolve_start_message_id()
	last_message_id = await fetch_messages_and_forward(start_message_id)
	print(last_message_id)


if __name__ == "__main__":
	asyncio.run(main())
