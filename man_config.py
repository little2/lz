import json
import os
from pathlib import Path

try:
	from dotenv import load_dotenv
except ModuleNotFoundError:
	def load_dotenv(*args, **kwargs):
		return False

load_dotenv(dotenv_path=Path(__file__).with_name('.man.env'))

config: dict = {}

try:
	configuration_json = json.loads(os.getenv('CONFIGURATION', '') or '{}')
	if isinstance(configuration_json, dict):
		config.update(configuration_json)
except Exception as error:
	print(f"⚠️ 無法解析 CONFIGURATION：{error}")


def _parse_session_strings(value: object) -> list[str]:
	def _resolve_item(item: str) -> str:
		name = item.strip()
		# 支持寫成 SESSION_STRING_FOO 這類環境變數名
		if name and name.replace('_', '').isalnum():
			resolved = os.getenv(name, '').strip()
			if resolved:
				return resolved
		return name

	if isinstance(value, list):
		return [_resolve_item(str(item)) for item in value if str(item).strip()]

	text = str(value or '').strip()
	if not text:
		return []

	# 优先尝试 JSON 数组格式，例如: ["s1", "s2"]
	try:
		parsed = json.loads(text)
		if isinstance(parsed, list):
			return [_resolve_item(str(item)) for item in parsed if str(item).strip()]
	except Exception:
		pass

	# 回退支持逗号分隔格式，例如: s1,s2,s3
	return [_resolve_item(item) for item in text.split(',') if item.strip()]


API_ID = int(config.get('api_id', os.getenv('API_ID', 0)))
API_HASH = config.get('api_hash', os.getenv('API_HASH', ''))
SESSION_STRING = config.get('session_string', os.getenv('SESSION_STRING', ''))
SESSION_STRINGS = _parse_session_strings(config.get('session_strings', os.getenv('SESSION_STRINGS', '')))
FORWARDER_RUN_TARGET = str(
	config.get('forwarder_run_target', os.getenv('FORWARDER_RUN_TARGET', 'forwarder_th'))
).strip().lower()


