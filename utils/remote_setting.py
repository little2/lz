import json
import os
from pathlib import Path
from urllib import request, error

SETTING_URL = os.getenv('SETTING_URL')
CACHE_FILE = Path(__file__).resolve().parent / 'remote_setting_cache.json'


def _load_local_setting() -> dict:
    try:
        if CACHE_FILE.exists():
            with CACHE_FILE.open('r', encoding='utf-8') as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def _save_local_setting(data: dict) -> None:
    try:
        with CACHE_FILE.open('w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except OSError:
        pass

def _fetch_remote_bot_setting() -> dict:
    url = SETTING_URL
    if not url:
        return _load_local_setting()

    try:
        with request.urlopen(url, timeout=5) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(payload or "{}")
            if isinstance(data, dict):
                _save_local_setting(data)
                return data
            return _load_local_setting()
    except (error.URLError, TimeoutError, json.JSONDecodeError):
        return _load_local_setting()
