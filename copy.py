import importlib.util
import sysconfig
from pathlib import Path


if __name__ == "copy":
    # 當標準庫嘗試 import copy 時，轉發到真正的 stdlib copy.py，避免被同名腳本污染。
    stdlib_copy_path = Path(sysconfig.get_paths()["stdlib"]) / "copy.py"
    spec = importlib.util.spec_from_file_location("_stdlib_copy", stdlib_copy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load stdlib copy module from {stdlib_copy_path}")
    _stdlib_copy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_stdlib_copy)

    for _name in dir(_stdlib_copy):
        if _name.startswith("__") and _name not in {"__all__", "__doc__"}:
            continue
        globals()[_name] = getattr(_stdlib_copy, _name)

    __all__ = getattr(_stdlib_copy, "__all__", [])
else:
    import requests

    BOT_TOKEN = "8666057394:AAHw-EO3tO0gcXDajjl3tZqb1hXdMYngOP4"

    def copy_message():
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/copyMessage"
        data = {
            "chat_id": 7972120149,
            "from_chat_id": 8666057394,
            "message_id": 1474041,
        }
        r = requests.post(url, data=data, timeout=30)
        return r.json()

    if __name__ == "__main__":
        print(copy_message())