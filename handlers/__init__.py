"""处理程序模块"""

from .download_url import DownloadUrl, download_limewire_url
try:
	from .bot_scripts import BOT_SCRIPTS, BotScripts, BotSession
except ModuleNotFoundError:
	BOT_SCRIPTS = {}
	BotScripts = None
	BotSession = None

try:
	from .group_media_forwarder import GroupMediaForwarder
except ModuleNotFoundError:
	GroupMediaForwarder = None
from .group_message_reader import GroupMessageReader
from .target_group_inspector import TargetGroupInspector

__all__ = [
	"DownloadUrl",
	"download_limewire_url",
	"TargetGroupInspector",
	"GroupMediaForwarder",
	"GroupMessageReader",
	"BotSession",
	"BotScripts",
	"BOT_SCRIPTS",
]
