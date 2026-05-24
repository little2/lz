"""LimeWire 媒体下载处理程序"""
import asyncio
import mimetypes
import re
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlparse, unquote


class DownloadUrl:
	"""通过浏览器执行 JavaScript 并下载媒体文件"""

	def __init__(self, url: str, *, wait_timeout_ms: int = 20000, save_dir: str | Path | None = None, headless: bool = False, window_width: int = 420, window_height: int = 320):
		self.url = url
		self.wait_timeout_ms = wait_timeout_ms
		self.save_dir = Path(save_dir) if save_dir is not None else Path(__file__).parent.parent / "limewire_downloads"
		self.headless = headless
		self.window_width = window_width
		self.window_height = window_height
		self._start_ts = None

	def _log(self, step: str, message: str) -> None:
		elapsed = asyncio.get_running_loop().time() - self._start_ts if self._start_ts else 0
		print(f"[LimeWire][{step}][{elapsed:.1f}s] {message}", flush=True)

	@staticmethod
	def _sanitize_filename(name: str) -> str:
		clean = re.sub(r"[\\/:*?\"<>|\r\n]+", "_", str(name or "").strip())
		clean = clean.strip(". ")
		return clean or "limewire_media"

	def _pick_destination(self, base_name: str, fallback_ext: str | None = None) -> Path:
		filename = self._sanitize_filename(base_name)
		path = Path(filename)
		stem = path.stem or "limewire_media"
		suffix = path.suffix
		if not suffix and fallback_ext:
			suffix = fallback_ext
		destination = self.save_dir / f"{stem}{suffix}"
		counter = 1
		while destination.exists():
			destination = self.save_dir / f"{stem}_{counter}{suffix}"
			counter += 1
		return destination

	@staticmethod
	def _filename_from_disposition(header: str | None) -> str | None:
		if not header:
			return None
		match = re.search(r"filename\*=UTF-8''([^;]+)", header, flags=re.IGNORECASE)
		if match:
			return DownloadUrl._sanitize_filename(unquote(match.group(1)))
		match = re.search(r'filename="?([^";]+)"?', header, flags=re.IGNORECASE)
		if match:
			return DownloadUrl._sanitize_filename(match.group(1))
		return None

	@staticmethod
	def _looks_like_media_bytes(data: bytes) -> bool:
		if not data:
			return False
		if data.startswith(b"\xFF\xD8\xFF"):
			return True
		if data.startswith(b"\x89PNG\r\n\x1a\n"):
			return True
		if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
			return True
		if data.startswith(b"RIFF") and len(data) > 12 and data[8:12] == b"WEBP":
			return True
		if data.startswith(b"%PDF-") or data.startswith(b"PK\x03\x04"):
			return True
		if len(data) > 12 and data[4:8] == b"ftyp":
			return True
		return False

	async def run(self) -> str:
		self._start_ts = asyncio.get_running_loop().time()
		self._log("INIT", f"准备下载，url={self.url}")
		try:
			from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
		except ImportError as exc:
			raise RuntimeError("需要安装 playwright 才能执行 JS 并抓取 LimeWire 页面内容") from exc

		self.save_dir.mkdir(parents=True, exist_ok=True)
		self._log("INIT", f"下载目录={self.save_dir}")

		async with async_playwright() as playwright:
			browser = None
			launch_error: Exception | None = None
			for launch_kwargs in (
				{"headless": self.headless, "channel": "msedge"},
				{"headless": self.headless, "channel": "chrome"},
				{"headless": True},
				{"headless": True, "channel": "msedge"},
				{"headless": True, "channel": "chrome"},
			):
				try:
					self._log("BROWSER", f"尝试启动浏览器参数={launch_kwargs}")
					browser = await playwright.chromium.launch(**launch_kwargs)
					self._log("BROWSER", f"浏览器启动成功，参数={launch_kwargs}")
					break
				except Exception as exc:
					self._log("BROWSER", f"浏览器启动失败，参数={launch_kwargs}，error={exc}")
					launch_error = exc

			if browser is None:
				raise RuntimeError("无法启动可执行 JS 的浏览器，请先安装 Playwright Chromium 或系统 Chrome/Edge") from launch_error

			try:
				self._log("PAGE", "创建新页面")
				page = await browser.new_page(
					accept_downloads=True,
					viewport={"width": int(self.window_width), "height": int(self.window_height)},
				)
				self._log("PAGE", f"视窗大小={int(self.window_width)}x{int(self.window_height)}")

				api_request_urls: list[str] = []
				api_payloads: list[dict] = []

				def _capture_api_request(req) -> None:
					if "api.limewire.com/sharing/download/" in req.url and req.method.upper() == "POST":
						api_request_urls.append(req.url)
						self._log("API", f"已捕获 sharing API 请求: {req.url}")

				async def _capture_api_response(resp) -> None:
					if "api.limewire.com/sharing/download/" not in resp.url:
						return
					if resp.request.method.upper() != "POST":
						return
					try:
						payload = await resp.json()
						total = len(payload.get("contentItems") or []) if isinstance(payload, dict) else 0
						api_payloads.append(payload if isinstance(payload, dict) else {})
						self._log("API", f"已捕获 sharing API 响应，contentItems={total}")
					except Exception as exc:
						self._log("API", f"读取 sharing API 响应失败: {exc}")

				page.on("request", _capture_api_request)
				page.on("response", lambda resp: asyncio.create_task(_capture_api_response(resp)))

				self._log("PAGE", "打开链接中")
				await page.goto(self.url, wait_until="domcontentloaded", timeout=self.wait_timeout_ms)
				self._log("PAGE", "页面已到 domcontentloaded")

				with suppress(PlaywrightTimeoutError):
					self._log("PAGE", "等待 load 完成")
					await page.wait_for_load_state("load", timeout=self.wait_timeout_ms)
					self._log("PAGE", "页面 load 已完成")

				self._log("FIND", "等待 Download 元素出现")
				downloading_span = page.locator("span.sr-only", has_text="Download").first
				await downloading_span.wait_for(state="attached", timeout=self.wait_timeout_ms)
				self._log("FIND", "Download 元素已出现")

				click_download_js = r"""() => {
					const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
					// 優先找 Download all
					const allSpans = Array.from(document.querySelectorAll('span'));
					for (const span of allSpans) {
						if (normalize(span.textContent) === 'download all') {
							const clickable = span.closest('button, a, [role=\"button\"]') || span.parentElement;
							if (clickable) {
								clickable.click();
								return 'download-all';
							}
						}
					}
					// 再找 Download (原本邏輯)
					const srSpans = Array.from(document.querySelectorAll('span.sr-only'));
					for (const span of srSpans) {
						if (normalize(span.textContent) !== 'download') continue;
						const clickable = span.closest('button, a, [role=\"button\"]') || span.parentElement;
						if (!clickable) continue;
						clickable.click();
						return 'sr-only-button';
					}
					const byVisibleButton = Array.from(document.querySelectorAll('button, [role=\"button\"], a')).find((el) => normalize(el.textContent) === 'download');
					if (byVisibleButton) {
						byVisibleButton.click();
						return 'visible-button';
					}
					throw new Error('No Download/Download all button found');
				}"""

				download = None
				self._log("CLICK", "点击 Download（优先等待浏览器下载事件）")
				try:
					async with page.expect_download(timeout=min(self.wait_timeout_ms, 9000)) as download_info:
						clicked_by = await page.evaluate(click_download_js)
						self._log("CLICK", f"已触发点击策略: {clicked_by}")
					download = await download_info.value
				except PlaywrightTimeoutError:
					self._log("DOWNLOAD", "未捕获到浏览器下载事件，改用 API 回传 downloadUrl 直抓")
					timeout_until = asyncio.get_running_loop().time() + 4
					while not api_payloads and not api_request_urls and asyncio.get_running_loop().time() < timeout_until:
						await page.wait_for_timeout(200)
					if not api_payloads and not api_request_urls:
						self._log("CLICK", "首次点击未触发 API，执行第二次点击重试")
						clicked_by = await page.evaluate(click_download_js)
						self._log("CLICK", f"第二次点击策略: {clicked_by}")

				if download is not None:
					self._log("DOWNLOAD", "已捕获浏览器下载事件")
					suggested_name = download.suggested_filename or Path(self.url.split("?", 1)[0]).name or "limewire_media"
					destination = self._pick_destination(suggested_name)
					self._log("SAVE", f"保存文件到 {destination}")
					await download.save_as(destination)
					self._log("DONE", f"媒体保存完成: {destination}")
					return str(destination)

				self._log("API", "等待 LimeWire sharing API 响应")
				deadline = asyncio.get_running_loop().time() + (self.wait_timeout_ms / 1000)
				while not api_payloads and asyncio.get_running_loop().time() < deadline:
					await page.wait_for_timeout(200)

				if not api_payloads and api_request_urls:
					fallback_api_url = api_request_urls[-1]
					self._log("API", f"未等到响应，改用已捕获请求 URL 主动拉取: {fallback_api_url}")
					api_resp = await page.context.request.post(
						fallback_api_url,
						timeout=self.wait_timeout_ms,
						fail_on_status_code=False,
					)
					if api_resp.status >= 400:
						raise RuntimeError(f"sharing API 主动拉取失败，status={api_resp.status}")
					api_payloads.append(await api_resp.json())

				if not api_payloads:
					raise RuntimeError("等待 sharing API 响应超时，且未捕获可重试请求")

				api_payload = api_payloads[-1]
				content_items = api_payload.get("contentItems") or []
				if not content_items:
					raise RuntimeError("sharing API 未返回 contentItems")

				chosen_item = None
				for item in content_items:
					if item.get("downloadUrl") and item.get("metadataId") is None:
						chosen_item = item
						break
				if chosen_item is None:
					for item in content_items:
						if item.get("downloadUrl"):
							chosen_item = item
							break
				if chosen_item is None:
					raise RuntimeError("sharing API 返回中找不到可用 downloadUrl")

				download_url = str(chosen_item["downloadUrl"])
				self._log("API", "已取得 downloadUrl，开始直接下载")
				raw_resp = await page.context.request.get(
					download_url,
					timeout=self.wait_timeout_ms,
					fail_on_status_code=False,
				)
				if raw_resp.status >= 400:
					raise RuntimeError(f"downloadUrl 请求失败，status={raw_resp.status}")

				content_disposition = raw_resp.headers.get("content-disposition")
				content_type = raw_resp.headers.get("content-type", "")
				name_from_header = self._filename_from_disposition(content_disposition)
				name_from_url = self._sanitize_filename(Path(unquote(urlparse(download_url).path)).name)
				page_title = await page.title()
				name_from_title = None
				if page_title.lower().startswith("download "):
					name_from_title = self._sanitize_filename(page_title[9:].split("|", 1)[0].strip())

				fallback_ext = None
				if "/" in content_type:
					fallback_ext = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())

				final_name = name_from_header or name_from_title or name_from_url or "limewire_media"
				destination = self._pick_destination(final_name, fallback_ext=fallback_ext)
				body_bytes = await raw_resp.body()
				if not self._looks_like_media_bytes(body_bytes):
					raise RuntimeError(
						"下载内容疑似仍为加密数据（非可识别媒体文件）。"
						"请优先使用非 headless 浏览器模式重试。"
					)
				self._log("SAVE", f"保存文件到 {destination}")
				destination.write_bytes(body_bytes)
				self._log("DONE", f"媒体保存完成: {destination}")
				return str(destination)
			except Exception as exc:
				self._log("ERROR", f"下载流程失败: {exc}")
				raise
			finally:
				self._log("BROWSER", "关闭浏览器")
				await browser.close()


async def download_limewire_url(
	url: str,
	*,
	wait_timeout_ms: int = 20000,
	save_dir: str | Path | None = None,
	headless: bool = False,
	window_width: int = 420,
	window_height: int = 320,
) -> str:
	"""用浏览器执行页面 JS，点击 Download 并把媒体文件保存到本地"""
	downloader = DownloadUrl(
		url,
		wait_timeout_ms=wait_timeout_ms,
		save_dir=save_dir,
		headless=headless,
		window_width=window_width,
		window_height=window_height,
	)
	return await downloader.run()
