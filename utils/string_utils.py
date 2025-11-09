import re
import json
from typing import List
import os

class LZString:
    # --- 小工具：安全转字符串 ---
    @staticmethod
    def _to_text(x) -> str:
        if x is None:
            return ""
        if isinstance(x, type) or callable(x):  # 避免把类/函数当成字符串
            return ""
        if not isinstance(x, str):
            try:
                return str(x)
            except Exception:
                return ""
        return x


import json
import re
from typing import Iterable

class LZString:
    _ZERO_WIDTH = re.compile(r'[\u200B-\u200F\uFEFF]')
    _URL = re.compile(r'https?://[^\s)>\]]+')
    _TME = re.compile(r'https?://t\.me/[^\s)>\]]+', re.IGNORECASE)
    _FLAT_JSON = re.compile(r'\{[^{}]{1,2000}\}')  # 防止极长文本卡顿
    _BLANK_LINE = re.compile(r'^[ \t]*$', re.MULTILINE)

    _AD_CUT_MARKS: tuple[str, ...] = (
        "- Advertisement - No Guarantee",
        "- 广告 - 无担保",
    )

    _NOISE_PHRASES: tuple[str, ...] = (
        "求打赏", "求赏", "可通过以下方式获取或分享文件",
        "✅共找到 1 个媒体",
        "私聊模式：将含有File ID的文本直接发送给机器人 @datapanbot 即可进行文件解析",
        "①私聊模式：将含有File ID的文本直接发送给机器人  即可进行文件解析",
        "单机复制：", "文件解码器:", "您的文件码已生成，点击复制：",
        "批量发送的媒体代码如下:", "此条媒体分享link:",
        "女侅搜索：@ seefilebot", "解码：@ MediaBK2bot",
        "如果您只是想备份，发送 /settings 可以设置关闭此条回复消息",
        "媒体包已创建！", "此媒体代码为:", "文件名称:", "分享链接:", "|_SendToBeach_|",
        "Forbidden: bot was kicked from the supergroup chat",
        "Bad Request: chat_id is empty",
    )

    _TEMPLATE_PATTERNS: tuple[re.Pattern, ...] = tuple(
        re.compile(p, re.IGNORECASE) for p in (
            r'LINK\s*\n[^\n]+#C\d+\s*\nOriginal:[^\n]*\n?',
            r'LINK\s*\n[^\n]+#C\d+\s*\nForwarded from:[^\n]*\n?',
            r'LINK\s*\n[^\n]*#C\d+\s*',
            r'Original caption:[^\n]*\n?',
        )
    )

    @staticmethod
    def _to_text(s) -> str:
        return "" if s is None else str(s)

    @staticmethod
    def _cut_at_any(hay: str, marks: Iterable[str]) -> str:
        cut = len(hay)
        for m in marks:
            p = hay.find(m)
            if p != -1:
                cut = min(cut, p)
        return hay[:cut]

    @staticmethod
    def clean_text(original_string: str) -> str:
        s = LZString._to_text(original_string)

        # 0) 归一化换行 & 去零宽字符
        s = s.replace('\r\n', '\n').replace('\r', '\n')
        s = LZString._ZERO_WIDTH.sub('', s)

        # 1) 截断广告块
        s = LZString._cut_at_any(s, LZString._AD_CUT_MARKS)

        # 2) 批量移除噪声短语
        for t in LZString._NOISE_PHRASES:
            if t in s:
                s = s.replace(t, "")

        # 3) 去掉分享到期提示
        s = re.sub(r"分享至\d{4}-\d{2}-\d{2} \d{2}:\d{2} 到期后您仍可重新分享", "", s)

        # 4) 尝试多段扁平 JSON 抽取 content/text
        def _json_repl(m):
            block = m.group(0)
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                return ""
            text_parts = []
            if isinstance(data, dict):
                c = data.get('content')
                t = data.get('text')
                if isinstance(c, str) and c.strip():
                    text_parts.append(c)
                if isinstance(t, str) and t.strip():
                    # 避免 content 和 text 重复
                    if not text_parts or t.strip() != text_parts[-1].strip():
                        text_parts.append(t)
            return ("\n" + "\n".join(text_parts)) if text_parts else ""
        s = LZString._FLAT_JSON.sub(_json_repl, s)

        # 5) 链接与模板移除
        s = LZString._TME.sub('', s)      # 先清 t.me
        s = LZString._URL.sub('', s)      # 其他链接
        for pat in LZString._TEMPLATE_PATTERNS:
            s = pat.sub('', s)

        # 6) 清空白行、去重、保序
        s = LZString._BLANK_LINE.sub('', s)
        lines = [ln.strip() for ln in s.split('\n') if ln.strip()]
        uniq = list(dict.fromkeys(lines))
        result = "\n".join(uniq)

        # 7) 特定符号前插入换行（避免 \r）
        for symbol in ('🔑', '💎'):
            result = result.replace(symbol, '\n' + symbol)

        # 8) 压尾部多余空白并截断
        result = result.strip()
        return result[:1500] if len(result) > 1500 else result


    @staticmethod
    def clean_text2(original_string: str) -> str:
        s = LZString._to_text(original_string)

        # 0) 统一换行 & 去掉零宽字符
        s = s.replace('\r\n', '\n').replace('\r', '\n')
        s = re.sub(r'[\u200B-\u200F\uFEFF]', '', s)


        # 1) 截断广告块
        for target in ["- Advertisement - No Guarantee", "- 广告 - 无担保"]:
            pos = s.find(target)
            if pos != -1:
                s = s[:pos]

        # 2) 批量替换噪声短语
        replace_texts = [
            "求打赏", "求赏", "可通过以下方式获取或分享文件",
            "✅共找到 1 个媒体",
            "私聊模式：将含有File ID的文本直接发送给机器人 @datapanbot 即可进行文件解析",
            "①私聊模式：将含有File ID的文本直接发送给机器人  即可进行文件解析",
            "单机复制：", "文件解码器:", "您的文件码已生成，点击复制：",
            "批量发送的媒体代码如下:", "此条媒体分享link:",
            "女侅搜索：@ seefilebot", "解码：@ MediaBK2bot",
            "如果您只是想备份，发送 /settings 可以设置关闭此条回复消息",
            "媒体包已创建！", "此媒体代码为:", "文件名称:", "分享链接:", "|_SendToBeach_|",
            "Forbidden: bot was kicked from the supergroup chat",
            "Bad Request: chat_id is empty",
        ]
        for t in replace_texts:
            s = s.replace(t, "")

        # 3) 去掉分享到期提示
        s = re.sub(r"分享至\d{4}-\d{2}-\d{2} \d{2}:\d{2} 到期后您仍可重新分享", "", s)

        # 4) 提取内嵌 JSON 里的 content，再移除原 JSON 块
        json_pattern = re.compile(r'\{[^{}]*?"text"\s*:\s*"[^"]+"[^{}]*?\}')
        def _extract_and_strip_json(m):
            block = m.group(0)
            try:
                data = json.loads(block)
                extra = ""
                if 'content' in data and isinstance(data['content'], str):
                    extra = "\n" + data['content']
                return extra  # 用 extra 替换整个 JSON 块
            except json.JSONDecodeError:
                return ""     # 解析失败就当作噪声移除
        s = json_pattern.sub(_extract_and_strip_json, s)

        # 5) 移除链接/模板段
        s = re.sub(r'https://t\.me/[^\s]+', '', s)
        for pat in [
            r'LINK\s*\n[^\n]+#C\d+\s*\nOriginal:[^\n]*\n?',
            r'LINK\s*\n[^\n]+#C\d+\s*\nForwarded from:[^\n]*\n?',
            r'LINK\s*\n[^\n]*#C\d+\s*',
            r'Original caption:[^\n]*\n?',
        ]:
            s = re.sub(pat, '', s)

        # 6) 去掉纯空白行，并做去重（保留先出现的行）
        s = re.sub(r'^\s*$', '', s, flags=re.MULTILINE)
        lines = [ln for ln in s.split('\n') if ln.strip() != ""]
        unique_lines = list(dict.fromkeys(lines))
        result = "\n".join(unique_lines)

        # 7) 特定符号前插入换行
        for symbol in ['🔑', '💎']:
            result = result.replace(symbol, '\r\n' + symbol)

        return result[:1500] if len(result) > 1500 else result



    @staticmethod
    def extract_meaningful_name(filename: str) -> str | None:
        """
        从文件名中提取有意义的部分。
        若无意义则返回 None。
        """
        # 去除副档名
        name, _ = os.path.splitext(filename)

        # 去除中括号、圆括号、下划线、横线等符号
        s = re.sub(r"[\[\]【】（）(){}<>_+\-.,，。:;!@#%^&*~]", " ", name)

        # 去除多余空格
        s = re.sub(r"\s+", " ", s).strip()

        # 若是纯数字或纯符号，则视为无意义
        if re.fullmatch(r"[\d\s]+", s):
            return None

        # 若包含大量无意义的随机字母（如 aJkRzTq）
        if re.fullmatch(r"[A-Za-z]{6,}", s):
            return None

        # 若中文或英文比例过低，也视为无意义
        zh_count = len(re.findall(r"[\u4e00-\u9fff]", s))
        en_count = len(re.findall(r"[A-Za-z]", s))
        num_count = len(re.findall(r"\d", s))

        total = zh_count + en_count + num_count
        if total == 0:
            return None

        # 若主要是数字或符号
        if num_count / (total + 1e-5) > 0.6:
            return None

        # 若只有少量有效字符（太短）
        if len(s) < 3:
            return None

        # 若匹配“1080p”、“4k”等纯视频信息，也视为无意义
        if re.search(r"(1080p|720p|4k|8k|h264|x264|hevc|mp4|mkv|mov|avi|webm)", s, re.I):
            return None

        # 若包含看似有意义的中英文单词（例如“旅行 日记”、“school project”）
        if zh_count > 0 or re.search(r"[A-Za-z]{3,}", s):
            return s

        return None


    @staticmethod
    def dedupe_cn_sentences(text: str, min_chars: int = 6, return_removed: bool = False, strict: bool = False):
        """
        去除中文文本中的重复句子/片段。
        - 断句：按 。！？!? 与换行
        - strict=False：若“当前句(去空白/标点后)”在前文出现过（作为子串），则删
        - strict=True ：仅删除“完全相同”的重复句（忽略空白与标点后的相等）
        """
        t = LZString._to_text(text)

        # ——断句 & 归一化——
        def _split_cn_sentences(s: str) -> List[str]:
            terms = set("。！？!?")
            sents, buf = [], []
            for ch in s:
                buf.append(ch)
                if ch in terms or ch == "\n":
                    sents.append("".join(buf))
                    buf = []
            if buf:
                sents.append("".join(buf))
            return sents

        _rm_ws = re.compile(r"\s+")
        _rm_punct = re.compile(r"[。！？!?…⋯，,、；;：:\n\r]+")
        def _strip_all(s: str) -> str:
            return _rm_punct.sub("", _rm_ws.sub("", s))

        sents = _split_cn_sentences(t)

        keep_mask = []
        if strict:
            seen = set()
            for s in sents:
                key = _strip_all(s)
                if not key or len(key) < min_chars:
                    keep_mask.append(True); continue
                if key in seen:
                    keep_mask.append(False)
                else:
                    seen.add(key); keep_mask.append(True)
        else:
            # 为了避免 O(n^2) 串接，可累计前缀（简单实现先保留你的写法）
            for i, s in enumerate(sents):
                content = _strip_all(s)
                if not content or len(content) < min_chars:
                    keep_mask.append(True); continue
                prefix_clean = _strip_all("".join(sents[:i]))
                keep_mask.append(content not in prefix_clean)

        # 聚合连续重复句
        removed_groups, cur = [], []
        for s, keep in zip(sents, keep_mask):
            if not keep:
                cur.append(s.strip())
            else:
                if cur:
                    removed_groups.append("".join(cur).strip()); cur = []
        if cur:
            removed_groups.append("".join(cur).strip())

        cleaned = "".join(s for s, keep in zip(sents, keep_mask) if keep)

        if return_removed:
            seen_keys, uniq_groups = set(), []
            for g in removed_groups:
                k = _strip_all(g)
                if k and k not in seen_keys:
                    uniq_groups.append(g); seen_keys.add(k)
            return cleaned, uniq_groups
        return cleaned

    def shorten_text(text: str, max_length: int = 30) -> str:
        if not text:
            return ""
        return text[:max_length] + "..." if len(text) > max_length else text