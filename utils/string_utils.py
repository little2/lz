import re
import json
from typing import List

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

    @staticmethod
    def clean_text(original_string: str) -> str:
        s = LZString._to_text(original_string)

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
