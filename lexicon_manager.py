# synonym_manager.py
from __future__ import annotations
from typing import Dict, List
from lz_mysql import MySQLPool


class LexiconManager:
    """
    负责：
    - 搜索词库资源加载（同义词 / 停用词 / 专有名词）
    - 统一令 search pipeline 做 normalize、filter
    - 支持资料库 ↔ 文本 的同步导出

    核心思想：词库维护在文本 / DB， runtime 用内存读一次即可。
    """

    # === 内存缓存 ===
    _synonym_map: dict[str, str] = {}     # e.g. {"小正太": "正太"}
    _stop_words: set[str] = set()         # e.g. {"视频", "资源"}
    _proper_nouns: set[str] = set()       # e.g. {"正太与正太"}

    _syn_loaded: bool = False
    _stop_loaded: bool = False
    _proper_loaded: bool = False

    # ======= 统一入口 =======
    @classmethod
    def ensure_loaded(
        cls,
        synonym_path: str = "search_synonyms.txt",
        stopword_path: str = "search_stopwords.txt",
        proper_path: str = "search_proper_nouns.txt"
    ) -> None:
        """
        统一入口：保证同义词、停用词、专有名词都至少加载一次。
        推荐在 connect() 或 search 第一次调用时执行。
        """
        cls.load_synonyms_once(synonym_path)
        cls.load_stop_words_once(stopword_path)
        cls.load_proper_nouns_once(proper_path)

    # ======= 载入同义词 =======
    @classmethod
    def load_synonyms_once(cls, path: str) -> None:
        import os

        if cls._syn_loaded:
            return

        if not os.path.exists(path):
            print(f"[Lexicon] 未找到同义词文件：{path}，跳过加载", flush=True)
            cls._synonym_map = {}
            cls._syn_loaded = True
            return

        mapping: dict[str, str] = {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split()
                    canonical = parts[0]
                    mapping[canonical] = canonical  # canonical 自反

                    for token in parts[1:]:
                        mapping[token] = canonical

            cls._synonym_map = mapping
            cls._syn_loaded = True
            print(f"[Lexicon] loaded {len(mapping)} synonym entries from {path}", flush=True)

        except Exception as e:
            print(f"[Lexicon] 同义词文件读取失败: {e}", flush=True)
            cls._synonym_map = {}
            cls._syn_loaded = True

    # ======= 载入停用词 =======
    @classmethod
    def load_stop_words_once(cls, path: str) -> None:
        import os

        if cls._stop_loaded:
            return

        if not os.path.exists(path):
            print(f"[Lexicon] 未找到停用词文件：{path}", flush=True)
            cls._stop_words = set()
            cls._stop_loaded = True
            return

        words: set[str] = set()
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    token = line.split()[0]
                    if token:
                        words.add(token)

            cls._stop_words = words
            cls._stop_loaded = True
            print(f"[Lexicon] loaded {len(words)} stop-words from {path}", flush=True)

        except Exception as e:
            print(f"[Lexicon] 停用词文件读取失败: {e}", flush=True)
            cls._stop_words = set()
            cls._stop_loaded = True

    # ======= 载入专有名词 =======
    @classmethod
    def load_proper_nouns_once(cls, path: str) -> None:
        import os

        if cls._proper_loaded:
            return

        if not os.path.exists(path):
            print(f"[Lexicon] 未找到专有名词文件：{path}", flush=True)
            cls._proper_nouns = set()
            cls._proper_loaded = True
            return

        nouns: set[str] = set()
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    token = line.split()[0]
                    if token:
                        nouns.add(token)

            cls._proper_nouns = nouns
            cls._proper_loaded = True
            print(f"[Lexicon] loaded {len(nouns)} proper nouns from {path}", flush=True)

        except Exception as e:
            print(f"[Lexicon] 专有名词文件读取失败: {e}", flush=True)
            cls._proper_nouns = set()
            cls._proper_loaded = True

    # ======= 对外处理接口 =======
    @classmethod
    def normalize_tokens(cls, tokens: list[str]) -> list[str]:
        if not cls._syn_loaded:
            cls.load_synonyms_once("search_synonyms.txt")

        if not cls._synonym_map:
            return tokens

        return [cls._synonym_map.get(t, t) for t in tokens]

    @classmethod
    def filter_stop_words(cls, tokens: list[str]) -> list[str]:
        if not cls._stop_loaded:
            cls.load_stop_words_once("search_stopwords.txt")

        if not cls._stop_words:
            return tokens

        # 若词属于专有名词，不过滤
        return [t for t in tokens if t not in cls._stop_words or t in cls._proper_nouns]

    @classmethod
    def reload_synonyms_from_file(cls, path: str = "search_synonyms.txt") -> None:
        """
        强制从本地文件重载同义词映射。
        """
        cls._syn_loaded = False
        cls.load_synonyms_once(path)

    @classmethod
    def reload_stop_words_from_file(cls, path: str = "search_stopwords.txt") -> None:
        """
        强制从本地文件重载停用词。
        """
        cls._stop_loaded = False
        cls.load_stop_words_once(path)