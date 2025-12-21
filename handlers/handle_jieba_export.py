# handlers/handle_jieba_export.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Dict, Any

from aiogram.types import Message

from lz_mysql import MySQLPool
from lexicon_manager import LexiconManager


def _ensure_parent_dir(path: Path) -> None:
    parent = path.parent
    if str(parent) and not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)


def ensure_file_exists(path: str, default_text: str = "") -> bool:
    """
    确保文件存在；若不存在则创建并写入 default_text
    return: True 表示本次新建，False 表示原本已存在
    """
    p = Path(path)
    if p.exists():
        return False
    _ensure_parent_dir(p)
    p.write_text(default_text, encoding="utf-8")
    return True


async def export_lexicon_files(
    message: Optional[Message] = None,
    output_dir: str = ".",
    force: bool = False,
) -> Dict[str, Any]:
    """
    从 MySQL 导出：
    - jieba_userdict.txt
    - search_synonyms.txt（并重载 LexiconManager）
    - search_stopwords.txt（并重载 LexiconManager）

    force=True：即使文件已存在也覆盖写入。
    """
    await MySQLPool.init_pool()

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) jieba userdict
    if message:
        await message.answer("⏳ 正在汇出 jieba 自定义词库，请稍候…")

    jieba_path = out_dir / "jieba_userdict.txt"
    if force or (not jieba_path.exists()):
        text = await MySQLPool.export_jieba_dict()
        if text:
            jieba_path.write_text(text, encoding="utf-8")
        else:
            # 没有可导出的内容，也要保证文件存在（避免启动时报不存在）
            ensure_file_exists(str(jieba_path), default_text="# jieba user dict\n")

    if message:
        await message.answer("✅ jieba_userdict.txt 已生成并写入本地。")

    # 2) synonyms
    if message:
        await message.answer("⏳ 正在汇出同义词词库，请稍候…")

    syn_path = out_dir / "search_synonyms.txt"
    if force or (not syn_path.exists()):
        syn_text = await MySQLPool.export_synonym_lexicon()
        if syn_text:
            syn_path.write_text(syn_text, encoding="utf-8")
            LexiconManager.reload_synonyms_from_file(str(syn_path))
        else:
            # 没内容也不要中断；保证文件存在
            ensure_file_exists(str(syn_path), default_text="# synonyms\n")
            LexiconManager.reload_synonyms_from_file(str(syn_path))

    if message:
        await message.answer("✅ 同义词词库已生成并写入本地。")

    # 3) stopwords
    if message:
        await message.answer("⏳ 正在汇出停用词词库，请稍候…")

    stop_path = out_dir / "search_stopwords.txt"
    if force or (not stop_path.exists()):
        stop_text = await MySQLPool.export_stopword_lexicon()
        if stop_text:
            stop_path.write_text(stop_text, encoding="utf-8")
            LexiconManager.reload_stop_words_from_file(str(stop_path))
        else:
            ensure_file_exists(str(stop_path), default_text="# stopwords\n")
            LexiconManager.reload_stop_words_from_file(str(stop_path))

    if message:
        await message.answer("✅ 停用词词库已生成并写入本地。")

    return {
        "jieba_userdict": str(jieba_path),
        "synonyms": str(syn_path),
        "stopwords": str(stop_path),
    }


async def ensure_lexicon_files(output_dir: str = ".", force: bool = False) -> Dict[str, Any]:
    """
    用于“启动时自动生成”：
    - 若目标文件不存在，则自动导出/生成（不会发消息）
    - force=True 可用于强制重建
    """
    out_dir = Path(output_dir)
    jieba_path = out_dir / "jieba_userdict.txt"
    syn_path = out_dir / "search_synonyms.txt"
    stop_path = out_dir / "search_stopwords.txt"

    need = force or (not jieba_path.exists()) or (not syn_path.exists()) or (not stop_path.exists())
    if not need:
        return {
            "skipped": True,
            "jieba_userdict": str(jieba_path),
            "synonyms": str(syn_path),
            "stopwords": str(stop_path),
        }

    return await export_lexicon_files(message=None, output_dir=output_dir, force=force)



# handlers/handle_jieba_export.py  （追加在文件底部）
import asyncio
import jieba

_LEXICON_BOOTSTRAPPED = False
_LEXICON_LOCK = asyncio.Lock()

async def ensure_and_load_lexicon_runtime(
    output_dir: str = ".",
    userdict_name: str = "jieba_userdict.txt",
    export_if_missing: bool = True,
) -> dict:
    """
    启动时调用（不发消息）：
    1) 确保 jieba_userdict.txt 存在（不存在先创建空文件，避免被跳过）
    2) 若 export_if_missing=True 且文件是本次新建，则尝试从 MySQL 导出三件套（userdict/syn/stop）
    3) jieba.load_userdict(userdict)
    4) LexiconManager.ensure_loaded()

    幂等：全局只执行一次
    """
    global _LEXICON_BOOTSTRAPPED
    if _LEXICON_BOOTSTRAPPED:
        return {"skipped": True}

    async with _LEXICON_LOCK:
        if _LEXICON_BOOTSTRAPPED:
            return {"skipped": True}

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        userdict_path = out_dir / userdict_name

        # 1) 兜底：先保证文件存在（避免你之前的“未找到→跳过加载”）
        created = ensure_file_exists(str(userdict_path), default_text="# jieba user dict\n")
        if created:
            print(f"[jieba] 未找到词典文件：{userdict_path}，已自动创建空词典", flush=True)

        # 2) 若刚创建，且允许导出，则从 MySQL 导出覆盖真实内容（以及 syn/stop）
        if created and export_if_missing:
            try:
                await ensure_lexicon_files(output_dir=output_dir, force=False)
                print("[jieba] 已从 MySQL 导出词库文件", flush=True)
            except Exception as e:
                # MySQL 不可用也不应阻断启动：空词典可继续跑
                print(f"[jieba] MySQL 词库导出失败（将继续使用空词典）：{e}", flush=True)

        # 3) 加载 jieba userdict（此时必然存在）
        try:
            jieba.load_userdict(str(userdict_path))
            print(f"[jieba] 自定义词典已加载：{userdict_path}", flush=True)
        except Exception as e:
            print(f"[jieba] 加载词典失败: {e}", flush=True)

        # 4) 统一加载 LexiconManager（三件套：syn/stop/proper 等）
        try:
            LexiconManager.ensure_loaded()
        except Exception as e:
            print(f"[Lexicon] ensure_loaded failed: {e}", flush=True)

        _LEXICON_BOOTSTRAPPED = True
        return {
            "skipped": False,
            "userdict": str(userdict_path),
        }
