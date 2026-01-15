# topic_worker.py



import os
import asyncio
from datetime import datetime, timedelta, date, timezone
from collections import defaultdict


print("=== Topic Worker Importing Modules ===", flush=True)


from bertopic import BERTopic

print("=== Topic Worker Importing Modules 2 ===", flush=True)

from sentence_transformers import SentenceTransformer

from pke_zh import TopicRank, TextRank
from dotenv import load_dotenv

from pg_stats_db import PGStatsDB

print("=== Topic Worker Importing Modules ===", flush=True)


# ======== 载入配置 ========
from ly_config import (
    API_ID,
    API_HASH,
    SESSION_STRING,
    COMMAND_RECEIVERS,
    ALLOWED_PRIVATE_IDS,
    ALLOWED_GROUP_IDS,
    PG_DSN,
    PG_MIN_SIZE,
    PG_MAX_SIZE,
    STAT_FLUSH_INTERVAL,
    STAT_FLUSH_BATCH_SIZE,
    KEY_USER_ID,
    THUMB_DISPATCH_INTERVAL,
    THUMB_BOTS,
    THUMB_PREFIX,
    DEBUG_HB_GROUP_ID,
    FORWARD_THUMB_USER
)

TG_TEXT_LIMIT = 4096


# ========== 配置 ==========
MODEL_VERSION = "bertopic_v1"
TOPIC_MIN_SIZE = 5          # 话题最少消息数
MAX_TOPICS = 20             # 每小时最多保留多少个 topic
REP_MSG_PER_TOPIC = 20      # 每个 topic 选多少条代表 message_id
KEYWORDS_PER_TOPIC = 12     # pke_zh 输出关键词数


# ========== pke_zh 关键词抽取 ==========
def extract_keywords2(texts: list[str], topn: int = KEYWORDS_PER_TOPIC) -> list[str]:
    """
    对一个 topic 下的文本集合抽取中文关键词短语
    优先 TopicRank，不稳定时可改用 TextRank
    """
    doc = "\n".join(texts)
    if not doc.strip():
        return []

    try:
        extractor = TopicRank()
        extractor.load_document(
            input=doc,
            language="zh",
            normalization=None,
        )
        # 中文 POS 在不同环境可能不稳定；如果你发现经常为空，可注释掉这行
        # extractor.candidate_selection(pos={"NOUN", "PROPN", "ADJ"})
        
        extractor.candidate_weighting()
        kws = extractor.get_n_best(n=topn)
        return [k for k, _ in kws]
    except Exception:
        # fallback
        extractor = TextRank()
        extractor.load_document(
            input=doc,
            language="zh",
            normalization=None,
        )
        extractor.candidate_weighting()
        kws = extractor.get_n_best(n=topn)
        return [k for k, _ in kws]


def extract_keywords(texts: list[str], topn: int = KEYWORDS_PER_TOPIC, debug: bool = False) -> list[str]:
    doc = "\n".join(t for t in texts if t and t.strip())
    if not doc.strip():
        return []

    try:
        extractor = TopicRank()
        extractor.load_document(input=doc, language="zh", normalization=None)
        # 先别做 POS 过滤，避免候选被过滤光
        # extractor.candidate_selection(pos={"NOUN", "PROPN", "ADJ"})
        extractor.candidate_weighting()
        kws = extractor.get_n_best(n=topn)
        out = [k for k, _ in kws]
        if debug and not out:
            print(f"[kw] TopicRank empty; doc_len={len(doc)} texts={len(texts)}", flush=True)
        return out
    except Exception as e:
        if debug:
            print("[kw] TopicRank exception:", repr(e), flush=True)

    try:
        extractor = TextRank()
        extractor.load_document(input=doc, language="zh", normalization=None)
        extractor.candidate_weighting()
        kws = extractor.get_n_best(n=topn)
        out = [k for k, _ in kws]
        if debug and not out:
            print(f"[kw] TextRank empty; doc_len={len(doc)} texts={len(texts)}", flush=True)
        return out
    except Exception as e:
        if debug:
            print("[kw] TextRank exception:", repr(e), flush=True)
        return []


# ========== 核心处理函数 ==========
async def process_hour(chat_id: int, stat_date: date, hour: int):
    """
    对某个群的某一天某一小时跑一次 topic
    """
    print(f"▶ Processing chat={chat_id} date={stat_date} hour={hour}", flush=True)

    rows = await PGStatsDB.fetch_hour_texts(chat_id, stat_date, hour)
    if not rows or len(rows) < TOPIC_MIN_SIZE:
        print("  - Skip: not enough texts", flush=True)
        return

    message_ids = [r["message_id"] for r in rows]
    texts = [r["text"] for r in rows]

    # -------- BERTopic --------
    embedder = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    topic_model = BERTopic(
        embedding_model=embedder,
        language="multilingual",
        min_topic_size=TOPIC_MIN_SIZE,
        calculate_probabilities=False,
        verbose=False,
    )

    topics, _ = topic_model.fit_transform(texts)

    # message_id -> topic_id
    msg_topic_map = {
        mid: tid for mid, tid in zip(message_ids, topics) if tid != -1
    }

    # topic_id -> message indices
    topic_to_indices = defaultdict(list)
    for idx, tid in enumerate(topics):
        if tid != -1:
            topic_to_indices[tid].append(idx)

    # -------- 组装结果 --------
    topic_rows = []
    for tid, indices in topic_to_indices.items():
        if len(indices) < TOPIC_MIN_SIZE:
            continue

        topic_texts = [texts[i] for i in indices]
        topic_mids = [message_ids[i] for i in indices]

        # pke_zh 精炼关键词
        
        keywords = extract_keywords(topic_texts, debug=True)
        if not keywords:
            print(f"  - topic {tid}: keywords EMPTY; msg_count={len(indices)} sample={topic_texts[0][:50]!r}", flush=True)
        else:
            print(f"  - topic {tid}: keywords={len(keywords)} top3={keywords[:3]}", flush=True)
        # 代表 message_id（按出现顺序取前 N 条）
        rep_mids = topic_mids[:REP_MSG_PER_TOPIC]

        topic_rows.append({
            "chat_id": chat_id,
            "stat_date": stat_date,
            "hour": hour,
            "topic_id": int(tid),
            "topic_label": f"topic_{tid}",
            "keywords": keywords,
            "message_ids": rep_mids,
            "msg_count": len(indices),
            "model_version": MODEL_VERSION,
        })

    if not topic_rows:
        print("  - No valid topics", flush=True)
        return

    # -------- 写入 PG --------

    await PGStatsDB.upsert_topics_hourly(chat_id, 0, stat_date, hour, topic_rows)
    await PGStatsDB.update_message_topics(
        chat_id=chat_id,
        stat_date=stat_date,
        hour=hour,
        mapping=msg_topic_map,
    )

    print(f"  ✓ topics saved: {len(topic_rows)}", flush=True)


# ========== 主入口 ==========
async def main():
    load_dotenv()

    print("▶ Connecting to PGStatsDB...", flush=True)

    await PGStatsDB.init_pool(PG_DSN, PG_MIN_SIZE, PG_MAX_SIZE)
    print("▶ Connected to PGStatsDB", flush=True)

    await PGStatsDB.ensure_table()

    # ---- 参数读取 ----
    # 支持三种模式：
    # 1) 指定：CHAT_ID=xxx DATE=2025-12-17 HOUR=14
    # 2) 只指定 DATE/HOUR（处理所有群）
    # 3) 不指定：默认跑“上一小时（UTC+8）”
    chat_id = os.getenv("CHAT_ID")
    date_str = os.getenv("DATE")
    hour_str = os.getenv("HOUR")

    if date_str and hour_str:
        stat_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        hour = int(hour_str)
    else:


        tz8 = timezone(timedelta(hours=8))
        now = datetime.now(timezone.utc).astimezone(tz8)
        prev = now - timedelta(hours=1)
        stat_date = prev.date()
        hour = prev.hour

    chat_id = -1002977834325
    hour = 17

    if chat_id:
        chat_ids = [int(chat_id)]
    else:
        # 自动找这一小时有哪些群有数据
        sql = """
        SELECT DISTINCT chat_id
        FROM tg_group_messages_raw
        WHERE stat_date=$1 AND hour=$2
        """
        async with PGStatsDB.pool.acquire() as conn:
            rows = await conn.fetch(sql, stat_date, hour)
            chat_ids = [int(r["chat_id"]) for r in rows]

    print(f"▶ Run for date={stat_date} hour={hour} chats={chat_ids}", flush=True)

    for cid in chat_ids:
        try:
            await process_hour(cid, stat_date, hour)
        except Exception as e:
            print(f"✗ chat {cid} failed: {e}", flush=True)

    await PGStatsDB.close_pool()


print(  "=== Topic Worker Loaded ===", flush=True)

if __name__ == "__main__":
    print("=== Topic Worker Started ===", flush=True)
    asyncio.run(main())
