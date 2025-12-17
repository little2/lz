# topic_worker.py
import os
import asyncio
from datetime import datetime, timedelta, date
from collections import defaultdict

from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

from pke import TopicRank, TextRank
from dotenv import load_dotenv

from pg_stats_db import PGStatsDB


# ========== 配置 ==========
MODEL_VERSION = "bertopic_v1"
TOPIC_MIN_SIZE = 5          # 话题最少消息数
MAX_TOPICS = 20             # 每小时最多保留多少个 topic
REP_MSG_PER_TOPIC = 20      # 每个 topic 选多少条代表 message_id
KEYWORDS_PER_TOPIC = 12     # pke_zh 输出关键词数


# ========== pke_zh 关键词抽取 ==========
def extract_keywords(texts: list[str], topn: int = KEYWORDS_PER_TOPIC) -> list[str]:
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
        extractor.candidate_selection(pos={"NOUN", "PROPN", "ADJ"})
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
        keywords = extract_keywords(topic_texts)

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

    await PGStatsDB.init_pool(
        dsn=os.getenv("PG_DSN"),
        min_size=int(os.getenv("PG_POOL_MIN", 1)),
        max_size=int(os.getenv("PG_POOL_MAX", 5)),
    )
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
        now = datetime.utcnow() + timedelta(hours=8)
        prev = now - timedelta(hours=1)
        stat_date = prev.date()
        hour = prev.hour

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


if __name__ == "__main__":
    asyncio.run(main())
