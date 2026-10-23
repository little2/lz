from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.enums import ChatType
from utils.string_utils import LZString
from lz_db import db
from lz_pgsql import PGPool
import lz_var

from utils.aes_crypto import AESCrypto
from lz_config import AES_KEY, ENVIRONMENT, RESULTS_PER_PAGE

router = Router()


def render_results_highlighted(results: list[dict], page: int, total: int, per_page: int = 10) -> str:
    total_pages = (total + per_page - 1) // per_page  # 向上取整
    lines = [f"<b>📄 第 {page + 1}/{total_pages} 页（共 {total} 项）</b>\n"]
    for r in results:
        content = r['highlighted_content']
        if len(content) > 300:
            content = content[:300] + "..."


        lines.append(
            f"<b>[{r['id']:07d}]</b>\n"
            f"<b>Type:</b> {r['file_type']}\n"
            f"<b>Source:</b> {r['source_id']}\n"
            f"<b>内容:</b> {r['highlighted_content']}"
        )
    return "\n\n".join(lines)

async def render_results_plain(results: list[dict], keyword: str, page: int, total: int, per_page: int = 10) -> str:
    total_pages = (total + per_page - 1) // per_page

    lines = [
        f"<b>🔍 关键词：</b> <code>{keyword}</code>\r\n"
    ]

    keyword_id = await PGPool.get_search_keyword_id(keyword)
    # lines = [f"<b>📄 第 {page + 1}/{total_pages} 页（共 {total} 项）</b>\n"]

    for r in results:
        # print(r)
        content = LZString.shorten_text(r["content"])
        # 根据 r['file_type'] 进行不同的处理
        if r['file_type'] == 'v':
            icon = "🎬"
        elif r['file_type'] == 'd':
            icon = "📄"
        elif r['file_type'] == 'p':
            icon = "🖼"
        else:
            icon = "🔹"


        aes = AESCrypto(AES_KEY)
        encoded = aes.aes_encode(r['id'])

        

        lines.append(
            f"{icon}<a href='https://t.me/{lz_var.bot_username}?start=f_{keyword_id}_{encoded}'>{content}</a>"
            # f"<b>Type:</b> {r['file_type']}\n"
            # f"<b>Source:</b> {r['source_id']}\n"
            # f"<b>内容:</b> {content}"
        )

    

    # 页码信息放到最后
    lines.append(f"\n<b>📃 第 {page + 1}/{total_pages} 页（共 {total} 项）</b>")


    return "\n".join(lines)  # ✅ 强制变成纯文字



@router.message(Command("sss"))
async def handle_search(message: Message):
    if (getattr(message.chat, "type", None) not in {ChatType.GROUP, ChatType.SUPERGROUP}) and ENVIRONMENT != "dev":
        await message.reply("⚠️ 此指令只能在群組中使用。")
        return

    if ENVIRONMENT != "dev":
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("请输入关键词： /s 正太 钢琴")
        return

    keyword = parts[1]

    await PGPool.insert_search_log(message.from_user.id, keyword)
    await PGPool.upsert_search_keyword_stat(keyword)

    result = await db.search_keyword_page_plain(keyword)
    if not result:
        await message.reply("没有找到任何结果")
        return

    page = 0
    sliced = result[0:RESULTS_PER_PAGE]
    has_next = RESULTS_PER_PAGE < len(result)

    text = await render_results_plain(sliced, keyword, page, total=len(result), per_page=RESULTS_PER_PAGE)

    keyword_id = await PGPool.get_search_keyword_id(keyword)

    await message.reply(
        text, parse_mode=ParseMode.HTML,
        # reply_markup = build_pagination_keyboard(keyword, page, has_next, has_prev=False)
        reply_markup = build_pagination_keyboard(keyword_id, page, has_next, has_prev=False)
    )




@router.callback_query(F.data.startswith("spageid|"))
async def handle_pagination(callback: CallbackQuery):
    _, keyword_id_str, page_str = callback.data.split("|")
    keyword_id = int(keyword_id_str) 
    page = int(page_str)

    # 用 keyword_id 查回 keyword 文本
    keyword = await PGPool.get_keyword_by_id(keyword_id)
    if not keyword:
        await callback.answer("⚠️ 无法找到对应关键词", show_alert=True)
        return


    result = await db.search_keyword_page_plain(keyword)

    start = page * RESULTS_PER_PAGE
    end = start + RESULTS_PER_PAGE
    sliced = result[start:end]
    has_next = end < len(result)
    has_prev = page > 0
    
    text = await render_results_plain(sliced, keyword, page, total=len(result), per_page=RESULTS_PER_PAGE)

    await callback.message.edit_text(
        text=text, parse_mode=ParseMode.HTML,
        reply_markup=build_pagination_keyboard(keyword_id, page, has_next, has_prev)
    )
    await callback.answer()
