from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def build_pagination_keyboard(keyword_id, page, has_next, has_prev):
    buttons = []
    if has_prev:
        buttons.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"pageid|{keyword_id}|{page - 1}"))
    if has_next:
        buttons.append(InlineKeyboardButton(text="➡️ 下一页", callback_data=f"pageid|{keyword_id}|{page + 1}"))
    return InlineKeyboardMarkup(inline_keyboard=[buttons]) if buttons else None
