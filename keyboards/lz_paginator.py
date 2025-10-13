from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def build_pagination_keyboard(keyword_id, page, has_next, has_prev, callback_function="pageid"):
    buttons = []
    if has_prev:
        buttons.append(InlineKeyboardButton(text="⬅️ 上一页X{callback_function}", callback_data=f"{callback_function}|{keyword_id}|{page - 1}"))
    if has_next:
        buttons.append(InlineKeyboardButton(text="➡️ 下一页", callback_data=f"{callback_function}|{keyword_id}|{page + 1}"))
    return InlineKeyboardMarkup(inline_keyboard=[buttons]) if buttons else None
