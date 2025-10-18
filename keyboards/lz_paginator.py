from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
def build_pagination_keyboard(keyword_id: int, page: int, has_next: bool, has_prev: bool,
                              callback_function: str | None = "pageid") -> InlineKeyboardMarkup:
    """
    分页键盘（两行）
    第一行：上一页 / 下一页
    第二行：返回、刷新
    """
    keyboard: list[list[InlineKeyboardButton]] = []

    # 第一行：分页按钮
    page_buttons: list[InlineKeyboardButton] = []
    if has_prev:
        page_buttons.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"{callback_function}|{keyword_id}|{page - 1}"))
    if has_next:
        page_buttons.append(InlineKeyboardButton(text="➡️ 下一页", callback_data=f"{callback_function}|{keyword_id}|{page + 1}"))
    if page_buttons:
        keyboard.append(page_buttons)

    # 第二行：自定义按钮（随意扩展）
    if callback_function in {"ul_pid", "fd_pid"}:
        page_buttons: list[InlineKeyboardButton] = []
        page_buttons.append(InlineKeyboardButton(text="🔙 返回我的历史", callback_data=f"my_history"))
        keyboard.append(page_buttons)

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


