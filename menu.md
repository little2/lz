[TOP] 商品
[data]
[handle] handle_product
[kb]

[TOP] 合集
[data] collection
[handle] handle_collection
[kb] collection_menu_keyboard

[1] 合集 - 我的合集
[data] clt_my
[handle] handle_clt_my
[kb] build_collections_keyboard *

    await _edit_caption_or_text(
        photo=lz_var.skins['clt_my']['file_id'],
        msg=callback.message,
        text=text, 
        reply_markup=await build_collections_keyboard(user_id=user_id, page=0, mode="mine")
    )

[2]
[data] clt_favorite
[handle] handle_clt_favorite
[kb] build_collections_keyboard *

[1-1]
[data] clt:create
[handle] handle_clt_create
[kb] clt_create_menu_keyboard


# 合集 Partal > 合集列表 CollectionList > [单一合集页 clt:info] > 显示合集内容 CollectItemList 或 编辑合集 CollectionEdit
[1-2]  合集 - 我的合集 - 指定的合集
[data] clt:my [单一合集页 ]   //f"clt:my:{cid}:0:tk")
[handle] _build_clt_info
[kb] _build_clt_info *显示合集的面板 (显示合集内容/收藏或编辑/返回)
        _build_clt_info_caption
        _build_clt_info_keyboard

# 合集 Partal > 合集列表 CollectionList > 单一合集页 clt:info > [显示合集内容 clti:list] 或 编辑合集 clt:edit
[1-2-1]
    [data] clti:list
    [handle] handle_clti_list
    [kb] 
    _get_clti_list
    _clti_list_keyboard

[1-2-2]
    [date] clt:edit
    [handle] handle_clt_edit 
    _build_clt_caption
    (await callback.message.edit_reply_markup(reply_markup=kb))
    [kb] _build_clt_edit_keyboard

[1-2-2-1]
    [date] clt:edit_title
    [handle] handle_clt_edit_title
    [kb] _build_clt_edit_keyboard

[1-2-2-2]
    [date] clt:edit_desc
    [handle] handle_clt_edit_desc
    [kb] _build_clt_edit_keyboard
//cc:description

[1-2-3]
    [data] clti:add
    [handle] handle_clti_add
    [kb] 
    _get_clti_list
    _clti_list_keyboard

# 合集 Partal > 合集列表 CollectionList > [单一合集页 clt:info] > 显示合集内容 CollectItemList 或 编辑合集 CollectionEdit
[1-3] 合集 - 收藏合集 - 指定的合集
[data] clt:fav [单一合集页 ]
[handle] handle_clt_fav
[kb] _build_clt_info *显示合集的面板 (显示合集内容/收藏或编辑/返回)
        _build_clt_info_caption
        _build_clt_info_keyboard



[data] collection (返回上页)

[data] explore_marketplace

[data] go_home