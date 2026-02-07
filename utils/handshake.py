# utils/media_utils.py

from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact,InputMediaContact

import lz_var
from lz_config import KEY_USER_ID,KEY_USER_PHONE

class HandshakeUtils:
    @classmethod
    async def handshake(cls,bot_username:str | None = None):
        try:
            contact1 = InputPhoneContact(client_id=0, phone=KEY_USER_PHONE, first_name="KeyMan", last_name="")
            await lz_var.user_client(ImportContactsRequest([contact1]))
            target = await lz_var.user_client.get_entity(KEY_USER_ID)     # 7550420493
            me = await lz_var.user_client.get_me()
            await lz_var.user_client.send_message(target, f"[HANDSHAKE] <code>{me.id}</code> {bot_username} - {me.first_name} {me.last_name or ''} {me.phone or ''}。",parse_mode='html')   
            print(f"发送消息给 KeyMan 成功。",flush=True)
        except Exception as e:
            print(f"发送消息给 KeyMan 失败：{e}",flush=True)

        try:
            if bot_username is None:
                bot_username = lz_var.bot_username

        
            contact2 = InputPhoneContact(client_id=0, phone=lz_var.x_man_bot_phone, first_name="KeyMan", last_name="")
            await lz_var.user_client(ImportContactsRequest([contact2]))
            target = await lz_var.user_client.get_entity(lz_var.x_man_bot_username)     
            await lz_var.user_client.send_message(target, f"|_kick_|{bot_username}")   
            


            await lz_var.user_client.send_message(
                target,   # 必须是已解析的 entity（不是裸 user_id）
                file=InputMediaContact(
                    phone_number=me.phone,                      # ⚠️ 必须有手机号
                    first_name=me.first_name or "LZ",
                    last_name=me.last_name or "",
                    vcard=""  # 可选 vCard 信息
                )
            )   
            

            print(f"发送消息给 x-man 成功。",flush=True)
        except Exception as e:
            print(f"发送消息给 x-man 失败：{e}",flush=True)
        

''''''