# # transaction_mixin.py
# import time

# class LYBase:


#     @classmethod
#     async def transaction_log(cls, transaction_data):
      
#         # timer.lap("get_conn_cursor")
#         conn, cur = await cls.get_conn_cursor()
#         # timer.lap("get_conn_cursor-END")
#         # print(f"🔍 处理交易记录: {transaction_data}")

#         user_info_row = None

#         if transaction_data.get('transaction_description', '') == '':
#             return {'ok': '', 'status': 'no_description', 'transaction_data': transaction_data}

        
#         try:
#             # 构造 WHERE 条件
#             where_clauses = []
#             params = []

#             if transaction_data.get('sender_id', '') != '':
#                 where_clauses.append('sender_id = %s')
#                 params.append(transaction_data['sender_id'])

#             if transaction_data.get('receiver_id', '') != '':
#                 where_clauses.append('receiver_id = %s')
#                 params.append(transaction_data['receiver_id'])

#             where_clauses.append('transaction_type = %s')
#             params.append(transaction_data['transaction_type'])

#             where_clauses.append('transaction_description = %s')
#             params.append(transaction_data['transaction_description'])

#             where_sql = ' AND '.join(where_clauses)

#             # 查询是否已有相同记录
#             # timer.lap("查询是否已有相同记录")

#             await cur.execute(f"""
#                 SELECT transaction_id FROM transaction
#                 WHERE {where_sql}
#                 LIMIT 1
#             """, params)

#             # timer.lap("查询是否已有相同记录END")

#             transaction_result = await cur.fetchone()

#             if transaction_result and transaction_result.get('transaction_id'):
#                 return {'ok': '1', 'status': 'exist', 'transaction_data': transaction_result}

#             # 禁止自己打赏自己
#             if transaction_data.get('sender_id') == transaction_data.get('receiver_id'):
#                 return {'ok': '', 'status': 'reward_self', 'transaction_data': transaction_data}

#             # 更新 sender point
#             if transaction_data.get('sender_id', '') != '' and transaction_data.get('sender_fee', 0) != 0:

               
#                 try:
#                     await cur.execute("""
#                         SELECT * 
#                         FROM user 
#                         WHERE user_id = %s
#                         LIMIT 0, 1
#                     """, (transaction_data['sender_id'],))
#                     user_info_row = await cur.fetchone()
#                 except Exception as e:
#                     print(f"⚠️ 数据库执行出错: {e}")
#                     user_info_row = None
            
#                 if not user_info_row or user_info_row['point'] < abs(transaction_data['sender_fee']):
#                     return {'ok': '', 'status': 'insufficient_funds', 'transaction_data': transaction_data, 'user_info': user_info_row}
#                 else:

#                     if transaction_data['sender_fee'] > 0:
#                         transaction_data['sender_fee'] = transaction_data['sender_fee'] * (-1)
#                     # 扣除 sender point
#                     await cur.execute("""
#                         UPDATE user
#                         SET point = point + %s
#                         WHERE user_id = %s
#                     """, (transaction_data['sender_fee'], transaction_data['sender_id']))

               

#             # 更新 receiver point，如果不在 block list
#             if transaction_data.get('receiver_id', '') != '':
#                 if not await cls.in_block_list(transaction_data['receiver_id']):
#                     await cur.execute("""
#                         UPDATE user
#                         SET point = point + %s
#                         WHERE user_id = %s
#                     """, (transaction_data['receiver_fee'], transaction_data['receiver_id']))

#             # 插入 transaction 记录
#             transaction_data['transaction_timestamp'] = int(time.time())

#             insert_columns = ', '.join(transaction_data.keys())
#             insert_placeholders = ', '.join(['%s'] * len(transaction_data))
#             insert_values = list(transaction_data.values())

#             # timer.lap("INSERT")

#             await cur.execute(f"""
#                 INSERT INTO transaction ({insert_columns})
#                 VALUES ({insert_placeholders})
#             """, insert_values)

#             transaction_id = cur.lastrowid
#             transaction_data['transaction_id'] = transaction_id

#             # 可选的 transaction_cache 插入
#             # if transaction_data['transaction_type'] == 'award':
#             #     await cur.execute("""
#             #         INSERT INTO transaction_cache (sender_id, receiver_id, transaction_type, transaction_timestamp)
#             #         VALUES (%s, %s, %s, %s)
#             #     """, (
#             #         transaction_data['sender_id'],
#             #         transaction_data['receiver_id'],
#             #         transaction_data['transaction_type'],
#             #         transaction_data['transaction_timestamp']
#             #     ))

#             return {'ok': '1', 'status': 'insert', 'transaction_data': transaction_data,'user_info': user_info_row}

#         finally:
#             await cls.release(conn, cur)

import lz_var
from lz_config import ENVIRONMENT, UPLOADER_BOT_NAME, PUBLISH_BOT_NAME
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup



import time

class LYBase:

    @classmethod
    async def transaction_log(cls, transaction_data):
        conn, cur = await cls.get_conn_cursor()
        user_info_row = None

        try:
            # ---------- 1) 入参校验与归一化（放进 try，避免泄漏） ----------
            if not isinstance(transaction_data, dict):
                return {"ok": "", "status": "bad_params", "transaction_data": transaction_data}

            desc = (transaction_data.get("transaction_description") or "").strip()
            if not desc:
                return {"ok": "", "status": "no_description", "transaction_data": transaction_data}

            tx_type = (transaction_data.get("transaction_type") or "").strip()
            if not tx_type:
                return {"ok": "", "status": "no_type", "transaction_data": transaction_data}

            sender_id = transaction_data.get("sender_id", "")
            receiver_id = transaction_data.get("receiver_id", "")

            # fee 统一 int（缺省为 0）
            try:
                sender_fee = int(transaction_data.get("sender_fee") or 0)
            except Exception:
                sender_fee = 0

            try:
                receiver_fee = int(transaction_data.get("receiver_fee") or 0)
            except Exception:
                receiver_fee = 0

            # 统一语义：sender_fee <= 0 表示扣款；receiver_fee >= 0 表示入账
            if sender_fee > 0:
                sender_fee = -abs(sender_fee)
            if receiver_fee < 0:
                receiver_fee = abs(receiver_fee)

            transaction_data["transaction_type"] = tx_type
            transaction_data["transaction_description"] = desc
            transaction_data["sender_fee"] = sender_fee
            transaction_data["receiver_fee"] = receiver_fee

            # 禁止自己打赏自己
            if sender_id != "" and sender_id == receiver_id:
                return {"ok": "", "status": "reward_self", "transaction_data": transaction_data}

            # ---------- 2) 开启事务（关键：保证一致性） ----------
            await conn.begin()

            # ---------- 3) 幂等查重（事务内 + FOR UPDATE 降并发重复） ----------
            where_clauses = []
            params = []

            if sender_id != "":
                where_clauses.append("sender_id = %s")
                params.append(sender_id)

            if receiver_id != "":
                where_clauses.append("receiver_id = %s")
                params.append(receiver_id)

            where_clauses.append("transaction_type = %s")
            params.append(tx_type)

            where_clauses.append("transaction_description = %s")
            params.append(desc)

            where_sql = " AND ".join(where_clauses)

            await cur.execute(
                f"""
                SELECT transaction_id
                FROM transaction
                WHERE {where_sql}
                LIMIT 1
                FOR UPDATE
                """,
                params,
            )
            exist_row = await cur.fetchone()
            if exist_row and exist_row.get("transaction_id"):
                await conn.rollback()
                return {"ok": "1", "status": "exist", "transaction_data": exist_row}

            # ---------- 4) 扣 sender（并发安全：锁定该用户行） ----------
            if sender_id != "" and sender_fee != 0:
                # 锁行读取余额（避免并发双花）
                await cur.execute(
                    "SELECT point, credit FROM user WHERE user_id=%s LIMIT 1 FOR UPDATE",
                    (sender_id,),
                )
                user_info_row = await cur.fetchone()

                need = abs(sender_fee)
                if (not user_info_row) or int(user_info_row.get("point") or 0) < need:
                    await conn.rollback()
                    return {
                        "ok": "",
                        "status": "insufficient_funds",
                        "transaction_data": transaction_data,
                        "user_info": user_info_row,
                    }

                await cur.execute(
                    """
                    UPDATE user
                    SET point = point + %s
                    WHERE user_id = %s
                    """,
                    (sender_fee, sender_id),
                )

                # 更新后的 sender 信息（给上层提示用）
                await cur.execute(
                    "SELECT point, credit FROM user WHERE user_id=%s LIMIT 1",
                    (sender_id,),
                )
                user_info_row = await cur.fetchone()

            # ---------- 5) 入账 receiver（同事务） ----------
            if receiver_id != "" and receiver_fee != 0:
                if not await cls.in_block_list(receiver_id):
                    await cur.execute(
                        """
                        UPDATE user
                        SET point = point + %s
                        WHERE user_id = %s
                        """,
                        (receiver_fee, receiver_id),
                    )

            # ---------- 6) 写 transaction ----------
            transaction_data["transaction_timestamp"] = int(time.time())

            insert_columns = ", ".join(transaction_data.keys())
            insert_placeholders = ", ".join(["%s"] * len(transaction_data))
            insert_values = list(transaction_data.values())

            await cur.execute(
                f"""
                INSERT INTO transaction ({insert_columns})
                VALUES ({insert_placeholders})
                """,
                insert_values,
            )
            transaction_data["transaction_id"] = cur.lastrowid

            # ---------- 7) 提交 ----------
            await conn.commit()

            return {"ok": "1", "status": "insert", "transaction_data": transaction_data, "user_info": user_info_row}

        except Exception as e:
            # 任何异常都回滚，避免半套账
            try:
                await conn.rollback()
            except Exception:
                pass
            return {"ok": "", "status": "error", "error": str(e), "transaction_data": transaction_data}

        finally:
            await cls.release(conn, cur)


    @classmethod
    async def in_block_list(cls, user_id):
        return False
    
    @classmethod
    async def media_auto_send(cls, data: dict):
        conn, cur = await cls.get_conn_cursor()
        try:
            if not data:
                return {'ok': '', 'status': 'empty_data'}

            # 拼字段与占位符
            insert_columns = ', '.join([f"`{key}`" for key in data.keys()])
            placeholders = ', '.join(['%s'] * len(data))
            insert_values = list(data.values())

            # 拼 SQL
            sql = f"""
                INSERT INTO `media_auto_send` ({insert_columns})
                VALUES ({placeholders})
            """

            await cur.execute(sql, insert_values)
            insert_id = cur.lastrowid

            return {
                'ok': '1',
                'status': 'insert',
                'media_auto_send_id': insert_id,
                'data': data
            }

        except Exception as e:
            return {'ok': '', 'status': 'error', 'error': str(e)}
        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_credit_score(cls, user_id, score_change):
        conn, cur = await cls.get_conn_cursor()
        try:
            sql = """
                UPDATE `user`
                SET credit = credit + %s
                WHERE user_id = %s
            """
            await cur.execute(sql, [score_change, user_id])

            if cur.rowcount == 0:
                return {'ok': '', 'status': 'not_found', 'user_id': user_id}

            return {'ok': '1', 'status': 'updated', 'user_id': user_id, 'change': score_change}

        except Exception as e:
            return {'ok': '', 'status': 'error', 'error': str(e)}

        finally:
            await cls.release(conn, cur)

    @classmethod
    async def update_today_contribute(cls, user_id: int, contribute: int = 1):
        """
        更新用户今日发言贡献数:
        - 如果不存在记录则插入
        - 如果存在记录则 count + 1 并更新 update_timestamp
        """
        conn, cur = await cls.get_conn_cursor()
        try:
            from datetime import datetime
            import time

            stat_date = datetime.now().strftime("%Y-%m-%d")
            now = int(time.time())

            sql = """
                INSERT INTO `contribute_today` (`user_id`, `stat_date`, `count`, `update_timestamp`)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    `count` = `count` + %s,
                    `update_timestamp` = VALUES(`update_timestamp`)
            """

            params = [user_id, stat_date, contribute, now, contribute]
            await cur.execute(sql, params)
            await conn.commit()

            return {
                "ok": "1",
                "status": "inserted_or_updated",
                "user_id": user_id,
                "stat_date": stat_date,
                "timestamp": now,
            }

        except Exception as e:
            return {"ok": "", "status": "error", "error": str(e)}

        finally:
            await cls.release(conn, cur)


    @classmethod
    async def show_main_menu(cls, message):
        current_message = await message.answer_photo(
                photo=lz_var.skins['home']['file_id'],
                caption="👋 欢迎使用 LZ 机器人！请选择操作：",
                parse_mode="HTML",
                reply_markup=cls.main_menu_keyboard()
        )   
        return current_message


    @classmethod
    def main_menu_keyboard(cls):
        keyboard = [
            [
                InlineKeyboardButton(text="🔍 搜索", url=f"https://t.me/{lz_var.publish_bot_name}?start=search", callback_data="search"),
                InlineKeyboardButton(text="🏆 排行", url=f"https://t.me/{lz_var.publish_bot_name}?start=rank",callback_data="ranking"),
            ],
        ]

        # 仅在 dev 环境显示「资源橱窗」 PUBLISH_BOT_TOKEN
        if ENVIRONMENT == "dev":
            keyboard.append([
                InlineKeyboardButton(text="🪟 资源橱窗", url=f"https://t.me/{lz_var.publish_bot_name}?start=collection",callback_data="collection"),
                InlineKeyboardButton(text="🕑 我的历史", url=f"https://t.me/{lz_var.publish_bot_name}?start=history", callback_data="my_history"),
            ])
        else:
            keyboard.append([
                InlineKeyboardButton(text="🕑 我的历史", url=f"https://t.me/{lz_var.publish_bot_name}?start=history", callback_data="my_history"),
            ])

        keyboard.append([
            InlineKeyboardButton(
                text="📤 上传资源",
                url=f"https://t.me/{UPLOADER_BOT_NAME}?start=upload"
            )
        ])

        keyboard.append([
            InlineKeyboardButton(
                text="🐲 小龙阳",
                url=f"https://t.me/{lz_var.guider_bot_name}?start=map"
            )
        ])



        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
   


''''''