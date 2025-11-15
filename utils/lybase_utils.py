# transaction_mixin.py
import time

class LYBase:
    @classmethod
    async def transaction_log(cls, transaction_data):
        conn, cur = await cls.get_conn_cursor()
        user_info_row = None

        if(transaction_data['sender_fee']>0):
            transaction_data['sender_fee'] = abs(transaction_data['sender_fee'])*(-1)

        print(f"🔍 处理交易记录: {transaction_data}")

        if transaction_data.get('transaction_description', '') == '':
            return {'ok': '', 'status': 'no_description', 'transaction_data': transaction_data}

        try:
            where_clauses = []
            params = []

            if transaction_data.get('sender_id', '') != '':
                where_clauses.append('sender_id = %s')
                params.append(transaction_data['sender_id'])

            if transaction_data.get('receiver_id', '') != '':
                where_clauses.append('receiver_id = %s')
                params.append(transaction_data['receiver_id'])

            where_clauses.append('transaction_type = %s')
            params.append(transaction_data['transaction_type'])

            where_clauses.append('transaction_description = %s')
            params.append(transaction_data['transaction_description'])

            where_sql = ' AND '.join(where_clauses)

            await cur.execute(f"""
                SELECT transaction_id FROM transaction
                WHERE {where_sql}
                LIMIT 1
            """, params)

            transaction_result = await cur.fetchone()

            if transaction_result and transaction_result.get('transaction_id'):
                return {'ok': '1', 'status': 'exist', 'transaction_data': transaction_result}

            if transaction_data.get('sender_id') == transaction_data.get('receiver_id'):
                return {'ok': '', 'status': 'reward_self', 'transaction_data': transaction_data}

            if transaction_data.get('sender_id', '') != '':
                try:
                    await cur.execute("SELECT * FROM user WHERE user_id = %s LIMIT 1", (transaction_data['sender_id'],))
                    user_info_row = await cur.fetchone()
                except Exception as e:
                    print(f"⚠️ 查询 sender 用户失败: {e}")
                    user_info_row = None



                if not user_info_row or user_info_row['point'] < abs(transaction_data['sender_fee']):
                    return {'ok': '', 'status': 'insufficient_funds', 'transaction_data': transaction_data, 'user_info': user_info_row}
                else:
                    # 扣除 sender point
                    await cur.execute("""
                        UPDATE user
                        SET point = point + %s
                        WHERE user_id = %s
                    """, (transaction_data['sender_fee'], transaction_data['sender_id']))



            if transaction_data.get('receiver_id', '') != '':
                if not await cls.in_block_list(transaction_data['receiver_id']):
                    await cur.execute(
                        "UPDATE user SET point = point + %s WHERE user_id = %s",
                        (transaction_data['receiver_fee'], transaction_data['receiver_id'])
                    )

            transaction_data['transaction_timestamp'] = int(time.time())

            insert_columns = ', '.join(transaction_data.keys())
            insert_placeholders = ', '.join(['%s'] * len(transaction_data))
            insert_values = list(transaction_data.values())

            await cur.execute(f"""
                INSERT INTO transaction ({insert_columns})
                VALUES ({insert_placeholders})
            """, insert_values)

            transaction_id = cur.lastrowid
            transaction_data['transaction_id'] = transaction_id

            return {
                'ok': '1',
                'status': 'insert',
                'transaction_data': transaction_data,
                'user_info': user_info_row
            }

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

