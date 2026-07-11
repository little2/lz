import asyncio
from datetime import datetime

from telethon import TelegramClient, utils
from telethon.errors.rpcerrorlist import ChatAdminRequiredError, FloodWaitError, UserAlreadyParticipantError, UserPrivacyRestrictedError
from telethon.tl.functions.channels import CreateChannelRequest, EditAdminRequest, GetParticipantRequest, InviteToChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator, ChatAdminRights, InputUser
import textwrap



class HarryClass:
	def __init__(self, client: TelegramClient, bot_client: TelegramClient, group_bots: list[int | str] | None = None) -> None:
		self.client = client
		self.bot_client = bot_client
		self.group_bots = group_bots or []

	@staticmethod
	def anonymous_admin_rights() -> ChatAdminRights:
		return ChatAdminRights(
			change_info=True,
			invite_users=True,
			ban_users=True,
			add_admins=True,
			post_messages=True,
			edit_messages=True,
			delete_messages=True,
			pin_messages=True,
			manage_call=True,
			anonymous=True,
			manage_topics=True,
			post_stories=True,
			edit_stories=True,
			delete_stories=True,
			other=True,
			manage_direct_messages=True,
		)

	@staticmethod
	def normalize_username(value: str) -> str:
		item = value.strip()
		for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
			if item.startswith(prefix):
				item = item[len(prefix):]
				break
		return item.split("?", 1)[0].strip("/")

	async def resolve_group_bot(self, bot: int | str):
		if isinstance(bot, int):
			raise ValueError("cannot invite by bare user_id; use @username or user_id:access_hash")

		item = self.normalize_username(bot)
		if ":" in item:
			user_id, access_hash = item.split(":", 1)
			if user_id.strip().lstrip("-").isdigit() and access_hash.strip().lstrip("-").isdigit():
				return InputUser(int(user_id), int(access_hash))

		if item.lstrip("-").isdigit():
			raise ValueError("cannot invite by bare user_id; use @username or user_id:access_hash")

		return await self.client.get_entity(item)

	async def batch_create_group(self) -> list[str]:
		results: list[str] = []
		for i in range(5):
			try:
				print(f"[harry] creating test groups (megagroup) attempt {i + 1}/5", flush=True)
				results.append(await self.create_group(megagroup=True))
				await asyncio.sleep(3)
				print(f"[harry] creating test groups (forum) attempt {i + 1}/5", flush=True)
				results.append(await self.create_group(forum=True))
				await asyncio.sleep(3)
			except Exception as exc:
				results.append(f"batch create failed on attempt {i + 1}: {exc}")
				await asyncio.sleep(1)
		return results

	async def create_group(self, megagroup: bool = False, forum: bool = False, broadcast: bool = False) -> str:
		if megagroup:
			group_title = "GROUP (megagroup)"
		elif forum:
			group_title = "Forum (forum)"
		elif broadcast:
			group_title = "Channel (broadcast)"
		else:
			group_title = "GROUP"

		try:
			result = await self.client(
				CreateChannelRequest(
					title=group_title,
					about="for Harry",
					megagroup=megagroup,
					forum=forum,
					broadcast=broadcast,
				)
			)
		except FloodWaitError as exc:
			wait_seconds = int(exc.seconds)
			wait_minutes = wait_seconds // 60
			return f"create failed: Telegram flood wait, retry after {wait_seconds} seconds ({wait_minutes} minutes)"

		group = result.chats[0]
		group_id = int(f"-100{group.id}")
		self_admin_result = "self: skipped"
		bot_results: list[str] = []
		book_result = "/book: skipped"
		time_result = "time message: skipped"

		try:
			me = await self.client.get_me()
			await self.client(
				EditAdminRequest(
					group,
					utils.get_input_user(me),
					self.anonymous_admin_rights(),
					rank="owner",
				)
			)
			self_admin_result = f"self: anonymous admin enabled ({me.id})"
		except Exception as exc:
			self_admin_result = f"self: failed: {exc}"

		for bot in self.group_bots:
			try:
				entity = await self.resolve_group_bot(bot)
				await self.client(
					EditAdminRequest(
						group,
						entity,
						self.anonymous_admin_rights(),
						rank="bot",
					)
				)
				bot_results.append(f"{bot}: invited as admin")
			except Exception as exc:
				bot_results.append(f"{bot}: failed: {exc}")

		try:
			await self.client.send_message(group, "/book")
			book_result = "/book: sent"
		except Exception as exc:
			book_result = f"/book: failed: {exc}"

		try:
			current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			await self.client.send_message(group, f"current time: {current_time}")
			time_result = f"time message: sent ({current_time})"
		except Exception as exc:
			time_result = f"time message: failed: {exc}"

		try:
			invite = await self.client(ExportChatInviteRequest(group))
			invite_link = getattr(invite, "link", "")
		except Exception as exc:
			invite_link = f"invite link failed: {exc}"

		bots_text = "\n".join(bot_results) if bot_results else "no bots configured"
		return (
			f"group created: {group.title}\n"
			f"id: {group_id}\n"
			f"invite: {invite_link}\n"
			f"admin:\n{self_admin_result}\n"
			f"bots:\n{bots_text}\n"
			f"command:\n{book_result}\n{time_result}"
		)



	async def grant_permissions(self, chat_id: int, user_id: int, nonanonymous: bool = False) -> None:
		"""
		Grant all permissions to a user in a chat.
		"""
		try:
			# target = await resolve_user_from_chat(
			# 	bot_client,
			# 	chat,
			# 	7270553727
			# )

			print(f"{chat_id}")
			try:
				chat = await self.bot_client.get_entity(chat_id)
			except Exception as e:
				print(f"❌ 取得群组失败 chat_id={chat_id}: {e}", flush=True)
				return False



			if not getattr(chat, "broadcast", False) and not getattr(chat, "megagroup", False):
				print(
					f"‼️grant_permissions skipped: chat {chat_id} is not a channel or supergroup",
					flush=True,
				)
				return

			user = await self.bot_client.get_entity(user_id)
			input_user = await self.bot_client.get_input_entity(user)

		except Exception as exc:
			print(f"‼️  Failed to resolve user {user_id} or chat {chat_id}: {exc}", flush=True)
			return False
		try:
			bot_me = await self.bot_client.get_me()
			result = await self.bot_client(GetParticipantRequest(
				channel=chat,
				participant=bot_me
			))

			participant = result.participant
			rights = participant.admin_rights

			print(f"{rights}")

			if nonanonymous:
				participant.admin_rights.anonymous = False
			await self.bot_client(EditAdminRequest(
				channel=chat,
				user_id=input_user,
				admin_rights=rights,
				rank="ㅤ"
			))



			print(f" Granted permissions to user {user_id} in chat {chat_id}", flush=True)
		except Exception as exc:
			print(f"‼️  Failed to grant permissions to user {user_id} in chat {chat_id}: {exc}", flush=True)
			return False



	async def grant_permissions_by_man(self, chat_id: int, bot_name: int | str, nonanonymous: bool = False) -> None:

		me = await self.client.get_me()
		result = await self.client(GetParticipantRequest(
			channel=chat_id,
			participant=me
		))

		participant = result.participant
		rights = participant.admin_rights


		
	
		await self.client(EditAdminRequest(
			channel=chat_id,
			user_id=bot_name,
			admin_rights=rights,
			rank="ㅤ"
		))

	async def revoke_permissions(self, chat_id: int, user_id: int):
		"""
		Revoke all permissions from a user in a chat.
		"""
		try:
			rights = ChatAdminRights(
				change_info=False,
				invite_users=False,
				ban_users=False,
				delete_messages=False,
				pin_messages=False,
				add_admins=False,
				anonymous=False,
				manage_call=False,
				manage_topics=False,
			)

			chat = await self.bot_client.get_entity(chat_id)
			user = await self.bot_client.get_entity(user_id)
			input_user = await self.bot_client.get_input_entity(user)

			await self.bot_client(EditAdminRequest(
				channel=chat,
				user_id=input_user,
				admin_rights=rights,
				rank=""
			))

			print(f"[harry] Revoked permissions from user {user_id} in chat {chat_id}", flush=True)
		except Exception as exc:
			print(f"[harry] Failed to revoke permissions from user {user_id} in chat {chat_id}: {exc}", flush=True)


	async def show_my_bot_rights(self,chat_id: int):
		chat = await self.bot_client.get_entity(chat_id)
		if not getattr(chat, "broadcast", False) and not getattr(chat, "megagroup", False):
			print(
				f"[harry] show_my_bot_rights skipped: {chat_id} is not a channel or supergroup",
				flush=True,
			)
			return
		me = await self.bot_client.get_me()

		result = await self.bot_client(GetParticipantRequest(
			channel=chat,
			participant=me
		))

		participant = result.participant

		print("chat title:", getattr(chat, "title", None))
		print("chat id:", chat_id)
		print("bot id:", me.id)
		print("bot username:", me.username)
		print("participant type:", type(participant).__name__)

		if isinstance(participant, ChannelParticipantCreator):
			print("身份：群主 / 頻道擁有者")
			print("權限：全部權限")
			return

		if not isinstance(participant, ChannelParticipantAdmin):
			print("身份：普通成員")
			print("權限：沒有 admin_rights")
			return

		rights = participant.admin_rights
		rights_dict = rights.to_dict()

		print("身份：管理員")
		print("admin_rights:")

		for key, value in rights_dict.items():
			if key == "_":
				continue

			print(f"  {key}: {value}")

	async def invite_to_group(self,chat_id: int, bot_username: str):
		chat = await self.client.get_entity(chat_id)
		
		input_user = await self.client.get_input_entity(bot_username)


		try:
			await self.client(InviteToChannelRequest(
				channel=chat,
				users=[input_user]
			))

			print(f"{bot_username} 已成功邀請入群")

		except UserAlreadyParticipantError:
			print(f"{bot_username} 已經在群裡")

		except ChatAdminRequiredError:
			print("邀請失敗：人類帳號沒有邀請成員的權限")

		except UserPrivacyRestrictedError:
			print("邀請失敗：目標 bot 或帳號受到隱私限制")

		except FloodWaitError as e:
			print(f"觸發 FloodWait，需等待 {e.seconds} 秒")

		except Exception as e:
			print(f"邀請失敗：{type(e).__name__}: {e}")


	async def set_chat(self, params: list[str], board_info: dict) -> str | None:
		if not params:
			print("[harry] set_chat called with empty params; skipped", flush=True)
			return None

		chat_type = params[0].lower()
	
		print(f"[harry] set_chat called with chat_type={chat_type}", flush=True)
		if chat_type == "school":
			print(f"[harry] set_chat: setting to school {chat_type}", flush=True)
			return await self.set_chat_school(board_info)
		elif chat_type == "public":
			return await self.set_chat_public(board_info)
		elif chat_type == "public_bk":
			return await self.set_chat_public(board_info, mode='public_bk')
		
		elif chat_type == "oldfriend":
			return await self.set_chat_old_friend(board_info)
		elif chat_type == "jwc":
			return await self.set_chat_jwc(board_info)
		elif chat_type == "tr_rw":
			return await self.set_chat_tr_rw(board_info)
		elif chat_type == "subscribe_preview":
			return await self.set_chat_subscribe_preview(board_info)	
		elif chat_type == "xiaodi_topic":
			return await self.set_chat_xiaodi_topic(board_info)
		elif chat_type == "ltgphoto":
			return await self.set_chat_ltgphoto(board_info)
		elif chat_type == "ltgphoto_review":
			return await self.set_chat_ltgphoto_review(board_info)		
		try:
			await self.client.send_message("me", f"/setchat {chat_type}")
			print(f"[harry] set_chat: sent /setchat {chat_type}", flush=True)
		except Exception as exc:
			print(f"[harry] set_chat failed: {exc}", flush=True)
		return None

	async def set_chat_school(self, board_info: dict) -> str:
		"""
		设置群组为学院群组
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>学院重建 基础设置</blockquote>
			1️⃣ 新增主题式群组
			2️⃣ 将核心机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 在群组中，发送 <code>!setchat school</code> 指令 
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入小龙阳机器人, 本子机器人, 技术小助手, 龙阳宝机器人, 宸宸机器人, 幻弟机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/bootstrap</code> 指令，一定会收到"完成"的提示，否则就再下一指令
			8️⃣ 更换群头像,置顶公告	
			9️⃣ 完成				 				                     
		""").strip()
		print(f"[harry] set_chat_school: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			print(f"5 set_chat_school: granting permissions to @lykeyman", flush=True)
			man_me = await self.client.get_me()
			await self.invite_to_group(chat_id, man_me.id)
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id)

			print(f"[harry] set_chat_school: granting permissions to @lykeyman", flush=True)
			# 6️⃣ 拉入小龙阳机器人, 本子机器人, 龙阳宝机器人, 宸宸机器人, 幻弟机器人给予所有的权限
			await self.invite_to_group(chat_id, "xiaolongyang007bot")
			await self.invite_to_group(chat_id, "exshota_ly_bot")
			await self.invite_to_group(chat_id, "longyangbaobot")
			await self.invite_to_group(chat_id, "chenchen808bot")
			await self.invite_to_group(chat_id, "dreamdidibot")

			await self.grant_permissions(chat_id=chat_id, user_id="xiaolongyang007bot")
			await self.grant_permissions(chat_id=chat_id, user_id="exshota_ly_bot")
			await self.grant_permissions(chat_id=chat_id, user_id="longyangbaobot")
			await self.grant_permissions(chat_id=chat_id, user_id="chenchen808bot")
			await self.grant_permissions(chat_id=chat_id, user_id="dreamdidibot")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			await self.client.send_message(chat_id, "/bootstrap")

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_school: failed to grant permissions or invite bots: {exc}", flush=True)
		




		return sop_text


	async def set_chat_public(self, board_info: dict, mode='public') -> str:
		"""
		设置群组为公开群备份
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>公开群备份重建 基础设置</blockquote>
			1️⃣ 新增一般群组
			2️⃣ 将核心机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 在群组中，发送 <code>!setchat public_bk</code> 指令 
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入小龙阳机器人, 萨莱机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/setup {mode}</code> 指令
			8️⃣ 设置萨莱 (GroupHelp) - 删除消息(指令/服务消息),进群欢迎力,其它(重发消息)
			9️⃣ 更换群头像,置顶公告,完成				 				                     
		""").strip()
		print(f"[harry] set_chat_{mode}: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			print(f"5 set_chat_{mode}: granting permissions to @lykeyman", flush=True)
			man_me = await self.client.get_me()
			await self.invite_to_group(chat_id, man_me.id)
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id)

			print(f"[harry] set_chat_{mode}: granting permissions to @lykeyman", flush=True)
			# 6️⃣ 拉入小龙阳机器人, 萨莱机器人给予所有的权限

			await self.grant_permissions(chat_id=chat_id, user_id="xiaolongyang007bot")
			await self.grant_permissions(chat_id=chat_id, user_id="salai009bot")

			await self.client.send_message(chat_id, f"/setup {mode}")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_{mode}: failed to grant permissions or invite bots: {exc}", flush=True)
		




		return sop_text


	async def set_chat_jwc(self, board_info: dict) -> str:
		"""
		设置群组为教务处
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>教务处重建 基础设置</blockquote>
			1️⃣ 新增主题式群组
			2️⃣ 将核心机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 将教务处机器人 token 向 @ModularBot 注册
			5️⃣ 拉入教务处机器人给予所有的权限
			6️⃣ 启用群组话题模式
			7️⃣ 在群组中，发送 <code>/bootstrap</code> 指令，一定会收到"完成"的提示，否则就再下一指令
			8️⃣ 更换群头像,置顶公告	
			9️⃣ 完成				 				                     
		""").strip()
		print(f"[harry] set_chat_jwc: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)

			print(f"[harry] set_chat_jwc: granting permissions to @lykeyman", flush=True)
			# 6️⃣ 拉入小龙阳机器人, 本子机器人, 龙阳宝机器人, 宸宸机器人, 幻弟机器人给予所有的权限
			await self.invite_to_group(chat_id, "ly120bot")
			await self.grant_permissions(chat_id=chat_id, user_id="ly120bot")
			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_jwc: failed to grant permissions or invite bots: {exc}", flush=True)
		




		return sop_text


	async def set_chat_tr_rw(self, board_info: dict) -> str:
		"""
		设置群组为资源审核群
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>资源审核群重建 基础设置</blockquote>
			1️⃣ 新增主题式群组
			2️⃣ 将核心机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 在群组中，发送 <code>!setchat tr_rw</code> 指令 
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入 @noexists666bot 给予所有的权限
			7️⃣ 在群组中，发送 <code>/setup_tr_rw</code> 指令
			8️⃣ 到鲁仔三号私信，发送 <code>/update_setting</code> 更新审核群信息
			9️⃣ 完成				 				                     
		""").strip()
		print(f"[harry] set_chat_report_rw: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)

			print(f"[harry] set_chat_report_rw: granting permissions to @lykeyman", flush=True)
			# 6️⃣ 拉入小龙阳机器人, 本子机器人, 龙阳宝机器人, 宸宸机器人, 幻弟机器人给予所有的权限
			
			await self.invite_to_group(chat_id, "noexists666bot")
			await self.grant_permissions(chat_id=chat_id, user_id="noexists666bot")
		
			await self.invite_to_group(chat_id, "luzai33003bot")
			await self.grant_permissions(chat_id=chat_id, user_id="luzai33003bot")

			await self.client.send_message(chat_id, "/setup_tr_rw")

			await self.client.send_message("luzai33003bot", "/update_setting")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_tr_rw: failed to grant permissions or invite bots: {exc}", flush=True)
		




		return sop_text

	async def set_chat_old_friend(self, board_info: dict) -> str:
		"""
		设置群组为老铁群
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>老铁群重建 基础设置</blockquote>
			1️⃣ 新增主题式群组
			2️⃣ 将核心一号机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 在群组中，发送 <code>!setchat old_friend</code> 指令 
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入小班弟弟/核心二号机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/setup oldfriend_ask</code> 指令
			8️⃣ 
			9️⃣ 完成				 				                     
		""").strip()
		print(f"[harry] set_chat_old_friend: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)

			print(f"[harry] set_chat_old_friend: granting permissions to @lykeyman", flush=True)

			# 6️⃣ 拉入小班弟弟/核心二号机器人给予所有的权限
			
			await self.invite_to_group(chat_id, "noexists666bot")
			await self.grant_permissions(chat_id=chat_id, user_id="noexists666bot")
		
			await self.invite_to_group(chat_id, "ztd7bot")
			await self.grant_permissions(chat_id=chat_id, user_id="ztd7bot")

			await self.client.send_message(chat_id, "/setup")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_old_friend: failed to grant permissions or invite bots: {exc}", flush=True)
		




		return sop_text

	async def set_chat_subscribe_preview(self, board_info: dict) -> str:
		"""
		设置群组为订阅预览频道
		"""
		
		chat_id = board_info["chat_id"]
		
		sop_text = textwrap.dedent("""
			<blockquote>订阅预览频道 基础设置</blockquote>
			1️⃣ 新增私密频道
			2️⃣ 将核心一号机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>机器人指令</blockquote>
			4️⃣ 在机器人私信中，发送 <code>!setchat subscribe_preview -100[群ID]</code> 指令
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入鲁仔四号(内容)/小龙阳(入群审核)/核心二号机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/setchat subscribe_preview_ask</code> 指令 (待完成)
			8️⃣ 到鲁仔四号私信，发送 <code>/update_setting</code> 更新预览群信息 
			9️⃣ 完成				 				                     
		""").strip()
		# print(f"[harry] subscribe_preview: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)

			
			# 6️⃣ 拉入鲁仔四号(内容)/小龙阳(入群审核)/核心二号机器人给予所有的权限
			
			
			await self.grant_permissions_by_man(chat_id=chat_id, bot_name="luzai4001bot")


		
			
			await self.grant_permissions_by_man(chat_id=chat_id, bot_name="xiaolongyang007bot")

			
			await self.grant_permissions_by_man(chat_id=chat_id, bot_name="noexists666bot")

			await self.client.send_message(chat_id, "/setup")

			await self.client.send_message("luzai33003bot", "/update_setting")


			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])
		except Exception as exc:
			print(f"[harry] subscribe_preview: failed to grant permissions or invite bots: {exc}", flush=True)
		return sop_text

	async def set_chat_xiaodi_topic(self, board_info: dict) -> str:
		"""
		设置群组为晓迪频道
		"""
		
		chat_id = board_info["chat_id"]
		
		sop_text = textwrap.dedent("""
			<blockquote>晓迪频道 基础设置</blockquote>
			1️⃣ 新增私密频道
			2️⃣ 将核心一号机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>机器人指令</blockquote>
			4️⃣ 在机器人私信中，发送 <code>!setchat xiaodi_topic -100[频道ID]</code> 指令
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入小龙阳(入群审核)/核心二号机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/setchat xiaodi_topic</code> 指令 (待完成)
			8️⃣ 
			9️⃣ 完成				 				                     
		""").strip()
		# print(f"[harry] subscribe_preview: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)

			
			# 6️⃣ 拉入鲁仔四号(内容)/小龙阳(入群审核)/核心二号机器人给予所有的权限
			await self.grant_permissions_by_man(chat_id=chat_id, bot_name="xiaolongyang007bot")
			await self.grant_permissions_by_man(chat_id=chat_id, bot_name="noexists666bot")
			# await self.client.send_message(chat_id, "/setup")
			# await self.client.send_message("luzai33003bot", "/update_setting")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])
		except Exception as exc:
			print(f"[harry] xiaodi_topic: failed to grant permissions or invite bots: {exc}", flush=True)
		return sop_text


	async def set_chat_ltgphoto(self, board_info: dict) -> str:
		"""
		设置群组为清水群
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>清水群重建 基础设置</blockquote>
			1️⃣ 新增主题式群组
			2️⃣ 将核心一号机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 在群组中，发送 <code>!setchat ltgphoto</code> 指令
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入宸宸弟弟/核心二号机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/setup oldfriend_ask</code> 指令
			8️⃣ 
			9️⃣ 完成				 				                     
		""").strip()
		print(f"[harry] set_chat_old_friend: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)

			print(f"[harry] set_chat_old_friend: granting permissions to @lykeyman", flush=True)

			# 6️⃣ 拉入小班弟弟/核心二号机器人给予所有的权限
			
			await self.invite_to_group(chat_id, "noexists666bot")
			await self.grant_permissions(chat_id=chat_id, user_id="noexists666bot")
		
			await self.invite_to_group(chat_id, "chenchen808bot")
			await self.grant_permissions(chat_id=chat_id, user_id="chenchen808bot")

			await self.client.send_message(chat_id, "/bootstrap")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_old_friend: failed to grant permissions or invite bots: {exc}", flush=True)
		




		return sop_text

	async def set_chat_ltgphoto_review(self, board_info: dict) -> str:
		"""
		设置群组为清水群
		"""
		
		chat_id = board_info["chat_id"]
		sop_text = textwrap.dedent("""
			<blockquote>清水群重建 基础设置</blockquote>
			1️⃣ 新增主题式群组
			2️⃣ 将核心一号机器人邀请为匿名管理员，给予所有的权限 ( @deletedaccount34654bot )
			3️⃣ 将 @lykeyman 以一般的成员身份邀请进群
			<blockquote>群组指令</blockquote>
			4️⃣ 在群组中，发送 <code>!setchat ltgphoto_review</code> 指令
			5️⃣ 授与人型机器人管理员权限
			6️⃣ 拉入宸宸弟弟/核心二号机器人给予所有的权限
			7️⃣ 在群组中，发送 <code>/setup ltgphoto_review</code> 指令
			8️⃣ 
			9️⃣ 完成				 				                     
		""").strip()
		print(f"[harry] set_chat_ltgphoto_review: returning SOP text {sop_text}", flush=True)


		try:
			# 5️⃣ 授与人型机器人管理员权限
			
			man_me = await self.client.get_me()
			result = await self.grant_permissions(chat_id=chat_id, user_id=man_me.id, nonanonymous=True)
			if result is False:
				print(f"[harry] set_chat_ltgphoto_review: failed to grant permissions to @lykeyman", flush=True)
				return False
			# print(f"[harry] set_chat_ltgphoto_review: granting permissions to @lykeyman", flush=True)

			# 6️⃣ 拉入小班弟弟/核心二号机器人给予所有的权限
			
			await self.invite_to_group(chat_id, "noexists666bot")
			await self.grant_permissions(chat_id=chat_id, user_id="noexists666bot")

			await self.client.send_message(chat_id, "/book")
		
			await self.invite_to_group(chat_id, "chenchen808bot")
			await self.grant_permissions(chat_id=chat_id, user_id="chenchen808bot")

			await self.client.send_message(chat_id, "/setup_ltgphoto_review")

			await self.revoke_permissions(chat_id=chat_id, user_id=man_me.id)

			

			

			# await self.grant_permissions(board_info["chat_id"], board_info["sender_id"])

		except Exception as exc:
			print(f"[harry] set_chat_old_friend: failed to grant permissions or invite bots: {exc}", flush=True)
		




		# return sop_text



__all__ = ["HarryClass"]
