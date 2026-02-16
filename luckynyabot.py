
import traceback
from os import getenv, path
import sys
from shutil import move
from retry import retry
import subprocess
import math
import time
from datetime import datetime, timedelta, timezone
import random
import string
import os.path
import hashlib
from mimetypes import guess_extension
import re
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser


import argparse
import asyncio
import numpy as np
import psutil
import json as json
import sqlite3
import pymssql

import aiogram
from aiogram import Bot, Dispatcher, Router, html, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton



BOT_FATHER_TOKEN = getenv("BOT_FATHER_TOKEN")
LY_BOT_USER_ID = getenv("LY_BOT_USER_ID")
CONFISCATE_TARGET_USER_ID = getenv("CONFISCATE_TARGET_USER_ID")


parser = argparse.ArgumentParser(
    description="Script for luckynyabot")
parser.add_argument(
    "--bot-token",
    required=BOT_FATHER_TOKEN == None,
    type=str,
    default=BOT_FATHER_TOKEN,
    help=
    'api_id from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_ID env var)'
)
parser.add_argument(
    "--lybot-id", #7794660519
    required=LY_BOT_USER_ID == None,
    type=str,
    default=LY_BOT_USER_ID,
    help=
    'api_id from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_ID env var)'
)

parser.add_argument(
    "--confiscate-target", #620917081
    required=CONFISCATE_TARGET_USER_ID == None,
    type=str,
    default=CONFISCATE_TARGET_USER_ID,
    help=
    'api_id from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_ID env var)'
)


#args parsing
args = parser.parse_args()
aio_bot_token = args.bot_token
if_tg_connect = False

#asyncio
# loop = asyncio.get_event_loop()
tasks = []

#paremeters
debug_mode = False
cover_folder = "./cover_photos"

MSG_transfer_rate = 0.05 #messages per second
MSG_trans_retry_delay = 8
MSG_trans_retry_limit = 3
actionlock_release_interval = 0.7
HB_refrsh_timeslot = 0.1 #in seconds

#confiscate
confiscate_time_limit = 21600 #in seconds
confiscate_target = str(args.confiscate_target)

DB_info = []
DB_conn = None
hb_list = [] #hongbao list [[id,hbid,hbname,proccess_count,bot_id],[...]]
hb_record_template = {
    "id": None,
    "hbid": None,
    "hb_SN": None,
    "sender_id": None,
    "sender_name": None,
    "reciever_id": None,
    "reciever_name": None,
    "approved": None,
    "reciver_reaction_time": None,
    "send_point": None,
    "recieve_point": None,
    "transection_id": None
}
hb_records = []
hb_pool = []
hb_click_record = []

#protector
max_click_frequency = 2 #max click per second
click_protector_penalty = 90 #penalty time in seconds
click_protector_refresh_interval = 1 #in seconds
clicking_list = [] #[[user_id,click_count,lock,if_penalty,if_alert],[..],......]
protector_lock = asyncio.Lock()

#message wk
msg_queue_L = asyncio.Queue()
msg_queue_H = asyncio.Queue()
msg_tracker_list = [] #[[tracker_code(10 random digit),msg(msg element)],....]
generate_message_traker_code_lock = asyncio.Lock()


#transection control
transection_list = []
lybot_id = args.lybot_id
lybot_respond_list = []
lychat_lock = asyncio.Lock()
lytimeout = 15 #seconds


#locks
start_up_flag = False
DB_action_lock = asyncio.Lock()
API_action_lock = asyncio.Lock()
daily_qualify_reset_lock = asyncio.Lock()


#TG connection
aio_bot = None
aio_dp = Dispatcher()
aio_router = Router()

@aio_router.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    if not await check_click_protector(message):
        return
    """
    This handler receives messages with `/start` command
    """
    # Most event objects have aliases for API methods that can be called in events' context
    # For example if you want to answer to incoming message you can use `message.answer(...)` alias
    # and the target chat will be passed to :ref:`aiogram.methods.send_message.SendMessage`
    # method automatically or call API method directly via
    # Bot instance: `bot.send_message(chat_id=message.chat.id, ...)`
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")
    print("senderid:"+str(message.from_user.id))



@aio_router.message(Command(commands=["sethbgroup"])) #format /sethbgroup [channel id or chat id] [topic_id](optional), only allow in private chat
async def set_hb_group_command_handler(message: Message) -> None:
    global hb_list
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    

    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return

    command_parts = message.text.split(" ")
    if len(command_parts) <2 or len(command_parts) >3:
        await msg_queue_L.put([0,message,"指令格式错误， use /sethbgroup [channel id or chat id] [topic_id](optional)"])
        return
    hb_group_id = command_parts[1]
    # if hb_group_id.startswith("-100"):
    #     hb_group_id = hb_group_id[4:]
    if not hb_group_id.lstrip("-").isdigit():
        await msg_queue_L.put([0,message,"invalid chat id format"])
        return
    
    topic_id = ""
    if len(command_parts) ==3:
        topic_id = command_parts[2]
        if not topic_id.lstrip("-").isdigit():
            await msg_queue_L.put([0,message,"invalid topic id format"])
            return

    # #check if bot is admin in the group
    # try:
    #     chat_member = await aio_bot.get_chat_member(chat_id=int(hb_group_id), user_id=(await aio_bot.get_me()).id)
    #     if chat_member.status not in ["administrator", "creator"]:
    #         await msg_queue_L.put([0,message,"bot is not an admin in the specified group/channel"])
    #         return
    # except Exception as e:
    #     await msg_queue_L.put([0,message,"failed to get bot status in the specified group/channel, make sure bot is added to the group/channel"])
    #     return
    #check bot is in group (no need to be admin)

    hb_group_id = hb_group_id.strip()
    group_name = ""
    try:
        print("checking chat access for chat id: ", hb_group_id)
        try:
            chat = await aio_bot.get_chat(chat_id=int(hb_group_id))
            group_name = chat.title
        except:
            chat = await aio_bot.get_chat(chat_id=int(hb_group_id)*-1)
            group_name = chat.title
    except Exception as e:
        traceback.print_exc()
        await msg_queue_L.put([0,message,"failed to access the specified group/channel, make sure bot is added to the group/channel"])
        return
    
    #check topic_id exist
    # if len(topic_id)>0:
    #     try:
    #         forum_topics = await aio_bot.get_forum_topics(chat_id=int(hb_group_id))
    #         topic_ids = [str(topic.message_thread_id) for topic in forum_topics]
    #         if topic_id not in topic_ids:
    #             await msg_queue_L.put([0,message,"the specified topic_id does not exist in the group/channel"])
    #             return
            
    #     except Exception as e:
    #         traceback.print_exc()
    #         await msg_queue_L.put([0,message,"failed to get forum topics, make sure the group is a forum channel and topic_id is correct"])
    #         return

    #add to hb_list if not exist
    if any(str(hb[1]) == str(abs(int(hb_group_id))) and str(hb[3]) == str(topic_id) for hb in hb_list):
        await msg_queue_L.put([0,message,"this group/channel is already registered"])
        return
    else:
        await add_hb_list(str(abs(int(hb_group_id))), group_name, topic_id, str((await aio_bot.get_me()).id))
        hb_list.append([None,str(abs(int(hb_group_id))), group_name, topic_id,0, str((await aio_bot.get_me()).id)])
        await msg_queue_L.put([0,message,"hongbao group/channel registered successfully"])
        return
    
@aio_router.message(Command(commands=["rmhbgroup"])) #format /rmhbgroup [channel id or chat id] [topic_id](optional), only allow in private chat
async def rm_hb_group_command_handler(message: Message) -> None:
    global hb_list
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    

    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return
    
    if not DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return

    command_parts = message.text.split(" ")
    if len(command_parts) <2 or len(command_parts) >3:
        await msg_queue_L.put([0,message,"指令格式错误， /rmhbgroup [channel id or chat id] [topic_id](optional)"])
        return
    hb_group_id = command_parts[1]
    if hb_group_id.startswith("-100"):
        hb_group_id = hb_group_id[4:]
    if not hb_group_id.lstrip("-").isdigit():
        await msg_queue_L.put([0,message,"invalid chat id format"])
        return
    
    topic_id = ""
    if len(command_parts) ==3:
        topic_id = command_parts[2]
        if not topic_id.lstrip("-").isdigit():
            await msg_queue_L.put([0,message,"invalid topic id format"])
            return

    #remove from hb_list if exist
    if len(topic_id)>0:
        if any(str(hb[1]) == str(abs(int(hb_group_id))) and str(hb[3]) == str(topic_id) for hb in hb_list):
            await remove_hb_list(str(abs(int(hb_group_id))), topic_id, str((await aio_bot.get_me()).id))
            hb_list = [hb for hb in hb_list if not (str(hb[1]) == str(abs(int(hb_group_id))) and str(hb[3]) == str(topic_id))]
            await msg_queue_L.put([0,message,"hongbao group/channel removed successfully"])
            return
        else:
            await msg_queue_L.put([0,message,"this group/channel is not registered"])
            return
    else:
        if any(str(hb[1]) == str(abs(int(hb_group_id))) for hb in hb_list):
            await remove_hb_list(str(abs(int(hb_group_id))), "", str((await aio_bot.get_me()).id))
            hb_list = [hb for hb in hb_list if not (str(hb[1]) == str(abs(int(hb_group_id))))]
            await msg_queue_L.put([0,message,"hongbao group/channel removed successfully"])
            return
        else:
            await msg_queue_L.put([0,message,"this group/channel is not registered"])
            return
   
@aio_router.message(Command(commands=["hb"]))
async def hb_command_handler(message: Message) -> None:
    print("hb command received")
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type == "private":
        await msg_queue_L.put([0,message,"指令仅支援与群聊使用"])
        return
        
    if message.chat.type in ["supergroup", "channel"] and message.is_topic_message:
        if_restrict_topic = await DB_if_user_strick_topic(message)
        if any(hb[1] == str(abs(message.chat.id)) and (str(hb[3]) == str(message.message_thread_id) or len(str(hb[3]))==0 or not if_restrict_topic ) for hb in hb_list):
            time_start = time.time()
            await creat_hb(message)
            time_end = time.time()
            print("create hb time:", time_end - time_start)
        else:
            await msg_queue_L.put([6,message,f"只能在闲聊区发红包({message.chat.id},{message.message_thread_id})"])
    else:
        if any(hb[1] == str(abs(message.chat.id)) and len(str(hb[3]))==0 for hb in hb_list):
            time_start = time.time()
            await creat_hb(message)
            time_end = time.time()
            print("create hb time:", time_end - time_start)
        else:
            await msg_queue_L.put([6,message,f"只能在闲聊区发红包({message.chat.id},{message.message_thread_id})"])

@aio_router.message(Command(commands=["hongbao"]))
async def hb_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type == "private":
        await msg_queue_L.put([0,message,"指令仅支援与群聊使用"])
        return
    
    if message.chat.type in ["supergroup", "channel"] and message.is_topic_message:
        if_restrict_topic = await DB_if_user_strick_topic(message)
        if any(hb[1] == str(abs(message.chat.id)) and (str(hb[3]) == str(message.message_thread_id) or len(str(hb[3]))==0 or not if_restrict_topic ) for hb in hb_list):
            time_start = time.time()
            await creat_hb(message)
            time_end = time.time()
            print("create hb time:", time_end - time_start)
        else:
            await msg_queue_L.put([6,message,f"只能在闲聊区发红包({message.chat.id},{message.message_thread_id})"])
    else:
        if any(hb[1] == str(abs(message.chat.id)) and len(str(hb[3]))==0 for hb in hb_list):
            time_start = time.time()
            await creat_hb(message)
            time_end = time.time()
            print("create hb time:", time_end - time_start)
        else:
            await msg_queue_L.put([6,message,f"只能在闲聊区发红包({message.chat.id},{message.message_thread_id})"])

@aio_router.message(Command(commands=["regist"]))
async def regist_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return
    
    await DB_regist_user(message)
    await msg_queue_L.put([6,message,f"wellcome to hongbao bot,user ID:{message.from_user.id}"])

@aio_router.message(Command(commands=["sethbcoverupload"]))
async def set_cover_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return
    
    await msg_queue_L.put([0,message,"目前不支持自订上传图案作为封面图"])
    return
    
    await DB_regist_user(message)

    if not message.photo:
        await msg_queue_L.put([0,message,"该指令需同时发送图片"])
        return
    
    largest_photo = max(message.photo, key=lambda p: p.file_size)
    file = await aio_bot.get_file(largest_photo.file_id)
    file_path = file.file_path
    #generate fileneme
    cover_id = str(message.from_user.id)
    distinct_file_path = f"{cover_folder}/{cover_id}.jpg"
    distinct_file_temp_path = f"{cover_folder}/{cover_id}_temp.jpg"

    
    try:
        await aio_bot.download_file(file_path, destination=distinct_file_temp_path)

        #remove old if exist
        if os.path.exists(distinct_file_path):
            os.remove(distinct_file_path)

        move(distinct_file_temp_path, distinct_file_path)
        
    except Exception as e:
        logging.error(traceback.format_exc())
        await msg_queue_L.put([0,message,"failed to download the photo"])
        return
    
    print(str(largest_photo.file_id))
    await DB_set_hb_cover(message,str(largest_photo.file_id), cover_id)

    await msg_queue_L.put([0,message,"cover photo set successfully"])

@aio_router.message(Command(commands=["sethbcover"])) #/sethbcover [promt] [info(optional)]
async def set_cover_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return
    
    await DB_regist_user(message)

    #decode prompt
    prompt = message.text.replace("/sethbcover","").strip()
    if len(prompt) ==0:
        await msg_queue_L.put([0,message,"指令格式错误, /sethbcover [id]"])
        return
    try:
        if is_integer(prompt) == False:
            await msg_queue_L.put([0,message,"指令格式错误, /sethbcover [id]"])
            return
    except:
        traceback.print_exc()
        await msg_queue_L.put([0,message,"指令格式错误, /sethbcover [id]"])
        return
    
    #query cover photo from DB
    #cover_id = await DB_get_hb_cover_by_prompt(prompt)
    cover_id = await DB_get_hb_cover_by_id(prompt)
    if cover_id[0]=="0":
        await msg_queue_L.put([0,message,"没有找到与提供名称相同的封面图"])
        return
    await DB_set_hb_cover(message,cover_id[1], cover_id[0])
    await msg_queue_L.put([0,message,"成功设置封面图为: "+prompt])

@aio_router.message(Command(commands=["addcover"])) #/sethbcover [promt] [info(optional)]
async def set_cover_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not await DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return
    
    if not message.photo:
        await msg_queue_L.put([0,message,"该指令需同时发送图片"])
        return
    
    cover_info  = ""
    
    largest_photo = max(message.photo, key=lambda p: p.file_size)
    file = await aio_bot.get_file(largest_photo.file_id)
    file_path = file.file_path
    #generate fileneme
    cover_id = await gen_cover_id()
    distinct_file_path = f"{cover_folder}/{cover_id}.jpg"
    distinct_file_temp_path = f"{cover_folder}/{cover_id}_temp.jpg"

    
    try:
        await aio_bot.download_file(file_path, destination=distinct_file_temp_path)

        #remove old if exist
        if os.path.exists(distinct_file_path):
            os.remove(distinct_file_path)

        move(distinct_file_temp_path, distinct_file_path)
        
    except Exception as e:
        logging.error(traceback.format_exc())
        await msg_queue_L.put([0,message,"failed to download the photo"])
        return
    

    #decode prompt
    print(message.caption)
    prompt = str(message.caption).replace("/addcover","").strip().split(" ")[0]
    if len(prompt) ==0:
        await msg_queue_L.put([0,message,"please provide a prompt with this command to set as cover photo"])
        return
    
    if len(str(message.caption).replace("/addcover","").strip().split(" "))>1:
        cover_info = " ".join(str(message.caption).replace("/addcover","").strip().split(" ")[1:])

    await DB_add_hb_cover(prompt,largest_photo.file_id,cover_id, cover_info)

    await msg_queue_L.put([0,message,"cover photo set successfully"])
    
@aio_router.message(Command(commands=["rmcover"]))# /rmcover [promt]
async def rm_cover_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not await DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return
    
    command_parts = message.text.split(" ")
    if len(command_parts) !=2:
        await msg_queue_L.put([0,message,"指令格式错误， /rmcover [prompt]"])
        return
    prompt = command_parts[1]

    await DB_remove_hb_cover(prompt)
    await msg_queue_L.put([0,message,f"封面图{prompt}已移除"])

@aio_router.message(Command(commands=["addadmin"]))# /addamin [user id]
async def add_admin_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not await DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return
    
    command_parts = message.text.split(" ")
    if len(command_parts) !=2:
        await msg_queue_L.put([0,message,"指令格式错误 /addadmin [user id]"])
        return
    user_id = command_parts[1]
    if not user_id.lstrip("-").isdigit():
        await msg_queue_L.put([0,message,"invalid user id format"])
        return

    await DB_set_user_admin(user_id, True)
    await msg_queue_L.put([0,message,"admin added successfully"])

@aio_router.message(Command(commands=["rmrestricttopic"]))# /rmrestricttopic [user id]
async def rm_restrict_topic_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not await DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return
    
    command_parts = message.text.split(" ")
    if len(command_parts) !=2:
        await msg_queue_L.put([0,message,"指令格式错误， /rmrestricttopic [user id]"])
        return
    user_id = command_parts[1]
    if not user_id.lstrip("-").isdigit():
        await msg_queue_L.put([0,message,"invalid user id format"])
        return

    await DB_set_user_strick_topic(user_id, False)
    await msg_queue_L.put([0,message,"remove restrict topic successfully"])

@aio_router.message(Command(commands=["listcover"]))# /listcover 
async def list_cover_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    cover_list = await DB_get_all_cover()
    if len(cover_list) ==0:
        await msg_queue_L.put([0,message,"系统中无封面图"])
        return
    
    cover_text = "封面图列表:\n\n"
    for cover in cover_list:
        cover_text += f"ID: {cover[0]} Name: {cover[1]} Info: {cover[4]}\n"

    await msg_queue_L.put([0,message,cover_text])

@aio_router.message(Command(commands=["rmcap"]))# /rmcap [caption id]
async def rm_cover_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not await DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return
    
    command_parts = message.text.split(" ")
    if len(command_parts) !=2:
        await msg_queue_L.put([0,message,"指令格式错误， /rmcap [caption id]"])
        return
    cover_id = command_parts[1]

    await DB_remove_caption(cover_id)
    await msg_queue_L.put([0,message,f"趣味文案 No.{cover_id} 成功移除"])

@aio_router.message(Command(commands=["listcap"]))# /liscap
async def list_caption_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return
    
    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    caption_list = await DB_get_all_caption()
    if len(caption_list) ==0:
        await msg_queue_L.put([0,message,"系统中目前没有趣味文案"])
        return
    
    caption_text = "趣味文案列表:\n\n"
    for caption in caption_list:
        caption_info = str(caption[1]).replace("<", "[").replace(">", "]")
        caption_text += f"Caption: {caption[0]} Info: {caption_info}\n"


    await msg_queue_L.put([0,message,caption_text])

@aio_router.message(Command(commands=["addcap"]))# /addcap [caption]
async def add_caption_command_handler(message: Message) -> None:
    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return

    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    if not await DB_if_user_admin(message):
        await msg_queue_L.put([0,message,"无操作权限"])
        return
    
    command_parts = message.text.split(" ")
    if len(command_parts) <2:
        await msg_queue_L.put([0,message,"指令格式错误, /addcap [caption]"])
        return
    caption = " ".join(command_parts[1:])

    await DB_add_caption(caption)
    await msg_queue_L.put([0,message,"已成功添加随机趣味文案"])

@aio_router.message(Command(commands=["about"]))# /about
async def about_hb_command_handler(message: Message) -> None:

    if not await check_click_protector(message):
        print("click protector triggered"+str(message.from_user.full_name))
        return

    if message.chat.type != "private":
        await msg_queue_L.put([0,message,"指令仅支援与机器人私聊"])
        return

    # info_msg = ""
    # info_msg += "红包使用说明:\n"
    # info_msg += "1. 在群组闲聊区发送 /hb 或 /hongbao 指令创建红包, 格式为 /hb 总分 总数 [附加讯息], 例如 /hb 66 6 校長的只有3cm\n"
    # info_msg += "2. 红包总分最低为2分, 最高为666分\n"
    # info_msg += "3. 红包数量最低为2个, 最高为66个\n"
    # info_msg += "4. 红包创建后, 群组成员可以点击红包消息中的按钮领取红包\n"
    # info_msg += "5. 每个成员每个红包只能领取一次, 领取后无法退款\n" 
    # info_msg += "6. 红包创建者可以在红包消息中查看红包领取情况\n"
    # info_msg += "7. 请不要用奇怪的手抢红包，会被封禁\n"
    # info_msg += "8. 恶意使用会被封禁或暂停使用:包含，但不限于:1.用上帝的手抢红包 2.不正常的乱点按钮或使用指令\n"

    # info_msg += "\n封面图说明:\n"   
    # info_msg += "发送 / sethbcover [封面图名] 指令, 可以将数据库中与该封面图名匹配的图片设置为红包封面图, 并在发送红包时随机出现小惊喜\n"

    # info_msg += "封面图名查看方式\n："
    # info_msg += " / listcover: 列出数据库中所有封面图的封面图名和封面图说明\n"
    # # info_msg += "\n封面图说明:\n"
    # # info_msg += "分为两种方式\n"
    # # info_msg += "1. 方式一: <s>发送 /sethbcoverupload 指令并附带一张图片, 可以将该图片设置为红包封面图</s>\n"
    # # info_msg += "2. 方式二: 发送 /sethbcover [封面图名] 指令, 可以将数据库中与该封面图名匹配的图片设置为红包封面图, 并在发送红包时随机出现小惊喜\n"
    # # info_msg += "3. /listcover: 列出数据库中所有封面图的封面图名和封面图说明\n"
    # # info_msg += "4. 使用方式二前先用/listcover指令列出封面图选项\n"
    # info_msg += "\n用户指令:\n"
    # info_msg += "1. /regist: 注册成为红包用户, 注册才会收到扣款通知\n"
    # # info_msg += "\n管理员指令:\n"
    # # info_msg += "封面图:\n"
    # # info_msg += "1. /addcover [封面图名] [封面图说明(optional)]: 添加封面图到数据库, 可以使用封面图说明参数添加图片描述\n"
    # # info_msg += "2. /rmcover [封面图名]: 移除指定封面图名的封面图\n"
    # # info_msg += "3. 封面图名自己决定, 且会成为趣味文案替换字\n"
    # # info_msg += "4. 一次增加或删除一个, 图名不可重复, 重复会被覆盖\n"  
    # # info_msg += "趣味文案:\n"
    # # info_msg += "5. /addcap [caption]: 添加红包趣味文案到数据库, 机器人发送红包时会随机选择一个趣味文案\n"
    # # info_msg += "6. /rmcap [caption id]: 移除指定趣味文案\n"
    # # info_msg += "7. /listcap: 列出数据库中所有趣味文案\n"
    # # info_msg += "8. 先用 /listcap 找到要删除的编号再删除\n"
    # # info_msg += "9. 趣味文案中 &lt;&gt; 會被代換成封面圖名\n"
    # # info_msg += "群组管理:\n"
    # # info_msg += "10. /sethbgroup [channel id or chat id] [topic_id](optional): 注册红包群组/频道, topic_id为可选参数, 仅在指定话题内允许发红包\n"
    # # info_msg += "11. /rmhbgroup [channel id or chat id] [topic_id](optional): 移除红包群组/频道注册\n"
    # # info_msg += "用户管理:\n"
    # # info_msg += "12./rmrestricttopic [user id]: 移除用户的话题限制, 允许用户在所有话题内发红包\n"
    # # info_msg += "13./addadmin [user id]: 添加管理员\n" 

    info_msg = ""
    info_msg += "🎊 小龙包红包使用指南🎊\n\n" 
 
    info_msg += "\n🌟 红包创建&领取攻略\n\n" 
    
    info_msg += "1. 发红包三步走：在群组闲聊区发送指令，格式超简单！ /hb 总分 总数 [附加信息]  或  /hongbao 总分 总数 [附加信息] 举例： /hb 66 6 校长的只有3cm🤣 \n" 
    info_msg += "2. 规则小提醒：\n" 
    info_msg += "- 总分：最低2分起，最高666分封顶～\n" 
    info_msg += "- 数量：最少2个，最多66个，大家一起抢才热闹！\n" 
    info_msg += "3. 领取方式：红包创建后，点击消息里的领取按钮就能解锁积分啦～\n" 
    info_msg += "4. 重要警告⚠️：禁止用脚本/三方插件抢红包！被系统抓到会封禁哦，乖乖手动抢才快乐～\n" 
    
    info_msg += "\n🎨 红包封面设置秘籍\n" 
    
    info_msg += "\n1. 先查库存：发送  /listcover  指令，小龙包会列出所有封面图的「名字+说明+序号」，任你挑选！\n" 
    info_msg += "2. 一键设置：两种方式都能搞定～\n" 
    info_msg += "- 发送  /sethbcover [封面序号]  👉 匹配序号直接生效，发红包时随机掉落惊喜文案！\n" 
    
    info_msg += "\n📌 必备用户指令\n" 
    
    info_msg += "\n1.  /regist  📝 ：先注册成为红包用户，才能快乐收发红包呀！\n" 
    info_msg += "2.  /listcover  📜 ：查看封面库所有美图的「序号+名字+说明」，挑到心动款～\n" 
    info_msg += "3.  /sethbcover [封面序号]  ✨ ：设置专属红包封面，发红包更有仪式感！\n" 
    info_msg += "4.  /about  ❓ ：查看小龙包全功能说明，玩转所有玩法～\n" 
    
    info_msg += "\n💡 小贴士：注册后就能和小伙伴们一起抢积分红包、自定义封面啦，惊喜文案随机触发，快乐翻倍！\n" 

    await msg_queue_L.put([6,message,info_msg])




@aio_router.message()
async def detect_specific_content(message: Message):
    global lybot_respond_list

    if message.text == None and message.caption == None:
        return
    
    if message.chat.type == "private" :
        if message.text.startswith("/"):
            #move command handlers to specific functions
            if message.text.startswith("/sethbgroup"):
                await set_hb_group_command_handler(message)
            elif message.text.startswith("/rmhbgroup"):
                await rm_hb_group_command_handler(message)
            elif message.text.startswith("/addcover"):
                await set_cover_command_handler(message)
            elif message.text.startswith("/rmcover"):
                await rm_cover_command_handler(message)
            elif message.text.startswith("/addadmin"):
                await add_admin_command_handler(message)
            elif message.text.startswith("/rmrestricttopic"):
                await rm_restrict_topic_command_handler(message)
            elif message.text.startswith("/rmcap"):
                await rm_cover_command_handler(message)
            elif message.text.startswith("/listcap"):
                await list_caption_command_handler(message)
            elif message.text.startswith("/addcap"):
                await add_caption_command_handler(message)
            elif message.text.startswith("/tip"):
                pass
            elif message.text.startswith("/award"):
                pass
            else:
                if not await check_click_protector(message):
                    print("click protector triggered"+str(message.from_user.full_name))
                    return
    else:
        if message.text.startswith("/"):
            if message.text.startswith("/hb"):
                print("receive /hb command")
                await hb_command_handler(message)
            elif message.text.startswith("/hongbao"):
                print("receive /hongbao command")
                await hb_command_handler(message)
            elif message.text.startswith("/tip"):
                pass
            elif message.text.startswith("/award"):
                pass
            else:
                if not await check_click_protector(message):
                    print("click protector triggered"+str(message.from_user.full_name))
                    return

        if "插眼" in message.text.lower():  # Check if the message contains "hello"
            #answer
            await msg_queue_L.put([6,message,"插眼要鸡腿，但是马眼不用"])
        elif "红包" == message.text.lower():  # Check if the message contains "hello"
            #answer
            await msg_queue_L.put([6,message,"看看哥哥發大包"])
        elif "没分" == message.text.lower() or "没积分了" == message.text.lower():  # Check if the message contains "hello"
            #answer
            await msg_queue_L.put([6,message,"发原创就有了"])
        elif "鸡腿" in message.text.lower():  # Check if the message contains "hello"
            #answer
            await msg_queue_L.put([6,message,"看看鸡腿"])

    # print("senderid:"+str(message.from_user.id) + " message:"+message.text)

    if lybot_id != "" and str(message.from_user.id) == str(lybot_id) and message.chat.type == "private" and check_json_format(message.text):
        global lybot_respond_list
        global lychat_lock
        lybot_respond_list.append(message)
        print("lybot message detected:", message.text)
        # {"ok": 1, "chatinfo": "-1003409715946_627"}
        #if message.text.lower().startswith("lybot respond:"):

    if message.chat.type != "private":
        await daily_qualify_reset_lock.acquire()
        daily_qualify_reset_lock.release()
        await DB_proof_daily_qualify(message.from_user.id,message.chat.id)#user_id,chat_id


# @aio_dp.message()
# async def echo_handler(message: Message) -> None:
#     """
#     Handler will forward receive a message back to the sender

#     By default, message handler will handle all message types (like a text, photo, sticker etc.)
#     """
#     try:
#         # Send a copy of the received message
#         await message.send_copy(chat_id=message.chat.id)
#     except TypeError:
#         # But not all the types is supported to be copied so need to handle it
#         await message.answer("Nice try!")

@aio_router.callback_query()
async def handle_button_click(callback_query: types.CallbackQuery):
    global hb_click_record
    global hb_pool
    global msg_queue_L
    global msg_queue_H

    if not await check_click_protector(callback_query):
        print("click protector triggered"+str(callback_query.from_user.full_name))
        return

    if callback_query.data == "button1":
        await callback_query.message.answer(f"{callback_query.from_user.full_name}({callback_query.from_user.id}) clicked Button 1 in {callback_query.message.chat.id}!")
    elif callback_query.data == "button2":
        await callback_query.message.answer(f"{callback_query.from_user.full_name}({callback_query.from_user.id}) clicked Button 2 in {callback_query.message.chat.id}!")
    elif callback_query.data.startswith("claimHB_"):
        #add record to hb_click_record
        #record:[callback_query, hb_SN,from user.id, click_time]
        hb_SN = callback_query.data.split("_")[1].strip()
        hb_SN = int(hb_SN)
        click_time = datetime.now(timezone.utc).astimezone()
        #detect duplicate clicks
        for record in hb_click_record:
            if record[1] == hb_SN and record[2] == callback_query.from_user.id:
                # await callback_query.answer("You have already clicked to claim this hongbao!", show_alert=True)
                await msg_queue_L.put([13,callback_query,"你已经点击过领取此红包了! HB_SN: "+str(hb_SN)])
                return
            
        # print(hb_pool)
        # print("hb_SN:", hb_SN)

        # #detect self_reward
        # for hb_entry in hb_pool:
        #     if int(hb_entry[0]) == int(hb_SN):
        #         if str(hb_entry[10][0]) == str(callback_query.from_user.id):
        #             # await callback_query.answer("You cannot claim your own hongbao!", show_alert=True)
        #             await msg_queue_L.put([13,callback_query,"你不能领取自己的红包! HB_SN: "+str(hb_SN)])
        #             return
        #         break

        if any((int(hb[0]) == int(hb_SN) and hb[2] != "finished") for hb in hb_pool):
            if not await DB_check_daily_qualify(callback_query.from_user.id,callback_query.message.chat.id):
                await msg_queue_L.put([13,callback_query,"你今天还没有发言，无法领取红包! HB_SN: "+str(hb_SN)])
                return
            hb_click_record.append([callback_query, hb_SN, callback_query.from_user.id, click_time])
        elif any(hb[0] == hb_SN for hb in hb_pool) == False:
            await msg_queue_L.put([13,callback_query,"红包已经抢完或不存在(1)! HB_SN: "+str(hb_SN)])
        else:
            await msg_queue_L.put([13,callback_query,"红包已经抢完或不存在(2)! HB_SN: "+str(hb_SN)])




#logic
async def creat_hb(message: Message):
    global hb_list
    global hb_pool
    global tasks

    print("creat_hb called")

    pt_max = 0
    amount_max = 0
    hb_message = ""
    
    
    #decode command
    #replace all unprintable characters in message.text with""
    #also allow chinese characters
    command_text = re.sub(r'[^\x20-\x7E\u4e00-\u9fa5]+', '', message.text)

    #command_text = re.sub(r'[^\x20-\x7E]+', '', message.text)
    #command should be in format Normal: "/hb pt amount text" or "/hongbao pt amount text" or Exclusive: "/hb pt text" or "/hongbao pt text", text is optional
    command_parts = command_text.split(" ")
    print("command_parts:", command_parts)
    if len(command_parts) < 2:
        await msg_queue_L.put([0,message,"红包格式错误,  /hb 总分 总数 [附加讯息] 或 /hongbao 总分 总数 [附加讯息]"])
        return
    elif len(command_parts) < 3:
        try:
            if "." in command_parts[1]:
                await msg_queue_L.put([0,message,"分數必須為整數"])
                return
            pt_max = int(command_parts[1])
            await msg_queue_L.put([0,message,"目前不支持专属红包"])
            return
        except:
            traceback.print_exc()
            await msg_queue_L.put([0,message,"紅红包格式错误,  /hb 总分 总数 [附加讯息] 或 /hongbao 总分 总数 [附加讯息]"])
            return
    elif len(command_parts) >= 3:
        try:
            if "." in command_parts[1]:
                await msg_queue_L.put([0,message,"分數必須為整數"])
                return
            pt_max = int(command_parts[1])
            if is_integer(pt_max) == False:
                await msg_queue_L.put([0,message,"分數必須為整數"])
                return
        except:
            traceback.print_exc()
            await msg_queue_L.put([0,message,"红包格式错误,  /hb 总分 总数 [附加讯息] 或 /hongbao 总分 总数 [附加讯息]"])
            return
        
        try:
            if "." in command_parts[2]:
                await msg_queue_L.put([0,message,"红包数量必须为整数"])
                return
            amount_max = int(command_parts[2])

            if is_integer(amount_max) == False:
                await msg_queue_L.put([0,message,"红包数量必须为整数"])
                return
        except:
            traceback.print_exc()
            # hb_message = " ".join(command_parts[2:])
            await msg_queue_L.put([0,message,"目前不支持专属红包"])
            return
        
        hb_message = " ".join(command_parts[3:])

    
    #validate command
    #pt_max = 666 min = 2
    #amount_max = 60 min =2
    if pt_max==0:
        pt_max = 66

    if pt_max <2:
        await msg_queue_L.put([0,message,"红包总分最少为2分"])
        return
    if amount_max <2:
        await msg_queue_L.put([0,message,"红包数量必须大于1"])
        return
    if pt_max > 666:
        await msg_queue_L.put([0,message,"红包总分不可超过666"])
        return
    if amount_max >66:
        await msg_queue_L.put([0,message,"红包数量不可大于66"])
        return
    
    

    allocation_method = "even"
    if pt_max ==amount_max:
        allocation_method = "even"
    else:
        if pt_max > amount_max:
            if pt_max % amount_max ==0:
                if random.randint(1,100)<=30:
                    allocation_method = "even"
                else:
                    allocation_method = "random"
            else:
                allocation_method = "random"
        else:
            await msg_queue_L.put([0,message,"总分必须大于红包个数"])
            return
        allocation_method = "random"  #force random for testing

    # allocation_method = "random"  #force random for testing
    
    time_start = time.time()
    chat_id = abs(message.chat.id)
    try:
        await aio_bot.get_chat(chat_id=int(chat_id))
        chat_id = chat_id
    except:
        await aio_bot.get_chat(chat_id=int(chat_id)*-1)
        chat_id =int(chat_id)*-1
    time_end = time.time()
    print("get chat id time:", time_end - time_start)

    
    # transection_list.append([chat_id,message.message_id,""])
    lybot_approved = await check_pt(chat_id,message.message_id)
    if lybot_approved[0]:
        pass
    else:
        if lybot_approved[1] == "insufficient pt":
            await msg_queue_L.put([0,message,"积分不足"])
        elif lybot_approved[1] == "lybot timeout":
            await msg_queue_L.put([0,message,"积分查询超时，请稍后再试"])
        return

    #[hb_SN,hb_id(chat_id),topic_id,status(create,ongoing,finished),hb_message_id,request_message_id,create_time,Allocation method,[max_pt,sent_pt,remain_pt],[max_amount,sent_amount,remain_amount],[sender_id,sender_name],[reciver_list([receiver_id,receiver_name,approved,recieve_pt,reaction time])],hb_lock,last_DP_update_time,hb_message,if_cover,caption_text,dp_update_flag]
    hb_SN,create_time = await DB_get_hb_SN()
    if hb_SN is None:
        await msg_queue_L.put([0,message,"红包创建失败，请稍后再试"])
        return
    
    
    #the hb

    topic_id = ''
    if message.chat.type in ["supergroup", "channel"] and message.is_topic_message:
        topic_id = message.message_thread_id
        
    time_start = time.time()
    hb_entry = [int(hb_SN),str(chat_id),topic_id,"ongoing",None,message.message_id,create_time,allocation_method,[pt_max,0,pt_max],[amount_max,0,amount_max],[message.from_user.id,message.from_user.full_name],[],asyncio.Lock(),datetime.now(timezone.utc).astimezone()- timedelta(seconds=3),hb_message,False,"",True]
    hb_pool.append(hb_entry)
    for hb_entry in hb_pool:
        if hb_entry[0] == hb_SN:
            await hb_entry[12].acquire()
            await DB_update_hb_pool_record(hb_SN)
            tasks.append(asyncio.create_task(hb_handler(hb_SN)))
            hb_entry[12].release()
            hb_entry[3] = "ongoing"
            break
   
    # print("hongbao created: ", hb_entry)
    time_start = time.time()
    await update_hb_display(hb_SN)
    time_end = time.time()
    print("initial hb display time:", time_end - time_start)
    
    
async def update_hb_display(hb_SN):
    #check if current time - hb_entry[11] > MSG_transfer_rate*2
    global hb_pool
    global msg_queue_H
    global msg_queue_L
    global msg_tracker_list

    for hb_entry in hb_pool:
        if hb_entry[0] == hb_SN:
            if hb_entry[-1] == False:
                return
            try:
                # print("A")
                current_time = datetime.now(timezone.utc).astimezone()
                time_diff = (current_time - hb_entry[13]).total_seconds()
                if time_diff < MSG_transfer_rate * 5 and hb_entry[3] != "finished":
                    return
                hb_entry[13] = current_time

                # print("B")

                display_text = f"🏮 {hb_entry[10][1]}  发红包啦～\n\n"

                if len(hb_entry[14])>0 and hb_entry[14] is not None:
                    display_text += f"并嘟囔着: <b>{hb_entry[14]}</b>\n\n"

                #addition caption
                if random.randint(1,100)<=100:
                    if len(hb_entry[16])==0:
                        #check if user set system cover
                        cover_id = await DB_get_hb_cover_by_user_id(hb_entry[10][0])
                        cover_list = await DB_get_all_cover()
                        #compare file_id(in cover_list) with cover_id[1] and filter the match promt
                        cover_promt = [cover[1] for cover in cover_list if (cover[2] == cover_id[1] and str(cover[3]) == str(cover_id[0]))]
                        caption_list = await DB_get_all_caption()

                        if len(cover_promt) >0 and len(caption_list) >0:
                            selected_promt = cover_promt[0]
                            selected_caption = random.choice(caption_list)

                            display_text_tmp = "<i>"+str(selected_caption[1]).replace("<>",selected_promt)+"</i>"
                            display_text += f"💬 {display_text_tmp}\n\n"
                            hb_entry[16] = display_text_tmp
                            await DB_HB_set_caption(hb_SN, hb_entry[16])
                    else:
                        display_text += f"💬 {hb_entry[16]}\n\n"

                display_text += f"🎁 总金额：{hb_entry[8][0]} 积分\n"
                if hb_entry[7] == "even":
                    display_text += f"🧧 红包数：{hb_entry[9][0]}（平均分配）\n"
                elif hb_entry[7] == "random":
                    display_text += f"🧧 红包数：{hb_entry[9][0]}（拼手气）\n"

                display_text += f"HB_SN: {hb_SN}\n"#紅包編號
                display_text += f"⏰ {str(hb_entry[6]).replace('T',' ').split('.')[0]}\n\n"

                if hb_entry[3] == "finished":
                    display_text += "\n✨ 已全部抢完呀！\n"

                display_text += f"💰已领取金额：{hb_entry[8][1]}/{hb_entry[8][0]} 积分\n"
                display_text += f"🧧已领取个数：{hb_entry[9][1]}/{hb_entry[9][0]} 个\n\n"

                display_text += "💖 幸运名单：\n\n"

                #lucky guy (the one who get most)
                # if possible remain points is less than the current heighest score than display the heighest score
                if len(hb_entry[11]) >0 and hb_entry[7] == "random":
                    heighest_score = max(record[3] for record in hb_entry[11])
                    lucky_guys = [record for record in hb_entry[11] if record[3] == heighest_score]

                    if (int(hb_entry[8][2]) - int(hb_entry[9][2]) +1 <= heighest_score) or int(hb_entry[9][2])==0:
                        display_text += f"👑 运气王：{lucky_guys[0][1]} 抢到了 {heighest_score} 积分"
                        display_text += "\n\n"


                if len(hb_entry[11]) == 0:
                    display_text += "无人领取\n"
                else:
                    dp_record_count = 0
                    for record in hb_entry[11]:
                        if record[2] :
                            #display_text +=f"- {record[1]} 抢到了 {record[3]} 积分 ({record[4]}ms)\n"
                            disp_time = (int(record[4])/1000) if int(record[4])<60000 else ">60"
                            display_text +=f"- {record[1]} 抢到了 {record[3]} 积分 ({disp_time}s)\n"
                            dp_record_count += 1
                        if dp_record_count >= 9:
                            if len(hb_entry[11]) - dp_record_count >1:
                                display_text += f"...\n"
                            if len(hb_entry[11]) - dp_record_count >0:
                                disp_time = (int(hb_entry[11][-1][4])/1000) if int(hb_entry[11][-1][4])<60000 else ">60"
                                display_text +=f"- {hb_entry[11][-1][1]} 抢到了 {hb_entry[11][-1][3]} 积分 ({disp_time}s)\n"

                            break
                
                

                # print("C")
                keyboard = []
                keyboard.append([])
                keyboard[0].append(InlineKeyboardButton(text="抢红包", callback_data=f"claimHB_{hb_entry[0]}"))
                keyboard = InlineKeyboardMarkup(row_width=1, inline_keyboard=keyboard)
                
                # print("D")
                cover_id = await DB_get_hb_cover_by_user_id(hb_entry[10][0])
                if hb_entry[4] is None:
                    #generate message traker code (check duplicate)
                    message_traker_code = await generate_message_traker_code()
                    #check if sender set cover photo
                    if cover_id[0] != "0":
                        hb_entry[15] = True
                        await msg_queue_H.put([10,message_traker_code,cover_id[1],[hb_entry[1],hb_entry[2]],display_text,ParseMode.HTML,keyboard])

                    else:
                        await msg_queue_H.put([8,message_traker_code,[hb_entry[1],hb_entry[2]],display_text,ParseMode.HTML,keyboard])
                    if_get_msd = False
                    msg = None
                    while if_get_msd==False:
                        for tracker in msg_tracker_list:
                            if tracker[0] == message_traker_code and tracker[1] is not None:
                                msg = tracker[1]
                                msg_tracker_list.remove(tracker)
                                if_get_msd = True
                                break
                        if if_get_msd==False:
                            await asyncio.sleep(0.5)

                    hb_entry[4] = msg.message_id
                    await DB_hb_msg_id(hb_entry[0], hb_entry[4],hb_entry[15])
                    #pin
                    await msg_queue_L.put([14,hb_entry[1],hb_entry[4]])
                else:
                    #edit existing message
                    if hb_entry[3] == "finished":
                        if hb_entry[9][1] != hb_entry[9][0]:
                            #data inconsistency detected
                            display_text += "\n⚠️ 小笼包放太久发霉了 不能吃了 ⚠️\n"

                        if hb_entry[15]:
                            await msg_queue_L.put([11,cover_id[1],[hb_entry[1],hb_entry[2]],display_text,hb_entry[4],ParseMode.HTML])
                        else:
                            await msg_queue_L.put([9,[hb_entry[1],hb_entry[2]],display_text,hb_entry[4],ParseMode.HTML])

                        #remove pin
                        await msg_queue_L.put([15,hb_entry[1],hb_entry[4]])
                    else:
                        if hb_entry[15]:
                            await msg_queue_L.put([11,cover_id[1],[hb_entry[1],hb_entry[2]],display_text,hb_entry[4],ParseMode.HTML,keyboard])
                        else:
                            await msg_queue_L.put([9,[hb_entry[1],hb_entry[2]],display_text,hb_entry[4],ParseMode.HTML,keyboard])

                    

                hb_entry[-1] = False
            except Exception as e:
                print('update_hb_display error: ', str(e))
            break

async def update_hb_status(hb_SN):
    global hb_pool
    global hb_click_record
    global msg_queue_L
    global msg_queue_H

    for hb_entry in hb_pool:
        if hb_entry[0] == hb_SN:

            if hb_entry[3] == "finished":
                print("HB_SN:", hb_SN, " is already finished.")
                
                try:
                    hb_pool.remove(hb_entry)
                    relevant_clicks = [record for record in hb_click_record if record[1] == hb_SN]
                    for record in relevant_clicks:
                        relevant_clicks.remove(record)
                        hb_click_record.remove(record)

                except Exception as e:
                    print('update_hb_status remove error: ', str(e))

            elif hb_entry[9][2] == 0:
                print("HB_SN:", hb_SN, " is now finished.")
                hb_entry[3] = "finished"
                relevant_clicks = [record for record in hb_click_record if record[1] == hb_SN]
                for record in relevant_clicks:
                    await add_hb_record({
                            "hbid": hb_entry[1],
                            "hb_SN": hb_entry[0],
                            "sender_id": hb_entry[10][0],
                            "sender_name": hb_entry[10][1],
                            "reciever_id": record[2],
                            "reciever_name": record[0].from_user.full_name,
                            "approved": str(0),
                            "reciver_reaction_time": str(int((record[3] - datetime.fromisoformat(hb_entry[6])).total_seconds() * 1000)),
                            "send_point": str(hb_entry[8][0]),
                            "recieve_point": str(0),
                            "transection_id": ""
                        })
                    if any(r[0] == record[2] for r in hb_entry[11]):
                        pass
                    else:
                        await msg_queue_L.put([13,record[0],"红包已经抢完!"])
                    relevant_clicks.remove(record)
                    hb_click_record.remove(record)

                
                await DB_update_hb_pool_record(hb_SN)

                
                hb_entry[-1] = True
            
            else:
                # extract click records for this hb_SN
                relevant_clicks = [record for record in hb_click_record if record[1] == hb_SN]
                if len(relevant_clicks) == 0:
                    break

                await hb_entry[12].acquire()  
                try:
                    if hb_entry[9][2] == 0:
                        hb_entry[12].release()
                        break
                except:
                    traceback.print_exc()
                    print("hb_entry release error at confiscate pre-check")

                try:
                    while True :
                        #randomly pick one record to process
                        record = random.choice(relevant_clicks)

                        #check duplicatee click
                        if any(str(r[0]) == str(record[2]) for r in hb_entry[11]):
                            relevant_clicks.remove(record)
                            hb_click_record.remove(record)
                            await msg_queue_L.put([13,record[0],"你已经抢过这个红包!"])
                            if len(relevant_clicks) ==0:
                                break
                            else:
                                continue
                        
                        #alocate point
                        give_pt = 0

                        if hb_entry[9][2] == 1:
                            #last hongbao
                            give_pt = hb_entry[8][2]
                            hb_entry[8][1] += give_pt
                            hb_entry[8][2] -= give_pt
                            hb_entry[9][1] += 1
                            hb_entry[9][2] -= 1

                        elif hb_entry[9][2] > 1:
                            if hb_entry[7] == "even":
                                give_pt = math.floor(hb_entry[8][0]/hb_entry[9][0])
                                hb_entry[8][1] += give_pt
                                hb_entry[8][2] -= give_pt
                                hb_entry[9][1] += 1
                                hb_entry[9][2] -= 1
                                
                            elif hb_entry[7] == "random":
                                give_pt = allocate_points_gaussian_with_minimum(hb_entry[8][2], hb_entry[9][2])
                                hb_entry[8][1] += give_pt
                                hb_entry[8][2] -= give_pt
                                hb_entry[9][1] += 1
                                hb_entry[9][2] -= 1

                        
                        transection_result = await transfer_pt(hb_SN,hb_entry[10][0], record[2], give_pt)
                        
                        if transection_result[0]:
                            hb_entry[11].append([record[2], record[0].from_user.full_name, True, give_pt, int((record[3] - datetime.fromisoformat(hb_entry[6])).total_seconds() * 1000)])
                            
                            #update DB
                            await add_hb_record({
                                "hbid": hb_entry[1],
                                "hb_SN": hb_entry[0],
                                "sender_id": hb_entry[10][0],
                                "sender_name": hb_entry[10][1],
                                "reciever_id": record[2],
                                "reciever_name": record[0].from_user.full_name,
                                "approved": str(1),
                                "reciver_reaction_time": str(int((record[3] - datetime.fromisoformat(hb_entry[6])).total_seconds() * 1000)),
                                "send_point": str(hb_entry[8][0]),
                                "recieve_point": str(give_pt),
                                "transection_id": str(transection_result[1])
                            })
                            await DB_update_hb_pool_record(hb_SN)
                            await msg_queue_L.put([13,record[0],f"恭喜你抢到 {give_pt} 鸡分!"])
                            hb_entry[-1] = True
                        else:
                            hb_entry[8][1] -= give_pt
                            hb_entry[8][2] += give_pt
                            hb_entry[9][1] -= 1
                            hb_entry[9][2] += 1

                            

                            await msg_queue_L.put([13,record[0],f"红包交易异常: {transection_result[1]}"])

                        



                        relevant_clicks.remove(record)
                        hb_click_record.remove(record)
                        break
                except Exception as e:
                    print('update_hb_status error: ', str(e))
                finally:
                    hb_entry[12].release()


async def hb_handler(hb_SN):
    global HB_refrsh_timeslot
    global hb_pool
    global hb_click_record
    
    alive = True
    while alive:
        await asyncio.sleep(HB_refrsh_timeslot)
        alive = False
        for hb_entry in hb_pool:
            if hb_entry[0] == hb_SN:
                await update_hb_status(hb_SN)
                if hb_entry[4]:
                    await update_hb_display(hb_SN)
                alive = True

    # print(len([record for record in hb_click_record if record[1] == hb_SN]))
        

def allocate_points_gaussian_with_minimum(total_points, num_people):
    # Ensure everyone gets at least 1 point
    allocations = [1] * num_people
    remaining_points = total_points - num_people

    for i in range(num_people):
        while True:
            if i == num_people - 1:
                # Allocate all remaining points to the last person
                allocations[i] += remaining_points
                break

            # Calculate mean and standard deviation for the current allocation
            mean = remaining_points / (num_people - i)  # Average points per remaining person
            std_dev = mean / 2  # Standard deviation (adjust as needed)

            # Generate a random allocation using Gaussian distribution
            allocation = max(1, min(remaining_points, int(random.gauss(mean, std_dev))))
            allocation = min(allocation, remaining_points - (num_people - i - 1))  # Ensure enough points remain for others

            if allocation >= 1:
                break
        # Append the allocation and update remaining points
        allocations[i] += allocation
        remaining_points -= allocation
            

    return allocations[0]

async def generate_message_traker_code():
    global msg_tracker_list

    code = ""

    if_generate = False
    while if_generate == False:
        code = ''.join(random.choices(string.digits, k=10))
        await generate_message_traker_code_lock.acquire()
        try:
            if any(tracker[0] == code for tracker in msg_tracker_list):
                pass
            else:
                msg_tracker_list.append([code, None])
                if_generate = True
        except Exception as e:
            print('generate_message_traker_code error: ', str(e))

        finally:
            generate_message_traker_code_lock.release()

    return code

async def gen_cover_id():
    #cover id will be name of file
    cover_id = '0' #default
    #check if duplicate
    while os.path.exists(f'{cover_folder}/{cover_id}.jpg'):
        cover_id = ''.join(random.choices(string.digits, k=10))

    return cover_id

def check_json_format(text):
    try:
        json_object = json.loads(text)
    except ValueError as e:
        return False
    return True

def is_integer(value):
    try:
        value = int(value)
    except:
        return False
    if isinstance(value, int):
        return True
    return False

# communicate with mother api
async def check_pt(chat_id,message_id):
    global transection_list
    global lybot_id
    global lybot_respond_list

    if_success = False
    reason = ""
    rettry_count = 0
    receive_reply = False
    await asyncio.sleep(2)
    await lychat_lock.acquire()
    try:
        while (rettry_count <6 and if_success==False and receive_reply) or (rettry_count==0 and not receive_reply):
            if rettry_count >0:
                await asyncio.sleep(1.5)

            #send message to ly_bot : f'{"chatinfo":"{chat_id}_{message_id}"}"'
            print("send: ", f'{{"chatinfo":"{chat_id}_{message_id}"}}')
            #await aio_bot.send_message(chat_id=ly_bot_id, text=f'{{"chatinfo":"{chat_id}_{message_id}"}}')
            message_traker_code = await generate_message_traker_code()
            await msg_queue_H.put([8,message_traker_code,[lybot_id,""],f'{{"chatinfo":"{chat_id}_{message_id}"}}'])
            # await msg_queue_H.put([8,None,["7501358629",""],f'{{"chatinfo":"{chat_id}_{message_id}"}}'])
            if_get_msd = False
            msg = None
            while if_get_msd==False:
                for tracker in msg_tracker_list:
                    if tracker[0] == message_traker_code and tracker[1] is not None:
                        msg = tracker[1]
                        msg_tracker_list.remove(tracker)
                        if_get_msd = True
                        break
                if if_get_msd==False:
                    await asyncio.sleep(0.5)

            #wait for response message from ly_bot # {"ok": 1, "chatinfo": "-1003409715946_627"}
            print("waiting for lybot response")
            receive_reply = False
            start_time = time.time()
            while (not receive_reply) and time.time() - start_time < lytimeout:
                for lybot_msg in lybot_respond_list:
                    if f"{chat_id}_{message_id}" in lybot_msg.text :
                        print("receive: ", lybot_msg.text)
                        receive_reply = True
                        lybot_respond_list.remove(lybot_msg)
                        #parse message
                        response_data = json.loads(lybot_msg.text)
                        if "ok" in response_data and response_data["ok"] == 1:
                            if_success =  True
                        else:
                            if_success = False
                            reason = "insufficient pt"
                        break
                await asyncio.sleep(0.5)

            rettry_count +=1
                
    except Exception as e:
        traceback.print_exc()
        print('check_pt error: ', str(e))
    finally:
        lychat_lock.release()

    if not receive_reply:
        if_success = False
        reason = "lybot timeout"

    return if_success, reason

async def transfer_pt(hb_SN,sender_id,reciever_id,pt):
    #True, "000000000"
    #False,"error message"
    #{"receiver_id":7839868969,"sender_id":7038631858,"fee":10,"keyword":"测试使用1234"}
    global transection_list
    global lybot_id
    global lybot_respond_list

    

    if_success = False
    info = str(hb_SN)+str(reciever_id)+str(pt)
    transection_id = ""
    await lychat_lock.acquire()
    try:
        message_traker_code = await generate_message_traker_code()
        #send message to ly_bot : f'{"receiver_id":{reciever_id},"sender_id":{sender_id},"fee":{pt},"keyword":"hb_SN"}'
        print("send: ", f'{{"receiver_id":{reciever_id},"sender_id":{sender_id},"receiver_fee":{pt},"keyword":"{info}","memo":"{message_traker_code}"}}')
        #await aio_bot.send_message(chat_id=ly_bot_id, text=f'{{"receiver_id":{reciever_id},"sender_id":{sender_id},"receiver_fee":{pt},"keyword":"{info}"}}')
        
        await msg_queue_H.put([8,message_traker_code,[lybot_id,""],f'{{"receiver_id":{reciever_id},"sender_id":{sender_id},"receiver_fee":{pt},"keyword":"{info}","memo":"{message_traker_code}"}}'])
        # await msg_queue_H.put([8,None,["7501358629",""],f'{{"receiver_id":{reciever_id},"sender_id":{sender_id},"fee":{pt},"keyword":"hb_SN"}}'])
        if_get_msd = False
        msg = None
        while if_get_msd==False:
            for tracker in msg_tracker_list:
                if tracker[0] == message_traker_code and tracker[1] is not None:
                    msg = tracker[1]
                    msg_tracker_list.remove(tracker)
                    if_get_msd = True
                    break
            if if_get_msd==False:
                await asyncio.sleep(0.2)

        #wait for response message from ly_bot 
        #{"ok": null, "status": "reward_self", "receiver_id": 7803797363, "receiver_fee": 0, "memo": "13", "transaction_id": null}
        #{"ok": 1, "status": "insert", "receiver_id": 620917081, "receiver_fee": 0, "memo": "13", "transaction_id": 857350}

        transection_id = None
        receive_reply = False
        start_time = time.time()
        while (not receive_reply) and time.time() - start_time < lytimeout:
            for lybot_msg in lybot_respond_list:
                if f"{reciever_id}" in lybot_msg.text and message_traker_code in lybot_msg.text and info in lybot_msg.text:
                    print("receive: ", lybot_msg.text)
                    receive_reply = True
                    lybot_respond_list.remove(lybot_msg)
                    #parse message
                    response_data = json.loads(lybot_msg.text)
                    if "ok" in response_data and response_data["ok"] == 1:
                        if_success =  True
                        transection_id = response_data["transaction_id"]
                        #transection_id = response_data["status"]
                    else:
                        if_success = False
                        transection_id = response_data["status"]
            await asyncio.sleep(0.2)
    
    except Exception as e:
        traceback.print_exc()
        print('check_pt error: ', str(e))
    finally:
        lychat_lock.release()

    if not receive_reply:
        transection_id = "lybot timeout"

    return if_success , transection_id


# Register the router with the dispatcher
aio_dp.include_router(aio_router)

async def bot_connect() -> None:
    global aio_bot
    global tasks
    global loop
    global start_up_flag
    print("connecting")
    try:
        # Initialize Bot instance with default bot properties which will be passed to all API calls
        aio_bot = Bot(token=aio_bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

        # And the run events dispatching
        start_up_flag = True
        await aio_dp.start_polling(aio_bot)
    except Exception as e:
        while True:
            logging.error('E: main: connect fail: ' + str(e))
            time.sleep(10)
    print("disconnected")

async def check_connection():
    global aio_bot
    await API_action_lock.acquire()
    try:
        await aio_bot.get_me()  # Makes a simple API call to check connection
        return True
    except Exception as e:
        return False
    finally:
        await asyncio.sleep(actionlock_release_interval)
        API_action_lock.release()

#protector
async def check_click_protector(message):
    global clicking_list

    user_id = message.from_user.id

    approve = False
    if_match = False
    for record in clicking_list:
        if_match = True
        if record[0] == user_id:
            if record[1]>max_click_frequency:
                approve = False
                if not record[4]:
                    await msg_queue_L.put([13,message,"你点太快啦！快高潮了.....让我缓缓～"])
                    record[4] = True
            else:
                approve = True

            await record[2].acquire()
            try:
                record[1] += 1
            except Exception as e:
                print('check_click_protector error: ', str(e))
            finally:
                record[2].release()

    if not if_match:
        clicking_list.append([user_id,1,asyncio.Lock(),False,False])
        approve = True

    return approve
    
async def click_protector_guard():
    global clicking_list
    last_check_time = time.time()

    while 1==1:
        try:
            while time.time() - last_check_time < click_protector_refresh_interval:
                await asyncio.sleep(0.1)
            last_check_time = time.time()

            for record in clicking_list:
                await record[2].acquire()
                try:
                    if record[1]>max_click_frequency and not record[3]:
                        record[1] += click_protector_penalty
                        record[3] = True
                    elif record[1]<max_click_frequency and record[3]:
                        record[3] = False
                        record[4] = False
                        if record[1]>0:
                            record[1] -=1
                    else:
                        if record[1]>0:
                            record[1] -=1

                    if record[1]<=0:
                        clicking_list.remove(record)
                except Exception as e:
                    print('click_protector_guard error: ', str(e))
                finally:
                    record[2].release()
        except Exception as e:
            traceback.print_exc()
            print('click_protector_guard main error: ', str(e))




#other
async def msg_transfer_worker():
        global MSG_trans_retry_delay
        global MSG_trans_retry_limit
        global MSG_transfer_rate
        global status_refresh_req_flag
        global status_msg
        global debug_mode
        global RC_chat_last_id
        global file_KEY_transfer_lock
        global start_up_flag
        global msg_tracker_list
        global if_tg_connect


        while not start_up_flag:
            await asyncio.sleep(1)
        
        while 1==1:
            
            retry_flag = True
            retry_count = 0
            fail_code = 0
            Q_select = 0
            msg_element = []
            
            # ms_time = time.time()
            #0
            try:
                if debug_mode:
                    print("msg transfer -0")
                    
                while if_tg_connect == False:
                    print("waiting for reconnect")
                    await asyncio.sleep(60)
            except:
                pass

            # print("msg transfer cycle time 1: {:.3f}".format(time.time() - ms_time))
            
            #1
            try:
                if debug_mode:
                    print("msg transfer -1")
                while msg_queue_H.qsize()==0 and  msg_queue_L.qsize()==0:
                    if debug_mode:
                        print("msg transfer -1.1")
                    await asyncio.sleep(0.3)
                if msg_queue_H.qsize()>0:
                    msg_element = await msg_queue_H.get()
                    Q_select=1
                elif msg_queue_L.qsize()>0:
                    msg_element = await msg_queue_L.get()
                    Q_select=3
                else:
                    retry_flag = False
                    await asyncio.sleep(0.3)
                    raise ValueError("no message(job) to transfer")
            except Exception as e:
                try:
                    if "no message(job) to transfer" in str(e):
                        pass
                    else:
                        print("message transfer error-1: "+ str(e))
                except Exception as e:
                    print('Events handler error(msg transfer1): ', str(e))
                    
            # print("msg transfer cycle time 2: {:.3f}".format(time.time() - ms_time))
            
            #2
            
            try:
                if debug_mode:
                    print("msg transfer -2")
                msg_element_group = []
                if msg_element[0] == 0:
                    msg_element_group.append(msg_element)
                    # for msg in split_string(msg_element[2], 4096):
                    #     msg_element_group.append([0,msg_element[1],msg])
                elif msg_element[0] == 1:
                    msg_element_group.append(msg_element)
                    # for msg in split_string(msg_element[2], 4096):
                    #     msg_element_group.append([1,msg_element[1],msg])
                elif msg_element[0] == 2:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 3:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 4:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 5:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 6:
                    msg_element_group.append(msg_element)
                    # for msg in split_string(msg_element[2], 4096):
                    #     msg_element_group.append([6,msg_element[1],msg])
                elif msg_element[0] == 7:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 8:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 9:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 10:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 11:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 12:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 13:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 14:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 15:
                    msg_element_group.append(msg_element)
                elif msg_element[0] == 16:
                    msg_element_group.append(msg_element)

            except Exception as e:
                if msg_element[0] == 7:
                    RC_chat_last_id = -1
                print('Events handler error(msg transfer2): ', str(e))
                
            # print("msg transfer cycle time 3: {:.3f}".format(time.time() - ms_time))
                    
            #3        
            while retry_flag:
                if debug_mode:
                    print("msg transfer -3")
                    
                retry_flag = False
                
                while if_tg_connect == False:
                    print("waiting for reconnect")
                    await asyncio.sleep(60)
                    
                try:
                    if retry_count>0 and debug_mode:
                        print("msg transfer retry../n reason: "+str(fail_code))
                        pass
                except Exception as e:
                    pass
                try:
                    if msg_element_group[0][0] == 0:#reply 1 with 2
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element[1].reply(sub_msg_element[2])
                    elif msg_element_group[0][0] == 1: #edit 1 text to 2
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element[1].edit_text(sub_msg_element[2])
                    elif msg_element_group[0][0] == 2:# delete 1
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element.delete()
                    elif  msg_element_group[0][0] == 3:#forward 1 to cht id 2
                        for sub_msg_element in msg_element_group:
                            await aio_bot.forward_message(chat_id=sub_msg_element[2], from_chat_id=sub_msg_element[1].chat.id, message_id=sub_msg_element[1].message_id)
                    elif  msg_element_group[0][0] == 4:#reply messsage(1) with text(2) and buttons(3)
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element[1].reply(sub_msg_element[2], reply_markup=sub_msg_element[3])
                    elif  msg_element_group[0][0] == 5:#reply messsage(1) with text(2) and buttons(3)
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element[1].anwser(sub_msg_element[2], reply_markup=sub_msg_element[3])
                    elif  msg_element_group[0][0] == 6:# direct answer
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element[1].answer(sub_msg_element[2])
                    elif  msg_element_group[0][0] == 7: #answer 1 with alert message 2
                        for sub_msg_element in msg_element_group:
                            await sub_msg_element[1].answer(sub_msg_element[2], show_alert=True)
                    elif  msg_element_group[0][0] == 8:
                        for sub_msg_element in msg_element_group:#send message(3) to chat id(2) with parse_mode(4) (could have reply_markup(5))
                            msg = None
                            s_time = time.time()
                            if len(sub_msg_element)==4:
                                if len(str(sub_msg_element[2][1]))>0:
                                    msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], message_thread_id=sub_msg_element[2][1])
                                else:
                                    msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3])
                            elif len(sub_msg_element)==5:
                                if len(str(sub_msg_element[2][1]))>0:
                                    msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], message_thread_id=sub_msg_element[2][1], parse_mode=sub_msg_element[4])
                                else:
                                    msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], parse_mode=sub_msg_element[4])
                            elif len(sub_msg_element)==6:
                                if len(str(sub_msg_element[2][1]))>0:
                                    msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], message_thread_id=sub_msg_element[2][1], reply_markup=sub_msg_element[5], parse_mode=sub_msg_element[4])
                                else:
                                    msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], reply_markup=sub_msg_element[5], parse_mode=sub_msg_element[4])

                            print("send message time: ", time.time() - s_time)
                            #update msg_tracker
                            for tracker in msg_tracker_list:
                                if tracker[0] == sub_msg_element[1]:
                                    tracker[1] = msg
                                    break
                    elif  msg_element_group[0][0] == 9:
                        for sub_msg_element in msg_element_group:#edit message(2) in chat id(1) with message id(3) with parse_mode(4) (could have reply_markup(5) )
                            if len(sub_msg_element)==4:
                                await aio_bot.edit_message_text(chat_id=int(sub_msg_element[1][0]), message_id=sub_msg_element[3], text=sub_msg_element[2])
                            elif len(sub_msg_element)==5:
                                await aio_bot.edit_message_text(chat_id=int(sub_msg_element[1][0]), message_id=sub_msg_element[3], text=sub_msg_element[2], parse_mode=sub_msg_element[4])
                            elif len(sub_msg_element)==6:
                                await aio_bot.edit_message_text(chat_id=int(sub_msg_element[1][0]), message_id=sub_msg_element[3], text=sub_msg_element[2], reply_markup=sub_msg_element[5], parse_mode=sub_msg_element[4])

                    elif  msg_element_group[0][0] == 10:
                        for sub_msg_element in msg_element_group:
                            msg = None
                            s_time = time.time()
                            if len(sub_msg_element)==6:
                                if len(str(sub_msg_element[3][1]))>0:
                                    msg = await aio_bot.send_photo(chat_id=int(sub_msg_element[3][0]), photo=sub_msg_element[2], caption=sub_msg_element[4], message_thread_id=sub_msg_element[3][1], parse_mode=sub_msg_element[5])
                                else:
                                    msg = await aio_bot.send_photo(chat_id=int(sub_msg_element[3][0]), photo=sub_msg_element[2], caption=sub_msg_element[4], parse_mode=sub_msg_element[5])
                            elif len(sub_msg_element)==7:
                                if len(str(sub_msg_element[3][1]))>0:
                                    msg = await aio_bot.send_photo(chat_id=int(sub_msg_element[3][0]), photo=sub_msg_element[2], caption=sub_msg_element[4], message_thread_id=sub_msg_element[3][1], reply_markup=sub_msg_element[6], parse_mode=sub_msg_element[5])
                                else:
                                    msg = await aio_bot.send_photo(chat_id=int(sub_msg_element[3][0]), photo=sub_msg_element[2], caption=sub_msg_element[4], reply_markup=sub_msg_element[6], parse_mode=sub_msg_element[5])

                            print("send photo time: ", time.time() - s_time)
                            #update msg_tracker
                            for tracker in msg_tracker_list:
                                if tracker[0] == sub_msg_element[1]:
                                    tracker[1] = msg
                                    break
                    elif  msg_element_group[0][0] == 11:
                        for sub_msg_element in msg_element_group:
                            if len(sub_msg_element)==6:
                                await aio_bot.edit_message_caption(chat_id=int(sub_msg_element[2][0]), message_id=sub_msg_element[4], caption=sub_msg_element[3], parse_mode=sub_msg_element[5])
                            elif len(sub_msg_element)==7:
                                await aio_bot.edit_message_caption(chat_id=int(sub_msg_element[2][0]), message_id=sub_msg_element[4], caption=sub_msg_element[3], reply_markup=sub_msg_element[6], parse_mode=sub_msg_element[5])

                    elif msg_element_group[0][0] == 12: #same as 8 but ultra fast
                        for sub_msg_element in msg_element_group:#send message(3) to chat id(2) with parse_mode(4) (could have reply_markup(5))
                            asyncio.create_task(msg_transfer_task_8(sub_msg_element))
                    elif msg_element_group[0][0] == 13: #same as 7 but ultra fast
                        for sub_msg_element in msg_element_group: #answer 1 with alert message 2
                            asyncio.create_task(msg_transfer_task_7(sub_msg_element))
                    elif msg_element_group[0][0] == 14:#pin message 2 in chat id 1
                        for sub_msg_element in msg_element_group:
                            await aio_bot.pin_chat_message(chat_id=int(sub_msg_element[1]), message_id=sub_msg_element[2], disable_notification=True)
                    elif msg_element_group[0][0] == 15:#unpin message 2 in chat id 1
                        for sub_msg_element in msg_element_group:
                            await aio_bot.unpin_chat_message(chat_id=int(sub_msg_element[1]), message_id=sub_msg_element[2])

                except Exception as e:
                    try:
                        if "no message(job) to transfer" in str(e):
                            fail_code=1
                            pass
                        elif "Content of the message was not modified (caused by EditMessageRequest)" in str(e):
                            fail_code=2
                            pass
                        elif "A wait of" in str(e) and "seconds is required" in str(e):
                            fail_code=3
                            print('message transfer retry: ', str(e))
                            await API_action_lock.acquire()
                            try:
                                await asyncio.sleep(int(str(e).split(" ")[3]))
                            except Exception as e:
                                print('Events handler error(msg transfer3-2): ', str(e))
                            finally:
                                await asyncio.sleep(actionlock_release_interval)
                                API_action_lock.release()
                            retry_flag = True
                        elif "The specified message ID is invalid or you can't do that operation on such message" in str(e):
                            fail_code=4
                        elif "You can't forward messages from a protected chat" in str(e):
                            fail_code=5
                            #status_refresh_req_flag =True
                        elif "The channel specified is private and you lack permission to access it" in str(e):
                            fail_code=6
                            status_refresh_req_flag =True
                        elif "Invalid channel object. Make sure to pass the right types, for instance making sure that the request is designed for channels or otherwise look for a different one more suited" in str(e):
                            fail_code=7
                            status_refresh_req_flag =True
                        elif "The provided media object is invalid or the current account may not be able to send it" in str(e):
                            fail_code=8
                        elif "Telegram server says - Bad Request: query is too old and response timeout expired or query ID is invalid" in str(e):
                            fail_code=11
                            print("msg_trans err 11")
                        elif "Telegram server says - Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message" in str(e):
                            fail_code=12
                            pass
                        elif (msg_element[0] == 4 or msg_element[0] == 5) and retry_count<=MSG_trans_retry_limit:
                            fail_code=9
                            retry_flag = True
                            print("message transfer error-3 code 9: "+str(msg_element[0])+": "+ str(e))
                            await asyncio.sleep(MSG_trans_retry_delay)
                        elif "Telegram server says - Flood control exceeded on method" in str(e):
                            print("message transfer flood control exceeded: ")
                            await asyncio.sleep(10)
                        elif "Telegram server says - Bad Request: not enough rights to manage pinned messages in the" in str(e):
                            fail_code=13
                            print("not enough rights to manage pinned messages")
                        elif "Telegram server says - Bad Request: query is too old and response timeout" in str(e):
                            fail_code=14
                            print("query is too old and response timeout")
                        elif "Telegram server says - Bad Request: not enough rights to manage pinned messages " in str(e):
                            fail_code=15
                            print("not enough rights to manage pinned messages")
                        else:
                            fail_code=10
                            traceback.print_exc()
                            print("message transfer error-3: "+str(msg_element[0])+": "+ str(e))
                            print(msg_element)
                            
                        if msg_element[0] == 7 and retry_flag == False:
                            RC_chat_last_id = -1
                    except Exception as e:
                        traceback.print_exc()
                        print('Events handler error(msg transfer3): ', str(e))
                        
                await asyncio.sleep(MSG_transfer_rate)

            # print("msg transfer cycle time 4: {:.3f}".format(time.time() - ms_time))
            #4
            try:
                if debug_mode:
                    print("msg transfer -4")
                    
                if Q_select==1:
                    msg_queue_H.task_done()
                elif Q_select==3:
                    msg_queue_L.task_done()
                else:
                    pass
            except Exception as e:
                try:
                    print("message transfer error-4: "+ str(e))
                except Exception as e:
                    print('Events handler error(msg transfer4): ', str(e))

            # print("msg transfer cycle time 5: {:.3f}".format(time.time() - ms_time))
        print("msg WK fail!!!!")

async def msg_transfer_task_8(sub_msg_element):
    global msg_tracker_list
    msg = None
    if len(sub_msg_element)==4:
        if len(str(sub_msg_element[2][1]))>0:
            msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], message_thread_id=sub_msg_element[2][1])
        else:
            msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3])
    elif len(sub_msg_element)==5:
        if len(str(sub_msg_element[2][1]))>0:
            msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], message_thread_id=sub_msg_element[2][1], parse_mode=sub_msg_element[4])
        else:
            msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], parse_mode=sub_msg_element[4])
    elif len(sub_msg_element)==6:
        if len(str(sub_msg_element[2][1]))>0:
            msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], message_thread_id=sub_msg_element[2][1], reply_markup=sub_msg_element[5], parse_mode=sub_msg_element[4])
        else:
            msg = await aio_bot.send_message(chat_id=int(sub_msg_element[2][0]), text=sub_msg_element[3], reply_markup=sub_msg_element[5], parse_mode=sub_msg_element[4])

    #update msg_tracker
    for tracker in msg_tracker_list:
        if tracker[0] == sub_msg_element[1]:
            tracker[1] = msg
            break
      
async def msg_transfer_task_7(sub_msg_element):
    await sub_msg_element[1].answer(sub_msg_element[2], show_alert=True)

async def daily_qualify_reset():
    #reset_daily_qualify everyday 00:00+08:00 UTC
    while True:
        current_time = datetime.utcnow() + timedelta(hours=8)
        if current_time.hour == 0 and current_time.minute == 0:

            await daily_qualify_reset_lock.acquire()
            try:
                await DB_reset_daily_qualify()
                print("daily qualify reset")
            except Exception as e:
                print('daily_qualify_reset error: ', str(e))
            finally:
                daily_qualify_reset_lock.release()

            await asyncio.sleep(61)

        await asyncio.sleep(1)

async def auto_confiscate():
    global hb_pool
    global if_tg_connect
    global confiscate_time_limit
    #confiscate hb not claimed for more than 3 HOURS every 1 minutes
    while True:
        while if_tg_connect == False:
            print("waiting for reconnect")
            await asyncio.sleep(60)
        try:
            for hb_entry in hb_pool:
                if hb_entry[3] == "ongoing":
                    if datetime.now(timezone.utc) - datetime.fromisoformat(hb_entry[6]) > timedelta(seconds=confiscate_time_limit) :
                        print(f"HBSN: {hb_entry[0]} confiscate triggered")
                        await hb_entry[12].acquire()
                        try:
                            confiscate_time_pt = hb_entry[8][2]
                            transection_result = await transfer_pt(hb_entry[0],hb_entry[10][0], confiscate_target, confiscate_time_pt)
                            if transection_result[0]:
                                hb_entry[9][2] = 0
                                hb_entry[8][2] = 0
                            else:
                                print("auto_confiscate transfer_pt fail:", transection_result[1])
                            


                        except Exception as e:
                            print('auto_confiscate error-2: ', str(e))
                        finally:
                            hb_entry[12].release()

                        print("auto confiscate hb_SN:", hb_entry[0])
            
        except Exception as e:
            traceback.print_exc()
            print('auto_confiscate error-1: ', str(e))
        
        await asyncio.sleep(60)

async def track_connection():
    global if_tg_connect
    while True:
        connection_status = await check_connection()
        if connection_status:
            if_tg_connect = True
        else:
            if_tg_connect = False
            
        # print("TG connection status: ", if_tg_connect)
        await asyncio.sleep(10)

async def load_unfinished_hb():
    global hb_pool


    await DB_get_hb_pool()

    #update reveiver list
    for hb_entry in hb_pool:
        reciever_list = await query_hb_record_receiver(hb_entry[0])
        for reciever in reciever_list:
            hb_entry[11].append([reciever[0], reciever[1], reciever[2], reciever[3], reciever[4]])


    for hb_entry in hb_pool:
        if hb_entry[3] == "ongoing":
            hb_entry[12] = asyncio.Lock()
            hb_entry[-1] = True
            asyncio.create_task(hb_handler(hb_entry[0]))
            print("loaded unfinished hb_SN:", hb_entry[0])

async def autoreload():
    while True:
        await asyncio.sleep(15)
        await get_hb_list()

async def startup():
    print("luckynyabot starting up")
    init_env()
    await read_config()
    await connect_DB()
    await get_hb_list()
    await load_unfinished_hb()
    print("luckynyabot started")
    # await read_DB_config()

async def read_config():
    global DB_info
    config_path = path.join(path.dirname(__file__), 'config.json')
    if path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            #DB_info[] = [server, user, password, database]
            DB_info = [
                config["database"]["DB_server"],
                config["database"]["DB_user"],
                config["database"]["DB_password"],
                config["database"]["DB_database"]
            ]
            print("config.json read successfully")
    else:
        
        print("config.json not found, exiting")
        sys.exit(1)

async def connect_DB():
    global DB_info
    global DB_conn
    
    DB_conn_tmp = DB_conn
    await DB_action_lock.acquire()
    try:
        DB_conn = pymssql.connect(server=DB_info[0],user=DB_info[1],password=DB_info[2],database=DB_info[3],as_dict=True)
        print("DB connected")
    except Exception as e:
        DB_conn = DB_conn_tmp
        print('connect_DB: '+str(e))
    finally:
        DB_action_lock.release()


#DB actions
async def get_hb_list(): #hongbao
    global DB_conn
    global hb_list #[[id,hbid,hbname,proccess_count,bot_id],[...]]
    await DB_action_lock.acquire()
    hb_list_temp = []
    try:
        cursor = DB_conn.cursor()
        cursor.execute("SELECT * FROM [TG_recordbot].[dbo].[list_hb] ")
        rows = cursor.fetchall()
        for row in rows:
            hb_list_temp.append([row['id'],row['hbid'],row['hbname'],row['topic_id'],row['proccess_count'],row['bot_id']])
             
        hb_list = hb_list_temp
    except Exception as e:
        logging.error('E: get_hb_list: '+str(e))
    finally:
        DB_action_lock.release()

async def add_hb_list(hbid,hbname,topic_id,bot_id): #hongbao
    global DB_conn
    global hb_list #[[id,hbid,hbname,proccess_count,bot_id],[...]]
    await DB_action_lock.acquire()
    try:
        cursor = DB_conn.cursor()
        cursor.execute("INSERT INTO [TG_recordbot].[dbo].[list_hb] (hbid,hbname,topic_id,proccess_count,bot_id) VALUES (%s,%s,%s,%d,%s) ",(hbid,hbname,topic_id,0,bot_id))
        DB_conn.commit()
        hb_list.append([cursor.lastrowid,hbid,hbname,0,bot_id])
             
    except Exception as e:
        logging.error('E: add_hb_list: '+str(e))
    finally:
        DB_action_lock.release()

async def update_hb_proccess_count(hbid,proccess_count,bot_id): #hongbao
    global DB_conn
    global hb_list #[[id,hbid,hbname,topic_id,proccess_count,bot_id]]
    await DB_action_lock.acquire()
    try:
        cursor = DB_conn.cursor()
        cursor.execute("UPDATE [TG_recordbot].[dbo].[list_hb] SET proccess_count=%d WHERE hbid=%s AND bot_id=%s ",(proccess_count,hbid,bot_id))
        DB_conn.commit()
        for hb in hb_list:
            if hb[1] == hbid and hb[5] == bot_id:
                hb[4] = proccess_count
                break
             
    except Exception as e:
        logging.error('E: update_hb_proccess_count: '+str(e))
    finally:
        DB_action_lock.release()

async def remove_hb_list(hbid,topic_id,bot_id): #hongbao
    global DB_conn
    global hb_list #[[id,hbid,hbname,topic_id,proccess_count,bot_id]]

    if len(topic_id)>0:
        await DB_action_lock.acquire()
        try:
            cursor = DB_conn.cursor()
            cursor.execute("DELETE FROM [TG_recordbot].[dbo].[list_hb] WHERE hbid=%s AND topic_id=%s AND bot_id=%s ",(hbid,topic_id,bot_id))
            DB_conn.commit()
            for hb in hb_list:
                if hb[1] == hbid and hb[3] == topic_id and hb[5] == bot_id:
                    hb_list.remove(hb)
                    break
             
        except Exception as e:
            logging.error('E: remove_hb: '+str(e))
        finally:
            DB_action_lock.release()
    else:
        await DB_action_lock.acquire()
        try:
            cursor = DB_conn.cursor()
            cursor.execute("DELETE FROM [TG_recordbot].[dbo].[list_hb] WHERE hbid=%s AND bot_id=%s ",(hbid,bot_id))
            DB_conn.commit()
            for hb in hb_list:
                if hb[1] == hbid and hb[5] == bot_id:
                    hb_list.remove(hb)
                    break
                
        except Exception as e:
            logging.error('E: remove_hb: '+str(e))
        finally:
            DB_action_lock.release()

async def add_hb_record(record): #hongbao record
    global DB_conn
    global hb_records 
    await DB_action_lock.acquire()
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "INSERT INTO [TG_recordbot].[dbo].[hb_record] "
            "(hbid,hb_SN,sender_id,sender_name,reciever_id,reciever_name,approved,reciver_reaction_time,send_point,recieve_point,transection_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ",
            (
                record["hbid"],
                record["hb_SN"],
                record["sender_id"],
                record["sender_name"],
                record["reciever_id"],
                record["reciever_name"],
                record["approved"],
                record["reciver_reaction_time"],
                record["send_point"],
                record["recieve_point"],
                record["transection_id"]
            )
        )
        DB_conn.commit()
        hb_records.append(record)
             
    except Exception as e:
        logging.error('E: add_hb_record: '+str(e))
    finally:
        DB_action_lock.release()

async def query_hb_record(hb_SN,reciever_id): #hongbao record
    global DB_conn
    global hb_records 
    record_found = None
    await DB_action_lock.acquire()
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT * FROM [TG_recordbot].[dbo].[hb_record] WHERE hb_SN=%s AND reciever_id=%s ",
            (hb_SN,reciever_id)
        )
        row = cursor.fetchone()
        if row:
            record_found = {
                "id": row['id'],
                "hbid": row['hbid'],
                "hb_SN": row['hb_SN'],
                "sender_id": row['sender_id'],
                "sender_name": row['sender_name'],
                "reciever_id": row['reciever_id'],
                "reciever_name": row['reciever_name'],
                "approved": row['approved'],
                "reciver_reaction_time": row['reciver_reaction_time'],
                "send_point": row['send_point'],
                "recieve_point": row['recieve_point']
            }
        else:
            record_found = None
             
    except Exception as e:
        logging.error('E: query_hb_record: '+str(e))
    finally:
        DB_action_lock.release()
    return record_found
   
async def query_hb_record_receiver(hb_SN): #hongbao record
    global DB_conn
    global hb_records 
    receivers = []
    await DB_action_lock.acquire()
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT * FROM [TG_recordbot].[dbo].[hb_record] WHERE hb_SN=%s ",
            (hb_SN,)
        )
        rows = cursor.fetchall()
        for row in rows:
            new_reciever = []
            new_reciever.append(row['reciever_id'])
            new_reciever.append(row['reciever_name'])
            new_reciever.append(row['approved'])
            new_reciever.append(row['recieve_point'])
            new_reciever.append(row['reciver_reaction_time'])
            receivers.append(new_reciever)
             
    except Exception as e:
        logging.error('E: query_hb_record_receiver: '+str(e))
    finally:
        DB_action_lock.release()
    return receivers

async def DB_regist_user(message: types.Message):
    global DB_conn
    # [uid],[user_name],[cover_msg_id],[cover_id],[admin],[strick_topic]

    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "IF NOT EXISTS (SELECT 1 FROM [TG_recordbot].[dbo].[hb_user] WHERE uid=%s) "
            "BEGIN "
            "INSERT INTO [TG_recordbot].[dbo].[hb_user] (uid, user_name) "
            "VALUES (%s, %s) "
            "END",
            (str(message.from_user.id), str(message.from_user.id), str(message.from_user.full_name))
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_regist_user: '+str(e))

async def DB_set_user_admin(user_id, is_admin: bool):
    global DB_conn
    # [uid],[user_name],[cover_msg_id],[cover_id],[admin],[strick_topic]

    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "UPDATE [TG_recordbot].[dbo].[hb_user] SET admin=%d WHERE uid=%s ",
            (1 if is_admin else 0, str(user_id))
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_set_user_admin: '+str(e))

async def DB_set_user_strick_topic(user_id, is_strick: bool):
    global DB_conn
    # [uid],[user_name],[cover_msg_id],[cover_id],[admin],[strick_topic]

    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "UPDATE [TG_recordbot].[dbo].[hb_user] SET strick_topic=%d WHERE uid=%s ",
            (1 if is_strick else 0, str(user_id))
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_set_user_strick_topic: '+str(e))

async def DB_if_user_admin(message: types.Message):
    global DB_conn
    # [uid],[user_name],[cover_msg_id],[cover_id],[admin],[strick_topic]
    is_admin = False
    await DB_regist_user(message)
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT admin FROM [TG_recordbot].[dbo].[hb_user] WHERE uid=%s ",
            (str(message.from_user.id),)
        )
        row = cursor.fetchone()
        if row:
            if row['admin'] == 1:
                is_admin = True
            else:
                is_admin = False
        else:
            is_admin = False
    except Exception as e:
        logging.error('E: DB_if_user_admin: '+str(e))
    return is_admin

async def DB_if_user_strick_topic(message: types.Message):
    global DB_conn
    # [uid],[user_name],[cover_msg_id],[cover_id],[admin],[strick_topic]
    is_strick = True
    await DB_regist_user(message)
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT strick_topic FROM [TG_recordbot].[dbo].[hb_user] WHERE uid=%s ",
            (str(message.from_user.id),)
        )
        row = cursor.fetchone()
        if row:
            if row['strick_topic'] == 1:
                is_strick = True
            else:
                is_strick = False
        else:
            is_strick = False
    except Exception as e:
        logging.error('E: DB_if_user_strick_topic: '+str(e))
    return is_strick

async def DB_set_hb_cover(message: types.Message,cover_msg_id,cover_id):
    global DB_conn
    # [uid],[user_name],[cover_msg_id],[cover_id],[admin],[strick_topic]

    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "UPDATE [TG_recordbot].[dbo].[hb_user] SET cover_msg_id=%s, cover_id=%s WHERE uid=%s ",
            (str(cover_msg_id), str(cover_id), str(message.from_user.id))
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: set_hb_cover: '+str(e))
    
async def DB_get_hb_cover_by_prompt(promt):
    #reutrn cover_id
    global DB_conn
    cover_id = ["0","0"]
    try:
        cursor = DB_conn.cursor()
        #SELECT TOP (1000) [promt] ,[cover_id] FROM [TG_recordbot].[dbo].[hb_cover]
        cursor.execute(
            "SELECT TOP (1) cover_id, file_id FROM [TG_recordbot].[dbo].[hb_cover] WHERE promt=%s",
            (str(promt),)
        )
        row = cursor.fetchone()
        if row:
            cover_id = [row['cover_id'], row['file_id']]

        else:
            cover_id = ["0","0"]
    except Exception as e:
        logging.error('E: query_hb_cover: '+str(e))
    return cover_id

async def DB_get_hb_cover_by_id(id):
    global DB_conn
    cover_id = ["0","0"]
    try:
        cover_list  =  await DB_get_all_cover()
        for cover_entry in cover_list:
            if int(cover_entry[0]) == int(id):
                cover_id = [cover_entry[3], cover_entry[2]]
                return cover_id

        return cover_id
    except Exception as e:
        traceback.print_exc()
        logging.error('E: DB_get_hb_cover_by_id: '+str(e))

async def DB_add_hb_cover(promt,file_id,cover_id,cover_info):
    global DB_conn
    #check if promt exist if exist remove it
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "DELETE FROM [TG_recordbot].[dbo].[hb_cover] WHERE promt=%s ",
            (str(promt),)
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_add_hb_cover remove existing: '+str(e))

    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "INSERT INTO [TG_recordbot].[dbo].[hb_cover] (promt, file_id, cover_id, info) VALUES (%s, %s, %s, %s) ",
            (str(promt), str(file_id), str(cover_id), str(cover_info))
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: add_hb_cover: '+str(e))

async def DB_get_hb_cover_by_user_id(user_id):
    #reutrn cover_id
    global DB_conn
    cover_id = ["0","0"]
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT TOP (1) cover_id, cover_msg_id FROM [TG_recordbot].[dbo].[hb_user] WHERE uid=%s ",
            (str(user_id),)
        )
        row = cursor.fetchone()
        if row:
            cover_id = row['cover_id']
            if not cover_id:
                cover_id = ["0","0"]
            else:
                if (len(row['cover_id'])<=1) or (len(row['cover_msg_id'])<=1):
                    cover_id = ["0","0"]
                else:
                    cover_id =[row['cover_id'], row['cover_msg_id']]
        else:
            cover_id = ["0","0"]
    except Exception as e:
        logging.error('E: DB_get_hb_cover_by_user_id: '+str(e))
    return cover_id

async def DB_get_all_cover():
    global DB_conn
    cover_list = []
    try:
        cursor = DB_conn.cursor()
        cursor.execute("SELECT * FROM [TG_recordbot].[dbo].[hb_cover] ")
        rows = cursor.fetchall()
        i= 1
        for row in rows:
            cover_entry  = []
            cover_entry.append(i)
            cover_entry.append(row['promt'])
            cover_entry.append(row['file_id'])
            cover_entry.append(row['cover_id'])
            cover_entry.append(row['info'])

            cover_list.append(cover_entry)

            i+=1
             
    except Exception as e:
        logging.error('E: DB_get_all_cover: '+str(e))

    return cover_list

async def DB_get_all_caption():
    #@SELECT TOP (1000) [captionid] ,[caption] FROM [TG_recordbot].[dbo].[hb_caption]
    global DB_conn
    caption_list = []
    try:
        cursor = DB_conn.cursor()
        cursor.execute("SELECT * FROM [TG_recordbot].[dbo].[hb_caption] ")
        rows = cursor.fetchall()
        for row in rows:
            caption_entry  = []
            caption_entry.append(row['captionid'])
            caption_entry.append(row['caption'])

            caption_list.append(caption_entry)
             
    except Exception as e:
        logging.error('E: DB_get_all_caption: '+str(e))
    return caption_list

async def DB_add_caption(caption):
    global DB_conn
    #check if caption exist if exist remove it
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "INSERT INTO [TG_recordbot].[dbo].[hb_caption] (caption) VALUES (%s) ",
            (str(caption),)
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_add_caption: '+str(e))

async def DB_remove_caption(captionid):
    global DB_conn
    #check if caption exist if exist remove it
    #get captionid
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "DELETE FROM [TG_recordbot].[dbo].[hb_caption] WHERE captionid=%d ",
            (str(captionid),)
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_remove_caption: '+str(e))

async def DB_remove_hb_cover(promt):
    global DB_conn
    #check if promt exist if exist remove it
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "DELETE FROM [TG_recordbot].[dbo].[hb_cover] WHERE promt=%s ",
            (str(promt),)
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_remove_hb_cover: '+str(e))

async def DB_HB_set_caption(hb_SN,caption_text):
    #update hb_message of hb_SN
    global DB_conn
    global hb_pool
    try:
        for hb in hb_pool:
            if hb[0] == hb_SN:
                cursor = DB_conn.cursor()
                cursor.execute(
                    "UPDATE [TG_recordbot].[dbo].[hb_pool] SET caption_text=%s WHERE hb_SN=%s ",
                    (caption_text, hb_SN)
                )
                DB_conn.commit()
                break
    except Exception as e:
        logging.error('E: DB_hb_set_caption: '+str(e))

async def DB_proof_daily_qualify(user_id,chat_id):
    #INSERT INTO [dbo].[hb_daily_qualify]([userid],[chat_id])

    global DB_conn
    #check record exist
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT TOP (1) userid FROM [TG_recordbot].[dbo].[hb_daily_qualify] WHERE userid=%s AND chat_id=%s ",
            (str(user_id), str(chat_id))
        )
        row = cursor.fetchone()
        if row:
            return True
    except Exception as e:
        logging.error('E: DB_proof_daily_qualify check exist: '+str(e))
        return False
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "INSERT INTO [TG_recordbot].[dbo].[hb_daily_qualify] (userid, chat_id) VALUES (%s, %s) ",
            (str(user_id), str(chat_id))
        )
        DB_conn.commit()
    except Exception as e:
        logging.error('E: DB_proof_daily_qualify: '+str(e))

async def DB_check_daily_qualify(user_id,chat_id):
    #SELECT TOP (1000) [userid] ,[chat_id] ,[qualify_time] FROM [TG_recordbot].[dbo].[hb_daily_qualify]
    global DB_conn
    qualify = False
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "SELECT TOP (1) userid FROM [TG_recordbot].[dbo].[hb_daily_qualify] WHERE userid=%s AND chat_id=%s ",
            (str(user_id), str(chat_id))
        )
        row = cursor.fetchone()
        if row:
            qualify = True
    except Exception as e:
        logging.error('E: check_daily_qualify: '+str(e))
    return qualify

async def DB_reset_daily_qualify():
    #DELETE FROM [TG_recordbot].[dbo].[hb_daily_qualify]
    global DB_conn
    try:
        cursor = DB_conn.cursor()
        cursor.execute("DELETE FROM [TG_recordbot].[dbo].[hb_daily_qualify] ")
        DB_conn.commit()
    except Exception as e:
        logging.error('E: reset_daily_qualify: '+str(e))

#DB logic
async def DB_hb_msg_id(hb_SN,msg_id,if_cover):
    #update hb_message_id of hb_SN
    global DB_conn
    global hb_pool
    try:
        for hb in hb_pool:
            if hb[0] == hb_SN:
                cursor = DB_conn.cursor()
                cursor.execute(
                    "UPDATE [TG_recordbot].[dbo].[hb_pool] SET hb_msg_id=%d, if_cover=%d WHERE hb_SN=%s ",
                    (msg_id, 1 if if_cover else 0, hb_SN)
                )
                DB_conn.commit()
                break
    except Exception as e:
        logging.error('E: DB_hb_msg_id: '+str(e))

async def DB_update_hb_pool_record(hb_SN):
    #update hbid, status, max_pt, sent_pt, remain_pt, max_amount, sent_amount, remain_amount,[sender_id,sender_name],Allocation_method of hb_SN,if_cover
    
    global DB_conn
    global hb_pool
    try:
        for hb in hb_pool:
            if hb[0] == hb_SN:
                cursor = DB_conn.cursor()
                cursor.execute(
                    "UPDATE [TG_recordbot].[dbo].[hb_pool] SET hbid=%s, status=%s, max_pt=%d, sent_pt=%d, remain_pt=%d, max_amount=%d, sent_amount=%d, remain_amount=%d, sender_id=%s, sender_name=%s, Allocation_method=%s, request_message_id = %s, hb_message = %s, topic_id = %s, if_cover = %d   WHERE hb_SN=%s ",
                    (hb[1], hb[3], hb[8][0], hb[8][1], hb[8][2], hb[9][0], hb[9][1], hb[9][2], hb[10][0], hb[10][1], hb[7], hb[5], hb[14], hb[2],1 if hb[15] else 0, hb_SN)
                )
                DB_conn.commit()
                break
    except Exception as e:
        logging.error('E: DB_update_hb_pool_record: '+str(e))

async def DB_get_hb_SN():
    global DB_conn
    global hb_pool

    # Get the current time with timezone info
    current_time = datetime.now(timezone.utc).astimezone()

    # Format the time to match datetimeoffset(7)
    formatted_time = current_time.isoformat(timespec='microseconds')

    #try go to db table: hb_pool and create a new record, than field hb_SN(AI) is the new hb_SN
    #[hb_SN],[hbid]->"",[status]-> set to "create",[create_time] ->set to current time with format datetimeoffset(7),[max_pt]->0,[sent_pt]->0,[remain_pt]->0
    try:
        cursor = DB_conn.cursor()
        cursor.execute(
            "INSERT INTO [TG_recordbot].[dbo].[hb_pool] (hbid, status, create_time, max_pt, sent_pt, remain_pt, max_amount, sent_amount, remain_amount) "
            "OUTPUT INSERTED.hb_SN "
            "VALUES (%s, %s, %s, %d, %d, %d, %d, %d, %d)",
            ("", "create", formatted_time, 0, 0, 0, 0, 0, 0)
        )
        row = cursor.fetchone()
        DB_conn.commit()
        return row['hb_SN'],formatted_time
    except Exception as e:
        logging.error('E: DB_get_hb_SN: '+str(e))
        return None, None


async def DB_get_hb_pool(): #get unfinished hb
    global DB_conn
    global hb_pool
    await DB_action_lock.acquire()
    try:
        cursor = DB_conn.cursor()
        cursor.execute("SELECT * FROM [TG_recordbot].[dbo].[hb_pool] WHERE status IN (%s, %s) ",("create","ongoing"))
        rows = cursor.fetchall()
        for row in rows:

            hb_entry = [
                int(row['hb_SN']),
                str(row['hbid']),
                str(row['topic_id']),
                str(row['status']),
                str(row['hb_msg_id']),
                str(row['request_message_id']),
                str(row['create_time']),
                str(row['Allocation_method']),
                [int(row['max_pt']), int(row['sent_pt']), int(row['remain_pt'])],
                [int(row['max_amount']), int(row['sent_amount']), int(row['remain_amount'])],
                [str(row['sender_id']), str(row['sender_name'])],
                [],
                asyncio.Lock(),
                datetime.now(timezone.utc).astimezone() - timedelta(seconds=3),
                str(row['hb_message']),
                True if str(row['if_cover']) == 'True' else False,
                str(row['caption_text']),
                True
            ]
            hb_pool.append(hb_entry)
             
    except Exception as e:
        logging.error('E: DB_get_hb_pool: '+str(e))
    finally:
        DB_action_lock.release()

def init_env():
    #check if cover_folder exists
    if not path.exists(cover_folder):
        os.makedirs(cover_folder)



async def main():
    global tasks
    try:
        tasks.append(asyncio.create_task(msg_transfer_worker()))
        tasks.append(asyncio.create_task(daily_qualify_reset()))
        tasks.append(asyncio.create_task(click_protector_guard()))
        tasks.append(asyncio.create_task(track_connection()))
        tasks.append(asyncio.create_task(auto_confiscate()))

        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        await startup()
        await bot_connect()
        start_up_flag = True
    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
    

