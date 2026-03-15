from string import Template
import textwrap
import os
import json
import lz_var
from utils.media_utils import Media
from utils.unit_converter import UnitConverter
from typing import Awaitable, Callable, Optional
GetFileIdsFn = Callable[[list[str]], Awaitable[list[str]]]


class Tplate:
    @classmethod
    async def pure_text_tpl(cls, tpl_data):
        

        if tpl_data.get('product_type'):
            tpl_data['file_type'] = tpl_data['product_type']

        if tpl_data.get('file_type'):
            type_map = {
                'p': "photo",
                'v': "video",
                'd': "document",
                'a': "album"
            }

            tpl_data['file_type'] = type_map.get(tpl_data['file_type'], tpl_data['file_type'])



        if not tpl_data.get('file_icon_type') and tpl_data.get('file_type'):
            icon_map = {
                'photo': "🖼 ",
                'p': "🖼 ",
                'video': "🎥 ",
                'v': "🎥 ",
                'document': "📄 ",
                'd': "📄 ",
                'album': "📂 ",
                'a': "📂 "
            }
            tpl_data['file_icon'] = icon_map.get(tpl_data['file_type'], "📄 ")

        
        


        if 'fee' in tpl_data and tpl_data['fee'] is not None:
            tpl_data['fee_string'] = f"💎 {tpl_data['fee']}" or ""
        else:
            tpl_data['fee_string'] = ""

        if 'file_size' in tpl_data and tpl_data['file_size'] is not None and tpl_data['file_type'] != 'album':
            tpl_data['file_size_string'] = f"📄 {UnitConverter.byte_to_human_readable(int(tpl_data['file_size']))}   "
        else:
            tpl_data['file_size_string'] = ""

        if 'duration' in tpl_data and tpl_data['duration'] is not None and tpl_data['file_type'] != 'album':
            tpl_data['duration_string'] = f"🕔 {UnitConverter.seconds_to_hms(tpl_data['duration'])}   "
        else:
            tpl_data['duration_string'] = ""

        if 'create_timestamp' in tpl_data and tpl_data['create_timestamp'] is not None:
            tpl_data['create_timestamp_string'] = f"🎴 {tpl_data['create_timestamp']}   "
        else:
            tpl_data['create_timestamp_string'] = ""

        if 'tag' in tpl_data and tpl_data['tag'] is not None:
            tpl_data['tag_string'] = f"{tpl_data['tag']}\r\n   "
        else:
            tpl_data['tag_string'] = ""

        if 'album_cont_list_text' in tpl_data and tpl_data['album_cont_list_text'] is not None:
            tpl_data['album_string'] = tpl_data['album_cont_list_text'] + "\r\n\r\n"
        else:
            tpl_data['album_string'] = ""

        if 'content_id_str' in tpl_data and tpl_data['content_id_str'] is not None:
            tpl_data['content_id_str'] = f"🌼 <code>{tpl_data['content_id_str']}</code>\r\n"
        else:
            tpl_data['content_id_str'] = ""      
        

        template_str = textwrap.dedent("""\
            <blockquote>ㅤ
            $file_icon $content
            ㅤ</blockquote>
            $content_id_str                           
            $album_string$tag_string
            $fee_string $file_size_string$duration_string$create_timestamp_string
        """)

        template = Template(template_str)
        return template.safe_substitute(tpl_data)

    @classmethod
    async def list_template(cls,results):
        album_list_text = ''
        album_cont_list_text = ''
        list_text = ''
        video_count = document_count = photo_count = 0
        video_total_size = document_total_size = 0
        video_total_duration = 0
        summary_text = ''

        total_size = 0
        total_duration = 0

        for row in results:
            file_type = row["file_type"]
            file_size = row.get("file_size", 0)
            duration = row.get("duration", 0)
            file_name = row.get("file_name", "")

            if file_size!= None:
                total_size = total_size + int(file_size)    
            else:
                total_size = 0
                file_size = 0

            if duration!= None:
                total_duration = total_duration + int(duration)

            if file_name:
                file_name = f"| {file_name}"

            if file_type == "v":
                video_count += 1
                video_total_size = video_total_size + int(file_size)
                if duration!= None:
                    video_total_duration = video_total_duration + int(duration)
                album_list_text += f"　🎬 {UnitConverter.byte_to_human_readable(file_size)} | {UnitConverter.seconds_to_hms(duration)}\n"
            elif file_type == "d":
                document_count += 1
                document_total_size = document_total_size + int(file_size)
                album_list_text += f"　📄 {UnitConverter.byte_to_human_readable(file_size)} {file_name}\n"
            elif file_type == "p":
                photo_count += 1
                album_list_text += f"　🖼️ {UnitConverter.byte_to_human_readable(file_size)}\n"


        summary_text += "📂 本资源夹包含：\r\n" 
        


        if video_count:
            summary_text += f"　　🎬 x{video_count} 　 {UnitConverter.byte_to_human_readable(video_total_size)} "
            if video_total_duration and video_total_duration > 0:
                summary_text += f"　　| {UnitConverter.seconds_to_hms(video_total_duration)} "
            summary_text += "\r\n"
            album_cont_list_text += f"🎬 x{video_count} 　"
        if document_count:
            summary_text += f"　　📄 x{document_count} 　 {UnitConverter.byte_to_human_readable(document_total_size)} \r\n"
            album_cont_list_text += f"📄 x{document_count} 　"
        if photo_count:
            summary_text += f"　　🖼️ x{photo_count} \r\n"
            album_cont_list_text += f"🖼️ x{photo_count}"

        if album_list_text :
            list_text += "\n📃 资源明细：\n" + album_list_text.rstrip()
        
        if album_cont_list_text :
            list_text += "\n\n📂 本资源夹包含：" + album_cont_list_text
        
        opt_text = ''
        limit = 100
        if len(list_text) < 100:
            opt_text = list_text
        elif len(summary_text) < 100 :
            opt_text = summary_text
        else:
            opt_text = album_cont_list_text

        return {"album_list_text": album_list_text, "album_cont_list_text": album_cont_list_text, "list_text": list_text, "summary_text": summary_text, "opt_text": opt_text}


        # import os, json, inspect
        # from typing import Optional, Callable, Awaitable

        # GetFileIdsFn = Callable[[list[str]], Awaitable[dict[str, str]]]



    @classmethod
    async def load_or_create_skins(
        cls,
        get_file_ids_fn: Optional[GetFileIdsFn] = None,
        if_del: bool = False,
        config_path: str = "skins.json",
    ) -> dict:
        """
        启动时加载皮肤配置（不依赖 YAML）。
        - 若文件存在则载入；不存在则从 default_skins 生成。
        - 若有 file_unique_id 但 file_id 为空，会调用 get_file_ids_fn 取得（依赖注入）。
        """

        if not callable(get_file_ids_fn):
            raise TypeError("get_file_ids_fn must be an async callable like PGPool.get_file_id_by_file_unique_id")


        config_path = f"{lz_var.bot_username}_skins.json"
        # print(f"🔍 载入或生成皮肤配置文件：{config_path}")

        # 默认注入 PGPool（外部可传入别的实现）


        default_skins = {
            "home":    {"file_id": "", "file_unique_id": "AQADHwtrG8puoUd-"},
            "loading": {"file_id": "", "file_unique_id": "AgADcAYAAtiwqUc"},
            "clt_menu":     {"file_id": "", "file_unique_id": "AQAD2wtrG-sSiUd-"},
            "clt_my":  {"file_id": "", "file_unique_id": "AQADcwtrG3QQCEZ-"},
            "clt_fav": {"file_id": "", "file_unique_id": "AQADcQtrG3QQCEZ-"},
            "clt_cover": {"file_id": "", "file_unique_id": "AQADHgtrG8puoUd-"},
            "clt_market": {"file_id": "", "file_unique_id": "AQADdQtrG3QQCEZ-"},
            "history": {"file_id": "", "file_unique_id": "AQAD6AtrG-sSiUd-"},
            "history_update": {"file_id": "", "file_unique_id": "AQAD4wtrG-sSiUd-"},
            "history_redeem": {"file_id": "", "file_unique_id": "AQAD5wtrG-sSiUd-"},
            "search": {"file_id": "", "file_unique_id": "AQADGgtrG8puoUd-"},
            "search_keyword": {"file_id": "", "file_unique_id": "AQADHAtrG8puoUd-"},
            "search_tag": {"file_id": "", "file_unique_id": "AQADHQtrG8puoUd-"},
            "ranking": {"file_id": "", "file_unique_id": "AQADCwtrG9iwoUd-"},
            "ranking_resource": {"file_id": "", "file_unique_id": "AQADDQtrG9iwoUd-"},
            "ranking_uploader": {"file_id": "", "file_unique_id": "AQADDgtrG9iwoUd-"},
            "product_cover1": {"file_id": "", "file_unique_id": "AQADMK0xG4g4QEV-"},
            "product_cover2": {"file_id": "", "file_unique_id": "AQADMq0xG4g4QEV-"},
            "product_cover3": {"file_id": "", "file_unique_id": "AQADMa0xG4g4QEV-"},
            "push_cover": {"file_id": "", "file_unique_id": "AQAD9wtrG3ZWyER-"}
        }

     


        if os.path.exists(config_path) and if_del:
            print(f"🗑 删除旧的皮肤配置文件：{config_path} {if_del}")
            os.remove(config_path)

        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    skins = json.load(f)
                    # print(f"✅ 载入已有皮肤配置文件：{skins}")
            except Exception as e:
                print(f"⚠️ 无法读取 {config_path}，将重新生成：{e}")
                skins = default_skins.copy()
        else:
            skins = default_skins.copy()

        # ---------- 1) 收集所有需要补齐的 fu，一次查询 ----------
        # --- 若 file_id 为空，尝试用数据库补齐 ---
        fu_row = []
        for name, obj in skins.items():
            if not obj.get("file_id") and obj.get("file_unique_id"):
                fu = obj["file_unique_id"]
                fu_row.append(fu)

                print(f"🔍 {name}: file_id 为空，尝试从数据库查询…（{fu}）")
        try:
            if fu_row:
                # //await get_file_ids_fn(fu_list)
                file_ids_row = await get_file_ids_fn(fu_row)
                print(f"📚 {file_ids_row}。", flush=True)

                for name, obj in skins.items():
                    if not obj.get("file_id") and obj.get("file_unique_id"):
                        fu = obj["file_unique_id"]
                        fid_row = file_ids_row.get(fu)
                        if fid_row:
                            obj["file_id"] = fid_row.get("file_id")
                            tmp = fid_row.get("file_id")
                            print(f"✅ {name}: 已从数据库查询到 file_id {tmp}", flush=True)
                    


        except Exception as e:
            print(f"❌ 查询 file_id 出错：{e}")


        # --- 若仍有 file_id 为空，尝试向 x-man 询问 ---
        need_fix = [(k, v) for k, v in skins.items() if not v.get("file_id") and v.get("file_unique_id")]
        for name, obj in need_fix:
            fu = obj["file_unique_id"]
            print(f"🧾 {name}: 向 x-man {lz_var.x_man_bot_id} 请求 file_id…（{fu}）")
            try:
                msg = await lz_var.bot.send_message(chat_id=lz_var.x_man_bot_id, text=f"{fu}")
                print(f"📨 已请求 {fu}，并已接收返回",flush=True)
            except Exception as e:
                print(f"⚠️ 向 x-man 请求失败：{e} - {lz_var.x_man_bot_id}",flush=True)
                return {"ok":None, "handshake": lz_var.x_man_bot_id}
                


        # --- 写入文件（即便有缺） ---
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(skins, f, ensure_ascii=False, indent=4)
        # print(f"💾 已写入 {config_path}")

        lz_var.default_thumb_file_id = [
            skins.get("product_cover1", {}).get("file_id", ""),
            skins.get("product_cover2", {}).get("file_id", ""),
            skins.get("product_cover3", {}).get("file_id", ""), 
        ]
        
        return {"ok":1, "skins": skins}



''''''