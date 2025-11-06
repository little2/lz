from string import Template
import textwrap
from utils.unit_converter import UnitConverter

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


        template_str = textwrap.dedent("""\
            <blockquote>ㅤ
            $file_icon $content
            ㅤ</blockquote>
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

            total_size = total_size + int(file_size)
            total_duration = total_duration + int(duration)

            if file_name:
                file_name = f"| {file_name}"

            if file_type == "v":
                video_count += 1
                video_total_size = video_total_size + int(file_size)
                video_total_duration = video_total_duration + int(duration)
                album_list_text += f"　🎬 {UnitConverter.byte_to_human_readable(file_size)} | {UnitConverter.seconds_to_hms(duration)}\n"
            elif file_type == "d":
                document_count += 1
                document_total_size = document_total_size + int(file_size)
                album_list_text += f"　📄 {UnitConverter.byte_to_human_readable(file_size)} {file_name}\n"
            elif file_type == "p":
                photo_count += 1
                album_list_text += f"　🖼️ {UnitConverter.byte_to_human_readable(file_size)}\n"


        summary_text += "\n\n📂 本资源夹包含：\r\n" 
        


        if video_count:
            summary_text += f"　　🎬 x{video_count} 　 {UnitConverter.byte_to_human_readable(video_total_size)} | {UnitConverter.seconds_to_hms(video_total_duration)} \r\n"
            album_cont_list_text += f"🎬 x{video_count} 　"
        if document_count:
            summary_text += f"　　📄 x{document_count} 　 {UnitConverter.byte_to_human_readable(document_total_size)} \r\n"
            album_cont_list_text += f"📄 x{document_count} 　"
        if photo_count:
            summary_text += f"　　🖼️ x{photo_count} \r\n"
            album_cont_list_text += f"🖼️ x{photo_count}"

        if album_list_text :
            list_text += "\n📂 资源列表：\n" + album_list_text.rstrip()
        
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

# # 测试
# import asyncio
# tpl_data = {
#     "name": "苹果",
#     "quantity": 5,
#     "price": 3,
#     "total": 15
# }

# print(asyncio.run(TPL.pure_text_tpl(tpl_data)))
