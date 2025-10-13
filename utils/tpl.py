from string import Template
import textwrap
from utils.unit_converter import UnitConverter

class Tplate:
    @classmethod
    async def pure_text_tpl(cls, tpl_data):
        if not tpl_data.get('file_icon_type') and tpl_data.get('file_type'):
            icon_map = {
                'photo': "🖼 ",
                'p': "🖼 ",
                'video': "🎥 ",
                'v': "🎥 ",
                'document': "📄 ",
                'd': "📄 "
            }
            tpl_data['file_icon'] = icon_map.get(tpl_data['file_type'], "📄 ")



        if 'fee' in tpl_data and tpl_data['fee'] is not None:
            tpl_data['fee_string'] = f"💎 {tpl_data['fee']}" or ""
        else:
            tpl_data['fee_string'] = ""

        if 'file_size' in tpl_data and tpl_data['file_size'] is not None:
            tpl_data['file_size_string'] = f"📄 {UnitConverter.byte_to_human_readable(int(tpl_data['file_size']))}   "
        else:
            tpl_data['file_size_string'] = ""

        if 'duration' in tpl_data and tpl_data['duration'] is not None:
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

        if 'album' in tpl_data and tpl_data['album'] is not None:
            tpl_data['album_string'] = tpl_data['album'] + "\r\n\r\n"
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


# # 测试
# import asyncio
# tpl_data = {
#     "name": "苹果",
#     "quantity": 5,
#     "price": 3,
#     "total": 15
# }

# print(asyncio.run(TPL.pure_text_tpl(tpl_data)))
