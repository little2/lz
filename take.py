'''
Docstring for take
接案机器人

1. 取出没有缩略图的内容
SELECT * FROM `sora_content` WHERE `thumb_file_unique_id` IS NULL and valid_state in (1,8,9) and file_type not in ('p','photo','a','album') ORDER BY `id` DESC;

2. 找出这个file_unique_id 是否有同质资源，就是尺度、格式、大小都一样的，只有file_unique_id不一样

3.1. 如果有同质资源，取出这些资源的缩略图，送出审核人工确认

3.2. 如果没有同质资源，进行自动生成缩略图

4.1 如果是视频, 取出视频的第一帧作为缩略图
4.1.1 成功生成，更新数据库
4.1.2 失败(没有thumb)，放到处理区

4.2 如果是文档，送出处理区

5.处理区

5.1 若是视频: 
get_file → 拿到 file_path → 拼出真实下载 URL。
用 aiohttp 打开这个 URL（流式读取，不落地整档文件）。
启动一个 ffmpeg 子进程：
从 stdin 读视频流（-i pipe:0）；
只解码到第 10 秒（-ss 10）；
只输出 1 帧（-frames:v 1）。
边从 Telegram 拉 chunk，边写到 ffmpeg 的 stdin。
一旦 ffmpeg 拿到足够数据解完前 10 秒并成功输出这一帧，它会自动退出（因为 -frames:v 1），你马上停止继续下载。

5.2 若是文档:
下载文件后，自动生成缩略图

tgone 要加入 file_extension
'''

