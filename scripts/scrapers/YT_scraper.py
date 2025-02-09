import os
import sys
import json
import scrapetube
import pandas as pd
from datetime import datetime
from youtube_comment_downloader import YoutubeCommentDownloader
from dotenv import load_dotenv

load_dotenv()

try:
    filter_date = datetime.fromisoformat(sys.argv[1])
except Exception as e:
    print(f"Ошибка при обработке даты: {e}")
    sys.exit(1)

channel_mapping = json.loads(os.getenv('CHANNEL_MAPPING', '{}'))

channel_ids = list(channel_mapping.keys())
all_videos = []

print(f"Сбор видео с каналов: {channel_ids}")

for channel_id in channel_ids:
    try:
        videos = scrapetube.get_channel(channel_id=channel_id)
        for video in videos:
            video_id = video.get('videoId')
            if video_id:
                video_title = video.get('title', '')
                video_desc = video.get('description', '')
                all_videos.append((video_id, channel_id, video_title, video_desc))
    except Exception as e:
        print(f"Ошибка при получении видео для канала {channel_id}: {e}")

print(f"Всего видео для обработки: {len(all_videos)}")

# Функция для обработки одного видео: сбор комментариев
def process_video(video_info):
    video_id, channel_id, video_title, video_desc = video_info
    youtube_downloader = YoutubeCommentDownloader()
    results = []
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        for comment in youtube_downloader.get_comments_from_url(video_url):
            comment_date = datetime.fromtimestamp(comment['time_parsed'])
            if comment_date > filter_date:
                results.append({
                    'Date': comment_date.strftime('%Y-%m-%d'),
                    'Source': channel_mapping.get(channel_id, channel_id),
                    'Comment': comment.get('text'),
                    'URL': video_url
                })
    except Exception as e:
        print(f"Ошибка при обработке видео {video_id}: {e}")

    return results

# Обрабатываем видео последовательно (без параллельных вычислений)
all_results = []
for idx, video_info in enumerate(all_videos, start=1):
    print(f"{idx}. Обработка видео: https://www.youtube.com/watch?v={video_info[0]}")
    video_comments = process_video(video_info)
    all_results.extend(video_comments)

# Формируем DataFrame из собранных комментариев и сортируем по дате
youtube_df = pd.DataFrame(all_results)
youtube_df.sort_values(by='Date', inplace=True)

# Создаем папку для результатов, если её нет, и сохраняем в CSV
youtube_df.to_csv('output/raw_comments/youtube_comments.csv', index=False, encoding='utf-8-sig')

print("Сбор комментариев завершён. Данные сохранены в /output/youtube_comments.csv.")