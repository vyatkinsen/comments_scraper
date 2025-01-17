import os
import sys
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import scrapetube
from youtube_comment_downloader import YoutubeCommentDownloader
from dotenv import load_dotenv  # Импортируем для работы с .env

load_dotenv()  # Загрузка переменных окружения из .env файла

try:
    filter_date = datetime.fromisoformat(sys.argv[1])
except Exception as e:
    print(f"Ошибка при обработке даты: {e}")
    sys.exit(1)

# Загрузка channel_mapping из переменной окружения и парсинг JSON
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
                all_videos.append((video_id, channel_id))
    except Exception as e:
        print(f"Ошибка при получении видео для канала {channel_id}: {e}")

print(f"Всего видео для обработки: {len(all_videos)}")

def process_video(video_info):
    video_id, channel_id = video_info
    youtube_downloader = YoutubeCommentDownloader()
    results = []
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"Обработка видео {video_url}")

    try:
        for comment in youtube_downloader.get_comments_from_url(video_url):
            comment_date = datetime.fromtimestamp(comment['time_parsed'])

            if comment_date > filter_date:
                results.append({
                    'date': comment_date.strftime('%Y-%m-%d'),
                    'source': channel_mapping.get(channel_id, channel_id),
                    'text': comment.get('text'),
                    'link': video_url
                })
    except Exception as e:
        print(f"Ошибка при обработке видео {video_id}: {e}")

    return results


all_results = []

with ThreadPoolExecutor(max_workers=20) as executor:
    future_to_video = {executor.submit(process_video, video_info): video_info for video_info in all_videos}

    for future in as_completed(future_to_video):
        video_info = future_to_video[future]
        try:
            video_comments = future.result()
            all_results.extend(video_comments)
        except Exception as exc:
            vid, _ = video_info
            print(f"Ошибка при обработке видео {vid}: {exc}")

# Создание DataFrame и сохранение результатов
youtube_df = pd.DataFrame(all_results)
youtube_df.sort_values(by='date', inplace=True)
youtube_df.to_csv('output/youtube_comments.csv', index=False, encoding='utf-8-sig')

print("Сбор комментариев завершён. Данные сохранены в youtube_comments.csv.")
