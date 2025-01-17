import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from instagrapi import Client
from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 2:
    print("Укажите дату в формате YYYY-MM-DD в качестве аргумента.")
    sys.exit(1)

try:
    filter_date = datetime.fromisoformat(sys.argv[1]).replace(tzinfo=timezone.utc)
except Exception as e:
    print(f"Ошибка при обработке даты: {e}")
    sys.exit(1)

profile_mapping = json.loads(os.getenv('PROFILE_MAPPING', '{}'))
number_of_posts = 10

cl = Client()
try:
    cl.login(os.getenv("INSTAGRAM_USERNAME"), os.getenv("INSTAGRAM_PASSWORD"))
except Exception as e:
    print(f"Ошибка при авторизации: {e}")

def process_profile(profile_name):
    print(f"Обработка профиля {profile_name}")
    source = profile_mapping.get(profile_name, profile_name)
    all_comments = []

    try:
        user_id = cl.user_id_from_username(profile_name)
        posts = cl.user_medias(user_id, number_of_posts)
    except Exception as e:
        print(f"Ошибка при получении постов для профиля {profile_name}: {e}")
        return all_comments

    for post in posts:
        try:
            print(f"Скачивание комментариев из поста {post.pk} профиля {profile_name}")
            comments = cl.media_comments(post.pk)
            for comment in comments:
                comment_date = comment.created_at_utc
                if comment_date > filter_date:
                    comment_date_str = comment_date.strftime('%Y-%m-%d')
                    all_comments.append({
                        "date": comment_date_str,
                        "source": source,
                        "text": comment.text,
                        "link": f"https://www.instagram.com/p/{post.code}/"
                    })
        except Exception as e:
            print(f"Ошибка при скачивании комментариев из поста {post.pk}: {e}")

    return all_comments

all_comments_data = []

with ThreadPoolExecutor(max_workers=len(profile_mapping)) as executor:
    futures = {executor.submit(process_profile, profile): profile for profile in profile_mapping.keys()}

    for future in as_completed(futures):
        profile = futures[future]
        try:
            comments = future.result()
            all_comments_data.extend(comments)
        except Exception as exc:
            print(f"Ошибка при обработке профиля {profile}: {exc}")

comments_df = pd.DataFrame(all_comments_data)
comments_df.sort_values(by='date', inplace=True)
comments_df.to_csv('output/instagram_comments.csv', index=False, encoding='utf-8-sig')

print("Сбор комментариев завершён. Данные сохранены в instagram_comments.csv.")
