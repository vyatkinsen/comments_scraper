#!/usr/bin/env python3
import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone
from instagrapi import Client
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# Проверяем наличие аргумента с датой
if len(sys.argv) < 2:
    print("Укажите дату в формате YYYY-MM-DD в качестве аргумента.")
    sys.exit(1)

try:
    filter_date = datetime.fromisoformat(sys.argv[1]).replace(tzinfo=timezone.utc)
except Exception as e:
    print(f"Ошибка при обработке даты: {e}")
    sys.exit(1)

# Загружаем маппинг профилей из переменной окружения (в формате JSON)
profile_mapping_str = os.getenv('INSTAGRAM_PROFILE_MAPPING', '{}')
try:
    profile_mapping = json.loads(profile_mapping_str)
except Exception as e:
    print(f"Ошибка парсинга INSTAGRAM_PROFILE_MAPPING: {e}")
    profile_mapping = {}

if not profile_mapping:
    print("Маппинг профилей пуст. Завершение работы.")
    sys.exit(1)

insta_username = os.getenv("INSTAGRAM_USERNAME")
insta_password = os.getenv("INSTAGRAM_PASSWORD")

if not insta_username or not insta_password:
    print("Не заданы переменные INSTAGRAM_USERNAME или INSTAGRAM_PASSWORD.")
    sys.exit(1)

cl = Client()
try:
    cl.login(insta_username, insta_password)
    print(f"Успешный вход в аккаунт {insta_username}")
except Exception as e:
    print(f"Ошибка при входе в Instagram: {e}")
    sys.exit(1)

results = []

for profile in profile_mapping.keys():
    print(f"Обработка профиля {profile}")
    source = profile_mapping.get(profile, profile)
    try:
        medias = cl.user_medias(profile, amount=50)
        for media in medias:
            comments = cl.media_comments(media.pk)
            for comment in comments:
                comment_date = comment.created_at_utc
                if comment_date.tzinfo is None:
                    comment_date = comment_date.replace(tzinfo=timezone.utc)
                if comment_date > filter_date:
                    date_str = comment_date.strftime("%Y-%m-%d")
                    media_url = f"https://www.instagram.com/p/{media.code}/"
                    results.append({
                        "Date": date_str,
                        "Source": source,
                        "Comment": comment.text,
                        "URL": media_url
                    })
    except Exception as e:
        print(f"Ошибка при обработке профиля {profile}: {e}")

if results:
    df = pd.DataFrame(results)
    df.sort_values(by="Date", inplace=True)
    output_path = "output/raw_comments/IG_comments.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Сбор комментариев завершён. Данные сохранены в {output_path}")
else:
    print("Нет данных для сохранения.")