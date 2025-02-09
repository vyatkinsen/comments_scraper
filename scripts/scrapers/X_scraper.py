import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone
from twscrape import API, gather
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

profile_mapping_str = os.getenv('TWITTER_PROFILE_MAPPING', '{}')
try:
    profile_mapping = json.loads(profile_mapping_str)
except Exception as e:
    print(f"Ошибка парсинга TWITTER_PROFILE_MAPPING: {e}")
    profile_mapping = {}

if not profile_mapping:
    print("Маппинг профилей пуст. Завершение работы.")
    sys.exit(1)

username = os.getenv("TWITTER_USERNAME")
password = os.getenv("TWITTER_PASSWORD")
email = os.getenv("TWITTER_EMAIL")
mail_password = os.getenv("TWITTER_MAIL_PASSWORD")

async def main():
    api = API()

    try:
        await api.pool.add_account(username, password, email, mail_password)
        await api.pool.login_all()
    except Exception as e:
        print(f"Ошибка при добавлении аккаунта или авторизации: {e}")
        return

    results = []

    for profile in profile_mapping.keys():
        print(f"Обработка профиля {profile}")
        source = profile_mapping.get(profile, profile)
        try:
            user = await api.user_by_login(profile)
            user_id = user.id

            tweets = await gather(api.user_tweets(user_id, limit=50))
            for tweet in tweets:
                replies = await gather(api.tweet_replies(tweet.id, limit=50))
                for reply in replies:
                    reply_date = reply.date
                    if reply_date and reply_date.tzinfo is None:
                        reply_date = reply_date.replace(tzinfo=timezone.utc)
                    if reply_date and reply_date > filter_date:
                        reply_date_str = reply_date.strftime("%Y-%m-%d")
                        reply_link = f"https://x.com/{profile}/status/{reply.id}"
                        results.append({
                            "Date": reply_date_str,
                            "Source": source,
                            "Comment": reply.rawContent,
                            "URL": reply_link
                        })
        except Exception as e:
            print(f"Ошибка при обработке профиля {profile}: {e}")

    if results:
        df = pd.DataFrame(results)
        df.sort_values(by="Date", inplace=True)
        df.to_csv("output/raw_comments/X_comments.csv", index=False, encoding="utf-8-sig")
        print("Сбор ответов на твиты завершён. Данные сохранены в output/raw_comments/X_comments.csv")
    else:
        print("Нет данных для сохранения.")

