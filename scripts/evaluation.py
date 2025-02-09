import os
import pandas as pd
import demoji
import re
import nltk
from nltk.corpus import stopwords
from transformers import pipeline
import matplotlib.pyplot as plt

directory = "../output/raw_comments/"

csv_files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith(".csv")]

dataframes = []

for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

combined_df = pd.concat(dataframes, ignore_index=True)

combined_df.to_csv(f'../output/combined_dataset.csv', index=False)

print(f'Объединенный датасет сохранен в файле output/combined_dataset.csv')
comments = pd.read_csv('../output/combined_dataset.csv')

comments = comments[comments['Comment'].notna()]
comments = comments[comments['Comment'].str.strip() != ""]
comments['clean_comments'] = comments['Comment'].apply(lambda x: demoji.replace(x, ""))

regex = r"[^0-9A-Za-zА-Яа-яЁё't]"
copy = comments.copy()
copy['regular_comments'] = copy['clean_comments'].apply(lambda x: re.sub(regex, "", x))

dataset = copy[['Date', 'Source', 'Comment', 'regular_comments', 'URL']].copy()
# dataset = dataset.rename(columns={"regular_comments": "Comment"})

# Удаляем дубликаты по комментариям
dataset.drop_duplicates(subset=['regular_comments'], inplace=True)
dataset = dataset[dataset['regular_comments'].str.strip() != ""]
print("Размер датасета после удаления дубликатов:", dataset.shape)

# nltk.download("stopwords")
# stop = set(stopwords.words('english'))
# dataset['no_stop'] = dataset['Comment'].apply(
#     lambda x: ' '.join(word for word in x.split() if word.lower() not in stop)
# )

dataset.to_csv("../output/no_stop_dataset.csv", index=False)
print("Очищенный датасет сохранён в output/no_stop_dataset.csv")

sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment",
    tokenizer="cardiffnlp/twitter-roberta-base-sentiment"
)

def get_sentiment(text):
    try:
        result = sentiment_pipeline(text, truncation=True, max_length=512)
        return result[0]['label'], result[0]['score']
    except Exception as e:
        print("Ошибка при анализе тональности для текста:", text)
        print("Детали ошибки:", e)
        return None, None

dataset[['sentiment_label', 'sentiment_score']] = dataset['regular_comments'].apply(lambda x: pd.Series(get_sentiment(x)))
label_mapping = {
    "LABEL_0": "Negative",
    "LABEL_1": "Neutral",
    "LABEL_2": "Positive"
}
new_dataset = dataset[['sentiment_label', 'Date', 'Source', 'Comment', 'URL']].copy()
new_dataset = new_dataset.rename(columns={"sentiment_label": "Type of comment"})
new_dataset["Type of comment"] = new_dataset["Type of comment"].map(label_mapping)
new_dataset.to_csv("../output/result.csv", index=False)
print("Итоговый датасет сохранен в output/result.csv")


sentiment_counts = new_dataset['Type of comment'].value_counts()
print("Распределение тональности:")
print(sentiment_counts)

plt.figure(figsize=(8, 6))
sentiment_counts.plot(kind='bar', color=['red', 'gray', 'green'])
plt.xlabel("Тональность")
plt.ylabel("Количество комментариев")
plt.title("Распределение тональности комментариев")
plt.show()
