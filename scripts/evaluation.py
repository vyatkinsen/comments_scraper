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

demoji.download_codes()
comments['clean_comments'] = comments['Comment'].apply(lambda x: demoji.replace(x, ""))

regex = r"[^0-9A-Za-z't]"
copy = comments.copy()
copy['regular_comments'] = copy['clean_comments'].apply(lambda x: re.sub(regex, " ", x))

dataset = copy[['Date', 'Source', 'regular_comments', 'URL']].copy()
dataset = dataset.rename(columns={"regular_comments": "Сomment"})

# Удаляем дубликаты по комментариям
dataset.drop_duplicates(subset=['Сomment'], inplace=True)
print("Размер датасета после удаления дубликатов:", dataset.shape)

# Загружаем стоп-слова (английский список) и удаляем их из комментариев
nltk.download("stopwords")
stop = set(stopwords.words('english'))
dataset['no_stop'] = dataset['Сomment'].apply(
    lambda x: ' '.join(word for word in x.split() if word.lower() not in stop)
)

dataset.to_csv("../output/no_stop_dataset.csv", index=False)
print("Очищенный датасет сохранён в output/no_stop_dataset.csv")
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment",
    tokenizer="cardiffnlp/twitter-roberta-base-sentiment"
)

def get_sentiment(text):
    try:
        if not isinstance(text, str):
            text = str(text)
        if not text.strip():
            return None, None
        result = sentiment_pipeline(text, truncation=True, max_length=512)
        return result[0]['label'], result[0]['score']
    except Exception as e:
        print("Ошибка при анализе тональности для текста:", text)
        print("Детали ошибки:", e)
        return None, None

dataset[['sentiment_label', 'sentiment_score']] = dataset['no_stop'].apply(lambda x: pd.Series(get_sentiment(x)))
label_mapping = {
    "LABEL_0": "Negative",
    "LABEL_1": "Neutral",
    "LABEL_2": "Positive"
}
new_dataset = dataset[['sentiment_label', 'Date', 'Source', 'URL']].copy()
new_dataset = new_dataset.rename(columns={"sentiment_label": "Type of comment"})
new_dataset["Type of comment"] = new_dataset["Type of comment"].map(label_mapping)
new_dataset.to_csv("../output/result.csv", index=False)
print("Итоговый датасет сохранен в output/result.csv")
print("Примеры комментариев с рассчитанной тональностью (RoBERTa):")
print(dataset[['Сomment', 'sentiment_label', 'sentiment_score']].head())

print("Top negative comments (RoBERTa):")
print(dataset[dataset.sentiment_label == 'Negative'].head(10))
print("Top positive comments (RoBERTa):")
print(dataset[dataset.sentiment_label == 'Positive'].head(10))

sentiment_counts = dataset['sentiment_label'].value_counts()
print("Распределение тональности (RoBERTa):")
print(sentiment_counts)

plt.figure(figsize=(8, 6))
sentiment_counts.plot(kind='bar', color=['red', 'gray', 'green'])
plt.xlabel("Тональность")
plt.ylabel("Количество комментариев")
plt.title("Распределение тональности комментариев (RoBERTa)")
plt.show()
