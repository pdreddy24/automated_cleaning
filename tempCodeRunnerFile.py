import json
from langchain_ollama import ChatOllama

from prompt_toolkit import prompt
import os
from datetime import datetime
import schedule, time


with open("C:\\Users\\deeks\\OneDrive\\Desktop\\books\\pp\\data\\raw_news_20251015_225521.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)
print(f" Loaded {len(raw_data)} articles from log file.")


llm = ChatOllama(model="mistral")  
response = llm.invoke("Hello, Ollama!")
print(response.content)


def clean_data_ai(raw_data):
    prompt = f"""clean the following news article text by :
    fixing grammar and punctuation
    removing duplicates
    removing special characters
    removing extra spaces
    removing HTML tags
    removing non-English text
    removing irrelevant content
    standardizing date formats
    formatting as JSON array of objects with fields: title, description, content, author, source, publishedAt
    raw_data:{raw_data}
    return only cleaned data
    """
    response = llm.invoke(prompt)
    return response.content.strip()    
cleaned_articles = []
for article in raw_data:
    cleaned_article = article.copy()
    cleaned_article["title"] = clean_data_ai(article.get("title", ""))
    cleaned_article["description"] = clean_data_ai(article.get("description", ""))
    cleaned_articles.append(cleaned_article)
    
os.makedirs("data/cleaned", exist_ok=True)
filename = f"data/cleaned/news_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

with open(filename, "w", encoding="utf-8") as f:
    json.dump(cleaned_articles, f, indent=2, ensure_ascii=False)

print(f"Cleaned data saved to {filename}")


def cleaning_job():
    print(" Starting automated cleaning...")
    with open("C:\\Users\\deeks\\OneDrive\\Desktop\\books\\pp\\data\\raw_news_20251015_225521.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    cleaned_data = clean_data_ai(raw_data)
    with open(f"data/cleaned/news_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
    print(" Automated cleaning complete.")


schedule.every().day.at("09:30").do(cleaning_job)

while True:
    schedule.run_pending()
    time.sleep(60)
