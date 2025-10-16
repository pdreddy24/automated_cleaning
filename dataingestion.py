from datetime import date, datetime, timedelta
from newsapi import NewsApiClient
import json
import os
import schedule
import time
import dotenv
#  Initialize API client load_dotenv()
dotenv.load_dotenv()
news_api_key = os.getenv("news_api_key")
if not news_api_key:
    raise ValueError("news_api_key not found. Check your .env file.")
newsapi = NewsApiClient(api_key=news_api_key)

#  Fetch top headlines
top_headlines = newsapi.get_top_headlines(
    q='bitcoin',
    category='business',
    language='en',
    country='us'
)

#  Fetch everything (past 7 days)
today = date.today()
from_date = today - timedelta(days=7)

all_articles = newsapi.get_everything(
    q='bitcoin',
    sources='bbc-news,the-verge',
    domains='bbc.co.uk,techcrunch.com',
    from_param=from_date.strftime("%Y-%m-%d"),
    to=today.strftime("%Y-%m-%d"),
    language='en',
    sort_by='relevancy',
    page=1
)

#  Extract articles
articles = all_articles.get('articles', [])
print(f" Fetched {len(articles)} articles")

#  Make data folder if not exists
os.makedirs("data", exist_ok=True)

#  Save full raw response to timestamped JSON file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = os.path.join("data", f"raw_news_{timestamp}.json")

with open(filename, "w", encoding="utf-8") as f:
    json.dump(all_articles, f, indent=2, ensure_ascii=False)

print(f" Raw data saved to: {filename}")

#  Append new articles to a log file
file_path = os.path.join("data", "raw_news_log.json")

# Load existing data if file exists
if os.path.exists(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        existing_data = json.load(f)
else:
    existing_data = []

# Append and save back
existing_data.extend(articles)

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(existing_data, f, indent=2, ensure_ascii=False)

print(f" Appended {len(articles)} new records to {file_path}")

def job():
    print(" Running automated ingestion...")
    all_articles(news_api_key)

# Run every day at 9:00 AM
schedule.every().day.at("09:00").do(job)

print(" Scheduler started. Waiting for next ingestion...")

while True:
    schedule.run_pending()
    time.sleep(60)

