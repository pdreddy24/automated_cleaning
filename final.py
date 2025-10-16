# news_pipeline.py

import os
import json
import time
import re
import schedule
from datetime import date, datetime, timedelta
from collections import Counter
from newsapi import NewsApiClient
from langchain_ollama import ChatOllama
import dotenv



def fetch_and_save_raw_news():
    dotenv.load_dotenv()
    news_api_key = os.getenv("news_api_key")
    if not news_api_key:
        raise ValueError("news_api_key not found. Check your .env file.")

    newsapi = NewsApiClient(api_key=news_api_key)

    # Fetch top headlines
    newsapi.get_top_headlines(
        q='bitcoin',
        category='business',
        language='en',
        country='us'
    )

    # Fetch everything (past 7 days)
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

    articles = all_articles.get('articles', [])
    print(f" Fetched {len(articles)} articles")

    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join("data", f"raw_news_{timestamp}.json")

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, indent=2, ensure_ascii=False)
    print(f" Raw data saved to: {filename}")

    # Append to log
    log_file = os.path.join("data", "raw_news_log.json")
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    existing_data.extend(articles)
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)
    print(f" Appended {len(articles)} new records to {log_file}")



def get_latest_raw_file(directory="data"):
    json_files = [f for f in os.listdir(directory) if f.endswith(".json") and f.startswith("raw_news")]
    if not json_files:
        return None
    json_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return os.path.join(directory, json_files[0])

# Initialize LLM
llm = ChatOllama(model="mistral")

def clean_data_ai(text: str) -> str:
    if not text:
        return ""
    prompt = f"""Clean the following news article text by:
    - fixing grammar and punctuation
    - removing duplicates
    - removing special characters
    - removing extra spaces
    - removing HTML tags
    - removing non-English text
    - removing irrelevant content
    - standardizing date formats

    Text: {text}
    """
    response = llm.invoke(prompt)
    return response.content.strip()

def post_process_article(article: dict) -> dict:
    processed = {}

    processed["title"] = re.sub(r"\s+", " ", article.get("title", "").strip())
    processed["description"] = re.sub(r"\s+", " ", article.get("description", "").strip())

    content = re.sub(r"<.*?>", "", article.get("content", ""))
    processed["content"] = re.sub(r"\s+", " ", content).strip()

    processed["author"] = article.get("author", "").strip()

    source_val = article.get("source", "")
    if isinstance(source_val, dict):
        source_val = source_val.get("name", "")
    processed["source"] = str(source_val).strip()

    pub_date = article.get("publishedAt", "")
    if pub_date:
        try:
            dt = datetime.fromisoformat(pub_date)
            processed["publishedAt"] = dt.isoformat()
        except Exception:
            processed["publishedAt"] = pub_date
    else:
        processed["publishedAt"] = ""

    return processed

def remove_duplicates(articles):
    seen_titles, seen_contents = set(), set()
    unique = []
    for a in articles:
        t, c = a.get("title", "").lower(), a.get("content", "").lower()
        if t in seen_titles or c in seen_contents:
            continue
        seen_titles.add(t)
        seen_contents.add(c)
        unique.append(a)
    print(f" Removed duplicates: {len(articles)-len(unique)}")
    return unique

def run_cleaning_pipeline():
    latest_file = get_latest_raw_file()
    if not latest_file:
        print(" No raw data files found.")
        return

    print(f" Cleaning file: {latest_file}")
    with open(latest_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Some raw files have `{"articles": [...]}`
    articles = raw_data.get("articles", raw_data)

    cleaned_articles = []
    for art in articles:
        ai_cleaned = {
            "title": clean_data_ai(art.get("title", "")),
            "description": clean_data_ai(art.get("description", "")),
            "content": clean_data_ai(art.get("content", "")),
            "author": art.get("author", ""),
            "source": art.get("source", ""),
            "publishedAt": art.get("publishedAt", ""),
        }
        fully = post_process_article(ai_cleaned)
        cleaned_articles.append(fully)

    cleaned_articles = remove_duplicates(cleaned_articles)

    os.makedirs("data/cleaned", exist_ok=True)
    out_file = f"data/cleaned/news_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(cleaned_articles, f, indent=2, ensure_ascii=False)
    print(f" Cleaned data saved to: {out_file}")
    return out_file



def summarize_cleaned_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        articles = json.load(f)

    text = " ".join(
        (a.get("title") or "") + " " +
        (a.get("description") or "") + " " +
        (a.get("content") or "")
        for a in articles
    )[:4000]

    num_articles = len(articles)
    sources = [a.get("source") for a in articles if a.get("source")]
    authors = [a.get("author") for a in articles if a.get("author")]

    stats = f"""
Number of articles: {num_articles}
Top Sources: {Counter(sources).most_common(5)}
Top Authors: {Counter(authors).most_common(5)}
"""

    prompt = f"Summarize the following news dataset in 1-2 paragraphs:\n\n{text}"
    resp = llm.invoke(prompt)
    summary = resp.content.strip()

    print("===== SUMMARY =====")
    print(stats)
    print(summary)

    summary_file = file_path.replace(".json", "_summary.txt")
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(stats + "\n\n" + summary)
    print(f" Summary saved to: {summary_file}")




def scheduled_pipeline():
    print("\n Running scheduled full pipeline...")
    fetch_and_save_raw_news()
    cleaned_file = run_cleaning_pipeline()
    if cleaned_file:
        summarize_cleaned_file(cleaned_file)



if __name__ == "__main__":
    # Run immediately
    scheduled_pipeline()

    # Schedule daily at 09:00
    schedule.every().day.at("09:00").do(scheduled_pipeline)
    print("⏰ Scheduler started. Pipeline will run daily at 09:00.")

    while True:
        schedule.run_pending()
        time.sleep(60)
