import json
import re
import os
import time
import schedule
from datetime import datetime
from langchain_ollama import ChatOllama

def get_latest_raw_file(directory):
    """Return the path of the latest JSON file in the given directory."""
    json_files = [f for f in os.listdir(directory) if f.endswith(".json")]
    if not json_files:
        return None
    json_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return os.path.join(directory, json_files[0])


llm = ChatOllama(model="mistral")
try:
    test_response = llm.invoke("Hello, Ollama!")
    print(f"LLM initialized successfully: {test_response.content}")
except Exception as e:
    print(f" Failed to initialize LLM: {e}")
    exit()


def clean_data_ai(text):
    """Clean text using LLM (grammar, spaces, special chars, etc.)."""
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

    Return only the cleaned text.
    """
    response = llm.invoke(prompt)
    return response.content.strip()

def post_process_article(article):
    """Programmatically clean fields after AI cleaning."""
    processed = {}

    # Title
    title = article.get("title", "").strip()
    title = re.sub(r"\s+", " ", title)
    processed["title"] = title

    # Description
    desc = article.get("description", "").strip()
    desc = re.sub(r"\s+", " ", desc)
    processed["description"] = desc

    # Content
    content = article.get("content", "")
    content = re.sub(r"<.*?>", "", content)  # remove HTML tags
    content = re.sub(r"\s+", " ", content).strip()
    processed["content"] = content

    # Author & Source
    processed["author"] = article.get("author", "").strip()

    source_val = article.get("source", "")
    if isinstance(source_val, dict):
        source_val = source_val.get("name", str(source_val))
    elif not isinstance(source_val, str):
        source_val = str(source_val)
    processed["source"] = source_val.strip()

    # Published Date
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
    """Remove duplicate articles based on title and content."""
    seen_titles = set()
    seen_contents = set()
    unique_articles = []

    for article in articles:
        title_key = article.get("title", "").strip().lower()
        content_key = article.get("content", "").strip().lower()

        if title_key in seen_titles or content_key in seen_contents:
            continue  # duplicate found, skip

        seen_titles.add(title_key)
        seen_contents.add(content_key)
        unique_articles.append(article)

    print(f" Removed duplicates: {len(articles) - len(unique_articles)} duplicate(s).")
    return unique_articles


def run_cleaning_pipeline():
    """Load latest raw file, clean, deduplicate, and save."""
    latest_file = get_latest_raw_file("data")
    if latest_file is None:
        print(" No raw data files found in 'data' folder.")
        return

    print(f" Cleaning latest file: {latest_file}")
    with open(latest_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    cleaned_articles = []
    for article in raw_data:
        if isinstance(article, dict):
            ai_cleaned = {
                "title": clean_data_ai(article.get("title", "")),
                "description": clean_data_ai(article.get("description", "")),
                "content": clean_data_ai(article.get("content", "")),
                "author": article.get("author", ""),
                "source": article.get("source", ""),
                "publishedAt": article.get("publishedAt", ""),
            }
        else:
            ai_cleaned = {
                "title": clean_data_ai(article),
                "description": clean_data_ai(article),
                "content": clean_data_ai(article),
                "author": "",
                "source": "",
                "publishedAt": "",
            }

        fully_cleaned = post_process_article(ai_cleaned)
        cleaned_articles.append(fully_cleaned)

    # Deduplicate
    cleaned_articles = remove_duplicates(cleaned_articles)

    # Save cleaned data
    os.makedirs("data/cleaned", exist_ok=True)
    filename = f"data/cleaned/news_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cleaned_articles, f, indent=2, ensure_ascii=False)

    print(f" Cleaning complete. Saved to: {filename}")


run_cleaning_pipeline()


def scheduled_job():
    print("\n Running scheduled cleaning job...")
    run_cleaning_pipeline()

# Schedule daily at 09:30
schedule.every().day.at("09:30").do(scheduled_job)

print(" Scheduler started. Cleaning will run daily at 09:30.")
while True:
    schedule.run_pending()
    time.sleep(60)
