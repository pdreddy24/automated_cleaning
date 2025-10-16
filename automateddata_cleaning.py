import json
import re
import os
import time
import schedule
from datetime import datetime
from langchain_ollama import ChatOllama

# ====== Load Raw Data ======

def get_latest_raw_file(directory):
    """Return the path of the latest JSON file in the given directory."""
    json_files = [f for f in os.listdir(directory) if f.endswith(".json")]
    if not json_files:
        return None
    json_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return os.path.join(directory, json_files[0])

#with open("C:\\Users\\deeks\\OneDrive\\Desktop\\books\\pp\\data\\raw_news_20251015_225521.json", "r", encoding="utf-8") as f:
    #raw_data = json.load(f)
#print(f"Loaded {len(raw_data)} articles from log file.")

# ====== Initialize LLM ======
llm = ChatOllama(model="mistral")  
response = llm.invoke("Hello, Ollama!")
print(response.content)

# ====== AI Cleaning Function ======
def clean_data_ai(text):
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

# ====== Post-processing Function ======
def post_process_article(article):
    """Programmatically clean fields after AI cleaning."""
    processed = {}

    # Clean and normalize title
    title = article.get("title", "").strip()
    title = re.sub(r"\s+", " ", title)
    processed["title"] = title

    # Clean and normalize description
    desc = article.get("description", "").strip()
    desc = re.sub(r"\s+", " ", desc)
    processed["description"] = desc

    # Clean content if available
    content = article.get("content", "")
    if content:
        content = re.sub(r"<.*?>", "", content)  # remove HTML tags
        content = re.sub(r"\s+", " ", content)
    processed["content"] = content

    # Author and source (optional)
    processed["author"] = article.get("author", "").strip()
    processed["source"] = article.get("source", "").strip()

    # Standardize published date to ISO format if present
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

# ====== Duplicate Removal Function ======
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

# ====== Main Cleaning Loop ======
cleaned_articles = []

for article in get_latest_raw_file("data"):
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

#  Remove duplicates before saving
cleaned_articles = remove_duplicates(cleaned_articles)

# ====== Save Cleaned Data ======
os.makedirs("data/cleaned", exist_ok=True)
filename = f"data/cleaned/news_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

with open(filename, "w", encoding="utf-8") as f:
    json.dump(cleaned_articles, f, indent=2, ensure_ascii=False)

print(f"Cleaned data saved to {filename}")

# ====== Scheduler Job ======
def cleaning_job():
    print("Starting automated cleaning...")
    with open("C:\\Users\\deeks\\OneDrive\\Desktop\\books\\pp\\data\\raw_news_20251015_225521.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    cleaned_articles = []
    for article in raw_data:
        ai_cleaned = {
            "title": clean_data_ai(article.get("title", "")),
            "description": clean_data_ai(article.get("description", "")),
            "content": clean_data_ai(article.get("content", "")),
            "author": article.get("author", ""),
            "source": article.get("source", ""),
            "publishedAt": article.get("publishedAt", ""),
        }
        fully_cleaned = post_process_article(ai_cleaned)
        cleaned_articles.append(fully_cleaned)

    #  Remove duplicates in scheduled job too
    cleaned_articles = remove_duplicates(cleaned_articles)

    filename = f"data/cleaned/news_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cleaned_articles, f, indent=2, ensure_ascii=False)

    print(f"Automated cleaning complete. Saved to {filename}")

# Schedule daily job
schedule.every().day.at("09:30").do(cleaning_job)

while True:
    schedule.run_pending()
    time.sleep(60)
