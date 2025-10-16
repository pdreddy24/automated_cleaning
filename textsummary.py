# summarization_script.py
from langchain_ollama import ChatOllama
import json
from collections import Counter

# ====== Initialize LLM ======
llm = ChatOllama(model="mistral")

# ====== Load your cleaned data (JSON file) ======
file_path = r"C:\\Users\\deeks\\OneDrive\\Desktop\\books\\pp\\data\\cleaned\\news_cleaned_20251016_002528.json"
with open(file_path, "r", encoding="utf-8") as f:
    articles = json.load(f)

# ====== Prepare text for summarization ======
all_text = " ".join(
    [
        (a.get("title") or "") + " " +
        (a.get("description") or "") + " " +
        (a.get("content") or "")
        for a in articles
    ]
)

# Truncate if it's too long for the model
text_snippet = all_text[:4000]

# ====== Optional: Quick stats ======
num_articles = len(articles)
sources = [a.get("source", "") for a in articles if a.get("source")]
authors = [a.get("author", "") for a in articles if a.get("author")]

top_sources = Counter(sources).most_common(5)
top_authors = Counter(authors).most_common(5)

stats_text = f"""
Number of articles: {num_articles}

Top Sources: {top_sources}
Top Authors: {top_authors}
"""

# ====== Summarization Prompt ======
prompt = f"""
Summarize the following dataset of news articles.
Include the key themes, trends, and any repeated topics.
Write the summary in 1-2 short paragraphs.

TEXT SNIPPET:
{text_snippet}
"""

# ====== Generate Summary ======
response = llm.invoke(prompt)
summary = response.content.strip()

# ====== Print and Save ======
print("=====  SUMMARY =====")
print(stats_text)
print(summary)

summary_file = file_path.replace(".json", "_summary.txt")
with open(summary_file, "w", encoding="utf-8") as f:
    f.write(stats_text + "\n\n" + summary)

print(f"\n Summary saved to: {summary_file}")
