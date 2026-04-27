# PodcastLens — Comment Intelligence Pipeline

Analyzes YouTube podcast comments for **sentiment** (positive / negative / neutral) and **spam detection** using HuggingFace transformers, with a dark-terminal web UI.

---

## Setup

```bash
cd podcast-analyzer
pip install -r requirements.txt
```

Python 3.10+ recommended. PyTorch with CUDA is optional but speeds up inference.

---

## Run the App

```bash
uvicorn main:app --reload
```

Open **http://localhost:8000** in your browser.

---

## Fine-tuning (Recommended — Run First)

Fine-tune both models before starting the app for best results:

```bash
python fine_tune.py
```

This will:
1. Download `cardiffnlp/tweet_eval` (sentiment) from HuggingFace.
2. Download the YouTube Spam Collection from UCI (or use `./data/youtube_spam.csv` if it exists).
3. Fine-tune `cardiffnlp/twitter-roberta-base-sentiment-latest` → `./models/sentiment-finetuned`.
4. Fine-tune `distilbert-base-uncased` → `./models/spam-finetuned`.
5. Write `models_config.json` so the app auto-loads the fine-tuned models.

Estimated time: 20–60 min (CPU) or 5–15 min (GPU).

### Use your own models

Edit `models_config.json` manually:

```json
{
  "sentiment_model": "./models/my-custom-sentiment",
  "spam_model":      "./models/my-custom-spam",
  "fine_tuned": true,
  "fine_tuned_at": "2024-01-01T00:00:00"
}
```

Any HuggingFace `AutoModelForSequenceClassification`-compatible model path works.

---

## Podcast Detection

PodcastLens uses the **YouTube oEmbed API** (no API key needed) to fetch the video title, then applies a keyword heuristic:

- If `"podcast"` appears directly in the title → accepted.
- If **2 or more** of these signals appear → accepted:
  `episode`, `ep.`, `interview`, `talk`, `discussion`, `conversation`, `show`, `#`, `feat.`, `with `, `hour`, `min`.
- Otherwise the video is rejected with a 400 error.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/analyze` | Submit URL for analysis. Returns `job_id` or cached result. |
| `GET`  | `/api/job/{job_id}` | Poll job status (`pending`/`processing`/`complete`/`failed`). |
| `GET`  | `/api/video/{video_id}/stats` | Full stats for an analyzed video. |
| `GET`  | `/api/history` | List all previously analyzed videos. |
| `DELETE` | `/api/video/{video_id}` | Delete video + all its comments. |

---

## Database

SQLite file: `podcast_analyzer.db` (created automatically in project root).

**Tables:**

`videos` — one row per analyzed podcast URL.  
`comments` — one row per scraped comment, linked to a video by FK.

Key comment columns: `sentiment_label`, `sentiment_score`, `is_spam`, `spam_score`, `spam_reason`, `analyzed_at`.

To inspect:
```bash
sqlite3 podcast_analyzer.db ".tables"
sqlite3 podcast_analyzer.db "SELECT title, total_comments FROM videos;"
```

---

## Project Structure

```
podcast-analyzer/
├── main.py          # FastAPI app + routes + background job runner
├── scraper.py       # YouTube comment scraper (youtube-comment-downloader)
├── models.py        # HuggingFace pipelines + rule-based spam fallback
├── database.py      # SQLAlchemy ORM + CRUD helpers
├── fine_tune.py     # Standalone fine-tuning script
├── requirements.txt
├── static/
│   └── index.html   # Dark-terminal single-page UI
└── README.md
```
