import asyncio
import uuid
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import (
    init_db,
    get_db,
    save_video,
    get_video_by_url,
    save_comments_batch,
    update_comment_analysis,
    get_video_stats,
    SessionLocal,
    Video,
    Comment,
)
from scraper import extract_video_id, is_podcast, scrape_comments
from models import analyzer

# ---------------------------------------------------------------------------
app = FastAPI(title="PodcastLens", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

# In-memory job store — keyed by UUID job_id
jobs: dict[str, dict] = {}


# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    init_db()


# ---------------------------------------------------------------------------
class AnalyzeRequest(BaseModel):
    url: str
    max_comments: int = 200


# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return FileResponse("static/index.html")


# ---------------------------------------------------------------------------
async def _run_analysis_job(job_id: str, url: str, video_id: str, max_comments: int):
    """Background task: scrape → save → analyze → save results."""
    db: Session = SessionLocal()
    start = datetime.utcnow()

    def _update(status: str, message: str, progress: int):
        jobs[job_id].update({"status": status, "message": message, "progress": progress})

    try:
        _update("processing", "Fetching video metadata…", 5)

        _, title = await is_podcast(video_id)

        _update("processing", "Scraping comments…", 10)

        comments = await asyncio.to_thread(scrape_comments, video_id, max_comments)

        if not comments:
            _update("failed", "No comments found or scraping failed.", 0)
            return

        _update("processing", f"Scraped {len(comments)} comments. Saving to database…", 40)

        video = save_video(db, url, video_id, title, True)
        save_comments_batch(db, video.id, comments)

        _update("processing", f"Loading ML models and analyzing {len(comments)} comments…", 50)

        analyzed = await asyncio.to_thread(analyzer.analyze_batch, comments)

        _update("processing", "Persisting analysis results…", 80)

        for result in analyzed:
            update_comment_analysis(db, result["comment_id"], result)

        video.total_comments = len(comments)
        db.commit()

        elapsed = round((datetime.utcnow() - start).total_seconds(), 2)
        stats = get_video_stats(db, video_id)
        if stats is not None:
            stats["processing_time"] = elapsed

        jobs[job_id].update(
            {
                "status": "complete",
                "progress": 100,
                "message": "Analysis complete.",
                "result": stats,
            }
        )

    except Exception as exc:
        jobs[job_id].update({"status": "failed", "message": str(exc), "progress": 0})

    finally:
        db.close()


# ---------------------------------------------------------------------------
@app.post("/api/analyze")
async def analyze(
    request: AnalyzeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    url = request.url.strip()

    if "youtube.com" not in url and "youtu.be" not in url:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL.")

    try:
        video_id = extract_video_id(url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    podcast, _ = await is_podcast(video_id)
    if not podcast:
        raise HTTPException(status_code=400, detail="Not a podcast video.")

    # Return cached result if already fully analyzed
    existing = get_video_by_url(db, url)
    if existing and existing.total_comments and existing.total_comments > 0:
        stats = get_video_stats(db, video_id)
        return {"job_id": None, "cached": True, "result": stats}

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "pending",
        "progress": 0,
        "message": "Job queued.",
        "result": None,
        "created_at": datetime.utcnow().isoformat(),
    }

    background_tasks.add_task(_run_analysis_job, job_id, url, video_id, request.max_comments)

    return {"job_id": job_id, "cached": False}


# ---------------------------------------------------------------------------
@app.get("/api/job/{job_id}")
async def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found.")
    return jobs[job_id]


# ---------------------------------------------------------------------------
@app.get("/api/video/{video_id}/stats")
async def video_stats(video_id: str, db: Session = Depends(get_db)):
    stats = get_video_stats(db, video_id)
    if stats is None:
        raise HTTPException(status_code=404, detail="Video not found.")
    return stats


# ---------------------------------------------------------------------------
@app.get("/api/history")
async def history(db: Session = Depends(get_db)):
    videos = db.query(Video).order_by(Video.scraped_at.desc()).all()
    return [
        {
            "id": v.id,
            "url": v.url,
            "video_id": v.video_id,
            "title": v.title,
            "total_comments": v.total_comments,
            "scraped_at": v.scraped_at.isoformat() if v.scraped_at else None,
        }
        for v in videos
    ]


# ---------------------------------------------------------------------------
@app.delete("/api/video/{video_id}")
async def delete_video(video_id: str, db: Session = Depends(get_db)):
    video = db.query(Video).filter(Video.video_id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found.")
    db.query(Comment).filter(Comment.video_id == video.id).delete()
    db.delete(video)
    db.commit()
    return {"message": "Video and its comments deleted."}
