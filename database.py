from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import datetime
from typing import Optional
import random

DATABASE_URL = "sqlite:///./podcast_analyzer.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Video(Base):
    __tablename__ = "videos"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, unique=True, index=True)
    video_id = Column(String, index=True)
    title = Column(String, nullable=True)
    total_comments = Column(Integer, default=0)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    is_podcast = Column(Boolean, default=True)


class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(Integer, ForeignKey("videos.id"), index=True)
    comment_id = Column(String, unique=True, index=True)
    author = Column(String)
    text = Column(String)
    likes = Column(Integer, default=0)
    published_at = Column(String)
    sentiment_label = Column(String, nullable=True)
    sentiment_score = Column(Float, nullable=True)
    is_spam = Column(Boolean, nullable=True)
    spam_score = Column(Float, nullable=True)
    spam_reason = Column(String, nullable=True)
    analyzed_at = Column(DateTime, nullable=True)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def save_video(db: Session, url: str, video_id: str, title: str, is_podcast_flag: bool) -> Video:
    existing = db.query(Video).filter(Video.url == url).first()
    if existing:
        existing.title = title
        existing.scraped_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return existing

    video = Video(
        url=url,
        video_id=video_id,
        title=title,
        total_comments=0,
        scraped_at=datetime.utcnow(),
        is_podcast=is_podcast_flag,
    )
    db.add(video)
    db.commit()
    db.refresh(video)
    return video


def get_video_by_url(db: Session, url: str) -> Optional[Video]:
    return db.query(Video).filter(Video.url == url).first()


def save_comments_batch(db: Session, video_db_id: int, comments: list) -> int:
    saved = 0
    for c in comments:
        existing = db.query(Comment).filter(Comment.comment_id == c["comment_id"]).first()
        if not existing:
            comment = Comment(
                video_id=video_db_id,
                comment_id=c["comment_id"],
                author=c.get("author", ""),
                text=c.get("text", "")[:1000],
                likes=int(c.get("likes", 0) or 0),
                published_at=c.get("published_at", ""),
            )
            db.add(comment)
            saved += 1
    db.commit()
    return saved


def update_comment_analysis(db: Session, comment_id: str, analysis: dict):
    comment = db.query(Comment).filter(Comment.comment_id == comment_id).first()
    if comment:
        comment.sentiment_label = analysis.get("sentiment_label")
        comment.sentiment_score = analysis.get("sentiment_score")
        comment.is_spam = analysis.get("is_spam")
        comment.spam_score = analysis.get("spam_score")
        comment.spam_reason = analysis.get("spam_reason")
        comment.analyzed_at = datetime.utcnow()
    db.commit()


def get_comments_by_video(db: Session, video_db_id: int) -> list:
    return db.query(Comment).filter(Comment.video_id == video_db_id).all()


def comment_to_dict(c: Comment) -> dict:
    return {
        "id": c.id,
        "comment_id": c.comment_id,
        "author": c.author,
        "text": c.text,
        "likes": c.likes,
        "published_at": c.published_at,
        "sentiment_label": c.sentiment_label,
        "sentiment_score": c.sentiment_score,
        "is_spam": c.is_spam,
        "spam_score": c.spam_score,
        "spam_reason": c.spam_reason,
    }


def get_video_stats(db: Session, video_id: str) -> Optional[dict]:
    video = db.query(Video).filter(Video.video_id == video_id).first()
    if not video:
        return None

    comments = get_comments_by_video(db, video.id)
    total = len(comments)

    sentiment_breakdown = {"positive": 0, "negative": 0, "neutral": 0}
    spam_reasons: dict = {}
    spam_count = 0
    sentiment_scores = []

    for c in comments:
        if c.sentiment_label and c.sentiment_label in sentiment_breakdown:
            sentiment_breakdown[c.sentiment_label] += 1
        if c.sentiment_score is not None:
            sentiment_scores.append(c.sentiment_score)
        if c.is_spam:
            spam_count += 1
            reason = c.spam_reason or "other"
            spam_reasons[reason] = spam_reasons.get(reason, 0) + 1

    sentiment_percentages = {
        k: round(v / total * 100, 1) if total > 0 else 0.0
        for k, v in sentiment_breakdown.items()
    }

    avg_sentiment = round(sum(sentiment_scores) / len(sentiment_scores), 4) if sentiment_scores else 0.0

    top_positive = sorted(
        [c for c in comments if c.sentiment_label == "positive" and c.sentiment_score is not None],
        key=lambda x: x.sentiment_score,
        reverse=True,
    )[:3]

    top_spam = sorted(
        [c for c in comments if c.is_spam and c.spam_score is not None],
        key=lambda x: x.spam_score,
        reverse=True,
    )[:3]

    legit = [c for c in comments if not c.is_spam]
    sample = random.sample(legit, min(10, len(legit)))

    all_comments = [comment_to_dict(c) for c in comments]

    return {
        "video_id": video.video_id,
        "title": video.title,
        "url": video.url,
        "total_comments": total,
        "avg_sentiment_score": avg_sentiment,
        "sentiment_breakdown": sentiment_breakdown,
        "sentiment_percentages": sentiment_percentages,
        "spam_count": spam_count,
        "spam_percentage": round(spam_count / total * 100, 1) if total > 0 else 0.0,
        "spam_reasons": {k: v for k, v in spam_reasons.items() if k != "legitimate"},
        "top_positive_comments": [comment_to_dict(c) for c in top_positive],
        "top_spam_comments": [comment_to_dict(c) for c in top_spam],
        "sample_comments": [comment_to_dict(c) for c in sample],
        "all_comments": all_comments,
    }
