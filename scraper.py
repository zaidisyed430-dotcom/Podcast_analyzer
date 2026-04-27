import re
import httpx
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR

PODCAST_KEYWORDS = [
    "podcast", "episode", "ep.", "interview", "talk", "discussion",
    "conversation", "show", "#", "feat.", "with ", "hour", "min",
]


def extract_video_id(url: str) -> str:
    """Extract YouTube video ID from any URL format."""
    patterns = [
        r"(?:v=|/v/|youtu\.be/|/embed/|/shorts/)([a-zA-Z0-9_-]{11})",
        r"^([a-zA-Z0-9_-]{11})$",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    raise ValueError(f"Could not extract video ID from URL: {url}")


async def is_podcast(video_id: str) -> tuple[bool, str]:
    """
    Detect if a YouTube video is a podcast using the oEmbed API.
    Returns (is_podcast, title).
    Heuristic: title contains 'podcast' directly, OR 2+ keyword signals match.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"https://www.youtube.com/oembed"
                f"?url=https://www.youtube.com/watch?v={video_id}&format=json"
            )
            if resp.status_code != 200:
                return False, ""

            data = resp.json()
            title: str = data.get("title", "")
            title_lower = title.lower()

            if "podcast" in title_lower:
                return True, title

            signal_count = sum(1 for kw in PODCAST_KEYWORDS if kw in title_lower)
            return signal_count >= 2, title

    except Exception:
        return False, ""


def scrape_comments(video_id: str, max_comments: int = 500) -> list[dict]:
    """Scrape top comments from a YouTube video. Returns empty list on failure."""
    try:
        downloader = YoutubeCommentDownloader()
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        comments: list[dict] = []

        for raw in downloader.get_comments_from_url(video_url, sort_by=SORT_BY_POPULAR):
            if len(comments) >= max_comments:
                break
            comments.append(
                {
                    "comment_id": raw.get("cid", ""),
                    "author": raw.get("author", ""),
                    "text": raw.get("text", ""),
                    "likes": int(raw.get("votes", 0) or 0),
                    "published_at": raw.get("time", ""),
                }
            )

        return comments

    except Exception as e:
        print(f"[scraper] Error scraping {video_id}: {e}")
        return []
