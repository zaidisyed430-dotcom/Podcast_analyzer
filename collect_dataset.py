"""
collect_dataset.py
==================
Scrapes 20k-30k English podcast YouTube comments WITH full video metadata.
Every comment is saved — no filtering — so the model learns all patterns.

Modes
-----
  python collect_dataset.py            # auto: process all 25 channels
  python collect_dataset.py --manual   # paste YouTube URLs one by one

YouTube Data API v3
-------------------
  1 unit  = 1 request  (max 100 comments per commentThreads request)
  1000 comments per video = 10 requests = 10 units
  Daily free quota: 10,000 units  →  comfortably handles all 25 channels

Quota breakdown per run:
  channels.list      25 calls   ×  1 unit  =    25 units
  playlistItems.list 75 calls   ×  1 unit  =    75 units
  videos.list        375 calls  ×  1 unit  =   375 units
  commentThreads     ~300 calls ×  1 unit  =   300 units
  ─────────────────────────────────────────────────────
  Total estimated                          ≈   775 units / run
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

import requests
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY       = os.getenv("YOUTUBE_API_KEY", "AIzaSyCiHzaxQwJRDwP-Fuda-nkiXJlW-5MX6Tc")
OUTPUT_CSV    = "dataset_raw.csv"
PROGRESS_FILE = ".scrape_progress.json"
YT_API        = "https://www.googleapis.com/youtube/v3"

TARGET_PER_CHANNEL        = 1_000   # min comments to collect per auto channel
MAX_VIDEOS_PER_CHANNEL    = 20      # max videos to try before moving on
MANUAL_COMMENTS_PER_VIDEO = 1_000   # comments fetched in --manual mode
API_DELAY                 = 0.25    # seconds between API calls
SCRAPER_DELAY             = 2.5     # seconds between scraper calls (polite)

# ---------------------------------------------------------------------------
# 25 diverse English podcast channels
# ---------------------------------------------------------------------------
PODCAST_CHANNELS = [
    {"name": "Lex Fridman Podcast",           "handle": "lexfridman"},
    {"name": "Huberman Lab",                  "handle": "hubermanlab"},
    {"name": "The Diary of a CEO",            "handle": "TheDiaryOfACEO"},
    {"name": "All-In Podcast",                "handle": "allinpodcast"},
    {"name": "My First Million",              "handle": "myfirstmillion"},
    {"name": "Tim Ferriss Show",              "handle": "timferriss"},
    {"name": "Stuff You Should Know",         "handle": "StuffYouShouldKnow"},
    {"name": "Hidden Brain",                  "handle": "hiddenbrain"},
    {"name": "Planet Money",                  "handle": "PlanetMoney"},
    {"name": "Freakonomics Radio",            "handle": "freakonomicsradio"},
    {"name": "SmartLess",                     "handle": "smartless"},
    {"name": "Armchair Expert",               "handle": "armchairexpert"},
    {"name": "BiggerPockets",                 "handle": "biggerpockets"},
    {"name": "Radiolab",                      "handle": "radiolab"},
    {"name": "Conan O'Brien Needs a Friend",  "handle": "ConanOBrien"},
    {"name": "TED Talks",                     "handle": "TED"},
    {"name": "How I Built This",              "handle": "HowIBuiltThis"},
    {"name": "Masters of Scale",              "handle": "mastersofscale"},
    {"name": "Darknet Diaries",               "handle": "darknetdiaries"},
    {"name": "Acquired",                      "handle": "acquiredfm"},
    {"name": "The Knowledge Project",         "handle": "theknowledgeproject"},
    {"name": "Invest Like the Best",          "handle": "investlikethebest"},
    {"name": "The Tim Dillon Show",           "handle": "TheTimDillonShow"},
    {"name": "Crime Junkie",                  "handle": "crimejunkiepodcast"},
    {"name": "This American Life",            "handle": "ThisAmericanLife"},
]

# ---------------------------------------------------------------------------
# CSV columns
# video metadata columns are repeated per comment row for easy ML use
# ---------------------------------------------------------------------------
COLUMNS = [
    # ── Video metadata ──────────────────────────────────────────────────────
    "video_id",
    "video_title",
    "video_description",      # first 400 chars
    "channel_id",
    "channel_name",
    "video_published_at",
    "view_count",
    "like_count",
    "video_total_comments",   # total comment count reported by YouTube
    "duration_seconds",
    "video_tags",             # up to 10 tags, comma-separated
    # ── Comment data ────────────────────────────────────────────────────────
    "comment_id",
    "author",
    "author_channel_id",
    "comment_text",
    "likes",
    "reply_count",
    "comment_published_at",
    "comment_updated_at",
    # ── Housekeeping ─────────────────────────────────────────────────────────
    "source",           # 'api' or 'scraper'
    "collection_mode",  # 'auto' or 'manual'
    # ── Labels (fill these in — DO NOT leave both blank when training) ───────
    "sentiment_label",  # positive / negative / neutral
    "spam_label",       # spam / ham
]

# ---------------------------------------------------------------------------
# Helpers — duration & video ID
# ---------------------------------------------------------------------------
def parse_duration(iso: str) -> int:
    """Convert ISO 8601 duration string (PT1H23M45S) to total seconds."""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return 0
    h, mn, s = (int(x or 0) for x in m.groups())
    return h * 3600 + mn * 60 + s


def extract_video_id(url: str) -> str | None:
    for pattern in [
        r"(?:v=|/v/|youtu\.be/|/embed/|/shorts/)([a-zA-Z0-9_-]{11})",
        r"^([a-zA-Z0-9_-]{11})$",
    ]:
        m = re.search(pattern, url.strip())
        if m:
            return m.group(1)
    return None


# ---------------------------------------------------------------------------
# YouTube Data API v3 — core GET wrapper
# ---------------------------------------------------------------------------
_QUOTA_EXHAUSTED = False


def _api(endpoint: str, params: dict) -> dict | None:
    global _QUOTA_EXHAUSTED
    params["key"] = API_KEY
    for attempt in range(3):
        try:
            r = requests.get(f"{YT_API}/{endpoint}", params=params, timeout=20)
            if r.status_code == 403:
                body   = r.json()
                errors = body.get("error", {}).get("errors") or [{}]
                reason = errors[0].get("reason", "")
                if reason == "quotaExceeded":
                    print("\n[!] Daily API quota exceeded — falling back to scraper.")
                    _QUOTA_EXHAUSTED = True
                    return None
                if reason == "commentsDisabled":
                    return None   # silent skip — video has comments turned off
            r.raise_for_status()
            time.sleep(API_DELAY)
            return r.json()
        except requests.RequestException as exc:
            if attempt == 2:
                print(f"  API error ({endpoint}): {exc}")
                return None
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# API — channel & playlist
# ---------------------------------------------------------------------------
def resolve_channel(handle: str) -> tuple[str | None, str | None]:
    """Return (channel_id, uploads_playlist_id) for a YouTube handle."""
    data = _api("channels", {
        "part":      "contentDetails",
        "forHandle": handle,
        "maxResults": 1,
    })
    if not data or not data.get("items"):
        return None, None
    item     = data["items"][0]
    ch_id    = item["id"]
    playlist = item["contentDetails"]["relatedPlaylists"]["uploads"]
    return ch_id, playlist


def iter_video_ids(playlist_id: str, max_videos: int):
    """Yield (video_id, title) from the uploads playlist."""
    page_token = None
    yielded    = 0
    while yielded < max_videos:
        params = {
            "part":       "snippet",
            "playlistId": playlist_id,
            "maxResults": min(50, max_videos - yielded),
        }
        if page_token:
            params["pageToken"] = page_token
        data = _api("playlistItems", params)
        if not data:
            break
        for item in data.get("items", []):
            sn = item["snippet"]
            yield sn["resourceId"]["videoId"], sn.get("title", "")
            yielded += 1
            if yielded >= max_videos:
                return
        page_token = data.get("nextPageToken")
        if not page_token:
            break


# ---------------------------------------------------------------------------
# API — video metadata
# ---------------------------------------------------------------------------
def get_video_metadata(video_id: str, fallback_channel_name: str = "") -> dict:
    """Return a dict of video metadata fields (never None — uses fallbacks)."""
    data = _api("videos", {
        "part": "snippet,statistics,contentDetails",
        "id":   video_id,
    })
    if data and data.get("items"):
        item = data["items"][0]
        sn   = item["snippet"]
        st   = item.get("statistics", {})
        cd   = item.get("contentDetails", {})
        return {
            "video_id":             video_id,
            "video_title":          sn.get("title", ""),
            "video_description":    sn.get("description", "")[:400].replace("\n", " "),
            "channel_id":           sn.get("channelId", ""),
            "channel_name":         sn.get("channelTitle", fallback_channel_name),
            "video_published_at":   sn.get("publishedAt", ""),
            "view_count":           int(st.get("viewCount", 0) or 0),
            "like_count":           int(st.get("likeCount", 0) or 0),
            "video_total_comments": int(st.get("commentCount", 0) or 0),
            "duration_seconds":     parse_duration(cd.get("duration", "")),
            "video_tags":           ", ".join(sn.get("tags", [])[:10]),
        }
    # Fallback when API is unavailable
    return {
        "video_id": video_id, "video_title": "", "video_description": "",
        "channel_id": "", "channel_name": fallback_channel_name,
        "video_published_at": "", "view_count": 0, "like_count": 0,
        "video_total_comments": 0, "duration_seconds": 0, "video_tags": "",
    }


# ---------------------------------------------------------------------------
# API — comments (1 unit per 100 comments)
# ---------------------------------------------------------------------------
def fetch_comments_api(video_id: str, target: int) -> list[dict]:
    """Fetch up to `target` top-level comments via the Data API."""
    comments   = []
    page_token = None
    while len(comments) < target:
        params = {
            "part":       "snippet",
            "videoId":    video_id,
            "maxResults": 100,
            "order":      "relevance",
            "textFormat": "plainText",
        }
        if page_token:
            params["pageToken"] = page_token
        data = _api("commentThreads", params)
        if not data:
            break
        for item in data.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "comment_id":           item["id"],
                "author":               top.get("authorDisplayName", ""),
                "author_channel_id":    top.get("authorChannelId", {}).get("value", ""),
                "comment_text":         top.get("textOriginal", "").strip(),
                "likes":                int(top.get("likeCount", 0)),
                "reply_count":          int(item["snippet"].get("totalReplyCount", 0)),
                "comment_published_at": top.get("publishedAt", ""),
                "comment_updated_at":   top.get("updatedAt", ""),
            })
            if len(comments) >= target:
                break
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return comments


# ---------------------------------------------------------------------------
# Fallback — youtube-comment-downloader (no API key needed)
# ---------------------------------------------------------------------------
def fetch_comments_scraper(video_id: str, target: int) -> list[dict]:
    """Scraper fallback when API quota is exhausted."""
    try:
        dl  = YoutubeCommentDownloader()
        url = f"https://www.youtube.com/watch?v={video_id}"
        out = []
        for raw in dl.get_comments_from_url(url, sort_by=SORT_BY_POPULAR):
            if len(out) >= target:
                break
            out.append({
                "comment_id":           raw.get("cid") or f"sc_{video_id}_{len(out)}",
                "author":               raw.get("author", ""),
                "author_channel_id":    "",
                "comment_text":         raw.get("text", "").strip(),
                "likes":                int(raw.get("votes", 0) or 0),
                "reply_count":          int(raw.get("replies", 0) or 0),
                "comment_published_at": raw.get("time", ""),
                "comment_updated_at":   "",
            })
        time.sleep(SCRAPER_DELAY)
        return out
    except Exception as exc:
        print(f"    Scraper error on {video_id}: {exc}")
        return []


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
def open_csv() -> tuple:
    """Open CSV in append mode. Write header if file is new."""
    is_new  = not Path(OUTPUT_CSV).exists()
    csv_f   = open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig")
    writer  = csv.DictWriter(csv_f, fieldnames=COLUMNS)
    if is_new:
        writer.writeheader()
    return csv_f, writer


def write_batch(
    writer:   csv.DictWriter,
    meta:     dict,
    comments: list[dict],
    source:   str,
    mode:     str,
    seen_ids: set,
) -> int:
    """Write comments to CSV, skip duplicates. Returns count of new rows."""
    added = 0
    for c in comments:
        cid = c["comment_id"]
        if cid in seen_ids:
            continue
        seen_ids.add(cid)
        writer.writerow({
            **meta,
            **c,
            "source":           source,
            "collection_mode":  mode,
            "sentiment_label":  "",
            "spam_label":       "",
        })
        added += 1
    return added


# ---------------------------------------------------------------------------
# Progress (resume across runs)
# ---------------------------------------------------------------------------
def load_progress() -> dict:
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"done_channels": [], "seen_ids": [], "total": 0}


def save_progress(p: dict, seen_ids: set, total: int, done: set) -> None:
    p["seen_ids"]       = list(seen_ids)[-300_000:]
    p["total"]          = total
    p["done_channels"]  = list(done)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(p, f)


# ---------------------------------------------------------------------------
# Auto mode — process all 25 channels
# ---------------------------------------------------------------------------
def run_auto() -> None:
    progress  = load_progress()
    seen_ids  = set(progress.get("seen_ids", []))
    total     = progress.get("total", 0)
    done_set  = set(progress.get("done_channels", []))

    csv_f, writer = open_csv()
    print(f"\nAuto mode  |  {len(PODCAST_CHANNELS)} channels  |  {TARGET_PER_CHANNEL} comments each")
    print(f"Target: ~{len(PODCAST_CHANNELS) * TARGET_PER_CHANNEL:,} comments  |  Already collected: {total:,}\n")

    try:
        for channel in PODCAST_CHANNELS:
            cname  = channel["name"]
            handle = channel["handle"]

            if cname in done_set:
                print(f"[skip] {cname}")
                continue

            print(f"\n{'─'*65}")
            print(f"  {cname}  (@{handle})  —  total so far: {total:,}")

            # Resolve channel to get uploads playlist
            if _QUOTA_EXHAUSTED:
                print("  Quota exhausted. Skipping (no video list without API).")
                done_set.add(cname)
                continue

            _, uploads_id = resolve_channel(handle)
            if not uploads_id:
                print(f"  Could not resolve @{handle} — skipping.")
                done_set.add(cname)
                continue

            ch_total = 0
            for vid_id, vid_title in iter_video_ids(uploads_id, MAX_VIDEOS_PER_CHANNEL):
                if ch_total >= TARGET_PER_CHANNEL:
                    break

                need = TARGET_PER_CHANNEL - ch_total
                meta = get_video_metadata(vid_id, fallback_channel_name=cname)

                if _QUOTA_EXHAUSTED:
                    raw    = fetch_comments_scraper(vid_id, need)
                    source = "scraper"
                else:
                    raw    = fetch_comments_api(vid_id, need)
                    source = "api"
                    if _QUOTA_EXHAUSTED:  # hit mid-video
                        raw    = fetch_comments_scraper(vid_id, need)
                        source = "scraper"

                added     = write_batch(writer, meta, raw, source, "auto", seen_ids)
                ch_total += added
                total    += added
                csv_f.flush()
                print(f"  {vid_title[:57]:<57}  +{added:>4}  (ch: {ch_total})")

            done_set.add(cname)
            save_progress(progress, seen_ids, total, done_set)
            print(f"  Channel done: {ch_total} comments  |  Running total: {total:,}")

    except KeyboardInterrupt:
        print("\n\nInterrupted — progress saved. Re-run to continue.")
    finally:
        csv_f.close()
        save_progress(progress, seen_ids, total, done_set)

    print(f"\n{'='*65}")
    print(f"Auto mode complete.  {total:,} comments saved to {OUTPUT_CSV}")
    print(f"{'='*65}")


# ---------------------------------------------------------------------------
# Manual URL mode — user pastes URLs interactively
# ---------------------------------------------------------------------------
def run_manual() -> None:
    progress = load_progress()
    seen_ids = set(progress.get("seen_ids", []))
    total    = progress.get("total", 0)
    done_set = set(progress.get("done_channels", []))

    csv_f, writer = open_csv()

    print("\n" + "="*65)
    print("  Manual URL mode")
    print(f"  Fetches {MANUAL_COMMENTS_PER_VIDEO} comments + full video metadata per URL.")
    print(f"  Appends to: {OUTPUT_CSV}  (already has {total:,} rows)")
    print("  Type 'quit' or press Ctrl+C to exit.")
    print("="*65 + "\n")

    try:
        while True:
            try:
                raw_input = input("Paste YouTube URL: ").strip()
            except EOFError:
                break

            if raw_input.lower() in ("quit", "exit", "q", "done", ""):
                if raw_input == "":
                    continue
                break

            vid_id = extract_video_id(raw_input)
            if not vid_id:
                print("  ✗ Could not parse a video ID from that URL. Try again.\n")
                continue

            print(f"  Video ID : {vid_id}")
            print("  Fetching metadata...")
            meta = get_video_metadata(vid_id)
            print(f"  Title    : {meta['video_title'] or '(unavailable)'}")
            print(f"  Channel  : {meta['channel_name'] or '(unavailable)'}")
            if meta["view_count"]:
                print(f"  Views    : {meta['view_count']:,}  |  Likes: {meta['like_count']:,}")
            if meta["duration_seconds"]:
                m, s = divmod(meta["duration_seconds"], 60)
                h, m = divmod(m, 60)
                print(f"  Duration : {h:02d}:{m:02d}:{s:02d}")

            print(f"  Fetching up to {MANUAL_COMMENTS_PER_VIDEO} comments...")

            if _QUOTA_EXHAUSTED:
                raw    = fetch_comments_scraper(vid_id, MANUAL_COMMENTS_PER_VIDEO)
                source = "scraper"
            else:
                raw    = fetch_comments_api(vid_id, MANUAL_COMMENTS_PER_VIDEO)
                source = "api"
                if _QUOTA_EXHAUSTED:
                    raw    = fetch_comments_scraper(vid_id, MANUAL_COMMENTS_PER_VIDEO)
                    source = "scraper"

            added  = write_batch(writer, meta, raw, source, "manual", seen_ids)
            total += added
            csv_f.flush()
            save_progress(progress, seen_ids, total, done_set)

            print(f"  ✓ Saved {added} new comments  |  Running total: {total:,}\n")

    except KeyboardInterrupt:
        print("\n\nExiting.")
    finally:
        csv_f.close()
        save_progress(progress, seen_ids, total, done_set)

    print(f"\nDone.  {total:,} comments in {OUTPUT_CSV}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="PodcastLens dataset collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python collect_dataset.py            # auto: all 25 channels\n"
            "  python collect_dataset.py --manual   # paste URLs interactively"
        ),
    )
    parser.add_argument(
        "--manual", action="store_true",
        help="Interactive mode: paste YouTube video URLs one by one",
    )
    args = parser.parse_args()

    if args.manual:
        run_manual()
    else:
        run_auto()
        print("\nTip: run with --manual to add more videos from any channel.")


if __name__ == "__main__":
    main()
