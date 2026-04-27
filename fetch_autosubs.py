"""
Fetch auto-generated subtitles for videos that have no manual transcript.
Uses youtube-transcript-api (lists all available tracks and picks the best one),
then falls back to yt-dlp with browser cookies if still blocked.
"""
import csv
import json
import os
import re
import sys
import tempfile
import time

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TRANSCRIPTS_DIR = "transcripts"
SUMMARY_FILE = os.path.join(TRANSCRIPTS_DIR, "_summary.csv")
MISSING = {
    "u3gYBBO3Iro": "Prakhar Gupta x The Rebel Kid | PGX #76 @the.rebelkid",
    "rPH5RUdaEMQ": "Samay Raina | Still Alive, India's Got Latent S2 & Standup Comedy | The Longest Interview",
    # 2y0wMI143bg has zero auto-subs, skip it
}


def safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip()[:100]


def fetch_via_transcript_api(video_id: str):
    from youtube_transcript_api import YouTubeTranscriptApi
    api = YouTubeTranscriptApi()
    transcript_list = api.list(video_id)
    # prefer manually created, then auto-generated English, then any English
    for t in transcript_list:
        if t.language_code.startswith("en") and not t.is_generated:
            return t.fetch()
    for t in transcript_list:
        if t.language_code.startswith("en"):
            return t.fetch()
    # take whatever is first
    for t in transcript_list:
        return t.fetch()
    return None


def fetch_via_ytdlp(video_id: str, tmp_dir: str):
    import yt_dlp

    def try_download(extra_opts):
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "writeautomaticsub": True,
            "writesubtitles": True,
            "subtitleslangs": ["en", "en-US", "en-GB"],
            "subtitlesformat": "json3",
            "outtmpl": os.path.join(tmp_dir, "%(id)s.%(ext)s"),
            **extra_opts,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

    # Try with browser cookies first
    for browser in ("chrome", "edge", "firefox", "brave"):
        try:
            print(f"  Trying yt-dlp with {browser} cookies...", end=" ", flush=True)
            try_download({"cookiesfrombrowser": (browser, None, None, None)})
            print("ok")
            break
        except Exception as e:
            print(f"failed ({e.__class__.__name__})")
    else:
        print("  Trying yt-dlp without cookies...")
        try_download({})

    # Find the downloaded subtitle file
    for fname in os.listdir(tmp_dir):
        if fname.startswith(video_id) and fname.endswith(".json3"):
            with open(os.path.join(tmp_dir, fname), encoding="utf-8") as f:
                data = json.load(f)
            return parse_json3(data)
    return None


def parse_json3(data: dict) -> list:
    rows = []
    for event in data.get("events", []):
        segs = event.get("segs")
        if not segs:
            continue
        text = "".join(s.get("utf8", "") for s in segs).strip()
        text = re.sub(r"\s+", " ", text)
        if not text:
            continue
        start_s = event.get("tStartMs", 0) / 1000
        dur_s = event.get("dDurationMs", 0) / 1000
        rows.append((start_s, dur_s, text))
    return rows


def segments_to_rows(segments) -> list:
    """Normalise both FetchedTranscript objects and plain (start,dur,text) tuples."""
    rows = []
    for seg in segments:
        if isinstance(seg, tuple):
            rows.append(seg)
        else:
            rows.append((seg.start, seg.duration, seg.text))
    return rows


def save_csv(video_id: str, title: str, rows: list) -> str:
    label = safe_filename(title) if title else video_id
    path = os.path.join(TRANSCRIPTS_DIR, f"{label}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["video_id", "start", "duration", "text"])
        for start, dur, text in rows:
            writer.writerow([video_id, start, dur, text])
    return path


def update_summary(video_id: str, new_file: str):
    with open(SUMMARY_FILE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        if video_id in row.get("url", ""):
            row["status"] = "ok"
            row["file"] = new_file
    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "status", "file"])
        writer.writeheader()
        writer.writerows(rows)


def main():
    os.makedirs(TRANSCRIPTS_DIR, exist_ok=True)

    for video_id, title in MISSING.items():
        print(f"\n[{video_id}] {title}")

        segments = None

        # Method 1: youtube-transcript-api (auto-generated captions)
        print("  Trying youtube-transcript-api...", end=" ", flush=True)
        try:
            segments = fetch_via_transcript_api(video_id)
            if segments:
                print(f"got {len(list(segments))} segments")
        except Exception as e:
            print(f"failed: {e.__class__.__name__}: {str(e)[:80]}")
            segments = None

        # Method 2: yt-dlp with browser cookies
        if not segments:
            with tempfile.TemporaryDirectory() as tmp:
                try:
                    raw = fetch_via_ytdlp(video_id, tmp)
                    if raw:
                        segments = raw
                        print(f"  yt-dlp: got {len(raw)} segments")
                except Exception as e:
                    print(f"  yt-dlp failed: {e}")

        if not segments:
            print(f"  Could not retrieve transcript by any method.")
            continue

        rows = segments_to_rows(segments)
        path = save_csv(video_id, title, rows)
        update_summary(video_id, path)
        print(f"  Saved {len(rows)} rows -> {path}")
        time.sleep(2)

    print("\nDone.")


if __name__ == "__main__":
    main()
