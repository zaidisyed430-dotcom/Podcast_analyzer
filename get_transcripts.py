import json
import csv
import os
import re
import sys
import time
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from urllib.parse import urlparse, parse_qs

# Fix Windows console encoding for non-ASCII titles
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

URLS_FILE = "urls.json"
OUTPUT_DIR = "transcripts"


def extract_video_id(url: str) -> str:
    """Extract YouTube video ID from youtu.be or youtube.com URLs."""
    parsed = urlparse(url)
    if parsed.netloc in ("youtu.be", "www.youtu.be"):
        return parsed.path.lstrip("/").split("?")[0]
    qs = parse_qs(parsed.query)
    return qs.get("v", [None])[0]


def safe_filename(name: str) -> str:
    """Strip characters that are illegal in Windows/Linux filenames."""
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip()[:100]


def fetch_transcript(video_id: str):
    """Return list of transcript segments or raise."""
    api = YouTubeTranscriptApi()
    return api.fetch(video_id)


def save_csv(video_id: str, title: str, segments: list, out_dir: str):
    label = safe_filename(title) if title else video_id
    path = os.path.join(out_dir, f"{label}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["video_id", "start", "duration", "text"])
        for seg in segments:
            writer.writerow([video_id, seg.start, seg.duration, seg.text])
    return path


def get_video_title(video_id: str) -> str:
    """Fetch video title via yt-dlp if available, otherwise return empty string."""
    try:
        import yt_dlp
        opts = {"quiet": True, "skip_download": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            return info.get("title", "")
    except Exception:
        return ""


def already_saved_files(out_dir: str) -> dict:
    """Return {video_id: filepath} for every transcript CSV already on disk."""
    mapping = {}
    for fname in os.listdir(out_dir):
        if not fname.endswith(".csv") or fname.startswith("_"):
            continue
        fpath = os.path.join(out_dir, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                f.readline()  # skip header
                first_data = f.readline()
            vid = first_data.split(",")[0].strip()
            if vid:
                mapping[vid] = fpath
        except Exception:
            pass
    return mapping


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(URLS_FILE, encoding="utf-8") as f:
        entries = json.load(f)

    done = already_saved_files(OUTPUT_DIR)

    results = []
    for entry in entries:
        url = entry["url"]
        video_id = extract_video_id(url)
        if not video_id:
            print(f"[SKIP] Could not parse video ID from: {url}")
            results.append({"url": url, "status": "bad_url", "file": ""})
            continue

        if video_id in done:
            print(f"[{video_id}] Already saved -> {done[video_id]}")
            results.append({"url": url, "status": "ok", "file": done[video_id]})
            continue

        print(f"[{video_id}] Fetching title...", end=" ", flush=True)
        title = get_video_title(video_id)
        print(title or "(no title)")

        print(f"[{video_id}] Fetching transcript...", end=" ", flush=True)
        try:
            segments = fetch_transcript(video_id)
            path = save_csv(video_id, title, segments, OUTPUT_DIR)
            print(f"saved -> {path}")
            results.append({"url": url, "status": "ok", "file": path})
            time.sleep(2)  # avoid YouTube rate limiting
        except TranscriptsDisabled:
            print("SKIPPED (transcripts disabled)")
            results.append({"url": url, "status": "disabled", "file": ""})
        except NoTranscriptFound:
            print("SKIPPED (no transcript found)")
            results.append({"url": url, "status": "not_found", "file": ""})
        except Exception as e:
            print(f"ERROR: {e}")
            results.append({"url": url, "status": f"error: {e}", "file": ""})

    summary_path = os.path.join(OUTPUT_DIR, "_summary.csv")
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "status", "file"])
        writer.writeheader()
        writer.writerows(results)

    ok = sum(1 for r in results if r["status"] == "ok")
    print(f"\nDone: {ok}/{len(results)} transcripts saved to '{OUTPUT_DIR}/'")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
