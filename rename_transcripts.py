"""
Rename transcript CSVs that are still named by video ID.
Fetches the proper title via yt-dlp and renames the file,
then updates _summary.csv accordingly.
"""
import csv
import os
import re
import sys
import time

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TRANSCRIPTS_DIR = "transcripts"
SUMMARY_FILE = os.path.join(TRANSCRIPTS_DIR, "_summary.csv")


def safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip()[:100]


def get_video_title(video_id: str) -> str:
    try:
        import yt_dlp
        opts = {"quiet": True, "skip_download": True, "no_warnings": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}", download=False
            )
            return info.get("title", "")
    except Exception:
        return ""


def main():
    with open(SUMMARY_FILE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    changed = False
    for row in rows:
        fpath = row.get("file", "")
        if not fpath:
            continue
        fname = os.path.basename(fpath)
        # CSV is ID-named if the stem (without .csv) matches a bare video ID pattern
        stem = fname[:-4]  # strip .csv
        if not re.fullmatch(r"[A-Za-z0-9_\-]{11}", stem):
            continue  # already has a proper title name

        video_id = stem
        print(f"[{video_id}] Fetching title...", end=" ", flush=True)
        title = get_video_title(video_id)
        time.sleep(1)

        if not title:
            print("(no title returned, skipping)")
            continue

        new_stem = safe_filename(title)
        new_fname = f"{new_stem}.csv"
        new_path = os.path.join(TRANSCRIPTS_DIR, new_fname)

        if os.path.exists(new_path):
            print(f"target already exists: {new_fname}")
            row["file"] = new_path
            changed = True
            continue

        old_path = os.path.join(TRANSCRIPTS_DIR, fname)
        os.rename(old_path, new_path)
        print(f"renamed -> {new_fname}")
        row["file"] = new_path
        changed = True

    if changed:
        with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["url", "status", "file"])
            writer.writeheader()
            writer.writerows(rows)
        print("\nSummary updated.")
    else:
        print("\nNothing to rename.")


if __name__ == "__main__":
    main()
