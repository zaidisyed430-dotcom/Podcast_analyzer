"""
Reformat Prianka_chopra.csv into the standard schema and rename it.
Since the source has no timestamps, each speaker turn becomes its own row.
"""
import csv
import os
import re

SRC = os.path.join("transcripts", "Prianka_chopra.csv")
VIDEO_ID = "2y0wMI143bg"
TITLE = "Priyanka Chopra Jonas Talks Returning to Indian Cinema and Playing a Pirate in The Bluff"
DST = os.path.join("transcripts", f"{TITLE}.csv")
SUMMARY = os.path.join("transcripts", "_summary.csv")

# Read the raw content
with open(SRC, encoding="utf-8") as f:
    raw = f.read()

# Split on speaker turns marked by " -" (dash pattern in this transcript)
# Each segment like "-Some text." becomes a row
segments = re.split(r"\s+-(?=[A-Z\[])", raw)
segments = [s.strip().lstrip("-").strip() for s in segments if s.strip()]

# Write properly formatted CSV
with open(DST, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["video_id", "start", "duration", "text"])
    for i, seg in enumerate(segments):
        text = re.sub(r"\s+", " ", seg).strip()
        if text:
            writer.writerow([VIDEO_ID, "", "", text])

print(f"Written {len(segments)} rows -> {DST}")

# Update summary
with open(SUMMARY, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

updated = False
for row in rows:
    if VIDEO_ID in row.get("url", ""):
        row["status"] = "ok"
        row["file"] = DST
        updated = True

if not updated:
    rows.append({"url": f"https://youtu.be/{VIDEO_ID}", "status": "ok", "file": DST})

with open(SUMMARY, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["url", "status", "file"])
    writer.writeheader()
    writer.writerows(rows)

# Remove old file
os.remove(SRC)
print(f"Removed {SRC}")
print("Summary updated.")
