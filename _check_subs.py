import yt_dlp
ids = ['u3gYBBO3Iro', '2y0wMI143bg', 'rPH5RUdaEMQ']
for vid in ids:
    opts = {"quiet": True, "skip_download": True, "no_warnings": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"https://www.youtube.com/watch?v={vid}", download=False)
        auto = list(info.get("automatic_captions", {}).keys())
        title = info.get("title", "")
        print(f"{vid} | title={title} | auto_langs={auto[:8]}")
