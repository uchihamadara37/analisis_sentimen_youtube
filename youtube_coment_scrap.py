# youtube_scraper_full.py
import csv
import json
import time
import yt_dlp
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_RECENT

# ─── KONFIGURASI ────────────────────────────────────────────
QUERIES = [
    "korban menjadi tersangka",
    "membuat roti",
    "belajar coding",
]
MAX_VIDEOS_PER_QUERY = 100     # jumlah video per query
MAX_COMMENTS_PER_VIDEO = 700   # jumlah komentar per video
OUTPUT_CSV = "youtube_dataset.csv"
OUTPUT_JSON = "youtube_dataset.json"
# ────────────────────────────────────────────────────────────


def search_videos(query, max_videos=100):
    """Cari video YouTube berdasarkan query, return list URL."""
    print(f"\n🔍 Mencari video untuk: '{query}'")

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,       # hanya ambil metadata, tidak download
        "skip_download": True,
        "ignoreerrors": True,
    }

    search_url = f"ytsearch{max_videos}:{query}"
    video_urls = []

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(search_url, download=False)
        if result and "entries" in result:
            for entry in result["entries"]:
                if entry and entry.get("id"):
                    url = f"https://www.youtube.com/watch?v={entry['id']}"
                    title = entry.get("title", "")
                    duration = entry.get("duration", 0)
                    # Tambahkan info video ke list
                    video_urls.append({
                        "url": url,
                        "title": title,
                        "duration": duration,
                        "query": query,
                    })
                    print(f"  [{len(video_urls)}] {title[:70]}")

    print(f"✅ Ditemukan {len(video_urls)} video untuk '{query}'")
    return video_urls


def scrape_comments(video_info, max_comments=200):
    """Scrape komentar dari satu video."""
    downloader = YoutubeCommentDownloader()
    comments = []

    try:
        generator = downloader.get_comments_from_url(
            video_info["url"],
            sort_by=SORT_BY_RECENT
        )
        for i, comment in enumerate(generator):
            if i >= max_comments:
                break

            comments.append({
                "query": video_info["query"],
                "video_url": video_info["url"],
                "video_title": video_info["title"],
                "comment_id": comment.get("cid", ""),
                "username": comment.get("author", ""),
                "text": comment.get("text", "").strip(),
                "timestamp": comment.get("time", ""),
                "likes": comment.get("votes", 0),
                "is_reply": comment.get("reply", False),
            })

    except Exception as e:
        print(f"  ⚠️  Gagal scrape {video_info['url']}: {e}")

    return comments


def main():
    all_comments = []
    all_videos = []

    # ── TAHAP 1: Kumpulkan semua URL video ──
    for query in QUERIES:
        videos = search_videos(query, MAX_VIDEOS_PER_QUERY)
        all_videos.extend(videos)

    print(f"\n📋 Total video ditemukan: {len(all_videos)}")
    print("=" * 60)

    # Simpan daftar video dulu (checkpoint)
    with open("video_list.json", "w", encoding="utf-8") as f:
        json.dump(all_videos, f, ensure_ascii=False, indent=2)
    print("💾 Daftar video disimpan ke video_list.json")

    # ── TAHAP 2: Scrape komentar tiap video ──
    for i, video in enumerate(all_videos, 1):
        print(f"\n[{i}/{len(all_videos)}] 🎬 {video['title'][:60]}")
        print(f"  🔗 {video['url']}")

        comments = scrape_comments(video, MAX_COMMENTS_PER_VIDEO)
        all_comments.extend(comments)

        print(f"  💬 {len(comments)} komentar | Total: {len(all_comments)}")

        # Simpan otomatis setiap 10 video (checkpoint)
        if i % 10 == 0:
            _save_csv(all_comments, OUTPUT_CSV)
            print(f"  💾 Auto-save: {len(all_comments)} komentar tersimpan")

        time.sleep(1)  # jeda sopan antar request

    # ── TAHAP 3: Simpan hasil akhir ──
    _save_csv(all_comments, OUTPUT_CSV)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_comments, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"🎉 SELESAI!")
    print(f"📊 Total video    : {len(all_videos)}")
    print(f"💬 Total komentar : {len(all_comments)}")
    print(f"📁 CSV  → {OUTPUT_CSV}")
    print(f"📁 JSON → {OUTPUT_JSON}")


def _save_csv(data, filename):
    if not data:
        return
    fieldnames = ["query", "video_url", "video_title", "comment_id",
                  "username", "text", "timestamp", "likes", "is_reply"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    main()