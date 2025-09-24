# fetcher.py
import os
import requests
import hashlib
import time
from datetime import datetime
from pathlib import Path

ARCHIVE_DIR = Path("./archive")
ARCHIVE_DIR.mkdir(exist_ok=True)

def fetch_url(url, use_cache=True, max_retries=3, sleep=2):
    """
    Fetch a URL and archive it locally.
    Returns dict with {raw_text, archive_path, fetched_at}.
    """
    # Hash filename by URL
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    archive_path = ARCHIVE_DIR / f"{url_hash}.html"

    if use_cache and archive_path.exists():
        return {
            "raw_text": archive_path.read_text(encoding="utf-8", errors="ignore"),
            "archive_path": str(archive_path),
            "fetched_at": datetime.utcnow().isoformat(),
            "from_cache": True
        }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Retry logic
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=20, headers=headers)
            r.raise_for_status()
            text = r.text

            # Save raw
            archive_path.write_text(text, encoding="utf-8", errors="ignore")

            return {
                "raw_text": text,
                "archive_path": str(archive_path),
                "fetched_at": datetime.utcnow().isoformat(),
                "from_cache": False
            }
        except requests.HTTPError as e:
            print(f"[WARN] Fetch attempt {attempt+1} failed for {url}: {e}")
            time.sleep(sleep)
        except Exception as e:
            print(f"[WARN] Fetch attempt {attempt+1} error for {url}: {e}")
            time.sleep(sleep)

    return {
        "raw_text": None,
        "archive_path": None,
        "fetched_at": datetime.utcnow().isoformat(),
        "from_cache": False,
        "error": f"Failed after {max_retries} attempts"
    }
