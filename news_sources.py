# news_sources.py
import os
import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from extractor import extract_basic_metadata
from fetcher import fetch_url
from dateutil.parser import parse as parse_date

load_dotenv()

# -----------------------------
# Config
# -----------------------------
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
BING_KEY = os.getenv("BING_KEY")
SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
CACHE_DIR = Path("./news_cache")
CACHE_DIR.mkdir(exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def _sanitize_filename(name: str) -> str:
    keep = (" ", ".", "_")
    s = "".join(c if c.isalnum() or c in keep else "_" for c in name).strip()
    return s.replace(" ", "_")[:180]

def _load_cache(company_name, ttl_hours=24):
    fn = CACHE_DIR / f"{_sanitize_filename(company_name)}.json"
    if not fn.exists():
        return None
    mtime = datetime.utcfromtimestamp(fn.stat().st_mtime)
    if datetime.utcnow() - mtime > timedelta(hours=ttl_hours):
        return None
    try:
        return json.loads(fn.read_text(encoding="utf-8"))
    except Exception:
        return None

def _save_cache(company_name, payload):
    fn = CACHE_DIR / f"{_sanitize_filename(company_name)}.json"
    fn.write_text(json.dumps(payload, indent=2), encoding="utf-8")

# -----------------------------
# SerpApi fetcher
# -----------------------------
def fetch_news_serpapi(company_name, max_results=10, sleep_between=1.0):
    if not SERPAPI_KEY:
        print("[WARN] No SERPAPI_KEY set. Skipping SerpApi.")
        return []

    params = {
        "engine": "google_news",
        "q": f'"{company_name}"',
        "api_key": SERPAPI_KEY,
        "num": min(max_results, 10)
    }

    try:
        r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        candidates = data.get("news_results") or data.get("news") or data.get("organic_results") or []
        articles = []
        for item in candidates[:max_results]:
            articles.append({
                "title": item.get("title") or item.get("headline"),
                "url": item.get("link") or item.get("source"),
                "source": (item.get("source") or {}).get("name") if isinstance(item.get("source"), dict) else item.get("source"),
                "snippet": item.get("snippet") or item.get("summary") or item.get("description"),
                "published": item.get("date") or item.get("published_date") or item.get("time"),
                "raw": item
            })
        time.sleep(sleep_between)
        return articles
    except Exception as e:
        print(f"[WARN] SerpApi fetch failed for {company_name}: {e}")
        return []

# -----------------------------
# Bing News fetcher
# -----------------------------
def fetch_news_bing(company_name, max_results=10):
    """
    Fetch news using Bing News Search API (Azure).
    Returns list of article dicts similar to SerpApi output.
    """
    if not BING_KEY:
        print("[WARN] No BING_KEY set. Skipping Bing News.")
        return []

    endpoint = "https://api.bing.microsoft.com/v7.0/news/search"
    headers = {"Ocp-Apim-Subscription-Key": BING_KEY}
    params = {
        "q": f'"{company_name}"',  # exact phrase search
        "count": min(max_results, 50),  # max 50 per request
        "sortBy": "Date",
        "mkt": "en-US",
    }

    try:
        r = requests.get(endpoint, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        articles = []

        for item in data.get("value", [])[:max_results]:
            articles.append({
                "title": item.get("name"),
                "url": item.get("url"),
                "source": item.get("provider")[0]["name"] if item.get("provider") else None,
                "snippet": item.get("description"),
                "published": item.get("datePublished"),
                "raw": item
            })

        return articles
    except Exception as e:
        print(f"[WARN] Bing fetch failed for {company_name}: {e}")
        return []

# -----------------------------
# GDELT News fetcher
# -----------------------------
def fetch_news_gdelt(company_name, max_results=10):
    """
    Fetch news mentions from GDELT 2.0.
    Returns list of article dicts similar to SerpApi output.
    """
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": f'"{company_name}"',  # exact phrase search
        "mode": "artlist",             # return article list
        "maxrecords": max_results,
        "format": "json"
    }

    try:
        r = requests.get(endpoint, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        articles = []

        for item in data.get("articles", [])[:max_results]:
            articles.append({
                "title": item.get("title"),
                "url": item.get("url"),
                "source": item.get("domain"),
                "snippet": item.get("seendescription") or item.get("title"),
                "published": item.get("seendate"),  # GDELT uses YYYYMMDDHHMMSS
                "raw": item
            })

        # Convert GDELT date format to ISO string if available
        for a in articles:
            if a["published"]:
                try:
                    dt = datetime.strptime(a["published"], "%Y%m%d%H%M%S")
                    a["published"] = dt.isoformat()
                except Exception:
                    pass  # keep original if parsing fails

        return articles
    except Exception as e:
        print(f"[WARN] GDELT fetch failed for {company_name}: {e}")
        return []
# -----------------------------
# HTML fallback extraction
# -----------------------------
def enrich_with_html_fallback(articles, max_fallback=3):
    enriched = []
    count = 0
    for a in articles:
        enriched.append(a)
        if (not a.get("headline") or not a.get("summary")) and a.get("source_url") and count < max_fallback:
            fetched = fetch_url(a["source_url"])
            if fetched.get("raw_text"):
                meta = extract_basic_metadata(fetched["raw_text"], a["source_url"])
                a["headline"] = a.get("headline") or meta.get("title")
                a["summary"] = a.get("summary") or meta.get("description") or meta.get("evidence_snippet")
                count += 1
    return enriched

# -----------------------------
# Orchestrator
# -----------------------------

def normalize_date(dt_str):
    if not dt_str:
        return None
    try:
        return parse_date(dt_str).isoformat()
    except Exception:
        return dt_str  # fallback to original if parsing fails
    
def fetch_company_news_multi(company_name, max_results=10, use_cache=True):
    if use_cache:
        cached = _load_cache(company_name)
        if cached:
            return cached

    # 1) SerpApi
    articles = fetch_news_serpapi(company_name, max_results=max_results)

    # 2) fallback Bing
    if len(articles) < max_results:
        articles += fetch_news_bing(company_name, max_results=max_results - len(articles))

    # 3) fallback GDELT
    if len(articles) < max_results:
        articles += fetch_news_gdelt(company_name, max_results=max_results - len(articles))

    # 4) HTML fallback for missing data
    articles = enrich_with_html_fallback(articles, max_fallback=3)

    # normalize
    norm = []
    for a in articles[:max_results]:
        norm.append({
            "headline": a.get("title") or a.get("headline"),
            "summary": a.get("snippet") or a.get("summary"),
            "source_url": a.get("url") or a.get("source_url"),
            "published": normalize_date(a.get("published")),
            "source_name": a.get("source"),
            "fetched": datetime.utcnow().isoformat(),
            "provenance": a.get("raw") or {},
        })

    if use_cache:
        _save_cache(company_name, norm)

    return norm

if __name__ == "__main__":
    print(fetch_company_news_multi("Genentech", max_results=5))
