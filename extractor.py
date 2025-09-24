# extractor.py
from bs4 import BeautifulSoup
from datetime import datetime

def extract_basic_metadata(html_text, source_url):
    """
    Rule-based HTML extractor for title + description.
    Tries multiple tags for better coverage.
    """
    try:
        soup = BeautifulSoup(html_text, "html.parser")

        # Title: <title> fallback to <h1>
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        elif soup.h1 and soup.h1.string:
            title = soup.h1.string.strip()

        # Description: meta[name=description] or meta[property=og:description]
        description = None
        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag and "content" in desc_tag.attrs:
            description = desc_tag["content"].strip()
        else:
            og_desc = soup.find("meta", attrs={"property": "og:description"})
            if og_desc and "content" in og_desc.attrs:
                description = og_desc["content"].strip()

        # Evidence snippet
        snippet = description[:200] if description else (title[:200] if title else None)

        return {
            "title": title,
            "description": description,
            "source_url": source_url,
            "fetched_at": datetime.utcnow().isoformat(),
            "extraction_confidence": 0.6 if title or description else 0.2,
            "evidence_snippet": snippet
        }
    except Exception as e:
        return {
            "title": None,
            "description": None,
            "source_url": source_url,
            "fetched_at": datetime.utcnow().isoformat(),
            "extraction_confidence": 0.0,
            "error": str(e)
        }
