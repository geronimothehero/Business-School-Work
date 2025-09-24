import os
from datetime import datetime
from sec_edgar_downloader import Downloader

SEC_EMAIL = "nusretkok@aol.com"
SEC_COMPANY_NAME = "personal use"

def fetch_edgar_filings(ticker=None, cik=None, count=2):
    """
    Downloads latest 10-K and 10-Q filings for a given company.
    Returns metadata for normalization.
    """
    if not ticker and not cik:
        # Private company or missing identifiers
        return []

    dl = Downloader(SEC_COMPANY_NAME, SEC_EMAIL, os.getcwd())

    filings = []
    for form in ["10-K", "10-Q"]:
        try:
            dl.get(form, ticker or cik, limit=count)
            base_path = os.path.join(os.getcwd(), "sec-edgar-filings", ticker or str(cik), form)
            if os.path.exists(base_path):
                for folder in sorted(os.listdir(base_path), reverse=True)[:count]:
                    filing_path = os.path.join(base_path, folder, "full-submission.txt")
                    if os.path.exists(filing_path):
                        filings.append({
                            "form": form,
                            "filing_date": folder,
                            "file_path": filing_path,
                            "source_url": f"https://www.sec.gov/Archives/edgar/data/{cik or ticker}/{folder}/index.html",
                            "fetched": datetime.utcnow().isoformat()
                        })
        except Exception as e:
            print(f"[WARN] Failed to fetch {form} for {ticker or cik}: {e}")
    return filings


def normalize_financials(filings, is_public=True):
    """
    Normalize EDGAR filings into schema.
    """
    if not is_public:
        return {
            "public": False,
            "filings": [],
            "provenance": []
        }

    if not filings:
        return {
            "public": True,
            "filings": [],
            "provenance": []
        }

    provenance = [{
        "source_url": f["source_url"],
        "source_type": "edgar",
        "fetched": f["fetched"],
        "evidence_snippet": f"{f['form']} filing on {f['filing_date']}"
    } for f in filings]

    return {
        "public": True,
        "filings": filings,
        "provenance": provenance
    }
