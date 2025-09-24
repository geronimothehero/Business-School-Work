import pandas as pd
import yfinance as yf
import requests
from rapidfuzz import fuzz, process

# -----------------------------
# Helpers
# -----------------------------

def get_cik(ticker):
    """
    Get CIK from SEC's company tickers file.
    Returns 10-digit string or None.
    """
    if not ticker:
        return None
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {"User-Agent": "pharma-pipeline/0.1 contact@example.com"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        for entry in data.values():
            if entry.get("ticker", "").upper() == ticker.upper():
                return str(entry.get("cik_str", "")).zfill(10)
    except Exception as e:
        print(f"[WARN] Error fetching CIK for {ticker}: {e}")
    return None


def lookup_yfinance(company_name):
    """
    Resolve ticker, resolved name, and website using Yahoo Finance.
    If input is a name, first map it to a ticker via Yahoo search API.
    """
    try:
        # Step 1: Search Yahoo Finance
        search_url = f"https://query2.finance.yahoo.com/v1/finance/search?q={company_name}"
        headers = {"User-Agent": "pharma-pipeline/0.1 contact@example.com"}
        r = requests.get(search_url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        ticker_symbol = None
        if data.get("quotes"):
            ticker_symbol = data["quotes"][0].get("symbol")

        if not ticker_symbol:
            return {
                "resolved_name": company_name,
                "ticker": None,
                "website": None,
                "source": "search_fail"
            }

        # Step 2: Fetch full info
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info

        return {
            "resolved_name": info.get("longName", company_name),
            "ticker": info.get("symbol", ticker_symbol),
            "website": info.get("website"),
            "source": "yfinance"
        }

    except Exception as e:
        print(f"[WARN] yfinance lookup failed for {company_name}: {e}")
        return {
            "resolved_name": company_name,
            "ticker": None,
            "website": None,
            "source": "exception"
        }


def generate_name_variants(name):
    """
    Generate simple name variants (strip Inc, Ltd, Corp).
    """
    variants = {name}
    suffixes = ["Inc.", "Inc", "Ltd.", "Ltd", "Corp.", "Corp", "Corporation", "LLC", "PLC"]
    for suf in suffixes:
        if name.endswith(suf):
            variants.add(name.replace(suf, "").strip())
    return list(variants)


# -----------------------------
# Main Canonicalizer
# -----------------------------
def canonicalize(companies_csv, output_csv="canonical_companies.csv"):
    """
    Reads company names from CSV, returns canonicalized records with
    resolved_name, ticker, CIK, website, name_variants, provenance, status.
    """
    df = pd.read_csv(companies_csv)
    results = []

    for input_name in df["company_name"]:
        record = lookup_yfinance(input_name)
        cik = get_cik(record["ticker"]) if record["ticker"] else None

        # Decide status
        if record["ticker"] and cik:
            status = "resolved"
        elif record["ticker"] and not cik:
            status = "partial"   # ticker found but not in SEC
        elif not record["ticker"]:
            status = "private"   # no ticker → assume private
        else:
            status = "failed"

        result = {
            "input_name": input_name,
            "resolved_name": record["resolved_name"],
            "ticker": record["ticker"],
            "cik": cik,
            "website": record["website"],
            "name_variants": generate_name_variants(record["resolved_name"]),
            "provenance": record["source"],
            "status": status
        }
        results.append(result)

    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)
    return results


# -----------------------------
# Run standalone
# -----------------------------
if __name__ == "__main__":
    canonical_records = canonicalize("companies.csv")
    print("\n✅ Canonicalization finished. Sample output:")
    print(pd.DataFrame(canonical_records).head())
