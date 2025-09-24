import pandas as pd
import json
import os

from clinical_trials import fetch_trials, normalize_trials
from edgar_financials import fetch_edgar_filings, normalize_financials
from news_sources import fetch_company_news_multi
from fetcher import fetch_url
from extractor import extract_basic_metadata
from news_sources import fetch_company_news_multi, normalize_date


# -----------------------------
# Runner
# -----------------------------
def build_profiles(canonical_csv, output_folder="profiles"):
    """
    Reads canonicalized companies from CSV,
    builds profiles with clinical + financial + news data,
    and saves per-company JSON files.
    """
    df = pd.read_csv(canonical_csv)
    os.makedirs(output_folder, exist_ok=True)
    results = []

    for _, row in df.iterrows():
        input_name = row.get("input_name")
        resolved_name = row.get("resolved_name")
        ticker = row.get("ticker")
        cik = row.get("cik")
        website = row.get("website")
        variants_str = row.get("name_variants")

        # Parse the string representation of the list into an actual list
        try:
            import ast
            variants = ast.literal_eval(variants_str) if pd.notna(variants_str) else [resolved_name]
        except (ValueError, SyntaxError):
            variants = [resolved_name]

        print(f"\n[INFO] Processing {resolved_name} ({ticker})...")

        # --- Clinical trials ---
        try:
            raw_trials = []
            for variant in variants:
                try:
                    trials_for_variant = fetch_trials(variant)
                    if trials_for_variant.get("studies"):
                        raw_trials.extend(trials_for_variant["studies"])
                except Exception as inner_e:
                    print(f"[WARN] Trials fetch failed for variant {variant}: {inner_e}")
            normalized_trials = normalize_trials({"studies": raw_trials}, resolved_name)
        except Exception as e:
            print(f"[WARN] Trials fetch failed for {resolved_name}: {e}")
            normalized_trials = {"pipeline": {"candidates": [], "counts_by_phase": {}}}

        # --- Financials ---
        try:
            if pd.notna(ticker) or pd.notna(cik):
                filings = fetch_edgar_filings(
                    ticker=ticker if pd.notna(ticker) else None,
                    cik=cik if pd.notna(cik) else None
                )
                normalized_financials = normalize_financials(filings, is_public=True)
            else:
                normalized_financials = normalize_financials([], is_public=False)
        except Exception as e:
            print(f"[WARN] EDGAR fetch failed for {resolved_name}: {e}")
            normalized_financials = {"public": bool(pd.notna(ticker) or pd.notna(cik)),
                                     "filings": [],
                                     "provenance": []}

        # --- News & Discovery ---
        news_items = []
        try:
            news_items = fetch_company_news_multi(resolved_name, max_results=8, use_cache=True)
            
            for item in news_items:
                if not item.get("headline") or not item.get("summary"):
                    url = item.get("source_url")
                    if url:
                        fetched = fetch_url(url)
                        if fetched.get("raw_text"):
                            extracted = extract_basic_metadata(fetched["raw_text"], url)
                            # Update news item with any missing info
                            item["headline"] = item.get("headline") or extracted.get("title")
                            item["summary"] = item.get("summary") or extracted.get("description")
                            item["fetched"] = extracted["fetched_at"]
                            item["extraction_confidence"] = extracted["extraction_confidence"]
                            item["evidence_snippet"] = extracted["evidence_snippet"]

                # ✅ Normalize published date (even if metadata fallback didn’t run)
                item["published"] = normalize_date(item.get("published"))
                
        except Exception as e:
            print(f"[WARN] News fetch failed for {resolved_name}: {e}")

        # --- Merge profile ---
        profile = {
            "input_name": input_name,
            "canonical": {
                "resolved_name": resolved_name,
                "ticker": ticker,
                "cik": cik,
                "website": website,
                "name_variants": variants,
            },
            "pipeline": normalized_trials["pipeline"],
            "financial": normalized_financials,
            "news": news_items,   # <-- new section
        }

        # Save to JSON
        safe_name = resolved_name.replace(" ", "_").replace("/", "_")
        out_path = os.path.join(output_folder, f"{safe_name}_profile.json")
        with open(out_path, "w") as f:
            json.dump(profile, f, indent=2)

        print(f"[INFO] Saved profile → {out_path}")
        results.append(profile)

    return results


if __name__ == "__main__":
    profiles = build_profiles("canonical_companies.csv")
    print("\n✅ Finished building profiles for all companies.")
