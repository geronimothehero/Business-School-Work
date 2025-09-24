import requests
from datetime import datetime

CLINICAL_TRIALS_API = "https://clinicaltrials.gov/api/v2/studies"


# -----------------------------
# Helpers
# -----------------------------
def _normalize_phase(raw_phase: str) -> str:
    """
    Map messy phase labels into canonical categories.
    """
    if not raw_phase:
        return "unknown"
    phase = raw_phase.lower()
    if "1/2" in phase:
        return "phase1_2"
    if "2/3" in phase:
        return "phase2_3"
    if "1" in phase:
        return "phase1"
    if "2" in phase:
        return "phase2"
    if "3" in phase:
        return "phase3"
    if "4" in phase:
        return "phase4"
    if "not applicable" in phase:
        return "not_applicable"
    return "unknown"


def _build_snippet(protocol: dict) -> str:
    """
    Build a compact evidence snippet from available fields.
    """
    ident = protocol.get("identificationModule", {})
    conditions = protocol.get("conditionsModule", {}).get("conditions", [])
    interventions = protocol.get("armsInterventionsModule", {}).get("interventions", [])

    parts = []
    if ident.get("briefTitle"):
        parts.append(ident["briefTitle"])
    if conditions:
        parts.append(f"Condition: {conditions[0]}")
    if interventions:
        parts.append(f"Intervention: {interventions[0].get('name')}")
    return " | ".join(parts)[:150]  # cap length


# -----------------------------
# Main functions
# -----------------------------
def fetch_trials(company_name, max_records=5):
    """
    Query ClinicalTrials.gov for trials mentioning the company.
    Always returns a dict with key 'studies'.
    """
    params = {
        "query.term": company_name,
        "pageSize": max_records
    }
    r = requests.get(CLINICAL_TRIALS_API, params=params)
    r.raise_for_status()
    data = r.json()

    # Normalize output
    if isinstance(data, dict) and "studies" in data:
        return data
    elif isinstance(data, list):
        return {"studies": data}
    else:
        return {"studies": []}


from datetime import datetime

def normalize_trials(raw_results, company_name):
    """
    Normalize ClinicalTrials.gov API JSON into a consistent schema.
    Works with either:
      - a single raw_json dict
      - a list of (variant, raw_json) tuples
    Deduplicates by NCT ID.
    """
    seen = set()
    candidates = []

    # Case 1: single JSON dict
    if isinstance(raw_results, dict):
        raw_results = [(company_name, raw_results)]

    # Case 2: list of tuples (variant, raw_json)
    for item in raw_results:
        if isinstance(item, tuple) and len(item) == 2:
            variant, raw_json = item
        else:
            # fallback if it's not a tuple
            variant, raw_json = company_name, item

        studies = raw_json.get("studies", [])
        for s in studies:
            if not isinstance(s, dict):
                continue  # skip bad shapes

            protocol = s.get("protocolSection", {})
            ident = protocol.get("identificationModule", {})
            status = protocol.get("statusModule", {})

            nct = ident.get("nctId")
            if not nct or nct in seen:
                continue
            seen.add(nct)

            phase_raw = protocol.get("designModule", {}).get("phases", ["Unknown"])[0]
            phase = _normalize_phase(phase_raw)

            trial = {
                "nct": nct,
                "status": status.get("overallStatus", "Unknown"),
                "phase": phase,
                "condition": (protocol.get("conditionsModule", {})
                              .get("conditions", [None])[0]),
                "intervention": (protocol.get("armsInterventionsModule", {})
                                 .get("interventions", [{}])[0].get("name")),
                "source_url": f"https://clinicaltrials.gov/study/{nct}",
                "fetched": datetime.utcnow().isoformat(),
                "evidence_snippet": _build_snippet(protocol),
                "matched_variant": variant,
            }
            candidates.append(trial)

    # Count trials by phase
    counts = {}
    for t in candidates:
        counts[t["phase"]] = counts.get(t["phase"], 0) + 1

    return {
        "company_name": company_name,
        "pipeline": {
            "candidates": candidates,
            "counts_by_phase": counts
        }
    }


# -----------------------------
# Run standalone
# -----------------------------
if __name__ == "__main__":
    # Example: use some variants for Pfizer
    variants = ["Pfizer", "Pfizer Inc.", "Pfizer Pharmaceuticals"]
    raw = fetch_trials(variants, max_records=10)
    norm = normalize_trials(raw, "Pfizer")
    from pprint import pprint
    pprint(norm)
