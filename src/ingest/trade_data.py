"""Fetch UN Comtrade international trade data.

Uses the UN Comtrade API to fetch bilateral trade flows between countries.
Two access tiers:
- Without API key: Preview endpoint, 500 records per call (unlimited calls/day)
- With API key: Full endpoint, 100k records per call (500 calls/day)

Data availability: Annual trade data from 1962 to present for ~200 countries.
The API uses UN numeric country codes.

API docs: https://comtradedeveloper.un.org/
Reference data: https://comtradeapi.un.org/files/v1/app/reference/Reporters.json
"""
import os
import time
from subsets_utils import get, save_raw_json, load_state, save_state

# API endpoints - C=Commodities, A=Annual, HS=Harmonized System classification
BASE_URL_PREVIEW = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"  # No key required
BASE_URL_FULL = "https://comtradeapi.un.org/data/v1/get/C/A/HS"  # Requires key
REPORTERS_URL = "https://comtradeapi.un.org/files/v1/app/reference/Reporters.json"

# Years available in UN Comtrade for HS classification
# HS (Harmonized System) data starts from ~1991, earlier data uses SITC
# We fetch 1990-present to capture everything available
YEAR_START = 1990
YEAR_END = 2024


def fetch_reporters() -> list[dict]:
    """Fetch the list of all reporter countries from UN Comtrade."""
    print("  Fetching reporter list...")
    response = get(REPORTERS_URL, timeout=60)
    data = response.json()

    reporters = []
    for r in data.get("results", []):
        # Skip expired/historical entities
        if r.get("entryExpiredDate"):
            continue
        # Skip group entities (like EU, ASEAN)
        if r.get("isGroup"):
            continue
        reporters.append({
            "code": r["reporterCode"],
            "name": r["reporterDesc"],
            "iso3": r.get("reporterCodeIsoAlpha3", ""),
        })

    print(f"    Found {len(reporters)} active reporters")
    return reporters


def fetch_trade_data(reporter_code: int, year: int, retry_count: int = 0) -> list[dict]:
    """Fetch trade data for a single reporter and year.

    Uses UN numeric reporter code. Fetches all partner countries for this
    reporter-year combination with TOTAL commodity aggregation (both imports
    and exports).

    Returns bilateral trade flows: each record is reporter -> partner with
    trade value, flow direction, and metadata.
    """
    api_key = os.environ.get("COMTRADE_API_KEY")

    # Use full endpoint with API key, otherwise preview endpoint
    if api_key:
        url = BASE_URL_FULL
    else:
        url = BASE_URL_PREVIEW

    params = {
        "reporterCode": str(reporter_code),
        "period": str(year),
        "cmdCode": "TOTAL",  # All commodities aggregated
        "includeDesc": "true",
    }

    if api_key:
        params["subscription-key"] = api_key

    response = get(url, params=params, timeout=120)

    if response.status_code == 429:
        wait_time = min(60 * (2 ** retry_count), 300)  # Exponential backoff, max 5 min
        print(f"    Rate limited, waiting {wait_time}s...")
        time.sleep(wait_time)
        return fetch_trade_data(reporter_code, year, retry_count + 1)

    if response.status_code == 404:
        # No data for this reporter/year combination
        return []

    if response.status_code != 200:
        print(f"    HTTP {response.status_code}: {response.text[:200]}")
        if retry_count < 3:
            time.sleep(10)
            return fetch_trade_data(reporter_code, year, retry_count + 1)
        return []

    data = response.json()
    return data.get("data", [])


def run():
    """Fetch UN Comtrade trade data for all reporters and years.

    Fetches annual trade data (HS classification, 1990-present) for all active
    reporter countries. Data is saved per reporter for memory management and
    incremental updates.

    Rate limiting: ~6 requests/minute to stay within free tier limits.
    Expected runtime: ~219 reporters × 35 years × 10s = ~21 hours for full crawl.
    """
    print("Fetching UN Comtrade trade data...")

    # Get list of all active reporters
    reporters = fetch_reporters()
    save_raw_json(reporters, "reporters")

    state = load_state("comtrade")
    completed = set(state.get("completed", []))

    # Build list of all years
    years = list(range(YEAR_START, YEAR_END + 1))

    # Build list of reporter-year combinations to fetch
    all_tasks = [(r["code"], r["name"], y) for r in reporters for y in years]
    pending = [(code, name, y) for code, name, y in all_tasks if f"{code}_{y}" not in completed]

    total_tasks = len(all_tasks)
    completed_count = total_tasks - len(pending)

    if not pending:
        print("  All trade data up to date")
        return

    print(f"  {completed_count:,}/{total_tasks:,} already completed")
    print(f"  {len(pending):,} reporter-year combinations remaining...")
    print(f"  Estimated time: ~{len(pending) * 10 / 60:.0f} minutes at 6 req/min")

    # Process by reporter to save incrementally
    current_reporter = None
    reporter_records = []

    for i, (reporter_code, reporter_name, year) in enumerate(pending, 1):
        # Save previous reporter's data when switching to new reporter
        if current_reporter is not None and reporter_code != current_reporter:
            if reporter_records:
                save_raw_json(reporter_records, f"trade_{current_reporter}")
                print(f"    Saved {len(reporter_records):,} records for reporter {current_reporter}")
            reporter_records = []

        current_reporter = reporter_code

        print(f"  [{i}/{len(pending)}] {reporter_name} ({reporter_code}) {year}...")

        records = fetch_trade_data(reporter_code, year)

        if records:
            reporter_records.extend(records)
            print(f"    -> {len(records)} records")
        else:
            print(f"    -> no data")

        completed.add(f"{reporter_code}_{year}")
        save_state("comtrade", {"completed": list(completed)})

        # Rate limit: ~6 requests per minute (10s between requests)
        # Free tier is ~10 req/min, but we're conservative to avoid 429s
        time.sleep(10)

    # Save final reporter's data
    if reporter_records:
        save_raw_json(reporter_records, f"trade_{current_reporter}")
        print(f"    Saved {len(reporter_records):,} records for reporter {current_reporter}")

    print("  Done fetching trade data")
