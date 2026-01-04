"""Fetch UN Comtrade international trade data.

Uses the UN Comtrade API to fetch bilateral trade flows between countries.
Free tier limits: 500 records per call (no auth), 100k records per call (with free token).

Note: Bulk download requires premium subscription. This connector uses the free API
to fetch trade data for major economies.

API docs: https://comtradedeveloper.un.org/
"""
import os
import time
from subsets_utils import get, save_raw_json, load_state, save_state

BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A"  # Commodities, Annual

# Major reporting economies to fetch (ISO3 codes)
# These are the largest trading nations
REPORTERS = [
    "USA",  # United States
    "CHN",  # China
    "DEU",  # Germany
    "JPN",  # Japan
    "GBR",  # United Kingdom
    "FRA",  # France
    "NLD",  # Netherlands
    "KOR",  # South Korea
    "ITA",  # Italy
    "CAN",  # Canada
    "MEX",  # Mexico
    "IND",  # India
    "BRA",  # Brazil
    "AUS",  # Australia
    "SGP",  # Singapore
]

# Recent years to fetch
YEARS = list(range(2015, 2025))


def fetch_trade_data(reporter: str, year: int) -> list[dict]:
    """Fetch trade data for a single reporter and year."""
    # Using HS commodity code level 2 (broad categories)
    # flowCode: M=imports, X=exports
    url = f"{BASE_URL}/{reporter}/{year}/all/TOTAL"

    params = {
        "includeDesc": "true",
    }

    # Add API key if available
    api_key = os.environ.get("COMTRADE_API_KEY")
    if api_key:
        params["subscription-key"] = api_key

    try:
        response = get(url, params=params, timeout=60)
    except Exception as e:
        print(f"    Error: {e}")
        return []

    if response.status_code == 429:
        print(f"    Rate limited, waiting...")
        time.sleep(60)
        return fetch_trade_data(reporter, year)

    if response.status_code != 200:
        print(f"    HTTP {response.status_code}")
        return []

    data = response.json()
    return data.get("data", [])


def run():
    """Fetch UN Comtrade trade data for major economies."""
    print("Fetching UN Comtrade trade data...")

    state = load_state("comtrade")
    completed = set(state.get("completed", []))

    # Build list of reporter-year combinations to fetch
    all_tasks = [(r, y) for r in REPORTERS for y in YEARS]
    pending = [(r, y) for r, y in all_tasks if f"{r}_{y}" not in completed]

    if not pending:
        print("  All trade data up to date")
        return

    print(f"  Fetching {len(pending)} reporter-year combinations...")

    all_records = []
    batch_size = 50  # Save every 50 requests

    for i, (reporter, year) in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] {reporter} {year}...")

        records = fetch_trade_data(reporter, year)

        if records:
            # Add metadata to each record
            for rec in records:
                rec["_reporter"] = reporter
                rec["_year"] = year
            all_records.extend(records)
            print(f"    -> {len(records)} records")
        else:
            print(f"    -> no data")

        completed.add(f"{reporter}_{year}")
        save_state("comtrade", {"completed": list(completed)})

        # Save periodically
        if len(all_records) >= batch_size * 500:
            batch_num = len(completed) // batch_size
            save_raw_json(all_records, f"trade_data_batch_{batch_num}")
            print(f"    Saved batch {batch_num} ({len(all_records):,} records)")
            all_records = []

        # Rate limit
        time.sleep(1)

    # Save remaining records
    if all_records:
        save_raw_json(all_records, "trade_data_final")
        print(f"  Saved final batch ({len(all_records):,} records)")

    print("  Done fetching trade data")
