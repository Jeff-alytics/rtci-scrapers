"""
Chicago Crime Scraper — Socrata Open Data API
Source: https://data.cityofchicago.org/resource/ijzp-q8t2.json
Uses IUCR code crosswalk to map Chicago PD crime codes to RTCI offenses.
Outputs chicago/data/latest.json in RTCI pipeline format.
Intended to overwrite Chicago data from the Illinois state (Optimum) scraper.
"""

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATE = "IL"
AGENCY = "Chicago"
AGENCY_TYPE = "City"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"
CROSSWALK_CSV = Path(__file__).parent / "iucr_crosswalk.csv"

API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"


def load_crosswalk():
    """Load IUCR -> RTCI offense mapping from crosswalk CSV.
    Returns {4-digit-iucr: offense_name}.
    API uses zero-padded 4-char IUCR codes (e.g., '0110' not '110').
    """
    iucr_map = {}
    with open(CROSSWALK_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            iucr = row["IUCR"].strip()
            offense = row["ACTIVE"].strip()
            if not offense:
                continue
            # Zero-pad to 4 chars to match API format
            padded = iucr.zfill(4)
            iucr_map[padded] = offense
    return iucr_map


def nine_month_window():
    """Return list of (year, month) tuples for the last 9 months,
    skipping the most recent month.
    """
    now = datetime.now().replace(day=1)
    months = []
    y, m = now.year, now.month - 2
    if m <= 0:
        y -= 1
        m += 12
    for _ in range(9):
        months.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return sorted(months)


def query_month(year, month, iucr_codes, retries=3):
    """Query Socrata API for one month's crime counts by IUCR code.
    Returns {iucr: count} dict.
    """
    # Build date range for the month
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"

    # Socrata SoQL: count by IUCR for our codes in this month
    codes_str = ", ".join(f"'{c}'" for c in iucr_codes)
    query = (
        f"SELECT iucr, count(*) as cnt "
        f"WHERE date >= '{start}' AND date < '{end}' "
        f"AND iucr in ({codes_str}) "
        f"GROUP BY iucr"
    )

    for attempt in range(retries):
        try:
            r = requests.get(API_URL, params={"$query": query}, timeout=60)
            if r.status_code == 200:
                return {row["iucr"]: int(row["cnt"]) for row in r.json()}
            elif r.status_code == 429:
                print(f"    rate limited, waiting...", file=sys.stderr)
                sleep(5)
            else:
                print(f"    HTTP {r.status_code}: {r.text[:200]}", file=sys.stderr)
                sleep(2)
        except Exception as e:
            print(f"    error: {e}", file=sys.stderr)
            sleep(2)
    return {}


def main():
    iucr_map = load_crosswalk()
    all_codes = sorted(iucr_map.keys())
    print(f"Loaded {len(iucr_map)} IUCR codes across {len(set(iucr_map.values()))} offenses")

    window = nine_month_window()
    print(f"9-month window: {window}\n")

    all_rows = []
    for y, m in window:
        month_name = datetime(y, m, 1).strftime("%b %Y")
        print(f"  {month_name}...", end=" ", flush=True)

        iucr_counts = query_month(y, m, all_codes)
        if not iucr_counts:
            print("no data")
            continue

        # Aggregate IUCR counts into RTCI offenses
        offense_totals = {}
        for iucr, count in iucr_counts.items():
            offense = iucr_map.get(iucr)
            if offense:
                offense_totals[offense] = offense_totals.get(offense, 0) + count

        for offense in ["Murder", "Rape", "Robbery", "Aggravated Assault",
                        "Burglary", "Theft", "Motor Vehicle Theft"]:
            all_rows.append({
                "agency": AGENCY,
                "state": STATE,
                "type": AGENCY_TYPE,
                "year": y,
                "month": m,
                "offense": offense,
                "count": offense_totals.get(offense, 0),
            })

        total = sum(offense_totals.values())
        print(f"{total} crimes ({len(offense_totals)} offenses)")
        sleep(0.5)

    if not all_rows:
        print("\nNo data collected.")
        return

    all_rows.sort(key=lambda r: (r["year"], r["month"], r["offense"]))

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f, indent=2)

    months = set((r["year"], r["month"]) for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(months)} months) to {OUT_JSON}")


if __name__ == "__main__":
    main()
