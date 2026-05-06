"""
South Dakota Crime Scraper — Optimum Platform (NIBRS offense codes).
Source: sdcrime.nibrs.com
Queries per-crime NIBRS codes, one API call per agency per crime type.
Outputs southdakota/data/latest.json in RTCI pipeline format.
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATE = "SD"
AGENCY_LIST_URL = "https://sdcrime.nibrs.com/Report/GetReportByValues?ReportType=Agency"
DATA_URL = "https://sdcrime.nibrs.com/Report/GetCrimeTrends"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

SD_AGENCIES = [
    {"ori": "SD0490200", "name": "Sioux Falls", "type": "City"},
    {"ori": "SD0510100", "name": "Rapid City", "type": "City"},
]

# NIBRS offense codes → RTCI offense names
CRIMES = {
    "Murder": "09a",
    "Rape": "11a,11b,11c",
    "Robbery": "120",
    "Aggravated Assault": "13a",
    "Burglary": "220",
    "Theft": "23a,23b,23c,23d,23e,23f,23g,23h",
    "Motor Vehicle Theft": "240",
}

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def get_with_retry(url, params=None, retries=3, delay=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt + 1}: {e}")
                time.sleep(delay)
            else:
                raise


def match_agencies(platform_list):
    ori_to_value = {}
    for a in platform_list:
        desc = a.get("Description", "")
        parts = desc.split(" - ", 1)
        if len(parts) == 2:
            ori_to_value[parts[0].strip().upper()] = a["Value"]

    matched = []
    for ag in SD_AGENCIES:
        val = ori_to_value.get(ag["ori"].upper())
        if not val:
            print(f"  WARNING: no platform match for {ag['ori']} ({ag['name']})")
            continue
        matched.append({**ag, "value": val})
    return matched


def main():
    # Date window: 12 months back
    now = datetime.now().replace(day=1)
    end = now - timedelta(days=1)
    end = end.replace(day=1)
    start = end.replace(year=end.year - 1)
    start_str = start.strftime("%m%Y")
    end_str = end.strftime("%m%Y")
    print(f"Date window: {start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}")
    print(f"{len(SD_AGENCIES)} SD agencies\n")

    print("Fetching platform agency list...")
    r = get_with_retry(AGENCY_LIST_URL)
    platform_list = r.json()
    print(f"  Platform has {len(platform_list)} agencies")

    matched = match_agencies(platform_list)
    print(f"  Matched {len(matched)} agencies\n")

    all_rows = []

    for ag in matched:
        print(f"Fetching {ag['name']}...", flush=True)

        for crime_name, offense_codes in CRIMES.items():
            params = {
                "ReportType": "Agency",
                "DrillDownReportIDs": -1,
                "IsGroupAOffense": True,
                "startDate": start_str,
                "endDate": end_str,
                "ReportIDs": ag["value"],
                "OffenseIDs": offense_codes,
            }
            try:
                r = get_with_retry(DATA_URL, params=params)
            except Exception as e:
                print(f"  ERROR {crime_name}: {e}")
                continue

            j = r.json()
            if j.get("Result") == "ERROR" or "periodlist" not in j or "crimeList" not in j:
                print(f"  Unexpected response for {crime_name}")
                continue

            dates = []
            for d in j["periodlist"]:
                parts = d.split("/")
                yr = int(parts[0])
                mo = MONTH_ABBR.get(parts[1], 0)
                if mo:
                    dates.append((yr, mo))

            crime_list = j["crimeList"]
            counts = crime_list[0]["data"] if crime_list and crime_list[0].get("data") else [None] * len(dates)

            for (yr, mo), val in zip(dates, counts):
                if val is None:
                    continue
                all_rows.append({
                    "agency": ag["name"],
                    "state": STATE,
                    "type": ag["type"],
                    "year": yr,
                    "month": mo,
                    "offense": crime_name,
                    "count": val,
                })

            time.sleep(0.3)

        print(f"  {sum(1 for r in all_rows if r['agency'] == ag['name'])} records")

    if not all_rows:
        print("No data collected.")
        return

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)
    agencies = set(r["agency"] for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(agencies)} agencies) to {OUT_JSON}")


if __name__ == "__main__":
    main()
