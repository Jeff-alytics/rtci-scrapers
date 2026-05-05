"""
Shared Optimum-platform NIBRS scraper.
Used by Idaho, Illinois, Rhode Island (and any future NIBRS-format states).
Each state provides: BASE_URL, STATE, AGENCIES list.
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


_session = requests.Session()
_session.headers.update(HEADERS)


def get_with_retry(url, params=None, retries=3, delay=5):
    for attempt in range(retries):
        try:
            r = _session.get(url, params=params, timeout=60, allow_redirects=True)
            r.raise_for_status()
            # Some platforms redirect to HTML on first hit — retry if not JSON
            ct = r.headers.get("content-type", "")
            if "json" not in ct and "javascript" not in ct and attempt < retries - 1:
                print(f"  Got {ct}, retrying…")
                time.sleep(delay)
                continue
            return r
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt + 1}/{retries - 1}: {e}")
                time.sleep(delay)
            else:
                raise


def match_agencies(agency_list, platform_list):
    ori_to_value = {}
    for a in platform_list:
        desc = a.get("Description", "")
        parts = desc.split(" - ", 1)
        if len(parts) == 2:
            ori_to_value[parts[0].strip().upper()] = str(a["Value"])
    matched = []
    for ag in agency_list:
        val = ori_to_value.get(ag["ori"].upper())
        if not val:
            print(f"  WARNING: no platform match for {ag['ori']} ({ag['name']})")
            continue
        matched.append({"name": ag["name"], "type": ag["type"], "value": val})
    return matched


def fetch_agency_data(base_url, state, agency, start_str, end_str):
    """Fetch 7 NIBRS crime categories for one agency. Returns list of pipeline-format dicts."""
    all_rows = []
    for crime_name, offense_codes in CRIMES.items():
        params = {
            "ReportType": "Agency",
            "DrillDownReportIDs": -1,
            "IsGroupAOffense": True,
            "startDate": start_str,
            "endDate": end_str,
            "ReportIDs": agency["value"],
            "OffenseIDs": offense_codes,
        }
        try:
            r = get_with_retry(base_url + "/GetCrimeTrends", params=params)
        except Exception as e:
            print(f"  ERROR {agency['name']}/{crime_name}: {e}")
            return []

        j = r.json()
        if j.get("Result") == "ERROR" or "periodlist" not in j or "crimeList" not in j:
            print(f"  Unexpected response for {agency['name']}/{crime_name}")
            return []

        dates = []
        for d in j["periodlist"]:
            parts = d.split("/")
            yr, mo = int(parts[0]), MONTH_ABBR.get(parts[1], 0)
            if mo:
                dates.append((yr, mo))

        crime_list = j["crimeList"]
        counts = crime_list[0]["data"] if crime_list and crime_list[0].get("data") else [0] * len(dates)

        for (yr, mo), val in zip(dates, counts):
            if val is None:
                val = 0
            all_rows.append({
                "agency": agency["name"],
                "state": state,
                "type": agency["type"],
                "year": yr,
                "month": mo,
                "offense": crime_name,
                "count": val,
            })
    return all_rows


def run_scraper(base_url, state, agencies, out_json):
    """Main entry point for any Optimum NIBRS state scraper."""
    print(f"Loading {state} agencies…")
    print(f"  {len(agencies)} agencies")

    print("Fetching platform agency list…")
    list_url = base_url + "/GetReportByValues?ReportType=Agency"
    r = get_with_retry(list_url)
    platform_list = r.json()
    print(f"  Platform has {len(platform_list)} agencies")

    matched = match_agencies(agencies, platform_list)
    print(f"  Matched {len(matched)} agencies")

    now = datetime.now().replace(day=1)
    end = now - timedelta(days=1)
    end = end.replace(day=1)
    start = end.replace(year=end.year - 1)
    start_str = start.strftime("%m%Y")
    end_str = end.strftime("%m%Y")
    print(f"Date window: {start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}")

    all_rows = []
    for i, ag in enumerate(matched):
        print(f"  [{i+1}/{len(matched)}] {ag['name']}…")
        rows = fetch_agency_data(base_url, state, ag, start_str, end_str)
        all_rows.extend(rows)
        time.sleep(0.3)

    if not all_rows:
        print("No data collected.")
        return

    out_path = Path(out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_rows, f)

    agencies_found = set(r["agency"] for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(agencies_found)} agencies) to {out_path}")
