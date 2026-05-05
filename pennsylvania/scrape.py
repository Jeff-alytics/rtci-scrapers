"""
Pennsylvania Crime Scraper — Optimum Platform (SRS format)
Source: https://www.ucr.pa.gov/PAUCRSPublic/
Outputs pennsylvania/data/latest.json in RTCI pipeline format.
Runs daily via GitHub Actions.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

STATE = "PA"
AGENCY_LIST_URL = "https://www.ucr.pa.gov/PAUCRSPublic/SRSReport/GetReportByValues?ReportType=Agency"
DATA_URL = "https://www.ucr.pa.gov/PAUCRSPublic/SRSReport/GetCrimeTrends"

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

# Agencies CSV — shared across scrapers
AGENCIES_CSV = os.environ.get(
    "AGENCIES_CSV",
    str(Path(__file__).resolve().parent.parent.parent
        / "Open Source Data" / "Scraping Workspace" / "agencies - sample.csv"),
)

SRS_CRIMES = {
    "Murder and Nonnegligent Homicide": "Murder",
    "Rape": "Rape",
    "Robbery": "Robbery",
    "Aggravated Assault": "Aggravated Assault",
    "Burglary": "Burglary",
    "Larceny - Theft": "Theft",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# Hardcoded PA agency list for GitHub Actions (no CSV dependency)
# Matches agencies - sample.csv where state=PA and exclude=No
PA_AGENCIES = [
    {"ori": "PA0061400", "name": "Reading Police Department", "type": "City"},
    {"ori": "PA0090100", "name": "Bensalem Township Police Department", "type": "City"},
    {"ori": "PA0140300", "name": "State College Police Department", "type": "City"},
    {"ori": "PA0220200", "name": "Harrisburg Police Department", "type": "City"},
    {"ori": "PA0220400", "name": "Lower Paxton Township Police Department", "type": "City"},
    {"ori": "PA0231400", "name": "Haverford Police Department", "type": "City"},
    {"ori": "PA0233700", "name": "Upper Darby Township Police Department", "type": "City"},
    {"ori": "PA0250200", "name": "Erie Police Department", "type": "City"},
    {"ori": "PA0250300", "name": "Millcreek Township, Erie County", "type": "City"},
    {"ori": "PA0350400", "name": "Scranton Police Department", "type": "City"},
    {"ori": "PA0360500", "name": "Lancaster Police Department", "type": "City"},
    {"ori": "PA0390100", "name": "Allentown Police Department", "type": "City"},
    {"ori": "PA0460100", "name": "Abington Township Police Department", "type": "City"},
    {"ori": "PA0461400", "name": "Lower Merion Township Police Department", "type": "City"},
    {"ori": "PA0480300", "name": "Bethlehem Police Department", "type": "City"},
    {"ori": "PAPEP0000", "name": "Philadelphia Police Department", "type": "City"},
    {"ori": "PAPPD0000", "name": "Pittsburgh Bureau of Police", "type": "City"},
]


def get_with_retry(url, params=None, retries=3, delay=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt + 1}/{retries - 1}: {e}")
                time.sleep(delay)
            else:
                raise


def load_agencies_from_csv():
    """Try to load PA agencies from the shared CSV; fall back to hardcoded list."""
    if not os.path.exists(AGENCIES_CSV):
        print(f"  CSV not found ({AGENCIES_CSV}), using hardcoded list")
        return PA_AGENCIES
    agencies = []
    with open(AGENCIES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["state"] == STATE and row["exclude"].strip().lower() == "no":
                agencies.append({"name": row["name"], "ori": row["ori"], "type": row["type"]})
    if not agencies:
        print("  No PA agencies in CSV, using hardcoded list")
        return PA_AGENCIES
    return agencies


def match_agencies(sample_agencies, platform_list):
    """Match sample agencies to platform Value IDs by ORI prefix."""
    ori_to_value = {}
    for a in platform_list:
        desc = a.get("Description", "")
        parts = desc.split(" - ", 1)
        if len(parts) == 2:
            ori_to_value[parts[0].strip().upper()] = a["Value"]

    matched = []
    for ag in sample_agencies:
        ori = ag["ori"].upper()
        val = ori_to_value.get(ori)
        if not val:
            print(f"  WARNING: no platform match for {ori} ({ag['name']})")
            continue
        matched.append({"name": ag["name"], "type": ag["type"], "value": val})
    return matched


def fetch_agency_data(agency, start_date, end_date):
    """Fetch SRS crime data for one agency. Returns list of pipeline-format dicts."""
    params = {
        "ReportType": "Agency",
        "DrillDownReportIDs": -1,
        "StartDate": start_date,
        "EndDate": end_date,
        "ReportIDs": agency["value"],
        "OffenseIDs": "P1",
    }
    try:
        r = get_with_retry(DATA_URL, params=params)
    except Exception as e:
        print(f"  ERROR {agency['name']}: {e}")
        return []

    j = r.json()
    if j.get("Result") == "ERROR" or "periodlist" not in j or "crimeList" not in j:
        print(f"  Unexpected response for {agency['name']}")
        return []

    dates = []
    for d in j["periodlist"]:
        parts = d.split("/")
        yr = int(parts[0])
        mo = MONTH_ABBR.get(parts[1], 0)
        if mo:
            dates.append((yr, mo))

    rows = []
    for crime in j["crimeList"]:
        offense = SRS_CRIMES.get(crime["name"])
        if not offense:
            continue
        data = crime.get("data") or [None] * len(dates)
        for (yr, mo), val in zip(dates, data):
            if val is None:
                continue
            rows.append({
                "agency": agency["name"],
                "state": STATE,
                "type": agency["type"],
                "year": yr,
                "month": mo,
                "offense": offense,
                "count": val,
            })
    return rows


def main():
    print(f"Loading {STATE} agencies…")
    sample = load_agencies_from_csv()
    # Use short names for pipeline compatibility
    # Short names for pipeline compatibility (must match Feb CSV)
    NAME_OVERRIDES = {
        "PA0090100": "Bensalem",
        "PA0220400": "Lower Paxton",
        "PA0231400": "Haverford",
        "PA0233700": "Upper Darby",
        "PA0250300": "Millcreek",
        "PA0460100": "Abington",
        "PA0461400": "Lower Merion",
        "PAPPD0000": "Pittsburgh",
    }
    for ag in sample:
        if ag["ori"] in NAME_OVERRIDES:
            ag["name"] = NAME_OVERRIDES[ag["ori"]]
        else:
            ag["name"] = (ag["name"]
                          .replace(" Police Department", "")
                          .replace(" Sheriff's Office", "")
                          .replace(" County Sheriff's Office", "")
                          .strip())
    print(f"  {len(sample)} included agencies")

    print("Fetching platform agency list…")
    r = get_with_retry(AGENCY_LIST_URL)
    platform_list = r.json()
    print(f"  Platform has {len(platform_list)} agencies")

    matched = match_agencies(sample, platform_list)
    print(f"  Matched {len(matched)} agencies")

    # Date window: last 12 months
    now = datetime.now().replace(day=1)
    end = now - timedelta(days=1)
    end = end.replace(day=1)
    start = end.replace(year=end.year - 1)
    start_str = start.strftime("%m/%d/%Y")
    end_str = end.strftime("%m/%d/%Y")
    print(f"Date window: {start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}")

    all_rows = []
    for ag in matched:
        print(f"  Fetching {ag['name']}…")
        rows = fetch_agency_data(ag, start_str, end_str)
        all_rows.extend(rows)
        time.sleep(0.5)  # gentle rate limiting

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
