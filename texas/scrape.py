"""
Texas Crime Scraper — Optimum Platform (SRS format)
Source: https://txucr.nibrs.com/
Outputs texas/data/latest.json in RTCI pipeline format.
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

STATE = "TX"
AGENCY_LIST_URL = "https://txucr.nibrs.com/SRSReport/GetSRSReportByValues?ReportType=Agency"
DATA_URL = "https://txucr.nibrs.com/SRSReport/GetCrimeTrends"

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

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

# Hardcoded TX agency list — matches Agency Sourcing.xlsx where state=TX
TX_AGENCIES = [
    {"ori": "TX0140400", "name": "Killeen", "type": "City"},
    {"ori": "TX0140700", "name": "Temple", "type": "City"},
    {"ori": "TX0150000", "name": "Bexar", "type": "County"},
    {"ori": "TX0200000", "name": "Brazoria", "type": "County"},
    {"ori": "TX0201000", "name": "Pearland", "type": "City"},
    {"ori": "TX0210200", "name": "College Station", "type": "City"},
    {"ori": "TX0310000", "name": "Cameron", "type": "County"},
    {"ori": "TX0310100", "name": "Brownsville", "type": "City"},
    {"ori": "TX0310300", "name": "Harlingen", "type": "City"},
    {"ori": "TX0430000", "name": "Collin", "type": "County"},
    {"ori": "TX0430100", "name": "Allen", "type": "City"},
    {"ori": "TX0430400", "name": "Frisco", "type": "City"},
    {"ori": "TX0430500", "name": "Mckinney", "type": "City"},
    {"ori": "TX0430600", "name": "Plano", "type": "City"},
    {"ori": "TX0430800", "name": "Wylie", "type": "City"},
    {"ori": "TX0460000", "name": "Comal", "type": "County"},
    {"ori": "TX0460100", "name": "New Braunfels", "type": "City"},
    {"ori": "TX0570400", "name": "Carrollton", "type": "City"},
    {"ori": "TX0570800", "name": "Desoto", "type": "City"},
    {"ori": "TX0571100", "name": "Garland", "type": "City"},
    {"ori": "TX0571200", "name": "Grand Prairie", "type": "City"},
    {"ori": "TX0571500", "name": "Irving", "type": "City"},
    {"ori": "TX0571800", "name": "Mesquite", "type": "City"},
    {"ori": "TX0572000", "name": "Richardson", "type": "City"},
    {"ori": "TX0573300", "name": "Rowlett", "type": "City"},
    {"ori": "TX0610000", "name": "Denton", "type": "County"},
    {"ori": "TX0610200", "name": "Denton", "type": "City"},
    {"ori": "TX0610600", "name": "Lewisville", "type": "City"},
    {"ori": "TX0611200", "name": "Flower Mound", "type": "City"},
    {"ori": "TX0611300", "name": "Little Elm", "type": "City"},
    {"ori": "TX0680200", "name": "Odessa", "type": "City"},
    {"ori": "TX0710000", "name": "El Paso", "type": "County"},
    {"ori": "TX0710200", "name": "El Paso", "type": "City"},
    {"ori": "TX0790000", "name": "Fort Bend", "type": "County"},
    {"ori": "TX0790100", "name": "Missouri City", "type": "City"},
    {"ori": "TX0790500", "name": "Sugar Land", "type": "City"},
    {"ori": "TX0840400", "name": "Galveston", "type": "City"},
    {"ori": "TX0840800", "name": "League City", "type": "City"},
    {"ori": "TX0840900", "name": "Texas City", "type": "City"},
    {"ori": "TX0920500", "name": "Longview", "type": "City"},
    {"ori": "TX1010000", "name": "Harris", "type": "County"},
    {"ori": "TX1010100", "name": "Baytown", "type": "City"},
    {"ori": "TX1011500", "name": "Pasadena", "type": "City"},
    {"ori": "TX1050000", "name": "Hays", "type": "County"},
    {"ori": "TX1050100", "name": "San Marcos", "type": "City"},
    {"ori": "TX1050700", "name": "Kyle", "type": "City"},
    {"ori": "TX1080000", "name": "Hidalgo", "type": "County"},
    {"ori": "TX1080400", "name": "Edinburg", "type": "City"},
    {"ori": "TX1080800", "name": "McAllen", "type": "City"},
    {"ori": "TX1081000", "name": "Mission", "type": "City"},
    {"ori": "TX1081100", "name": "Pharr", "type": "City"},
    {"ori": "TX1230100", "name": "Beaumont", "type": "City"},
    {"ori": "TX1230700", "name": "Port Arthur", "type": "City"},
    {"ori": "TX1260200", "name": "Burleson", "type": "City"},
    {"ori": "TX1290000", "name": "Kaufman", "type": "County"},
    {"ori": "TX1520200", "name": "Lubbock", "type": "City"},
    {"ori": "TX1551200", "name": "Waco", "type": "City"},
    {"ori": "TX1650100", "name": "Midland", "type": "City"},
    {"ori": "TX1700100", "name": "Conroe", "type": "City"},
    {"ori": "TX1701700", "name": "Montgomery", "type": "County"},
    {"ori": "TX1780200", "name": "Corpus Christi", "type": "City"},
    {"ori": "TX1840000", "name": "Parker", "type": "County"},
    {"ori": "TX1880100", "name": "Amarillo", "type": "City"},
    {"ori": "TX1990100", "name": "Rockwall", "type": "City"},
    {"ori": "TX2120000", "name": "Smith", "type": "County"},
    {"ori": "TX2120400", "name": "Tyler", "type": "City"},
    {"ori": "TX2200100", "name": "Arlington", "type": "City"},
    {"ori": "TX2200900", "name": "Euless", "type": "City"},
    {"ori": "TX2201200", "name": "Fort Worth", "type": "City"},
    {"ori": "TX2201300", "name": "Grapevine", "type": "City"},
    {"ori": "TX2202000", "name": "Mansfield", "type": "City"},
    {"ori": "TX2202100", "name": "North Richland Hills", "type": "City"},
    {"ori": "TX2210100", "name": "Abilene", "type": "City"},
    {"ori": "TX2260100", "name": "San Angelo", "type": "City"},
    {"ori": "TX2270000", "name": "Travis", "type": "County"},
    {"ori": "TX2270100", "name": "Austin", "type": "City"},
    {"ori": "TX2270900", "name": "Pflugerville", "type": "City"},
    {"ori": "TX2350100", "name": "Victoria", "type": "City"},
    {"ori": "TX2400100", "name": "Laredo", "type": "City"},
    {"ori": "TX2430500", "name": "Wichita Falls", "type": "City"},
    {"ori": "TX2460000", "name": "Williamson", "type": "County"},
    {"ori": "TX2460200", "name": "Georgetown", "type": "City"},
    {"ori": "TX2460500", "name": "Round Rock", "type": "City"},
    {"ori": "TX2460900", "name": "Cedar Park", "type": "City"},
    {"ori": "TX2461700", "name": "Leander", "type": "City"},
    {"ori": "TXDPD0000", "name": "Dallas", "type": "City"},
    {"ori": "TXHPD0000", "name": "Houston", "type": "City"},
    {"ori": "TXSPD0000", "name": "San Antonio", "type": "City"},
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


def match_agencies(agency_list, platform_list):
    ori_to_value = {}
    for a in platform_list:
        desc = a.get("Description", "")
        parts = desc.split(" - ", 1)
        if len(parts) == 2:
            ori_to_value[parts[0].strip().upper()] = a["Value"]

    matched = []
    for ag in agency_list:
        ori = ag["ori"].upper()
        val = ori_to_value.get(ori)
        if not val:
            print(f"  WARNING: no platform match for {ori} ({ag['name']})")
            continue
        matched.append({"name": ag["name"], "type": ag["type"], "value": val})
    return matched


def fetch_agency_data(agency, start_date, end_date):
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
    print(f"  {len(TX_AGENCIES)} agencies")

    print("Fetching platform agency list…")
    r = get_with_retry(AGENCY_LIST_URL)
    platform_list = r.json()
    print(f"  Platform has {len(platform_list)} agencies")

    matched = match_agencies(TX_AGENCIES, platform_list)
    print(f"  Matched {len(matched)} agencies")

    now = datetime.now().replace(day=1)
    end = now - timedelta(days=1)
    end = end.replace(day=1)
    start = end.replace(year=end.year - 1)
    start_str = start.strftime("%m/%d/%Y")
    end_str = end.strftime("%m/%d/%Y")
    print(f"Date window: {start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}")

    all_rows = []
    for i, ag in enumerate(matched):
        print(f"  [{i+1}/{len(matched)}] {ag['name']}…")
        rows = fetch_agency_data(ag, start_str, end_str)
        all_rows.extend(rows)
        time.sleep(0.3)

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
