"""
CDE Return-A Pipeline Processor
Downloads RETA master files for 2025+2026 from FBI CDE,
parses fixed-width records, filters to CDE-sourced agencies,
and outputs pipeline-ready CSV with the last 9 months of data.

Output format matches the pipeline's CDE CSV:
  ori, Agency Name, Murder, Rape, Robbery, Aggravated Assault,
  Burglary, Theft, Motor Vehicle Theft, Year, Month, State, Type
"""

import csv
import io
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

OUT_CSV = Path(__file__).parent / "data" / "cde_reta_latest.csv"
AGENCIES_CSV = Path(__file__).parent / "cde_agencies.csv"

CDE_SIGNED_URL = "https://cde.ucr.cjis.gov/LATEST/s3/signedurl"

STATE_CODES = {
    "50": "AK", "01": "AL", "03": "AR", "54": "AS", "02": "AZ",
    "04": "CA", "05": "CO", "06": "CT", "52": "CZ", "08": "DC",
    "07": "DE", "09": "FL", "10": "GA", "55": "GU", "51": "HI",
    "14": "IA", "11": "ID", "12": "IL", "13": "IN", "15": "KS",
    "16": "KY", "17": "LA", "20": "MA", "19": "MD", "18": "ME",
    "21": "MI", "22": "MN", "24": "MO", "23": "MS", "25": "MT",
    "32": "NC", "33": "ND", "26": "NE", "28": "NH", "29": "NJ",
    "30": "NM", "27": "NV", "31": "NY", "34": "OH", "35": "OK",
    "36": "OR", "37": "PA", "53": "PR", "38": "RI", "39": "SC",
    "40": "SD", "41": "TN", "42": "TX", "43": "UT", "62": "VI",
    "45": "VA", "44": "VT", "46": "WA", "48": "WI", "47": "WV",
    "49": "WY",
}

# EBCDIC overpunch for negative numbers
NEG_MAP = {"}": 0, "J": -1, "K": -2, "L": -3, "M": -4,
           "N": -5, "O": -6, "P": -7, "Q": -8, "R": -9}

# Offense field indices within each card's 28 fields (5 chars each)
# We only need the "Actual Offenses" card (Card 1)
# NOTE: Index 10 (Assault Total) includes BOTH aggravated and simple assault.
# For aggravated assault, sum indices 11-14 (gun + knife + other + hands/feet).
OFFENSE_INDICES = {
    "Murder": 0,
    "Rape": 2,          # Rape Total
    "Robbery": 5,       # Robbery Total
    "Burglary": 16,     # Burglary Total
    "Theft": 20,        # Larceny Total
    "Motor Vehicle Theft": 21,  # MV Theft Total
}
# Aggravated assault subcategory indices (sum these for true agg assault)
AGG_ASSAULT_INDICES = [11, 12, 13, 14]  # Gun, Knife, Other Weapon, Hands/Feet

PIPELINE_HEADERS = [
    "ori", "Agency Name", "Murder", "Rape", "Robbery",
    "Aggravated Assault", "Burglary", "Theft", "Motor Vehicle Theft",
    "Year", "Month", "State", "Type",
]


def parse_reta_num(s):
    """Parse a RETA numeric field, handling EBCDIC overpunch negatives."""
    s = s.strip()
    if not s:
        return 0
    last = s[-1]
    if last in NEG_MAP:
        prefix = int(s[:-1] or "0")
        return -(prefix * 10 + abs(NEG_MAP[last]))
    return int(s)


def resolve_year(yy):
    """Convert 2-digit year to 4-digit."""
    y = int(yy)
    return 2000 + y if y < 50 else 1900 + y


def nine_month_window():
    """Return set of (year, month) tuples for the last 9 months,
    skipping the most recent month (agencies often haven't reported).
    """
    now = datetime.now().replace(day=1)
    months = set()
    y, m = now.year, now.month - 2
    if m <= 0:
        y -= 1
        m += 12
    for _ in range(9):
        months.add((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return months


def load_cde_agencies():
    """Load CDE-sourced agencies from cde_agencies.csv.
    Returns {ori: {name, state, type}}.
    Also creates alternate lookups for ORIs ending in X (mapped to 0)
    since some agencies use X-suffix ORIs that correspond to standard
    0-suffix ORIs in the RETA master file.
    """
    agencies = {}
    with open(AGENCIES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ori = row["ori"]
            info = {
                "name": row["Agency Name"],
                "state": row["State"],
                "type": row["Type"],
                "source_ori": ori,
            }
            agencies[ori] = info
            # If ORI ends in X, also register the 0-ending version
            # (RETA uses standard 0-suffix, Agency Sourcing may use X)
            if ori.endswith("X"):
                agencies[ori[:-1] + "0"] = info
    return agencies


def download_reta(year):
    """Download RETA master file for a given year from CDE. Returns bytes or None."""
    key = f"master_files/reta/reta-{year}.zip"
    print(f"  Requesting signed URL for {key}...")
    resp = requests.get(f"{CDE_SIGNED_URL}?key={key}", timeout=60)
    data = resp.json()
    if not data:
        print(f"  WARNING: {key} not available yet")
        return None
    signed_url = list(data.values())[0]
    print(f"  Downloading reta-{year}.zip...")
    r = requests.get(signed_url, stream=True, timeout=300)
    r.raise_for_status()
    content = b""
    for chunk in r.iter_content(65536):
        content += chunk
    size_mb = len(content) / (1024 * 1024)
    print(f"  Downloaded {size_mb:.1f} MB")
    return content


def parse_reta_zip(zip_bytes, target_oris, target_months):
    """Parse RETA zip file and extract matching records.
    Returns list of dicts in pipeline format.
    """
    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            raw = zf.read(name).decode("latin-1")
            lines = raw.split("\n")
            print(f"  Parsing {name}: {len(lines)} lines...")
            matched = 0
            for line in lines:
                if len(line) < 7385:
                    continue
                if line[0] != "1":
                    continue
                state_code = line[1:3]
                state = STATE_CODES.get(state_code, state_code)
                # RETA ORI field is 7 chars (includes state letters);
                # pad to 9 chars to match Agency Sourcing format
                ori_raw = line[3:10].strip()
                ori = (ori_raw + "00")[:9] if len(ori_raw) == 7 else ori_raw
                if ori not in target_oris:
                    continue
                year = resolve_year(line[13:15])
                agency_info = target_oris[ori]

                # Parse 12 monthly blocks (590 chars each, starting at pos 305)
                for m in range(12):
                    month = m + 1
                    if (year, month) not in target_months:
                        continue
                    base = 305 + (m * 590)
                    # Card 1 = Actual Offenses, starts at offset 157 within month block
                    card1_base = base + 17 + 140  # offset 17 + card0(140)

                    crimes = {}
                    for offense_name, field_idx in OFFENSE_INDICES.items():
                        start = card1_base + (field_idx * 5)
                        val = parse_reta_num(line[start:start + 5])
                        crimes[offense_name] = max(val, 0)
                    # Aggravated assault = sum of subcategories (not index 10 which includes simple)
                    aa = 0
                    for idx in AGG_ASSAULT_INDICES:
                        start = card1_base + (idx * 5)
                        aa += max(parse_reta_num(line[start:start + 5]), 0)
                    crimes["Aggravated Assault"] = aa

                    rows.append({
                        "ori": agency_info.get("source_ori", ori),
                        "Agency Name": agency_info["name"],
                        "Murder": crimes["Murder"],
                        "Rape": crimes["Rape"],
                        "Robbery": crimes["Robbery"],
                        "Aggravated Assault": crimes["Aggravated Assault"],
                        "Burglary": crimes["Burglary"],
                        "Theft": crimes["Theft"],
                        "Motor Vehicle Theft": crimes["Motor Vehicle Theft"],
                        "Year": year,
                        "Month": month,
                        "State": agency_info["state"],
                        "Type": agency_info["type"],
                    })
                    matched += 1
            print(f"  → {matched} agency-month records extracted")
    return rows


def main():
    window = nine_month_window()
    years_needed = sorted(set(y for y, m in window))
    print(f"9-month window: {sorted(window)}")
    print(f"Years to download: {years_needed}")

    agencies = load_cde_agencies()
    print(f"CDE-sourced agencies: {len(agencies)}")

    all_rows = []
    for year in years_needed:
        print(f"\n--- RETA {year} ---")
        zip_bytes = download_reta(year)
        if not zip_bytes:
            continue
        rows = parse_reta_zip(zip_bytes, agencies, window)
        all_rows.extend(rows)
        print(f"  Total so far: {len(all_rows)} records")

    if not all_rows:
        print("\nNo data collected.")
        return

    # Sort by state, agency, year, month
    all_rows.sort(key=lambda r: (r["State"], r["Agency Name"], r["Year"], r["Month"]))

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PIPELINE_HEADERS)
        writer.writeheader()
        writer.writerows(all_rows)

    states = set(r["State"] for r in all_rows)
    agencies_found = set(r["ori"] for r in all_rows)
    months_found = set((r["Year"], r["Month"]) for r in all_rows)
    # Count unique source ORIs (exclude duplicate X→0 mappings)
    source_oris = set(v["source_ori"] for v in agencies.values() if "source_ori" in v)
    total_agencies = len(source_oris) if source_oris else len(agencies)
    print(f"\nWrote {len(all_rows)} records to {OUT_CSV}")
    print(f"  {len(agencies_found)}/{total_agencies} agencies, {len(states)} states, {len(months_found)} months")

    # Report missing agencies
    missing = source_oris - agencies_found
    if missing:
        print(f"\n  {len(missing)} agencies not found in RETA files:")
        for ori in sorted(missing):
            info = agencies.get(ori, {})
            print(f"    {ori} ({info.get('name','?')}, {info.get('state','?')})")


if __name__ == "__main__":
    main()
