"""
Albany, GA Police Department -- Monthly NIBRS Crime Counts Scraper
=================================================================

Data source:
  https://www.albanyga.gov/City-Departments/Albany-Police-Department/Crime-Statistics
  Monthly NIBRS PDF reports. Page 3 contains the NIBRS Profile table with
  current-month incident counts per offense category.

  The site requires curl_cffi with Chrome impersonation (plain requests
  returns 403 or bot-detection page).

Output: data/latest.json in RTCI pipeline format:
  [{agency, state, type, year, month, offense, count}, ...]

Window: 24 months ending the month before today.

Usage:
    python scrape.py           # save to data/latest.json
    python scrape.py --json    # output JSON to stdout
"""

import io
import re
import sys
import json
import argparse
from datetime import date
from pathlib import Path

import pdfplumber
from bs4 import BeautifulSoup
from curl_cffi import requests

# -- Constants ----------------------------------------------------------------

ARCHIVE_URL = "https://www.albanyga.gov/City-Departments/Albany-Police-Department/Crime-Statistics"
BASE_URL = "https://www.albanyga.gov"

AGENCY = "Albany"
STATE = "GA"
TYPE = "City"
WINDOW_MONTHS = 6

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# Offense name in PDF table -> RTCI category
# These are the PARENT rows; sub-rows (e.g. "Armed Robbery") are skipped.
OFFENSE_MAP = {
    "Murder":                       "Murder",
    "Forcible Rape":                "Rape",
    "Robbery":                      "Robbery",
    "Aggravated Assault":           "Aggravated Assault",
    "Burglary/Breaking & Entering": "Burglary",
    "Larceny/Theft Offenses":       "Theft",
    "Motor Vehicle Theft":          "Motor Vehicle Theft",
}

RTCI_OFFENSES = [
    "Murder", "Rape", "Robbery", "Aggravated Assault",
    "Burglary", "Theft", "Motor Vehicle Theft",
]

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"


# -- Window -------------------------------------------------------------------

def compute_window():
    """Return (start_date, end_date) for the rolling 24-month window.
    End = last completed month. Start = 24 months back (inclusive)."""
    today = date.today()
    if today.month == 1:
        end = date(today.year - 1, 12, 1)
    else:
        end = date(today.year, today.month - 1, 1)
    start_month = end.month - (WINDOW_MONTHS - 1)
    start_year = end.year
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    return date(start_year, start_month, 1), end


# -- Link discovery -----------------------------------------------------------

def parse_link_date(title):
    """Extract (year, month) from link text like 'February 2026 NIBRS Report(PDF,...'."""
    for i, name in enumerate(MONTH_NAMES):
        m = re.search(rf"\b{name}\s+(20\d{{2}})\b", title)
        if m:
            return int(m.group(1)), i + 1
    return None


def fetch_pdf_links(session):
    """Return list of (year, month, full_url) for all NIBRS monthly PDF links."""
    resp = session.get(ARCHIVE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        if "NIBRS" not in text or ".pdf" not in href.lower():
            continue
        parsed = parse_link_date(text)
        if parsed is None:
            continue
        year, month = parsed
        full_url = href if href.startswith("http") else BASE_URL + href
        links.append((year, month, full_url))

    return links


# -- PDF parsing --------------------------------------------------------------

def extract_rtci(pdf_bytes):
    """Parse a monthly NIBRS PDF and return a dict of RTCI category -> count.

    The NIBRS Profile table is on page index 2 (3rd page).
    Table columns: [category_label, nibrs_code, offense_name, current_incidents, ...]
    We read column index 3 (current month incidents) for matching offense names.
    """
    results = {cat: 0 for cat in RTCI_OFFENSES}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if len(pdf.pages) < 3:
            return results

        tables = pdf.pages[2].extract_tables()
        if not tables:
            return results

        table = tables[0]
        for row in table:
            if not row or len(row) < 4:
                continue
            offense_name = str(row[2] or "").strip()
            if offense_name not in OFFENSE_MAP:
                continue
            rtci_cat = OFFENSE_MAP[offense_name]
            raw = str(row[3] or "").strip()
            try:
                results[rtci_cat] = int(raw.replace(",", ""))
            except ValueError:
                results[rtci_cat] = 0

    return results


# -- Main ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Albany GA monthly crime scraper")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    print("=" * 60)
    print("  Albany, GA PD -- Monthly NIBRS Crime Scraper")
    print("=" * 60)

    start_date, end_date = compute_window()
    print(f"Window: {start_date} to {end_date} (inclusive)\n")

    session = requests.Session(impersonate="chrome120")

    print("Fetching archive page...")
    pdf_links = fetch_pdf_links(session)

    # Filter to window
    pdf_links = [
        (y, m, u) for y, m, u in pdf_links
        if start_date <= date(y, m, 1) <= end_date
    ]
    pdf_links.sort(key=lambda x: (x[0], x[1]))
    print(f"Found {len(pdf_links)} PDF(s) in window\n")

    records = []
    for year, month, url in pdf_links:
        label = f"{year}-{month:02d}"
        print(f"  Downloading {label} ...", end=" ", flush=True)
        resp = session.get(url)
        if resp.status_code != 200:
            print(f"ERROR {resp.status_code}")
            continue
        counts = extract_rtci(resp.content)
        print(
            f"Mur={counts['Murder']} Rape={counts['Rape']} "
            f"Rob={counts['Robbery']} AA={counts['Aggravated Assault']} "
            f"Burg={counts['Burglary']} Theft={counts['Theft']} "
            f"MVT={counts['Motor Vehicle Theft']}"
        )
        for offense in RTCI_OFFENSES:
            records.append({
                "agency": AGENCY,
                "state": STATE,
                "type": TYPE,
                "year": year,
                "month": month,
                "offense": offense,
                "count": counts[offense],
            })

    print(f"\nTotal records: {len(records)}")

    if args.json:
        print(json.dumps(records, indent=2))
    else:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        with OUT_JSON.open("w") as f:
            json.dump(records, f, indent=2)
        print(f"Saved to: {OUT_JSON.resolve()}")


if __name__ == "__main__":
    main()
