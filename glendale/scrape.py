"""
Glendale, CA Police Department -- Monthly UCR/NIBRS Crime Counts Scraper
========================================================================

Data source:
  https://www.glendaleca.gov/government/departments/police-department/
  community-outreach-resources-and-engagement-c-o-r-e/
  crime-prevention-programs-resources/crime-statistics-booking-logs

  The most recent NIBRS Report Data PDF contains all monthly data from 2019
  onward (UCR through September 2022, NIBRS from October 2022). A single PDF
  download covers the full rolling window.

  The site requires curl_cffi with Chrome impersonation (plain requests
  returns 403).

  Each data row in the PDF:
    REPORT_SYSTEM YEAR MONTH  HOMICIDE RAPE ROBBERY AGG_ASSAULT BURGLARY
    LARCENY AUTO_THEFT ARSON GRAND_TOTAL
  where REPORT_SYSTEM is "UCR" or "NIBRS". Annual total rows start with a
  4-digit year and are skipped.

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
from dateutil.relativedelta import relativedelta

# -- Constants ----------------------------------------------------------------

BASE_URL = "https://www.glendaleca.gov"
PAGE_URL = (
    f"{BASE_URL}/government/departments/police-department/community-outreach-"
    "resources-and-engagement-c-o-r-e/crime-prevention-programs-resources/"
    "crime-statistics-booking-logs"
)

AGENCY = "Glendale"
STATE = "CA"
TYPE = "City"
WINDOW_MONTHS = 12

MONTH_ABBREVS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Numeric positions within a data row's values (0-indexed after SYSTEM/YEAR/MONTH):
#   0=Homicide, 1=Rape, 2=Robbery, 3=AggAssault, 4=Burglary,
#   5=Larceny/Theft, 6=AutoTheft, 7=Arson, 8=GrandTotal
RTCI_POSITIONS = {
    0: "Murder",
    1: "Rape",
    2: "Robbery",
    3: "Aggravated Assault",
    4: "Burglary",
    5: "Theft",
    6: "Motor Vehicle Theft",
}

RTCI_OFFENSES = [
    "Murder", "Rape", "Robbery", "Aggravated Assault",
    "Burglary", "Theft", "Motor Vehicle Theft",
]

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"


# -- Window -------------------------------------------------------------------

def compute_window():
    """Return list of (year, month) tuples for the 24-month rolling window,
    ending the month before today."""
    today = date.today()
    if today.month == 1:
        end = date(today.year - 1, 12, 1)
    else:
        end = date(today.year, today.month - 1, 1)
    start = end - relativedelta(months=WINDOW_MONTHS - 1)
    months = []
    d = start
    while d <= end:
        months.append((d.year, d.month))
        d += relativedelta(months=1)
    return months


# -- Link discovery -----------------------------------------------------------

def find_latest_nibrs_url(session):
    """Fetch the listing page and return the URL of the most recent NIBRS PDF.
    The most recent link has the highest document ID embedded in its URL."""
    r = session.get(PAGE_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text().strip()
        if "showpublisheddocument" not in href.lower():
            continue
        if "nibrs" not in text.lower():
            continue
        m = re.search(r"/showpublisheddocument/(\d+)/", href, re.IGNORECASE)
        if m:
            doc_id = int(m.group(1))
            full_url = href if href.startswith("http") else BASE_URL + href
            candidates.append((doc_id, full_url))

    if not candidates:
        return None

    # Highest document ID = most recent publication
    candidates.sort(reverse=True)
    return candidates[0][1]


# -- PDF parsing --------------------------------------------------------------

def parse_pdf(pdf_bytes):
    """Parse the NIBRS Report PDF and return a nested dict:
    { year: { month: { rtci_col: count } } }
    """
    data = {}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = ""
        for page in pdf.pages:
            text += (page.extract_text() or "") + "\n"

    for line in text.splitlines():
        tokens = line.split()
        if len(tokens) < 12:
            continue

        # Data rows start with UCR or NIBRS
        if tokens[0] not in ("UCR", "NIBRS"):
            continue

        # Second token must be a 4-digit year
        if not re.match(r"^\d{4}$", tokens[1]):
            continue

        # Third token must be a month abbreviation
        month_str = tokens[2].lower()
        if month_str not in MONTH_ABBREVS:
            continue

        year = int(tokens[1])
        month = MONTH_ABBREVS[month_str]

        # Collect the 9 numeric values after the month token
        nums = []
        for tok in tokens[3:]:
            clean = tok.replace(",", "")
            if re.match(r"^\d+$", clean):
                nums.append(int(clean))
            if len(nums) == 9:
                break

        if len(nums) < 7:
            continue

        counts = {col: nums[pos] for pos, col in RTCI_POSITIONS.items()}
        data.setdefault(year, {})[month] = counts

    return data


# -- Main ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Glendale CA monthly crime scraper")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    print("=" * 60)
    print("  Glendale, CA PD -- Monthly NIBRS Crime Scraper")
    print("=" * 60)

    rolling = compute_window()
    print(f"Window: {rolling[0][0]}-{rolling[0][1]:02d} to "
          f"{rolling[-1][0]}-{rolling[-1][1]:02d} ({len(rolling)} months)\n")

    session = requests.Session(impersonate="chrome120")

    print("Fetching listing page...")
    url = find_latest_nibrs_url(session)
    if not url:
        print("ERROR: no NIBRS PDF link found on listing page.")
        sys.exit(1)
    print(f"  Most recent NIBRS PDF: {url}\n")

    print("Downloading PDF...")
    r = session.get(url, timeout=60)
    r.raise_for_status()
    print(f"  Downloaded {len(r.content):,} bytes.\n")

    print("Parsing PDF...")
    year_data = parse_pdf(r.content)
    print("Coverage in PDF:")
    for yr in sorted(year_data):
        print(f"  {yr}: months {sorted(year_data[yr].keys())}")

    records = []
    for yr, mo in rolling:
        counts = year_data.get(yr, {}).get(mo, {})
        if not counts:
            print(f"  WARNING: no data for {yr}-{mo:02d} -- skipping")
            continue

        print(
            f"  {yr}-{mo:02d}: Mur={counts.get('Murder',0)} "
            f"Rape={counts.get('Rape',0)} Rob={counts.get('Robbery',0)} "
            f"AA={counts.get('Aggravated Assault',0)} "
            f"Burg={counts.get('Burglary',0)} Theft={counts.get('Theft',0)} "
            f"MVT={counts.get('Motor Vehicle Theft',0)}"
        )

        for offense in RTCI_OFFENSES:
            records.append({
                "agency": AGENCY,
                "state": STATE,
                "type": TYPE,
                "year": yr,
                "month": mo,
                "offense": offense,
                "count": counts.get(offense, 0),
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
