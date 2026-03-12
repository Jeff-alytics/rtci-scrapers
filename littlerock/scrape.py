"""
Little Rock Part I Offenses Scraper
https://littlerock.gov/government/mayors-office/initiatives/crime-reduction-strategy/crime-stats/

Downloads the "Part I Offenses by Month" PDF, parses two years of data
from pages 2 and 4, and outputs JSON in the RTCI pipeline format.

Usage:
    python scrape.py            # save to data/latest.json
    python scrape.py --json     # output JSON to stdout
"""

import re
import sys
import json
import argparse
import requests
from bs4 import BeautifulSoup
import pdfplumber
from pathlib import Path
from tempfile import NamedTemporaryFile

PAGE_URL = "https://littlerock.gov/government/mayors-office/initiatives/crime-reduction-strategy/crime-stats/"
LINK_TEXT_PATTERN = re.compile(r"Part\s+I\s+Offenses\s+by\s+Month", re.IGNORECASE)

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
MONTH_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}

# PDF label -> RTCI offense name
CRIME_MAP = {
    "All Homicide Offenses": "Murder",
    "Forcible Rape":         "Rape",
    "Robbery":               "Robbery",
    "Aggravated Assault":    "Aggravated Assault",
    "Burglary/B & E":        "Burglary",
    "Larceny":               "Theft",
    "Stolen Vehicle":        "Motor Vehicle Theft",
}

# For detecting how many months have data
INDIVIDUAL_CRIMES = list(CRIME_MAP.keys())

# Pages to parse (0-indexed): page 2 and page 4
DATA_PAGES = [1, 3]


def download_pdf():
    print("Fetching page…", file=sys.stderr)
    r = requests.get(PAGE_URL)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.find("a", string=LINK_TEXT_PATTERN)
    if not link:
        for a in soup.find_all("a", href=True):
            if LINK_TEXT_PATTERN.search(a.get_text()):
                link = a
                break
    if not link:
        raise RuntimeError("Could not find 'Part I Offenses by Month' link")

    pdf_url = link["href"]
    if not pdf_url.startswith("http"):
        pdf_url = "https://littlerock.gov" + pdf_url

    print(f"Downloading: {pdf_url}", file=sys.stderr)
    pdf_r = requests.get(pdf_url)
    pdf_r.raise_for_status()

    tmp = NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(pdf_r.content)
    tmp.close()
    print(f"Downloaded {len(pdf_r.content) // 1024} KB", file=sys.stderr)
    return tmp.name


def parse_page(text):
    lines = text.strip().split("\n")
    year = int(lines[0].strip())

    crime_data = {}
    for line in lines:
        for pdf_label in CRIME_MAP:
            if line.startswith(pdf_label):
                numbers_str = line[len(pdf_label):].strip()
                numbers = [int(x) for x in numbers_str.split()]
                monthly = numbers[:-1] if len(numbers) > 1 else numbers
                crime_data[pdf_label] = monthly
                break

    num_months = max(
        (len(crime_data.get(c, [])) for c in INDIVIDUAL_CRIMES), default=0
    )

    records = []
    for m in range(num_months):
        for pdf_label, offense in CRIME_MAP.items():
            values = crime_data.get(pdf_label, [])
            count = values[m] if m < len(values) else 0
            records.append({
                "agency": "Little Rock",
                "state": "AR",
                "type": "City",
                "year": year,
                "month": m + 1,
                "offense": offense,
                "count": count,
            })

    return records


def scrape():
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    pdf_path = download_pdf()
    all_records = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pg_idx in DATA_PAGES:
                page = pdf.pages[pg_idx]
                text = page.extract_text()
                if not text:
                    print(f"WARNING: No text on page {pg_idx + 1}", file=sys.stderr)
                    continue
                records = parse_page(text)
                all_records.extend(records)
                year = records[0]["year"] if records else "?"
                months = len(set(r["month"] for r in records))
                print(f"  Page {pg_idx + 1}: {year} — {months} months", file=sys.stderr)
    finally:
        Path(pdf_path).unlink(missing_ok=True)

    all_records.sort(key=lambda r: (r["year"], r["month"], r["offense"]))
    print(f"Total: {len(all_records)} records", file=sys.stderr)
    return all_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    records = scrape()

    if args.json:
        print(json.dumps(records, indent=2))
    else:
        with open(OUT_JSON, "w") as f:
            json.dump(records, f, indent=2)
        print(f"Saved {len(records)} records to {OUT_JSON}", file=sys.stderr)
