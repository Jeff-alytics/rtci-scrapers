"""
Omaha Police Department Crime Statistics Scraper
=================================================
Source: https://police.cityofomaha.org/crime-statistics
PDF: https://police.cityofomaha.org/images/crime-statistics-reports/{YEAR_PATH}/Web_IndexCrimes_{YYYYMM}.pdf
Uses Playwright to bypass Akamai, then PyMuPDF to extract text from PDF.
Outputs data/latest.json in RTCI pipeline format.

NOTE: The year in the URL path (e.g., /2024/) may need updating annually.
"""

import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

# URL path year — may need annual update
URL_PATH_YEAR = "2024"
BASE_URL = f"https://police.cityofomaha.org/images/crime-statistics-reports/{URL_PATH_YEAR}"

CRIME_ORDER = [
    "Criminal Homicide",
    "Rape",
    "Robbery",
    "Aggravated Assault",
    "Burglary",
    "Theft",
    "Auto Theft",
]

RTCI_MAP = {
    "Criminal Homicide": "Murder",
    "Rape": "Rape",
    "Robbery": "Robbery",
    "Aggravated Assault": "Aggravated Assault",
    "Burglary": "Burglary",
    "Theft": "Theft",
    "Auto Theft": "Motor Vehicle Theft",
}

MONTHS = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


def download_pdf(url):
    """Download PDF via Playwright (bypasses Akamai)."""
    from playwright.sync_api import sync_playwright

    pdf_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        )
        page = ctx.new_page()

        # Visit main page first to get cookies
        page.goto('https://police.cityofomaha.org/crime-statistics', timeout=30000)
        page.wait_for_timeout(3000)

        # Intercept PDF response
        def handle_route(route):
            resp = route.fetch()
            pdf_data.append(resp.body())
            route.fulfill(response=resp)

        page.route('**/*.pdf', handle_route)
        page.goto(url, timeout=30000)
        page.wait_for_timeout(3000)

        browser.close()

    return pdf_data[0] if pdf_data else None


def parse_pdf(pdf_bytes):
    """Extract crime data from Omaha PDF. Returns list of RTCI records."""
    import fitz

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    text = page.get_text()

    # Find the 2026 section (current year data)
    # Structure: month headers like "January 2026", then 7 crime values per month
    records = []

    # Extract all numbers from the text (in order)
    lines = text.split('\n')

    # Find month+year headers and their associated crime counts
    current_year = None
    collecting = False
    month_data = {}
    current_month = None
    crime_idx = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Stop at YTD row
        if line.upper().startswith('YTD'):
            collecting = False
            continue

        # Check for month+year header
        month_match = re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$', line)
        if month_match:
            month_name, year = month_match.group(1), int(month_match.group(2))
            if year == date.today().year:
                current_year = year
                current_month = MONTHS[month_name]
                collecting = True
                crime_idx = 0
                continue
            else:
                if collecting:
                    collecting = False
                continue

        if not collecting:
            continue

        # Try to parse a count value
        clean = line.replace(',', '').strip()
        if re.match(r'^\d+$', clean):
            count = int(clean)
            if crime_idx < len(CRIME_ORDER):
                crime = CRIME_ORDER[crime_idx]
                rtci = RTCI_MAP[crime]
                records.append({
                    'agency': 'Omaha',
                    'state': 'NE',
                    'type': 'City',
                    'year': current_year,
                    'month': current_month,
                    'offense': rtci,
                    'count': count,
                })
                crime_idx += 1
        elif re.match(r'^[+-]?\d+%$', clean):
            # Skip percentage change values
            continue

    return records


def scrape():
    today = date.today()
    # Try current month's PDF, then previous months
    for months_back in range(0, 4):
        m = today.month - months_back
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        yyyymm = f"{y}{m:02d}"
        url = f"{BASE_URL}/Web_IndexCrimes_{yyyymm}.pdf"
        print(f"Trying {url}...")

        try:
            pdf_bytes = download_pdf(url)
            if pdf_bytes and len(pdf_bytes) > 10000:
                print(f"  Downloaded {len(pdf_bytes):,} bytes")
                records = parse_pdf(pdf_bytes)
                if records:
                    print(f"  Parsed {len(records)} records")
                    return records
                else:
                    print(f"  No records parsed")
            else:
                print(f"  Too small or failed ({len(pdf_bytes) if pdf_bytes else 0} bytes)")
        except Exception as e:
            print(f"  Error: {e}")

    return []


def main():
    records = scrape()
    print(f"\nTotal: {len(records)} records")
    for r in records:
        print(f"  {r['year']}-{r['month']:02d} {r['offense']}: {r['count']}")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w") as f:
        json.dump(records, f, indent=2)
    print(f"Saved to: {OUT_JSON.resolve()}")


if __name__ == "__main__":
    main()
