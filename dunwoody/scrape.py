"""
Dunwoody, GA Crime Scraper
===========================
Source: https://www.dunwoodyga.gov/police/crime-info-stats/crime-comparison
Uses curl_cffi (Chrome TLS impersonation) to bypass Akamai CDN.
Parses Crime Comparison PDFs with pdfplumber.
Outputs data/latest.json in RTCI pipeline format.

Usage:
    python scrape.py            # save to data/latest.json
    python scrape.py --json     # output JSON to stdout
"""

import io
import re
import json
import argparse
from datetime import date
from pathlib import Path
from bs4 import BeautifulSoup
from curl_cffi import requests
import pdfplumber

# ── Config ───────────────────────────────────────────────────────────────────

ARCHIVE_URL = "https://www.dunwoodyga.gov/police/crime-info-stats/crime-comparison"
BASE_URL = "https://www.dunwoodyga.gov"
AGENCY = "Dunwoody"
STATE = "GA"
TYPE = "City"
WINDOW_MONTHS = 24

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

# PDF row label → RTCI category
RTCI_MAP = {
    "Homicide":           "Murder",
    "Rape":               "Rape",
    "Armed Robbery":      "Robbery",
    "Aggravated Assault": "Aggravated Assault",
    "Burglary":           "Burglary",
    "Larceny":            "Theft",
    "Motor Veh Theft":    "Motor Vehicle Theft",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_window():
    """Return (start_date, end_date) for the rolling 24-month window."""
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


def parse_data_year(title):
    """
    Extract the data year from a PDF link title.
    Convention: the SECOND 4-digit year in the title is the data year.
      '2024 - 2025 Crime Comparison'                    -> 2025
      '2025-2026 Crime Comparison (Through February)'   -> 2026
    """
    years = re.findall(r"\b(20\d{2})\b", title)
    if len(years) >= 2:
        return int(years[1])
    if len(years) == 1:
        return int(years[0])
    return None


def fetch_pdf_links(session):
    """Return list of (data_year, title, full_url) for all Crime Comparison PDFs."""
    resp = session.get(ARCHIVE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        if "Crime Comparison" not in text or "showpublisheddocument" not in href:
            continue
        data_year = parse_data_year(text)
        if data_year is None:
            continue
        full_url = href if href.startswith("http") else BASE_URL + href
        links.append((data_year, text, full_url))

    return links


def extract_monthly_data(pdf_bytes, data_year, start_date, end_date):
    """
    Parse a Crime Comparison PDF and return records in RTCI long format.

    Table structure:
      - Single page, single table
      - Header row: 'Violent Crimes' | Jan-XX...Dec-XX | YTD-YYYY | YTD-YYYY | % Change
      - Monthly values at column indices 1-12 (Jan=1, Dec=12)
      - Months with no data yet have empty strings / None
      - NOTE: column year labels may be off by 1 due to template reuse;
        always use data_year from the PDF title.
    """
    records = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        tables = pdf.pages[0].extract_tables()
        if not tables:
            print(f"    WARNING: no tables found in PDF for {data_year}")
            return records
        table = tables[0]

        # Collect monthly values for each RTCI row label
        data = {}
        for row in table:
            if not row or not row[0]:
                continue
            label = str(row[0]).strip()
            if label not in RTCI_MAP:
                continue
            monthly = []
            for i in range(1, 13):
                cell = row[i] if i < len(row) else None
                if cell and str(cell).strip():
                    try:
                        monthly.append(int(str(cell).replace(",", "")))
                    except ValueError:
                        monthly.append(0)
                else:
                    monthly.append(None)  # not yet reported
            data[label] = monthly

        if not data:
            print(f"    WARNING: no matching crime rows in PDF for {data_year}")
            return records

        # Emit records for each month in window
        for month_idx in range(12):
            month_num = month_idx + 1
            month_date = date(data_year, month_num, 1)

            if month_date < start_date or month_date > end_date:
                continue

            # Skip months not yet reported
            if all(data.get(lbl, [None] * 12)[month_idx] is None for lbl in data):
                continue

            for pdf_label, rtci_offense in RTCI_MAP.items():
                count = (data.get(pdf_label, [0] * 12)[month_idx]) or 0
                if count > 0:
                    records.append({
                        "agency": AGENCY,
                        "state": STATE,
                        "type": TYPE,
                        "year": data_year,
                        "month": month_num,
                        "offense": rtci_offense,
                        "count": count,
                    })

    return records


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Dunwoody GA crime scraper")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    print("=" * 50)
    print("  Dunwoody, GA — Crime Comparison PDF Scraper")
    print("=" * 50)

    start_date, end_date = get_window()
    needed_years = set(range(start_date.year, end_date.year + 1))
    print(f"Window: {start_date} to {end_date}")

    session = requests.Session(impersonate="chrome120")

    print("Fetching archive page...")
    pdf_links = fetch_pdf_links(session)
    pdf_links = [(y, t, u) for y, t, u in pdf_links if y in needed_years]
    pdf_links.sort(key=lambda x: x[0])
    print(f"Found {len(pdf_links)} relevant PDF(s): {[t for _, t, _ in pdf_links]}")

    all_records = []
    for data_year, title, url in pdf_links:
        print(f"  Downloading: {title}...")
        resp = session.get(url)
        resp.raise_for_status()
        records = extract_monthly_data(resp.content, data_year, start_date, end_date)
        months = len(set((r["year"], r["month"]) for r in records))
        print(f"    -> {months} month(s), {len(records)} records")
        all_records.extend(records)

    all_records.sort(key=lambda r: (r["year"], r["month"], r["offense"]))
    print(f"\nTotal: {len(all_records)} records")

    if args.json:
        print(json.dumps(all_records, indent=2))
    else:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        with OUT_JSON.open("w") as f:
            json.dump(all_records, f, indent=2)
        print(f"Saved to: {OUT_JSON.resolve()}")


if __name__ == "__main__":
    main()
