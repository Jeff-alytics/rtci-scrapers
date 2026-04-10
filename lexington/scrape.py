"""
Lexington KY NIBRS Monthly Crime Data Scraper

Downloads NIBRS PDFs from lexingtonky.gov and extracts Part I crime counts.
Each PDF's page 1 has NIBRS code rows with: Prior5YrAvg, MonPrior, MonCurrent, ...
We extract the single-month current-year values for the 7 UCR offenses.

Usage:
    python scrape.py              # scrape latest PDFs, merge with existing data
    python scrape.py --all        # re-scrape all PDFs from the site
"""

import pdfplumber
import re
import json
import os
import sys
import urllib.request
from html.parser import HTMLParser

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "data", "latest.json")
PAGE_URL = "https://www.lexingtonky.gov/government/departments-programs/public-safety/police/crime-data"

OFFENSES = [
    (r"\b09A\s+", "Murder"),
    (r"\b11A\s+Forcible\s+Rape", "Rape"),
    (r"\b13A\s+Aggravated\s+Assault", "Aggravated Assault"),
    (r"\b120\s+", "Robbery"),
    (r"\b220\s+", "Burglary"),
    (r"\b23A-H\s+", "Theft"),
    (r"\b240\s+Motor\s+Vehicle\s+Theft", "Motor Vehicle Theft"),
]

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


class LinkParser(HTMLParser):
    """Extract PDF links with month/year from the crime data page."""
    def __init__(self):
        super().__init__()
        self.links = []
        self._href = None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if ".pdf" in href.lower():
                self._href = href
            else:
                self._href = None

    def handle_data(self, data):
        if self._href:
            text = data.strip()
            m = re.search(
                r"\b(January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+(20\d{2})\b",
                text, re.IGNORECASE,
            )
            if m and re.search(r"NIBRS|Crime\s*Data", text, re.IGNORECASE):
                mo = MONTH_MAP[m.group(1).lower()]
                yr = int(m.group(2))
                url = self._href if self._href.startswith("http") else \
                    "https://www.lexingtonky.gov" + self._href
                self.links.append({"url": url, "text": text, "year": yr, "month": mo})

    def handle_endtag(self, tag):
        if tag == "a":
            self._href = None


def discover_pdfs():
    """Scrape the crime data page for PDF links."""
    req = urllib.request.Request(PAGE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    parser = LinkParser()
    parser.feed(html)
    parser.links.sort(key=lambda x: (x["year"], x["month"]), reverse=True)
    return parser.links


def download_pdf(url, dest):
    """Download a PDF to a local path."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        with open(dest, "wb") as f:
            f.write(resp.read())


def parse_pdf(path, report_year, report_month):
    """Extract single-month crime counts from page 1 of a Lexington NIBRS PDF."""
    pdf = pdfplumber.open(path)
    text = pdf.pages[0].extract_text() or ""
    pdf.close()

    results = []
    for line in text.split("\n"):
        stripped = line.strip()
        for pat, offense in OFFENSES:
            if re.match(pat, stripped, re.IGNORECASE):
                # After the 5yr avg (has decimal), next two ints = prior_month, current_month
                m = re.search(r"(\d[\d,]*\.\d+)\s+([\d,]+)\s+([\d,]+)", stripped)
                if m:
                    current_val = int(m.group(3).replace(",", ""))
                    results.append({
                        "agency": "Lexington",
                        "state": "KY",
                        "type": "City",
                        "year": report_year,
                        "month": report_month,
                        "offense": offense,
                        "count": current_val,
                    })
                break
    return results


def main():
    scrape_all = "--all" in sys.argv

    # Load existing data
    existing = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            existing = json.load(f)

    existing_keys = {(r["year"], r["month"], r["offense"]) for r in existing}

    # Discover available PDFs
    print("Discovering PDFs...")
    links = discover_pdfs()
    print(f"  Found {len(links)} PDFs")

    if not scrape_all:
        # Only download PDFs for months we don't have yet
        links = [l for l in links
                 if not all((l["year"], l["month"], off) in existing_keys
                            for _, off in OFFENSES)]
        if not links:
            print("  No new months to scrape")
            return

    print(f"  Scraping {len(links)} PDFs...")
    tmp_dir = os.path.join(SCRIPT_DIR, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    new_data = []
    for lnk in links:
        fname = f"lex_{lnk['year']}_{lnk['month']:02d}.pdf"
        fpath = os.path.join(tmp_dir, fname)
        print(f"  {lnk['year']}-{lnk['month']:02d}: {lnk['text']}")
        try:
            download_pdf(lnk["url"], fpath)
            rows = parse_pdf(fpath, lnk["year"], lnk["month"])
            new_data.extend(rows)
            print(f"    -> {len(rows)} offenses extracted")
        except Exception as e:
            print(f"    ERROR: {e}")

    # Merge: new data overwrites existing for same (year, month, offense)
    merged = {(r["year"], r["month"], r["offense"]): r for r in existing}
    for r in new_data:
        merged[(r["year"], r["month"], r["offense"])] = r

    final = sorted(merged.values(), key=lambda r: (r["year"], r["month"], r["offense"]))

    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(final, f, indent=2)

    print(f"\nWrote {len(final)} rows to {DATA_FILE}")


if __name__ == "__main__":
    main()
