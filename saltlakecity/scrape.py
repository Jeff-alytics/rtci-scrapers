"""
Salt Lake City Police Department — Monthly Crime Counts Scraper
Data source : https://slcpd.com/open-data/crimestatistics/
              (PowerBI Embedded via app.powerbigov.us — public, no login required)
Tool        : Playwright (Chromium)

Ported into rtci-scrapers. Outputs the unified LONG-format JSON at
  saltlakecity/data/latest.json
as a list of records:
  {"agency": "Salt Lake City", "state": "UT", "type": "City",
   "year": 2026, "month": 3, "offense": "Murder", "count": 1}
One record per (year, month, offense). The scraper MERGES with any existing
latest.json (upsert on (year, month, offense)) so older history is preserved.

Offense names (exact): Murder, Rape, Robbery, Aggravated Assault,
Burglary, Theft, Motor Vehicle Theft.

Dashboard structure
-------------------
  Page URL  : https://slcpd.com/open-data/crimestatistics/
  PowerBI   : embedded in an <iframe src="https://app.powerbigov.us/view?r=...">
  Year slicer  : [aria-label="Choose Year: "] .button-slicer-text-wrap
  Month slicer : [aria-label="Choose Month: "] .button-slicer-text-wrap
  Table cells  : .cell-interactive  — "Selected" (chosen month's count) is at
                 offset +1 from the crime_name cell in the flat cell list.

Crime name → UCR mapping
------------------------
  Criminal Homicide               → Murder
  Rape                            → Rape
  Robbery - Business              → Robbery  (summed with Robbery - All Other)
  Robbery - All Other             → Robbery
  Aggravated Assault - Family     → Aggravated Assault  (summed)
  Aggravated Assault - NonFamily  → Aggravated Assault
  Burglary - Residential          → Burglary  (summed)
  Burglary - All Other            → Burglary
  Larceny - Vehicle Burg          → Theft  (summed)
  Larceny/Theft - Multi           → Theft
  Motor Vehicle Theft             → Motor Vehicle Theft
"""

import json
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DASHBOARD_URL = "https://slcpd.com/open-data/crimestatistics/"
AGENCY        = "Salt Lake City"
STATE         = "UT"
TYPE_VALUE    = "City"

OUT_JSON = Path(__file__).parent / "data" / "latest.json"

MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

OFFENSES = [
    "Murder", "Rape", "Robbery", "Aggravated Assault",
    "Burglary", "Theft", "Motor Vehicle Theft",
]

# Maps dashboard crime names → RTCI offense name
CRIME_MAP = {
    "Criminal Homicide":              "Murder",
    "Rape":                           "Rape",
    "Robbery - Business":             "Robbery",
    "Robbery - All Other":            "Robbery",
    "Aggravated Assault - Family":    "Aggravated Assault",
    "Aggravated Assault - NonFamily": "Aggravated Assault",
    "Burglary - Residential":         "Burglary",
    "Burglary - All Other":           "Burglary",
    "Larceny - Vehicle Burg":         "Theft",
    "Larceny/Theft - Multi":          "Theft",
    "Motor Vehicle Theft":            "Motor Vehicle Theft",
}


def _scrape_start_year():
    """Scrape the current year and the prior year (covers recent completed months)."""
    return date.today().year - 1


def _parse_count(raw):
    """Parse a cell value into an integer. Returns 0 for blank/dash values."""
    raw = raw.split("\n")[0].strip()
    raw = re.sub(r"[▲▼\s]", "", raw)
    raw = raw.replace(",", "")
    if not raw or raw == "-":
        return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def _parse_cells(cells):
    """
    Walk the flat .cell-interactive text list. Whenever a cell matches a known
    crime name, the next cell is the "Selected" month count.
    Returns a dict {offense: total_count}.
    """
    counts = defaultdict(int)
    for i, text in enumerate(cells):
        offense = CRIME_MAP.get(text.strip())
        if offense is not None and i + 1 < len(cells):
            counts[offense] += _parse_count(cells[i + 1])
    return counts


def _click_exact(locator, target_text):
    """Click the first element whose stripped inner text equals target_text."""
    for btn in locator.all():
        if btn.inner_text().strip() == target_text:
            btn.scroll_into_view_if_needed()
            btn.click()
            return True
    return False


def _wait_for_cells_change(frame, page, prev_cells,
                           max_wait_ms=12_000, poll_ms=500):
    """Poll .cell-interactive until content differs from prev_cells; return new list."""
    elapsed = 0
    while elapsed < max_wait_ms:
        page.wait_for_timeout(poll_ms)
        elapsed += poll_ms
        cells = frame.locator(".cell-interactive").all_inner_texts()
        if cells != prev_cells:
            page.wait_for_timeout(500)  # let table settle
            return frame.locator(".cell-interactive").all_inner_texts()
    return frame.locator(".cell-interactive").all_inner_texts()


def scrape(headless=True):
    """Scrape recent months. Returns a list of long-format records."""
    start_year = _scrape_start_year()
    today = date.today()
    current_month = date(today.year, today.month, 1)

    records = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        print(f"Loading dashboard: {DASHBOARD_URL}")
        page.goto(DASHBOARD_URL, timeout=90_000)
        page.wait_for_load_state("networkidle", timeout=90_000)

        frame = page.frame_locator("iframe[src*='powerbigov.us']")

        print("Waiting for dashboard to initialise...", end=" ", flush=True)
        frame.locator('[aria-label="Choose Year: "]').wait_for(timeout=90_000)
        print("ready.")

        year_locator  = frame.locator('[aria-label="Choose Year: "] .button-slicer-text-wrap')
        month_locator = frame.locator('[aria-label="Choose Month: "] .button-slicer-text-wrap')

        year_texts = [t.strip() for t in year_locator.all_inner_texts()]
        print(f"Years available: {year_texts}")

        for yr_text in year_texts:
            try:
                yr = int(yr_text)
            except ValueError:
                continue

            if yr < start_year or yr > today.year:
                continue

            print(f"\n  Selecting year {yr}...")
            _click_exact(year_locator, yr_text)
            page.wait_for_timeout(4000)  # allow month slicer to update

            month_texts = [t.strip() for t in month_locator.all_inner_texts()]
            print(f"  Months available: {month_texts}")

            prev_cells = []

            for mo_text in month_texts:
                if mo_text not in MONTH_ABBR:
                    continue
                mo = MONTH_ABBR.index(mo_text) + 1

                row_date = date(yr, mo, 1)
                if row_date >= current_month:
                    continue  # current/future month — incomplete data

                print(f"    {mo_text} {yr}...", end=" ", flush=True)

                _click_exact(month_locator, mo_text)

                if prev_cells:
                    cells = _wait_for_cells_change(frame, page, prev_cells)
                else:
                    page.wait_for_timeout(4000)
                    cells = frame.locator(".cell-interactive").all_inner_texts()

                prev_cells = cells
                counts = _parse_cells(cells)

                for offense in OFFENSES:
                    records.append({
                        "agency":  AGENCY,
                        "state":   STATE,
                        "type":    TYPE_VALUE,
                        "year":    yr,
                        "month":   mo,
                        "offense": offense,
                        "count":   int(counts.get(offense, 0)),
                    })

                print(
                    f"Murder={counts.get('Murder', 0)}  Rape={counts.get('Rape', 0)}  "
                    f"Robbery={counts.get('Robbery', 0)}  AA={counts.get('Aggravated Assault', 0)}  "
                    f"Burg={counts.get('Burglary', 0)}  Theft={counts.get('Theft', 0)}  "
                    f"MVT={counts.get('Motor Vehicle Theft', 0)}"
                )

        browser.close()

    return records


def load_existing():
    if OUT_JSON.exists():
        with OUT_JSON.open(encoding="utf-8") as f:
            return json.load(f)
    return []


def merge(existing, scraped):
    """Upsert scraped records over existing on (year, month, offense)."""
    merged = {}
    for rec in existing:
        merged[(rec["year"], rec["month"], rec["offense"])] = rec
    for rec in scraped:
        merged[(rec["year"], rec["month"], rec["offense"])] = rec

    offense_order = {name: i for i, name in enumerate(OFFENSES)}
    return sorted(
        merged.values(),
        key=lambda r: (r["year"], r["month"], offense_order.get(r["offense"], 99)),
    )


def main():
    existing = load_existing()
    print(f"Existing records: {len(existing)}")

    scraped = scrape(headless=True)
    print(f"\nScraped records: {len(scraped)}")

    if not scraped:
        print("No data scraped — leaving existing latest.json unchanged.")
        return

    merged = merge(existing, scraped)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(merged, f)

    months = sorted(set((r["year"], r["month"]) for r in merged))
    print(f"\nWrote {len(merged)} records to {OUT_JSON}")
    print(f"Month range: {months[0]} .. {months[-1]}")


if __name__ == "__main__":
    main()
