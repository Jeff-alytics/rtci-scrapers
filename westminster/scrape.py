"""
Westminster CA Crime Scraper — city crime-statistics page.

The site's WAF (Akamai) blocks headless browsers and plain HTTP clients but
allows a headed browser, so this runs Playwright with headless=False —
use xvfb-run on CI.

Page has one table per year: header row [YYYY, HOMICIDE, RAPE, ROBBERY,
AGGRAVATED ASSAULT, BURGLARY, LARCENY, STOLEN VEHICLE, ARSON, TOTAL] and one
row per month (empty cells = month not yet published). Westminster publishes
with a ~3-4 month lag.

Output: westminster/data/latest.json in unified pipeline format.
"""

import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

URL = ("https://www.westminster-ca.gov/departments/police/services/"
       "investigations-bureau/crime-analysis/crime-statistics")
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

COLMAP = {
    "HOMICIDE": "Murder",
    "RAPE": "Rape",
    "ROBBERY": "Robbery",
    "AGGRAVATED ASSAULT": "Aggravated Assault",
    "BURGLARY": "Burglary",
    "LARCENY": "Theft",
    "STOLEN VEHICLE": "Motor Vehicle Theft",
}
MONTHS = {"JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
          "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12}


def main():
    rows_out = []
    with sync_playwright() as pw:
        # headless=False is required to pass the WAF — run under xvfb on CI
        browser = pw.chromium.launch(headless=False,
                                     args=["--disable-blink-features=AutomationControlled"])
        page = browser.new_context(locale="en-US",
                                   viewport={"width": 1400, "height": 900}).new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(6000)
        title = page.title()
        print(f"Page title: {title}")
        if "denied" in title.lower():
            print("WAF denied access.")
            sys.exit(1)

        tables = page.evaluate(
            "() => [...document.querySelectorAll('table')]"
            ".map(t => [...t.rows].map(r => [...r.cells].map(c => c.innerText.trim())))")
        browser.close()

    for table in tables:
        if not table or not table[0]:
            continue
        hdr = [c.upper() for c in table[0]]
        if not hdr[0].isdigit() or len(hdr[0]) != 4:
            continue
        year = int(hdr[0])
        cols = {i: COLMAP[h] for i, h in enumerate(hdr) if h in COLMAP}
        if len(cols) < 5:
            continue
        for row in table[1:]:
            month = MONTHS.get((row[0] or "").upper())
            if not month:
                continue
            vals = {}
            for i, off in cols.items():
                cell = (row[i] if i < len(row) else "").replace(",", "").strip()
                if cell.isdigit():
                    vals[off] = int(cell)
            if not vals:
                continue  # month not published yet
            for off, cnt in vals.items():
                rows_out.append({"agency": "Westminster", "state": "CA", "type": "City",
                                 "year": year, "month": month, "offense": off, "count": cnt})
        print(f"  {year}: {len({(r['year'], r['month']) for r in rows_out if r['year'] == year})} months")

    if not rows_out:
        print("No data collected.")
        sys.exit(1)

    # Keep the 6 most recent months
    keep = sorted({(r["year"], r["month"]) for r in rows_out}, reverse=True)[:6]
    rows_out = [r for r in rows_out if (r["year"], r["month"]) in keep]

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(rows_out, f)
    print(f"\nWrote {len(rows_out)} records ({len(keep)} months) to {OUT_JSON}")


if __name__ == "__main__":
    main()
