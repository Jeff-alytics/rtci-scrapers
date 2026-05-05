"""
Connecticut SRS Crime Scraper — B2020 Report 239
Source: https://ct.beyond2020.com/ct_public/View/dispview.aspx?ReportId=239
SRS format: agencies as rows, offenses as columns.
All agencies in one table read per year — just need ShowDim for month.
Outputs connecticut/data/latest.json in RTCI pipeline format.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

STATE = "CT"
REPORT_URL = "https://ct.beyond2020.com/ct_public/View/dispview.aspx?ReportId=239"
ENTRY_URL = "https://ct.beyond2020.com/ct_public/Dim/dimension.aspx"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

# Dim 3 = Return A Date (year), Dim 8 = Summary Month
DIM_DATE = 3
DIM_MONTH = 8

AGENCIES = {
    "Bridgeport": "City", "Bristol": "City", "Danbury": "City",
    "East Hartford": "City", "Fairfield": "City", "Greenwich": "City",
    "Hamden": "City", "Hartford": "City", "Manchester": "City",
    "Meriden": "City", "Milford": "City", "New Britain": "City",
    "New Haven": "City", "Norwalk": "City", "Stamford": "City",
    "Stratford": "City", "Waterbury": "City", "West Hartford": "City",
    "West Haven": "City",
}

OFFENSE_MAP = {
    "Criminal Homicide": "Murder",
    "Forcible Rape Total": "Rape",
    "Robbery Total": "Robbery",
    "Assault Total": "Aggravated Assault",
    "Burglary Total": "Burglary",
    "Larceny - Theft Total": "Theft",
    "Motor Vehicle Theft Total": "Motor Vehicle Theft",
}

MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]


def nine_month_window():
    now = datetime.now().replace(day=1)
    months = []
    y, m = now.year, now.month - 1
    if m == 0: y, m = y - 1, 12
    for _ in range(9):
        months.append((y, m))
        m -= 1
        if m == 0: y, m = y - 1, 12
    return sorted(months)


def nav_select(page, dim_num, search_term, member_name):
    """Open dim browser, search, clear, select member, return to report."""
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            page.evaluate(f"ShowDim({dim_num}, {dim_num});")
        page.wait_for_timeout(800)
    except Exception:
        return False
    try:
        page.fill('input[name="SearchString"]', search_term)
        with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
            page.evaluate('MembersSearch(document.querySelector(\'input[name="SearchString"]\'));')
        page.wait_for_timeout(500)
    except Exception:
        return False

    body = page.evaluate("() => document.body.innerText")
    if member_name.lower() not in body.lower():
        return False

    page.evaluate("OnSelAllClick(0);")
    page.wait_for_timeout(200)
    clicked = page.evaluate("""
    (name) => {
        var spans = Array.from(document.querySelectorAll('span.rtIn'));
        var nl = name.toLowerCase();
        for (var span of spans) {
            var t = span.textContent.trim().toLowerCase();
            if (t === nl || t.startsWith(nl + ' ') || t.startsWith(nl + ',')) {
                var label = span.closest('label');
                if (label) { var chk = label.querySelector('input.rtChk'); if (chk) { chk.click(); return 'ok'; } }
            }
        }
        return 'no';
    }
    """, member_name)
    if clicked != 'ok':
        return False
    page.wait_for_timeout(200)
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            page.evaluate("ShowUpdatedReportAfterSelection();")
        page.wait_for_timeout(1500)
    except Exception:
        return False
    return True


def parse_count(text):
    t = str(text).strip().replace(",", "").replace("\xa0", "")
    if not t or t in (".", "-", "*", " "):
        return 0
    try:
        return int(t)
    except ValueError:
        return 0


def read_all_agencies(page):
    """Read the full agency × offense table. Returns {agency_name: {offense: count}}."""
    # Row labels = agency names
    agencies = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    # Col labels = offense names
    offenses = page.evaluate("""
    () => {
        var t = document.getElementById('headerRowTable');
        if (!t || t.rows.length < 1) return null;
        // Last row has the offense names
        var lastRow = t.rows[t.rows.length - 1];
        return Array.from(lastRow.cells).map(c => c.innerText.trim());
    }
    """)
    # Body data
    body = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r =>
            Array.from(r.cells).map(c => c.innerText.trim())
        );
    }
    """)
    if not agencies or not offenses or not body:
        return None

    # Build col index map
    col_map = {}
    for i, label in enumerate(offenses):
        rtci = OFFENSE_MAP.get(label)
        if rtci:
            col_map[i] = rtci

    result = {}
    for row_idx, agency_name in enumerate(agencies):
        if agency_name not in AGENCIES:
            continue
        if row_idx >= len(body):
            break
        crimes = {}
        for col_idx, rtci in col_map.items():
            if col_idx < len(body[row_idx]):
                crimes[rtci] = parse_count(body[row_idx][col_idx])
        result[agency_name] = crimes

    return result


def main():
    window = nine_month_window()
    months_by_year = {}
    for y, m in window:
        months_by_year.setdefault(y, []).append(m)
    print(f"9-month window: {window}")
    print(f"{len(AGENCIES)} CT agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        # Establish session
        page.goto(ENTRY_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(1000)

        for year, month_list in sorted(months_by_year.items()):
            print(f"Year {year}...")

            for mo in month_list:
                month_name = MONTH_NAMES[mo - 1]
                print(f"  {month_name} {year}...", end=" ", flush=True)

                # Load report fresh each month
                page.goto(REPORT_URL, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)

                # Set year
                if not nav_select(page, DIM_DATE, str(year), str(year)):
                    print("FAILED year select")
                    continue

                # Set month
                if not nav_select(page, DIM_MONTH, month_name, month_name):
                    print("FAILED month select")
                    continue

                # Read all agencies at once
                data = read_all_agencies(page)
                if not data:
                    print("no data")
                    continue

                count = 0
                for agency_name, crimes in data.items():
                    for offense, val in crimes.items():
                        all_rows.append({
                            "agency": agency_name,
                            "state": STATE,
                            "type": AGENCIES[agency_name],
                            "year": year,
                            "month": mo,
                            "offense": offense,
                            "count": val,
                        })
                        count += 1

                print(f"{len(data)} agencies, {count} records")

        browser.close()

    if not all_rows:
        print("\nNo data collected.")
        return

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)
    agencies = set(r["agency"] for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(agencies)} agencies) to {OUT_JSON}")


if __name__ == "__main__":
    main()
