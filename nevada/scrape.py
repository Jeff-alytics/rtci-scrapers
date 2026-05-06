"""
Nevada Crime Scraper — nevadacrimestats.nv.gov B2020 (Summary UCR / SRS)
Report 17: months in rows, offenses in columns. 12 months in ONE read per year.
Outputs nevada/data/latest.json in RTCI pipeline format.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

STATE = "NV"
REPORT_URL = "https://nevadacrimestats.nv.gov/public/View/dispview.aspx?ReportId=17"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

DIM_DATE = 3
DIM_OFFENSE = 4
DIM_JURISDICTION = 5

MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

TARGET_OFFENSES = [
    "Murder and Nonnegligent Homicide", "Forcible Rape Total", "Robbery Total",
    "Aggravated Assault Total", "Burglary Total", "Larceny - Theft Total",
    "Motor Vehicle Theft Total",
]

OFFENSE_MAP = {
    "Murder and Nonnegligent Homicide": "Murder",
    "Forcible Rape Total": "Rape",
    "Robbery Total": "Robbery",
    "Aggravated Assault Total": "Aggravated Assault",
    "Burglary Total": "Burglary",
    "Larceny - Theft Total": "Theft",
    "Motor Vehicle Theft Total": "Motor Vehicle Theft",
}

AGENCIES = [
    {"ori": "NV0020300", "name": "Henderson", "type": "City", "b2020": "Henderson Police Department"},
    # Las Vegas scraped separately via LVMPD ArcGIS API in pipeline
    {"ori": "NV0020200", "name": "North Las Vegas", "type": "City", "b2020": "North Las Vegas Police Department"},
    {"ori": "NV0160100", "name": "Reno", "type": "City", "b2020": "Reno Police Department"},
    {"ori": "NV0160200", "name": "Sparks", "type": "City", "b2020": "Sparks Police Department"},
    {"ori": "NV0160000", "name": "Washoe", "type": "County", "b2020": "Washoe County Sheriff's Office"},
]


def nine_month_window():
    now = datetime.now().replace(day=1)
    months = set()
    y, m = now.year, now.month - 1
    if m == 0: y, m = y - 1, 12
    for _ in range(9):
        months.add((y, m))
        m -= 1
        if m == 0: y, m = y - 1, 12
    return months


def needed_years(window):
    return sorted(set(y for y, m in window))


def parse_count(text):
    t = str(text).strip().replace(",", "").replace("\xa0", "")
    if not t or t in (".", "-", "*", " "):
        return None
    try:
        return int(t)
    except ValueError:
        return None


def load_report(page):
    for attempt in range(3):
        try:
            page.goto(REPORT_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            if page.evaluate("() => typeof ShowDim !== 'undefined'"):
                return True
        except Exception as e:
            print(f"    Load attempt {attempt+1}: {e}")
    return False


def nav_select_single(page, dim_num, search_term, member_name):
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
                if (!label) return 'no';
                var chk = label.querySelector('input.rtChk');
                if (!chk) return 'no';
                chk.click(); return 'ok';
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


def nav_select_offenses(page, offense_names):
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            page.evaluate(f"ShowDim({DIM_OFFENSE}, {DIM_OFFENSE});")
        page.wait_for_timeout(800)
    except Exception:
        return False
    page.evaluate("OnSelAllClick(0);")
    page.wait_for_timeout(300)
    for offense in offense_names:
        try:
            page.fill('input[name="SearchString"]', offense)
            with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                page.evaluate('MembersSearch(document.querySelector(\'input[name="SearchString"]\'));')
            page.wait_for_timeout(400)
        except Exception:
            continue
        page.evaluate("""
        (name) => {
            var spans = Array.from(document.querySelectorAll('span.rtIn'));
            var nl = name.toLowerCase();
            for (var span of spans) {
                var t = span.textContent.trim().toLowerCase();
                if (t === nl || t.startsWith(nl)) {
                    var label = span.closest('label');
                    if (label) { var chk = label.querySelector('input.rtChk'); if (chk) chk.click(); }
                    break;
                }
            }
        }
        """, offense)
        page.wait_for_timeout(150)
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            page.evaluate("ShowUpdatedReportAfterSelection();")
        page.wait_for_timeout(1500)
        return True
    except Exception:
        return False


def read_table(page):
    month_names = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    offense_cols = page.evaluate("""
    () => {
        var t = document.getElementById('headerRowTable');
        if (!t || t.rows.length < 2) return null;
        return Array.from(t.rows[1].cells).map(c => c.innerText.trim());
    }
    """)
    body = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim()));
    }
    """)
    if not month_names or not offense_cols or not body:
        return None
    col_idx = {}
    for i, name in enumerate(offense_cols):
        rtci = OFFENSE_MAP.get(name)
        if rtci and rtci not in col_idx:
            col_idx[rtci] = i
    return month_names, col_idx, body


def main():
    window = nine_month_window()
    years = needed_years(window)
    print(f"9-month window: {sorted(window)}")
    print(f"Years: {years}, {len(AGENCIES)} NV agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        for ag in AGENCIES:
            print(f"{ag['name']}...", flush=True)
            if not load_report(page):
                print("  FAILED to load")
                continue

            b2020 = ag.get("b2020", ag["name"])
            search = b2020.split()[0]
            if not nav_select_single(page, DIM_JURISDICTION, search, b2020):
                print("  FAILED agency select")
                continue
            if not nav_select_offenses(page, TARGET_OFFENSES):
                print("  FAILED offense select")
                continue

            current_year = None
            for year in years:
                if year != current_year:
                    if not nav_select_single(page, DIM_DATE, str(year), str(year)):
                        print(f"  FAILED year {year}")
                        continue
                    current_year = year

                result = read_table(page)
                if not result:
                    print(f"  {year}: no table")
                    continue

                month_names, col_idx, body = result
                count = 0
                for row_idx, row in enumerate(body):
                    if row_idx >= len(month_names):
                        break
                    mn = month_names[row_idx]
                    if mn not in MONTH_NAMES:
                        continue
                    mo = MONTH_NAMES.index(mn) + 1
                    if (year, mo) not in window:
                        continue
                    for rtci, ci in col_idx.items():
                        val = parse_count(row[ci]) if ci < len(row) else None
                        all_rows.append({
                            "agency": ag["name"], "state": STATE, "type": ag["type"],
                            "year": year, "month": mo, "offense": rtci,
                            "count": val if val is not None else 0,
                        })
                        count += 1
                print(f"  {year}: {count} records")

        browser.close()

    if not all_rows:
        print("No data collected.")
        return
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)
    agencies = set(r["agency"] for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(agencies)} agencies) to {OUT_JSON}")


if __name__ == "__main__":
    main()
