"""
Missouri Crime Scraper — ShowMeCrime B2020 (showmecrime.mo.gov)
Report 20: per-month navigation via ShowDim (agency + year + month).
Outputs missouri/data/latest.json in RTCI pipeline format.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

STATE = "MO"
REPORT_URL = "https://showmecrime.mo.gov/public/View/dispview.aspx?ReportId=20"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

DIM_INCIDENT_DATE = 6
DIM_INCIDENT_MONTH = 8
DIM_JURISDICTION = 9

MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

OFFENSE_MAP = {
    "Murder and Nonnegligent Manslaughter": "Murder",
    "All Rape": "Rape",
    "Aggravated Assault": "Aggravated Assault",
    "Burglary/Breaking & Entering": "Burglary",
    "Robbery": "Robbery",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

THEFT_OFFENSES = {
    "Pocket-picking", "Purse-snatching", "Shoplifting", "Theft From Building",
    "Theft From Coin Operated Machine or Device", "Theft From Motor Vehicle",
    "Theft of Motor Vehicle Parts/Accessories", "All Other Larceny",
}

SPECIAL_COUNTY = {"MOKPD0000": "048", "MOSPD0000": "119"}

AGENCY_NAME_MAP = {
    "St Louis": "St Louis Metropolitan Police Department",
    "St Joseph": "St Joseph Police Department",
    "St Charles": "St Charles Police Department",
    "St Peters": "St Peters Police Department",
}

# County-type agencies need their B2020 display names too
AGENCY_NAME_MAP_COUNTY = {
    "St Charles": "St Charles County Police Department",
    "St Louis": "St Louis County Police Department",
}

AGENCIES = [
    {"ori": "MO0480100", "name": "Blue Springs", "type": "City"},
    {"ori": "MO0100200", "name": "Columbia", "type": "City"},
    {"ori": "MO0953000", "name": "Florissant", "type": "City"},
    {"ori": "MO0480600", "name": "Independence", "type": "City"},
    {"ori": "MO0500000", "name": "Jefferson", "type": "County"},
    {"ori": "MO0490700", "name": "Joplin", "type": "City"},
    {"ori": "MOKPD0000", "name": "Kansas City", "type": "City"},
    {"ori": "MO0480800", "name": "Lee's Summit", "type": "City"},
    {"ori": "MO0920100", "name": "O'Fallon", "type": "City"},
    {"ori": "MO0390300", "name": "Springfield", "type": "City"},
    {"ori": "MO0920300", "name": "St Charles", "type": "City"},
    {"ori": "MO0922400", "name": "St Charles", "type": "County"},
    {"ori": "MO0110100", "name": "St Joseph", "type": "City"},
    {"ori": "MOSPD0000", "name": "St Louis", "type": "City"},
    {"ori": "MO0950000", "name": "St Louis", "type": "County"},
    {"ori": "MO0920400", "name": "St Peters", "type": "City"},
]


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


def b2020_name(ag):
    if ag["type"] == "County":
        return AGENCY_NAME_MAP_COUNTY.get(ag["name"], ag["name"] + " County Sheriff's Office")
    return AGENCY_NAME_MAP.get(ag["name"], ag["name"] + " Police Department")


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


def nav_select(page, dim_num, search_term, member_name):
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
        for (var span of spans) {
            if (span.textContent.trim() === name) {
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


def read_table(page):
    names = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    vals = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    if not names or not vals:
        return None
    return dict(zip(names, vals))


def build_rows(ag, year, month, raw_counts):
    rows = []
    for name, rtci in OFFENSE_MAP.items():
        val = parse_count(raw_counts.get(name, ""))
        rows.append({"agency": ag["name"], "state": STATE, "type": ag["type"],
                      "year": year, "month": month, "offense": rtci, "count": val or 0})
    theft = sum(parse_count(raw_counts.get(n, "")) or 0 for n in THEFT_OFFENSES)
    rows.append({"agency": ag["name"], "state": STATE, "type": ag["type"],
                  "year": year, "month": month, "offense": "Theft", "count": theft})
    return rows


def main():
    months = nine_month_window()
    months_by_year = {}
    for y, m in months:
        months_by_year.setdefault(y, []).append(m)
    print(f"9-month window: {months}")
    print(f"{len(AGENCIES)} MO agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        for ag in AGENCIES:
            search_name = b2020_name(ag)
            print(f"{ag['name']} ({ag['type']})...", flush=True)

            if not load_report(page):
                print("  FAILED to load")
                continue

            if not nav_select(page, DIM_JURISDICTION, search_name, search_name):
                print(f"  FAILED agency select: {search_name}")
                continue

            current_year = 2025  # default

            for year, month_list in sorted(months_by_year.items()):
                if year != current_year:
                    # Clear time series before year switch
                    page.evaluate("""
                    () => {
                        var el = document.querySelector('[name="TimeSeriesSelections6"]');
                        if (el) el.value = '';
                        el = document.querySelector('[name="TimeSetNames6"]');
                        if (el) el.value = '';
                    }
                    """)
                    if not nav_select(page, DIM_INCIDENT_DATE, str(year), str(year)):
                        print(f"  FAILED year {year}")
                        continue
                    current_year = year

                count = 0
                for mo in month_list:
                    month_name = MONTH_NAMES[mo - 1]
                    ok = nav_select(page, DIM_INCIDENT_MONTH, month_name, month_name)
                    if not ok:
                        # Recovery: reload + re-select agency + year
                        if load_report(page) and nav_select(page, DIM_JURISDICTION, search_name, search_name):
                            if year != 2025:
                                page.evaluate("""() => { var el = document.querySelector('[name="TimeSeriesSelections6"]'); if (el) el.value = ''; el = document.querySelector('[name="TimeSetNames6"]'); if (el) el.value = ''; }""")
                                nav_select(page, DIM_INCIDENT_DATE, str(year), str(year))
                            ok = nav_select(page, DIM_INCIDENT_MONTH, month_name, month_name)
                        if not ok:
                            continue

                    raw = read_table(page)
                    if not raw or not any(parse_count(v) is not None for v in raw.values()):
                        continue

                    rows = build_rows(ag, year, mo, raw)
                    all_rows.extend(rows)
                    count += len(rows)

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
