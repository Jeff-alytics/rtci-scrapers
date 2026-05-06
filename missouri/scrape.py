"""
Missouri SRS Crime Scraper — B2020 Report 1 (showmecrime.mo.gov)
SRS format: offenses in rows, measures in cols. Per-agency, per-year.
ShowDim(6) for Summary Month gives 12-month grid per year.
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
REPORT_URL = "https://showmecrime.mo.gov/public/View/dispview.aspx?ReportId=1"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

# Report 1 dims: 3=Summary Date(year), 5=Jurisdiction, 6=Summary Month, 4=Summary Offense
DIM_DATE = 3
DIM_JURISDICTION = 5
DIM_MONTH = 6

MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

OFFENSE_MAP = {
    # "Criminal Homicide" includes Negligent Manslaughter — must drill down
    # "Murder and Nonnegligent Homicide": "Murder",  (handled by drill-down)
    "Forcible Rape Total": "Rape",
    "Robbery Total": "Robbery",
    # "Assault Total" includes Simple — must drill down for Aggravated
    # "Aggravated Assault Total": "Aggravated Assault",  (handled by drill-down)
    "Burglary Total": "Burglary",
    "Larceny - Theft Total": "Theft",
    "Motor Vehicle Theft Total": "Motor Vehicle Theft",
}

SPECIAL_COUNTY = {"MOKPD0000": "048", "MOSPD0000": "119"}
GEO_HIERARCHY = "[Summary Jurisdiction by County].[Summary Jurisdiction by County Hierarchy]"

# B2020 display names differ from short names for St. agencies
AGENCY_B2020_MAP = {
    "St Louis|City": "St Louis Police Department",
    "St Joseph|City": "St Joseph Police Department",
    "St Charles|City": "St Charles Police Department",
    "St Peters|City": "St Peters Police Department",
    "St Charles|County": "St Charles County Police Department",
    "St Louis|County": "St Louis County Police Department",
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


def geo_path(ori):
    county = SPECIAL_COUNTY.get(ori, ori[2:5])
    return f"{GEO_HIERARCHY}.[State].&[MO].&[{county}].&[{ori}]"


def b2020_name(ag):
    key = f"{ag['name']}|{ag['type']}"
    return AGENCY_B2020_MAP.get(key, ag["name"])


def parse_count(text):
    t = str(text).strip().replace(",", "").replace("\xa0", "")
    if not t or t in (".", "-", "*", " "):
        return 0
    try:
        return int(t)
    except ValueError:
        return 0


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


def setup_agency(page, agency):
    """Select agency via form POST using geo path (faster than ShowDim navigation)."""
    path = geo_path(agency["ori"])
    page.evaluate("""
    (p) => {
        var f = document.getElementById('aspnetForm');
        f.querySelector('[name="Chk5"]').value = p + '\\x1c0\\x1c';
        f.querySelector('[name="ActiveMember5"]').value = p;
    }
    """, path)
    with page.expect_navigation(wait_until="networkidle", timeout=30000):
        page.evaluate("() => document.getElementById('aspnetForm').submit()")
    page.wait_for_timeout(1000)


def drill_homicide(page):
    """Drill into 'Criminal Homicide' to get 'Murder and Nonnegligent Homicide'."""
    link = page.locator("a:has-text('Criminal Homicide')").first
    if not link.count():
        return None
    try:
        with page.expect_navigation(wait_until="networkidle", timeout=30000):
            link.click()
        page.wait_for_timeout(1500)
    except Exception:
        return None

    offenses = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return [];
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    body = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return [];
        return Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim()));
    }
    """)

    val = None
    for i, name in enumerate(offenses):
        if "Murder" in name and "Nonnegligent" in name and i < len(body) and body[i]:
            val = parse_count(body[i][0])
            break

    try:
        page.go_back(wait_until="networkidle", timeout=15000)
        page.wait_for_timeout(1000)
    except Exception:
        pass

    return val


def drill_agg_assault(page):
    """Drill into 'Assault Total' to get 'Aggravated Assault Total'."""
    link = page.locator("a:has-text('Assault Total')").first
    if not link.count():
        return None
    try:
        with page.expect_navigation(wait_until="networkidle", timeout=30000):
            link.click()
        page.wait_for_timeout(1500)
    except Exception:
        return None

    offenses = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return [];
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    body = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return [];
        return Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim()));
    }
    """)

    val = None
    for i, name in enumerate(offenses):
        if name == "Aggravated Assault Total" and i < len(body) and body[i]:
            val = parse_count(body[i][0])
            break

    # Go back to main table
    try:
        page.go_back(wait_until="networkidle", timeout=15000)
        page.wait_for_timeout(1000)
    except Exception:
        pass

    return val


def read_table(page):
    """Read offense rows for the current agency/year/month. Returns {offense: count}."""
    offenses = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    body = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r =>
            Array.from(r.cells).map(c => c.innerText.trim())
        );
    }
    """)
    if not offenses or not body:
        return None

    result = {}
    for row_idx, offense_name in enumerate(offenses):
        rtci = OFFENSE_MAP.get(offense_name)
        if not rtci or row_idx >= len(body):
            continue
        val = parse_count(body[row_idx][0]) if body[row_idx] else 0
        result[rtci] = val

    # Drill into Criminal Homicide to get Murder only (exclude Negligent Manslaughter)
    murder = drill_homicide(page)
    if murder is not None:
        result["Murder"] = murder

    # Drill into Assault Total to get Aggravated Assault
    agg = drill_agg_assault(page)
    if agg is not None:
        result["Aggravated Assault"] = agg

    return result


def main():
    window = nine_month_window()
    years = needed_years(window)
    months_by_year = {}
    for y, m in window:
        months_by_year.setdefault(y, []).append(m)

    print(f"9-month window: {sorted(window)}")
    print(f"Years: {years}, {len(AGENCIES)} MO agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        for ag in AGENCIES:
            search_name = b2020_name(ag)
            print(f"{ag['name']} ({ag['type']})...", flush=True)

            if not load_report(page):
                print("  FAILED to load report")
                continue

            # Select agency
            try:
                setup_agency(page, ag)
            except Exception as e:
                print(f"  FAILED agency setup: {e}")
                # Fallback: try ShowDim navigation
                if not load_report(page):
                    continue
                if not nav_select(page, DIM_JURISDICTION, search_name, search_name):
                    print(f"  FAILED agency select via ShowDim")
                    continue

            current_year = None
            for year in years:
                month_list = months_by_year.get(year, [])
                if not month_list:
                    continue

                # Select year
                if year != current_year:
                    if not nav_select(page, DIM_DATE, str(year), str(year)):
                        print(f"  FAILED year {year}")
                        continue
                    current_year = year

                # Iterate months
                for mo in month_list:
                    month_name = MONTH_NAMES[mo - 1]
                    ok = nav_select(page, DIM_MONTH, month_name, month_name)
                    if not ok:
                        continue

                    data = read_table(page)
                    if not data:
                        continue

                    for offense, val in data.items():
                        all_rows.append({
                            "agency": ag["name"],
                            "state": STATE,
                            "type": ag["type"],
                            "year": year,
                            "month": mo,
                            "offense": offense,
                            "count": val,
                        })

                count = len([r for r in all_rows if r["agency"] == ag["name"] and r["year"] == year])
                print(f"  {year}: {count} records")

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
