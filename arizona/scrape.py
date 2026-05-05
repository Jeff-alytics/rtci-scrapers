"""
Arizona Crime Scraper — Beyond 20/20 (azcrimestatistics.azdps.gov)
Report 55: "Crime Overview Trend (Combined)" — 12 months × offenses per year.
Gets full year in one table read. Aggravated Assault requires drill-down.
Outputs arizona/data/latest.json in RTCI pipeline format.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

STATE = "AZ"
REPORT_URL = "https://azcrimestatistics.azdps.gov/public/View/dispview.aspx?ReportId=55"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"
GEO_HIERARCHY = "[Summary Jurisdiction by County].[Summary Jurisdiction by County Hierarchy]"

AGENCIES = [
    {"ori": "AZ0070100", "name": "Avondale", "type": "City"},
    {"ori": "AZ0070300", "name": "Buckeye", "type": "City"},
    {"ori": "AZ0110100", "name": "Casa Grande", "type": "City"},
    {"ori": "AZ0070500", "name": "Chandler", "type": "City"},
    {"ori": "AZ0030100", "name": "Flagstaff", "type": "City"},
    {"ori": "AZ0071100", "name": "Gilbert", "type": "City"},
    {"ori": "AZ0071300", "name": "Glendale", "type": "City"},
    {"ori": "AZ0071500", "name": "Goodyear", "type": "City"},
    {"ori": "AZ0080400", "name": "Lake Havasu City", "type": "City"},
    {"ori": "AZ0100900", "name": "Marana", "type": "City"},
    {"ori": "AZ0111700", "name": "Maricopa", "type": "City"},
    {"ori": "AZ0071700", "name": "Mesa", "type": "City"},
    {"ori": "AZ0072100", "name": "Peoria", "type": "City"},
    {"ori": "AZ0072300", "name": "Phoenix", "type": "City"},
    {"ori": "AZ0100000", "name": "Pima", "type": "County"},
    {"ori": "AZ0110000", "name": "Pinal", "type": "County"},
    {"ori": "AZ0131100", "name": "Prescott Valley", "type": "City"},
    {"ori": "AZ0072500", "name": "Scottsdale", "type": "City"},
    {"ori": "AZ0072700", "name": "Surprise", "type": "City"},
    {"ori": "AZ0072900", "name": "Tempe", "type": "City"},
    {"ori": "AZ0100300", "name": "Tucson", "type": "City"},
    {"ori": "AZ0130000", "name": "Yavapai", "type": "County"},
    {"ori": "AZ0140500", "name": "Yuma", "type": "City"},
]

OFFENSE_MAP = {
    "Criminal Homicide": "Murder",
    "Forcible Rape Total": "Rape",
    "Robbery Total": "Robbery",
    "Burglary Total": "Burglary",
    "Larceny - Theft Total": "Theft",
    "Motor Vehicle Theft Total": "Motor Vehicle Theft",
}


def nine_month_window():
    now = datetime.now().replace(day=1)
    months = set()
    y, m = now.year, now.month - 1
    if m == 0:
        y, m = y - 1, 12
    for _ in range(9):
        months.add((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return months


def needed_years(window):
    return sorted(set(y for y, m in window))


def geo_path(ori):
    county = ori[2:5]
    return f"{GEO_HIERARCHY}.[State].&[AZ].&[{county}].&[{ori}]"


def setup_view(page, agency_path, year):
    page.evaluate("""
    (p) => {
        var f = document.getElementById('aspnetForm');
        f.querySelector('[name="Rows"]').value = '4';
        f.querySelector('[name="Cols"]').value = '6';
        f.querySelector('[name="Others"]').value = '2,3,5';
        f.querySelector('[name="OtherDimCount"]').value = '3';
        f.querySelector('[name="Chk5"]').value = p.path + '\\x1c0\\x1c';
        f.querySelector('[name="ActiveMember5"]').value = p.path;
        f.querySelector('[name="ActiveMember3"]').value =
            '[Return A Date].[Return A Date Hierarchy].[Year].&[' + p.year + ']';
    }
    """, {"path": agency_path, "year": str(year)})
    with page.expect_navigation(wait_until="networkidle", timeout=30000):
        page.evaluate("() => document.getElementById('aspnetForm').submit()")


def change_year(page, year):
    page.evaluate("""
    (y) => {
        document.getElementById('aspnetForm')
            .querySelector('[name="ActiveMember3"]').value =
            '[Return A Date].[Return A Date Hierarchy].[Year].&[' + y + ']';
    }
    """, str(year))
    with page.expect_navigation(wait_until="networkidle", timeout=30000):
        page.evaluate("() => document.getElementById('aspnetForm').submit()")


def expand_table(page):
    link = page.locator("a:has-text('All Summary Offenses')").first
    if link.count() == 0:
        return False
    with page.expect_navigation(wait_until="networkidle", timeout=30000):
        link.click()
    link2 = page.locator("a:has-text('All Summary Months')").first
    if link2.count() == 0:
        return False
    with page.expect_navigation(wait_until="networkidle", timeout=30000):
        link2.click()
    return True


def is_expanded(page):
    return page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        return !!(t && t.rows.length >= 7 && t.rows[0] && t.rows[0].cells.length >= 12);
    }
    """)


def parse_table(page):
    row_names = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0].innerText.trim());
    }
    """)
    data_grid = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r =>
            Array.from(r.cells).map(c => {
                var t = c.innerText.trim().replace(/,/g, '');
                if (!t || t === '\\u00a0' || t === '.' || t === '-') return null;
                var n = parseInt(t, 10);
                return isNaN(n) ? null : n;
            })
        );
    }
    """)
    if not row_names or not data_grid:
        return None
    rows = []
    for idx, name in enumerate(row_names):
        rtci = OFFENSE_MAP.get(name)
        if not rtci or idx >= len(data_grid):
            continue
        for m_idx, val in enumerate(data_grid[idx][:12]):
            rows.append({"offense": rtci, "month": m_idx + 1, "value": val})
    return rows


def parse_agg_assault(page):
    if not page.locator("a:has-text('Assault Total')").count():
        return []
    with page.expect_navigation(wait_until="networkidle", timeout=30000):
        page.locator("a:has-text('Assault Total')").first.click()
    values = page.evaluate("""
    () => {
        var hdr = document.getElementById('headerColumnTable');
        var body = document.getElementById('bodyTable');
        if (!hdr || !body) return null;
        var rows = Array.from(hdr.rows);
        for (var i = 0; i < rows.length; i++) {
            if (rows[i].cells[0].innerText.trim() === 'Aggravated Assault Total') {
                return Array.from(body.rows[i].cells).map(function(c) {
                    var t = c.innerText.trim().replace(/,/g, '');
                    if (!t || t === '\\u00a0' || t === '.' || t === '-') return null;
                    var n = parseInt(t, 10);
                    return isNaN(n) ? null : n;
                });
            }
        }
        return null;
    }
    """)
    page.go_back(wait_until="networkidle", timeout=15000)
    if not values:
        return []
    return [{"offense": "Aggravated Assault", "month": m + 1, "value": values[m]}
            for m in range(min(12, len(values)))]


def main():
    window = nine_month_window()
    years = needed_years(window)
    print(f"9-month window: {sorted(window)}")
    print(f"Years to fetch: {years}")
    print(f"{len(AGENCIES)} AZ agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        for ag in AGENCIES:
            path = geo_path(ag["ori"])
            print(f"{ag['name']}...", flush=True)
            page.goto(REPORT_URL, wait_until="networkidle", timeout=30000)

            for i, year in enumerate(years):
                print(f"  {year}...", end=" ", flush=True)
                try:
                    if i == 0:
                        setup_view(page, path, year)
                        if not expand_table(page):
                            print("expand failed")
                            continue
                    else:
                        change_year(page, year)
                        if not is_expanded(page) and not expand_table(page):
                            print("expand failed")
                            continue

                    rows = parse_table(page)
                    agg = parse_agg_assault(page)
                    if agg:
                        rows = (rows or []) + agg
                except Exception as e:
                    print(f"ERROR: {e}")
                    continue

                if not rows:
                    print("no data")
                    continue

                count = 0
                for r in rows:
                    if (year, r["month"]) in window:
                        all_rows.append({
                            "agency": ag["name"], "state": STATE, "type": ag["type"],
                            "year": year, "month": r["month"],
                            "offense": r["offense"], "count": r["value"] if r["value"] is not None else 0,
                        })
                        count += 1
                print(f"{count} records")

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
