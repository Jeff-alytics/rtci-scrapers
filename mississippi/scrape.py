"""
Mississippi Crime Scraper — DPS NIBRS via Beyond 20/20 Report 74.
Source: mscrimestats.dps.ms.gov

Single agency (Southaven PD). Uses B2020 dimensional viewer:
Rows=Geography, Cols=Incident Month, Others=Year+Offense+Measures.
Iterates over offenses and years. ~14 reads per year.
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATE = "MS"
REPORT_URL = "https://mscrimestats.dps.ms.gov/public/View/dispview.aspx?ReportId=74"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

# B2020 dimension numbers for this report
DIM_DATE = 5
DIM_MONTH = 7
DIM_GEO = 8
DIM_MEASURES = 12
DIM_OFFENSE = 20

AGENCIES = [
    {"name": "Southaven", "type": "City", "b2020": "Southaven Police Department", "search": "Southaven"},
]

OFFENSE_MAP = {
    "Murder and Nonnegligent Manslaughter": "Murder",
    "All Rape": "Rape",
    "Aggravated Assault": "Aggravated Assault",
    "Robbery": "Robbery",
    "Burglary/Breaking & Entering": "Burglary",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

THEFT_OFFENSES = [
    "Pocket-picking",
    "Purse-snatching",
    "Shoplifting",
    "Theft From Building",
    "Theft From Coin Operated Machine or Device",
    "Theft From Motor Vehicle",
    "Theft of Motor Vehicle Parts/Accessories",
    "All Other Larceny",
]

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


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
    return sorted(set(y for y, _ in window))


# ── B2020 helpers ──────────────────────────────────────────────────────

def show_dim(page, dim_num):
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            page.evaluate(f"ShowDim({dim_num}, {dim_num});")
        page.wait_for_timeout(1500)
    except Exception:
        page.wait_for_timeout(3000)


def select_none(page):
    page.evaluate("OnSelAllClick(0);")
    page.wait_for_timeout(300)


def select_all(page):
    page.evaluate("OnSelAllClick(1);")
    page.wait_for_timeout(300)


def search_and_select(page, search_term, exact_name):
    try:
        page.fill('input[name="SearchString"]', search_term)
        with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
            page.evaluate(
                'MembersSearch(document.querySelector(\'input[name="SearchString"]\'));'
            )
        page.wait_for_timeout(400)
    except Exception:
        return False
    return page.evaluate("""
    (name) => {
        var spans = Array.from(document.querySelectorAll('span.rtIn'));
        var nl = name.toLowerCase();
        for (var span of spans) {
            if (span.textContent.trim().toLowerCase() === nl) {
                var chk = span.closest('label')?.querySelector('input.rtChk');
                if (chk) { chk.click(); return true; }
            }
        }
        return false;
    }
    """, exact_name)


def apply_selection(page):
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
            page.evaluate("ShowUpdatedReportAfterSelection();")
        page.wait_for_timeout(2000)
        return True
    except Exception:
        page.wait_for_timeout(5000)
        return page.evaluate("() => !!document.getElementById('aspnetForm')")


def swap_layout(page):
    layout = page.evaluate("""
    () => {
        var f = document.getElementById('aspnetForm');
        if (!f) return null;
        return f.querySelector('[name="Cols"]')?.value;
    }
    """)
    if layout == str(DIM_MONTH):
        return
    page.evaluate("""
    (cfg) => {
        var f = document.getElementById('aspnetForm');
        if (!f) return;
        function setField(name, val) {
            var el = f.querySelector('[name="' + name + '"]');
            if (!el) { el = document.createElement('input'); el.type = 'hidden'; el.name = name; f.appendChild(el); }
            el.value = val;
        }
        setField('Rows', cfg.geo);
        setField('Cols', cfg.month);
        setField('Others', cfg.date + ',' + cfg.offense + ',' + cfg.measures);
        setField('OtherDimCount', '3');
        setField('TableRowPageSize', '200');
    }
    """, {"geo": str(DIM_GEO), "month": str(DIM_MONTH), "date": str(DIM_DATE),
          "offense": str(DIM_OFFENSE), "measures": str(DIM_MEASURES)})
    with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
        page.evaluate("() => document.getElementById('aspnetForm').submit()")
    page.wait_for_timeout(2000)


def read_table(page):
    month_cols = page.evaluate("""
    () => {
        var t = document.getElementById('headerRowTable');
        if (!t || t.rows.length < 1) return null;
        return Array.from(t.rows[0].cells).map(c => c.innerText.trim());
    }
    """)
    row_names = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => {
            var cell = r.cells[0];
            if (!cell) return '';
            var a = cell.querySelector('a');
            return (a ? a.title || a.innerText.trim() : cell.innerText.trim());
        });
    }
    """)
    data = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r =>
            Array.from(r.cells).map(c => c.innerText.trim())
        );
    }
    """)
    return row_names, month_cols, data


def parse_count(text):
    t = text.strip().replace(",", "").replace("\xa0", "")
    if not t or t in (".", "-", "*", " "):
        return 0
    try:
        return int(t)
    except ValueError:
        return 0


def match_row(row_names, agency_b2020):
    prefix = agency_b2020.lower()[:20]
    for i, name in enumerate(row_names or []):
        if name.lower().rstrip(".").startswith(prefix) or agency_b2020.lower().startswith(name.lower().rstrip(".")):
            return i
    return None


# ── Main ───────────────────────────────────────────────────────────────

def main():
    window = nine_month_window()
    years = needed_years(window)
    print(f"9-month window: {sorted(window)}")
    print(f"Years: {years}, {len(AGENCIES)} MS agencies\n")

    all_rows = []
    theft_accum = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        print("Loading Report 74...", flush=True)
        for attempt in range(3):
            try:
                page.goto(REPORT_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(4000)
                if page.evaluate("() => typeof ShowDim !== 'undefined'"):
                    break
            except Exception as e:
                if attempt == 2:
                    raise

        # Setup dimensions
        print("Setting up dimensions...", flush=True)

        show_dim(page, DIM_DATE)
        select_none(page)
        search_and_select(page, str(years[0]), str(years[0]))
        apply_selection(page)
        print(f"  Year: {years[0]}")

        show_dim(page, DIM_MEASURES)
        select_none(page)
        search_and_select(page, "Number of Crimes", "Number of Crimes")
        apply_selection(page)
        print("  Measures: Number of Crimes")

        show_dim(page, DIM_MONTH)
        select_all(page)
        apply_selection(page)
        print("  Incident Month: all")

        show_dim(page, DIM_GEO)
        select_none(page)
        for ag in AGENCIES:
            ok = search_and_select(page, ag["search"], ag["b2020"])
            print(f"  Agency {ag['b2020']}: {'OK' if ok else 'FAILED'}")
        apply_selection(page)

        first_offense = list(OFFENSE_MAP.keys())[0]
        show_dim(page, DIM_OFFENSE)
        select_none(page)
        search_and_select(page, first_offense.split(" and ")[0][:15], first_offense)
        apply_selection(page)

        swap_layout(page)
        print("  Layout: Rows=Geography, Cols=Month\n")

        for year in years:
            if year != years[0]:
                print(f"Switching to year {year}...", flush=True)
                show_dim(page, DIM_DATE)
                select_none(page)
                search_and_select(page, str(year), str(year))
                apply_selection(page)
                swap_layout(page)

            print(f"Year {year}:")

            for nibrs_name, rtci_name in OFFENSE_MAP.items():
                search_term = nibrs_name.split(" and ")[0].split("/")[0][:20]
                show_dim(page, DIM_OFFENSE)
                select_none(page)
                search_and_select(page, search_term, nibrs_name)
                apply_selection(page)
                swap_layout(page)

                row_names, month_cols, data = read_table(page)
                if not row_names or not data:
                    print(f"  {rtci_name}: no data")
                    continue

                month_idx = {}
                for ci, col_name in enumerate(month_cols or []):
                    if col_name in MONTH_NAMES:
                        month_idx[MONTH_NAMES.index(col_name) + 1] = ci

                count = 0
                for ag in AGENCIES:
                    ri = match_row(row_names, ag["b2020"])
                    if ri is None or ri >= len(data):
                        continue
                    for mo, ci in month_idx.items():
                        if (year, mo) not in window:
                            continue
                        val = parse_count(data[ri][ci]) if ci < len(data[ri]) else 0
                        all_rows.append({
                            "agency": ag["name"], "state": STATE, "type": ag["type"],
                            "year": year, "month": mo, "offense": rtci_name, "count": val,
                        })
                        count += 1
                print(f"  {rtci_name}: {count} records")

            for theft_offense in THEFT_OFFENSES:
                search_term = theft_offense.split(" From ")[0].split(" of ")[0].split("/")[0][:20]
                show_dim(page, DIM_OFFENSE)
                select_none(page)
                search_and_select(page, search_term, theft_offense)
                apply_selection(page)
                swap_layout(page)

                row_names, month_cols, data = read_table(page)
                if not row_names or not data:
                    continue

                month_idx = {}
                for ci, col_name in enumerate(month_cols or []):
                    if col_name in MONTH_NAMES:
                        month_idx[MONTH_NAMES.index(col_name) + 1] = ci

                for ag in AGENCIES:
                    ri = match_row(row_names, ag["b2020"])
                    if ri is None or ri >= len(data):
                        continue
                    for mo, ci in month_idx.items():
                        if (year, mo) not in window:
                            continue
                        val = parse_count(data[ri][ci]) if ci < len(data[ri]) else 0
                        key = (ag["name"], year, mo)
                        theft_accum[key] = theft_accum.get(key, 0) + val

                print(f"  Theft/{theft_offense}: done")

        browser.close()

    for (agency_name, year, month), total in theft_accum.items():
        ag = next(a for a in AGENCIES if a["name"] == agency_name)
        all_rows.append({
            "agency": agency_name, "state": STATE, "type": ag["type"],
            "year": year, "month": month, "offense": "Theft", "count": total,
        })

    if not all_rows:
        print("No data collected.")
        return

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)
    agencies = set(r["agency"] for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(agencies)} agencies) to {OUT_JSON}")


if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"Elapsed: {time.time() - t0:.0f}s ({(time.time() - t0) / 60:.1f}m)")
