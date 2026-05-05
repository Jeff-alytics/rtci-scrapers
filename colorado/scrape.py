"""
Colorado Crime Scraper — CBI NIBRS via Beyond 20/20 Report 98.
Source: coloradocrimestats.state.co.us

Uses the B2020 dimensional viewer to read per-agency, per-month crime counts.
Layout: Rows=Geography (agencies), Cols=Incident Month, Others=Year+Offense+Measures.
Iterates over offenses and years, reading all 24 agencies × 12 months per table load.
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATE = "CO"
REPORT_URL = "https://coloradocrimestats.state.co.us/public/View/dispview.aspx?ReportId=98"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

AGENCIES = [
    {"name": "Adams", "type": "County", "b2020": "Adams County Sheriff's Office", "search": "Adams County Sheriff"},
    {"name": "Arapahoe", "type": "County", "b2020": "Arapahoe County Sheriff's Office", "search": "Arapahoe County Sheriff"},
    {"name": "Arvada", "type": "City", "b2020": "Arvada Police Department", "search": "Arvada Police"},
    {"name": "Aurora", "type": "City", "b2020": "Aurora Police Department", "search": "Aurora Police"},
    {"name": "Boulder", "type": "City", "b2020": "Boulder Police Department", "search": "Boulder Police"},
    {"name": "Broomfield", "type": "City", "b2020": "Broomfield Police Department", "search": "Broomfield Police"},
    {"name": "Castle Rock", "type": "City", "b2020": "Castle Rock Police Department", "search": "Castle Rock Police"},
    {"name": "Centennial", "type": "City", "b2020": "Centennial Police Department", "search": "Centennial Police"},
    {"name": "Colorado Springs", "type": "City", "b2020": "Colorado Springs Police Department", "search": "Colorado Springs Police"},
    {"name": "Commerce City", "type": "City", "b2020": "Commerce City Police Department", "search": "Commerce City Police"},
    {"name": "Denver", "type": "City", "b2020": "Denver Police Department", "search": "Denver Police"},
    {"name": "Douglas", "type": "County", "b2020": "Douglas County Sheriff's Office", "search": "Douglas County Sheriff"},
    {"name": "El Paso", "type": "County", "b2020": "El Paso County Sheriff's Office", "search": "El Paso County Sheriff"},
    {"name": "Fort Collins", "type": "City", "b2020": "Fort Collins Police Department", "search": "Fort Collins Police"},
    {"name": "Grand Junction", "type": "City", "b2020": "Grand Junction Police Department", "search": "Grand Junction Police"},
    {"name": "Greeley", "type": "City", "b2020": "Greeley Police Department", "search": "Greeley Police"},
    {"name": "Jefferson", "type": "County", "b2020": "Jefferson County Sheriff's Office", "search": "Jefferson County Sheriff"},
    {"name": "Lakewood", "type": "City", "b2020": "Lakewood Police Department", "search": "Lakewood Police"},
    {"name": "Longmont", "type": "City", "b2020": "Longmont Department of Public Safety", "search": "Longmont Department"},
    {"name": "Loveland", "type": "City", "b2020": "Loveland Police Department", "search": "Loveland Police"},
    {"name": "Parker", "type": "City", "b2020": "Parker Police Department", "search": "Parker Police"},
    {"name": "Pueblo", "type": "City", "b2020": "Pueblo Police Department", "search": "Pueblo Police"},
    {"name": "Thornton", "type": "City", "b2020": "Thornton Police Department", "search": "Thornton Police"},
    {"name": "Westminster", "type": "City", "b2020": "Westminster Police Department", "search": "Westminster Police"},
]

# NIBRS offenses → RTCI offense names
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
    """Open dimension selection dialog."""
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
    """Search for a member and check its checkbox."""
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
    """Apply the current dimension selection and return to the table."""
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
            page.evaluate("ShowUpdatedReportAfterSelection();")
        page.wait_for_timeout(2000)
        return True
    except Exception:
        page.wait_for_timeout(5000)
        return page.evaluate("() => !!document.getElementById('aspnetForm')")


def swap_layout(page):
    """Ensure layout is Rows=Geography(9), Cols=Month(8), Others=Year+Offense+Measures."""
    layout = page.evaluate("""
    () => {
        var f = document.getElementById('aspnetForm');
        if (!f) return null;
        return f.querySelector('[name="Cols"]')?.value;
    }
    """)
    if layout == "8":
        return  # already correct
    page.evaluate("""
    () => {
        var f = document.getElementById('aspnetForm');
        f.querySelector('[name="Rows"]').value = '9';
        f.querySelector('[name="Cols"]').value = '8';
        f.querySelector('[name="Others"]').value = '6,23,13';
        f.querySelector('[name="OtherDimCount"]').value = '3';
        f.querySelector('[name="TableRowPageSize"]').value = '200';
    }
    """)
    with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
        page.evaluate("() => document.getElementById('aspnetForm').submit()")
    page.wait_for_timeout(2000)


def read_table(page):
    """Read the B2020 table: returns (row_names, month_cols, data_grid)."""
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


# ── Name matching ──────────────────────────────────────────────────────

def build_name_map():
    """Build a map from lowercase B2020 prefix → agency dict."""
    m = {}
    for ag in AGENCIES:
        # Use first 20 chars lowercase as key (handles truncation)
        key = ag["b2020"].lower()[:20]
        m[key] = ag
    return m


def match_row_name(name, name_map):
    """Match a potentially truncated row name to an agency."""
    nl = name.lower().rstrip(".")
    for prefix, ag in name_map.items():
        if nl.startswith(prefix) or ag["b2020"].lower().startswith(nl):
            return ag
    return None


# ── Main ───────────────────────────────────────────────────────────────

def main():
    window = nine_month_window()
    years = needed_years(window)
    print(f"9-month window: {sorted(window)}")
    print(f"Years: {years}, {len(AGENCIES)} CO agencies\n")

    name_map = build_name_map()
    all_rows = []
    # theft_accum: {(agency_name, year, month): total_theft_count}
    theft_accum = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        # ── Load report ──
        print("Loading Report 98...", flush=True)
        for attempt in range(3):
            try:
                page.goto(REPORT_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(4000)
                if page.evaluate("() => typeof ShowDim !== 'undefined'"):
                    break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"  Load attempt {attempt + 1}: {e}")

        # ── Setup dimensions ──
        print("Setting up dimensions...", flush=True)

        # Year (dim 6) — start with first year
        show_dim(page, 6)
        select_none(page)
        search_and_select(page, str(years[0]), str(years[0]))
        apply_selection(page)
        print(f"  Year: {years[0]}")

        # Measures (dim 13) — Number of Crimes only
        show_dim(page, 13)
        select_none(page)
        search_and_select(page, "Number of Crimes", "Number of Crimes")
        apply_selection(page)
        print("  Measures: Number of Crimes")

        # Incident Month (dim 8) — all months
        show_dim(page, 8)
        select_all(page)
        apply_selection(page)
        print("  Incident Month: all")

        # Geography (dim 9) — select 24 target agencies
        show_dim(page, 9)
        select_none(page)
        selected = 0
        for ag in AGENCIES:
            ok = search_and_select(page, ag["search"], ag["b2020"])
            if ok:
                selected += 1
            else:
                print(f"  WARNING: could not select {ag['b2020']}")
        apply_selection(page)
        print(f"  Agencies: {selected}/{len(AGENCIES)} selected")

        # First offense (dim 23) — set initial
        first_offense = list(OFFENSE_MAP.keys())[0]
        show_dim(page, 23)
        select_none(page)
        search_and_select(page, first_offense.split(" and ")[0][:15], first_offense)
        apply_selection(page)
        print(f"  Offense: {first_offense}")

        # Swap to target layout
        swap_layout(page)
        print("  Layout: Rows=Geography, Cols=Month\n")

        # ── Scrape loop ──
        for year in years:
            if year != years[0]:
                print(f"\nSwitching to year {year}...", flush=True)
                show_dim(page, 6)
                select_none(page)
                search_and_select(page, str(year), str(year))
                apply_selection(page)
                swap_layout(page)

            print(f"Year {year}:")

            # Main offenses
            for nibrs_name, rtci_name in OFFENSE_MAP.items():
                search_term = nibrs_name.split(" and ")[0].split("/")[0][:20]
                show_dim(page, 23)
                select_none(page)
                search_and_select(page, search_term, nibrs_name)
                apply_selection(page)
                swap_layout(page)

                row_names, month_cols, data = read_table(page)
                if not row_names or not data:
                    print(f"  {rtci_name}: no data")
                    continue

                # Find month column indices
                month_idx = {}
                for ci, col_name in enumerate(month_cols or []):
                    if col_name in MONTH_NAMES:
                        month_idx[MONTH_NAMES.index(col_name) + 1] = ci

                count = 0
                for ri, rn in enumerate(row_names):
                    ag = match_row_name(rn, name_map)
                    if not ag or ri >= len(data):
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

            # Theft sub-types
            for theft_offense in THEFT_OFFENSES:
                search_term = theft_offense.split(" From ")[0].split(" of ")[0].split("/")[0][:20]
                show_dim(page, 23)
                select_none(page)
                search_and_select(page, search_term, theft_offense)
                apply_selection(page)
                swap_layout(page)

                row_names, month_cols, data = read_table(page)
                if not row_names or not data:
                    print(f"  Theft/{theft_offense}: no data")
                    continue

                month_idx = {}
                for ci, col_name in enumerate(month_cols or []):
                    if col_name in MONTH_NAMES:
                        month_idx[MONTH_NAMES.index(col_name) + 1] = ci

                count = 0
                for ri, rn in enumerate(row_names):
                    ag = match_row_name(rn, name_map)
                    if not ag or ri >= len(data):
                        continue
                    for mo, ci in month_idx.items():
                        if (year, mo) not in window:
                            continue
                        val = parse_count(data[ri][ci]) if ci < len(data[ri]) else 0
                        key = (ag["name"], year, mo)
                        theft_accum[key] = theft_accum.get(key, 0) + val
                        count += 1
                print(f"  Theft/{theft_offense}: {count} cells")

        browser.close()

    # ── Merge theft totals ──
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
    elapsed = time.time() - t0
    print(f"Elapsed: {elapsed:.0f}s ({elapsed / 60:.1f}m)")
