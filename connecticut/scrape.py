"""
Connecticut Crime Scraper — Beyond 20/20 (ct.beyond2020.com)
Report 419: "Group A Crimes Count by Offense Type, Monthly Report" (24-month rolling)
Gets 24 months in ONE table read per agency — most efficient B2020 scraper.
Outputs connecticut/data/latest.json in RTCI pipeline format.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

STATE = "CT"
ENTRY_URL = "https://ct.beyond2020.com/ct_public/Dim/dimension.aspx"
REPORT_URL = "https://ct.beyond2020.com/CT_public/View/dispview.aspx?ReportId=419"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

AGENCIES = [
    {"ori": "CT0001500", "name": "Bridgeport", "type": "City"},
    {"ori": "CT0001700", "name": "Bristol", "type": "City"},
    {"ori": "CT0003400", "name": "Danbury", "type": "City"},
    {"ori": "CT0004300", "name": "East Hartford", "type": "City"},
    {"ori": "CT0005100", "name": "Fairfield", "type": "City"},
    {"ori": "CT0005700", "name": "Greenwich", "type": "City"},
    {"ori": "CT0006200", "name": "Hamden", "type": "City"},
    {"ori": "CT0006400", "name": "Hartford", "type": "City"},
    {"ori": "CT0007700", "name": "Manchester", "type": "City"},
    {"ori": "CT0008000", "name": "Meriden", "type": "City"},
    {"ori": "CT0008400", "name": "Milford", "type": "City"},
    {"ori": "CT0008900", "name": "New Britain", "type": "City"},
    {"ori": "CT0009300", "name": "New Haven", "type": "City"},
    {"ori": "CT0010300", "name": "Norwalk", "type": "City"},
    {"ori": "CT0013500", "name": "Stamford", "type": "City"},
    {"ori": "CT0013800", "name": "Stratford", "type": "City"},
    {"ori": "CT0015100", "name": "Waterbury", "type": "City"},
    {"ori": "CT0015500", "name": "West Hartford", "type": "City"},
    {"ori": "CT0015600", "name": "West Haven", "type": "City"},
]

OFFENSE_MAP = {
    "Murder": "Murder",
    "All Rape": "Rape",
    "Robbery": "Robbery",
    "Aggravated Assault": "Aggravated Assault",
    "Burglary": "Burglary",
    "Larceny": "Theft",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def nine_month_window():
    """Return set of (year, month) tuples for last 9 complete months."""
    now = datetime.now().replace(day=1)
    months = set()
    y, m = now.year, now.month - 1  # start from last complete month
    if m == 0:
        y, m = y - 1, 12
    for _ in range(9):
        months.add((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return months


def normalize_offense(display_name):
    for prefix, rtci_col in OFFENSE_MAP.items():
        if display_name.startswith(prefix):
            return rtci_col
    return None


def parse_month_label(label):
    parts = label.strip().split()
    if len(parts) == 2 and parts[0][:3] in MONTH_ABBR:
        try:
            return int(parts[1]), MONTH_ABBR[parts[0][:3]]
        except ValueError:
            pass
    return None


def parse_cell(text):
    text = text.strip().replace(",", "")
    if not text or text in ("\u00a0", ".", "-", "*", " "):
        return None
    try:
        return int(text)
    except ValueError:
        return None


def select_agency(page, agency_name):
    try:
        with page.expect_navigation(wait_until="networkidle", timeout=20000):
            page.evaluate("ShowDim(11, 11);")
        page.wait_for_timeout(1000)
    except Exception as e:
        print(f"    ShowDim error: {e}")
        return False

    try:
        page.fill('input[name="SearchString"]', agency_name)
        with page.expect_navigation(wait_until="networkidle", timeout=15000):
            page.evaluate('MembersSearch(document.querySelector(\'input[name="SearchString"]\'));')
        page.wait_for_timeout(1000)
    except Exception as e:
        print(f"    Search error: {e}")
        return False

    body = page.evaluate("() => document.body.innerText")
    if agency_name.lower() not in body.lower():
        print(f"    '{agency_name}' not found")
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
                if (!label) return 'no label';
                var chk = label.querySelector('input.rtChk');
                if (!chk) return 'no rtChk';
                chk.click();
                return 'ok:' + span.textContent.trim();
            }
        }
        // Debug: return first few span texts
        var samples = spans.slice(0, 5).map(s => s.textContent.trim());
        return 'not found (samples: ' + samples.join(', ') + ')';
    }
    """, agency_name)
    if not clicked.startswith("ok"):
        print(f"    Click failed: {clicked}")
        return False

    page.wait_for_timeout(200)
    try:
        with page.expect_navigation(wait_until="networkidle", timeout=20000):
            page.evaluate("ShowUpdatedReportAfterSelection();")
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"    Return error: {e}")
        return False
    return True


def read_table(page):
    month_labels = page.evaluate("""
    () => {
        var t = document.getElementById('headerRowTable');
        if (!t || t.rows.length === 0) return null;
        return Array.from(t.rows[0].cells).map(c => c.innerText.trim());
    }
    """)
    row_names = page.evaluate("""
    () => {
        var t = document.getElementById('headerColumnTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => r.cells[0] ? r.cells[0].innerText.trim() : '');
    }
    """)
    data_grid = page.evaluate("""
    () => {
        var t = document.getElementById('bodyTable');
        if (!t) return null;
        return Array.from(t.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim()));
    }
    """)
    if not month_labels or not row_names or not data_grid:
        return None
    return month_labels, row_names, data_grid


def main():
    window = nine_month_window()
    print(f"9-month window: {sorted(window)}")
    print(f"{len(AGENCIES)} CT agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        # Establish session
        page.goto(ENTRY_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(1000)

        for ag in AGENCIES:
            print(f"{ag['name']}...", end=" ", flush=True)
            try:
                page.goto(REPORT_URL, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)
                if not select_agency(page, ag["name"]):
                    print("FAILED")
                    continue

                result = read_table(page)
                if not result:
                    print("no table")
                    continue

                month_labels, row_names, data_grid = result
                month_keys = [parse_month_label(l) for l in month_labels]
                count = 0

                for row_idx, offense_name in enumerate(row_names):
                    rtci_col = normalize_offense(offense_name)
                    if not rtci_col or row_idx >= len(data_grid):
                        continue
                    for col_idx, cell in enumerate(data_grid[row_idx]):
                        if col_idx >= len(month_keys) or month_keys[col_idx] is None:
                            continue
                        yr, mo = month_keys[col_idx]
                        if (yr, mo) not in window:
                            continue
                        val = parse_cell(cell)
                        if val is None:
                            val = 0
                        all_rows.append({
                            "agency": ag["name"], "state": STATE, "type": ag["type"],
                            "year": yr, "month": mo, "offense": rtci_col, "count": val,
                        })
                        count += 1

                print(f"{count} records")
            except Exception as e:
                print(f"ERROR: {e}")
                try:
                    page.goto(ENTRY_URL, wait_until="networkidle", timeout=30000)
                except Exception:
                    pass

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
