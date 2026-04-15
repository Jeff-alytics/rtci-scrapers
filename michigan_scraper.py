"""
Michigan Crime Dashboard Scraper
Scrapes monthly offense data from the Michigan State Police Crime Dashboard
(Tableau Server) via Playwright + Tableau Embedding v3 JS API.

Outputs a CSV compatible with the RTCI Crime Data Pipeline.

Usage:
    python michigan_scraper.py [--months 6] [--output michigan_output.csv] [--headless]
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ─── Target agencies ───────────────────────────────────────────────
TARGET_AGENCIES = {
    "Battle Creek Police Department",
    "Lansing Police Department",
    "Kalamazoo County Sheriff's Office",
    "Kalamazoo Department of Public Safety",
    "Kent County Sheriff's Office",
    "Grand Rapids Police Department",
    "Wyoming Police Department",
    "Kentwood Police Department",
    "Livingston County Sheriff's Office",
    "Macomb County Sheriff's Office",
    "Saint Clair Shores Police Department",
    "Shelby Township Police Department",
    "Sterling Heights Police Department",
    "Warren Police Department",
    "Clinton Township Police Department",
    "Farmington Hills Police Department",
    "Novi Police Department",
    "Royal Oak Police Department",
    "Southfield Police Department",
    "Troy Police Department",
    "Waterford Township Police Department",
    "West Bloomfield Township Police Department",
    "Ottawa County Sheriff's Office",
    "Washtenaw County Sheriff's Office",
    "Ann Arbor Police Department",
    "Dearborn Police Department",
    "Dearborn Heights Police Department",
    "Detroit Police Department",
    "Livonia Police Department",
    "Taylor Police Department",
    "Westland Police Department",
    "Canton Township Police Department",
}

# ─── MICR offense code → RTCI category mapping ────────────────────
OFFENSE_MAP = {
    "09001": "Murder",
    "11001": "Rape", "11002": "Rape", "11003": "Rape",
    "11004": "Rape", "11005": "Rape", "11006": "Rape",
    "11007": "Rape", "11008": "Rape",
    "12000": "Robbery", "12001": "Robbery",
    "13002": "Aggravated Assault", "13004": "Aggravated Assault",
    "22001": "Burglary", "22002": "Burglary", "22003": "Burglary",
    "23001": "Theft", "23002": "Theft", "23003": "Theft",
    "23004": "Theft", "23005": "Theft", "23006": "Theft", "23007": "Theft",
    "24001": "Motor Vehicle Theft", "24002": "Motor Vehicle Theft",
    "30002": "Theft", "30004": "Theft",
}

RTCI_OFFENSES = ["Murder", "Rape", "Robbery", "Aggravated Assault",
                 "Burglary", "Theft", "Motor Vehicle Theft"]

DASHBOARD_URL = "https://www.michigan.gov/msp/divisions/cjic/dashboard-portal/crime-dashboard"


def get_target_months(num_months):
    """Return list of (year, month) tuples for the last N complete months."""
    now = datetime.now()
    y, m = now.year, now.month
    months = []
    for _ in range(num_months):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        months.append((y, m))
    months.reverse()
    return months


def last_day_of_month(year, month):
    """Return the last day of the given month."""
    if month == 12:
        return 31
    return (datetime(year, month + 1, 1) - timedelta(days=1)).day


def scrape_michigan(num_months=6, output_path=None, headless=False):
    """Main scraper using Tableau Embedding v3 JS API via Playwright."""
    from playwright.sync_api import sync_playwright

    target_months = get_target_months(num_months)
    print(f"Scraping {len(target_months)} months: {', '.join(f'{y}-{m:02d}' for y, m in target_months)}")
    print(f"Target agencies: {len(TARGET_AGENCIES)}")

    # Build the offense map for JS (code -> RTCI category)
    offense_map_js = json.dumps(OFFENSE_MAP)
    target_agencies_js = json.dumps(list(TARGET_AGENCIES))

    all_results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
        )
        page = ctx.new_page()

        print("\n[1/4] Loading Michigan Crime Dashboard...")
        page.goto(DASHBOARD_URL, timeout=60000, wait_until='domcontentloaded')
        # Wait for Tableau viz to fully bootstrap
        page.wait_for_timeout(45000)

        # Verify tableau-viz element exists
        has_viz = page.evaluate("!!document.querySelector('tableau-viz')")
        if not has_viz:
            # Check inside iframes
            for frame in page.frames:
                if 'michigan.gov' in frame.url:
                    has_viz = frame.evaluate("!!document.querySelector('tableau-viz')")
                    if has_viz:
                        break
        if not has_viz:
            print("ERROR: Could not find <tableau-viz> element. Page may not have loaded.")
            browser.close()
            return []

        print("[2/4] Activating Data Export sheet...")
        result = page.evaluate("""async () => {
            const viz = document.querySelector('tableau-viz');
            if (!viz || !viz.workbook) return {error: 'No viz or workbook'};
            try {
                await viz.workbook.activateSheetAsync('Data Export');
                return {ok: true};
            } catch(e) {
                return {error: e.message};
            }
        }""")
        if result.get('error'):
            print(f"ERROR activating Data Export: {result['error']}")
            browser.close()
            return []
        page.wait_for_timeout(10000)

        print("[3/4] Switching to Live Data...")
        result = page.evaluate("""async () => {
            const viz = document.querySelector('tableau-viz');
            try {
                await viz.workbook.changeParameterValueAsync('Live Data Toggle', 'Live Data');
                return {ok: true};
            } catch(e) {
                return {error: e.message};
            }
        }""")
        if result.get('error'):
            print(f"ERROR switching to Live Data: {result['error']}")
            browser.close()
            return []
        page.wait_for_timeout(15000)
        print("   Switched to Live Data")

        print("[4/4] Downloading monthly data...")
        for month_idx, (year, month) in enumerate(target_months):
            label = f"{year}-{month:02d}"
            print(f"\n   --- {label} ({month_idx+1}/{len(target_months)}) ---")

            last_day = last_day_of_month(year, month)

            # Apply date filter and get summary data via Tableau JS API
            js_code = """async (params) => {
                const viz = document.querySelector('tableau-viz');
                const sheet = viz.workbook.activeSheet;
                const ws = sheet.worksheets.find(w => w.name === '008. Offenses by County/Agency');
                if (!ws) return {error: 'Worksheet 008 not found'};

                try {
                    // Clear county filter to get all agencies
                    await ws.clearFilterAsync('County');
                } catch(e) {
                    // May already be cleared
                }

                try {
                    // Set date range to this specific month
                    await ws.applyRangeFilterAsync('INCIDENT_DATE', {
                        min: new Date(params.year, params.month - 1, 1),
                        max: new Date(params.year, params.month - 1, params.lastDay)
                    });
                } catch(e) {
                    return {error: 'Date filter failed: ' + e.message};
                }

                // Wait for data to load
                await new Promise(r => setTimeout(r, 5000));

                try {
                    const summaryData = await ws.getSummaryDataAsync();
                    const columns = summaryData.columns.map(c => c.fieldName);
                    const rows = summaryData.data.map(row =>
                        row.map(cell => cell.formattedValue)
                    );
                    return {columns, rows: rows, rowCount: rows.length};
                } catch(e) {
                    return {error: 'getSummaryDataAsync failed: ' + e.message};
                }
            }"""

            result = page.evaluate(
                js_code,
                {'year': year, 'month': month, 'lastDay': last_day}
            )

            if result.get('error'):
                print(f"   ERROR: {result['error']}")
                continue

            row_count = result.get('rowCount', 0)
            columns = result.get('columns', [])
            rows = result.get('rows', [])

            if row_count == 0:
                print(f"   Warning: 0 rows returned for {label}")
                continue

            print(f"   Got {row_count} agency rows, {len(columns)} columns")

            # Build column index map
            col_map = {col: idx for idx, col in enumerate(columns)}

            # Find agency column (could be 'Agency' or similar)
            agency_col = None
            for candidate in ['Agency', 'AGG(Agency)', 'agency']:
                if candidate in col_map:
                    agency_col = candidate
                    break
            if agency_col is None:
                # Try to find it
                for col in columns:
                    if 'agency' in col.lower():
                        agency_col = col
                        break
            if agency_col is None:
                print(f"   ERROR: Cannot find Agency column. Columns: {columns[:5]}")
                continue

            # Parse rows and aggregate to RTCI categories
            agencies_found = set()
            for row in rows:
                agency_name = row[col_map[agency_col]].strip()
                if agency_name not in TARGET_AGENCIES:
                    continue

                agencies_found.add(agency_name)
                totals = {off: 0 for off in RTCI_OFFENSES}

                for code, rtci_cat in OFFENSE_MAP.items():
                    # Column names are like 'AGG(09001)' or just '09001'
                    col_name = None
                    for candidate in [f'AGG({code})', code]:
                        if candidate in col_map:
                            col_name = candidate
                            break
                    if col_name is None:
                        continue

                    val_str = row[col_map[col_name]].strip().replace(',', '')
                    if val_str and val_str != '' and val_str != 'Null':
                        try:
                            totals[rtci_cat] += int(val_str)
                        except ValueError:
                            try:
                                totals[rtci_cat] += int(float(val_str))
                            except ValueError:
                                pass

                for offense, count in totals.items():
                    all_results.append({
                        'agency': agency_name,
                        'year': year,
                        'month': month,
                        'offense': offense,
                        'count': count,
                    })

            print(f"   Matched {len(agencies_found)}/{len(TARGET_AGENCIES)} target agencies")
            missing = TARGET_AGENCIES - agencies_found
            if missing:
                sample = sorted(missing)[:5]
                suffix = '...' if len(missing) > 5 else ''
                print(f"   Missing: {', '.join(sample)}{suffix}")

            # Small delay between months
            page.wait_for_timeout(2000)

        browser.close()

    print(f"\n{'='*60}")
    print(f"Total records: {len(all_results)}")

    # Add state/type fields for pipeline compatibility
    SO_KEYWORDS = ['Sheriff', 'County']
    for r in all_results:
        r['state'] = 'MI'
        r['type'] = 'County' if any(k in r['agency'] for k in SO_KEYWORDS) else 'City'

    # Write output JSON (repo convention: data/latest.json)
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'latest.json')
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2)
    print(f"JSON saved to: {json_path}")

    # Also write CSV if requested
    if output_path and all_results:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['agency', 'state', 'type', 'year', 'month', 'offense', 'count'])
            writer.writeheader()
            writer.writerows(all_results)
        print(f"CSV saved to: {output_path}")

    return all_results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Michigan Crime Dashboard Scraper')
    parser.add_argument('--months', type=int, default=6, help='Number of months to scrape (default: 6)')
    parser.add_argument('--output', default='michigan_output.csv', help='Output CSV path')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()

    results = scrape_michigan(
        num_months=args.months,
        output_path=args.output,
        headless=args.headless,
    )

    if results:
        from collections import Counter
        agency_counts = Counter(r['agency'] for r in results)
        print(f"\nAgency summary ({len(agency_counts)} agencies):")
        for agency, count in sorted(agency_counts.items()):
            print(f"  {agency}: {count} records")
