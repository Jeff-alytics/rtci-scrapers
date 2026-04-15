"""
San Jose Police Department NIBRS Scraper
========================================
Source: https://www.sjpd.org/records/crime-stats-maps/crime-statistics
Uses Playwright to bypass Akamai CDN, parses embedded NIBRS table.
Outputs data/latest.json in RTCI pipeline format.
"""

import json
import sys
import re
from datetime import date
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

URL = "https://www.sjpd.org/records/crime-stats-maps/crime-statistics"

OFFENSE_MAP = {
    'Murder': 'Murder',
    'Manslaughter': 'Murder',
    'Rape': 'Rape',
    'Aggravated Assault': 'Aggravated Assault',
    'Robbery': 'Robbery',
    'Burglary': 'Burglary',
    'Larceny/Theft Offenses': 'Theft',
    'Motor Vehicle Theft': 'Motor Vehicle Theft',
}

MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
          'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def scrape():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = ctx.new_page()
        print(f"Loading {URL}...")
        page.goto(URL, timeout=30000, wait_until='load')
        page.wait_for_timeout(10000)

        # Extract year from header text
        body_text = page.evaluate('() => document.body.innerText')
        year_match = re.search(r'TIME\s+PERIOD:\s+JAN\s*[-\u2011]\s*DEC\s+(\d{4})', body_text)
        year = int(year_match.group(1)) if year_match else date.today().year
        print(f"Year: {year}")

        # Extract table data
        table_data = page.evaluate('''() => {
            const tables = document.querySelectorAll('table');
            for (const t of tables) {
                if (t.innerText.includes('Murder') && t.innerText.includes('Robbery')) {
                    const rows = [];
                    for (const tr of t.rows) {
                        const cells = Array.from(tr.cells).map(c => c.textContent.trim());
                        rows.push(cells);
                    }
                    return rows;
                }
            }
            return null;
        }''')

        browser.close()

    if not table_data:
        print("ERROR: Could not find NIBRS table")
        return []

    # Find month columns from header row
    month_cols = []
    for row in table_data:
        for i, cell in enumerate(row):
            if cell.upper() in MONTHS:
                month_cols.append((i, MONTHS.index(cell.upper()) + 1))
        if month_cols:
            break

    if not month_cols:
        print("ERROR: Could not find month columns")
        return []

    print(f"Month columns found: {[(MONTHS[m-1], i) for i, m in month_cols]}")

    # Parse crime rows
    records = []
    for row in table_data:
        if not row:
            continue
        label = row[0].strip()
        rtci = OFFENSE_MAP.get(label)
        if not rtci:
            continue

        for col_idx, month_num in month_cols:
            if col_idx >= len(row):
                continue
            val_str = row[col_idx].strip().replace(',', '')
            if not val_str or not val_str.isdigit():
                continue
            count = int(val_str)
            records.append({
                'agency': 'San Jose',
                'state': 'CA',
                'type': 'City',
                'year': year,
                'month': month_num,
                'offense': rtci,
                'count': count,
            })

    # Aggregate (Murder + Manslaughter both map to Murder)
    agg = {}
    for r in records:
        key = (r['year'], r['month'], r['offense'])
        if key not in agg:
            agg[key] = dict(r)
            agg[key]['count'] = 0
        agg[key]['count'] += r['count']

    result = sorted(agg.values(), key=lambda r: (r['year'], r['month'], r['offense']))
    return result


def main():
    records = scrape()
    print(f"\nTotal: {len(records)} records")
    for r in records:
        print(f"  {r['year']}-{r['month']:02d} {r['offense']}: {r['count']}")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w") as f:
        json.dump(records, f, indent=2)
    print(f"Saved to: {OUT_JSON.resolve()}")


if __name__ == "__main__":
    main()
