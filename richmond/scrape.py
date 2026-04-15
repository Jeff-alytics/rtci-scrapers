"""
Richmond CA Crime Statistics Scraper
====================================
Source: https://www.ci.richmond.ca.us/4010/Crime-Stat-Reports
Scrapes the "Citywide Index Crimes" HTML table directly from the page.
Outputs data/latest.json in RTCI pipeline format.
"""

import json
import re
import sys
import requests
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

URL = "https://www.ci.richmond.ca.us/4010/Crime-Stat-Reports"

OFFENSE_MAP = {
    'Murder': 'Murder',
    'Sexual Assault': 'Rape',
    'Robbery': 'Robbery',
    'Aggravated Assault': 'Aggravated Assault',
    'Burglary': 'Burglary',
    'Larceny-Theft': 'Theft',
    'Vehicle-Theft': 'Motor Vehicle Theft',
}

MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
          'Jul', 'Aug', 'Sep', 'Sept', 'Oct', 'Nov', 'Dec']

MONTH_NUM = {m.lower(): i + 1 for i, m in enumerate(
    ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
)}
MONTH_NUM['sept'] = 9


def scrape():
    print(f"Fetching {URL}...")
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Find all tables
    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    print(f"Found {len(tables)} tables")

    records = []

    for table in tables:
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL | re.IGNORECASE)
        if len(rows) < 5:
            continue

        # Parse header row for month columns and year
        header_cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', rows[0], re.DOTALL | re.IGNORECASE)
        header_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in header_cells]

        # Check if this is the "Citywide Index Crimes YYYY" table
        header_text = ' '.join(header_cells).lower()
        if 'index crimes' not in header_text and 'citywide' not in header_text:
            continue

        # Extract year from header
        year_match = re.search(r'20\d{2}', header_text)
        if not year_match:
            continue
        year = int(year_match.group())

        # Map column indices to month numbers
        col_months = {}
        for i, cell in enumerate(header_cells):
            cell_lower = cell.lower().strip()
            if cell_lower in MONTH_NUM:
                col_months[i] = MONTH_NUM[cell_lower]

        if not col_months:
            continue

        print(f"Found table: year={year}, months={list(col_months.values())}")

        # Parse data rows
        for row_html in rows[1:]:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL | re.IGNORECASE)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if not cells:
                continue

            label = cells[0].strip()
            rtci = OFFENSE_MAP.get(label)
            if not rtci:
                continue

            for col_idx, month_num in col_months.items():
                if col_idx >= len(cells):
                    continue
                val_str = cells[col_idx].strip().replace(',', '')
                if not val_str or not val_str.isdigit():
                    continue
                count = int(val_str)
                records.append({
                    'agency': 'Richmond',
                    'state': 'CA',
                    'type': 'City',
                    'year': year,
                    'month': month_num,
                    'offense': rtci,
                    'count': count,
                })

        # Only use the first matching table (Citywide Index Crimes)
        break

    return records


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
