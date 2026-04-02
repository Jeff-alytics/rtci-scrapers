"""
San Bernardino, CA — Monthly UCR Part I Crime Counts
====================================================

Data source:
  CivicPlus document center at sanbernardino.gov. Uses Playwright + stealth
  to establish a session, then queries the document API for PDFs.
  Each PDF is an infographic with monthly bar charts parsed via pdfplumber.

Source: https://www.sanbernardino.gov/DocumentCenter/Index/1112

Output: data/latest.json in RTCI pipeline format:
  [{agency, state, type, year, month, offense, count}, ...]

Window: 24 months ending previous month.

NOTE: San Bernardino is typically 6-9 months behind.

Usage:
    python scrape.py          # save to data/latest.json
    python scrape.py --json   # output JSON to stdout
"""

import io
import re
import sys
import json
import argparse
from datetime import date
from pathlib import Path

import pdfplumber
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# ── Constants ────────────────────────────────────────────────────────────────

ARCHIVE_URL = "https://www.sanbernardino.gov/DocumentCenter/Index/1112"
BASE_URL = "https://www.sanbernardino.gov"

AGENCY = "San Bernardino"
STATE = "CA"
TYPE = "City"
WINDOW_MONTHS = 24

ROOT_FOLDER_ID = 1112
MODULE_ID = 75

SECTION_KEY_MAP = {
    "HOMICIDE":   "Murder",
    "RAPE":       "Rape",
    "ROBBERY":    "Robbery",
    "AGGRAVATED": "Aggravated Assault",
    "BURGLARY":   "Burglary",
    "LARCENY":    "Theft",
    "MOTOR":      "Motor Vehicle Theft",
}

# Fixed month x-centers from PDF layout
MONTH_X_CENTERS = [301.1, 314.9, 328.8, 342.7, 356.6, 370.4, 384.3, 398.2, 412.1, 425.9, 439.8, 453.7]
MONTHLY_X_MIN = 293
MONTH_X_TOLERANCE = 8

OFFENSES = ["Murder", "Rape", "Robbery", "Aggravated Assault",
            "Burglary", "Theft", "Motor Vehicle Theft"]

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_window():
    today = date.today()
    if today.month == 1:
        end = date(today.year - 1, 12, 1)
    else:
        end = date(today.year, today.month - 1, 1)
    sm = end.month - (WINDOW_MONTHS - 1)
    sy = end.year
    while sm <= 0:
        sm += 12
        sy -= 1
    return date(sy, sm, 1), end


def x_to_month(x0, x1):
    xc = (x0 + x1) / 2
    distances = [abs(xc - cx) for cx in MONTH_X_CENTERS]
    idx = min(range(len(distances)), key=lambda i: distances[i])
    return (idx + 1) if distances[idx] <= MONTH_X_TOLERANCE else None


def parse_doc_ym(display_name):
    m = re.search(r'(\d{4})-(\d{2})', display_name or "")
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


# ── API helpers ──────────────────────────────────────────────────────────────

def js_get_folders(page):
    data = page.evaluate(f"""
        async () => {{
            const r = await fetch('/admin/DocumentCenter/Home/_AjaxLoadingReact?type=0', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest'}},
                body: JSON.stringify({{
                    value: "{ROOT_FOLDER_ID}",
                    expandTree: true,
                    loadSource: 7,
                    selectedFolder: {ROOT_FOLDER_ID}
                }})
            }});
            return await r.json();
        }}
    """)
    return {int(f['Text']): int(f['Value']) for f in data.get('Data', []) if str(f['Text']).isdigit()}


def js_get_docs(page, folder_id):
    data = page.evaluate(f"""
        async () => {{
            const r = await fetch('/Admin/DocumentCenter/Home/Document_AjaxBinding?renderMode=0&loadSource=7', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest'}},
                body: JSON.stringify({{
                    folderId: {folder_id},
                    getDocuments: 1,
                    imageRepo: false,
                    renderMode: 0,
                    loadSource: 7,
                    requestingModuleID: {MODULE_ID},
                    searchString: '',
                    pageNumber: 1,
                    rowsPerPage: 100,
                    sortColumn: 'DisplayName',
                    sortOrder: 0
                }})
            }});
            return await r.json();
        }}
    """)
    return data.get('Documents', [])


# ── PDF parser ───────────────────────────────────────────────────────────────

def extract_monthly_from_pdf(pdf_bytes):
    section_data = {}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=3, y_tolerance=3)

            by_month_items = []
            for i, w in enumerate(words):
                if w['text'] != 'MONTH':
                    continue
                by_word = next(
                    (words[j] for j in range(max(0, i - 3), i)
                     if words[j]['text'] == 'BY'
                     and abs(words[j]['top'] - w['top']) < 4),
                    None
                )
                if by_word is None:
                    continue

                cat_words = [
                    cw for cw in words
                    if abs(cw['top'] - w['top']) < 4
                    and cw['x1'] <= by_word['x0']
                    and cw['text'] not in ('BY', 'MONTH')
                ]
                cat_words.sort(key=lambda cw: cw['x0'])
                header_text = ' '.join(cw['text'] for cw in cat_words)

                category = None
                for key, cat in SECTION_KEY_MAP.items():
                    if key in header_text:
                        category = cat
                        break

                by_month_items.append({'y': w['top'], 'category': category})

            jan_ys = sorted(set(
                round(w['top'])
                for w in words
                if w['text'] == 'Jan' and w['x0'] > MONTHLY_X_MIN - 10
            ))

            for jan_y in jan_ys:
                above = [bm for bm in by_month_items if bm['y'] < jan_y]
                if not above:
                    continue
                bm = max(above, key=lambda b: b['y'])
                category = bm['category']
                if category is None:
                    continue

                section_top_y = bm['y']

                month_vals = {}
                for w in words:
                    if w['x0'] < MONTHLY_X_MIN:
                        continue
                    if w['top'] <= section_top_y or w['top'] >= jan_y:
                        continue
                    try:
                        val = int(w['text'].replace(',', ''))
                    except ValueError:
                        continue
                    month = x_to_month(w['x0'], w['x1'])
                    if month is not None:
                        month_vals[month] = val

                if category not in section_data:
                    section_data[category] = {}
                section_data[category].update(month_vals)

    return section_data


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    start_date, end_date = get_window()
    print(f"Window: {start_date} to {end_date}")
    needed_years = set(range(start_date.year, end_date.year + 1))

    all_section_data = {}  # {year: {category: {month: count}}}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        print("Loading archive page...")
        page.goto(ARCHIVE_URL, wait_until="networkidle", timeout=60000)

        print("Getting year folder IDs...")
        year_folders = js_get_folders(page)

        for year in sorted(needed_years):
            folder_id = year_folders.get(year)
            if folder_id is None:
                print(f"  {year}: no folder found, skipping")
                continue

            docs = js_get_docs(page, folder_id)
            if not docs:
                print(f"  {year}: no documents found")
                continue

            parsed = []
            for doc in docs:
                y, m = parse_doc_ym(doc.get('DisplayName', ''))
                if y == year and m is not None:
                    parsed.append((m, doc))

            if not parsed:
                print(f"  {year}: could not parse document dates")
                continue

            parsed.sort(key=lambda x: x[0])
            latest_month, latest_doc = parsed[-1]
            doc_url = BASE_URL + latest_doc['URL']

            print(f"  {year}: downloading '{latest_doc['DisplayName']}' ...")
            resp = page.request.get(doc_url, timeout=60000)
            if resp.status != 200:
                print(f"    ERROR: HTTP {resp.status}")
                continue

            section_data = extract_monthly_from_pdf(resp.body())
            all_section_data[year] = (section_data, latest_month)

        browser.close()

    # Build output
    data = []
    for year, (section_data, latest_month) in sorted(all_section_data.items()):
        for month_num in range(1, latest_month + 1):
            month_date = date(year, month_num, 1)
            if month_date < start_date or month_date > end_date:
                continue

            for off in OFFENSES:
                cnt = section_data.get(off, {}).get(month_num, 0)
                data.append({
                    "agency": AGENCY, "state": STATE, "type": TYPE,
                    "year": year, "month": month_num,
                    "offense": off, "count": cnt,
                })

    if args.json:
        print(json.dumps(data, indent=2))
    else:
        out_path = Path(__file__).parent / "data" / "latest.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(data, indent=2))
        print(f"\nWrote {len(data)} records to {out_path}")


if __name__ == "__main__":
    main()
