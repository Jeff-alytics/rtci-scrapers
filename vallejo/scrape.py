"""
Vallejo, CA — Monthly UCR Part I Crime Counts
==============================================

Data source:
  Vallejo PD Operations Bureau Reports — monthly JPG images with YTD cumulative
  crime statistics. Monthly counts are computed via YTD subtraction.

Source: https://www.vallejopd.net/public_information/crime_data/operations_bureau_reports

Output: data/latest.json in RTCI pipeline format:
  [{agency, state, type, year, month, offense, count}, ...]

Window: 12 months ending previous month.

Usage:
    python scrape.py          # save to data/latest.json
    python scrape.py --json   # output JSON to stdout
"""

import re
import io
import sys
import json
import argparse
from datetime import date
from pathlib import Path

import numpy as np
import easyocr
from PIL import Image
from curl_cffi import requests
from dateutil.relativedelta import relativedelta

# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://www.vallejopd.net"
AGENCY = "Vallejo"
STATE = "CA"
TYPE = "City"

# Note: 2025/2026 URLs have a typo ("Operatons" instead of "Operations")
YEAR_PAGES = {
    2020: "/public_information/crime_data/operations_bureau_reports/operations_bureau_2020_reports",
    2021: "/public_information/crime_data/operations_bureau_reports/operations_bureau_2021_reports",
    2022: "/public_information/crime_data/operations_bureau_reports/operations_bureau_2022_reports",
    2023: "/public_information/crime_data/operations_bureau_reports/operations_bureau_2023_reports",
    2024: "/public_information/crime_data/operations_bureau_reports/operations_bureau_2024_reports",
    2025: "/public_information/crime_data/operations_bureau_reports/operatons_bureau_2025_reports",
    2026: "/public_information/crime_data/operations_bureau_reports/operatons_bureau_2026_reports",
}

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

CATEGORY_MAP = [
    (r"\bmurder\b",                      "Murder"),
    (r"\brape",                          "Rape"),
    (r"\brobbery\b",                     "Robbery"),
    (r"\baggravated assault\b",          "Aggravated Assault"),
    (r"burglary.*auto.*larcen|auto.*burglary.*larcen", "Theft"),
    (r"\bburglary\b",                    "Burglary"),
    (r"\blarceny\b",                     "Theft"),
    (r"\bstolen vehicles\b",             "Motor Vehicle Theft"),
]

YTD_OVERRIDES = {
    (2026, 1): {"Murder": 0, "Rape": 3},
}

OFFENSES = ["Murder", "Rape", "Robbery", "Aggravated Assault",
            "Burglary", "Theft", "Motor Vehicle Theft"]

# ── Helpers ──────────────────────────────────────────────────────────────────

def rolling_window():
    today = date.today()
    end = date(today.year, today.month, 1) - relativedelta(months=1)
    start = end - relativedelta(months=11)
    return start, end


def fetch_year_images(session, year):
    url = BASE_URL + YEAR_PAGES[year]
    r = session.get(url, impersonate="chrome")

    m = re.search(r"tabs\s*:\s*(\[.*?\]),\s*//\s*required", r.text, re.DOTALL)
    if not m:
        print(f"  WARNING: could not find tabs array on {url}")
        return {}

    tabs_str = m.group(1)
    titles = re.findall(r'"title"\s*:\s*"([A-Za-z]+ 20\d{2})"', tabs_str)
    img_paths = re.findall(r'src=\\"(/UserFiles/[^\\"]+\.jpg)', tabs_str)

    if len(titles) != len(img_paths):
        print(f"  WARNING: {len(titles)} titles vs {len(img_paths)} images for {year}")

    result = {}
    for title, path in zip(titles, img_paths):
        for i, name in enumerate(MONTH_NAMES):
            if name.lower() in title.lower():
                result[i + 1] = BASE_URL + path
                break

    return result


def ocr_items(reader, img_bytes):
    img = Image.open(io.BytesIO(img_bytes))
    img = img.resize((img.width * 2, img.height * 2), Image.LANCZOS)
    img_np = np.array(img)
    raw = reader.readtext(img_np, detail=1)
    items = []
    for bbox, text, _conf in raw:
        xs = [p[0] for p in bbox]
        ys = [p[1] for p in bbox]
        items.append((
            (min(xs) + max(xs)) / 2,
            (min(ys) + max(ys)) / 2,
            text.strip(),
        ))
    return items


def group_rows(items, tol=22):
    if not items:
        return []
    by_y = sorted(items, key=lambda t: t[1])
    rows, cur = [], [by_y[0]]
    for item in by_y[1:]:
        if abs(item[1] - cur[-1][1]) <= tol:
            cur.append(item)
        else:
            rows.append(sorted(cur, key=lambda t: t[0]))
            cur = [item]
    rows.append(sorted(cur, key=lambda t: t[0]))
    return rows


def find_year_col_x(items, report_year):
    year_str = str(report_year)
    prev_year_str = str(report_year - 1)

    rows = group_rows(items, tol=30)

    for row in reversed(rows):
        row_texts = [t for _, _, t in row]
        if year_str in row_texts and prev_year_str in row_texts:
            for x, y, t in row:
                if t == year_str:
                    return x, y

    matches = [(x, y) for x, y, t in items if t.strip() == year_str]
    if matches:
        return max(matches, key=lambda xy: xy[1])
    return None, None


def extract_crime_ytd(reader, img_bytes, report_year):
    items = ocr_items(reader, img_bytes)

    subcategory_y = min(
        (y for x, y, t in items if "subcategor" in t.lower()),
        default=float("inf"),
    )
    items = [(x, y, t) for x, y, t in items if y < subcategory_y - 10]

    curr_year_x, header_y = find_year_col_x(items, report_year)
    if curr_year_x is None:
        print(f"    WARNING: year {report_year} column header not found in image")
        return {}

    below = [(x, y, t) for x, y, t in items if y >= header_y - 30]
    rows = group_rows(below, tol=22)

    results = {}
    for row in rows:
        row_text = " ".join(t for _, _, t in row).lower()

        matched = None
        for pattern, rtci_cat in CATEGORY_MAP:
            if re.search(pattern, row_text):
                if "other" in row_text and "sexual" in row_text:
                    break
                matched = rtci_cat
                break

        if matched is None:
            continue

        numbers = [
            (x, t) for x, _, t in row
            if re.fullmatch(r"\d[\d,]*", t)
        ]
        if not numbers:
            continue

        best_x, best_val = min(numbers, key=lambda xt: abs(xt[0] - curr_year_x))
        if abs(best_x - curr_year_x) > 400:
            continue
        try:
            results[matched] = results.get(matched, 0) + int(best_val.replace(",", ""))
        except ValueError:
            pass

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    start_date, end_date = rolling_window()
    print(f"Rolling window: {start_date} to {end_date}")

    session = requests.Session()
    reader = easyocr.Reader(["en"], gpu=False)

    prev_start = start_date - relativedelta(months=1)

    needed = set()
    d = prev_start
    while d <= end_date:
        needed.add((d.year, d.month))
        d += relativedelta(months=1)

    needed_years = sorted(set(y for y, _ in needed))

    year_month_urls = {}
    for year in needed_years:
        if year not in YEAR_PAGES:
            print(f"No page configured for year {year}, skipping")
            continue
        print(f"Fetching {year} image list...")
        imgs = fetch_year_images(session, year)
        for month, url in imgs.items():
            if (year, month) in needed:
                year_month_urls[(year, month)] = url

    print(f"Found {len(year_month_urls)} images to OCR\n")

    ytd_data = {}
    for year, month in sorted(year_month_urls):
        url = year_month_urls[(year, month)]
        label = f"{MONTH_NAMES[month-1]} {year}"
        print(f"  OCR {label}...")
        r = session.get(url, impersonate="chrome")
        if r.status_code != 200:
            print(f"    ERROR: HTTP {r.status_code}")
            continue
        counts = extract_crime_ytd(reader, r.content, year)
        if (year, month) in YTD_OVERRIDES:
            counts.update(YTD_OVERRIDES[(year, month)])
            print(f"    Applied YTD overrides for {label}: {YTD_OVERRIDES[(year, month)]}")
        if counts:
            ytd_data[(year, month)] = counts
            print(f"    {counts}")
        else:
            print(f"    WARNING: no data extracted")

    # Compute monthly counts (YTD subtraction)
    data = []
    d = start_date
    while d <= end_date:
        year, month = d.year, d.month
        curr_ytd = ytd_data.get((year, month))
        if not curr_ytd:
            print(f"WARNING: missing YTD for {MONTH_NAMES[month-1]} {year}, skipping")
            d += relativedelta(months=1)
            continue

        prev = d - relativedelta(months=1)
        prev_ytd = ytd_data.get((prev.year, prev.month), {})

        for off in OFFENSES:
            curr_val = curr_ytd.get(off, 0)
            if month == 1:
                cnt = curr_val
            else:
                prev_val = prev_ytd.get(off, 0)
                cnt = curr_val - prev_val

            data.append({
                "agency": AGENCY, "state": STATE, "type": TYPE,
                "year": year, "month": month,
                "offense": off, "count": cnt,
            })

        d += relativedelta(months=1)

    if args.json:
        print(json.dumps(data, indent=2))
    else:
        out_path = Path(__file__).parent / "data" / "latest.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(data, indent=2))
        print(f"\nWrote {len(data)} records to {out_path}")


if __name__ == "__main__":
    main()
