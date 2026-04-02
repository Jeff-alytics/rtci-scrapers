"""
San Francisco, CA — Monthly UCR Part I Crime Counts
====================================================

Data source:
  Tableau dashboard embedded on the SFPD website. The dashboard's summary table
  is screenshotted per month and OCR'd via easyocr.

Dashboard: https://public.tableau.com/views/CrimeNumbersDashboardFullSize/Crime_Numbers_Full_Size

Output: data/latest.json in RTCI pipeline format:
  [{agency, state, type, year, month, offense, count}, ...]

Window: 24 months ending current month.

Usage:
    python scrape.py                # save to data/latest.json
    python scrape.py --json         # output JSON to stdout
    python scrape.py --no-headless  # show browser for debugging
"""

import io
import re
import sys
import json
import time
import argparse
from datetime import date
from pathlib import Path

import numpy as np
import easyocr
from PIL import Image
from dateutil.relativedelta import relativedelta
from playwright.sync_api import sync_playwright

# ── Constants ────────────────────────────────────────────────────────────────

TABLEAU_URL = (
    "https://public.tableau.com/views/CrimeNumbersDashboardFullSize"
    "/Crime_Numbers_Full_Size?:embed=y&:showVizHome=no"
)

AGENCY = "San Francisco"
STATE = "CA"
TYPE = "City"

# Manual overrides for months where OCR fails to extract a value.
MONTHLY_OVERRIDES = {
    "2024-06": {"Murder": 4},
}

# Maps dashboard row label (uppercase) → RTCI offense
_LABEL_MAP = {
    "homicide":            "Murder",
    "rape":                "Rape",
    "robbery":             "Robbery",
    "assault":             "Aggravated Assault",
    "burglary":            "Burglary",
    "motor vehicle theft": "Motor Vehicle Theft",
    "larceny theft":       "Theft",
}

# Table crop region (pixels) — validated against a 1400×900 viewport
_TABLE_CLIP = {"x": 270, "y": 485, "width": 800, "height": 270}
_OCR_SCALE = 3

OFFENSES = ["Murder", "Rape", "Robbery", "Aggravated Assault",
            "Burglary", "Theft", "Motor Vehicle Theft"]

# ── Helpers ──────────────────────────────────────────────────────────────────

def rolling_window():
    today = date.today()
    end = date(today.year, today.month, 1)
    start = end - relativedelta(months=11)
    return start, end


def last_day(d):
    return (d + relativedelta(months=1)) - relativedelta(days=1)


def _set_date_range(page, start_str, end_str):
    for label, val in [("Start Date", start_str), ("End Date", end_str)]:
        ta = page.locator(f'textarea[aria-label*="{label}"]')
        ta.click()
        page.keyboard.press("Control+a")
        ta.fill(val)
        page.keyboard.press("Enter")
        time.sleep(1.5)
    time.sleep(4)
    page.mouse.click(700, 150)
    time.sleep(0.5)


def _screenshot_table(page):
    png_bytes = page.screenshot(clip=_TABLE_CLIP)
    img = Image.open(io.BytesIO(png_bytes))
    return img.resize(
        (img.width * _OCR_SCALE, img.height * _OCR_SCALE), Image.LANCZOS
    )


def _parse_table(img, reader, month_label):
    results = reader.readtext(np.array(img), detail=1)

    labels = []
    numbers = []

    for bbox, text, conf in results:
        xs = [pt[0] for pt in bbox]
        ys = [pt[1] for pt in bbox]
        x_left = min(xs)
        x_right = max(xs)
        x_center = (x_left + x_right) / 2
        y_center = sum(ys) / 4
        text_clean = text.strip().replace(",", "")

        if re.match(r"^\d{1,5}$", text_clean):
            try:
                numbers.append((int(text_clean), x_center, y_center))
            except ValueError:
                pass
        else:
            labels.append((text.strip(), x_left, x_right, y_center))

    if not labels or not numbers:
        return {}

    img_width = img.width
    label_y = {}
    label_x_rights = []
    for txt, x_left, x_right, y in labels:
        if x_left < img_width * 0.15:
            label_y[txt.upper()] = y
            label_x_rights.append(x_right)

    if not label_y:
        return {}

    label_col_right = max(label_x_rights)

    row_numbers = {}
    for val, x, y in numbers:
        if x <= label_col_right:
            continue
        closest = min(label_y.items(), key=lambda kv: abs(kv[1] - y))
        lbl_text, lbl_y = closest
        if abs(lbl_y - y) > _OCR_SCALE * 20:
            continue
        row_numbers.setdefault(lbl_text, []).append((x, val))

    row_counts = {
        lbl: min(pairs, key=lambda p: p[0])[1]
        for lbl, pairs in row_numbers.items()
    }

    counts = {}
    for raw_label, val in row_counts.items():
        for key, col in _LABEL_MAP.items():
            if key.upper() in raw_label:
                counts[col] = val
                break

    found = sorted(counts.keys())
    missing = [c for c in _LABEL_MAP.values() if c not in counts]
    status = "OK" if not missing else f"MISSING: {missing}"
    print(f"    {month_label}: {status}  {counts}")

    return counts


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    parser.add_argument("--no-headless", action="store_true", help="Show browser")
    args = parser.parse_args()

    start_date, end_date = rolling_window()
    print(f"Rolling window: {start_date} to {end_date}")

    print("Initialising easyocr reader...")
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.no_headless)
        page = browser.new_page()
        page.set_viewport_size({"width": 1400, "height": 900})

        print("Loading Tableau dashboard...")
        page.goto(TABLEAU_URL, timeout=90_000)
        page.wait_for_timeout(20_000)

        try:
            page.locator('textarea[aria-label*="Start Date"]').wait_for(timeout=10_000)
            print("  Dashboard loaded successfully.")
        except Exception:
            print("  ERROR: dashboard date inputs not found. Aborting.")
            browser.close()
            return

        monthly_data = {}
        d = start_date
        while d <= end_date:
            label = f"{d.year}-{d.month:02d}"
            s_str = f"{d.month}/{d.day}/{d.year}"
            e_str = f"{last_day(d).month}/{last_day(d).day}/{d.year}"

            print(f"  Processing {label}...")
            _set_date_range(page, s_str, e_str)

            img = _screenshot_table(page)
            counts = _parse_table(img, reader, label)

            if label in MONTHLY_OVERRIDES:
                for col, val in MONTHLY_OVERRIDES[label].items():
                    if col not in counts:
                        counts[col] = val
                        print(f"    {label}: applied override {col}={val}")

            if counts:
                monthly_data[(d.year, d.month)] = counts
            else:
                print(f"    WARNING: no data extracted for {label}")

            d += relativedelta(months=1)

        browser.close()

    # Build output in RTCI long format
    data = []
    d = start_date
    while d <= end_date:
        counts = monthly_data.get((d.year, d.month), {})
        if counts:
            for off in OFFENSES:
                cnt = counts.get(off, 0)
                data.append({
                    "agency": AGENCY, "state": STATE, "type": TYPE,
                    "year": d.year, "month": d.month,
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
