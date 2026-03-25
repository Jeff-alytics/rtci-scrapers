"""
Portland Police Bureau — Monthly Crime Counts Scraper
=====================================================

Data sources:
  - All UCR categories EXCEPT Aggravated Assault: Tableau Public CSV snapshots
    (PPBOpenDataDownloads workbook), fetched via plain HTTP.
  - Aggravated Assault: Live Tableau dashboard (MonthlyReportedCrimeStatistics),
    scraped with Playwright.  The CSV undercounts AA by ~15-20% because Portland
    excludes sensitive cases (likely DV) from the public CSV.

Output: data/latest.json in RTCI pipeline format:
  [{agency, state, type, year, month, offense, count}, ...]

Window: 12 months ending the month before today.

Usage:
    python scrape.py                # save to data/latest.json
    python scrape.py --json         # output JSON to stdout
    python scrape.py --no-headless  # show browser for debugging
"""

import csv
import io
import re
import sys
import json
import argparse
import requests
from collections import defaultdict
from datetime import date
from pathlib import Path

# ── Constants ────────────────────────────────────────────────────────────────

CSV_URL = (
    "https://public.tableau.com/views/PPBOpenDataDownloads"
    "/New_Offense_Data_{year}.csv?:showVizHome=no"
)

TABLEAU_DASH_URL = (
    "https://public.tableau.com/views/MonthlyReportedCrimeStatistics"
    "/MonthlyStatistics?:embed=y&:showVizHome=no&:toolbar=no"
)

AGENCY = "Portland"
STATE = "OR"
TYPE = "City"

# Portland OffenseType → UCR Part I category (AA excluded — from dashboard)
OFFENSE_MAP = {
    "Murder and Non-negligent Manslaughter": "Murder",
    "Rape": "Rape",
    "Sodomy": "Rape",
    "Sexual Assault With An Object": "Rape",
    "Robbery": "Robbery",
    "Burglary": "Burglary",
    "Shoplifting": "Theft",
    "All Other Larceny": "Theft",
    "Theft From Motor Vehicle": "Theft",
    "Theft From Building": "Theft",
    "Theft of Motor Vehicle Parts or Accessories": "Theft",
    "Purse-Snatching": "Theft",
    "Pocket-Picking": "Theft",
    "Theft From Coin-Operated Machine or Device": "Theft",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

MONTH_NAME = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}
MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

# Sanity bounds for Portland monthly AA (reject obvious scraping errors)
AA_MIN, AA_MAX = 50, 600


# ── Window calculation ───────────────────────────────────────────────────────

def compute_window():
    """12-month window ending the month before today."""
    today = date.today()
    end = date(today.year, today.month, 1)  # exclusive
    yr, mo = end.year, end.month - 12
    if mo <= 0:
        mo += 12
        yr -= 1
    start = date(yr, mo, 1)
    return start, end


# ══════════════════════════════════════════════════════════════════════════════
# CSV download (Murder, Rape, Robbery, Burglary, Theft, MVT)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_csv(year):
    url = CSV_URL.format(year=year)
    print(f"  CSV: downloading {year}...", end=" ", flush=True)
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=120)
    resp.raise_for_status()
    print(f"{len(resp.content):,} bytes")
    return resp.text


def aggregate_csv(csv_text, start, end):
    """Parse CSV, filter by ReportMonthYear within [start, end), return
    {(yr, mo): {offense: count}} excluding AA."""
    data = defaultdict(lambda: defaultdict(int))
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        ot = row.get("OffenseType", "").strip()
        ucr = OFFENSE_MAP.get(ot)
        if not ucr:
            continue
        rmy = row.get("ReportMonthYear", "").strip()
        if not rmy:
            continue
        try:
            month_name, yr_str = rmy.rsplit(" ", 1)
            mo = MONTH_NAME[month_name]
            yr = int(yr_str)
            row_date = date(yr, mo, 1)
        except (KeyError, ValueError):
            continue
        if row_date < start or row_date >= end:
            continue
        cnt = int(row.get("OffenseCount") or 1)
        data[(yr, mo)][ucr] += cnt
    return data


def scrape_csv_data(start, end):
    """Download CSV files and aggregate non-AA offenses."""
    years_needed = sorted(set(range(start.year, end.year + 1)))
    print(f"  CSV years: {years_needed}")
    combined = defaultdict(lambda: defaultdict(int))
    for yr in years_needed:
        csv_text = fetch_csv(yr)
        monthly = aggregate_csv(csv_text, start, end)
        for key, counts in monthly.items():
            for offense, cnt in counts.items():
                combined[key][offense] += cnt
    return combined


# ══════════════════════════════════════════════════════════════════════════════
# Tableau dashboard scrape (Aggravated Assault only)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_bar_label(text):
    """'Feb 2025' → (2025, 2) or None."""
    m = re.match(r"^([A-Z][a-z]{2}) (\d{4})$", text.strip())
    if not m:
        return None
    mo = MONTH_ABBR.get(m.group(1))
    return (int(m.group(2)), mo) if mo else None


def scrape_aa_from_dashboard(start, end, headless=True):
    """
    Scrape monthly Aggravated Assault counts from the live Tableau dashboard.

    Strategy: for each month bar, click to filter the Offense Category Table,
    then read the AA count via tooltip hover on the count cell.

    Returns {(year, month): aa_count}.
    """
    from playwright.sync_api import sync_playwright

    aa_counts = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_viewport_size({"width": 1600, "height": 1000})

        print(f"\n  Dashboard: loading...", end=" ", flush=True)
        page.goto(TABLEAU_DASH_URL, timeout=120_000)
        page.wait_for_load_state("domcontentloaded", timeout=120_000)

        # Wait for Tableau to render
        try:
            page.wait_for_selector(".tab-vizHeader", timeout=90_000)
        except Exception:
            pass
        try:
            page.wait_for_function(
                r"() => Array.from(document.querySelectorAll('text'))"
                r".some(t => /^[A-Z][a-z]{2} \d{4}$/.test(t.textContent.trim()))",
                timeout=30_000,
            )
        except Exception:
            pass
        page.wait_for_timeout(4_000)
        print("ready.")

        # Find the bar chart and offense table canvases
        bar_canvas = _find_canvas(page, "Monthly Reported Offenses")
        count_canvas = _find_canvas(page, "Crime Category")

        if not bar_canvas or not count_canvas:
            print("  ERROR: could not locate canvases — AA scrape aborted.")
            browser.close()
            return {}

        # Capture all month label positions before clicking
        label_positions = _get_label_positions(page)
        print(f"  Dashboard months: {list(label_positions.keys())}")

        # For each month: click bar → wait → read AA → deselect
        for label, label_x in label_positions.items():
            ym = _parse_bar_label(label)
            if not ym:
                continue
            yr, mo = ym
            row_date = date(yr, mo, 1)
            if row_date < start or row_date >= end:
                continue

            print(f"    {label}...", end=" ", flush=True)

            # Click the bar to filter
            click_y = bar_canvas["y"] + bar_canvas["h"] * 0.4
            page.mouse.click(label_x, click_y)
            page.wait_for_timeout(3_000)  # wait for filter to apply

            # Read the AA count from tooltip
            count = _read_aa_tooltip(page, count_canvas)

            # Validate: reject obviously wrong values
            if count is not None and AA_MIN <= count <= AA_MAX:
                aa_counts[(yr, mo)] = count
                print(f"AA={count}")
            elif count is not None:
                print(f"REJECTED AA={count} (outside {AA_MIN}-{AA_MAX})")
                # Retry once after reset
                page.keyboard.press("Escape")
                page.wait_for_timeout(2_000)
                page.mouse.click(label_x, click_y)
                page.wait_for_timeout(3_000)
                count = _read_aa_tooltip(page, count_canvas)
                if count is not None and AA_MIN <= count <= AA_MAX:
                    aa_counts[(yr, mo)] = count
                    print(f"      Retry OK: AA={count}")
                else:
                    print(f"      Retry failed: AA={count}")
            else:
                print("(no reading)")

            # Deselect: press Escape then click empty area to fully reset
            page.keyboard.press("Escape")
            page.wait_for_timeout(1_500)

        browser.close()

    return aa_counts


def _find_canvas(page, aria_fragment):
    """Find the bounding rect of the main canvas in a tab-zone matching aria_fragment."""
    return page.evaluate(r"""
        (fragment) => {
            const fb = document.querySelector(`[aria-label*="${fragment}"]`);
            if (!fb) return null;
            let zone = fb;
            while (zone && !(zone.classList && zone.classList.contains('tab-zone')))
                zone = zone.parentElement;
            if (!zone) return null;
            const canvases = Array.from(zone.querySelectorAll('canvas'));
            if (!canvases.length) return null;
            canvases.sort((a, b) => (b.width * b.height) - (a.width * a.height));
            const r = canvases[0].getBoundingClientRect();
            return { x: r.x, y: r.y, w: r.width, h: r.height };
        }
    """, aria_fragment)


def _get_label_positions(page):
    """Return {label_str: x_center} for month labels on the bar chart x-axis."""
    return page.evaluate(r"""
        () => {
            const pattern = /^[A-Z][a-z]{2} \d{4}$/;
            const result = {};
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
            let node;
            while ((node = walker.nextNode())) {
                const txt = node.textContent.trim();
                if (pattern.test(txt) && !(txt in result)) {
                    const r = node.parentElement.getBoundingClientRect();
                    result[txt] = r.x + r.width / 2;
                }
            }
            return result;
        }
    """)


def _read_aa_tooltip(page, count_canvas):
    """Hover over the AA row in the offense table and read the tooltip count."""
    aa_header = (
        page.locator(".tab-vizHeader")
        .filter(has_text="Assault: Aggravated")
        .first
    )
    try:
        header_box = aa_header.bounding_box()
    except Exception:
        return None
    if not header_box:
        return None

    hover_x = count_canvas["x"] + count_canvas["w"] / 2
    hover_y = header_box["y"] + header_box["height"] / 2

    # Move away first to ensure a fresh tooltip
    page.mouse.move(0, 0)
    page.wait_for_timeout(500)
    page.mouse.move(hover_x, hover_y)
    page.wait_for_timeout(2_000)

    tooltip_text = page.evaluate(r"""
        () => {
            const sels = [
                '.tab-tooltip', '.tvTooltipCss', '[class*="Tooltip"]',
                '[role="tooltip"]', '[data-tb-test-id*="tooltip" i]',
                '[class*="tooltip" i]'
            ];
            for (const sel of sels) {
                for (const el of document.querySelectorAll(sel)) {
                    const s = window.getComputedStyle(el);
                    if (s.display !== 'none' && s.visibility !== 'hidden'
                            && s.opacity !== '0' && el.innerText.trim()) {
                        return el.innerText.trim();
                    }
                }
            }
            return null;
        }
    """)

    if not tooltip_text:
        return None

    m = re.search(r"Number of Offenses:\s*([\d,]+)", tooltip_text)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def build_json(csv_data, aa_counts):
    """Merge CSV data + AA counts into RTCI pipeline JSON format."""
    all_keys = sorted(set(csv_data.keys()) | set(aa_counts.keys()))
    records = []
    for yr, mo in all_keys:
        counts = csv_data.get((yr, mo), {})
        offenses = {
            "Murder": counts.get("Murder", 0),
            "Rape": counts.get("Rape", 0),
            "Robbery": counts.get("Robbery", 0),
            "Aggravated Assault": aa_counts.get((yr, mo), 0),
            "Burglary": counts.get("Burglary", 0),
            "Theft": counts.get("Theft", 0),
            "Motor Vehicle Theft": counts.get("Motor Vehicle Theft", 0),
        }
        for offense, count in offenses.items():
            if count > 0:
                records.append({
                    "agency": AGENCY,
                    "state": STATE,
                    "type": TYPE,
                    "year": yr,
                    "month": mo,
                    "offense": offense,
                    "count": count,
                })
    return records


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Portland PD monthly crime scraper")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--csv-only", action="store_true",
                        help="Skip dashboard scrape; use CSV AA counts (undercounted ~15%%)")
    args = parser.parse_args()

    print("=" * 60)
    print("  Portland PD — Monthly Crime Scraper")
    print("=" * 60)

    start, end = compute_window()
    print(f"Window: {start} to {end} (exclusive)\n")

    # Step 1: CSV data (all non-AA offenses)
    csv_data = scrape_csv_data(start, end)

    # Print CSV summary
    for (yr, mo) in sorted(csv_data.keys()):
        c = csv_data[(yr, mo)]
        print(f"  {yr}-{mo:02d}: Mur={c.get('Murder',0)}  Rape={c.get('Rape',0)}  "
              f"Rob={c.get('Robbery',0)}  Burg={c.get('Burglary',0)}  "
              f"Theft={c.get('Theft',0)}  MVT={c.get('Motor Vehicle Theft',0)}")

    # Step 2: AA from dashboard (or fall back to CSV AA)
    if args.csv_only:
        print("\n  --csv-only: using CSV AA counts (undercounted ~15%)")
        aa_counts = {}
        # Re-parse CSVs to include AA
        for yr_needed in sorted(set(range(start.year, end.year + 1))):
            csv_text = fetch_csv(yr_needed)
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                if row.get("OffenseType", "").strip() != "Aggravated Assault":
                    continue
                rmy = row.get("ReportMonthYear", "").strip()
                if not rmy:
                    continue
                try:
                    mn, ys = rmy.rsplit(" ", 1)
                    mo_n = MONTH_NAME[mn]
                    yr_n = int(ys)
                    rd = date(yr_n, mo_n, 1)
                except (KeyError, ValueError):
                    continue
                if rd < start or rd >= end:
                    continue
                cnt = int(row.get("OffenseCount") or 1)
                aa_counts[(yr_n, mo_n)] = aa_counts.get((yr_n, mo_n), 0) + cnt
    else:
        aa_counts = scrape_aa_from_dashboard(start, end, headless=not args.no_headless)

    print(f"\n  AA counts: {len(aa_counts)} months")
    for (yr, mo) in sorted(aa_counts.keys()):
        print(f"    {yr}-{mo:02d}: AA={aa_counts[(yr, mo)]}")

    # Step 3: Build output
    records = build_json(csv_data, aa_counts)
    print(f"\n  Total records: {len(records)}")

    if args.json:
        print(json.dumps(records, indent=2))
    else:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        with OUT_JSON.open("w") as f:
            json.dump(records, f, indent=2)
        print(f"  Saved to: {OUT_JSON.resolve()}")


if __name__ == "__main__":
    main()
