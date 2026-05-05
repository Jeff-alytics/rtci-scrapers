"""
Shared SSRS NIBRS Agency Crime Overview scraper (Colorado, North Dakota).
Both use identical RSReport.aspx layout with Year/Period/Jurisdiction/DateType dropdowns.
Each state provides: REPORT_URL, STATE, AGENCIES list.
"""

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

YEAR_SEL = "ctl00$MainContent$RptViewer$ctl08$ctl03$ddValue"
PERIOD_SEL = "ctl00$MainContent$RptViewer$ctl08$ctl05$ddValue"
JURIS_SEL = "ctl00$MainContent$RptViewer$ctl08$ctl07$ddValue"
DATE_SEL = "ctl00$MainContent$RptViewer$ctl08$ctl09$ddValue"

PERIOD_TO_MONTH = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

OFFENSE_MAP = {
    "Murder and Nonnegligent Manslaughter": "Murder",
    "All Rape": "Rape",
    "Aggravated Assault": "Aggravated Assault",
    "Robbery": "Robbery",
    "Burglary/Breaking & Entering": "Burglary",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

THEFT_OFFENSES = {
    "Pocket-picking", "Purse-snatching", "Shoplifting", "Theft From Building",
    "Theft From Coin Operated Machine or Device", "Theft From Motor Vehicle",
    "Theft of Motor Vehicle Parts/Accessories", "All Other Larceny",
}

ALL_TARGETS = set(OFFENSE_MAP.keys()) | THEFT_OFFENSES


def nine_month_window():
    now = datetime.now().replace(day=1)
    months = set()
    y, m = now.year, now.month - 1
    if m == 0: y, m = y - 1, 12
    for _ in range(9):
        months.add((y, m))
        m -= 1
        if m == 0: y, m = y - 1, 12
    return months


def needed_years(window):
    return sorted(set(y for y, m in window))


def wait_loading(page):
    try:
        page.wait_for_selector("text=Loading...", state="visible", timeout=3000)
    except Exception:
        pass
    try:
        page.wait_for_selector("text=Loading...", state="hidden", timeout=30000)
    except Exception:
        pass
    page.wait_for_timeout(2000)


def build_juris_map(page):
    opts = page.evaluate(f"""
    () => {{
        var sel = document.querySelector('[name="{JURIS_SEL}"]');
        if (!sel) return [];
        return Array.from(sel.options).map(o => ({{ val: o.value, text: o.text }}));
    }}
    """)
    m = {}
    for o in opts:
        text = o["text"].replace("\xa0", " ").strip()
        match = re.search(r" - ([A-Z0-9]+)$", text)
        if match:
            m[match.group(1)] = o["val"]
    return m


def get_available_periods(page, year_val):
    page.wait_for_timeout(500)
    page.locator(f"select[name='{YEAR_SEL}']").select_option(str(year_val))
    page.locator("input[value='View Report']").click()
    wait_loading(page)
    page.wait_for_timeout(300)
    opts = page.evaluate(f"""
    () => {{
        var sel = document.querySelector('[name="{PERIOD_SEL}"]');
        if (!sel) return [];
        return Array.from(sel.options).map(o => ({{ val: o.value, text: o.text.replace(/\\xa0/g,' ').trim() }}));
    }}
    """)
    periods = []
    for o in opts:
        label = o["text"].strip()
        if label in PERIOD_TO_MONTH:
            periods.append({"val": o["val"], "label": label, "month": PERIOD_TO_MONTH[label]})
    return periods


def fetch_report(page, year_val, period_val, juris_val):
    page.wait_for_timeout(300)
    page.locator(f"select[name='{YEAR_SEL}']").select_option(str(year_val))
    page.wait_for_timeout(300)
    page.locator(f"select[name='{PERIOD_SEL}']").select_option(str(period_val))
    page.wait_for_timeout(300)
    page.locator(f"select[name='{JURIS_SEL}']").select_option(str(juris_val))
    page.wait_for_timeout(300)
    page.locator("input[value='View Report']").click()
    wait_loading(page)
    return page.evaluate("() => document.body.innerText")


def parse_counts(text):
    lines = [ln.strip() for ln in text.splitlines()]
    counts = {}
    for i, line in enumerate(lines):
        if line in ALL_TARGETS:
            for j in range(i + 1, min(i + 5, len(lines))):
                c = lines[j].replace(",", "")
                if c.isdigit():
                    counts[line] = int(c)
                    break
    return counts


def build_rows(agency, state, year, month, counts):
    rows = []
    for offense_name, rtci in OFFENSE_MAP.items():
        val = counts.get(offense_name, 0)
        rows.append({"agency": agency["name"], "state": state, "type": agency["type"],
                      "year": year, "month": month, "offense": rtci, "count": val or 0})
    theft = sum(counts.get(n, 0) for n in THEFT_OFFENSES)
    rows.append({"agency": agency["name"], "state": state, "type": agency["type"],
                  "year": year, "month": month, "offense": "Theft", "count": theft})
    return rows


def run_scraper(report_url, state, agencies, out_json):
    window = nine_month_window()
    years = needed_years(window)
    target_months = {(y, m) for y, m in window}
    print(f"9-month window: {sorted(window)}")
    print(f"Years: {years}, {len(agencies)} {state} agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        # Load report + build jurisdiction map
        for attempt in range(3):
            try:
                page.goto(report_url, wait_until="networkidle", timeout=60000)
                break
            except Exception:
                if attempt == 2:
                    raise
        page.locator(f"select[name='{DATE_SEL}']").select_option("1", timeout=30000)

        juris_map = build_juris_map(page)
        print(f"Jurisdiction map: {len(juris_map)} agencies")

        year_opts = page.evaluate(f"""
        () => {{
            var sel = document.querySelector('[name="{YEAR_SEL}"]');
            if (!sel) return [];
            return Array.from(sel.options).map(o => ({{ val: o.value, text: o.text.trim() }}));
        }}
        """)
        year_val_map = {int(o["text"]): o["val"] for o in year_opts if o["text"].isdigit()}
        print(f"Year map: {year_val_map}\n")

        for ag in agencies:
            ori = ag["ori"]
            juris_val = juris_map.get(ori)
            if not juris_val:
                print(f"{ag['name']}: ORI {ori} not in dropdown — skipping")
                continue
            print(f"{ag['name']}...", flush=True)

            # Reload per agency for clean SSRS state
            try:
                page.goto(report_url, wait_until="networkidle", timeout=60000)
                page.locator(f"select[name='{DATE_SEL}']").select_option("1", timeout=30000)
            except Exception as e:
                print(f"  reload failed: {e}")
                continue

            for year in years:
                yv = year_val_map.get(year)
                if not yv:
                    print(f"  {year}: not in dropdown")
                    continue

                periods = get_available_periods(page, yv)
                target_periods = [p for p in periods if (year, p["month"]) in target_months]
                if not target_periods:
                    continue

                count = 0
                for p in target_periods:
                    try:
                        body = fetch_report(page, yv, p["val"], juris_val)
                        if not body:
                            continue
                        counts = parse_counts(body)
                        if not counts:
                            continue
                        rows = build_rows(ag, state, year, p["month"], counts)
                        all_rows.extend(rows)
                        count += len(rows)
                    except Exception as e:
                        print(f"  {p['label']} {year}: ERROR {e}")
                        try:
                            page.goto(report_url, wait_until="networkidle", timeout=60000)
                            page.locator(f"select[name='{DATE_SEL}']").select_option("1", timeout=30000)
                        except Exception:
                            pass

                print(f"  {year}: {count} records")

        browser.close()

    if not all_rows:
        print("No data collected.")
        return

    out = Path(out_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(all_rows, f)
    ags = set(r["agency"] for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(ags)} agencies) to {out}")
