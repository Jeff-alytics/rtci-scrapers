"""
Massachusetts SRS Crime Scraper — Beyond2020 SSRS Report 584
Source: https://ma.beyond2020.com/ma_public/View/RSReport.aspx?ReportId=584
SRS format: one agency at a time via ORI dropdown, report text parsed.
Outputs massachusetts/data/latest.json in RTCI pipeline format.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

from playwright.sync_api import sync_playwright

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATE = "MA"
REPORT_URL = "https://ma.beyond2020.com/ma_public/View/RSReport.aspx?ReportId=584"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

# SSRS ReportViewer selectors
ORI_SELECT = "select[name='ctl00$MainContent$RptViewer$ctl08$ctl03$ddValue']"
PERIOD_SELECT = "select[name='ctl00$MainContent$RptViewer$ctl08$ctl05$ddValue']"
VIEW_BUTTON = "input[id='ctl00_MainContent_RptViewer_ctl08_ctl00']"

# 29 MA agencies from Agency Sourcing spreadsheet — {name: (type, ORI)}
AGENCIES = {
    "Barnstable": ("City", "MA0010100"),
    "Boston": ("City", "MA0130100"),
    "Brockton": ("City", "MA0120300"),
    "Brookline": ("City", "MA0110400"),
    "Cambridge": ("City", "MA0091100"),
    "Chicopee": ("City", "MA0070500"),
    "Everett": ("City", "MA0091700"),
    "Fall River": ("City", "MA0030800"),
    "Framingham": ("City", "MA0091800"),
    "Haverhill": ("City", "MA0051100"),
    "Lawrence": ("City", "MA0051300"),
    "Lowell": ("City", "MA0092600"),
    "Lynn": ("City", "MA0051400"),
    "Malden": ("City", "MA0092700"),
    "Medford": ("City", "MA0093000"),
    "Methuen": ("City", "MA0051900"),
    "New Bedford": ("City", "MA0031100"),
    "Newton": ("City", "MA0093300"),
    "Peabody": ("City", "MA0052500"),
    "Pittsfield": ("City", "MA0022200"),
    "Plymouth": ("City", "MA0122000"),
    "Quincy": ("City", "MA0112000"),
    "Revere": ("City", "MA0130400"),
    "Somerville": ("City", "MA0093900"),
    "Springfield": ("City", "MA0071800"),
    "Taunton": ("City", "MA0031900"),
    "Waltham": ("City", "MA0094700"),
    "Weymouth": ("City", "MA0112700"),
    "Worcester": ("City", "MA0146000"),
}

# Map report text labels to RTCI offense names (skip arson)
CRIME_MAP = {
    "a. Murder and Nonnegligent Homicide": "Murder",
    "2. Forcible Rape Total": "Rape",
    "3. Robbery Total": "Robbery",
    "Aggravated Assault Total": "Aggravated Assault",
    "5. Burglary Total": "Burglary",
    "6. Larceny - Theft Total": "Theft",
    "7. Motor Vehicle Theft Total": "Motor Vehicle Theft",
}

MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]


def normalize(text):
    """Normalize non-breaking spaces and whitespace variants to plain ASCII."""
    return re.sub(r"[\xa0\u2002\u2003\u200b]+", " ", text).strip()


def six_month_window():
    """Return list of (year, month) tuples for the last 6 months,
    skipping the most recent month (agencies often haven't reported yet).
    In May 2026 this gives Oct 2025 through Mar 2026.
    """
    now = datetime.now().replace(day=1)
    months = []
    # Start from 2 months back (skip most recent incomplete month)
    y, m = now.year, now.month - 2
    if m <= 0:
        y -= 1
        m += 12
    for _ in range(6):
        months.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return sorted(months)


def enumerate_dropdown(page, selector):
    """Return list of {value, text} for all <option> elements in a <select>."""
    return page.evaluate("""(sel) => {
        var s = document.querySelector(sel);
        if (!s) return [];
        return Array.from(s.options).map(function(o) {
            return {value: o.value, text: o.text.trim()};
        });
    }""", selector)


def match_agencies(ori_options):
    """Match our agency list to ORI dropdown values by ORI code.
    Dropdown text format: "Boston - MA0130100" (with non-breaking spaces).
    Returns {agency_name: dropdown_value} for matched agencies.
    """
    # Build ORI code -> dropdown value lookup
    ori_to_val = {}
    for opt in ori_options:
        text = normalize(opt["text"])
        # Extract ORI code (MA followed by 7 digits)
        m = re.search(r"(MA\d{7})", text)
        if m:
            ori_to_val[m.group(1)] = opt["value"]

    matched = {}
    for agency_name, (agency_type, ori_code) in AGENCIES.items():
        if ori_code in ori_to_val:
            matched[agency_name] = ori_to_val[ori_code]
        else:
            print(f"  WARNING: ORI {ori_code} ({agency_name}) not found in dropdown",
                  file=sys.stderr)
    return matched


def match_periods(period_options, window):
    """Match (year, month) tuples to period dropdown values.
    Dropdown text format: "Apr 2026" (abbreviated months, non-breaking spaces).
    Returns {(year, month): dropdown_value}.
    """
    matched = {}
    for opt in period_options:
        text = normalize(opt["text"])
        val = opt["value"]
        for y, m in window:
            if (y, m) in matched:
                continue
            abbr = MONTH_ABBR[m - 1]
            # Match "Apr 2026" or "April 2026"
            if (abbr in text or MONTH_NAMES[m - 1] in text) and str(y) in text:
                # Skip year totals ("2026") and half-years ("H1 2026")
                if text.startswith("H") or text == str(y):
                    continue
                matched[(y, m)] = val
    return matched


def parse_report_text(report_text):
    """Parse the SSRS report text for crime counts.
    Returns {offense_name: count} dict.
    """
    if not report_text:
        return None

    text = report_text.replace("\r", "")
    text = re.sub(r"\n\t", "\t", text)
    text = re.sub(r"\t\n", "\t", text)
    text = re.sub(r"\t+", "\t", text)

    crimes = {}
    for line in text.split("\n"):
        line = line.strip()
        for label, offense_name in CRIME_MAP.items():
            if line.startswith(label):
                rest = line[len(label):]
                nums = re.findall(r"[\d,]+", rest)
                reported = int(nums[0].replace(",", "")) if nums else 0
                crimes[offense_name] = reported
                break
    return crimes if crimes else None


def extract_report_text(page):
    """Extract the report text from the rendered SSRS report."""
    return page.evaluate("""() => {
        var node = document.evaluate(
            "//text()[contains(.,'Grand Total')]",
            document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null
        ).singleNodeValue;
        if (!node) return null;
        var el = node.parentElement;
        for (var i = 0; i < 5; i++) { if (el.parentElement) el = el.parentElement; }
        return el.innerText;
    }""")


def scrape_agency_month(page, ori_val, period_val, retries=2):
    """Load report for one agency/month combo.  Returns parsed crimes dict or None."""
    for attempt in range(retries + 1):
        try:
            # Capture current report text so we can detect when it changes
            old_text = extract_report_text(page) or ""

            page.select_option(ORI_SELECT, ori_val)
            sleep(0.3)
            page.select_option(PERIOD_SELECT, period_val)
            sleep(0.3)
            page.click(VIEW_BUTTON)

            # Wait for report to actually refresh — poll until Grand Total
            # text changes or appears fresh (old text was from previous query)
            deadline = 30
            elapsed = 0
            while elapsed < deadline:
                sleep(0.5)
                elapsed += 0.5
                try:
                    new_text = extract_report_text(page)
                    if new_text and new_text != old_text:
                        break
                except Exception:
                    pass
            else:
                # Timed out waiting for change — try extracting anyway
                new_text = extract_report_text(page)

            sleep(0.3)
            report_text = extract_report_text(page)
            return parse_report_text(report_text)
        except Exception as e:
            if attempt < retries:
                print(f"    retry {attempt + 1}... ({e})", file=sys.stderr)
                sleep(2)
                try:
                    page.goto(REPORT_URL, timeout=30000)
                    page.wait_for_selector(ORI_SELECT, timeout=15000)
                    sleep(1)
                except Exception:
                    pass
            else:
                print(f"    FAILED after {retries + 1} attempts: {e}", file=sys.stderr)
                return None


def main():
    window = six_month_window()
    print(f"6-month window: {window}")
    print(f"{len(AGENCIES)} MA agencies\n")

    all_rows = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(ignore_https_errors=True).new_page()

        # Load report page
        page.goto(REPORT_URL, timeout=30000)
        page.wait_for_selector(ORI_SELECT, timeout=15000)
        sleep(1)

        # Enumerate dropdowns and build mappings
        ori_options = enumerate_dropdown(page, ORI_SELECT)
        period_options = enumerate_dropdown(page, PERIOD_SELECT)
        print(f"ORI dropdown: {len(ori_options)} options")
        print(f"Period dropdown: {len(period_options)} options")

        agency_map = match_agencies(ori_options)
        period_map = match_periods(period_options, window)

        unmatched = [a for a in AGENCIES if a not in agency_map]
        if unmatched:
            print(f"\nWARNING: {len(unmatched)} agencies not found in dropdown: {unmatched}")

        unmatched_periods = [(y, m) for y, m in window if (y, m) not in period_map]
        if unmatched_periods:
            print(f"WARNING: periods not found: {unmatched_periods}")

        print(f"\nMatched {len(agency_map)} agencies, {len(period_map)} periods")
        print()

        total = len(agency_map) * len(period_map)
        done = 0

        for agency_name in sorted(agency_map.keys()):
            ori_val = agency_map[agency_name]
            agency_type, agency_ori = AGENCIES[agency_name]
            print(f"{agency_name} ({agency_ori}):")

            for y, m in sorted(period_map.keys()):
                period_val = period_map[(y, m)]
                month_name = MONTH_NAMES[m - 1]
                done += 1
                print(f"  {month_name} {y} [{done}/{total}]...", end=" ", flush=True)

                crimes = scrape_agency_month(page, ori_val, period_val)
                if not crimes:
                    print("no data")
                    continue

                count = 0
                for offense, val in crimes.items():
                    all_rows.append({
                        "agency": agency_name,
                        "state": STATE,
                        "type": agency_type,
                        "year": y,
                        "month": m,
                        "offense": offense,
                        "count": val,
                    })
                    count += 1
                print(f"{count} offenses")

            # Small delay between agencies
            sleep(0.5)

        browser.close()

    if not all_rows:
        print("\nNo data collected.")
        return

    # Sort for deterministic output
    all_rows.sort(key=lambda r: (r["agency"], r["year"], r["month"], r["offense"]))

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f, indent=2)

    agencies = set(r["agency"] for r in all_rows)
    months = set((r["year"], r["month"]) for r in all_rows)
    print(f"\nWrote {len(all_rows)} records ({len(agencies)} agencies, {len(months)} months) to {OUT_JSON}")


if __name__ == "__main__":
    main()
