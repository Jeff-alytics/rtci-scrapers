"""
Minnesota Crime Data Explorer Scraper
======================================
Source: https://cde.state.mn.us/CrimesAgainstPerson/CrimesAgainstPerson
        https://cde.state.mn.us/CrimesAgainstProperty/CrimesAgainstProperty
Uses tableauscraper to extract monthly crime data from Tableau dashboards.
Outputs data/latest.json in RTCI pipeline format.

Usage:
    python scrape.py            # save to data/latest.json
    python scrape.py --json     # output JSON to stdout
"""

import json
import argparse
import time
from datetime import date
from pathlib import Path
from tableauscraper import TableauScraper as TS

# ── Config ───────────────────────────────────────────────────────────────────

PERSON_URL = "https://cde.state.mn.us/views/MNCDE-CrimesAgainstPerson/CrimesAgainstPerson"
PROPERTY_URL = "https://cde.state.mn.us/views/MNCDE-CrimesAgainstProperty/CrimesAgainstProperty"

STATE = "MN"
WINDOW_MONTHS = 9

OUTPUT_DIR = Path(__file__).parent
OUT_JSON = OUTPUT_DIR / "data" / "latest.json"

# Agency name in Tableau filter → (display name, type)
AGENCIES = {
    "Apple Valley Police Dept (MN0191000)":    ("Apple Valley", "City"),
    "Blaine Police Dept (MN0020200)":          ("Blaine", "City"),
    "Bloomington Police Dept (MN0270100)":     ("Bloomington", "City"),
    "Brooklyn Park Police Dept (MN0270300)":   ("Brooklyn Park", "City"),
    "Burnsville Police Dept (MN0190100)":      ("Burnsville", "City"),
    "Coon Rapids Police Dept (MN0020500)":     ("Coon Rapids", "City"),
    "Duluth Police Dept (MN0690600)":          ("Duluth", "City"),
    "Eagan Police Dept (MN0190800)":           ("Eagan", "City"),
    "Eden Prairie Police Dept (MN0272600)":    ("Eden Prairie", "City"),
    "Edina Police Dept (MN0270600)":           ("Edina", "City"),
    "Lakeville Police Dept (MN0191100)":       ("Lakeville", "City"),
    "Maple Grove Police Dept (MN0272700)":     ("Maple Grove", "City"),
    "Minneapolis Police Dept (MN0271100)":     ("Minneapolis", "City"),
    "Minnetonka Police Dept (MN0271200)":      ("Minnetonka", "City"),
    "Plymouth Police Dept (MN0271700)":        ("Plymouth", "City"),
    "Rochester Police Dept (MN0550100)":       ("Rochester", "City"),
    "St Cloud Police Dept (MN0730400)":        ("St. Cloud", "City"),
    "St Paul Police Dept (MN0620900)":         ("St. Paul", "City"),
    "Woodbury Police Dept (MN0821100)":        ("Woodbury", "City"),
    "Wright County Sheriff (MN0860000)":       ("Wright", "County"),
}

# Tableau offense name(s) → RTCI offense
# Person crimes dashboard
PERSON_OFFENSES = {
    "Murder": ["Murder & Non-negligent Manslaughter"],
    "Rape": ["Rape", "Sodomy", "Sexual Assault With An Object"],
    "Aggravated Assault": ["Aggravated Assault"],
}

# Property crimes dashboard
PROPERTY_OFFENSES = {
    "Robbery": ["Robbery"],
    "Burglary": ["Burglary/Breaking & Entering"],
    "Theft": [
        "Shoplifting",
        "All Other Larceny",
        "Theft From Motor Vehicle",
        "Theft From Building",
        "Theft of Motor Vehicle Parts or Accessories",
        "Theft From Coin-Operated Machine or Device",
        "Purse-snatching",
        "Pocket-picking",
    ],
    "Motor Vehicle Theft": ["Motor Vehicle Theft"],
}

MONTH_ABBR = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_window():
    """Return (start_date, end_date) for the rolling 24-month window."""
    today = date.today()
    if today.month == 1:
        end = date(today.year - 1, 12, 1)
    else:
        end = date(today.year, today.month - 1, 1)
    start_month = end.month - (WINDOW_MONTHS - 1)
    start_year = end.year
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    return date(start_year, start_month, 1), end


def load_dashboard(url):
    """Load a Tableau dashboard and return the workbook."""
    ts = TS()
    ts.loads(url)
    return ts.getWorkbook()


def scrape_offense_monthly(wb, agency_filter, offense_name):
    """
    Apply agency + offense filters and return dict of {(year, month): count}.
    """
    results = {}

    # Apply agency filter
    ws = wb.getWorksheet("Offenses")
    filtered = ws.setFilter("Agency (ORI)", agency_filter, dashboardFilter=True)

    # Apply offense filter
    ws2 = filtered.getWorksheet("Offenses")
    filtered2 = ws2.setFilter("Offense", offense_name, dashboardFilter=True)

    # Read monthly data
    for w in filtered2.worksheets:
        if w.name == "Offenses by Month":
            df = w.data
            for _, row in df.iterrows():
                month_str = str(row.get("Month Name Short-value", "")).strip()
                year_val = row.get("Year-value", None)
                count_val = row.get("AGG(Offenses)-alias", 0)

                if month_str not in MONTH_ABBR or year_val is None:
                    continue

                month_num = MONTH_ABBR[month_str]
                year_num = int(float(year_val))
                count = int(float(count_val)) if count_val else 0

                key = (year_num, month_num)
                results[key] = results.get(key, 0) + count
            break

    return results


def scrape_dashboard(url, agency_filter, offense_map, agency_name, agency_type,
                     start_date, end_date):
    """Scrape one dashboard for one agency, return RTCI records."""
    records = []
    wb = load_dashboard(url)

    for rtci_offense, tableau_offenses in offense_map.items():
        monthly_totals = {}

        for tab_offense in tableau_offenses:
            try:
                counts = scrape_offense_monthly(wb, agency_filter, tab_offense)
                for key, val in counts.items():
                    monthly_totals[key] = monthly_totals.get(key, 0) + val
            except Exception as e:
                print(f"    WARNING: {agency_name} / {tab_offense}: {e}")
                continue

            # Small delay between offense queries
            time.sleep(0.5)

        # Emit records within window
        for (year, month), count in sorted(monthly_totals.items()):
            month_date = date(year, month, 1)
            if month_date < start_date or month_date > end_date:
                continue
            if count > 0:
                records.append({
                    "agency": agency_name,
                    "state": STATE,
                    "type": agency_type,
                    "year": year,
                    "month": month,
                    "offense": rtci_offense,
                    "count": count,
                })

    return records


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Minnesota CDE crime scraper")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    print("=" * 55)
    print("  Minnesota — Crime Data Explorer Tableau Scraper")
    print("=" * 55)

    start_date, end_date = get_window()
    print(f"Window: {start_date} to {end_date}")
    print(f"Agencies: {len(AGENCIES)}")

    all_records = []

    for agency_filter, (agency_name, agency_type) in AGENCIES.items():
        print(f"\n  [{agency_name}]")

        # Person crimes (Murder, Rape, Aggravated Assault)
        try:
            print(f"    Person crimes dashboard...")
            person_records = scrape_dashboard(
                PERSON_URL, agency_filter, PERSON_OFFENSES,
                agency_name, agency_type, start_date, end_date
            )
            print(f"      -> {len(person_records)} records")
            all_records.extend(person_records)
        except Exception as e:
            print(f"    ERROR (person): {e}")

        # Small delay between dashboards
        time.sleep(1)

        # Property crimes (Robbery, Burglary, Theft, MVT)
        try:
            print(f"    Property crimes dashboard...")
            property_records = scrape_dashboard(
                PROPERTY_URL, agency_filter, PROPERTY_OFFENSES,
                agency_name, agency_type, start_date, end_date
            )
            print(f"      -> {len(property_records)} records")
            all_records.extend(property_records)
        except Exception as e:
            print(f"    ERROR (property): {e}")

        # Delay between agencies
        time.sleep(1)

    all_records.sort(key=lambda r: (r["agency"], r["year"], r["month"], r["offense"]))
    print(f"\nTotal: {len(all_records)} records across {len(AGENCIES)} agencies")

    if args.json:
        print(json.dumps(all_records, indent=2))
    else:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        with OUT_JSON.open("w") as f:
            json.dump(all_records, f, indent=2)
        print(f"Saved to: {OUT_JSON.resolve()}")


if __name__ == "__main__":
    main()
