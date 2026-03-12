"""
ARJIS Crime Statistics Scraper
https://crimestats.arjis.org/default.aspx

Downloads one XLS file per available month from ARJIS (San Diego area agencies),
reshapes from wide to long format, aggregates sub-crimes into RTCI offenses,
and outputs JSON in the pipeline format.

Usage:
    python scrape.py                  # full scrape (all months)
    python scrape.py --recent 24      # last 24 months only
    python scrape.py --json           # output JSON to stdout

Requirements:
    pip install requests pandas lxml html5lib beautifulsoup4
"""

import re
import sys
import json
import time
import argparse
import requests
import urllib3
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

URL = "https://crimestats.arjis.org/default.aspx"

OUTPUT_DIR = Path(__file__).parent
XLS_DIR    = OUTPUT_DIR / "monthly_xls"
OUT_JSON   = OUTPUT_DIR / "data" / "latest.json"

# ARJIS agency name (uppercase in XLS) → (display name, state, type)
AGENCIES = {
    "CARLSBAD":       ("Carlsbad",              "CA", "City"),
    "CHULA VISTA":    ("Chula Vista",           "CA", "City"),
    "CORONADO":       ("Coronado",              "CA", "City"),
    "COUNTY SHERIFF": ("San Diego",             "CA", "County"),
    "EL CAJON":       ("El Cajon",              "CA", "City"),
    "ESCONDIDO":      ("Escondido",             "CA", "City"),
    "LA MESA":        ("La Mesa",               "CA", "City"),
    "NATIONAL CITY":  ("National City",         "CA", "City"),
    "OCEANSIDE":      ("Oceanside",             "CA", "City"),
    "SAN DIEGO":      ("San Diego",             "CA", "City"),
}

# Crime rows to keep (after stripping ** footnote markers)
CRIME_COLS = {
    "Murder":                   "murder",
    "Rape":                     "rape",
    "Armed Robbery":            "armed_robbery",
    "Strong Arm Robbery":       "strong_arm_robbery",
    "Aggravated Assault":       "aggravated_assault",
    "Residential Burglary":     "residential_burglary",
    "Non-Residential Burglary": "non_residential_burglary",
    "Theft >= $400":            "theft_400_plus",
    "Theft < $400":             "theft_under_400",
    "Motor Vehicle Theft":      "motor_vehicle_theft",
}

# Aggregation: ARJIS sub-crimes → RTCI offense names
OFFENSE_MAP = {
    "murder":                   "Murder",
    "rape":                     "Rape",
    "armed_robbery":            "Robbery",
    "strong_arm_robbery":       "Robbery",
    "aggravated_assault":       "Aggravated Assault",
    "residential_burglary":     "Burglary",
    "non_residential_burglary": "Burglary",
    "theft_400_plus":           "Theft",
    "theft_under_400":          "Theft",
    "motor_vehicle_theft":      "Motor Vehicle Theft",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_hidden_fields(html: str) -> dict:
    fields = {}
    for m in re.finditer(r'<input[^>]+type=["\']hidden["\'][^>]*>', html, re.IGNORECASE):
        tag = m.group()
        name = re.search(r'name=["\']([^"\']+)["\']', tag)
        val  = re.search(r'value=["\']([^"\']*)["\']', tag)
        if name:
            fields[name.group(1)] = val.group(1) if val else ""
    return fields


def get_begin_date_options(html: str) -> list:
    m = re.search(
        r'<select[^>]+name="ddBeginDate"[^>]*>(.*?)</select>', html, re.DOTALL
    )
    if not m:
        return []
    opts = re.findall(r'<option[^>]+value="([^"]+)"', m.group(1))
    return [o for o in opts if o != "Month / Year"]


def build_payload(hidden, begin, eventtarget, extra=None):
    payload = {
        **hidden,
        "__EVENTTARGET":   eventtarget,
        "__EVENTARGUMENT": "",
        "ddBeginDate":     begin,
        "ddEndDate":       begin,
        "ddAgency":        "All Agencies",
    }
    if extra:
        payload.update(extra)
    return payload


def parse_xls_response(content: bytes, year: int, month: int):
    html = content.decode("utf-8", errors="replace")
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None
    if not tables:
        return None

    df = tables[0]
    df = df.drop(columns=[c for c in ["SORT_ORDER", "TOTAL"] if c in df.columns])
    df["CRIME"] = df["CRIME"].str.replace(r"\*+", "", regex=True).str.strip()
    df = df[df["CRIME"].isin(CRIME_COLS.keys())].copy()
    if df.empty:
        return None

    agency_cols = [c for c in df.columns if c != "CRIME"]
    melted = df.melt(
        id_vars="CRIME", value_vars=agency_cols,
        var_name="agency_raw", value_name="value"
    )
    pivoted = (
        melted
        .pivot_table(index="agency_raw", columns="CRIME", values="value", aggfunc="first")
        .reset_index()
    )
    pivoted.columns.name = None
    pivoted = pivoted.rename(columns=CRIME_COLS)
    pivoted["year"]  = year
    pivoted["month"] = month
    return pivoted


def to_pipeline_json(df: pd.DataFrame) -> list:
    """Convert wide DataFrame to pipeline JSON format: [{agency, state, type, year, month, offense, count}]"""
    records = []
    crime_cols = [c for c in CRIME_COLS.values() if c in df.columns]

    for _, row in df.iterrows():
        agency_raw = row["agency_raw"]
        info = AGENCIES.get(agency_raw)
        if not info:
            continue

        name, state, atype = info
        year  = int(row["year"])
        month = int(row["month"])

        # Aggregate sub-crimes into RTCI offenses
        offense_totals = {}
        for col in crime_cols:
            val = row.get(col, 0)
            if pd.isna(val):
                val = 0
            offense = OFFENSE_MAP[col]
            offense_totals[offense] = offense_totals.get(offense, 0) + int(val)

        for offense, count in offense_totals.items():
            records.append({
                "agency": name,
                "state":  state,
                "type":   atype,
                "year":   year,
                "month":  month,
                "offense": offense,
                "count":  count,
            })

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scrape(recent_months=None):
    XLS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print("Loading page...", file=sys.stderr)
    r = session.get(URL, verify=False)
    html = r.text

    date_options = get_begin_date_options(html)
    if not date_options:
        print("ERROR: could not find date options on page.", file=sys.stderr)
        return []

    # Filter to recent months if requested
    if recent_months:
        date_options = date_options[-recent_months:]

    print(f"Scraping {len(date_options)} months: {date_options[0]} to {date_options[-1]}", file=sys.stderr)

    all_frames = []

    for i, date_opt in enumerate(date_options, start=1):
        dt    = datetime.strptime(date_opt, "%b / %Y")
        year  = dt.year
        month = dt.month
        tag   = dt.strftime("%Y_%m")
        xls_path = XLS_DIR / f"CrimeStats_{tag}.xls"

        try:
            hidden1  = get_hidden_fields(html)
            payload1 = build_payload(hidden1, date_opt, eventtarget="ddBeginDate")
            r1       = session.post(URL, data=payload1, verify=False)
            html1    = r1.text

            hidden2  = get_hidden_fields(html1)
            payload2 = build_payload(
                hidden2, date_opt, eventtarget="btnExport",
                extra={"btnExport": "Export To Excel"}
            )
            r2 = session.post(URL, data=payload2, verify=False)

            if "ms-excel" not in r2.headers.get("Content-Type", ""):
                print(f"  [{i}/{len(date_options)}] {date_opt}: unexpected response, skipping", file=sys.stderr)
                html = html1
                continue

            xls_path.write_bytes(r2.content)

            df = parse_xls_response(r2.content, year, month)
            if df is not None and not df.empty:
                all_frames.append(df)
                print(f"  [{i}/{len(date_options)}] {date_opt}: {len(df)} agencies", file=sys.stderr)
            else:
                print(f"  [{i}/{len(date_options)}] {date_opt}: empty", file=sys.stderr)

            html = html1
            time.sleep(0.25)

        except Exception as e:
            print(f"  [{i}/{len(date_options)}] {date_opt}: ERROR - {e}", file=sys.stderr)
            time.sleep(1)

    if not all_frames:
        print("No data collected.", file=sys.stderr)
        return []

    combined = pd.concat(all_frames, ignore_index=True)
    records = to_pipeline_json(combined)
    records.sort(key=lambda r: (r["agency"], r["year"], r["month"], r["offense"]))

    print(f"Total: {len(records)} records from {len(all_frames)} month-files", file=sys.stderr)
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent", type=int, help="Only scrape last N months")
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout")
    args = parser.parse_args()

    records = scrape(recent_months=args.recent)

    if args.json:
        print(json.dumps(records, indent=2))
    else:
        # Save to file
        with open(OUT_JSON, "w") as f:
            json.dump(records, f, indent=2)
        print(f"Saved {len(records)} records to {OUT_JSON}", file=sys.stderr)
