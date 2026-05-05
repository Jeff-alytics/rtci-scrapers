"""
Oregon Crime Scraper — OSP Open Data CSV (NIBRS offense counts).
Source: oregon.gov/osp/pages/uniform-crime-reporting-data.aspx

Downloads the statewide NIBRS offenses CSV (~100 MB), filters to RTCI agencies
and Part I crime categories, sums Distinct Offenses by agency/year/month/crime.
Outputs oregon/data/latest.json in RTCI pipeline format.
"""

import io
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STATE = "OR"
OFFENSES_URL = "https://www.oregon.gov/osp/Docs/Open-Data/OpenData-Offenses-All.csv"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"

AGENCIES = [
    {"csv_name": "Albany PD", "name": "Albany", "type": "City"},
    {"csv_name": "Beaverton PD MIP", "name": "Beaverton", "type": "City"},
    {"csv_name": "Bend PD MIP", "name": "Bend", "type": "City"},
    {"csv_name": "Clackamas SO", "name": "Clackamas", "type": "County"},
    {"csv_name": "Corvallis PD", "name": "Corvallis", "type": "City"},
    {"csv_name": "Eugene PD MIP", "name": "Eugene", "type": "City"},
    {"csv_name": "Gresham PD MIP", "name": "Gresham", "type": "City"},
    {"csv_name": "Hillsboro PD MIP", "name": "Hillsboro", "type": "City"},
    {"csv_name": "Medford PD", "name": "Medford", "type": "City"},
    {"csv_name": "Salem PD MIP", "name": "Salem", "type": "City"},
    {"csv_name": "Springfield PD", "name": "Springfield", "type": "City"},
    {"csv_name": "Tigard PD", "name": "Tigard", "type": "City"},
    {"csv_name": "Washington SO", "name": "Washington", "type": "County"},
]

CRIME_MAP = {
    "Willful Murder": "Murder",
    "Forcible Rape": "Rape",
    "Robbery": "Robbery",
    "Aggravated Assault": "Aggravated Assault",
    "Burglary": "Burglary",
    "Larceny/Theft Offenses": "Theft",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
}

CSV_NAME_TO_AGENCY = {ag["csv_name"]: ag for ag in AGENCIES}


def nine_month_window():
    now = datetime.now().replace(day=1)
    months = set()
    y, m = now.year, now.month - 1
    if m == 0:
        y, m = y - 1, 12
    for _ in range(9):
        months.add((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return months


def main():
    window = nine_month_window()
    print(f"9-month window: {sorted(window)}")
    print(f"{len(AGENCIES)} OR agencies\n")

    print("Downloading Oregon offenses CSV...")
    t0 = time.time()
    r = requests.get(OFFENSES_URL, timeout=300)
    r.raise_for_status()
    print(f"  {len(r.content) / 1e6:.1f} MB in {time.time() - t0:.1f}s")

    df = pd.read_csv(io.StringIO(r.text))
    print(f"  {len(df):,} total rows")

    # Filter to target agencies and crimes
    df = df[df["Agency Name"].isin(CSV_NAME_TO_AGENCY.keys())]
    df = df[df["NIBRS Report Title"].isin(CRIME_MAP.keys())]

    # Parse dates, filter to window
    df["IncidentDate"] = pd.to_datetime(df["IncidentDate"])
    df["year"] = df["IncidentDate"].dt.year
    df["month"] = df["IncidentDate"].dt.month
    df = df[df.apply(lambda row: (row["year"], row["month"]) in window, axis=1)]
    print(f"  {len(df):,} rows after filtering")

    # Aggregate
    agg = (
        df.groupby(["Agency Name", "year", "month", "NIBRS Report Title"])["Distinct Offenses"]
        .sum()
        .reset_index()
    )

    all_rows = []
    for _, row in agg.iterrows():
        ag = CSV_NAME_TO_AGENCY[row["Agency Name"]]
        all_rows.append({
            "agency": ag["name"], "state": STATE, "type": ag["type"],
            "year": int(row["year"]), "month": int(row["month"]),
            "offense": CRIME_MAP[row["NIBRS Report Title"]],
            "count": int(row["Distinct Offenses"]),
        })

    # Fill missing combos with 0
    all_offenses = list(CRIME_MAP.values())
    existing = {(r["agency"], r["year"], r["month"], r["offense"]) for r in all_rows}
    for ag in AGENCIES:
        for y, m in window:
            for off in all_offenses:
                if (ag["name"], y, m, off) not in existing:
                    all_rows.append({
                        "agency": ag["name"], "state": STATE, "type": ag["type"],
                        "year": y, "month": m, "offense": off, "count": 0,
                    })

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)

    agencies = set(r["agency"] for r in all_rows)
    months_with_data = sorted(set((r["year"], r["month"]) for r in all_rows if r["count"] > 0))
    print(f"\nWrote {len(all_rows)} records ({len(agencies)} agencies) to {OUT_JSON}")
    print(f"Months with data: {months_with_data}")


if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"Elapsed: {time.time() - t0:.0f}s")
