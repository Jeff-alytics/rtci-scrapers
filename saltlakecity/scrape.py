"""
Salt Lake City Police Department - Monthly Crime Counts Scraper
Data source : SLCPD Crime Dashboard V1.0, embedded at
              https://police.slc.gov/open-data/crimestatistics/
              (Power BI Gov report, public resource key, no login)
Method      : Power BI backend API (querydata), no browser. The page moved from
              slcpd.com to police.slc.gov in 2026; the report key below is the
              one embedded on the new page.

Outputs the unified LONG-format JSON at saltlakecity/data/latest.json:
  {"agency": "Salt Lake City", "state": "UT", "type": "City",
   "year": 2026, "month": 3, "offense": "Murder", "count": 1}
One record per (year, month, offense). MERGES with the existing latest.json
(upsert on (year, month, offense)) so older history is preserved.

Counts: distinct RMS_data[id] per Calendar year/month and off_cstat_cat, with
RMS_data[Citywide?] = 'Citywide' (the dashboard's default "Citywide" area
selection). This matches the dashboard's own DistCnt measures.

Crime name -> UCR mapping
  Criminal Homicide               -> Murder
  Rape                            -> Rape
  Robbery - Business / All Other  -> Robbery
  Aggravated Assault - Family / NonFamily -> Aggravated Assault
  Burglary - Residential / All Other      -> Burglary
  Larceny - Vehicle Burg / Larceny/Theft - Multi -> Theft
  Motor Vehicle Theft             -> Motor Vehicle Theft
"""

import json
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RK = "8fb0f31e-091d-4686-9aab-646db6f6b6a3"
BASE = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports"
HEADERS = {
    "X-PowerBI-ResourceKey": RK,
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
}

AGENCY = "Salt Lake City"
STATE = "UT"
TYPE_VALUE = "City"

OUT_JSON = Path(__file__).parent / "data" / "latest.json"

OFFENSES = [
    "Murder", "Rape", "Robbery", "Aggravated Assault",
    "Burglary", "Theft", "Motor Vehicle Theft",
]

CRIME_MAP = {
    "Criminal Homicide":              "Murder",
    "Rape":                           "Rape",
    "Robbery - Business":             "Robbery",
    "Robbery - All Other":            "Robbery",
    "Aggravated Assault - Family":    "Aggravated Assault",
    "Aggravated Assault - NonFamily": "Aggravated Assault",
    "Burglary - Residential":         "Burglary",
    "Burglary - All Other":           "Burglary",
    "Larceny - Vehicle Burg":         "Theft",
    "Larceny/Theft - Multi":          "Theft",
    "Motor Vehicle Theft":            "Motor Vehicle Theft",
}


def _col(src, prop, name):
    return {"Column": {"Expression": {"SourceRef": {"Source": src}}, "Property": prop}, "Name": name}


def _in(src, prop, literals):
    return {"Condition": {"In": {
        "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": src}}, "Property": prop}}],
        "Values": [[{"Literal": {"Value": v}}] for v in literals],
    }}}


def _get(url, **kw):
    """GET/POST with retries."""
    last = None
    for attempt in range(4):
        try:
            resp = requests.request(timeout=120, url=url, **kw)
            if resp.status_code == 200:
                return resp
            last = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            last = str(e)
        time.sleep(2 ** attempt)
    raise RuntimeError(f"Power BI request failed after retries: {last}")


def fetch_rows(years):
    """Return [(year, month, crime_name, count)] from the Power BI model."""
    schema = _get(
        f"{BASE}/{RK}/modelsAndExploration?preferReadOnlySession=true",
        method="GET", headers={"X-PowerBI-ResourceKey": RK},
    ).json()
    model = schema["models"][0]

    query = {
        "Version": 2,
        "From": [
            {"Name": "c", "Entity": "Calendar", "Type": 0},
            {"Name": "r", "Entity": "RMS_data", "Type": 0},
        ],
        "Select": [
            _col("c", "Year", "Year"),
            _col("c", "MonthNum", "MonthNum"),
            _col("r", "off_cstat_cat", "crime"),
            {"Aggregation": {"Expression": {"Column": {
                "Expression": {"SourceRef": {"Source": "r"}}, "Property": "id"}},
                "Function": 2}, "Name": "n"},  # 2 = distinct count
        ],
        "Where": [
            _in("c", "Year", [f"{y}L" for y in years]),
            _in("r", "Citywide?", ["'Citywide'"]),
        ],
    }
    body = {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{"SemanticQueryDataShapeCommand": {
                "Query": query,
                "Binding": {
                    "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3]}]},
                    "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 30000}}},
                    "Version": 1,
                },
            }}]},
            "QueryId": "",
            "ApplicationContext": {"DatasetId": model["dbName"],
                                   "Sources": [{"ReportId": "x", "VisualId": "y"}]},
        }],
        "cancelQueries": [],
        "modelId": model["id"],
    }
    data = _get(f"{BASE}/querydata?synchronous=true", method="POST",
                headers=HEADERS, json=body).json()
    ds = data["results"][0]["result"]["data"]["dsr"]["DS"][0]
    if "odata.error" in ds:
        raise RuntimeError(f"Power BI query error: {json.dumps(ds['odata.error'])[:300]}")

    # Decode the compressed DSR: S = schema row, R = repeat-from-previous bitmask,
    # Ø = null bitmask, DN = value-dictionary name.
    vd = ds.get("ValueDicts", {})
    rows, schema_row, prev = [], None, None
    for row in ds["PH"][0]["DM0"]:
        if "S" in row:
            schema_row = row["S"]
        c, rmask, zmask = row.get("C", []), row.get("R", 0), row.get("Ø", 0)
        vals, ci = [], 0
        for i in range(len(schema_row)):
            if rmask >> i & 1:
                vals.append(prev[i])
            elif zmask >> i & 1:
                vals.append(None)
            else:
                vals.append(c[ci])
                ci += 1
        prev = vals
        dec = [vd[schema_row[i]["DN"]][v] if schema_row[i].get("DN") and v is not None else v
               for i, v in enumerate(vals)]
        year, month, crime, n = dec
        if year is not None and month is not None and crime:
            rows.append((int(year), int(month), crime, int(n or 0)))
    return rows


def scrape():
    """Current and prior year, completed months only."""
    today = date.today()
    years = [today.year - 1, today.year]
    rows = fetch_rows(years)
    if not rows:
        raise RuntimeError("Power BI returned no rows")

    counts = defaultdict(int)
    months = set()
    for year, month, crime, n in rows:
        if (year, month) >= (today.year, today.month):
            continue  # current month is still in progress
        months.add((year, month))
        offense = CRIME_MAP.get(crime)
        if offense:
            counts[(year, month, offense)] += n

    records = []
    for year, month in sorted(months):
        month_total = sum(counts.get((year, month, o), 0) for o in OFFENSES)
        if month_total == 0:
            continue  # month not loaded yet; leave it blank rather than writing zeros
        for offense in OFFENSES:
            records.append({
                "agency": AGENCY, "state": STATE, "type": TYPE_VALUE,
                "year": year, "month": month, "offense": offense,
                "count": counts.get((year, month, offense), 0),
            })
    return records


def load_existing():
    if OUT_JSON.exists():
        with OUT_JSON.open(encoding="utf-8") as f:
            return json.load(f)
    return []


def merge(existing, scraped):
    """Upsert scraped records over existing on (year, month, offense)."""
    merged = {(r["year"], r["month"], r["offense"]): r for r in existing}
    for rec in scraped:
        merged[(rec["year"], rec["month"], rec["offense"])] = rec
    order = {name: i for i, name in enumerate(OFFENSES)}
    return sorted(merged.values(),
                  key=lambda r: (r["year"], r["month"], order.get(r["offense"], 99)))


def main():
    existing = load_existing()
    print(f"Existing records: {len(existing)}")

    scraped = scrape()
    print(f"Scraped records: {len(scraped)}")
    if not scraped:
        print("ERROR: no data scraped", file=sys.stderr)
        sys.exit(1)

    merged = merge(existing, scraped)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(merged, f)

    months = sorted({(r["year"], r["month"]) for r in merged})
    print(f"Wrote {len(merged)} records to {OUT_JSON}")
    print(f"Month range: {months[0]} .. {months[-1]}")


if __name__ == "__main__":
    main()
