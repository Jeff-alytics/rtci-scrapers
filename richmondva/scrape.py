"""
Richmond VA Crime Scraper — CrimeInfo IncidentListing

Fetches incident-level offense listings from apps.richmondgov.com for the
4 police precincts (drillDownAreaCode 1-4), maps OFFENSE DESCRIPTION through
the RTCI crosswalk, and aggregates to monthly counts.

Window: the 6 most recent complete months ending 2 months back (e.g. run in
July -> Dec through May), matching RTCI's reporting-lag convention.

Output: richmondva/data/latest.json in unified pipeline format.
"""

import json
import re
import sys
import urllib.request
from datetime import datetime
from html import unescape
from html.parser import HTMLParser  # noqa: F401  (kept for parity with other scrapers)
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

OUT_JSON = Path(__file__).parent / "data" / "latest.json"
BASE = ("https://apps.richmondgov.com/applications/crimeinfo/Home/IncidentListing"
        "?beginningDate={b}&endingDate={e}&crimeType=ALL&locationCode=CSZ"
        "&areaCode=ALL&drillDownAreaCode={p}&drillDownAreaDesc=2")
PRECINCTS = [1, 2, 3, 4]

# From "Richmond Crosswalk.xlsx" (Ben and Jeff working files/Cities)
CROSSWALK = {
    "AGGRAVATED ASSAULT": "Aggravated Assault",
    "AGGRAVATED ASSAULT DOMESTIC": "Aggravated Assault",
    "BURGLARY/B&E/COMMERCIAL": "Burglary",
    "BURGLARY/B&E/OUTBUILDING": "Burglary",
    "BURGLARY/B&E/RESIDENTIAL": "Burglary",
    "FORCIBLE RAPE": "Rape",
    "MOTOR VEHICLE THEFT": "Motor Vehicle Theft",
    "MURDER/NON-NEGLIGENT MANSLAUGHTER": "Murder",
    "PICKPOCKET": "Theft",
    "PURSE SNATCHING": "Theft",
    "ROBBERY/BANK": "Robbery",
    "ROBBERY/CARJACKING": "Robbery",
    "ROBBERY/COMMERCIAL HOUSE": "Robbery",
    "ROBBERY/INDIVIDUAL": "Robbery",
    "ROBBERY/RESIDENCE": "Robbery",
    "SHOPLIFTING": "Theft",
    "THEFT FROM BUILDING": "Theft",
    "THEFT FROM COIN OPERATED MACHINE OR DEVICE": "Theft",
    "THEFT FROM MOTOR VEHICLE": "Theft",
    "THEFT OF MOPED/OTHER VEHICLE TYPE": "Theft",
    "THEFT OF MOTOR VEHICLE PARTS/ACCESSORIES": "Theft",
    "ALL OTHER LARCENY": "Theft",
    "SEXUAL ASSAULT WITH AN OBJECT": "Rape",
    "FORCIBLE SODOMY": "Rape",
}
OFFENSES = ["Murder", "Rape", "Robbery", "Aggravated Assault",
            "Burglary", "Theft", "Motor Vehicle Theft"]


def month_window(n=6, lag=2):
    """The n most recent complete months ending `lag` months back."""
    now = datetime.now().replace(day=1)
    y, m = now.year, now.month - lag
    while m <= 0:
        y, m = y - 1, m + 12
    months = []
    for _ in range(n):
        months.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return sorted(months)


def days_in_month(y, m):
    if m == 12:
        return 31
    return (datetime(y, m + 1, 1) - datetime(y, m, 1)).days


def fetch_month_precinct(y, m, precinct):
    b = f"{m}%2F1%2F{y}"
    e = f"{m}%2F{days_in_month(y, m)}%2F{y}"
    url = BASE.format(b=b, e=e, p=precinct)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_offense_rows(raw):
    """Yield OFFENSE DESCRIPTION strings from the incident listing table."""
    t = re.search(r"<table.*?</table>", raw, re.S | re.I)
    if not t:
        return
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", t.group(0), re.S | re.I)
    for row in rows[1:]:
        cells = [unescape(re.sub(r"<[^>]+>", "", c)).strip()
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S | re.I)]
        if len(cells) >= 3 and cells[2]:
            yield cells[2].upper()


def main():
    window = month_window()
    print(f"Window: {window}")
    all_rows = []
    for (y, m) in window:
        counts = {o: 0 for o in OFFENSES}
        total_rows = 0
        for p in PRECINCTS:
            try:
                raw = fetch_month_precinct(y, m, p)
            except Exception as e:
                print(f"  {y}-{m:02d} precinct {p}: FETCH FAILED {e}")
                continue
            for desc in parse_offense_rows(raw):
                total_rows += 1
                cat = CROSSWALK.get(desc)
                if cat:
                    counts[cat] += 1
        print(f"  {y}-{m:02d}: {total_rows} offense rows -> {counts}")
        if total_rows == 0:
            # No listing at all (site failure) — don't emit fake zeros
            continue
        for off, cnt in counts.items():
            all_rows.append({"agency": "Richmond", "state": "VA", "type": "City",
                             "year": y, "month": m, "offense": off, "count": cnt})

    if not all_rows:
        print("\nNo data collected.")
        sys.exit(1)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)
    months = sorted({(r["year"], r["month"]) for r in all_rows})
    print(f"\nWrote {len(all_rows)} records ({len(months)} months) to {OUT_JSON}")


if __name__ == "__main__":
    main()
