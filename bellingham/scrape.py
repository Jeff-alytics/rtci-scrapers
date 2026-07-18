"""
Bellingham WA Crime Scraper — police.cob.org calls-for-service statistics.

ASP.NET form (CallsForm.aspx): GET the page for hidden fields, then POST
with the year to render the "Reported Incidents" table (offense rows x
month columns, ALL NEIGHBORHOODS).

RTCI convention for Bellingham (matches how the pipeline base is filled):
only Murder (Homicide), Robbery, Theft, and Motor Vehicle Theft (Auto
Theft) are sourced — Rape / Aggravated Assault / Burglary are calls-based
categories that don't map to UCR and are left unsourced.

Trailing months with an all-zero Totals column are not-yet-reported and
skipped, as is the current (incomplete) calendar month.

Output: bellingham/data/latest.json in unified pipeline format.
"""

import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from html import unescape
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

URL = "https://police.cob.org/pircrimestatistics/CallsForm.aspx"
OUT_JSON = Path(__file__).parent / "data" / "latest.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

ROWMAP = {
    "Homicide": "Murder",
    "Robbery": "Robbery",
    "Theft": "Theft",
    "Auto Theft": "Motor Vehicle Theft",
}


def get_hidden_fields(html):
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION", "__ncforminfo"]:
        m = re.search(r'name="%s"[^>]*value="([^"]*)"' % re.escape(name), html)
        if m:
            fields[name] = m.group(1)
    return fields


def fetch_year(year):
    req = urllib.request.Request(URL, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        html = r.read().decode("utf-8", errors="replace")
        cookie = r.headers.get("Set-Cookie", "")
    fields = get_hidden_fields(html)
    fields.update({"ddlFromYear": str(year), "ddlNeighborhood": "99", "btnGo": "Go"})
    headers = dict(UA)
    if cookie:
        headers["Cookie"] = cookie.split(";")[0]
    req = urllib.request.Request(URL, data=urllib.parse.urlencode(fields).encode(),
                                 headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_table(html, year):
    """Return {month: {rtci_offense: count}} plus per-month totals."""
    t = None
    for cand in re.findall(r"<table.*?</table>", html, re.S | re.I):
        if "Reported Incidents" in cand:
            t = cand
    if not t:
        return {}, {}
    data, totals = {}, {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S | re.I):
        cells = [unescape(re.sub(r"<[^>]+>", "", c)).strip()
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S | re.I)]
        if len(cells) < 13:
            continue
        label = cells[0]
        vals = []
        for c in cells[1:13]:
            vals.append(int(c.replace(",", "")) if c.replace(",", "").isdigit() else 0)
        if label == "Totals":
            totals = {m + 1: v for m, v in enumerate(vals)}
        elif label in ROWMAP:
            for m, v in enumerate(vals):
                data.setdefault(m + 1, {})[ROWMAP[label]] = v
    return data, totals


def main():
    now = datetime.now()
    years = [now.year] if now.month > 7 else [now.year - 1, now.year]
    all_rows = []
    for year in years:
        print(f"Fetching {year}...")
        html = fetch_year(year)
        data, totals = parse_table(html, year)
        if not data:
            print(f"  no table parsed for {year}")
            continue
        for month in sorted(data):
            if year == now.year and month >= now.month:
                continue  # current/future month — incomplete
            if totals.get(month, 0) == 0:
                continue  # not yet reported
            for off, cnt in data[month].items():
                all_rows.append({"agency": "Bellingham", "state": "WA", "type": "City",
                                 "year": year, "month": month, "offense": off, "count": cnt})
        got = sorted({(r['year'], r['month']) for r in all_rows if r['year'] == year})
        print(f"  {year}: months {[m for _, m in got]}")

    if not all_rows:
        print("No data collected.")
        sys.exit(1)

    keep = sorted({(r["year"], r["month"]) for r in all_rows}, reverse=True)[:6]
    all_rows = [r for r in all_rows if (r["year"], r["month"]) in keep]

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(all_rows, f)
    print(f"\nWrote {len(all_rows)} records ({len(keep)} months) to {OUT_JSON}")


if __name__ == "__main__":
    main()
