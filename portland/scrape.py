"""
Portland Police Bureau — Monthly Crime Counts Scraper
=====================================================

Data sources:
  - All UCR categories EXCEPT Aggravated Assault: Tableau Public CSV snapshots
    (PPBOpenDataDownloads workbook), fetched via plain HTTP.
  - Aggravated Assault: live Tableau dashboard (MonthlyReportedCrimeStatistics),
    read from its backend (Tableau session commands, no browser).  The CSV undercounts AA by ~15-20% because Portland
    excludes sensitive cases (likely DV) from the public CSV.

Output: data/latest.json in RTCI pipeline format:
  [{agency, state, type, year, month, offense, count}, ...]

Window: 12 months ending the month before today.

Usage:
    python scrape.py                # save to data/latest.json
    python scrape.py --json         # output JSON to stdout
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
# Tableau dashboard scrape (Aggravated Assault only) - backend session, no browser
# ══════════════════════════════════════════════════════════════════════════════
#
# The dashboard's "Offense Category Table" shows the 12 months combined until a
# month bar is selected. We open a Tableau Public viewing session over HTTP,
# then send the same `tabdoc/select` command the page sends when a bar is
# clicked (worksheet "Monthly Stats", objectIds [n]) and read the updated
# Offense Table out of the JSON response. objectId n is the n-th row of the
# Monthly Stats worksheet (rows are newest month first).

VIZ_ROOT = ("https://public.tableau.com/vizql/w/MonthlyReportedCrimeStatistics"
            "/v/MonthlyStatistics")
EMBED_URL = ("https://public.tableau.com/views/MonthlyReportedCrimeStatistics"
             "/MonthlyStatistics?:embed=y&:showVizHome=no&:toolbar=no")
AA_ROW_NAME = "Assault: Aggravated"
CHECK_ROW_NAME = "Burglary"   # its count for the selected month must agree with the CSV,
                              # which confirms the objectId -> month mapping is still right


def _split_sized(text):
    """Bootstrap payload = repeated '<length>;<json>' chunks."""
    parts, i = [], 0
    while i < len(text):
        m = re.match(r"(\d+);", text[i:])
        if not m:
            break
        n = int(m.group(1))
        st = i + len(m.group(0))
        parts.append(text[st:st + n])
        i = st + n
    return parts


def _dict_by_type(data_columns, into=None):
    into = into if into is not None else {}
    for col in data_columns:
        into.setdefault(col["dataType"], []).extend(col["dataValues"])
    return into


def _build_dictionary(segments):
    """Join dictionary segments (keyed by id) in id order into {dataType: [values]}."""
    out = {}
    for sid in sorted(segments, key=int):
        _dict_by_type(segments[sid], out)
    return out


def _pane_values(viz_cols, pane_cols, dictionary, col_idx):
    vc, pc = viz_cols[col_idx], pane_cols[col_idx]
    idxs = pc.get("valueIndices") or pc.get("aliasIndices") or []
    vals = dictionary.get(vc["dataType"], [])
    return [vals[i] if 0 <= i < len(vals) else None for i in idxs]


def _table_columns(viz_data):
    pcd = viz_data["paneColumnsData"]
    return pcd["vizDataColumns"], pcd["paneColumnsList"][0]["vizPaneColumns"]


def _col_index(viz_cols, caption):
    for i, c in enumerate(viz_cols):
        if c.get("fieldCaption") == caption:
            return i
    raise RuntimeError(f"Tableau column not found: {caption}")


def scrape_aa_from_dashboard(start, end, csv_data=None):
    """Monthly Aggravated Assault from the Tableau dashboard backend.

    Returns {(year, month): aa_count}. Raises on any structural surprise so the
    workflow fails loudly instead of silently dropping aggravated assault.
    """
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.get(EMBED_URL, timeout=60)
    start_resp = s.post(VIZ_ROOT + "/startSession/viewing?%3Aembed=y&%3AshowVizHome=no"
                        "&%3Atoolbar=no&%3Aredirect=auth",
                        headers={"Accept": "application/json", "Referer": EMBED_URL},
                        timeout=60)
    start_resp.raise_for_status()
    st = start_resp.json()
    sid, sheet = st["sessionid"], st["sheetId"]
    tab_hdr = {"X-Tsi-Active-Tab": sheet, "Accept": "text/javascript", "Referer": EMBED_URL}
    # The response names the server node that owns this session; every later request must echo it.
    gsh = start_resp.headers.get("global-session-header")
    if gsh:
        tab_hdr["global-session-header"] = gsh

    boot_form = {
        "worksheetPortSize": '{"w":850,"h":1100}',
        "dashboardPortSize": '{"w":850,"h":1100}',
        "clientDimension": '{"w":1400,"h":1200}',
        "renderMapsClientSide": "true", "isBrowserRendering": "true",
        "browserRenderingThreshold": "100", "formatDataValueLocally": "false",
        "clientNum": "", "navType": "Nav", "navSrc": "Top", "devicePixelRatio": "1",
        "clientRenderPixelLimit": "16000000",
        "allowAutogenWorksheetPhoneLayouts": "true", "sheet_id": sheet,
        "showParams": '{"checkpoint":false,"refresh":false,"refreshUnmodified":false}',
        "stickySessionKey": st["stickySessionKey"], "filterTileSize": "200",
        "locale": "en_US", "language": "en", "verboseMode": "false",
        ":session_feature_flags": st.get("features_json", "{}"),
    }
    r = s.post(f"{VIZ_ROOT}/bootstrapSession/sessions/{sid}", data=boot_form,
               headers={**tab_hdr, "X-XSRF-TOKEN": "null"}, timeout=180)
    r.raise_for_status()
    parts = _split_sized(r.text)
    if len(parts) < 2:
        raise RuntimeError("Tableau bootstrap response had an unexpected shape")
    sec = json.loads(parts[1])["secondaryInfo"]["presModelMap"]
    boot_segs = (sec["dataDictionary"]["presModelHolder"]["genDataDictionaryPresModel"]
                 ["dataSegments"])
    segments = {sid: seg["dataColumns"] for sid, seg in boot_segs.items()}
    dictionary = _build_dictionary(segments)
    sheets = sec["vizData"]["presModelHolder"]["genPresModelMapPresModel"]["presModelMap"]

    # Month list (newest first) from the Monthly Stats worksheet.
    vc, pc = _table_columns(sheets["Monthly Stats"]["presModelHolder"]["genVizDataPresModel"])
    month_col = _col_index(vc, "MONTH(ReportDate)")
    months = [(int(str(v)[:4]), int(str(v)[5:7]))
              for v in _pane_values(vc, pc, dictionary, month_col)]
    if len(months) < 6:
        raise RuntimeError(f"Expected ~12 months in Monthly Stats, got {len(months)}")

    # Row positions in the Offense Table (same order in every response).
    vc, pc = _table_columns(sheets["Offense Table"]["presModelHolder"]["genVizDataPresModel"])
    names = _pane_values(vc, pc, dictionary, _col_index(vc, "OpenDataOffense"))
    if AA_ROW_NAME not in names or CHECK_ROW_NAME not in names:
        raise RuntimeError("Offense Table rows changed; cannot find Aggravated Assault row")
    aa_row, chk_row = names.index(AA_ROW_NAME), names.index(CHECK_ROW_NAME)
    cnt_col = _col_index(vc, "SUM(OffenseCount)")

    aa_counts = {}
    for n, (yr, mo) in enumerate(months, start=1):
        if not (start <= date(yr, mo, 1) < end):
            continue
        fields = {
            "worksheet": (None, "Monthly Stats"),
            "dashboard": (None, "Monthly Statistics"),
            "selection": (None, json.dumps({"objectIds": [n], "selectionType": "tuples"})),
            "selectOptions": (None, "select-options-simple"),
            "zoneId": (None, "27"),
            "zoneSelectionType": (None, "replace"),
        }
        resp = s.post(f"{VIZ_ROOT}/sessions/{sid}/commands/tabdoc/select", files=fields,
                      headers={**tab_hdr, "X-Requested-With": "XMLHttpRequest"}, timeout=180)
        resp.raise_for_status()
        app = resp.json()["vqlCmdResponse"]["layoutStatus"]["applicationPresModel"]
        # Responses carry dictionary segments by id: a new id extends the dictionary, a
        # repeated id replaces that segment. Values index into the segments joined in id order.
        for seg_id, seg in app["dataDictionary"]["dataSegments"].items():
            if seg:  # a null segment means "nothing new under this id"
                segments[seg_id] = seg["dataColumns"]
        merged = _build_dictionary(segments)
        table = None
        for z in app["workbookPresModel"]["dashboardPresModel"]["zones"].values():
            viz = ((z.get("presModelHolder") or {}).get("visual") or {}).get("vizData")
            if viz and any(c.get("fieldCaption") == "OpenDataOffense"
                           for c in viz["paneColumnsData"]["vizDataColumns"]):
                table = viz
        if table is None:
            raise RuntimeError(f"No Offense Table in select response for {yr}-{mo:02d}")
        tvc, tpc = _table_columns(table)
        counts = _pane_values(tvc, tpc, merged, cnt_col)
        if len(counts) != len(names):
            raise RuntimeError(f"Offense Table has {len(counts)} rows, expected {len(names)}")
        aa, burg = counts[aa_row], counts[chk_row]
        csv_burg = (csv_data or {}).get((yr, mo), {}).get("Burglary")
        if csv_burg is not None and abs(burg - csv_burg) > max(3, 0.03 * csv_burg):
            raise RuntimeError(f"Month check failed for {yr}-{mo:02d}: dashboard Burglary "
                               f"{burg} vs CSV {csv_burg}; objectId mapping may have changed")
        aa_counts[(yr, mo)] = int(aa)
        print(f"  {yr}-{mo:02d}: AA={aa} (Burglary check {burg})")
    return aa_counts


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
        aa_counts = scrape_aa_from_dashboard(start, end, csv_data)

    if not aa_counts:
        print("ERROR: no Aggravated Assault months scraped; not writing output", file=sys.stderr)
        sys.exit(1)

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
