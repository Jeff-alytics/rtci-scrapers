"""Fail the workflow when a scraper's output is empty or stale.

Runs as a workflow step right after the scraper. Exits 1 (failing the run,
so it shows red in the Actions list) when the output file is missing, empty,
or its newest data month is older than the allowed lag. A green run then
means "fresh data was actually collected", not just "the script didn't crash".

Usage:
    python scripts/verify_freshness.py <path> [--max-lag N] [--min-records N]

Supports the unified latest.json format (list of {year, month, ...} records)
and RTCI-format CSVs with Year/Month columns (cde-reta).
"""

import argparse
import csv
import json
import sys
from datetime import date


def month_index(year, month):
    return year * 12 + (month - 1)


def load_months(path):
    if path.endswith(".csv"):
        with open(path, newline="", encoding="utf-8-sig") as f:
            return [(int(r["Year"]), int(r["Month"]))
                    for r in csv.DictReader(f) if r.get("Year", "").strip()]
    with open(path) as f:
        data = json.load(f)
    return [(int(r["year"]), int(r["month"])) for r in data]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--max-lag", type=int, default=2,
                    help="max allowed months between today and the newest data month (default 2)")
    ap.add_argument("--min-records", type=int, default=1)
    args = ap.parse_args()

    try:
        records = load_months(args.path)
    except FileNotFoundError:
        print(f"STALE: {args.path} does not exist")
        sys.exit(1)
    except (ValueError, KeyError, json.JSONDecodeError) as e:
        print(f"STALE: could not parse {args.path}: {e}")
        sys.exit(1)

    if len(records) < args.min_records:
        print(f"STALE: {args.path} has {len(records)} records (min {args.min_records})")
        sys.exit(1)

    newest = max(records)
    today = date.today()
    lag = month_index(today.year, today.month) - month_index(*newest)
    if lag > args.max_lag:
        print(f"STALE: newest month in {args.path} is {newest[0]}-{newest[1]:02d} "
              f"({lag} months old, max allowed {args.max_lag})")
        sys.exit(1)

    print(f"OK: {args.path} newest month {newest[0]}-{newest[1]:02d} "
          f"({lag} months old), {len(records)} records")


if __name__ == "__main__":
    main()
