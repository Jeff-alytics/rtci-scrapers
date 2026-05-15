"""
Santa Fe NM Monthly Crime Data Scraper

Downloads crime stats PDFs from santafenm.gov and extracts Part I crime counts.
Each PDF has a monthly table for the full year-to-date.
The latest PDF for each year contains all months for that year.

Note: Santa Fe reports "Assault Offenses" (all assaults) not just Aggravated
Assault, and "Sex Offenses, Forcible" (includes fondling) not just Rape.
Rape is set to 0; Aggravated Assault uses the broader assault figure.

Usage:
    python scrape.py              # scrape latest PDFs, merge with existing
    python scrape.py --all        # re-scrape all available PDFs
"""

import json
import os
import re
import sys
import urllib.request
from html.parser import HTMLParser

import pdfplumber

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "data", "latest.json")
PAGE_URL = "https://santafenm.gov/police/police-criminal-investigations/police-crime-analyst"

# Map PDF labels to RTCI offenses
# "Sex Offenses, Forcible" is broader than Rape — omit from RTCI
# "Assault Offenses" is broader than Aggravated Assault — mapped as best available
CRIME_MAP = {
    "Homicide Offenses": "Murder",
    "Robbery": "Robbery",
    "Motor Vehicle Theft": "Motor Vehicle Theft",
    "Burglary/Breaking & Entering": "Burglary",
    "Larceny/Theft Offenses": "Theft",
    "Assault Offenses": "Aggravated Assault",
}

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def discover_pdfs():
    """Scrape the crime analyst page for Crime Statistics PDF links.
    URLs appear as plain text in <td> cells, not as <a href> attributes.
    """
    req = urllib.request.Request(PAGE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    # Find all PDF URLs in the HTML (may be in href or plain text)
    urls = re.findall(r"https?://santafenm\.gov/media/files/police/[^\s\"<>]+\.pdf", html)

    links = []
    seen = set()
    for url in urls:
        fname = urllib.request.url2pathname(url.split("/")[-1]).upper()
        if fname in seen:
            continue
        seen.add(fname)
        # Only want stats/report PDFs, not mapping PDFs
        if "MAPPING" in fname:
            continue
        if "STAT" not in fname and "REPORT" not in fname:
            continue
        # Skip annual summary PDFs (e.g. CRIME_STATS_2024.PDF)
        if re.match(r"^CRIME_STATS_\d{4}\.PDF$", fname):
            continue
        # Extract month and year from filename
        m = re.search(
            r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|"
            r"SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\D+(\d{4})",
            fname,
        )
        if m:
            month = MONTH_NAMES[m.group(1).lower()]
            year = int(m.group(2))
            links.append({"url": url, "year": year, "month": month, "text": fname})

    links.sort(key=lambda x: (x["year"], x["month"]), reverse=True)
    return links


def download_pdf(url, dest):
    """Download a PDF to a local path."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        with open(dest, "wb") as f:
            f.write(resp.read())


def parse_pdf(path):
    """Extract monthly crime counts from page 1 of a Santa Fe stats PDF.
    Returns list of {year, month, offense, count} dicts.
    """
    pdf = pdfplumber.open(path)
    text = pdf.pages[0].extract_text() or ""
    pdf.close()

    lines = text.split("\n")
    results = []

    # Find the year and number of active months from header row
    # Format: "2025 Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec Total"
    year = None
    for line in lines:
        m = re.match(r"^(20\d{2})\s+Jan\b", line)
        if m:
            year = int(m.group(1))
            break

    if not year:
        return []

    # Only parse the table section — stop at comparison/footnote sections
    in_table = False
    num_months = None
    for line in lines:
        stripped = line.strip()

        # Table starts after the year/month header
        if re.match(r"^20\d{2}\s+Jan\b", stripped):
            in_table = True
            continue

        # Stop at comparison sections
        if in_table and re.search(r"Prior Month|Counts do not include|\*", stripped):
            break

        if not in_table:
            continue

        for label, offense in CRIME_MAP.items():
            if stripped.startswith(label):
                rest = stripped[len(label):]
                nums = re.findall(r"\d+", rest)
                if not nums:
                    break
                # Last number is "Total", monthly values are before it
                if len(nums) > 1:
                    monthly = [int(n) for n in nums[:-1]]
                else:
                    monthly = [int(nums[0])]

                # Detect number of active months from first crime row
                if num_months is None:
                    num_months = len(monthly)

                for month_idx, count in enumerate(monthly):
                    results.append({
                        "agency": "Santa Fe",
                        "state": "NM",
                        "type": "City",
                        "year": year,
                        "month": month_idx + 1,
                        "offense": offense,
                        "count": count,
                    })
                # Add Rape=0 for each month when we process Murder
                if offense == "Murder":
                    for month_idx in range(len(monthly)):
                        results.append({
                            "agency": "Santa Fe",
                            "state": "NM",
                            "type": "City",
                            "year": year,
                            "month": month_idx + 1,
                            "offense": "Rape",
                            "count": 0,
                        })
                break

    return results


def main():
    scrape_all = "--all" in sys.argv

    # Load existing data
    existing = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            existing = json.load(f)

    existing_keys = {(r["year"], r["month"], r["offense"]) for r in existing}

    # Discover available PDFs
    print("Discovering PDFs...")
    links = discover_pdfs()
    print(f"  Found {len(links)} stats PDFs")

    if not links:
        print("  No PDFs found on page")
        return

    if scrape_all:
        # Scrape all PDFs
        to_scrape = links
    else:
        # Only scrape the latest PDF per year (it has all months for that year)
        latest_by_year = {}
        for lnk in links:
            y = lnk["year"]
            if y not in latest_by_year or lnk["month"] > latest_by_year[y]["month"]:
                latest_by_year[y] = lnk

        # Only scrape years where we might have new data
        to_scrape = []
        for y, lnk in sorted(latest_by_year.items()):
            max_existing = max(
                (r["month"] for r in existing if r["year"] == y), default=0
            )
            if lnk["month"] > max_existing or scrape_all:
                to_scrape.append(lnk)

        if not to_scrape:
            print("  No new months to scrape")
            return

    print(f"  Scraping {len(to_scrape)} PDFs...")
    tmp_dir = os.path.join(SCRIPT_DIR, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    new_data = []
    for lnk in to_scrape:
        fname = f"santafe_{lnk['year']}_{lnk['month']:02d}.pdf"
        fpath = os.path.join(tmp_dir, fname)
        print(f"  {lnk['year']}-{lnk['month']:02d}: {lnk['text']}")
        try:
            download_pdf(lnk["url"], fpath)
            rows = parse_pdf(fpath)
            new_data.extend(rows)
            months_found = len(set((r["year"], r["month"]) for r in rows))
            print(f"    -> {len(rows)} records ({months_found} months)")
        except Exception as e:
            print(f"    ERROR: {e}")

    # Merge: new data overwrites existing for same (year, month, offense)
    merged = {(r["year"], r["month"], r["offense"]): r for r in existing}
    for r in new_data:
        merged[(r["year"], r["month"], r["offense"])] = r

    final = sorted(merged.values(), key=lambda r: (r["year"], r["month"], r["offense"]))

    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(final, f, indent=2)

    print(f"\nWrote {len(final)} rows to {DATA_FILE}")


if __name__ == "__main__":
    main()
