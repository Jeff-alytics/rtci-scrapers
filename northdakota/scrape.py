"""North Dakota Crime Scraper — crimestats.nd.gov NIBRS (RSReport.aspx)."""
import sys; sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from ssrs_nibrs import run_scraper

AGENCIES = [
    {"ori": "ND0080100", "name": "Bismarck", "type": "City"},
    {"ori": "ND0090200", "name": "Fargo", "type": "City"},
    {"ori": "ND0180100", "name": "Grand Forks", "type": "City"},
]

if __name__ == "__main__":
    from pathlib import Path
    run_scraper(
        report_url="https://crimestats.nd.gov/public/View/RSReport.aspx?ReportId=94",
        state="ND",
        agencies=AGENCIES,
        out_json=Path(__file__).parent / "data" / "latest.json",
    )
