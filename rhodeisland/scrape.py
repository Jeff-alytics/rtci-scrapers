"""Rhode Island Crime Scraper — Optimum NIBRS platform. Source: https://riucr.nibrs.com/"""
import sys; sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from optimum_nibrs import run_scraper

AGENCIES = [
    {"ori": "RI0040200", "name": "Cranston", "type": "City"},
    {"ori": "RI0040800", "name": "Pawtucket", "type": "City"},
    {"ori": "RI0040900", "name": "Providence", "type": "City"},
    {"ori": "RI0020300", "name": "Warwick", "type": "City"},
]

if __name__ == "__main__":
    from pathlib import Path
    run_scraper(
        base_url="https://riucr.nibrs.com/Report",
        state="RI",
        agencies=AGENCIES,
        out_json=Path(__file__).parent / "data" / "latest.json",
    )
