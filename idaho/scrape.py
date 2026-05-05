"""Idaho Crime Scraper — Optimum NIBRS platform. Source: https://nibrs.isp.idaho.gov/CrimeInIdaho/"""
import sys; sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from optimum_nibrs import run_scraper

AGENCIES = [
    {"ori": "ID0010000", "name": "Ada", "type": "County"},
    {"ori": "ID0010100", "name": "Boise", "type": "City"},
    {"ori": "ID0140100", "name": "Caldwell", "type": "City"},
    {"ori": "ID0280100", "name": "Coeur D Alene", "type": "City"},
    {"ori": "ID0100200", "name": "Idaho Falls", "type": "City"},
    {"ori": "ID0010300", "name": "Meridian", "type": "City"},
    {"ori": "ID0140200", "name": "Nampa", "type": "City"},
    {"ori": "ID0030200", "name": "Pocatello", "type": "City"},
    {"ori": "ID0420200", "name": "Twin Falls", "type": "City"},
]

if __name__ == "__main__":
    from pathlib import Path
    run_scraper(
        base_url="https://nibrs.isp.idaho.gov/CrimeInIdaho/Report",
        state="ID",
        agencies=AGENCIES,
        out_json=Path(__file__).parent / "data" / "latest.json",
    )
