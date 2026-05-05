"""Illinois Crime Scraper — Optimum NIBRS platform. Source: https://ilucr.nibrs.com/"""
import sys; sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from optimum_nibrs import run_scraper

AGENCIES = [
    {"ori": "IL0160200", "name": "Arlington Heights", "type": "City"},
    {"ori": "IL0990200", "name": "Bolingbrook", "type": "City"},
    {"ori": "IL0100100", "name": "Champaign", "type": "City"},
    {"ori": "ILCPD0000", "name": "Chicago", "type": "City"},
    {"ori": "IL0162100", "name": "Cicero", "type": "City"},
    {"ori": "IL0580200", "name": "Decatur", "type": "City"},
    {"ori": "IL0162500", "name": "Des Plaines", "type": "City"},
    {"ori": "IL0163200", "name": "Evanston", "type": "City"},
    {"ori": "IL0167200", "name": "Mount Prospect", "type": "City"},
    {"ori": "IL0570200", "name": "Normal", "type": "City"},
    {"ori": "IL0168000", "name": "Oak Lawn", "type": "City"},
    {"ori": "IL0168400", "name": "Palatine", "type": "City"},
    {"ori": "IL1010400", "name": "Rockford", "type": "City"},
    {"ori": "IL0162A00", "name": "Schaumburg", "type": "City"},
    {"ori": "IL0164A00", "name": "Skokie", "type": "City"},
    {"ori": "IL0840200", "name": "Springfield", "type": "City"},
    {"ori": "IL0165M00", "name": "Tinley Park", "type": "City"},
    {"ori": "IL0990000", "name": "Will", "type": "County"},
]

if __name__ == "__main__":
    from pathlib import Path
    run_scraper(
        base_url="https://ilucr.nibrs.com/Report",
        state="IL",
        agencies=AGENCIES,
        out_json=Path(__file__).parent / "data" / "latest.json",
    )
