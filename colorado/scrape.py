"""Colorado Crime Scraper — CBI NIBRS (RSReport.aspx). Source: coloradocrimestats.state.co.us"""
import sys; sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from ssrs_nibrs import run_scraper

AGENCIES = [
    {"ori": "CO0010000", "name": "Adams", "type": "County"},
    {"ori": "CO0030000", "name": "Arapahoe", "type": "County"},
    {"ori": "CO0300100", "name": "Arvada", "type": "City"},
    {"ori": "CO0010100", "name": "Aurora", "type": "City"},
    {"ori": "CO0070100", "name": "Boulder", "type": "City"},
    {"ori": "CO0640100", "name": "Broomfield", "type": "City"},
    {"ori": "CO0180100", "name": "Castle Rock", "type": "City"},
    {"ori": "CO0031100", "name": "Centennial", "type": "City"},
    {"ori": "CO0210100", "name": "Colorado Springs", "type": "City"},
    {"ori": "CO0010300", "name": "Commerce City", "type": "City"},
    {"ori": "CODPD0000", "name": "Denver", "type": "City"},
    {"ori": "CO0180000", "name": "Douglas", "type": "County"},
    {"ori": "CO0210000", "name": "El Paso", "type": "County"},
    {"ori": "CO0350300", "name": "Fort Collins", "type": "City"},
    {"ori": "CO0390100", "name": "Grand Junction", "type": "City"},
    {"ori": "CO0620200", "name": "Greeley", "type": "City"},
    {"ori": "CO0300000", "name": "Jefferson", "type": "County"},
    {"ori": "CO0300400", "name": "Lakewood", "type": "City"},
    {"ori": "CO0070400", "name": "Longmont", "type": "City"},
    {"ori": "CO0350400", "name": "Loveland", "type": "City"},
    {"ori": "CO0180500", "name": "Parker", "type": "City"},
    {"ori": "CO0510100", "name": "Pueblo", "type": "City"},
    {"ori": "CO0010400", "name": "Thornton", "type": "City"},
    {"ori": "CO0010500", "name": "Westminster", "type": "City"},
]

if __name__ == "__main__":
    from pathlib import Path
    run_scraper(
        report_url="https://coloradocrimestats.state.co.us/public/View/RSReport.aspx?ReportId=45",
        state="CO",
        agencies=AGENCIES,
        out_json=Path(__file__).parent / "data" / "latest.json",
    )
