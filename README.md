# RTCI Scrapers

Automated scrapers for the Real Time Crime Index pipeline. Each subfolder contains a standalone scraper + its output data. GitHub Actions runs them monthly and commits updated JSON.

## Scrapers

### `compstat/` — NYPD CompStat

Puppeteer-based scraper for [NYPD CompStat 2.0](https://compstat.nypdonline.org/). Reads exact values from SVG `aria-label` attributes on Kendo chart data points.

- **Schedule:** 10th of each month
- **Agencies:** New York City
- **Output:** `compstat/data/latest.json`

```bash
cd compstat && npm install && node scrape.js --json
```

### `arjis/` — San Diego Area (ARJIS)

Python scraper for [ARJIS CrimeStats](https://crimestats.arjis.org/). Downloads Excel exports via ASP.NET ViewState postbacks, reshapes wide→long, and aggregates sub-crimes into RTCI offenses.

- **Schedule:** 12th of each month
- **Agencies:** Carlsbad, Chula Vista, Coronado, El Cajon, Escondido, La Mesa, National City, Oceanside, San Diego (City), San Diego County Sheriff
- **Output:** `arjis/data/latest.json`

```bash
cd arjis && pip install -r requirements.txt && python scrape.py --recent 24 --json
```

## JSON Format

All scrapers output the same format for pipeline consumption:

```json
[
  { "agency": "San Diego", "state": "CA", "type": "City", "year": 2025, "month": 1, "offense": "Murder", "count": 5 }
]
```

The pipeline fetches `data/latest.json` from each scraper via `raw.githubusercontent.com`.

## Crime Mapping

| ARJIS Source | RTCI Offense |
|---|---|
| Armed Robbery + Strong Arm Robbery | Robbery |
| Residential + Non-Residential Burglary | Burglary |
| Theft >= $400 + Theft < $400 | Theft |
| Murder, Rape, Aggravated Assault, Motor Vehicle Theft | (direct mapping) |
