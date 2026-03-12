# NYPD CompStat Scraper

Scrapes monthly crime data from the [NYPD CompStat 2.0](https://compstat.nypdonline.org/) dashboard using Puppeteer.

## Why?

The CompStat API requires server-side session state — endpoints return 405 without it. This scraper loads the full Angular app in a headless browser, clicks each crime's Year-to-Date cell, and reads exact values from SVG `aria-label` attributes on the chart data points.

## Data

Extracts monthly counts for the 7 major felonies:

| CompStat | RTCI Offense |
|---|---|
| Murder | Murder |
| Rape | Rape |
| Robbery | Robbery |
| Felony Assault | Aggravated Assault |
| Burglary | Burglary |
| Grand Larceny | Theft |
| Grand Larceny Auto | Motor Vehicle Theft |

Only completed months are included in output (current partial month is excluded).

## Usage

```bash
npm install
node scrape.js            # pretty-print table
node scrape.js --json     # JSON output for pipeline consumption
```

### Example output

```
Month       Murder    Rape      Robbery   Aggravated Assault  Burglary  Theft     Motor Vehicle Theft
---------   ------    ------    -------   ------------------  --------  ------    -------------------
Jan 2026        16       170      1003                 2146       897     3423                    864
Feb 2026        16       161       923                 1943       804     3017                    789
```

### JSON output

```json
[
  { "agency": "New York City", "year": 2026, "month": 1, "offense": "Murder", "count": 16 },
  { "agency": "New York City", "year": 2026, "month": 1, "offense": "Rape", "count": 170 },
  ...
]
```

## How it works

1. Launches headless Chrome via Puppeteer
2. Navigates to `compstat.nypdonline.org` and waits for the Kendo grid to render
3. For each of the 7 major felonies:
   - Finds the crime row in the locked column table
   - Clicks the YTD cell in the corresponding data table (column index 6)
   - Waits for the timeline chart (second `.k-chart`) to update
   - Reads `aria-label` attributes on SVG `<circle>` elements — these contain exact integer values
   - Reads month labels from SVG `<text>` elements
4. Filters to completed months only and outputs results

## Notes

- The CompStat dashboard shows the current year's data (YTD through the most recent CompStat week)
- Data updates weekly (Monday–Sunday CompStat periods)
- Partial months (current month) are automatically excluded from output
- Run monthly after the first CompStat week of each new month to capture the previous month's final numbers
