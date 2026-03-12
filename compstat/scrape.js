/**
 * NYPD CompStat 2.0 Scraper
 *
 * Navigates to the CompStat dashboard, clicks each crime's YTD cell,
 * and reads exact monthly values from SVG circle aria-label attributes.
 *
 * Usage:
 *   node scrape.js              # pretty-print table
 *   node scrape.js --json       # output JSON
 *   node scrape.js --json > data/2026-03.json
 */

const puppeteer = require("puppeteer");

const URL = "https://compstat.nypdonline.org/";
const CRIMES = [
  "Murder",
  "Rape",
  "Robbery",
  "Felony Assault",
  "Burglary",
  "Grand Larceny",
  "Grand Larceny Auto",
];

// CompStat → RTCI offense mapping
const OFFENSE_MAP = {
  Murder: "Murder",
  Rape: "Rape",
  Robbery: "Robbery",
  "Felony Assault": "Aggravated Assault",
  Burglary: "Burglary",
  "Grand Larceny": "Theft",
  "Grand Larceny Auto": "Motor Vehicle Theft",
};

const MONTH_NAMES = [
  "",
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

async function scrape() {
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  process.stderr.write("Loading CompStat dashboard...\n");
  await page.goto(URL, { waitUntil: "networkidle2", timeout: 60000 });

  // Wait for the Kendo grid to render (locked table has crime names)
  await page.waitForSelector("table", { timeout: 30000 });
  // Extra wait for Angular to finish rendering all rows
  await page.waitForFunction(
    () => {
      const tables = document.querySelectorAll("table");
      if (tables.length < 3) return false;
      const locked = tables[2];
      const rows = locked.querySelectorAll("tr");
      // Need at least the 7 crime rows + header
      return rows.length >= 8;
    },
    { timeout: 30000 }
  );

  process.stderr.write("Dashboard loaded. Extracting data...\n");

  const results = [];

  for (const crime of CRIMES) {
    // Click the YTD cell for this crime
    const clicked = await page.evaluate((crimeName) => {
      const lockedTable = document.querySelectorAll("table")[2];
      const dataTable = document.querySelectorAll("table")[3];
      if (!lockedTable || !dataTable) return false;

      const lockedRows = lockedTable.querySelectorAll("tr");
      const dataRows = dataTable.querySelectorAll("tr");

      for (let i = 0; i < lockedRows.length; i++) {
        const name = lockedRows[i].querySelector("td")?.textContent?.trim();
        if (name === crimeName) {
          const cells = dataRows[i]?.querySelectorAll("td");
          if (cells && cells[6]) {
            cells[6].click(); // YTD 2026 column
            return true;
          }
        }
      }
      return false;
    }, crime);

    if (!clicked) {
      process.stderr.write(`  WARNING: Could not find "${crime}" in table\n`);
      continue;
    }

    // Wait for chart to update with new data
    await new Promise((r) => setTimeout(r, 2500));

    // Wait for SVG circles to appear in the timeline chart
    await page
      .waitForFunction(
        () => {
          const chart = document.querySelectorAll(".k-chart")[1];
          if (!chart) return false;
          return chart.querySelectorAll("svg circle").length > 0;
        },
        { timeout: 10000 }
      )
      .catch(() => {});

    // Read exact values from aria-label attributes on SVG circles
    const chartData = await page.evaluate(() => {
      const chart = document.querySelectorAll(".k-chart")[1];
      if (!chart) return null;
      const svg = chart.querySelector("svg");
      if (!svg) return null;

      const texts = Array.from(svg.querySelectorAll("text")).map(
        (t) => t.textContent
      );
      const months = texts.filter((t) => /^[A-Z][a-z]{2}$/.test(t));
      const circles = Array.from(svg.querySelectorAll("circle"));
      const title =
        texts.find((t) => t.includes("Citywide")) || "";

      const points = circles.map((c, i) => ({
        month: months[i] || `M${i + 1}`,
        value: parseInt(c.getAttribute("aria-label")),
      }));

      return { points, title };
    });

    if (!chartData || !chartData.points.length) {
      process.stderr.write(`  WARNING: No chart data for "${crime}"\n`);
      continue;
    }

    // Verify: title should contain the crime name
    const titleOk =
      chartData.title.toLowerCase().includes(crime.toLowerCase()) ||
      chartData.title.includes("Citywide");

    const sum = chartData.points.reduce((s, p) => s + p.value, 0);
    process.stderr.write(
      `  ${crime}: ${chartData.points.map((p) => `${p.month}=${p.value}`).join(", ")} (sum=${sum})${titleOk ? "" : " [TITLE MISMATCH]"}\n`
    );

    for (const pt of chartData.points) {
      results.push({
        crime,
        offense: OFFENSE_MAP[crime],
        month: pt.month,
        value: pt.value,
        title: chartData.title,
      });
    }
  }

  await browser.close();
  return results;
}

// Determine which months are complete (exclude current partial month)
function getCompleteMonths(results) {
  const now = new Date();
  const curYear = now.getFullYear();
  const curMonth = now.getMonth() + 1;

  // Parse the date range from title (e.g., "Murder - Citywide - 01/01/26 - 03/08/26")
  const title = results[0]?.title || "";
  const yearMatch = title.match(/(\d{2})\/(\d{2})\/(\d{2})\s*$/);
  const dataYear = yearMatch ? 2000 + parseInt(yearMatch[3]) : curYear;

  const monthNums = {
    Jan: 1,
    Feb: 2,
    Mar: 3,
    Apr: 4,
    May: 5,
    Jun: 6,
    Jul: 7,
    Aug: 8,
    Sep: 9,
    Oct: 10,
    Nov: 11,
    Dec: 12,
  };

  const complete = [];
  for (const r of results) {
    const mn = monthNums[r.month];
    if (!mn) continue;
    // Skip current incomplete month
    if (dataYear === curYear && mn >= curMonth) continue;
    complete.push({
      agency: "New York City",
      year: dataYear,
      month: mn,
      offense: r.offense,
      count: r.value,
    });
  }
  return complete;
}

(async () => {
  try {
    const raw = await scrape();
    const complete = getCompleteMonths(raw);

    if (process.argv.includes("--json")) {
      console.log(JSON.stringify(complete, null, 2));
    } else {
      // Pretty table output
      const byMonth = {};
      for (const r of complete) {
        const key = `${MONTH_NAMES[r.month]} ${r.year}`;
        if (!byMonth[key]) byMonth[key] = {};
        byMonth[key][r.offense] = r.count;
      }

      const offenses = [
        "Murder",
        "Rape",
        "Robbery",
        "Aggravated Assault",
        "Burglary",
        "Theft",
        "Motor Vehicle Theft",
      ];
      const header = ["Month", ...offenses];
      const widths = header.map((h) => Math.max(h.length, 8));

      console.log(header.map((h, i) => h.padEnd(widths[i])).join("  "));
      console.log(widths.map((w) => "-".repeat(w)).join("  "));

      for (const [month, data] of Object.entries(byMonth)) {
        const row = [
          month.padEnd(widths[0]),
          ...offenses.map((o, i) =>
            String(data[o] || 0)
              .padStart(widths[i + 1])
          ),
        ];
        console.log(row.join("  "));
      }

      console.log(
        `\n${complete.length} records (${Object.keys(byMonth).length} complete months)`
      );
    }
  } catch (err) {
    process.stderr.write(`Error: ${err.message}\n`);
    process.exit(1);
  }
})();
