"""Read-only VPS retrieval probe. Run: python3 check-city-access.py

Uses existing Python Playwright and Chromium; installs nothing and does not
change the proxy, nginx, or pm2. Exit 0 = all three checks passed, 1 = retrieval
failed, 2 = setup/runtime error. No CAPTCHA handling or stealth plugins.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

MONTHS = 'january february march april may june july august september october november december'.split()
READY = r"""kind => {
  if (kind === 'index') return Array.from(document.querySelectorAll('a[href]')).some(a => /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d{2}\s+Crime Statistics$/i.test(a.textContent.trim()));
  return Array.from(document.querySelectorAll('table')).some(t => /Murder|Homicide/i.test(t.textContent) && /Robbery/i.test(t.textContent));
}"""
EVIDENCE = r"""kind => {
  const tables = Array.from(document.querySelectorAll('table')).map(t => Array.from(t.querySelectorAll('tr')).map(r => Array.from(r.querySelectorAll('th,td')).map(c => c.textContent.trim())));
  const table = tables.find(rows => rows.some(r => /^(Murder|Homicide)$/i.test(r[0] || '')));
  const links = Array.from(document.querySelectorAll('a[href]')).filter(a => /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d{2}\s+Crime Statistics$/i.test(a.textContent.trim())).map(a => ({title:a.textContent.trim(),url:a.href}));
  return {
    title:document.title,
    yearHeading:(document.body.textContent.match(/City of Antioch\s+(20\d{2})\s+Crime Statistics/i)||[])[1]||null,
    tableHeader:table ? table[0] : null,
    sampleRows:table ? table.filter(r => /^(Murder|Homicide|Robbery)$/i.test(r[0]||'')) : [],
    monthlyLinks:kind === 'index' ? links : [],
    blockDetected:/Sorry, you have been blocked|Access Denied|Just a moment|Verify you are human/i.test(document.title+' '+document.body.innerText),
    usable:kind === 'index' ? links.length > 0 : !!table
  };
}"""


def main():
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    results = []
    with sync_playwright() as pw:
        # Use the full Chromium executable, avoiding a separate headless-shell
        # download. Prefer the package's matching revision. If absent, use only
        # the explicitly reported installed revision, and report that choice.
        executable = Path(pw.chromium.executable_path)
        if not executable.is_file():
            candidates = [p for p in Path('/root/.cache/ms-playwright/chromium-1208').glob('*/chrome') if p.is_file()]
            if len(candidates) != 1:
                raise RuntimeError('Installed Chromium executable not found unambiguously; no installation attempted.')
            executable = candidates[0]
        browser = pw.chromium.launch(headless=True, executable_path=str(executable))
        try:
            context = browser.new_context()

            def inspect(name, url, kind):
                page = context.new_page()
                result = {'name': name, 'requestedUrl': url, 'usable': False}
                try:
                    response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
                    try:
                        page.wait_for_function(READY, arg=kind, timeout=20000)
                    except PlaywrightTimeout:
                        pass  # Inspect the actual page, including any block.
                    result.update(page.evaluate(EVIDENCE, kind))
                    result.update(finalUrl=page.url, httpStatus=response.status if response else None)
                except Exception as error:
                    result['error'] = str(error)
                finally:
                    results.append(result)
                    page.close()
                return result

            inspect('Antioch', 'https://www.antiochca.gov/446/Crime-Statistics', 'table')
            index = inspect('Fort Wayne index', 'https://www.cityoffortwayne.in.gov/699/Crime-Stats', 'index')
            links = [link for link in index.get('monthlyLinks', []) if urlparse(link['url']).scheme == 'https' and urlparse(link['url']).netloc == 'www.cityoffortwayne.in.gov']
            def rank(link):
                month, year, *_ = link['title'].split()
                return int(year) * 12 + MONTHS.index(month.lower())
            if links:
                inspect('Fort Wayne latest month', max(links, key=rank)['url'], 'table')
            context.close()
        finally:
            browser.close()
            print(json.dumps({'checkedAt': datetime.now(timezone.utc).isoformat(), 'executable': str(executable), 'results': results}, indent=2))
    return 0 if len(results) == 3 and all(r.get('usable') and not r.get('blockDetected') and r.get('httpStatus') == 200 for r in results) else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('PROBE_FAILED: ' + str(error), file=sys.stderr)
        sys.exit(2)
