// CORS Proxy for RTCI Crime Data Pipeline
// Fetches URLs on behalf of the browser and returns with CORS headers.
// Follows redirects server-side (up to 10 hops).
// Includes /state-srs endpoint for batch Optimum-platform SRS fetching (TX, PA, etc.).
// Run: node cors-proxy.js   (or via pm2: pm2 start cors-proxy.js --name cors-proxy)

const http = require('http');
const https = require('https');
const { URL } = require('url');

const PORT = 3001;
const MAX_REDIRECTS = 10;

// Whitelist of allowed domains (security: only proxy known city/agency sites)
const ALLOWED_DOMAINS = new Set([
  'jpso.com', 'www.jpso.com',
  'sarasotasheriff.org', 'www.sarasotasheriff.org',
  'glendaleca.gov', 'www.glendaleca.gov',
  'fontanaca.gov', 'www.fontanaca.gov',
  'milpitas.gov', 'www.milpitas.gov',
  'murrietaca.gov', 'www.murrietaca.gov',
  'cityofbowie.org', 'www.cityofbowie.org',
  'stocktonca.gov', 'www.stocktonca.gov',
  'cms3.revize.com',
  'tracypd.com', 'www.tracypd.com',
  'alhambraca.gov', 'www.alhambraca.gov',
  'cityofconcord.org', 'www.cityofconcord.org',
  'cityoffortwayne.in.gov', 'www.cityoffortwayne.in.gov',
  'antiochca.gov', 'www.antiochca.gov',
  'northbergenpolice.com', 'www.northbergenpolice.com',
  'westminster-ca.gov', 'www.westminster-ca.gov',
  'normanok.gov', 'www.normanok.gov',
  'cob.org', 'www.cob.org',
  'crimestats.ne.gov',
  'crimestats.dos.nh.gov',
  'data.princegeorgescountymd.gov',
  'sanbernardino.gov', 'www.sanbernardino.gov',
  'vallejopd.net', 'www.vallejopd.net',
  'sanfranciscopolice.org', 'www.sanfranciscopolice.org',
  'cabq.gov', 'www.cabq.gov',
  'cjis.lincoln.ne.gov',
  'albanyga.gov', 'www.albanyga.gov',
  'txucr.nibrs.com',
  'www.ucr.pa.gov',
  'data.wprdc.org',
]);

function isAllowed(hostname) {
  if (ALLOWED_DOMAINS.has(hostname)) return true;
  for (const d of ALLOWED_DOMAINS) {
    if (hostname.endsWith('.' + d)) return true;
  }
  return false;
}

// Simple HTTPS GET that returns a promise with the response body
function httpsGet(url, timeout) {
  timeout = timeout || 30000;
  return new Promise(function(resolve, reject) {
    var parsed = new URL(url);
    var req = https.get({
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json,*/*',
      },
      timeout: timeout,
    }, function(res) {
      var data = '';
      res.on('data', function(c) { data += c; });
      res.on('end', function() {
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(data);
        else reject(new Error('HTTP ' + res.statusCode));
      });
    });
    req.on('error', reject);
    req.on('timeout', function() { req.destroy(); reject(new Error('timeout')); });
  });
}

// Generic Optimum-platform SRS batch endpoint
// GET /state-srs?base=https://txucr.nibrs.com/SRSReport&state=TX&values=16,708,...&names=N1|||N2|||...&types=City|||County|||...&start=06/01/2025&end=05/01/2026
// Works for any Optimum SRS platform (TX, PA, etc.) — just change the base URL and state.
var SRS_MAP = {
  'Murder and Nonnegligent Homicide': 'Murder',
  'Rape': 'Rape',
  'Robbery': 'Robbery',
  'Aggravated Assault': 'Aggravated Assault',
  'Burglary': 'Burglary',
  'Larceny - Theft': 'Theft',
  'Motor Vehicle Theft': 'Motor Vehicle Theft',
};
var MONTH_MAP = {Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12};

// Only allow known Optimum SRS base URLs
var ALLOWED_SRS_BASES = [
  'https://txucr.nibrs.com/SRSReport',
  'https://www.ucr.pa.gov/PAUCRSPublic/SRSReport',
];

async function handleStateSRS(reqUrl, res) {
  var baseUrl = reqUrl.searchParams.get('base') || '';
  var stateCode = reqUrl.searchParams.get('state') || '';
  var valuesParam = reqUrl.searchParams.get('values') || '';
  var namesParam = reqUrl.searchParams.get('names') || '';
  var typesParam = reqUrl.searchParams.get('types') || '';
  var startDate = reqUrl.searchParams.get('start') || '';
  var endDate = reqUrl.searchParams.get('end') || '';

  if (!ALLOWED_SRS_BASES.includes(baseUrl)) {
    res.writeHead(403, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ error: 'SRS base URL not allowed: ' + baseUrl }));
    return;
  }

  var values = valuesParam.split(',').filter(Boolean);
  var names = namesParam.split('|||').filter(Boolean);
  var types = typesParam.split('|||').filter(Boolean);

  if (!values.length || !startDate || !endDate || !stateCode) {
    res.writeHead(400, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ error: 'Missing base, state, values, start, or end params' }));
    return;
  }

  var allData = [];
  var errors = 0;

  // Fetch all agencies in parallel server-side
  var promises = values.map(function(val, i) {
    var agName = names[i] || ('Agency_' + val);
    var agType = types[i] || 'City';
    return (async function() {
      try {
        var url = baseUrl + '/GetCrimeTrends?ReportType=Agency&DrillDownReportIDs=-1'
          + '&StartDate=' + encodeURIComponent(startDate)
          + '&EndDate=' + encodeURIComponent(endDate)
          + '&ReportIDs=' + val + '&OffenseIDs=P1';
        var text = await httpsGet(url, 15000);
        var j = JSON.parse(text);
        if (!j.periodlist || !j.crimeList) return;
        var dates = j.periodlist.map(function(d) {
          var parts = d.split('/');
          return { year: parseInt(parts[0]), month: MONTH_MAP[parts[1]] };
        });
        for (var ci = 0; ci < j.crimeList.length; ci++) {
          var crime = j.crimeList[ci];
          var offense = SRS_MAP[crime.name];
          if (!offense) continue;
          var vals = crime.data || [];
          for (var k = 0; k < dates.length && k < vals.length; k++) {
            if (vals[k] == null) continue;
            allData.push({
              agency: agName, state: stateCode, type: agType,
              year: dates[k].year, month: dates[k].month,
              offense: offense, count: vals[k],
            });
          }
        }
      } catch (e) {
        errors++;
      }
    })();
  });

  await Promise.all(promises);

  res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify({ data: allData, total: values.length, errors: errors }));
}

// Generic proxy

function doFetch(targetUrl, res, redirectCount) {
  var parsed;
  try { parsed = new URL(targetUrl); } catch(e) {
    res.writeHead(400); res.end('Invalid URL: ' + targetUrl); return;
  }

  if (!isAllowed(parsed.hostname)) {
    res.writeHead(403); res.end('Domain not whitelisted: ' + parsed.hostname); return;
  }

  var proto = parsed.protocol === 'https:' ? https : http;
  var fetchOpts = {
    hostname: parsed.hostname,
    port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
    path: parsed.pathname + parsed.search,
    method: 'GET',
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    timeout: 30000,
  };

  var proxyReq = proto.request(fetchOpts, function(proxyRes) {
    if ([301, 302, 303, 307, 308].includes(proxyRes.statusCode) && proxyRes.headers.location) {
      if (redirectCount >= MAX_REDIRECTS) {
        res.writeHead(502); res.end('Too many redirects'); return;
      }
      var loc = new URL(proxyRes.headers.location, targetUrl).href;
      proxyRes.resume();
      doFetch(loc, res, redirectCount + 1);
      return;
    }

    var ct = proxyRes.headers['content-type'] || 'application/octet-stream';
    res.writeHead(proxyRes.statusCode, {
      'Content-Type': ct,
      'Access-Control-Allow-Origin': '*',
    });
    proxyRes.pipe(res);
  });

  proxyReq.on('error', function(e) {
    if (!res.headersSent) { res.writeHead(502); res.end('Proxy error: ' + e.message); }
  });
  proxyReq.on('timeout', function() {
    proxyReq.destroy();
    if (!res.headersSent) { res.writeHead(504); res.end('Proxy timeout'); }
  });
  proxyReq.end();
}

var server = http.createServer(function(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  if (req.method !== 'GET') { res.writeHead(405); res.end('Method not allowed'); return; }

  var reqUrl = new URL(req.url, 'http://localhost:' + PORT);

  // Optimum SRS batch endpoint (generic for TX, PA, etc.)
  if (reqUrl.pathname === '/state-srs' || reqUrl.pathname === '/texas-srs') {
    // Backward compat: /texas-srs defaults base/state to TX
    if (reqUrl.pathname === '/texas-srs' && !reqUrl.searchParams.get('base')) {
      reqUrl.searchParams.set('base', 'https://txucr.nibrs.com/SRSReport');
      reqUrl.searchParams.set('state', 'TX');
    }
    handleStateSRS(reqUrl, res);
    return;
  }

  // Generic proxy
  var target = reqUrl.searchParams.get('url');
  if (!target) { res.writeHead(400); res.end('Missing ?url= parameter'); return; }
  doFetch(target, res, 0);
});

server.listen(PORT, '0.0.0.0', function() {
  console.log('CORS proxy listening on port ' + PORT);
  console.log('Whitelisted domains: ' + ALLOWED_DOMAINS.size);
  console.log('State SRS endpoint: /state-srs (TX, PA)');
});
