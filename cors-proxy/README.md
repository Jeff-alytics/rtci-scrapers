# CORS proxy

Lets the RTCI pipeline (rtci-pipeline-tool.vercel.app) read city sites that don't send CORS headers.
Public URL: `https://assistant.crimeindex.org/proxy/?url=<encoded url>`

This folder is a copy for version history. The running copy lives on the Hetzner VPS (178.156.243.51):

| What | Where on the VPS |
|---|---|
| Proxy script | `/opt/cors-proxy.js` (pm2 app `cors-proxy`, port 3001) |
| nginx site | `/etc/nginx/sites-enabled/assistant-crimeindex` (`/proxy/` → 127.0.0.1:3001) |

## Adding a site

1. Add the domain to `ALLOWED_DOMAINS` in `/opt/cors-proxy.js` (back the file up first).
2. `node --check /opt/cors-proxy.js && pm2 restart cors-proxy` (restart only this app; never `pm2 update`).
3. Copy the updated file into this folder and commit, so the two stay in sync.

A 403 that comes back with the site's own "Access Denied" or Cloudflare "Just a moment…" page is the site blocking us, not the allowlist.
