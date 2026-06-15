# Web — what's deployed vs. what's local-only

There used to be two parallel website build paths. After the reorg, **`web/public/` is
the single source of truth.** This note exists so the orphaned path doesn't get
wired back into deployment.

## Deployed (production)

- **`web/public/`** — the static site Cloudflare Pages serves at
  https://weather.salvadordali256.net. It is hand-authored
  (`index.html` + `forecast.js`, `planner.html` + `planner.js`, `report.html` +
  `report.js`) and reads JSON that the pipeline drops next to it.
- The pipeline publishes by copying `forecast_output/*.json` into `web/public/`
  and pushing to `master` — see `scripts/shell/push_forecast.sh`. A push to
  `master` is a deploy.
- Cloudflare Pages **build output directory must stay `web/public`.**

## Local preview only (NOT deployed)

- **`web/apps/forecast_web_dashboard.py`** — a Flask app for previewing forecasts
  locally (`python web/apps/forecast_web_dashboard.py`, then http://localhost:5000).
  It renders `web/templates/` and reads `forecast_output/latest_forecast.json`.
  Handy for development; it is not the production site.
- **`web/apps/build_static.py`** — *deprecated.* It renders `web/templates/` into
  `./dist`, but `dist/` is gitignored and is **not** what Cloudflare deploys, so
  its output never ships. Kept for reference/local preview only. Do not add it to
  CI or the publish flow.

## CI

`.github/workflows/forecast-pages-rebuild.yml` only **validates** that
`web/public/` is well-formed (entry point present, JSON parses, forecast has a
non-empty `forecasts` array). It does not generate or commit anything — the NAS
cron owns generation and publishing.
