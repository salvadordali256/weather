# Migration Notes — Project Reorganization (2026-06-06)

The repo was reorganized into a `snowforecast` package. The pipeline now lives
under `scripts/` (the root shims were removed — §5), and external systems
(NAS crontab, Cloudflare Pages) need a one-time update.

## 1. NAS venv — install the package (REQUIRED)

After pulling the reorganized tree on the NAS:

```bash
cd /path/to/weather
source venv/bin/activate     # or use ./venv/bin/pip directly
pip install -e .
```

The cron commands changed when the root shims were removed — update the NAS
crontab to invoke the pipeline directly under `scripts/` (see §5).

## 2. Cloudflare Pages — change the build output directory (REQUIRED)

The static site moved from `public/` to `web/public/`. In the Cloudflare Pages
project settings:

- **Build output directory:** `public` → `web/public`

No other Pages setting changes. The deployed JSON files
(`web/public/latest_forecast.json`, etc.) remain tracked in git.

## 3. Databases & outputs (informational)

Databases (`*.db`), `logs/`, `forecast_output/`, `forecast_reports/`,
`snowfall_graphs/`, and root result `*.json`/`*.csv` are now **gitignored** and
**untracked**. They remain on disk at the repo root, where the code expects them
(paths are hardcoded/anchored relative to the repo root). Nothing was deleted.

## 4. GitHub Actions backup workflow

`.github/workflows/forecast-pages-rebuild.yml` now runs `pip install -e .` and
calls `python web/apps/build_static.py`. Its commit step uses a single
`git add -A` (the old explicit adds targeted now-gitignored files). The workflow
is disabled by default (manual `workflow_dispatch` only); the NAS cron is the
primary pipeline.

## 5. Root shims removed — point cron at `scripts/` (REQUIRED)

The root shims have been removed. Update the NAS crontab to call the entry
points directly by path (the cron still runs **from the repo root**, so
root-relative data paths keep resolving):

| Old (root shim) | New (direct path) |
|---|---|
| `python update_recent_data.py` | `python scripts/pipeline/update_recent_data.py` |
| `python update_global_predictors.py` | `python scripts/pipeline/update_global_predictors.py` |
| `python collect_world_data.py --days 7` | `python scripts/pipeline/collect_world_data.py --days 7` |
| `python daily_automated_forecast.py` | `python scripts/pipeline/daily_automated_forecast.py` |
| `bash push_forecast.sh` | `bash scripts/shell/push_forecast.sh` |

The remaining entry points moved similarly: `collect_hourly_data.py`,
`collect_noaa_data.py`, `collect_radiosonde.py`, `collect_resort_reports.py`,
and `passive_backfill.py` now live under `scripts/collect/`;
`generate_daily_report.py` and `generate_station_forecasts.py` under
`scripts/generate/`.

## New layout

See `docs/superpowers/specs/2026-06-06-weather-project-reorganization-design.md`
and `docs/superpowers/plans/2026-06-06-weather-project-reorganization.md`.
