# Migration Notes — Project Reorganization (2026-06-06)

The repo was reorganized into a `snowforecast` package. The live pipeline keeps
working via root shims, but two **external** systems need a one-time update.

## 1. NAS venv — install the package (REQUIRED)

After pulling the reorganized tree on the NAS:

```bash
cd /path/to/weather
source venv/bin/activate     # or use ./venv/bin/pip directly
pip install -e .
```

The cron commands do **not** change — root shims (`daily_automated_forecast.py`,
`update_recent_data.py`, `update_global_predictors.py`, `collect_world_data.py`,
`push_forecast.sh`) still live at the repo root and forward to `scripts/`.

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

## 5. Optional later cleanup — drop the root shims

To point cron directly at the new paths and remove the shims:

```bash
# in the NAS crontab, replace e.g.
#   python daily_automated_forecast.py
# with
#   python -m scripts.pipeline.daily_automated_forecast
# and for the shell shim:
#   bash scripts/shell/push_forecast.sh
```
Then delete the 4 `.py` shims and the `push_forecast.sh` shim from the repo root.

## New layout

See `docs/superpowers/specs/2026-06-06-weather-project-reorganization-design.md`
and `docs/superpowers/plans/2026-06-06-weather-project-reorganization.md`.
