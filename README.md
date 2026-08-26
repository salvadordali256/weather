# Wisconsin Snowfall Forecast

Live site: [weather.salvadordali256.net](https://weather.salvadordali256.net)

7-day snowfall probability forecast for Northern Wisconsin (Phelps, Land O'Lakes, Eagle River) using global teleconnection patterns and regional predictors.

## Architecture

```
NAS (Synology/Pi) ── cron ──> collect data ──> generate forecast ──> git push ──> Cloudflare Pages
```

- **NAS (10.0.0.249)**: Primary runner. Synology NAS powered by Raspberry Pi running Debian.
- **Mac**: Development only. No scheduled jobs.
- **Data source**: Open-Meteo API (60+ global weather stations)
- **Database**: SQLite (`demo_global_snowfall.db`, ~970K records)
- **Hosting**: Cloudflare Pages serves `web/public/index.html` + `latest_forecast.json`

## Scheduled Updates (NAS Cron)

| Time (CST) | What runs |
|---|---|
| **5:30 PM** | `scripts/pipeline/update_recent_data.py` → `scripts/pipeline/update_global_predictors.py` → `scripts/pipeline/collect_world_data.py --days 7` → `scripts/pipeline/daily_automated_forecast.py` → `scripts/shell/push_forecast.sh` |
| **6:00 AM** | `scripts/pipeline/daily_automated_forecast.py` → `scripts/shell/push_forecast.sh` |

The 5:30 PM run collects fresh data from all stations and generates a new forecast. The 6:00 AM run refreshes the forecast with any overnight pattern changes.

## Key Scripts

| Script | Purpose |
|---|---|
| `scripts/pipeline/update_recent_data.py` | Updates 7 days of regional station data (8 stations) |
| `scripts/pipeline/update_global_predictors.py` | Updates 14 days of global lead indicators (Sapporo, Chamonix, etc.) |
| `scripts/pipeline/collect_world_data.py` | Collects data for 60+ global weather stations |
| `scripts/pipeline/daily_automated_forecast.py` | Generates 7-day ensemble forecast as JSON |
| `scripts/shell/push_forecast.sh` | Copies forecast to `web/public/`, commits, and pushes to GitHub |
| `src/snowforecast/engines/enhanced_regional_forecast_system.py` | Core forecast engine (regional + global ensemble) |

## Setup

```bash
# Clone and install
git clone https://github.com/salvadordali256/weather.git
cd weather
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .   # install the snowforecast package (editable)

# Configure environment
cp .env.example .env
# Edit .env with your settings
```

## Environment Variables (.env)

```
DB_PATH=demo_global_snowfall.db          # SQLite database path
FORECAST_OUTPUT_DIR=forecast_output       # Where forecasts are saved
NOAA_API_TOKEN=your_token                 # Optional NOAA API access
```

## Cloudflare Pages

The site is deployed via Git integration. Every `git push` to `master` triggers a Cloudflare Pages build. The NAS pushes automatically after each forecast run.

Cloudflare serves the `web/public/` directory which contains:
- `index.html` — Single-page forecast dashboard (auto-refreshes every 30 min)
- `latest_forecast.json` — Current forecast data

## GitHub Actions

The workflow `.github/workflows/forecast-pages-rebuild.yml` runs as a backup pipeline. The NAS cron is the primary source of updates.

## SQLite on NAS

All `sqlite3.connect()` calls in cron-critical scripts use `timeout=30` to prevent "database is locked" errors on the NAS filesystem. If adding new scripts that run on the NAS, always include the timeout:

```python
conn = sqlite3.connect('demo_global_snowfall.db', timeout=30)
```

## Local Preview

```bash
source venv/bin/activate
python web/apps/forecast_web_dashboard.py    # Flask app on localhost:5000
```

## Project layout

The codebase is organized into three areas — **Frontend**, **Backend**, and **Data Retrieval** — laid over a conventional Python structure (an installable `snowforecast` library in `src/`, thin runnable `scripts/`, and the `web/` site). Use this table to find things:

| Area | Where | What |
|---|---|---|
| **Frontend** | `web/` | `public/` = deployed static site (Cloudflare Pages); `templates/` + `static/` + `apps/` = local-preview Flask; `worker/` = Cloudflare worker |
| **Backend** (forecast logic) | `src/snowforecast/engines/`, `src/snowforecast/analysis/`, `src/snowforecast/config.py`; `scripts/generate/`, `scripts/pipeline/`, `scripts/backtest/` | Forecast models, orchestration, and the cron pipeline. There is no live app server — production is a static site fed by a batch cron pipeline. |
| **Data Retrieval** | `src/snowforecast/fetchers/`, `src/snowforecast/storage/`; `scripts/collect/` | Weather-API clients, DB access/queries, and the collection jobs |
| *Shared / global* | repo root, `docs/`, `tests/`, `tools/` | config, dependencies, Docker, cron shims, docs, tests, dev tooling |

`src/snowforecast/` is the reusable, importable, testable core; `scripts/` are thin runnable entry points (and the cron pipeline) that import it.

```
weather/
├── src/snowforecast/          # Library package: config, fetchers, engines, storage, analysis
├── scripts/                   # Runnable jobs and shell scripts
│   ├── pipeline/              # Cron-driven daily flow (forecast, data collection)
│   ├── collect/               # Data-retrieval collection jobs
│   ├── generate/ backtest/    # Backend forecast generation & validation
│   ├── shell/ storage/ ci/    # Shell scripts, capacity utilities, CI checks
│   └── setup_guide.py
├── web/                       # Frontend: the website
│   ├── public/                # Cloudflare Pages build output (tracked JSON/HTML)
│   ├── templates/ static/     # Flask templates and assets
│   ├── apps/                  # Local-preview Flask apps
│   └── worker/                # Cloudflare Worker scripts
├── explorations/              # One-off seasonal & visualization studies (not imported)
├── tools/                     # Dev tooling (agents.py security scanner)
├── tests/                     # Test suite
└── docs/                      # Guides, reports, specs, plans, MIGRATION.md
```

The pipeline is invoked directly under `scripts/` (e.g. `python scripts/pipeline/daily_automated_forecast.py`, `bash scripts/shell/push_forecast.sh`) — the root no longer carries shim wrappers, so the NAS crontab points at these paths.
See [docs/MIGRATION.md](docs/MIGRATION.md) for the NAS crontab and Cloudflare Pages setup steps.
