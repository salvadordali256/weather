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
| **5:30 PM** | `update_recent_data.py` → `update_global_predictors.py` → `collect_world_data.py --days 7` → `daily_automated_forecast.py` → `push_forecast.sh` |
| **6:00 AM** | `daily_automated_forecast.py` → `push_forecast.sh` |

The 5:30 PM run collects fresh data from all stations and generates a new forecast. The 6:00 AM run refreshes the forecast with any overnight pattern changes.

## Key Scripts

| Script | Purpose |
|---|---|
| `update_recent_data.py` | Updates 7 days of regional station data (8 stations) — cron calls root shim → `scripts/pipeline/` |
| `update_global_predictors.py` | Updates 14 days of global lead indicators (Sapporo, Chamonix, etc.) — cron calls root shim → `scripts/pipeline/` |
| `collect_world_data.py` | Collects data for 60+ global weather stations — cron calls root shim → `scripts/pipeline/` |
| `daily_automated_forecast.py` | Generates 7-day ensemble forecast as JSON — cron calls root shim → `scripts/pipeline/` |
| `push_forecast.sh` | Copies forecast to `web/public/`, commits, and pushes to GitHub — root shim → `scripts/shell/push_forecast.sh` |
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

```
weather/
├── src/snowforecast/          # Library package (config, fetchers, engines, storage, analysis)
├── scripts/                   # Pipeline jobs and shell scripts
│   ├── pipeline/              # Python pipeline modules (daily forecast, data collection, etc.)
│   ├── shell/                 # Shell scripts (push_forecast.sh)
│   ├── collect/, generate/, backtest/, storage/
│   └── setup_guide.py
├── web/                       # Web site
│   ├── public/                # Cloudflare Pages build output (tracked JSON/HTML)
│   ├── templates/, static/    # Flask templates and assets
│   ├── apps/                  # Flask apps (forecast_web_dashboard.py, build_static.py)
│   └── worker/                # Cloudflare Worker scripts
├── analysis/                  # One-off seasonal and visualization scripts
├── tests/                     # Test suite
├── docs/                      # Guides, reports, specs, and plans
├── daily_automated_forecast.py     # Root shim → scripts/pipeline/
├── update_recent_data.py           # Root shim → scripts/pipeline/
├── update_global_predictors.py     # Root shim → scripts/pipeline/
├── collect_world_data.py           # Root shim → scripts/pipeline/
└── push_forecast.sh                # Root shim → scripts/shell/push_forecast.sh
```

Root shims exist so the NAS cron commands require no changes after the reorg.
See [MIGRATION.md](MIGRATION.md) for one-time update steps required on the NAS and Cloudflare Pages.
