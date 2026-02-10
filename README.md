# Wisconsin Snowfall Forecast

Live site: [weather.salvadordali256.net](https://weather.salvadordali256.net)

7-day snowfall probability forecast for Northern Wisconsin (Phelps, Land O'Lakes, Eagle River) using global teleconnection patterns and regional predictors.

## Architecture

```
NAS (Synology/Pi) ── cron ──> collect data ──> generate forecast ──> git push ──> Cloudflare Pages
```

- **NAS (10.144.150.177)**: Primary runner. Synology NAS powered by Raspberry Pi running Debian.
- **Mac**: Development only. No scheduled jobs.
- **Data source**: Open-Meteo API (60+ global weather stations)
- **Database**: SQLite (`demo_global_snowfall.db`, ~970K records)
- **Hosting**: Cloudflare Pages serves `public/index.html` + `latest_forecast.json`

## Scheduled Updates (NAS Cron)

| Time (CST) | What runs |
|---|---|
| **5:30 PM** | `update_recent_data.py` → `update_global_predictors.py` → `collect_world_data.py --days 7` → `daily_automated_forecast.py` → `push_forecast.sh` |
| **6:00 AM** | `daily_automated_forecast.py` → `push_forecast.sh` |

The 5:30 PM run collects fresh data from all stations and generates a new forecast. The 6:00 AM run refreshes the forecast with any overnight pattern changes.

## Key Scripts

| Script | Purpose |
|---|---|
| `update_recent_data.py` | Updates 7 days of regional station data (8 stations) |
| `update_global_predictors.py` | Updates 14 days of global lead indicators (Sapporo, Chamonix, etc.) |
| `collect_world_data.py` | Collects data for 60+ global weather stations |
| `daily_automated_forecast.py` | Generates 7-day ensemble forecast as JSON |
| `push_forecast.sh` | Copies forecast to `public/`, commits, and pushes to GitHub |
| `enhanced_regional_forecast_system.py` | Core forecast engine (regional + global ensemble) |

## Setup

```bash
# Clone and install
git clone https://github.com/salvadordali256/weather.git
cd weather
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

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

Cloudflare serves the `public/` directory which contains:
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
python forecast_web_dashboard.py    # Flask app on localhost:5000
```
