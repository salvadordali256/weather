# Weather Project Reorganization — Design Spec

**Date:** 2026-06-06
**Status:** Approved (pending written-spec review)
**Repo:** `weather` (live site: weather.salvadordali256.net)
**Package name:** `snowforecast`

## 1. Problem

The repository was built ad-hoc and has ~245 tracked files, ~105 of them Python
scripts, almost all dumped in the root directory. Concrete pain points:

- **No structure / no "likeness":** collectors, forecast engines, one-off
  analyses, web app, tests, docs, and data all sit flat in the root.
- **Merge churn / contributors colliding:** generated artifacts (`.db`, logs,
  `forecast_output/`, JSON/CSV results) and editor/cache files (`.DS_Store`,
  `__pycache__/*.pyc`, `dist/`) are committed to git. Every forecast run mutates
  tracked files, so unrelated work constantly conflicts.
- **No clear ownership boundaries:** someone editing the website cannot avoid
  the forecast scripts, and vice-versa.
- **Duplication:** several near-duplicate forecast engines and dozens of dated
  one-off analysis scripts (`january_2026_*`, `predict_*`, `analyze_*`).

## 2. Goal

Reorganize into a conventional, modular Python project so that each top-level
area has one clear purpose and owner, contributors can work in parallel without
stepping on each other, and the **live forecast pipeline keeps running with zero
downtime**.

Decisions confirmed during brainstorming:

| Decision | Choice |
|---|---|
| Depth | Full restructure into an installable package |
| Legacy/one-off scripts | **Keep everything** — organize, do not delete or archive |
| Pipeline safety | Move + **root shims** + `MIGRATION.md` notes (zero NAS changes) |
| `public/` location | Move to `web/public/` + one-time Cloudflare setting change |
| Git hygiene | **Untrack** all generated/binary files (keep on disk) |

## 3. Non-Goals

- No rewriting of forecast logic or algorithms.
- No deleting or archiving of scripts (everything is kept, just relocated).
- No deduplication/merging of the duplicate engines (kept as-is per decision).
- No changes to the data-collection schedule or forecast cadence.

## 4. Target Structure

```
weather/                              # repo root
├── pyproject.toml                    # NEW: declares the snowforecast package (src layout)
├── README.md                         # updated for new layout
├── MIGRATION.md                      # NEW: cron + Cloudflare + venv migration notes
├── requirements.txt                  # consolidated (see §8)
├── .env.example
├── .gitignore                        # expanded (see §7)
├── Dockerfile, docker-compose.yml, .dockerignore
│
├── src/snowforecast/                 # THE importable library
│   ├── __init__.py
│   ├── config.py
│   ├── fetchers/                     # data-source clients (shared, imported widely)
│   │   ├── __init__.py
│   │   ├── openmeteo_weather_fetcher.py
│   │   ├── noaa_weather_fetcher.py
│   │   ├── gfs_atmospheric_fetcher.py
│   │   └── global_snowfall_fetcher.py
│   ├── engines/                      # forecast engines
│   │   ├── __init__.py
│   │   ├── enhanced_regional_forecast_system.py   # CANONICAL (cron uses this)
│   │   ├── enhanced_forecast_system.py
│   │   ├── comprehensive_forecast_system.py
│   │   ├── integrated_forecast_system.py
│   │   ├── pattern_matching_forecast.py
│   │   ├── major_event_predictor.py
│   │   └── weather_orchestrator.py
│   ├── storage/                      # database layer
│   │   ├── __init__.py
│   │   ├── snowfall_duckdb.py
│   │   ├── duckdb_queries.py
│   │   └── migrate_data_source.py
│   └── analysis/                     # reusable analysis library
│       ├── __init__.py
│       ├── jetstream_analyzer.py
│       ├── local_event_analyzer.py
│       ├── global_correlation_analysis.py
│       └── snowfall_analysis.py
│
├── scripts/                          # runnable jobs (import the library)
│   ├── pipeline/                     # cron-driven daily flow (ROOT-SHIMMED)
│   │   ├── update_recent_data.py
│   │   ├── update_global_predictors.py
│   │   ├── collect_world_data.py
│   │   ├── daily_automated_forecast.py
│   │   ├── automated_forecast_runner.py
│   │   ├── daily_forecast_runner.py
│   │   └── daily_snow_update.py
│   ├── collect/                      # collectors / fetch jobs / data QA
│   │   ├── collect_7days_openmeteo.py, collect_hourly_data.py,
│   │   ├── collect_noaa_data.py, collect_northwoods_full_history.py,
│   │   ├── collect_northwoods_wi.py, collect_radiosonde.py,
│   │   ├── collect_regional_stations.py, collect_resort_reports.py,
│   │   ├── collect_snotel_data.py, collect_up_michigan.py,
│   │   ├── quick_collect_7days.py, snowfall_collector.py,
│   │   ├── fetch_atmospheric_data.py, fetch_noaa_validation.py,
│   │   ├── historical_backfill.py, passive_backfill.py,
│   │   ├── expand_global_network.py, add_lake_superior_stations.py,
│   │   ├── research_noaa_stations.py, research_snotel_stations.py,
│   │   ├── check_data_completeness.py, check_noaa_coverage.py,
│   │   ├── check_stations.py, check_historical_event.py,
│   │   ├── investigate_data_units.py, ski_resort_validation_data.py,
│   │   └── noaa_forecast_integration_2025.py
│   ├── generate/                     # forecast/report generation, queries
│   │   ├── generate_daily_report.py, generate_forecast.py,
│   │   ├── generate_station_forecasts.py, forecast_14day.py,
│   │   └── query_current_snow.py
│   ├── backtest/                     # backtesting & validation
│   │   ├── backtesting_diagnostic_analysis.py,
│   │   ├── comprehensive_backtesting_report.py,
│   │   ├── enhanced_system_backtesting.py, FORECAST_ERROR_ANALYSIS.py,
│   │   ├── validate_noaa_data.py, verify_nws_forecast.py,
│   │   └── correct_winter_season_validation.py
│   ├── storage/                      # storage/infra utilities
│   │   ├── storage_calculator.py, storage_optimization_guide.py,
│   │   └── upload_to_kv.py
│   ├── setup_guide.py
│   └── shell/                        # all .sh scripts (ROOT-SHIMMED where cron-used)
│       ├── push_forecast.sh, daily_update.sh, early_morning_collection.sh,
│       ├── run_daily_forecast.sh, setup_automation.sh,
│       ├── setup_daily_automation.sh, setup_venv.sh, show_forecast.sh,
│       ├── snow-update.sh, sync_to_nas.sh
│
├── analysis/                         # one-off & dated explorations (kept, grouped)
│   ├── seasonal/                     # dated/seasonal forecasts & comparisons
│   │   ├── analyze_2012_2013_winter.py, analyze_7day_data.py,
│   │   ├── analyze_85year_trends.py, analyze_northwoods.py,
│   │   ├── analyze_polar_vortex.py, compare_2013_2017_winters.py,
│   │   ├── january_15_20_2026_UPDATED_forecast.py,
│   │   ├── january_15_20_SNOW_REASSESSMENT_jan5.py,
│   │   ├── january_2025_snowstorm_forecast.py,
│   │   ├── january_2026_FINAL_canadian_analysis.py,
│   │   ├── january_2026_GLOBAL_teleconnections.py,
│   │   ├── january_2026_REVISED_forecast.py,
│   │   ├── january_2026_snowstorm_forecast.py,
│   │   ├── predict_2024_2025_winter.py, predict_best_snow_week_2025.py,
│   │   ├── predict_up_michigan_2025.py, fetch_jan_15_20_forecast.py,
│   │   ├── fetch_jan_2026_realtime.py, retroactive_forecast_demo.py,
│   │   ├── demo_global_analysis.py, weekend_forecast_check.py,
│   │   └── run_global_analysis.py
│   └── visualize/                    # chart/plot generators
│       ├── visualize_demo_results.py, visualize_enso_snowfall.py,
│       ├── visualize_global_snowfall.py, visualize_snowfall_data.py
│
├── web/                              # the website
│   ├── public/                       # static site served by Cloudflare Pages
│   ├── templates/                    # Flask templates (base, dashboard, history, about, no_forecast)
│   ├── static/                       # app.css
│   ├── worker/                       # Cloudflare worker (index.js, package.json, wrangler.toml)
│   └── apps/                         # Flask/build apps
│       ├── weather_app.py, forecast_web_dashboard.py,
│       ├── forecast_verification_dashboard.py, build_static.py
│
├── data/                             # databases & generated outputs (gitignored, see §7)
│   ├── databases/                    # *.db (untracked, kept on disk)
│   ├── output/                       # forecast_output/, forecast_reports/, generated json/csv/txt
│   ├── logs/                         # runtime logs (untracked)
│   └── reference/                    # weather_data/ GHCND station CSV (TRACKED input data)
│
├── docs/                             # all .md + .txt documentation
│   ├── guides/                       # *_GUIDE.md, *_README.md, QUICK_*.{md,txt}, setup docs
│   ├── reports/                      # *_SUMMARY.{md,txt}, *_REPORT.md, analysis writeups
│   └── superpowers/specs/            # this design + future specs
│
├── tests/                            # test suite
│   ├── test_duckdb_setup.py, test_multiple_events.py,
│   ├── test_weather_app.py, full_data_test.py, debug_forecast.py
│
├── .github/workflows/                # forecast-pages-rebuild.yml (paths updated)
│
└── <root shims>                      # thin wrappers so the NAS crontab is untouched (§6)
    ├── update_recent_data.py, update_global_predictors.py,
    ├── collect_world_data.py, daily_automated_forecast.py,
    └── push_forecast.sh
```

### 4.1 Files staying at repo root (intentionally)

`README.md`, `MIGRATION.md`, `pyproject.toml`, `requirements.txt`,
`.env.example`, `.gitignore`, `Dockerfile`, `docker-compose.yml`,
`.dockerignore`, and the AI-assistant config files already in the repo
(`agents.md`, `gemini.md`, `agents.py`) remain at root. `agents.py` and
`run_global_analysis.py` are flagged in §10 as ambiguous — confirm placement
during implementation.

## 5. Import & Execution Model

- `pyproject.toml` declares `snowforecast` with a `src/` layout. A one-time
  `pip install -e .` in each environment (Mac dev + NAS venv) makes the library
  importable from anywhere.
- The shared library modules move into `src/snowforecast/{fetchers,engines,storage,analysis}/`.
  The ~27 existing cross-import statements are updated to package imports, e.g.:

  ```python
  # before
  from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher
  # after
  from snowforecast.fetchers.openmeteo_weather_fetcher import OpenMeteoWeatherFetcher
  ```

  Known import hotspots to update: `openmeteo_weather_fetcher` (10 sites),
  `snowfall_duckdb` (8), `noaa_weather_fetcher` (4),
  `enhanced_regional_forecast_system` (2), and one each for
  `gfs_atmospheric_fetcher`, `enhanced_forecast_system`,
  `comprehensive_forecast_system`.
- `scripts/**` and `analysis/**` import from `snowforecast` and are run either
  via the root shims (cron) or `python -m scripts.pipeline.daily_automated_forecast`.
- `git mv` is used for every move so per-file history (`git blame`) is preserved.

## 6. Pipeline Safety (zero downtime)

### 6.1 Root shims for cron-invoked entrypoints

The NAS crontab (outside this repo) invokes scripts by name from the repo root.
For each cron-referenced entrypoint, a thin root shim is left in place:

```python
# daily_automated_forecast.py  (root shim)
from scripts.pipeline.daily_automated_forecast import main
if __name__ == "__main__":
    main()
```

Shimmed entrypoints (from README + `.sh` scripts):
`update_recent_data.py`, `update_global_predictors.py`, `collect_world_data.py`,
`daily_automated_forecast.py`, and `push_forecast.sh`.

> Implementation note: each target script must expose a callable `main()` (or
> equivalent) for the shim to import. Where a script currently runs everything
> at module top level, wrap that body in a `main()` function guarded by
> `if __name__ == "__main__"`. This refactor must preserve existing behavior.

### 6.2 `push_forecast.sh`

Update the copy target from `public/` to `web/public/` inside the real script.
The root shim preserves the old invocation path for cron.

### 6.3 External config (documented in `MIGRATION.md`, not code)

- **Cloudflare Pages:** change build output directory `public` → `web/public`.
- **NAS venv:** run `pip install -e .` once after pulling the reorganized tree.
- **Optional cleanup:** how to later drop the root shims and point cron at the
  new paths.

### 6.4 GitHub Actions

`.github/workflows/forecast-pages-rebuild.yml` path references are updated to
the new locations.

## 7. Git Hygiene

`git rm --cached` (files remain on disk) + `.gitignore` rules for:

- `*.db` (the 7 databases) → live under `data/databases/`
- `__pycache__/`, `*.pyc`
- `.DS_Store` (all)
- `dist/` (build artifact of `build_static.py`)
- `logs/` and `data/logs/`
- generated outputs: `data/output/**` (forecast_output, forecast_reports,
  generated `*.json` / `*.csv` / `*.txt` results)

**Stays tracked (deployed data the site reads):**
`web/public/latest_forecast.json`, `web/public/station_data.json`,
`web/public/daily_report.json`. The pipeline overwrites these each run — intended.

**Stays tracked (input reference data):**
`data/reference/weather_data/stations_GHCND_*.csv`.

## 8. Requirements Consolidation

Merge `requirements.txt` and `weather_requirements.txt` into a single
`requirements.txt`. Any package present in only one file is preserved; the diff
is recorded in the migration commit message. `pyproject.toml` references the
same dependency set.

## 9. Migration Sequence (phased, each step a commit)

1. **Scaffold:** add `pyproject.toml`, package dirs + `__init__.py`, expanded
   `.gitignore`. Commit.
2. **Git hygiene + data move:** `git rm --cached` binaries/outputs/caches;
   `git mv` data into `data/**`. Commit. *(files stay on disk)*
3. **Library:** `git mv` shared modules into `src/snowforecast/**`; fix the ~27
   imports; `pip install -e .`; smoke-test canonical engine + fetcher imports.
   Commit.
4. **Scripts:** `git mv` runnable scripts into `scripts/**`; add root shims;
   wrap top-level bodies in `main()` as needed; update `push_forecast.sh` +
   GitHub workflow paths. Commit.
5. **Web / analysis / docs:** `git mv` web assets into `web/**`, one-offs into
   `analysis/**`, docs into `docs/**`. Commit.
6. **Verify + document:** run the daily pipeline locally end-to-end (collect →
   forecast → confirm `web/public/latest_forecast.json` written); run `tests/`;
   write `MIGRATION.md`; update `README.md`. Commit.

## 10. Open Items to Confirm During Implementation

- `agents.py` — purpose unclear (AI-assistant helper vs. orchestration). Default:
  leave at root with `agents.md`/`gemini.md`. Confirm on inspection.
- `run_global_analysis.py` — placed in `analysis/seasonal/`; confirm it is a
  one-off vs. a maintained entrypoint.
- `snowfall_collector.py` — placed in `scripts/collect/`; confirm it is a job,
  not a library module imported elsewhere.
- Exact set of cron-invoked scripts — validated against the live NAS crontab
  before finalizing the shim list (README + `.sh` scripts are the current source
  of truth).

## 11. Success Criteria

- `pip install -e .` succeeds; `python -c "import snowforecast"` works.
- Canonical engine import path resolves:
  `from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem`.
- Root shims run the relocated pipeline entrypoints unchanged.
- Local end-to-end pipeline run produces a fresh `web/public/latest_forecast.json`.
- `git status` is clean after a forecast run (no binary/output churn) — i.e.
  generated files are gitignored.
- `tests/` pass (to the same extent they passed before the move).
- Root directory contains only config, docs entry points, and shims — no loose
  library/script `.py` files.
- `git log --follow` still shows history for moved files.
```
