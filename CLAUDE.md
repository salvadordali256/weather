# CLAUDE.md

Guidance for Claude Code (and humans) working in this repo.

## What this is

A Wisconsin snowfall forecast system. A scheduled pipeline collects weather data,
generates a 7-day snowfall probability forecast, and publishes a static site to
Cloudflare Pages.

Pipeline: `NAS cron → collect data → generate forecast → git push → Cloudflare Pages`.
Live site: https://weather.salvadordali256.net

## Layout (post-reorg — see docs/MIGRATION.md)

The tree is organized into three areas — **Frontend** (`web/`), **Backend** (forecast
logic), and **Data Retrieval** (data clients, DB access, collection) — laid over the
Python convention of an installable library (`src/`) plus runnable `scripts/`. See the
README "Project layout" table for the full mapping.

```
src/snowforecast/        Installable library (import as `snowforecast`)
  config.py
  fetchers/              Data Retrieval — source clients (openmeteo, noaa, gfs, global_snowfall)
  engines/               Backend — forecast engines; enhanced_regional_forecast_system is CANONICAL
  storage/               Data Retrieval — snowfall_duckdb, duckdb_queries, migrate_data_source
  analysis/              Backend — reusable analysis (jetstream, correlations, local_event)
scripts/                 Runnable jobs (import the library)
  pipeline/              Backend — the cron-driven daily flow
  collect/               Data Retrieval — collection jobs
  generate/ backtest/    Backend — forecast generation & validation
  storage/ shell/ ci/
explorations/            One-off & dated studies (seasonal/, visualize/) — not imported
web/                     Frontend — the website
  public/                Static site served by Cloudflare Pages (build output dir)
  templates/ static/ worker/ apps/
tools/                   Dev tooling (agents.py security scanner)
docs/                    guides/, reports/, superpowers/ (specs + plans), MIGRATION.md
tests/
<root>                   Config, README, dependency & build files
```

## Setup / commands

```bash
pip install -r requirements.txt
pip install -e .                       # REQUIRED — makes `import snowforecast` work
python -m pytest tests/                 # tests (some need a local DB and will skip/fail offline)
python web/apps/forecast_web_dashboard.py   # local Flask preview on :5000
python web/apps/build_static.py             # DEPRECATED local preview -> ./dist (NOT deployed; see web/README.md)
```

Canonical engine import:
`from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem`

## Critical conventions — read before changing structure

- **The pipeline lives under `scripts/`; there are no root shims.** The NAS
  crontab invokes the entry points directly by path — e.g.
  `python scripts/pipeline/daily_automated_forecast.py` and
  `bash scripts/shell/push_forecast.sh`. (The old `runpy` wrappers at the repo
  root were removed once the crontab was updated; see docs/MIGRATION.md §5.)
  When adding a new cron-run entry point, put it under `scripts/` and point the
  crontab at that path. The cron still runs **from the repo root**, so
  root-relative data paths keep resolving.
- **Databases and generated outputs are gitignored but live at the repo root.**
  Scripts open them by root-relative paths (`sqlite3.connect('demo_global_snowfall.db')`,
  `forecast_output/...`). The cron runs from the repo root, so these files must
  stay at the root. **Do not "tidy" them into a `data/` folder** without first
  centralizing the ~32 hardcoded paths behind a `DATA_DIR`.
- **SQLite on the NAS needs a timeout.** Any new cron-path `sqlite3.connect()`
  must pass `timeout=30` to avoid "database is locked" on the NAS filesystem.
- **`web/public/` is the single source of truth for the website.** It is the
  hand-authored static site Cloudflare Pages deploys; the pipeline publishes by
  copying `forecast_output/*.json` into it (`scripts/shell/push_forecast.sh`).
  `web/apps/build_static.py` (-> `./dist`) and the Flask dashboard are
  local-preview only and are NOT deployed. See `web/README.md`.
- **`web/public/*.json` stays tracked.** It's the deployed forecast data the site
  reads; the pipeline overwrites it each run (intended).
- **Cloudflare Pages build output dir is `web/public`** (not the repo root).

## Two external systems depend on file locations

- **NAS venv:** run `pip install -e .` once after pulling a reorganized tree.
- **Cloudflare Pages:** build output directory must be `web/public`.

See `docs/MIGRATION.md` for the full details.

## Workflow notes

- Do real work on a feature branch, not `master`. Cloudflare deploys on push to
  `master`, so merging is a deploy.
- Design specs and implementation plans live in `docs/superpowers/`.
- `agents.md` / `gemini.md` are configs for other AI tools; this file is Claude's.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).