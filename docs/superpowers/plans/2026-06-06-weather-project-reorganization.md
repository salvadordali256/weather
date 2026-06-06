# Weather Project Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the flat ~245-file weather repo into a modular `snowforecast` Python package with clear `src/` / `scripts/` / `web/` / `docs/` boundaries, untracking generated artifacts, while keeping the live forecast pipeline running with zero downtime.

**Architecture:** A `src/` layout package (`snowforecast`) holds shared library code (fetchers, engines, storage, analysis). Runnable jobs live in `scripts/`, one-off explorations in `analysis/`, the website in `web/`. Cron-invoked entrypoints get thin `runpy` shims at the repo root so the NAS crontab needs no changes. Generated/binary files are untracked in place (not moved) because 32+ files hardcode their paths relative to the repo root.

**Tech Stack:** Python 3.10+, setuptools (`pyproject.toml`, editable install), git, Flask (web preview), Cloudflare Pages/Workers (hosting).

**Spec:** `docs/superpowers/specs/2026-06-06-weather-project-reorganization-design.md`

**Conventions for every command:** run from the repo root `c:\Users\cwirk\projects\weather` using the Bash tool (git-bash). Use `git mv` for all relocations to preserve history. Commit after each task.

---

## Task 0: Baseline snapshot (safety net)

**Files:** none (read-only + a scratch file)

- [ ] **Step 1: Confirm clean tree and capture current branch**

Run:
```bash
git status --porcelain && git rev-parse --abbrev-ref HEAD
```
Expected: no output from the first command (clean tree); branch printed (e.g. `task/reorg`).

- [ ] **Step 2: Record a baseline import + compile check**

Run:
```bash
python -c "import compileall,sys; sys.exit(0 if compileall.compile_dir('.', quiet=1, maxlevels=0) else 1)" ; echo "compile_root_exit=$?"
git ls-files | wc -l
```
Expected: `compile_root_exit=0` (root scripts byte-compile) and the tracked-file count (~245). Note these numbers; they are the before-state.

- [ ] **Step 3: No commit** (read-only task).

---

## Task 1: Scaffold the package skeleton

**Files:**
- Create: `pyproject.toml`
- Create: `src/snowforecast/__init__.py`
- Create: `src/snowforecast/fetchers/__init__.py`
- Create: `src/snowforecast/engines/__init__.py`
- Create: `src/snowforecast/storage/__init__.py`
- Create: `src/snowforecast/analysis/__init__.py`
- Create: `scripts/__init__.py`, `scripts/pipeline/__init__.py`, `scripts/collect/__init__.py`, `scripts/generate/__init__.py`, `scripts/backtest/__init__.py`, `scripts/storage/__init__.py`
- Modify: `.gitignore`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "snowforecast"
version = "0.1.0"
description = "Wisconsin snowfall forecast system"
requires-python = ">=3.10"
dynamic = ["dependencies"]

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.dynamic]
dependencies = { file = ["requirements.txt"] }
```

- [ ] **Step 2: Create the package directories with empty `__init__.py`**

Run:
```bash
mkdir -p src/snowforecast/fetchers src/snowforecast/engines src/snowforecast/storage src/snowforecast/analysis
mkdir -p scripts/pipeline scripts/collect scripts/generate scripts/backtest scripts/storage scripts/shell
for d in src/snowforecast src/snowforecast/fetchers src/snowforecast/engines src/snowforecast/storage src/snowforecast/analysis scripts scripts/pipeline scripts/collect scripts/generate scripts/backtest scripts/storage; do
  : > "$d/__init__.py"
done
echo '"""snowforecast — Wisconsin snowfall forecast system."""' > src/snowforecast/__init__.py
```
Note: `scripts/shell` holds `.sh` files only, so it gets **no** `__init__.py`.

- [ ] **Step 3: Append hygiene rules to `.gitignore`**

Add these lines to the end of `.gitignore` (do not remove existing lines):
```gitignore

# --- Reorg: Python & build ---
__pycache__/
*.pyc
*.egg-info/
build/
.venv/
venv/

# --- Reorg: OS noise ---
.DS_Store

# --- Reorg: build artifacts ---
dist/

# --- Reorg: databases (untracked in place) ---
*.db

# --- Reorg: logs ---
logs/
*.log

# --- Reorg: generated pipeline outputs ---
forecast_output/
forecast_reports/
snowfall_graphs/

# --- Reorg: generated result files (root) ---
/backtesting_diagnostic_summary.json
/backtesting_metrics_2000_2025.json
/daily_forecast_history.json
/daily_update_history.json
/demo_correlation_results.json
/enhanced_forecast_history.json
/enhanced_system_metrics.json
/forecast_jan15_20_2026.json
/realtime_jan2026_data.json
/security_report.json
/storage_estimates.json
/backtesting_results_2000_2025.csv
/enhanced_system_backtest_results.csv
```

- [ ] **Step 4: Install the package editable + verify import**

Run:
```bash
pip install -e . && python -c "import snowforecast; print('snowforecast OK', snowforecast.__file__)"
```
Expected: install succeeds; prints `snowforecast OK <path>/src/snowforecast/__init__.py`.

- [ ] **Step 5: Verify the egg-info is ignored, not staged**

Run:
```bash
git status --porcelain | grep -E "egg-info" || echo "egg-info correctly ignored"
```
Expected: `egg-info correctly ignored`.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/ scripts/ .gitignore
git commit -m "chore: scaffold snowforecast package + gitignore hygiene rules"
```

---

## Task 2: Untrack generated/binary files in place

**Files:** removes the following from the index only (they stay on disk): `*.db`, `__pycache__/*.pyc`, all `.DS_Store`, `dist/`, `logs/`, `forecast_output/`, `forecast_reports/`, `snowfall_graphs/`, and the root generated `.json`/`.csv` listed in `.gitignore`.

- [ ] **Step 1: Untrack caches, OS noise, build artifacts**

Run:
```bash
git rm -r --cached --quiet __pycache__ dist logs forecast_output forecast_reports snowfall_graphs
git rm --cached --quiet .DS_Store
git ls-files | grep '\.DS_Store$' | xargs -r git rm --cached --quiet
```
Expected: no errors (some globs may already be covered; `xargs -r` skips if empty).

- [ ] **Step 2: Untrack the 7 databases (kept on disk)**

Run:
```bash
git rm --cached --quiet atmospheric_data.db global_snowfall.db northwoods_full_history.db northwoods_snowfall.db snowfall_7day.db snowfall_demo.db up_michigan_snowfall.db
```

- [ ] **Step 3: Untrack root generated json/csv**

Run:
```bash
git rm --cached --quiet \
  backtesting_diagnostic_summary.json backtesting_metrics_2000_2025.json \
  daily_forecast_history.json daily_update_history.json demo_correlation_results.json \
  enhanced_forecast_history.json enhanced_system_metrics.json forecast_jan15_20_2026.json \
  realtime_jan2026_data.json security_report.json storage_estimates.json \
  backtesting_results_2000_2025.csv enhanced_system_backtest_results.csv
```

- [ ] **Step 4: Verify files still exist on disk but are untracked**

Run:
```bash
test -f northwoods_full_history.db && echo "db still on disk: yes"
git status --porcelain | grep -E "\.db$" || echo "no tracked db remaining"
git ls-files | grep -E "(__pycache__|\.DS_Store|^dist/|^logs/|^forecast_output/|^forecast_reports/|^snowfall_graphs/)" || echo "generated dirs untracked"
```
Expected: `db still on disk: yes`; no tracked `.db` listed by `git ls-files`; `generated dirs untracked`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: untrack databases, caches, logs, and generated outputs (kept on disk)"
```

---

## Task 3: Move library modules into `src/snowforecast` and fix imports

**Files:**
- `git mv` 4 fetchers → `src/snowforecast/fetchers/`
- `git mv` 7 engines → `src/snowforecast/engines/`
- `git mv` 3 storage modules → `src/snowforecast/storage/`
- `git mv` 4 analysis modules → `src/snowforecast/analysis/`
- `git mv` `config.py` → `src/snowforecast/config.py`
- Modify: all `.py` files containing the 19 intra-project imports (27 sites)

- [ ] **Step 1: Move the fetchers**

```bash
git mv openmeteo_weather_fetcher.py noaa_weather_fetcher.py gfs_atmospheric_fetcher.py global_snowfall_fetcher.py src/snowforecast/fetchers/
```

- [ ] **Step 2: Move the engines**

```bash
git mv enhanced_regional_forecast_system.py enhanced_forecast_system.py comprehensive_forecast_system.py integrated_forecast_system.py pattern_matching_forecast.py major_event_predictor.py weather_orchestrator.py src/snowforecast/engines/
```

- [ ] **Step 3: Move storage + analysis + config**

```bash
git mv snowfall_duckdb.py duckdb_queries.py migrate_data_source.py src/snowforecast/storage/
git mv jetstream_analyzer.py local_event_analyzer.py global_correlation_analysis.py snowfall_analysis.py src/snowforecast/analysis/
git mv config.py src/snowforecast/config.py
```

- [ ] **Step 4: Rewrite all intra-project imports to package paths**

This sed pass maps every moved module to its new dotted path across all `.py` files (covers both `from X import` and `import X`). Run:
```bash
PYFILES=$(git ls-files '*.py')
sed -i -E \
  -e 's/\b(from|import) openmeteo_weather_fetcher\b/\1 snowforecast.fetchers.openmeteo_weather_fetcher/g' \
  -e 's/\b(from|import) noaa_weather_fetcher\b/\1 snowforecast.fetchers.noaa_weather_fetcher/g' \
  -e 's/\b(from|import) gfs_atmospheric_fetcher\b/\1 snowforecast.fetchers.gfs_atmospheric_fetcher/g' \
  -e 's/\b(from|import) global_snowfall_fetcher\b/\1 snowforecast.fetchers.global_snowfall_fetcher/g' \
  -e 's/\b(from|import) enhanced_regional_forecast_system\b/\1 snowforecast.engines.enhanced_regional_forecast_system/g' \
  -e 's/\b(from|import) enhanced_forecast_system\b/\1 snowforecast.engines.enhanced_forecast_system/g' \
  -e 's/\b(from|import) comprehensive_forecast_system\b/\1 snowforecast.engines.comprehensive_forecast_system/g' \
  -e 's/\b(from|import) integrated_forecast_system\b/\1 snowforecast.engines.integrated_forecast_system/g' \
  -e 's/\b(from|import) pattern_matching_forecast\b/\1 snowforecast.engines.pattern_matching_forecast/g' \
  -e 's/\b(from|import) major_event_predictor\b/\1 snowforecast.engines.major_event_predictor/g' \
  -e 's/\b(from|import) weather_orchestrator\b/\1 snowforecast.engines.weather_orchestrator/g' \
  -e 's/\b(from|import) snowfall_duckdb\b/\1 snowforecast.storage.snowfall_duckdb/g' \
  -e 's/\b(from|import) duckdb_queries\b/\1 snowforecast.storage.duckdb_queries/g' \
  -e 's/\b(from|import) migrate_data_source\b/\1 snowforecast.storage.migrate_data_source/g' \
  -e 's/\b(from|import) jetstream_analyzer\b/\1 snowforecast.analysis.jetstream_analyzer/g' \
  -e 's/\b(from|import) local_event_analyzer\b/\1 snowforecast.analysis.local_event_analyzer/g' \
  -e 's/\b(from|import) global_correlation_analysis\b/\1 snowforecast.analysis.global_correlation_analysis/g' \
  -e 's/\b(from|import) snowfall_analysis\b/\1 snowforecast.analysis.snowfall_analysis/g' \
  $PYFILES
```
Note: `enhanced_regional_forecast_system` is rewritten before `enhanced_forecast_system` would mis-match because the two strings are distinct; sed applies all `-e` to each line, and the regex `\benhanced_forecast_system\b` does not match inside `enhanced_regional_forecast_system`. `config` is intentionally NOT sed-rewritten (it has 0 import sites; the bare word `config` is too common to safely replace).

- [ ] **Step 5: Verify no stale bare imports remain**

Run:
```bash
git ls-files '*.py' | xargs grep -nE "^(from|import) (openmeteo_weather_fetcher|noaa_weather_fetcher|gfs_atmospheric_fetcher|global_snowfall_fetcher|enhanced_regional_forecast_system|enhanced_forecast_system|comprehensive_forecast_system|integrated_forecast_system|pattern_matching_forecast|major_event_predictor|weather_orchestrator|snowfall_duckdb|duckdb_queries|migrate_data_source|jetstream_analyzer|local_event_analyzer|global_correlation_analysis|snowfall_analysis) " || echo "no stale bare imports"
```
Expected: `no stale bare imports`.

- [ ] **Step 6: Smoke-test the package imports**

Run:
```bash
python -c "from snowforecast.fetchers.openmeteo_weather_fetcher import OpenMeteoWeatherFetcher; print('fetcher OK')"
python -c "from snowforecast.storage.snowfall_duckdb import SnowfallDuckDB; print('storage OK')"
python -c "from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem; print('engine OK')"
python -c "from snowforecast.engines.enhanced_forecast_system import EnhancedForecastSystem; print('engine2 OK')"
```
Expected: `fetcher OK`, `storage OK`, `engine OK`, `engine2 OK`.
If an import fails on a *data file path* (e.g. opens a `.db` at import time), that is a pre-existing behavior — note it but it does not block; the modules still import for class definitions. If it fails on a missing module name, re-check Step 4.

- [ ] **Step 7: Byte-compile the package**

Run:
```bash
python -c "import compileall,sys; sys.exit(0 if compileall.compile_dir('src', quiet=1) else 1)"; echo "exit=$?"
```
Expected: `exit=0`.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: move shared modules into snowforecast package, update imports"
```

---

## Task 4: Move runnable scripts and add `runpy` root shims

**Files:**
- `git mv` pipeline/collect/generate/backtest/storage scripts into `scripts/**`
- `git mv` `.sh` scripts into `scripts/shell/`
- Create: 4 `.py` root shims + 1 `.sh` root shim
- Modify: `scripts/shell/push_forecast.sh` (copy target), `.github/workflows/forecast-pages-rebuild.yml`

- [ ] **Step 1: Move pipeline scripts**

```bash
git mv update_recent_data.py update_global_predictors.py collect_world_data.py daily_automated_forecast.py automated_forecast_runner.py daily_forecast_runner.py daily_snow_update.py scripts/pipeline/
```

- [ ] **Step 2: Move collectors / data-QA scripts**

```bash
git mv collect_7days_openmeteo.py collect_hourly_data.py collect_noaa_data.py collect_northwoods_full_history.py collect_northwoods_wi.py collect_radiosonde.py collect_regional_stations.py collect_resort_reports.py collect_snotel_data.py collect_up_michigan.py quick_collect_7days.py snowfall_collector.py fetch_atmospheric_data.py fetch_noaa_validation.py historical_backfill.py passive_backfill.py expand_global_network.py add_lake_superior_stations.py research_noaa_stations.py research_snotel_stations.py check_data_completeness.py check_noaa_coverage.py check_stations.py check_historical_event.py investigate_data_units.py ski_resort_validation_data.py noaa_forecast_integration_2025.py scripts/collect/
```

- [ ] **Step 3: Move generate + backtest + storage-util scripts**

```bash
git mv generate_daily_report.py generate_forecast.py generate_station_forecasts.py forecast_14day.py query_current_snow.py scripts/generate/
git mv backtesting_diagnostic_analysis.py comprehensive_backtesting_report.py enhanced_system_backtesting.py FORECAST_ERROR_ANALYSIS.py validate_noaa_data.py verify_nws_forecast.py correct_winter_season_validation.py scripts/backtest/
git mv storage_calculator.py storage_optimization_guide.py upload_to_kv.py scripts/storage/
git mv setup_guide.py scripts/
```

- [ ] **Step 4: Move shell scripts**

```bash
git mv push_forecast.sh daily_update.sh early_morning_collection.sh run_daily_forecast.sh setup_automation.sh setup_daily_automation.sh setup_venv.sh show_forecast.sh snow-update.sh sync_to_nas.sh scripts/shell/
```

- [ ] **Step 5: Re-run the import sed on the moved scripts**

The Task 3 sed already ran across all tracked `.py`, but these files were at root then and are still tracked, so they were covered. Confirm no stale imports survived the moves:
```bash
git ls-files 'scripts/*.py' | xargs grep -lnE "^(from|import) (openmeteo_weather_fetcher|snowfall_duckdb|noaa_weather_fetcher|enhanced_regional_forecast_system|enhanced_forecast_system|comprehensive_forecast_system|gfs_atmospheric_fetcher) " || echo "scripts imports clean"
```
Expected: `scripts imports clean`.

- [ ] **Step 6: Create the 4 Python root shims**

Create `daily_automated_forecast.py` at repo root:
```python
"""Root shim — keeps the NAS crontab working after reorg.
Real script: scripts/pipeline/daily_automated_forecast.py
"""
import runpy
runpy.run_module("scripts.pipeline.daily_automated_forecast", run_name="__main__", alter_sys=True)
```

Create `update_recent_data.py` at repo root:
```python
"""Root shim — see scripts/pipeline/update_recent_data.py"""
import runpy
runpy.run_module("scripts.pipeline.update_recent_data", run_name="__main__", alter_sys=True)
```

Create `update_global_predictors.py` at repo root:
```python
"""Root shim — see scripts/pipeline/update_global_predictors.py"""
import runpy
runpy.run_module("scripts.pipeline.update_global_predictors", run_name="__main__", alter_sys=True)
```

Create `collect_world_data.py` at repo root:
```python
"""Root shim — see scripts/pipeline/collect_world_data.py (argparse preserved via runpy)"""
import runpy
runpy.run_module("scripts.pipeline.collect_world_data", run_name="__main__", alter_sys=True)
```

- [ ] **Step 7: Create the `push_forecast.sh` root shim**

Create `push_forecast.sh` at repo root:
```bash
#!/usr/bin/env bash
# Root shim — keeps the NAS crontab working after reorg.
# Real script: scripts/shell/push_forecast.sh
exec "$(dirname "$0")/scripts/shell/push_forecast.sh" "$@"
```
Then mark executable:
```bash
chmod +x push_forecast.sh
```

- [ ] **Step 8: Update the real `push_forecast.sh` copy target (`public/` → `web/public/`)**

Inspect the moved script and update any `public/` path. Run to find them:
```bash
grep -nE "public/" scripts/shell/push_forecast.sh
```
For each match, change the destination `public/` to `web/public/` (e.g. `cp forecast_output/latest_forecast.json public/` → `cp forecast_output/latest_forecast.json web/public/`). Edit each occurrence with the Edit tool. Do NOT change references that are inside a `git add`/commit message describing the repo — only the actual copy destinations. Re-run the grep and confirm the destinations now read `web/public/`.

- [ ] **Step 9: Update the GitHub Actions workflow paths**

Run to see current references:
```bash
grep -nE "public|\.py|\.sh" .github/workflows/forecast-pages-rebuild.yml
```
Update any path that points at a moved file: root `public/` → `web/public/`; bare script names that moved (e.g. `python daily_automated_forecast.py`) can stay (the root shims cover them) — prefer leaving cron-equivalent invocations on the shims for consistency. Only edit paths that would otherwise 404 (e.g. an output-dir reference to `public/`). Use the Edit tool per line.

- [ ] **Step 10: Verify a shim resolves the module (dry import, no side effects)**

Run:
```bash
python -c "import importlib.util as u; print('module found' if u.find_spec('scripts.pipeline.daily_automated_forecast') else 'MISSING')"
```
Expected: `module found`.

- [ ] **Step 11: Verify shell shim points to a real file**

Run:
```bash
test -x scripts/shell/push_forecast.sh || chmod +x scripts/shell/push_forecast.sh
test -f scripts/shell/push_forecast.sh && echo "real push_forecast.sh present"
```
Expected: `real push_forecast.sh present`.

- [ ] **Step 12: Commit**

```bash
git add -A
git commit -m "refactor: move runnable scripts into scripts/, add runpy root shims for cron"
```

---

## Task 5: Move web assets, one-off analyses, and docs

**Files:**
- `git mv` `public/ templates/ static/ worker/` → `web/`
- `git mv` web apps → `web/apps/`
- `git mv` one-off analyses → `analysis/seasonal/`, visualizations → `analysis/visualize/`
- `git mv` docs → `docs/guides/` and `docs/reports/`
- `git mv` tests → `tests/`

- [ ] **Step 1: Move the website**

```bash
mkdir -p web/apps
git mv public web/public
git mv templates web/templates
git mv static web/static
git mv worker web/worker
git mv weather_app.py forecast_web_dashboard.py forecast_verification_dashboard.py build_static.py web/apps/
```

- [ ] **Step 2: Move one-off / dated analyses and visualizations**

```bash
mkdir -p analysis/seasonal analysis/visualize
git mv analyze_2012_2013_winter.py analyze_7day_data.py analyze_85year_trends.py analyze_northwoods.py analyze_polar_vortex.py compare_2013_2017_winters.py january_15_20_2026_UPDATED_forecast.py january_15_20_SNOW_REASSESSMENT_jan5.py january_2025_snowstorm_forecast.py january_2026_FINAL_canadian_analysis.py january_2026_GLOBAL_teleconnections.py january_2026_REVISED_forecast.py january_2026_snowstorm_forecast.py predict_2024_2025_winter.py predict_best_snow_week_2025.py predict_up_michigan_2025.py fetch_jan_15_20_forecast.py fetch_jan_2026_realtime.py retroactive_forecast_demo.py demo_global_analysis.py weekend_forecast_check.py run_global_analysis.py analysis/seasonal/
git mv visualize_demo_results.py visualize_enso_snowfall.py visualize_global_snowfall.py visualize_snowfall_data.py analysis/visualize/
```

- [ ] **Step 3: Move tests**

```bash
mkdir -p tests
git mv test_duckdb_setup.py test_multiple_events.py test_weather_app.py full_data_test.py debug_forecast.py tests/
```

- [ ] **Step 4: Move docs (guides + reports)**

```bash
mkdir -p docs/guides docs/reports
git mv AUTOMATED_FORECAST_GUIDE.md AUTOMATION_SETUP.md DAILY_UPDATES_GUIDE.md DASHBOARD_AND_AUTOMATION_README.md DEPLOYMENT_GUIDE.md DUCKDB_GUIDE.md GLOBAL_SNOWFALL_README.md OPERATIONAL_FORECAST_GUIDE.md QUICK_START_V3.md SNOWFALL_GUIDE.md STORAGE_QUICK_REFERENCE.md SYSTEM_ENHANCEMENTS_README.md WEATHER_README.md GLOBAL_QUICK_START.txt QUICK_REFERENCE.txt docs/guides/
git mv CODE_REVIEW_SUMMARY.md DATA_SUMMARY.md ENHANCED_NETWORK_ANALYSIS.md ENHANCEMENT_SUMMARY.md EXECUTIVE_BACKTESTING_SUMMARY.md FINAL_SYSTEM_PERFORMANCE_REPORT.md PROJECT_COMPLETE.md REGIONAL_DATA_SUCCESS.md SCIENTIFIC_ANALYSIS_REPORT.md SYSTEM_CAPABILITIES_AND_IMPROVEMENTS.md SYSTEM_VALIDATION_JAN_10_EVENT.md BACKTESTING_SUMMARY_2000_2025.txt DATA_COLLECTION_COMPLETE.txt EXPANSION_SUCCESS_SUMMARY.txt FINAL_RESULTS_SUMMARY.txt STORAGE_ESTIMATES.txt docs/reports/
```
Note: `README.md`, `agents.md`, `gemini.md`, `requirements.txt`, `weather_requirements.txt` stay at root.

- [ ] **Step 5: Re-run import sed across web/apps + analysis + tests (they moved; confirm clean)**

```bash
git ls-files 'web/apps/*.py' 'analysis/**/*.py' 'tests/*.py' | xargs grep -lnE "^(from|import) (openmeteo_weather_fetcher|snowfall_duckdb|noaa_weather_fetcher|enhanced_regional_forecast_system|enhanced_forecast_system|comprehensive_forecast_system|gfs_atmospheric_fetcher) " || echo "moved files imports clean"
```
Expected: `moved files imports clean` (Task 3's sed already rewrote them while tracked).

- [ ] **Step 6: Verify root is clean of loose library/script `.py`**

Run:
```bash
git ls-files | grep -E '^[^/]+\.py$' | sort
```
Expected: ONLY the 4 root shims (`collect_world_data.py`, `daily_automated_forecast.py`, `update_global_predictors.py`, `update_recent_data.py`) plus `agents.py`. Anything else means a file was missed — move it to its correct folder per the spec §4 mapping.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: move web/, analysis/, tests/, and docs/ into dedicated folders"
```

---

## Task 6: Consolidate requirements, verify end-to-end, document

**Files:**
- Modify/Create: `requirements.txt` (consolidated), remove `weather_requirements.txt`
- Create: `MIGRATION.md`
- Modify: `README.md`

- [ ] **Step 1: Diff the two requirements files**

Run:
```bash
echo "=== only in requirements.txt ==="; comm -23 <(sort -u requirements.txt) <(sort -u weather_requirements.txt)
echo "=== only in weather_requirements.txt ==="; comm -13 <(sort -u requirements.txt) <(sort -u weather_requirements.txt)
```
Note the lines that appear only in `weather_requirements.txt`.

- [ ] **Step 2: Merge into a single `requirements.txt`**

Run (produces a union, de-duplicated, sorted, ignoring blank/comment-only diffs — then review):
```bash
cat requirements.txt weather_requirements.txt | grep -vE '^\s*(#|$)' | sort -u > requirements.merged.txt
mv requirements.merged.txt requirements.txt
git rm weather_requirements.txt
```
Open `requirements.txt` and confirm it reads sensibly (no duplicate packages with conflicting versions; if a package appears with two versions, keep the higher and note it).

- [ ] **Step 3: Reinstall to confirm the merged requirements + package resolve**

Run:
```bash
pip install -e . && python -c "import snowforecast; print('reinstall OK')"
```
Expected: `reinstall OK`.

- [ ] **Step 4: End-to-end smoke of the canonical engine via its shim path**

Run (import-only, no network/cron needed):
```bash
python -c "from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem; print('canonical engine import OK')"
python -c "import importlib.util as u; print('all shims resolve' if all(u.find_spec(m) for m in ['scripts.pipeline.daily_automated_forecast','scripts.pipeline.update_recent_data','scripts.pipeline.update_global_predictors','scripts.pipeline.collect_world_data']) else 'SHIM MISSING')"
```
Expected: `canonical engine import OK` and `all shims resolve`.

- [ ] **Step 5: Run the test suite (record pass/fail honestly)**

Run:
```bash
python -m pytest tests/ -q 2>&1 | tail -20 || echo "pytest exit non-zero"
```
Record the result. Pre-existing failures (e.g. tests that need a live DB or network) are acceptable IF they fail the same way they did before the reorg — note them in the commit message. New import errors are NOT acceptable and must be fixed (re-check Task 3/4 sed coverage for the failing module).

- [ ] **Step 6: Create `MIGRATION.md`**

```markdown
# Migration Notes — Project Reorganization (2026-06-06)

The repo was reorganized into a `snowforecast` package. The live pipeline keeps
working via root shims, but two **external** systems need a one-time update.

## 1. NAS venv — install the package (REQUIRED)

After pulling the reorganized tree on the NAS:

```bash
cd /path/to/weather
source venv/bin/activate     # or: ./venv/bin/pip ...
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
(paths are hardcoded relative to root). Nothing was deleted.

## 4. Optional later cleanup — drop the root shims

To point cron directly at the new paths and remove the shims:

```bash
# in the NAS crontab, replace e.g.
#   python daily_automated_forecast.py
# with
#   python -m scripts.pipeline.daily_automated_forecast
# and for the shell shim:
#   bash scripts/shell/push_forecast.sh
```
Then delete the 4 `.py` shims and `push_forecast.sh` shim from the repo root.

## New layout

See `docs/superpowers/specs/2026-06-06-weather-project-reorganization-design.md`.
```

- [ ] **Step 7: Update `README.md` for the new layout**

Update the README so paths reflect the move. Specifically:
- "Cloudflare serves the `public/` directory" → `web/public/`.
- Key Scripts table: `enhanced_regional_forecast_system.py` →
  `src/snowforecast/engines/enhanced_regional_forecast_system.py`;
  pipeline scripts now under `scripts/pipeline/` (note cron still calls the root
  shims).
- Local Preview: `python forecast_web_dashboard.py` →
  `python web/apps/forecast_web_dashboard.py`.
- Add a one-line "Project layout" section linking to `MIGRATION.md` and the spec.
- Add the new install step: `pip install -e .` after `pip install -r requirements.txt`.

Make these edits with the Edit tool, matching the existing README wording.

- [ ] **Step 8: Final structure verification**

Run:
```bash
echo "=== root .py (should be 4 shims + agents.py) ==="; git ls-files | grep -E '^[^/]+\.py$'
echo "=== tracked db (should be none) ==="; git ls-files | grep -E '\.db$' || echo none
echo "=== package importable ==="; python -c "import snowforecast; print('ok')"
echo "=== git history preserved (sample) ==="; git log --oneline --follow -1 -- src/snowforecast/engines/enhanced_regional_forecast_system.py
```
Expected: only shims + `agents.py` at root; no tracked `.db`; `ok`; a commit line proving `--follow` history works.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "chore: consolidate requirements, add MIGRATION.md, update README for new layout"
```

---

## Task 7: Resolve the flagged ambiguous files (spec §10)

**Files:** `agents.py`, `run_global_analysis.py` (already placed in Task 5), `snowfall_collector.py` (already placed in Task 4)

- [ ] **Step 1: Inspect `agents.py`**

Run:
```bash
head -40 agents.py
git ls-files '*.py' | xargs grep -lnE "import agents\b|from agents\b" || echo "agents.py not imported anywhere"
```
Decision: if it is an AI-assistant/orchestration helper not imported by the pipeline, leave it at root next to `agents.md`/`gemini.md`. If it IS imported by package code, `git mv agents.py src/snowforecast/` and re-run the Task 3 sed for `agents`. Record the decision in the commit message.

- [ ] **Step 2: Confirm `run_global_analysis.py` and `snowfall_collector.py` placements**

Run:
```bash
grep -lnE "from snowforecast|import snowforecast" analysis/seasonal/run_global_analysis.py scripts/collect/snowfall_collector.py
git ls-files '*.py' | xargs grep -lnE "import snowfall_collector\b|from snowfall_collector\b" || echo "snowfall_collector not imported as a library"
```
Decision: `run_global_analysis.py` stays in `analysis/seasonal/` unless it is cron-referenced (then move to `scripts/generate/` + add a shim). `snowfall_collector.py` stays in `scripts/collect/` if not imported as a library; if it IS imported elsewhere, `git mv` it to `src/snowforecast/fetchers/` and re-run the Task 3 sed for `snowfall_collector`. Record decisions.

- [ ] **Step 3: Commit (only if any file moved)**

```bash
git add -A
git commit -m "chore: finalize placement of agents.py / run_global_analysis.py / snowfall_collector.py"
```
If nothing moved, skip the commit.

---

## Done criteria (from spec §11)

- `pip install -e .` succeeds; `python -c "import snowforecast"` works.
- `from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem` resolves.
- All 4 `runpy` shims resolve their target modules.
- `git status` is clean after a forecast run (generated files gitignored).
- `tests/` pass to the same extent as before the reorg (pre-existing failures noted).
- Root contains only config, README, `MIGRATION.md`, AI-config files, and shims — no loose library/script `.py`.
- `git log --follow` shows history for moved files.
- `MIGRATION.md` documents the NAS `pip install -e .` and Cloudflare output-dir change.
