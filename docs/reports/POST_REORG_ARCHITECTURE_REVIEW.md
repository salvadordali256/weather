# Post-Reorg Architecture & Validation Review

**Project:** Wisconsin Snowfall Forecast System
**Branch reviewed:** `task/reorg` (post-restructuring)
**Fixes applied on:** `review/post-reorg-fixes` (commit `850d294`)
**Date:** 2026-06-14
**Reviewer scope:** architecture, data pipeline, integrations, Docker/deploy, tests, forecast meteorology

---

## A. Current-State Assessment

### High-level architecture

The system is a batch, file-based forecasting pipeline — not a live service. There is no request-time backend doing forecasting; everything is precomputed by a nightly job and published as static JSON.

```
                       NAS cron (17:30 daily)
                               │
        ┌──────────────────────┴───────────────────────┐
        │            scripts/shell/daily_update.sh       │
        └──────────────────────┬───────────────────────┘
   ingest (fetchers)           │            generate
        │                      │                │
  Open-Meteo / NOAA /   ─►  SQLite DB  ─►  EnhancedRegionalForecastSystem
  SNOTEL / world data    (demo_global_   (teleconnection/analog engine)
        │                 snowfall.db)           │
        │                      │                 ▼
        │                      │        forecast_output/latest_forecast.json
        │                      │                 │
        │              generate_station_forecasts.py (separate Open-Meteo NWP path)
        │                      │                 │
        │                      ▼                 ▼
        │            station_data.json     daily_report.json
        │                      │                 │
        └──────────────►  push_forecast.sh: cp *.json → web/public/ ; git push master
                                                  │
                                          Cloudflare Pages (serves web/public/)
                                                  │
                                   https://weather.salvadordali256.net
```

Component layers (post-reorg):

- **Library** (`src/snowforecast/`): `config`, `fetchers/`, `engines/`, `storage/`, `analysis/`. Installed via `pip install -e .`. Imports resolve cleanly (verified).
- **Runnable jobs** (`scripts/`): `pipeline/`, `collect/`, `generate/`, `backtest/`, `storage/`, `shell/`.
- **Root shims**: thin `runpy` wrappers so the NAS crontab keeps calling root-level names.
- **Web** (`web/`): two *different* frontends — a tracked static site in `web/public/` (the one actually deployed) and a Flask/Jinja app in `web/apps/` + `web/templates/` (local preview + an orphaned static builder).

### Top risks at a glance

1. **Docker image could not build at all** before this review (stale `COPY` paths). *Fixed.*
2. **`daily_update.sh` orchestrator was broken by the reorg** — wrong working directory and stale script paths; ~5 of 11 steps would fail. *Fixed.*
3. **Two parallel website build systems** that disagree on output location (`dist/` vs `web/public/`). The orphaned one is wired into a GitHub Action that effectively deploys nothing.
4. **The forecast engine cannot actually forecast the near term** — it uses *observed* upstream snowfall as the predictor, but for future target dates those observations don't exist yet, so short-range regional signals are almost always empty.
5. **Silent "no data" forecasts** — when the SQLite DB is missing/empty (it's gitignored and absent from fresh checkouts/CI/containers), every day returns a plausible-looking 10% "MINIMAL" forecast with no error.

### Broken integrations / missing dependencies (summary)

- Docker build context excluded the Python package (`*.py` in `.dockerignore`). *Fixed.*
- `jinja2` is imported by `build_static.py` but not declared in `requirements.txt` (works only transitively via Flask).
- The DB the engine reads (`demo_global_snowfall.db`, SQLite) is decoupled from the DuckDB storage layer in `src/snowforecast/storage/` — two storage stacks, only one used by the live path.
- GitHub Action `forecast-pages-rebuild.yml` builds to `dist/` (gitignored) and commits with `git add -A`; nothing it produces reaches `web/public/`, so it does not actually refresh the deployed site.

---

## B. Findings Report

Severity key: **Critical** (blocks build/deploy or produces wrong public output) · **High** (a documented capability is broken or materially misleading) · **Medium** (reliability/correctness gap) · **Low** (hygiene).

### F1 — Dockerfile referenced pre-reorg paths · Critical · ✅ Fixed
**Description.** The image copied `forecast_web_dashboard.py`, `templates/`, and `forecast_output/` from the repo root and ran `python forecast_web_dashboard.py`. After the reorg the dashboard is at `web/apps/`, templates at `web/templates/`, and `forecast_output/` is gitignored (so the `COPY` would fail on a clean checkout). The image also never ran `pip install -e .`, so `import snowforecast` would fail.
**Root cause.** Dockerfile not updated during the reorg.
**Fix.** Rewrote the Dockerfile to copy `src/` and `web/`, run `pip install -e .`, seed `forecast_output/` from the tracked `web/public/latest_forecast.json`, serve via `gunicorn web/apps/forecast_web_dashboard:app`, and added a `HEALTHCHECK`.
**Effort:** done (~0.5 d to fully validate with a real `docker build`).

### F2 — `.dockerignore` excluded all Python source · Critical · ✅ Fixed
**Description.** `*.py` was ignored except `forecast_web_dashboard.py`, which would strip `src/` and `web/apps/` out of the build context, defeating any corrected Dockerfile.
**Root cause.** Leftover from the single-file dashboard era.
**Fix.** Removed the blanket `*.py` ignore; added `forecast_output/`.
**Effort:** done.

### F3 — `docker-compose.yml` mounted stale paths · High · ✅ Fixed
**Description.** Mounted `./templates` (now `web/templates`) and used `FLASK_ENV=development`; bound to Flask's default dev host.
**Fix.** Mounts `./web/templates` and `./web/static`, binds `0.0.0.0`, keeps `./forecast_output` for live updates.
**Effort:** done.

### F4 — `daily_update.sh` broken by reorg · High · ✅ Fixed
**Description.** The cron orchestrator did `cd "$SCRIPT_DIR"` (i.e. into `scripts/shell/`) and then invoked bare script names (`python collect_snotel_data.py`, `generate_station_forecasts.py`, etc.) and `cp ... public/`. After the reorg those scripts live under `scripts/collect/` and `scripts/generate/`, and the public dir is `web/public/`. Roughly half the pipeline steps (SNOTEL, NOAA, resort reports, station forecasts, daily report, NAS sync) would fail with "file not found"; the forecast itself would also fail because the run started from the wrong directory.
**Root cause.** Script not migrated alongside the tree; relies on the *old* "everything at root" assumption that only the documented root shims preserve.
**Fix.** `cd` to the repo root (`../..`), call moved scripts via explicit `scripts/<area>/` paths, keep root-shimmed scripts as bare names, and drop the dead `public/` copy (push_forecast.sh already copies to `web/public/`). Syntax validated.
**Caveat.** Whether the NAS crontab calls this script or the individual root shims directly is not visible in-repo — see Open Questions. The fix is correct under either reading.
**Effort:** done.

### F5 — Two competing website build paths; CI deploys nothing · High · ⛔ Not fixed (needs a decision)
**Description.** `web/apps/build_static.py` renders `web/templates/*.html` into `dist/`. But the live site is the hand-authored static app in `web/public/` (`index.html` + `forecast.js`, `planner.html` + `planner.js`, `report.html`), and CLAUDE.md states Cloudflare's build output dir is `web/public`. `dist/` is gitignored. The GitHub Action `forecast-pages-rebuild.yml` runs `build_static.py` then `git add -A` — but its output (`dist/`) and the forecast data (`forecast_output/`) are both gitignored, so the Action commits nothing meaningful and never updates the deployed `web/public/` JSON.
**Root cause.** A Flask-era dashboard/static-builder was never retired when the `web/public/` static app + `push_forecast.sh` publish flow became canonical.
**Recommended fix.** Pick one. Either (a) delete `build_static.py` + the Flask dashboard and treat `web/public/` as the single source of truth (simplest, matches reality), or (b) make `build_static.py` output into `web/public/` and have it own the publish, then retire `push_forecast.sh`'s hand-copy. Until then, fix or disable the misleading GitHub Action.
**Effort:** 0.5–1 d.

### F6 — Engine forecasts the future from not-yet-observed data · High · ⛔ Not fixed (design)
**Description.** `EnhancedRegionalForecastSystem` predicts snow at the Wisconsin targets by reading *observed* snowfall at upstream stations (Winnipeg, Thunder Bay, Duluth, …) at lags of 0–2 days, plus global stations (Sapporo, Chamonix, Irkutsk) at 5–7 days. For a genuine future target date, `check_date = target_date − lag`; with regional lags of 0–2 those dates are in the future, so `get_station_snow()` returns `None` and the regional signal is empty. The model therefore mostly runs on the 30%-weighted global signal and degrades to its floor for true forecasting. It works as a *hindcast/analog* tool, not a forward forecast.
**Root cause.** Predictor design conflates "explain past events from observations" with "forecast future events." A real forward forecast would need *forecasted* upstream precip (e.g. from the GFS fetcher that already exists but is unused here).
**Recommended fix.** Drive the regional predictors from NWP forecast fields (GFS/Open-Meteo) for future dates instead of station observations, or clearly relabel the engine's output as analog/nowcast and lean on the Open-Meteo-based `generate_station_forecasts.py` path for the actual 7-day outlook.
**Effort:** 3–5 d.

### F7 — No "no data" guard; missing DB yields fake forecasts · Medium/High · ⛔ Not fixed
**Description.** `generate_ensemble_forecast()` computes `data_quality = 'no_data'` when no station returned anything, but `daily_automated_forecast.py` ignores it and still emits `probability: 10`, `confidence: LOW`. The SQLite DB is gitignored and absent from fresh checkouts, CI, and containers, so the published forecast can silently be "all 10%, all week" with no failure signal.
**Root cause.** Data-availability not treated as a first-class failure.
**Recommended fix.** In the daily runner, if `data_quality == 'no_data'` for all days, exit non-zero and skip publishing (don't overwrite a good `latest_forecast.json` with an empty one). Surface `data_quality` in the JSON and on the page.
**Effort:** 0.5 d.

### F8 — Score normalization inflates confidence on sparse data · Medium · ⛔ Not fixed
**Description.** Both `check_global_predictors` and `check_regional_predictors` normalize by the summed weight of *stations that returned data*, not the total possible weight. If only one low-weight station reports and it's "heavy," the normalized score is 1.0 → 85% probability. Sparse data thus reads as *high* confidence — the opposite of correct behavior.
**Recommended fix.** Normalize by total configured weight (or scale the score by data coverage), and require a minimum number/weight of active stations before allowing high-probability buckets.
**Effort:** 0.5–1 d.

### F9 — Snowboarding "quality" score ignores most quality factors · Medium · ⛔ Not fixed (meteorology)
**Description.** The trip-planner score (`compute_snow_score`, surfaced as "Excellent/Good/Moderate" in `planner.js`) is built only from forecast snowfall + a cold-day precip-probability term + a weekly climatology term. Despite the objective's list, it does **not** use base depth (collected by `collect_resort_reports.py` but unused in scoring), wind, visibility, precipitation type, or freeze/thaw cycles. So a windswept, icy, or rain-contaminated day with some snow can still read "Good." See Section G for the recommended factor model.
**Effort:** 2–4 d.

### F10 — Two storage stacks (SQLite live, DuckDB shelved) · Medium · ⛔ Not fixed
**Description.** The live forecast path uses SQLite (`demo_global_snowfall.db`); `src/snowforecast/storage/` is a full DuckDB layer (`snowfall_duckdb.py`, `duckdb_queries.py`, `migrate_data_source.py`) that the daily pipeline doesn't touch. `duckdb` is also a heavyweight dependency pulled into every install/image.
**Recommended fix.** Decide on one engine. If DuckDB is the future, finish the migration and point the engine at it; if not, move DuckDB deps/code behind an optional extra so the core image stays slim.
**Effort:** 1–3 d.

### F11 — `requirements.txt` mixes core and heavy optionals; `jinja2` undeclared · Low/Medium · ⛔ Not fixed
**Description.** `great-expectations`, `influxdb-client`, `pymongo`, `psycopg2-binary`, `matplotlib`, `plotly` are all hard requirements though marked "optional," bloating the Docker image and CI. `jinja2` (used directly by `build_static.py`) is only present transitively via Flask.
**Recommended fix.** Split into `[project.optional-dependencies]` extras (`storage`, `viz`, `quality`); add `jinja2` explicitly.
**Effort:** 0.5 d.

### F12 — `forecast-pages-rebuild.yml` is a no-op / contradicts NAS flow · Low · ⛔ Not fixed
**Description.** Beyond F5, the workflow regenerates a forecast in CI using `update_recent_data.py` (which needs `NOAA_API_TOKEN`, not provided as a secret here) and commits to the same `master` the NAS pushes to — a potential double-writer. It's currently `workflow_dispatch`-only, limiting blast radius.
**Recommended fix.** Either remove it or repurpose it to *only* rebuild presentation from already-committed data, with explicit secrets and output into `web/public/`.
**Effort:** 0.5 d.

### F13 — Working tree is CRLF; shell scripts choke under non-git tooling · Low · ℹ️ Informational
**Description.** This checkout presents files with CRLF line endings (committed blobs are LF, `core.autocrlf=false`), which makes the whole tree show as "modified" and makes `bash` reject the shell scripts directly (`\r` → "unexpected end of file"). This is a local-checkout artifact, not a production bug — but it's worth adding a `.gitattributes` enforcing `*.sh text eol=lf` so a Windows clone can't accidentally commit CRLF and break the NAS scripts.
**Effort:** 0.25 d.

---

## C. Connection Verification Matrix

| Component | Depends on | Status | Issues found |
|---|---|---|---|
| Root shims (`daily_automated_forecast.py`, …) | `scripts/pipeline/*` via `runpy` | ✅ Working | Verified import/resolve |
| `scripts/pipeline/daily_automated_forecast.py` | `snowforecast` pkg (`pip install -e .`) | ✅ Working | Import verified |
| Forecast engine | SQLite `demo_global_snowfall.db` | ⚠️ Conditional | DB gitignored/absent on fresh env → silent empty forecasts (F7) |
| Engine predictors | Upstream station *observations* | ⛔ Logic gap | Future dates have no obs → near-term signal empty (F6) |
| `src/snowforecast/storage` (DuckDB) | duckdb | 🟡 Orphaned | Not used by live path (F10) |
| `generate_station_forecasts.py` | Open-Meteo API + climatology DB | ✅ Working | Separate NWP path; feeds planner |
| Snowboard "quality" score | snowfall + temp + climatology | ⚠️ Partial | Ignores base depth/wind/vis/precip-type (F9) |
| `daily_update.sh` orchestration | moved `scripts/*` + root shims | ✅ Fixed | Was broken by reorg (F4) |
| `push_forecast.sh` | `forecast_output/*.json` → `web/public/` → `git push master` | ✅ Working | Canonical publish path |
| Cloudflare Pages | `web/public/` build output dir | ✅ Working | Deployed site = `web/public/` |
| `build_static.py` | `web/templates` → `dist/` | 🟡 Orphaned | `dist/` never deployed (F5) |
| GitHub Action (pages rebuild) | `build_static.py`, `git add -A` | ⛔ No-op | Commits nothing to `web/public/` (F5, F12) |
| Flask dashboard (`web/apps`) | `forecast_output/latest_forecast.json` | ✅ Working | Local preview only |
| Docker image | `src/`, `web/`, `pip install -e .` | ✅ Fixed | Was unbuildable (F1, F2, F3) |
| Config / secrets | `.env` (`NOAA_API_TOKEN`, `STORAGE_PATH`) | ✅ Working | `.env.example` present; not needed by engine, needed by collectors |
| Logging / monitoring | per-run log files in `logs/` | ⚠️ Minimal | No alerting on failed steps reaching an operator; statuses only in log |

---

## D. Docker Readiness Assessment

**Build readiness: 3/10 before review → 8/10 after fixes.**
Before: the image could not build (stale `COPY` of moved/gitignored paths) and could not import the package. After the fixes on `review/post-reorg-fixes`, the Dockerfile copies the real source, installs the package, seeds a forecast, and serves via gunicorn. *Not yet verified with an actual `docker build` in this environment* — see required step below.

**Deployment readiness: 5/10.**
The container now models a sensible local/preview deployment, but production publishing does **not** go through Docker at all — it's the NAS cron → `web/public/` → Cloudflare path. The container is a dashboard/preview, not the production server. That's fine, but it should be stated explicitly so no one assumes the image is the production deploy.

**Required before relying on Docker:**
1. Run `docker build .` and `docker compose up` once and confirm `/` and `/api/forecast` return 200 (the one validation this review could not execute here).
2. Add `jinja2` to requirements (F11) if you keep `build_static.py` in-image.
3. Decide whether the image should include the heavy optional deps (F10/F11) — currently it will, making a large image.
4. Add `.gitattributes` (`* text=auto`, `*.sh text eol=lf`) so a CRLF checkout can't poison the image's shell scripts (F13).

---

## E. Recommended Path Forward

**1. Critical (do first — mostly done)**
- ✅ F1–F4 Docker stack + `daily_update.sh` (applied on `review/post-reorg-fixes`). Validate with a real `docker build` and one NAS dry-run of `daily_update.sh`.

**2. Architecture improvements**
- F5 Collapse the two website build paths to one (`web/public/` is the de-facto winner); fix or delete the GitHub Action (F12).
- F10 Choose SQLite *or* DuckDB and retire the other from the hot path.
- F11 Split requirements into core + extras.

**3. Reliability improvements**
- F7 Fail loudly on `no_data`; never publish an empty forecast over a good one.
- F8 Fix score normalization so sparse data ≠ high confidence.
- F13 Add `.gitattributes`; add per-step alerting (email/webhook) on pipeline failure (the alerting code in `automated_forecast_runner.py` exists but is disabled and off the live path).
- Add a smoke test that runs the full pipeline against a tiny fixture DB in CI.

**4. Forecasting improvements**
- F6 Drive near-term predictors from NWP forecast fields (the GFS fetcher already exists) rather than not-yet-existent observations, or relabel the engine honestly and make the Open-Meteo path the headline 7-day forecast.
- F9 Expand the snowboarding-quality score into a real multi-factor model (Section G).

**5. Long-term scalability**
- Replace the bespoke flat-file + `git push` publish with a small object store / KV (a `web/worker/` and `upload_to_kv.py` already hint at this direction) so publishing isn't coupled to git history growth.
- Add forecast-verification/backtesting as a scheduled job feeding a calibration table, so the hard-coded score→probability buckets become data-driven and calibrated.

---

## F. Testing & Verification Plan

Current tests (`tests/`) are mostly ad-hoc scripts (`debug_forecast.py`, `full_data_test.py`, `test_multiple_events.py`) that need a populated local DB and will skip/fail offline; there is no isolated unit coverage of the scoring math and no CI gate.

Recommended coverage:

- **Unit** — `categorize_activity` thresholds; `compute_snow_score` boundaries; the score→probability mapping; normalization with sparse vs full station coverage (locks in F8).
- **Data-pipeline** — run the engine against a committed tiny fixture SQLite DB with known inputs and assert exact probabilities; assert `data_quality == 'no_data'` path exits non-zero (locks in F7).
- **Integration** — `build`/publish dry run that asserts `web/public/latest_forecast.json` is well-formed and schema-stable for `forecast.js`/`planner.js`.
- **API** — hit the Flask routes (`/`, `/api/forecast`, `/api/history`) with and without a forecast present.
- **Container startup** — `docker build` + compose up + curl `/api/forecast` in CI.
- **Forecast accuracy** — wire the existing backtests into a scheduled job that emits hit-rate/Brier-score and feeds calibration.

---

## G. Snowboarding Forecast Quality — Meteorological Review

The current public outputs are (1) a teleconnection snow-probability (engine) and (2) a snowfall-weighted "snow score" (planner). Neither is a snowboarding-conditions model. A defensible rideability score for Northern Wisconsin should blend:

- **New snowfall (last 24–72 h)** — primary driver of fresh/soft conditions; weight highest. *(partially present)*
- **Base depth** — gates whether terrain is rideable at all; already collected from resort reports but unused — fold it in as a hard floor/multiplier. *(missing in score)*
- **Temperature & freeze/thaw history** — Wisconsin's marginal climate makes this decisive: a thaw-then-refreeze produces ice; persistent sub-freezing preserves powder. Track recent max/min and count freeze/thaw crossings. *(missing)*
- **Precipitation type** — rain or mixed contamination ruins a surface even with "snow" in the total; use the cold-day gate more strictly (e.g. require sub-freezing through the event, not just `temp_max ≤ 2 °C`). *(weak)*
- **Wind / gusts** — already fetched in `generate_station_forecasts.py` but not scored; high wind means scoured runs, lift holds, and poor visibility — apply a penalty above ~40 km/h gusts. *(fetched, unused)*
- **Visibility / sky** — penalize whiteout/fog days. *(missing)*

Suggested structure: a 0–100 score = base snowfall-quality term × surface-quality multiplier (freeze/thaw + precip-type) × wind/visibility penalty, with base depth as a gate. Also fix the cold threshold in `compute_snow_score` (currently `temp_max_c <= 2`) — 2 °C max often means wet/melting snow at these latitudes; tighten to ≤ 0 °C for "quality snow" accumulation and reserve the 0–2 °C band for "wet."

Finally, calibrate the engine's score→probability buckets against the backtests rather than the current hand-set step function (10/25/40/55/70/85), so "70%" actually verifies ~70% of the time.

---

## Open Questions (could not be resolved from the repo)

1. **What does the NAS crontab actually invoke** — `daily_update.sh`, or the individual root shims directly? This determines whether F4 was a live outage or latent. (The fix is safe either way.)
2. **Is the Flask dashboard / `build_static.py` still intended to exist**, or is `web/public/` now the sole frontend? Drives F5.
3. **SQLite vs DuckDB** — is the DuckDB layer the planned future, or abandoned? Drives F10.
4. **Should the Docker image be the production server**, or remain a local preview? Drives the Docker deployment scope.

---

### Changes applied in this review (`review/post-reorg-fixes`, commit `850d294`)

`Dockerfile`, `.dockerignore`, `docker-compose.yml`, `scripts/shell/daily_update.sh`, `scripts/pipeline/automated_forecast_runner.py` — see F1–F4. No changes were made to `master` or to the forecast engine's logic.
