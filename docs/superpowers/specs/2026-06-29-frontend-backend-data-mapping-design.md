# Design: Frontend / Backend / Data-Retrieval mapping + targeted cleanup (reorg v2)

**Date:** 2026-06-29
**Branch:** `task/reorg-v2`
**Status:** Approved (Approach B). Awaiting spec review.

> **Workflow override:** The user explicitly instructed **do not commit or push** anything
> on this branch. All changes — including this spec — stay in the working tree for the user
> to review and commit themselves. The brainstorming skill's "commit the design doc" step is
> overridden by this instruction.

## 1. Context

This repository was already reorganized once (the `task/reorg` work, now merged to `master`)
from a flat ~245-file layout into a conventional Python project:

- `src/snowforecast/` — installable library (`fetchers/`, `storage/`, `engines/`, `analysis/`, `config.py`)
- `scripts/` — runnable jobs (`collect/`, `generate/`, `pipeline/`, `backtest/`, `storage/`, `shell/`, `ci/`)
- `web/` — the website (`public/` deployed, `templates/`, `static/`, `apps/`, `worker/`)
- `analysis/`, `tests/`, `docs/`, plus root cron shims

`task/reorg-v2` currently sits at `master` (0 commits ahead). This effort is a **refinement**,
not a re-do.

### Why this is not a literal frontend/backend/data-retrieval folder split

The brief asks for three areas: **Frontend, Backend, Data Retrieval**. That is a *domain* axis.
The repo's physical layout follows the *idiomatic Python* axis: installable library (`src/`)
vs. runnable jobs (`scripts/`) vs. site (`web/`). The two axes cross-cut each other —
data-retrieval code lives in both `src/snowforecast/fetchers/` and `scripts/collect/`; backend
code lives in both `src/snowforecast/engines/` and `scripts/generate/`.

Making the domain axis the top-level folder structure would break:

- the installable package (`import snowforecast`, `pip install -e .`),
- the NAS cron shims at the repo root,
- the Cloudflare Pages build output dir (`web/public`),
- ~32 root-relative DB/output paths.

These are **fixed external integration points** (per the user's "keep fixed / compat-only" choice).
So the design **keeps the convention and documents the domain mapping** rather than fighting it.

## 2. Goals / non-goals

**Goals**
1. Make the three areas (Frontend / Backend / Data Retrieval) discoverable to a new developer
   without source-spelunking — via documentation at the entry point.
2. Remove the genuine root clutter and the one confusing name, with verified-safe moves only.
3. Preserve **all** functionality and every external integration point.

**Non-goals (explicitly deferred — these were Approach C)**
- Splitting/renaming `scripts/storage/` (mixed capacity calculators + KV uploader).
- Pruning deprecated files (`web/apps/build_static.py`, `scripts/setup_guide.py`).
- Reviewing the 7 forecast engines for redundancy.
- Any change to `src/`, `scripts/` package layout, cron shims, `web/public`, `weather_data/`,
  or DB paths.

## 3. The three-area mapping (primary deliverable)

| Area | Lives in | What it is |
|---|---|---|
| **Frontend** | `web/` | `public/` = the deployed static site (Cloudflare Pages); `templates/` + `static/` + `apps/` = local-preview Flask; `worker/` = Cloudflare worker |
| **Backend** (forecast logic) | `src/snowforecast/engines/`, `src/snowforecast/analysis/`, `src/snowforecast/config.py`; `scripts/generate/`, `scripts/pipeline/`, `scripts/backtest/` | Forecast models, orchestration, and the cron pipeline that runs them. (There is no live application server — production is a static site fed by a batch cron pipeline; "backend" = that pipeline + library core.) |
| **Data Retrieval** | `src/snowforecast/fetchers/`, `src/snowforecast/storage/`; `scripts/collect/` | Weather-API clients, DB access/queries, and the data-collection jobs |
| *Shared / global* | repo root, `docs/`, `tests/` | config, dependencies, Docker, the cron shims, docs, tests |

**The `src/` vs `scripts/` distinction inside Backend and Data Retrieval is intentional and will
be documented:** `src/snowforecast/` is the reusable, importable, unit-testable core; `scripts/`
are thin runnable entry points (and the cron pipeline) that import that core.

## 4. Targeted refinements (Approach B)

All moves use `git mv` to preserve history. Each was reference-checked.

### 4.1 Move `agents.py` → `tools/agents.py`
- **Why:** It is a standalone AST security scanner, **not imported anywhere** (verified) and not
  part of the runtime/pipeline. It is the only non-shim `.py` cluttering the root.
- **Action:** create `tools/`, `git mv agents.py tools/agents.py`.
- **Reference updates:** update the usage examples in its own docstring (`python agents.py` →
  `python tools/agents.py`). Historical reports under `docs/reports/` and `docs/superpowers/`
  describe past state and are **left as-is**.
- **Risk:** none (no importers; no code path).

### 4.2 Rename top-level `analysis/` → `explorations/`
- **Why:** It collides conceptually with `src/snowforecast/analysis/`. The top-level dir holds
  **dated one-off studies** (`seasonal/`, `visualize/`); the `src/` one holds **reusable library
  analysis**. Renaming ends the "two analysis dirs" ambiguity.
- **Action:** `git mv analysis explorations` (25 files; `seasonal/` and `visualize/` subdirs move with it).
- **Reference updates:** none in code/shell/CI (verified — referenced only in prose). Update the
  `CLAUDE.md` Layout section and `README.md`.
- **Risk:** none (never imported).

### 4.3 Relocate `MIGRATION.md` → `docs/MIGRATION.md`
- **Why:** The migration it documents is already merged; it is reference material now, and `docs/`
  is the home for guides. Reduces root clutter.
- **Action:** `git mv MIGRATION.md docs/MIGRATION.md`.
- **Reference updates:** `README.md` and `CLAUDE.md` both link to `MIGRATION.md` → repoint to
  `docs/MIGRATION.md`.
- **Risk:** none.

### 4.4 Documentation
- **`README.md`** (developer entry point) gets a concise **"Repository layout — the three areas"**
  section containing the §3 table and the `src/`-vs-`scripts/` note. This is the *authoritative*
  copy of the mapping.
- **`CLAUDE.md`** Layout section is updated to: name the three areas, reflect the `explorations/`
  rename, the `tools/` dir, and the `MIGRATION.md` move, and cross-reference the README mapping.
- **No separate `ARCHITECTURE.md`** — keeping the mapping in one authoritative place (README)
  avoids a third copy to keep in sync.

### 4.5 What stays fixed (do not touch)
- All 10 root cron shims (`daily_automated_forecast.py`, `collect_*.py`, `generate_*.py`,
  `update_*.py`, `passive_backfill.py`, `collect_world_data.py`, `push_forecast.sh`).
- `web/public/` and the Cloudflare build output dir.
- `weather_data/` (root-relative default `STORAGE_PATH` in `config.py`).
- All `*.db` / `forecast_output/` root-relative paths.
- Root config/build/dependency files: `pyproject.toml`, `requirements.txt`, `Dockerfile`,
  `docker-compose.yml`, `.dockerignore`, `.env.example`, `.gitignore`, `.graphifyignore`,
  `agents.md`, `gemini.md`, `README.md`, `CLAUDE.md`.

## 5. Resulting root directory

After the change, the repo root contains only: the 10 cron shims, the config/build/dependency
files listed in §4.5, `README.md`, `CLAUDE.md`, and the directories
(`src/`, `scripts/`, `web/`, `tools/`, `explorations/`, `docs/`, `tests/`, `weather_data/`,
`graphify-out/`, `.claude/`, `.github/`). `agents.py` and `MIGRATION.md` are gone from root.

## 6. Verification plan

Run after the changes (report actual output, no success claims without evidence):

1. `pip install -e .` succeeds.
2. `python -c "import snowforecast"` and
   `python -c "from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem"`.
3. Byte-compile moved/edited files: `python -m py_compile tools/agents.py` and a compile sweep of
   `explorations/`.
4. `python -m pytest tests/` — expect the same pass/skip profile as before (some tests need a
   local DB and skip/fail offline; compare against the pre-change baseline, don't introduce new failures).
5. Smoke a couple of root shims resolve (e.g. `python daily_automated_forecast.py --help`-style
   import resolution) — confirm the moves didn't disturb them.
6. Grep that no code/shell/CI/config references the **old** locations:
   `agents.py` at root, top-level `analysis/`, root `MIGRATION.md`.
7. Confirm `git status` shows renames (history preserved) and the working tree is **left uncommitted**.

## 7. Out of scope / open follow-ups

- Approach C items (§2 non-goals) can be a later pass if desired.
- `scripts/storage/` naming collision with `src/snowforecast/storage/` is noted but deliberately
  left untouched this round.
