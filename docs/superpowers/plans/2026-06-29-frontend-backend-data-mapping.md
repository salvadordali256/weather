# Frontend / Backend / Data-Retrieval Mapping + Targeted Cleanup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the three areas (Frontend / Backend / Data Retrieval) discoverable via documentation, remove the genuine root clutter, and end the two-`analysis` naming collision — without breaking any external integration point.

**Architecture:** Keep the existing Python layout (installable `src/snowforecast/` library + runnable `scripts/` + `web/` site) and document how it maps to the three domain areas. Three verified-safe `git mv` moves (`agents.py`→`tools/`, top-level `analysis/`→`explorations/`, `MIGRATION.md`→`docs/`) plus authoritative mapping docs in `README.md` and a synced `CLAUDE.md`.

**Tech Stack:** Python 3 (setuptools editable install), pytest, git. Docs in Markdown.

**Spec:** `docs/superpowers/specs/2026-06-29-frontend-backend-data-mapping-design.md`

## Global Constraints

- **DO NOT commit or push.** Leave every change in the working tree for the user to review. (User override of the skill's default "commit each task" step — there are no `git commit` steps in this plan.)
- **Use `git mv` for every file move** so history is preserved (`git log --follow` must still work).
- **Do NOT touch any of these** (fixed external integration points): the 10 root cron shims (`daily_automated_forecast.py`, `update_recent_data.py`, `update_global_predictors.py`, `collect_world_data.py`, `collect_hourly_data.py`, `collect_noaa_data.py`, `collect_radiosonde.py`, `collect_resort_reports.py`, `generate_daily_report.py`, `generate_station_forecasts.py`, `passive_backfill.py`, `push_forecast.sh`); `web/public/`; `weather_data/`; any `*.db` / `forecast_output/` root-relative path; and root config/build/dependency files (`pyproject.toml`, `requirements.txt`, `Dockerfile`, `docker-compose.yml`, `.dockerignore`, `.env.example`, `.gitignore`, `.graphifyignore`, `agents.md`, `gemini.md`).
- **`src/snowforecast/analysis/` STAYS** — only the **top-level** `analysis/` directory is renamed to `explorations/`.
- All commands assume CWD = repo root (`c:\Users\cwirk\projects\weather`). Use the Bash tool (git-bash).

## File Structure

| Path | Responsibility | Change |
|---|---|---|
| `tools/agents.py` | Standalone AST security scanner (dev tooling, not runtime) | Moved from root `agents.py` |
| `explorations/` (was `analysis/`) | One-off dated studies (`seasonal/`, `visualize/`) | Renamed from top-level `analysis/` |
| `docs/MIGRATION.md` | Post-reorg migration notes (reference material) | Moved from root `MIGRATION.md` |
| `README.md` | Developer entry point; **authoritative** three-area mapping | Edited (layout section) |
| `CLAUDE.md` | Claude/human guidance; layout synced to README | Edited (layout + MIGRATION refs) |

---

### Task 1: Capture pre-change baseline

**Files:** none (read-only baseline).

**Interfaces:**
- Produces: a saved baseline of test results + import health, used by Task 6 to prove nothing regressed.

- [ ] **Step 1: Confirm the editable install is active**

Run:
```bash
python -c "import snowforecast, sys; print('snowforecast OK', snowforecast.__file__)"
```
Expected: prints `snowforecast OK <path>`. If it errors with `ModuleNotFoundError`, run `pip install -e .` first, then re-run.

- [ ] **Step 2: Record the canonical engine import**

Run:
```bash
python -c "from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem; print('engine import OK')"
```
Expected: prints `engine import OK`.

- [ ] **Step 3: Capture the pytest baseline**

Run (the trailing `| tee` saves it for comparison; `; true` keeps going even if tests fail):
```bash
python -m pytest tests/ -q 2>&1 | tee "$TMPDIR/pytest_baseline.txt"; true
```
If `$TMPDIR` is unset, save to `./.pytest_baseline.txt` instead (it is gitignored as a dotfile? — if not, delete it in Task 6). Note the summary line (e.g. `9 passed, 2 failed, 1 skipped`). **This is the baseline; Task 6 must match it (no NEW failures).**

- [ ] **Step 4: Record the current root `.py` inventory**

Run:
```bash
git ls-files | grep -E '^[^/]+\.py$' | sort
```
Expected: 11 files — the 10 cron shims **plus `agents.py`**. After this plan, `agents.py` must be gone from this list (Task 2).

---

### Task 2: Move `agents.py` → `tools/agents.py`

**Files:**
- Move: `agents.py` → `tools/agents.py`
- Modify: `tools/agents.py` (usage strings in its docstring/`__main__`)

**Interfaces:**
- Consumes: nothing (verified: `agents.py` is imported nowhere).
- Produces: `tools/` directory containing the security scanner; root no longer has a non-shim `.py`.

- [ ] **Step 1: Re-verify it is not imported (safety gate)**

Run:
```bash
git grep -nE "import agents\b|from agents\b" -- '*.py' || echo "NOT IMPORTED — safe to move"
```
Expected: `NOT IMPORTED — safe to move`. If anything prints, STOP and reassess.

- [ ] **Step 2: Move the file with history preserved**

Run:
```bash
mkdir -p tools
git mv agents.py tools/agents.py
```
Expected: no output; `git status` shows `renamed: agents.py -> tools/agents.py`.

- [ ] **Step 3: Update the usage examples inside the file**

The docstring and `__main__` block invoke the script as `python agents.py ...`. Update the **invocation prefix only** (not the scan-target arguments) so the printed help matches the new path. In `tools/agents.py`, replace each occurrence of the command prefix:

- `python agents.py /path/to/source` → `python tools/agents.py /path/to/source`
- `python agents.py .` → `python tools/agents.py .`
- `python agents.py noaa_weather_fetcher.py` → `python tools/agents.py noaa_weather_fetcher.py`
- `python agents.py . --json` → `python tools/agents.py . --json`
- the `print("Usage: python agents.py /path/to/source [--json]")` line → `print("Usage: python tools/agents.py /path/to/source [--json]")`

Use a single replace-all on the literal `python agents.py` → `python tools/agents.py` within `tools/agents.py` (it only appears in these usage strings).

- [ ] **Step 4: Verify it still compiles and runs**

Run:
```bash
python -m py_compile tools/agents.py && echo "compile OK"
python tools/agents.py tools/agents.py --json >/dev/null && echo "run OK"
```
Expected: `compile OK` then `run OK` (the scanner scans itself; exit 0).

- [ ] **Step 5: Verify no stale references to root `agents.py`**

Run:
```bash
git grep -n "\bagents\.py\b" -- '*.py' '*.sh' '*.yml' '*.yaml' '*.toml' | grep -v "tools/agents.py" | grep -v "^docs/" || echo "no stale code/config refs"
```
Expected: `no stale code/config refs`. (Matches under `docs/reports/` and `docs/superpowers/` describe past state and are intentionally left as-is.)

- [ ] **Step 6: Checkpoint (DO NOT commit)**

Run `git status`. Confirm `renamed: agents.py -> tools/agents.py` and the working tree is uncommitted. Do **not** run `git commit`.

---

### Task 3: Rename top-level `analysis/` → `explorations/`

**Files:**
- Move: `analysis/` → `explorations/` (25 files under `seasonal/` and `visualize/`)

**Interfaces:**
- Consumes: nothing (verified: top-level `analysis/` is never imported and not referenced by any script/shell/CI).
- Produces: `explorations/` directory; ends the naming collision with `src/snowforecast/analysis/` (which is untouched).

- [ ] **Step 1: Re-verify no code/shell/CI references the top-level dir (safety gate)**

Run:
```bash
git grep -nE "analysis/(seasonal|visualize)" -- '*.py' '*.sh' '*.yml' '*.yaml' || echo "no code refs — safe to rename"
```
Expected: `no code refs — safe to rename`.

- [ ] **Step 2: Rename the directory with history preserved**

Run:
```bash
git mv analysis explorations
```
Expected: no output; `git status` shows the 25 files as `renamed: analysis/... -> explorations/...`.

- [ ] **Step 3: Confirm `src/snowforecast/analysis/` is untouched**

Run:
```bash
test -d src/snowforecast/analysis && echo "library analysis intact"
test ! -d analysis && echo "top-level analysis renamed"
```
Expected: both lines print.

- [ ] **Step 4: Byte-compile the renamed tree**

Run:
```bash
python -m compileall -q explorations && echo "explorations compile OK"
```
Expected: `explorations compile OK` (no syntax errors introduced — these are standalone scripts; a non-zero exit only if a file fails to parse, which the rename cannot cause, so this is a sanity check).

- [ ] **Step 5: Checkpoint (DO NOT commit)**

Run `git status`. Confirm the renames and an uncommitted tree. Do **not** commit. (Doc references to `analysis/` in `README.md`/`CLAUDE.md` are updated in Task 5.)

---

### Task 4: Move `MIGRATION.md` → `docs/MIGRATION.md`

**Files:**
- Move: `MIGRATION.md` → `docs/MIGRATION.md`

**Interfaces:**
- Consumes: nothing (verified: `MIGRATION.md` contains no markdown relative links that break; its path references are repo-root-relative prose in code spans).
- Produces: `docs/MIGRATION.md`. External referrers in `README.md`/`CLAUDE.md` are repointed in Task 5.

- [ ] **Step 1: Move the file with history preserved**

Run:
```bash
git mv MIGRATION.md docs/MIGRATION.md
```
Expected: `git status` shows `renamed: MIGRATION.md -> docs/MIGRATION.md`.

- [ ] **Step 2: Confirm no markdown relative links inside need fixing**

Run:
```bash
grep -nE "\]\(([^h)][^)]*)\)" docs/MIGRATION.md || echo "no relative markdown links — nothing to fix"
```
Expected: `no relative markdown links — nothing to fix`. (Its `docs/superpowers/...` references are backtick code spans describing repo-root-relative paths and remain valid.)

- [ ] **Step 3: Checkpoint (DO NOT commit)**

Run `git status`; confirm the rename and uncommitted tree. Do **not** commit.

---

### Task 5: Documentation — README mapping, CLAUDE.md sync, MIGRATION links

**Files:**
- Modify: `README.md` (the `## Project layout` section, lines ~90–117)
- Modify: `CLAUDE.md` (the `## Layout` section, lines ~14–33; plus MIGRATION refs at lines ~54 and ~76)

**Interfaces:**
- Consumes: the moves from Tasks 2–4 (so the docs describe the new tree).
- Produces: the **authoritative** three-area mapping (in README) and a CLAUDE.md that matches the new structure.

- [ ] **Step 1: Replace the README `## Project layout` section**

In `README.md`, replace the entire block from the `## Project layout` heading through the final `See [MIGRATION.md](MIGRATION.md) ...` line with:

````markdown
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
├── docs/                      # Guides, reports, specs, plans, MIGRATION.md
├── daily_automated_forecast.py     # Root shim → scripts/pipeline/
├── update_recent_data.py           # Root shim → scripts/pipeline/
├── update_global_predictors.py     # Root shim → scripts/pipeline/
├── collect_world_data.py           # Root shim → scripts/pipeline/
└── push_forecast.sh                # Root shim → scripts/shell/push_forecast.sh
```

Root shims exist so the NAS cron commands require no changes after the reorg.
See [docs/MIGRATION.md](docs/MIGRATION.md) for one-time update steps required on the NAS and Cloudflare Pages.
````

- [ ] **Step 2: Replace the CLAUDE.md `## Layout` section**

In `CLAUDE.md`, replace the block from `## Layout (post-reorg — see MIGRATION.md)` through the closing ``` fence of the layout tree with:

````markdown
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
<root>                   Config, README, and cron shims
```
````

- [ ] **Step 3: Repoint the remaining `MIGRATION.md` references in CLAUDE.md**

Two prose references remain. Make these exact replacements in `CLAUDE.md`:

- `delete them** unless you also update the crontab (see MIGRATION.md §5).`
  → `delete them** unless you also update the crontab (see docs/MIGRATION.md §5).`
- `See \`MIGRATION.md\` for the full details.`
  → `See \`docs/MIGRATION.md\` for the full details.`

- [ ] **Step 4: Verify no stale doc references remain**

Run:
```bash
git grep -nE "\]\(MIGRATION\.md\)|\bMIGRATION\.md §|See \`MIGRATION\.md\`" -- README.md CLAUDE.md || echo "README/CLAUDE MIGRATION links repointed"
git grep -nE "^├── analysis/|\banalysis/  +#|One-off seasonal" -- README.md || echo "README analysis ref updated"
git grep -n "analysis/                One-off" -- CLAUDE.md || echo "CLAUDE analysis ref updated"
```
Expected: all three echo lines print (no stale matches).

- [ ] **Step 5: Confirm the new structure is documented**

Run:
```bash
git grep -n "explorations/" -- README.md CLAUDE.md
git grep -n "tools/" -- README.md CLAUDE.md
git grep -nE "Frontend|Data Retrieval" -- README.md CLAUDE.md
```
Expected: each prints at least one match in both files.

- [ ] **Step 6: Checkpoint (DO NOT commit)**

Run `git status`. Confirm `README.md` and `CLAUDE.md` are modified and the tree is uncommitted. Do **not** commit.

---

### Task 6: Final end-to-end verification

**Files:** none (verification only).

**Interfaces:**
- Consumes: the completed state of Tasks 1–5.
- Produces: evidence that functionality is preserved and the working tree is clean-but-uncommitted.

- [ ] **Step 1: Editable install still resolves**

Run:
```bash
pip install -e . >/dev/null 2>&1 && echo "install OK"
python -c "import snowforecast; print('import OK')"
python -c "from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem; print('engine OK')"
```
Expected: `install OK`, `import OK`, `engine OK`.

- [ ] **Step 2: Cron shims still resolve to their targets**

Run (imports the shim target without executing the pipeline):
```bash
python -c "import importlib; importlib.import_module('scripts.pipeline.daily_automated_forecast'); print('shim target OK')"
```
Expected: `shim target OK`. (Confirms the moves did not disturb the `scripts/` package the root shims forward to.)

- [ ] **Step 3: Re-run tests and compare to the Task 1 baseline**

Run:
```bash
python -m pytest tests/ -q 2>&1 | tail -5; true
```
Expected: the **same** pass/skip/fail summary recorded in Task 1, Step 3. **No NEW failures.** If a new failure appears, STOP and investigate before declaring done.

- [ ] **Step 4: Root directory is decluttered**

Run:
```bash
git ls-files | grep -E '^[^/]+\.py$' | sort
test ! -e agents.py && echo "agents.py gone from root"
test ! -e MIGRATION.md && echo "MIGRATION.md gone from root"
```
Expected: the `.py` list shows **only the 10 cron shims** (no `agents.py`); both `echo` lines print.

- [ ] **Step 5: No stale references to old locations anywhere in code/config**

Run:
```bash
git grep -nE "\bagents\.py\b" -- '*.py' '*.sh' '*.yml' '*.yaml' '*.toml' | grep -v "tools/agents.py" | grep -v "^docs/" || echo "agents.py refs clean"
git grep -nE "analysis/(seasonal|visualize)" -- '*.py' '*.sh' '*.yml' '*.yaml' || echo "explorations refs clean"
```
Expected: `agents.py refs clean` and `explorations refs clean`.

- [ ] **Step 6: History preserved + tree uncommitted**

Run:
```bash
git log --follow --oneline -1 -- tools/agents.py && echo "history follows agents.py"
git status --short | grep -E "^R" | head && echo "renames staged"
git log --oneline -1
```
Expected: `--follow` shows pre-move history; `git status` shows `R` (rename) entries; the latest commit is the **pre-existing** branch tip (i.e. **we made no commits**). If `git log` shows a new commit authored this session, that violates the no-commit constraint.

- [ ] **Step 7: Clean up the baseline temp file (if created at repo root)**

Run:
```bash
test -f ./.pytest_baseline.txt && rm -f ./.pytest_baseline.txt && echo "removed stray baseline file" || echo "no stray file"
```
Expected: one of the two messages; ensures Task 1's fallback file isn't left behind.

---

## Self-Review

**Spec coverage:**
- §3 three-area mapping → Task 5 (README table + CLAUDE.md). ✓
- §4.1 move `agents.py`→`tools/` → Task 2. ✓
- §4.2 rename `analysis/`→`explorations/` → Task 3. ✓
- §4.3 move `MIGRATION.md`→`docs/` → Task 4. ✓
- §4.4 docs in README (authoritative) + CLAUDE sync, no separate ARCHITECTURE.md → Task 5. ✓
- §4.5 fixed integration points → Global Constraints + Task 6 Steps 2,4. ✓
- §6 verification plan (install/import/compile/pytest/shims/grep/git-status) → Tasks 1 & 6. ✓
- Workflow override (no commit) → Global Constraints + every Checkpoint step + Task 6 Step 6. ✓

**Placeholder scan:** No TBD/TODO; every edit shows exact target strings and replacement text; every command shows expected output. ✓

**Type/name consistency:** Directory names used consistently (`tools/`, `explorations/`, `docs/MIGRATION.md`); README and CLAUDE.md edits use the same area names (Frontend / Backend / Data Retrieval) and the same tree contents. ✓
