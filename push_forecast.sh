#!/bin/bash
# Push latest forecast to GitHub for Cloudflare Pages.
#
# Lives at the repo root so the NAS crontab / daily_update.sh can call it by
# name. Refactored after the reorg: the deployed site folder is web/public/
# (Cloudflare Pages build output dir), not public/.

# Run from the repo root (this script's directory).
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

# Source .env for FORECAST_OUTPUT_DIR
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

FORECAST_DIR="${FORECAST_OUTPUT_DIR:-forecast_output}"
PUBLIC_DIR="web/public"   # Cloudflare Pages build output dir (was: public/)

# Reconcile with the remote BEFORE publishing so a diverged local branch
# (a rejected push, or a commit made elsewhere) can't silently wedge the
# pipeline into repeatedly reporting "No changes to push" (audit SEV-008).
# The three published JSON files are fully regenerated each run, so discarding
# any un-pushed local commits here is safe.
if ! git fetch --quiet origin master; then
    echo "❌ git fetch failed — aborting push." >&2
    exit 1
fi
git reset --hard origin/master

# Copy the published forecast JSON into the deployed site folder.
for f in latest_forecast.json station_data.json daily_report.json; do
    if [ -f "$FORECAST_DIR/$f" ]; then
        cp "$FORECAST_DIR/$f" "$PUBLIC_DIR/"
    fi
done

# Safety guard: never push a broken/empty site to production. Validate the
# deployed folder (pages present, JSON parses, data non-empty) and abort the
# push if it fails, so a bad run can't ruin the live site.
if [ -f scripts/ci/validate_public.py ]; then
    if ! python3 scripts/ci/validate_public.py; then
        echo "❌ web/public failed validation — refusing to push." >&2
        exit 1
    fi
fi

# Git operations
git add "$PUBLIC_DIR/latest_forecast.json" "$PUBLIC_DIR/station_data.json" "$PUBLIC_DIR/daily_report.json" 2>/dev/null
# Pipeline health files written by check_pipeline_health.sh (if it ran)
git add PIPELINE_STATUS.md pipeline_status.json 2>/dev/null
# Only track forecast_output if it's inside the repo (relative path)
case "$FORECAST_DIR" in
    /*) ;; # absolute path, skip git add
    *)  git add "$FORECAST_DIR/latest_forecast.json" "$FORECAST_DIR/station_data.json" "$FORECAST_DIR/daily_report.json" 2>/dev/null ;;
esac

git diff --cached --quiet && { echo "No changes to push"; exit 0; }

git commit -m "Update forecast $(date '+%Y-%m-%d %H:%M')"

# Push, and if the remote advanced between our fetch and our push (a race with
# another run or a manual push), reconcile once and retry rather than leaving an
# un-pushed local commit behind.
if ! git push origin master; then
    echo "⚠️  push rejected — reconciling with origin/master and retrying once." >&2
    if ! { git fetch --quiet origin master && git rebase origin/master; }; then
        echo "❌ reconcile failed — aborting." >&2
        exit 1
    fi
    git push origin master || { echo "❌ Git push failed after retry"; exit 1; }
fi

echo "Forecast pushed to GitHub"
