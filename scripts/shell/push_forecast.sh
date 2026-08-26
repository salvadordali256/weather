#!/bin/bash
# Push latest forecast to GitHub for Cloudflare Pages.
#
# Lives at scripts/shell/. The deployed site folder is web/public/
# (Cloudflare Pages build output dir), not public/.

# Run from the repo root (two levels up from this script).
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1

# Source .env for FORECAST_OUTPUT_DIR
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

FORECAST_DIR="${FORECAST_OUTPUT_DIR:-forecast_output}"
PUBLIC_DIR="web/public"   # Cloudflare Pages build output dir (was: public/)

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
git push origin master || { echo "❌ Git push failed"; exit 1; }

echo "Forecast pushed to GitHub"
