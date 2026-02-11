#!/bin/bash
# Push latest forecast to GitHub for Cloudflare Pages

cd "$(dirname "${BASH_SOURCE[0]}")"

# Source .env for FORECAST_OUTPUT_DIR
if [ -f .env ]; then
    export $(grep -v '^#' .env | grep FORECAST_OUTPUT_DIR | xargs)
fi
FORECAST_DIR="${FORECAST_OUTPUT_DIR:-forecast_output}"

# Copy forecast to public folder
if [ -f "$FORECAST_DIR/latest_forecast.json" ]; then
    cp "$FORECAST_DIR/latest_forecast.json" public/
fi

# Git operations
git add public/latest_forecast.json
# Only track forecast_output if it's inside the repo (relative path)
case "$FORECAST_DIR" in
    /*) ;; # absolute path, skip git add
    *)  git add "$FORECAST_DIR/latest_forecast.json" 2>/dev/null ;;
esac
git diff --cached --quiet && { echo "No changes to push"; exit 0; }

git commit -m "Update forecast $(date '+%Y-%m-%d %H:%M')"
git push origin master

echo "Forecast pushed to GitHub"
