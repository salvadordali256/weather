#!/bin/bash
# Push latest forecast to GitHub for Cloudflare Pages

cd "$(dirname "${BASH_SOURCE[0]}")"

# Copy forecast to public folder (from local forecast_output)
if [ -f forecast_output/latest_forecast.json ]; then
    cp forecast_output/latest_forecast.json public/
fi

# Git operations
git add public/latest_forecast.json forecast_output/latest_forecast.json
git diff --cached --quiet && { echo "No changes to push"; exit 0; }

git commit -m "Update forecast $(date '+%Y-%m-%d %H:%M')"
git push origin master

echo "Forecast pushed to GitHub"
