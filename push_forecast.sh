#!/bin/bash
# Push latest forecast to GitHub for Cloudflare Pages

cd "$(dirname "${BASH_SOURCE[0]}")"

# Copy forecast to public folder
cp /mnt/nas/forecast/latest_forecast.json public/

# Git operations
git add public/latest_forecast.json
git commit -m "Update forecast $(date '+%Y-%m-%d %H:%M')"
git push origin master

echo "Forecast pushed to GitHub"
