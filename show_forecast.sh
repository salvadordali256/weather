#!/bin/bash
#
# Quick Forecast Viewer
# Shows today's forecast in a clean format
#

cd "$(dirname "$0")"

TODAY=$(date +%Y-%m-%d)
FORECAST_FILE="forecast_reports/forecast_${TODAY}.txt"

if [ -f "$FORECAST_FILE" ]; then
    echo ""
    cat "$FORECAST_FILE"
    echo ""
else
    echo ""
    echo "No forecast available for today (${TODAY})"
    echo ""
    echo "Run: python3 daily_forecast_runner.py"
    echo "Or:  ./run_daily_forecast.sh"
    echo ""
fi
