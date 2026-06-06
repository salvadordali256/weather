#!/bin/bash
#
# Daily Snow Forecast Automation Script
# Run this script once per day (recommended: 7 AM local time)
#
# Setup instructions:
# 1. Make executable: chmod +x run_daily_forecast.sh
# 2. Test run: ./run_daily_forecast.sh
# 3. Automate with cron: crontab -e
#    Add line: 0 7 * * * /Users/kyle.jurgens/weather/run_daily_forecast.sh
#

# Change to weather directory
cd "$(dirname "$0")"

# Log file
LOGFILE="forecast_logs/forecast_$(date +%Y%m%d).log"
mkdir -p forecast_logs

echo "========================================"
echo "Daily Forecast - $(date)"
echo "========================================"

# Run the daily forecast
python3 daily_forecast_runner.py 2>&1 | tee "$LOGFILE"

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Forecast completed successfully"

    # Optional: Send notification (uncomment if you want macOS notifications)
    # osascript -e 'display notification "Daily snow forecast updated" with title "Weather Forecast"'

else
    echo ""
    echo "❌ Forecast failed - check log: $LOGFILE"

    # Optional: Send error notification
    # osascript -e 'display notification "Forecast generation failed" with title "Weather Forecast Error"'
fi

echo "========================================"
