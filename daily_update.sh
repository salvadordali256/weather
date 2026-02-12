#!/bin/bash
# Daily Automated Weather Data Update & Forecast Generation
# Runs at 17:30 daily via cron

# Change to script directory (works on Mac or Pi)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Activate virtual environment
source venv/bin/activate

# Log file with timestamp
LOG_FILE="logs/daily_update_$(date +%Y%m%d).log"
mkdir -p logs

echo "========================================" >> "$LOG_FILE"
echo "Daily Update Started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Step 1: Update regional station data
echo "" >> "$LOG_FILE"
echo "Updating regional stations..." >> "$LOG_FILE"
python update_recent_data.py >> "$LOG_FILE" 2>&1
REGIONAL_STATUS=$?

# Step 2: Update global predictor stations (quick update - 5 key stations)
echo "" >> "$LOG_FILE"
echo "Updating global predictor stations..." >> "$LOG_FILE"
python update_global_predictors.py >> "$LOG_FILE" 2>&1
GLOBAL_STATUS=$?

# Step 3: Collect world data (all 60+ stations)
echo "" >> "$LOG_FILE"
echo "Collecting world data (all stations)..." >> "$LOG_FILE"
python collect_world_data.py --days 7 >> "$LOG_FILE" 2>&1
WORLD_STATUS=$?

# Step 4: Generate new forecast
echo "" >> "$LOG_FILE"
echo "Generating forecast..." >> "$LOG_FILE"
python daily_automated_forecast.py >> "$LOG_FILE" 2>&1
FORECAST_STATUS=$?

# Step 4.5: Generate station forecasts for trip planner
echo "" >> "$LOG_FILE"
echo "Generating station forecasts for trip planner..." >> "$LOG_FILE"
python generate_station_forecasts.py >> "$LOG_FILE" 2>&1
STATION_STATUS=$?

# Step 5: Sync to NAS (if available)
echo "" >> "$LOG_FILE"
echo "Syncing to NAS..." >> "$LOG_FILE"
if [ -f sync_to_nas.sh ]; then
    bash sync_to_nas.sh >> "$LOG_FILE" 2>&1
    NAS_STATUS=$?
else
    echo "sync_to_nas.sh not found, skipping NAS sync" >> "$LOG_FILE"
    NAS_STATUS=0
fi

# Step 6: Push forecast to GitHub (if configured)
echo "" >> "$LOG_FILE"
echo "Pushing to GitHub..." >> "$LOG_FILE"
if [ -f push_forecast.sh ]; then
    # Copy latest forecast to public folder first
    cp forecast_output/latest_forecast.json public/ 2>/dev/null
    bash push_forecast.sh >> "$LOG_FILE" 2>&1
    GIT_STATUS=$?
else
    echo "push_forecast.sh not found, skipping git push" >> "$LOG_FILE"
    GIT_STATUS=0
fi

# Summary
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "Daily Update Completed: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "Regional Update: $([ $REGIONAL_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Global Update: $([ $GLOBAL_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "World Data: $([ $WORLD_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Forecast Generation: $([ $FORECAST_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Station Forecasts: $([ $STATION_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "NAS Sync: $([ $NAS_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Git Push: $([ $GIT_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Exit with error if critical steps failed
if [ $REGIONAL_STATUS -ne 0 ] || [ $FORECAST_STATUS -ne 0 ]; then
    exit 1
fi

exit 0
