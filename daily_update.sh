#!/bin/bash
# Daily Automated Weather Data Update & Forecast Generation
# Runs at 17:30 daily via cron

cd /Users/kyle.jurgens/weather

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

# Step 2: Update global predictor stations
echo "" >> "$LOG_FILE"
echo "Updating global predictor stations..." >> "$LOG_FILE"
python update_global_predictors.py >> "$LOG_FILE" 2>&1
GLOBAL_STATUS=$?

# Step 3: Generate new forecast
echo "" >> "$LOG_FILE"
echo "Generating forecast..." >> "$LOG_FILE"
python daily_automated_forecast.py >> "$LOG_FILE" 2>&1
FORECAST_STATUS=$?

# Summary
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "Daily Update Completed: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "Regional Update: $([ $REGIONAL_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Global Update: $([ $GLOBAL_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Forecast Generation: $([ $FORECAST_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Exit with error if any step failed
if [ $REGIONAL_STATUS -ne 0 ] || [ $GLOBAL_STATUS -ne 0 ] || [ $FORECAST_STATUS -ne 0 ]; then
    exit 1
fi

exit 0
