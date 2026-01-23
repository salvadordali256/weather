#!/bin/bash
#
# Setup Daily Forecast Automation
# Configures cron job to run forecast daily at 6 AM
#

echo "================================================================================"
echo "WISCONSIN SNOWFALL FORECAST - DAILY AUTOMATION SETUP"
echo "================================================================================"
echo ""

# Get the current directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FORECAST_SCRIPT="$SCRIPT_DIR/daily_automated_forecast.py"
PYTHON_PATH=$(which python3)

echo "Configuration:"
echo "  Script directory: $SCRIPT_DIR"
echo "  Python path: $PYTHON_PATH"
echo "  Forecast script: $FORECAST_SCRIPT"
echo ""

# Check if forecast script exists
if [ ! -f "$FORECAST_SCRIPT" ]; then
    echo "❌ Error: Forecast script not found at $FORECAST_SCRIPT"
    exit 1
fi

echo "✅ Forecast script found"
echo ""

# Create cron job entry
CRON_JOB="0 6 * * * cd $SCRIPT_DIR && $PYTHON_PATH $FORECAST_SCRIPT >> $SCRIPT_DIR/forecast_cron.log 2>&1"

echo "Cron job to be added:"
echo "  Schedule: Daily at 6:00 AM"
echo "  Command: $CRON_JOB"
echo ""

# Check if cron job already exists
crontab -l 2>/dev/null | grep -q "daily_automated_forecast.py"

if [ $? -eq 0 ]; then
    echo "⚠️  A cron job for this script already exists."
    echo ""
    read -p "Do you want to remove the existing job and add a new one? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Remove existing job
        crontab -l 2>/dev/null | grep -v "daily_automated_forecast.py" | crontab -
        echo "✅ Removed existing cron job"
    else
        echo "❌ Setup cancelled"
        exit 0
    fi
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

if [ $? -eq 0 ]; then
    echo "✅ Cron job added successfully!"
    echo ""
    echo "The forecast will now run automatically every day at 6:00 AM"
    echo ""
    echo "To view current cron jobs:"
    echo "  crontab -l"
    echo ""
    echo "To view the forecast log:"
    echo "  tail -f $SCRIPT_DIR/forecast_cron.log"
    echo ""
    echo "To manually run the forecast now:"
    echo "  python3 $FORECAST_SCRIPT"
    echo ""
    echo "To remove the cron job:"
    echo "  crontab -e"
    echo "  (then delete the line containing 'daily_automated_forecast.py')"
    echo ""
else
    echo "❌ Error: Failed to add cron job"
    exit 1
fi

echo "================================================================================"
echo "AUTOMATION SETUP COMPLETE"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  1. Run a manual forecast to test: python3 daily_automated_forecast.py"
echo "  2. Start the web dashboard: python3 forecast_web_dashboard.py"
echo "  3. View dashboard at: http://localhost:5000"
echo ""
echo "================================================================================"
