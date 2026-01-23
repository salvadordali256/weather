#!/bin/bash
#
# Automated Forecast Setup Script
# Sets up daily automated forecasts with alerting
#
# This script will:
# 1. Make all necessary scripts executable
# 2. Test the forecast system
# 3. Generate a crontab entry for daily execution
# 4. Optionally install the cron job
#

WEATHER_DIR="/Users/kyle.jurgens/weather"
cd "$WEATHER_DIR"

echo "========================================"
echo "AUTOMATED FORECAST SETUP"
echo "========================================"
echo ""

# Step 1: Make scripts executable
echo "Step 1: Making scripts executable..."
chmod +x run_daily_forecast.sh
chmod +x snow-update.sh
chmod +x setup_automation.sh
echo "✅ Scripts are now executable"
echo ""

# Step 2: Test the forecast system
echo "Step 2: Testing enhanced forecast system..."
echo "This will take about 30-60 seconds..."
echo ""

python3 automated_forecast_runner.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Forecast system test passed!"
else
    echo ""
    echo "❌ Forecast system test failed"
    echo "Please check the error messages above"
    exit 1
fi

echo ""
echo "========================================"
echo "CRON SETUP"
echo "========================================"
echo ""

# Generate cron entries
CRON_ENTRY_7AM="0 7 * * * cd $WEATHER_DIR && python3 automated_forecast_runner.py >> forecast_logs/auto_forecast_\$(date +\%Y\%m\%d).log 2>&1"
CRON_ENTRY_5AM="0 5 * * * cd $WEATHER_DIR && python3 daily_snow_update.py >> forecast_logs/data_update_\$(date +\%Y\%m\%d).log 2>&1"

echo "Recommended cron schedule:"
echo ""
echo "# Daily data collection at 5 AM"
echo "$CRON_ENTRY_5AM"
echo ""
echo "# Daily forecast with alerting at 7 AM"
echo "$CRON_ENTRY_7AM"
echo ""

# Ask if user wants to install
echo "Would you like to install these cron jobs now? (y/n)"
read -r INSTALL_CRON

if [ "$INSTALL_CRON" = "y" ] || [ "$INSTALL_CRON" = "Y" ]; then
    # Create forecast_logs directory
    mkdir -p forecast_logs

    # Get existing crontab
    crontab -l > /tmp/current_cron 2>/dev/null || touch /tmp/current_cron

    # Check if already exists
    if grep -q "automated_forecast_runner.py" /tmp/current_cron; then
        echo ""
        echo "⚠️  Forecast automation already exists in crontab"
        echo "Skipping installation to avoid duplicates"
    else
        echo ""
        echo "# Weather forecast automation" >> /tmp/current_cron
        echo "$CRON_ENTRY_5AM" >> /tmp/current_cron
        echo "$CRON_ENTRY_7AM" >> /tmp/current_cron

        # Install new crontab
        crontab /tmp/current_cron

        echo "✅ Cron jobs installed!"
        echo ""
        echo "Your forecasts will now run:"
        echo "  • 5:00 AM - Data collection"
        echo "  • 7:00 AM - Forecast generation + alerts"
        echo ""
        echo "Logs will be saved to: $WEATHER_DIR/forecast_logs/"
    fi

    rm /tmp/current_cron
else
    echo ""
    echo "To install manually, run:"
    echo "  crontab -e"
    echo ""
    echo "Then add these lines:"
    echo "$CRON_ENTRY_5AM"
    echo "$CRON_ENTRY_7AM"
fi

echo ""
echo "========================================"
echo "SETUP COMPLETE"
echo "========================================"
echo ""
echo "Your enhanced forecast system is ready!"
echo ""
echo "Features enabled:"
echo "  ✅ Real-time atmospheric pattern detection"
echo "  ✅ Alberta Clipper early warning (24-48h lead time)"
echo "  ✅ Lake effect snow detection"
echo "  ✅ Smart alerting system"
echo "  ✅ macOS notifications"
echo ""
echo "Manual commands:"
echo "  Run forecast now:  python3 automated_forecast_runner.py"
echo "  View cron jobs:    crontab -l"
echo "  Edit cron jobs:    crontab -e"
echo "  View logs:         ls -lh forecast_logs/"
echo ""
echo "Alert configuration:"
echo "  Edit: automated_forecast_runner.py"
echo "  Class: AlertConfig (top of file)"
echo "  Options: Email, thresholds, notification settings"
echo ""
