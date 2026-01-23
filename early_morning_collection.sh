#!/bin/bash
# Early Morning Data Collection
# Runs at 4:30 AM when API quota is fresh
# Completes Lake Superior stations and atmospheric data

cd /Users/kyle.jurgens/weather

# Activate virtual environment
source venv/bin/activate

# Log file
LOG_FILE="logs/early_morning_$(date +%Y%m%d).log"
mkdir -p logs

echo "========================================" >> "$LOG_FILE"
echo "Early Morning Collection Started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Step 1: Complete Lake Superior snow belt stations
echo "" >> "$LOG_FILE"
echo "Adding Lake Superior snow belt stations..." >> "$LOG_FILE"
python add_lake_superior_stations.py << EOF >> "$LOG_FILE" 2>&1
y
EOF
LAKE_STATUS=$?

# Wait a bit between collections
sleep 10

# Step 2: Resume atmospheric data collection (with very slow delays to avoid limits)
echo "" >> "$LOG_FILE"
echo "Resuming atmospheric data collection..." >> "$LOG_FILE"
python fetch_atmospheric_data.py << EOF >> "$LOG_FILE" 2>&1
y
EOF
ATMOS_STATUS=$?

# Summary
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "Early Morning Collection Completed: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "Lake Superior Stations: $([ $LAKE_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "Atmospheric Data: $([ $ATMOS_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Exit with error if any step failed
if [ $LAKE_STATUS -ne 0 ] || [ $ATMOS_STATUS -ne 0 ]; then
    exit 1
fi

exit 0
