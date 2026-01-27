#!/bin/bash
# Sync database and forecast files to NAS
# Run after data collection to backup to NAS storage

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# NAS mount point (configured on Pi)
NAS_MOUNT="/mnt/nas"
NAS_WEATHER_DIR="$NAS_MOUNT/weather"

# Local files
LOCAL_DB="demo_global_snowfall.db"
LOCAL_FORECAST_DIR="forecast_output"
LOCAL_PUBLIC_DIR="public"

# Log file
LOG_FILE="logs/nas_sync_$(date +%Y%m%d).log"
mkdir -p logs

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "NAS SYNC STARTED"
log "=========================================="

# Check if NAS is mounted
if [ ! -d "$NAS_MOUNT" ]; then
    log "ERROR: NAS mount point $NAS_MOUNT does not exist"
    exit 1
fi

if ! mountpoint -q "$NAS_MOUNT" 2>/dev/null; then
    log "WARNING: $NAS_MOUNT may not be a mount point (continuing anyway)"
fi

# Create weather directory on NAS if needed
if [ ! -d "$NAS_WEATHER_DIR" ]; then
    log "Creating $NAS_WEATHER_DIR"
    mkdir -p "$NAS_WEATHER_DIR"
    mkdir -p "$NAS_WEATHER_DIR/forecast"
    mkdir -p "$NAS_WEATHER_DIR/backup"
fi

# Sync database to NAS
if [ -f "$LOCAL_DB" ]; then
    log "Syncing database to NAS..."

    # Create timestamped backup first
    BACKUP_NAME="demo_global_snowfall_$(date +%Y%m%d_%H%M%S).db"

    # Copy main database
    cp "$LOCAL_DB" "$NAS_WEATHER_DIR/demo_global_snowfall.db"
    if [ $? -eq 0 ]; then
        log "✅ Database synced to $NAS_WEATHER_DIR/demo_global_snowfall.db"

        # Keep daily backup (overwrite same-day backup)
        cp "$LOCAL_DB" "$NAS_WEATHER_DIR/backup/demo_global_snowfall_$(date +%Y%m%d).db"
        log "✅ Daily backup created"
    else
        log "❌ Failed to sync database"
    fi
else
    log "WARNING: Local database $LOCAL_DB not found"
fi

# Sync forecast files to NAS
if [ -d "$LOCAL_FORECAST_DIR" ]; then
    log "Syncing forecast files to NAS..."

    # Copy latest forecast
    if [ -f "$LOCAL_FORECAST_DIR/latest_forecast.json" ]; then
        cp "$LOCAL_FORECAST_DIR/latest_forecast.json" "$NAS_WEATHER_DIR/forecast/"
        log "✅ Latest forecast synced"
    fi

    # Copy recent forecast files (last 7 days worth)
    find "$LOCAL_FORECAST_DIR" -name "forecast_*.json" -mtime -7 -exec cp {} "$NAS_WEATHER_DIR/forecast/" \;
    log "✅ Recent forecasts synced"
else
    log "WARNING: Forecast directory $LOCAL_FORECAST_DIR not found"
fi

# Sync public files (for web serving)
if [ -d "$LOCAL_PUBLIC_DIR" ]; then
    log "Syncing public files to NAS..."
    cp -r "$LOCAL_PUBLIC_DIR"/* "$NAS_WEATHER_DIR/forecast/" 2>/dev/null
    log "✅ Public files synced"
fi

# Clean old backups (keep last 30 days)
log "Cleaning old backups..."
find "$NAS_WEATHER_DIR/backup" -name "*.db" -mtime +30 -delete 2>/dev/null
log "✅ Old backups cleaned"

# Show NAS disk usage
log ""
log "NAS Weather Directory:"
du -sh "$NAS_WEATHER_DIR" 2>/dev/null | while read size dir; do
    log "  Total size: $size"
done
du -sh "$NAS_WEATHER_DIR"/* 2>/dev/null | while read size dir; do
    log "  $(basename $dir): $size"
done

log ""
log "=========================================="
log "NAS SYNC COMPLETED"
log "=========================================="
