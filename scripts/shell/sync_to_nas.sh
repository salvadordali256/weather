#!/bin/bash
# Sync database and forecast files to the NAS (production storage).
#
# SAFETY: the NAS copy of demo_global_snowfall.db is the master. This script
# refuses to overwrite it with a local DB that is missing, invalid, empty, or
# drastically smaller than the NAS copy, and it always backs up the existing
# NAS DB before overwriting. Override the guards with FORCE_DB_SYNC=1 only if
# you are certain the local DB is correct.

# Run from the repo root (this script lives at scripts/shell/, two levels down).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Load .env if present (e.g. for DB_PATH overrides)
if [ -f .env ]; then set -a; source .env; set +a; fi

# NAS mount point
NAS_MOUNT="/mnt/nas"
NAS_WEATHER_DIR="$NAS_MOUNT/weather"

# Local files
LOCAL_DB="${DB_PATH:-demo_global_snowfall.db}"
LOCAL_FORECAST_DIR="${FORECAST_OUTPUT_DIR:-forecast_output}"
LOCAL_PUBLIC_DIR="web/public"

# Log file
LOG_FILE="logs/nas_sync_$(date +%Y%m%d).log"
mkdir -p logs

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

# Echo the snowfall_daily row count of a SQLite DB, or nothing on error.
db_row_count() {
    python3 - "$1" <<'PY' 2>/dev/null
import sqlite3, sys
try:
    c = sqlite3.connect(sys.argv[1], timeout=30)
    c.execute("PRAGMA integrity_check")
    print(c.execute("SELECT COUNT(*) FROM snowfall_daily").fetchone()[0])
except Exception:
    sys.exit(1)
PY
}

file_size() { stat -c%s "$1" 2>/dev/null || wc -c < "$1"; }

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
mkdir -p "$NAS_WEATHER_DIR/forecast" "$NAS_WEATHER_DIR/backup"

# --- Database sync (guarded) -------------------------------------------------
NAS_DB="$NAS_WEATHER_DIR/demo_global_snowfall.db"
DB_SYNC_STATUS=0

if [ ! -f "$LOCAL_DB" ]; then
    log "WARNING: local DB '$LOCAL_DB' not found — skipping DB sync (NAS copy left UNTOUCHED)"
else
    local_rows="$(db_row_count "$LOCAL_DB")"
    if [ -z "$local_rows" ]; then
        log "ERROR: local DB failed validation (not valid SQLite / unreadable) — refusing to sync. NAS DB left untouched."
        DB_SYNC_STATUS=1
    elif [ "$local_rows" -eq 0 ] && [ "${FORCE_DB_SYNC:-0}" != "1" ]; then
        log "ERROR: local DB has 0 snowfall rows — refusing to overwrite NAS DB. NAS DB left untouched. (FORCE_DB_SYNC=1 to override)"
        DB_SYNC_STATUS=1
    else
        # Shrink guard + pre-overwrite backup.
        if [ -f "$NAS_DB" ]; then
            local_size="$(file_size "$LOCAL_DB")"
            nas_size="$(file_size "$NAS_DB")"
            if [ "${FORCE_DB_SYNC:-0}" != "1" ] && [ "$nas_size" -gt 0 ] && [ "$((local_size * 2))" -lt "$nas_size" ]; then
                log "ERROR: local DB (${local_size}B) is <50% of NAS DB (${nas_size}B) — refusing to overwrite. NAS DB left untouched. (FORCE_DB_SYNC=1 to override)"
                DB_SYNC_STATUS=1
            else
                if cp "$NAS_DB" "$NAS_WEATHER_DIR/backup/demo_global_snowfall_preoverwrite_$(date +%Y%m%d_%H%M%S).db"; then
                    log "Backed up existing NAS DB before overwrite"
                fi
            fi
        fi

        if [ "$DB_SYNC_STATUS" -eq 0 ]; then
            if cp "$LOCAL_DB" "$NAS_DB"; then
                log "✅ Database synced to NAS ($local_rows rows)"
                cp "$LOCAL_DB" "$NAS_WEATHER_DIR/backup/demo_global_snowfall_$(date +%Y%m%d).db" \
                    && log "✅ Daily backup created"
            else
                log "❌ Failed to copy database to NAS"
                DB_SYNC_STATUS=1
            fi
        fi
    fi
fi

# --- Forecast files ----------------------------------------------------------
if [ -d "$LOCAL_FORECAST_DIR" ]; then
    [ -f "$LOCAL_FORECAST_DIR/latest_forecast.json" ] && \
        cp "$LOCAL_FORECAST_DIR/latest_forecast.json" "$NAS_WEATHER_DIR/forecast/" && \
        log "✅ Latest forecast synced"
    find "$LOCAL_FORECAST_DIR" -name "forecast_*.json" -mtime -7 -exec cp {} "$NAS_WEATHER_DIR/forecast/" \; 2>/dev/null
    log "✅ Recent forecasts synced"
else
    log "WARNING: forecast directory $LOCAL_FORECAST_DIR not found"
fi

# --- Public site files -------------------------------------------------------
if [ -d "$LOCAL_PUBLIC_DIR" ]; then
    cp -r "$LOCAL_PUBLIC_DIR"/* "$NAS_WEATHER_DIR/forecast/" 2>/dev/null
    log "✅ Public files synced"
fi

# --- Backup retention (only deletes timestamped backups, never the live DB) --
find "$NAS_WEATHER_DIR/backup" -name "*.db" -mtime +30 -delete 2>/dev/null
log "✅ Old backups (>30d) cleaned"

log "=========================================="
log "NAS SYNC COMPLETED (db_status=$DB_SYNC_STATUS)"
log "=========================================="

exit "$DB_SYNC_STATUS"
