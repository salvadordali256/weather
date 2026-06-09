#!/bin/bash
# Daily database backup to NAS — keeps 7 days of rolling backups

DB_PATH="${DB_PATH:-/home/kylej/weather/global_snowfall.db}"
BACKUP_DIR="/mnt/nas/db_backups"

if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    exit 1
fi

if [ ! -d "$BACKUP_DIR" ]; then
    echo "ERROR: Backup dir $BACKUP_DIR not mounted or missing"
    exit 1
fi

DEST="$BACKUP_DIR/global_snowfall_$(date +%Y%m%d).db"
cp "$DB_PATH" "$DEST" && echo "Backed up to $DEST" || { echo "Backup FAILED"; exit 1; }

# Keep only last 7 backups
find "$BACKUP_DIR" -name "global_snowfall_*.db" -mtime +7 -delete
echo "Cleanup done. Current backups:"
ls -lh "$BACKUP_DIR"/global_snowfall_*.db 2>/dev/null
