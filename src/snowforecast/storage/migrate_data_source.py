#!/usr/bin/env python3
"""
Migrate snowfall_daily table to support data_source tracking.

Adds a 'data_source' column to distinguish NOAA vs Open-Meteo records.
Safe to run multiple times (idempotent).

Run on both local dev machine and NAS:
    python migrate_data_source.py
"""

import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')


def migrate(db_path=None):
    path = db_path or DB_PATH
    print(f"Migrating database: {path}")

    if not os.path.exists(path):
        print(f"  Database not found: {path}")
        return False

    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=30000")

    cursor = conn.cursor()

    # Check if column already exists
    cursor.execute("PRAGMA table_info(snowfall_daily)")
    columns = {row[1] for row in cursor.fetchall()}

    if 'data_source' in columns:
        print("  data_source column already exists — skipping ALTER")
    else:
        cursor.execute("ALTER TABLE snowfall_daily ADD COLUMN data_source TEXT DEFAULT 'open-meteo'")
        print("  Added column: data_source TEXT DEFAULT 'open-meteo'")

    # Tag all existing NULL rows as open-meteo
    cursor.execute("UPDATE snowfall_daily SET data_source = 'open-meteo' WHERE data_source IS NULL")
    affected = cursor.rowcount
    print(f"  Tagged {affected} existing rows as 'open-meteo'")

    # Create index for efficient source-aware queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_snowfall_data_source ON snowfall_daily(data_source)")
    print("  Created index: idx_snowfall_data_source")

    conn.commit()

    # Verify
    cursor.execute("SELECT data_source, COUNT(*) FROM snowfall_daily GROUP BY data_source")
    counts = cursor.fetchall()
    print(f"\n  Data source counts:")
    for source, count in counts:
        print(f"    {source}: {count:,} records")

    conn.close()
    print("\nMigration complete!")
    return True


if __name__ == '__main__':
    migrate()
