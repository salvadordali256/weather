#!/usr/bin/env python3
"""
Historical Data Backfill
Fetches snowfall data back to 1940 for all stations
"""

import requests
import sqlite3
from datetime import datetime, timedelta
import time
import sys

def fetch_historical_range(station_id, lat, lon, name, start_year=1940):
    """Fetch all historical data for a station from start_year to present"""

    print(f"\n{'='*80}")
    print(f"Fetching historical data for: {name}")
    print(f"Station ID: {station_id}")
    print(f"Coordinates: {lat}, {lon}")
    print(f"{'='*80}")

    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    # Check what data we already have
    cursor.execute("""
        SELECT MIN(date), MAX(date), COUNT(*)
        FROM snowfall_daily
        WHERE station_id = ?
    """, (station_id,))

    existing = cursor.fetchone()
    if existing[0]:
        print(f"Existing data: {existing[0]} to {existing[1]} ({existing[2]} records)")
    else:
        print(f"No existing data - starting fresh")

    # Fetch data in yearly chunks to avoid timeouts
    current_year = datetime.now().year
    total_records = 0

    for year in range(start_year, current_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # For current year, only go up to today
        if year == current_year:
            end_date = datetime.now().strftime('%Y-%m-%d')

        print(f"  Fetching {year}...", end='', flush=True)

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'snowfall_sum',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'daily' in data and 'time' in data['daily']:
                dates = data['daily']['time']
                snowfall = data['daily']['snowfall_sum']

                records = []
                for date, snow in zip(dates, snowfall):
                    if snow is not None:
                        snow_mm = snow * 10.0  # Convert cm to mm
                        records.append((station_id, date, snow_mm))

                if records:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO snowfall_daily (station_id, date, snowfall_mm)
                        VALUES (?, ?, ?)
                    """, records)
                    conn.commit()
                    total_records += len(records)

                    # Show progress
                    snow_days = sum(1 for r in records if r[2] > 5.0)
                    print(f" ✅ {len(records)} days, {snow_days} snow days")
                else:
                    print(f" ⚠️  No data available")
            else:
                print(f" ⚠️  No data in response")

            # Rate limiting - be respectful to the API
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            print(f" ❌ Error: {e}")
            # Continue with next year even if one fails
            time.sleep(1)
            continue
        except Exception as e:
            print(f" ❌ Unexpected error: {e}")
            time.sleep(1)
            continue

    conn.close()

    print(f"\n{'─'*80}")
    print(f"Total records added/updated: {total_records}")
    print(f"{'='*80}")

    return total_records

def main():
    """Backfill all stations with historical data"""

    print(f"\n{'#'*80}")
    print(f"HISTORICAL DATA BACKFILL - 1940 TO PRESENT")
    print(f"{'#'*80}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nThis will take approximately 10-15 minutes to complete.")
    print(f"Fetching 85+ years of data for all stations...")
    print(f"{'#'*80}\n")

    # All stations - regional and global
    stations = [
        # Target locations (Wisconsin)
        ('phelps_wi', 46.06, -89.08, 'Phelps, WI'),
        ('land_o_lakes_wi', 46.15, -89.34, 'Land O Lakes, WI'),
        ('eagle_river_wi', 45.92, -89.24, 'Eagle River, WI'),

        # Regional predictors (North America)
        ('winnipeg_mb', 49.90, -97.14, 'Winnipeg, MB'),
        ('thunder_bay_on', 48.38, -89.25, 'Thunder Bay, ON'),
        ('marquette_mi', 46.54, -87.40, 'Marquette, MI'),
        ('duluth_mn', 46.79, -92.10, 'Duluth, MN'),
        ('green_bay_wi', 44.52, -88.02, 'Green Bay, WI'),
        ('iron_mountain_mi', 45.82, -88.06, 'Iron Mountain, MI'),
        ('minneapolis_mn', 44.98, -93.27, 'Minneapolis, MN'),
        ('escanaba_mi', 45.75, -87.06, 'Escanaba, MI'),

        # Global predictors (Asia)
        ('sapporo_japan', 43.06, 141.35, 'Sapporo, Japan'),
        ('niigata_japan', 37.92, 139.04, 'Niigata, Japan'),
        ('nagano_japan', 36.65, 138.19, 'Nagano, Japan'),

        # Global predictors (Europe)
        ('chamonix_france', 45.92, 6.87, 'Chamonix, France'),
        ('zermatt_switzerland', 46.02, 7.75, 'Zermatt, Switzerland'),
        ('st_moritz_switzerland', 46.50, 9.84, 'St. Moritz, Switzerland'),

        # Global predictors (Russia)
        ('irkutsk_russia', 52.27, 104.30, 'Irkutsk, Russia'),
        ('novosibirsk_russia', 55.03, 82.92, 'Novosibirsk, Russia'),

        # Global predictors (US West)
        ('steamboat_springs_co', 40.48, -106.83, 'Steamboat Springs, CO'),
        ('lake_tahoe_ca', 39.10, -120.15, 'Lake Tahoe, CA'),
        ('mount_baker_wa', 48.85, -121.67, 'Mount Baker, WA'),
        ('jackson_hole_wy', 43.58, -110.83, 'Jackson Hole, WY'),
        ('aspen_co', 39.19, -106.82, 'Aspen, CO'),
    ]

    total_stations = len(stations)
    completed = 0
    total_records = 0

    for station_id, lat, lon, name in stations:
        completed += 1
        print(f"\n[{completed}/{total_stations}] Processing {name}...")

        records = fetch_historical_range(station_id, lat, lon, name, start_year=1940)
        total_records += records

        # Longer pause between stations
        if completed < total_stations:
            print(f"\nPausing 2 seconds before next station...")
            time.sleep(2)

    print(f"\n{'#'*80}")
    print(f"BACKFILL COMPLETE")
    print(f"{'#'*80}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Stations processed: {completed}/{total_stations}")
    print(f"Total records added/updated: {total_records:,}")
    print(f"Database: demo_global_snowfall.db")
    print(f"{'#'*80}\n")

    # Show database size
    import os
    db_size = os.path.getsize('demo_global_snowfall.db')
    print(f"Database size: {db_size / (1024*1024):.1f} MB")

    # Show summary statistics
    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    print(f"\n{'─'*80}")
    print("DATABASE SUMMARY:")
    print(f"{'─'*80}")

    cursor.execute("""
        SELECT
            station_id,
            MIN(date) as earliest,
            MAX(date) as latest,
            COUNT(*) as total_days,
            COUNT(CASE WHEN snowfall_mm > 5 THEN 1 END) as snow_days,
            ROUND(AVG(snowfall_mm), 1) as avg_snowfall,
            ROUND(MAX(snowfall_mm), 1) as max_snowfall
        FROM snowfall_daily
        GROUP BY station_id
        ORDER BY station_id
    """)

    results = cursor.fetchall()
    for row in results:
        station, earliest, latest, days, snow_days, avg, max_snow = row
        years = (datetime.strptime(latest, '%Y-%m-%d').year -
                datetime.strptime(earliest, '%Y-%m-%d').year + 1)
        print(f"{station:25s} {earliest} to {latest} ({years:2d} years, {days:5d} days, {snow_days:4d} snow days)")

    conn.close()

    print(f"{'─'*80}\n")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Partial data has been saved.")
        sys.exit(1)
