#!/usr/bin/env python3
"""
Add Lake Superior Snow Belt Stations
Strategic stations for enhanced lake effect coverage
"""

import requests
import sqlite3
from datetime import datetime, timedelta
import time
import sys

def add_station_to_database(station_id, name, lat, lon):
    """Add station to stations table if not exists"""
    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO stations (station_id, name, latitude, longitude)
        VALUES (?, ?, ?, ?)
    """, (station_id, name, lat, lon))

    conn.commit()
    conn.close()

def fetch_station_data(station_id, name, lat, lon, start_year=1940):
    """Fetch snowfall data for a station from 1940 to present"""

    print(f"\n{'='*80}")
    print(f"Station: {name}")
    print(f"ID: {station_id}")
    print(f"Location: {lat}°N, {lon}°W")
    print(f"{'='*80}")

    # Add to stations table
    add_station_to_database(station_id, name, lat, lon)

    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    # Check existing data
    cursor.execute("""
        SELECT MIN(date), MAX(date), COUNT(*)
        FROM snowfall_daily
        WHERE station_id = ?
    """, (station_id,))

    existing = cursor.fetchone()
    if existing[0]:
        print(f"Existing data: {existing[0]} to {existing[1]} ({existing[2]} records)")
    else:
        print("No existing data - starting fresh")

    current_year = datetime.now().year
    total_records = 0

    for year in range(start_year, current_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        if year == current_year:
            end_date = datetime.now().strftime('%Y-%m-%d')

        print(f"  {year}...", end='', flush=True)

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

                    # Count snow days
                    snow_days = sum(1 for r in records if r[2] > 5.0)
                    print(f" ✅ {len(records)} days ({snow_days} snow days)")
                else:
                    print(f" ⚠️  No data")
            else:
                print(f" ⚠️  No data")

            time.sleep(0.2)  # Rate limiting

        except Exception as e:
            print(f" ❌ Error: {e}")
            time.sleep(1)
            continue

    conn.close()

    print(f"{'─'*80}")
    print(f"Total records: {total_records}")
    print(f"{'='*80}")

    return total_records

def main():
    """Add Lake Superior snow belt stations"""

    print(f"\n{'#'*80}")
    print("ADDING LAKE SUPERIOR SNOW BELT STATIONS")
    print(f"{'#'*80}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Lake Superior snow belt hot spots
    stations = [
        ('houghton_mi', 'Houghton, MI (Keweenaw)', 47.12, -88.57),
        ('hancock_mi', 'Hancock, MI (Keweenaw)', 47.13, -88.58),
        ('ashland_wi', 'Ashland, WI', 46.59, -90.88),
        ('grand_marais_mn', 'Grand Marais, MN', 47.75, -90.33),
        ('ironwood_mi', 'Ironwood, MI', 46.45, -90.17),
    ]

    print("Lake Superior Snow Belt Enhancement:")
    print("  - Extreme lake effect zones (Keweenaw Peninsula)")
    print("  - Wisconsin north shore (Ashland)")
    print("  - Minnesota north shore (Grand Marais)")
    print("  - Western UP gap filler (Ironwood)")
    print()
    print(f"Fetching 1940-2026 for {len(stations)} stations")
    print(f"Estimated time: ~10-15 minutes")
    print()

    response = input("Continue? [y/N]: ")
    if response.lower() not in ['y', 'yes']:
        print("Cancelled.")
        return

    print()

    total_stations = len(stations)
    completed = 0
    total_records = 0

    for station_id, name, lat, lon in stations:
        completed += 1
        print(f"\n[{completed}/{total_stations}] Processing {name}...")

        records = fetch_station_data(station_id, name, lat, lon)
        total_records += records

        if completed < total_stations:
            print(f"\nPausing 2 seconds...")
            time.sleep(2)

    print(f"\n{'#'*80}")
    print("LAKE SUPERIOR STATIONS ADDED")
    print(f"{'#'*80}")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Stations: {completed}/{total_stations}")
    print(f"Records: {total_records:,}")
    print(f"{'#'*80}\n")

    # Show summary
    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    print("LAKE SUPERIOR COVERAGE SUMMARY:")
    print(f"{'─'*80}")

    cursor.execute("""
        SELECT
            station_id,
            COUNT(*) as days,
            COUNT(CASE WHEN snowfall_mm > 5 THEN 1 END) as snow_days,
            ROUND(AVG(snowfall_mm), 1) as avg_mm,
            ROUND(MAX(snowfall_mm), 1) as max_mm
        FROM snowfall_daily
        WHERE station_id LIKE '%houghton%'
           OR station_id LIKE '%hancock%'
           OR station_id LIKE '%ashland%'
           OR station_id LIKE '%grand_marais%'
           OR station_id LIKE '%ironwood%'
        GROUP BY station_id
        ORDER BY snow_days DESC
    """)

    for row in cursor.fetchall():
        station, days, snow_days, avg, max_snow = row
        pct = (snow_days / days * 100) if days > 0 else 0
        print(f"{station:25s} {days:5d} days | {snow_days:4d} snow days ({pct:4.1f}%) | Max: {max_snow:5.1f}mm")

    conn.close()
    print(f"{'─'*80}\n")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted. Partial data saved.")
        sys.exit(1)
