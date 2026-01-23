#!/usr/bin/env python3
"""
Update Recent Data for Regional Stations
Gets the last 7 days to ensure we have the latest conditions
"""

import requests
import sqlite3
from datetime import datetime, timedelta
import time

def update_station_recent(station_id, lat, lon, name):
    """Update last 7 days of data for a station"""

    print(f"Updating {name}...")

    # Last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'daily': 'snowfall_sum',
        'timezone': 'America/Chicago'
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
                    snow_mm = snow * 10.0
                    records.append((station_id, date, snow_mm))

            # Save to database
            conn = sqlite3.connect('demo_global_snowfall.db')
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO snowfall_daily (station_id, date, snowfall_mm)
                VALUES (?, ?, ?)
            """, records)
            conn.commit()
            conn.close()

            print(f"  ✅ Updated {len(records)} records")

            # Show recent values
            for date, snow_mm in zip(dates[-3:], [s*10 if s else 0 for s in snowfall[-3:]]):
                print(f"     {date}: {snow_mm:.1f}mm")

            return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

    return False

def main():
    print(f"\n{'='*80}")
    print(f"UPDATING RECENT DATA FOR REGIONAL STATIONS")
    print(f"{'='*80}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Updating last 7 days to capture latest conditions")
    print(f"{'='*80}\n")

    # Critical regional stations
    stations = [
        ('winnipeg_mb', 49.90, -97.14, 'Winnipeg, MB'),
        ('thunder_bay_on', 48.38, -89.25, 'Thunder Bay, ON'),
        ('marquette_mi', 46.54, -87.40, 'Marquette, MI'),
        ('green_bay_wi', 44.52, -88.02, 'Green Bay, WI'),
        ('duluth_mn', 46.79, -92.10, 'Duluth, MN'),
        ('iron_mountain_mi', 45.82, -88.06, 'Iron Mountain, MI'),
        ('phelps_wi', 46.06, -89.08, 'Phelps, WI'),
        ('land_o_lakes_wi', 46.15, -89.34, 'Land O Lakes, WI'),
    ]

    success = 0
    for station_id, lat, lon, name in stations:
        if update_station_recent(station_id, lat, lon, name):
            success += 1
        time.sleep(0.5)
        print()

    print(f"{'='*80}")
    print(f"UPDATE COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully updated: {success}/{len(stations)} stations\n")

if __name__ == '__main__':
    main()
