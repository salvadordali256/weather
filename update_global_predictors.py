#!/usr/bin/env python3
"""Update global predictor stations with latest data"""

import requests
import sqlite3
import os
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Default database path, can be overridden via DB_PATH environment variable
DEFAULT_DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')

def update_station(station_id, lat, lon, name):
    """Update last 14 days of data for a station"""
    print(f"Updating {name}...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=14)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
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
                    snow_mm = snow * 10.0
                    records.append((station_id, date, snow_mm))

            conn = sqlite3.connect(DEFAULT_DB_PATH, timeout=30)
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO snowfall_daily (station_id, date, snowfall_mm)
                VALUES (?, ?, ?)
            """, records)
            conn.commit()
            conn.close()

            print(f"  ✅ Updated {len(records)} records")

            # Show recent significant values
            recent_snow = [(d, s*10 if s else 0) for d, s in zip(dates[-5:], snowfall[-5:])]
            for date, snow_mm in recent_snow:
                if snow_mm > 5:
                    print(f"     {date}: {snow_mm:.1f}mm ❄️")

            return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

    return False

def main():
    # Global predictor stations
    stations = [
        ('sapporo_japan', 43.06, 141.35, 'Sapporo, Japan'),
        ('chamonix_france', 45.92, 6.87, 'Chamonix, France'),
        ('irkutsk_russia', 52.27, 104.30, 'Irkutsk, Russia'),
        ('zermatt_switzerland', 46.02, 7.75, 'Zermatt, Switzerland'),
        ('niigata_japan', 37.92, 139.04, 'Niigata, Japan'),
    ]

    print(f"\n{'='*80}")
    print("UPDATING GLOBAL PREDICTOR STATIONS")
    print(f"{'='*80}\n")

    success = 0
    for station_id, lat, lon, name in stations:
        if update_station(station_id, lat, lon, name):
            success += 1
        time.sleep(0.5)
        print()

    print(f"{'='*80}")
    print(f"Successfully updated: {success}/{len(stations)} global stations")
    print(f"{'='*80}\n")

if __name__ == '__main__':
    main()
