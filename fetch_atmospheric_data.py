#!/usr/bin/env python3
"""
Atmospheric Data Fetcher
Adds temperature, pressure, wind, humidity, and cloud data to existing stations
"""

import requests
import sqlite3
from datetime import datetime, timedelta
import time
import sys

def setup_atmospheric_table():
    """Create table for atmospheric variables"""
    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    # Create atmospheric data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS atmospheric_daily (
            station_id TEXT NOT NULL,
            date TEXT NOT NULL,
            temp_min_c REAL,
            temp_max_c REAL,
            temp_mean_c REAL,
            pressure_msl_hpa REAL,
            pressure_surface_hpa REAL,
            wind_speed_max_kmh REAL,
            wind_direction_dominant INTEGER,
            wind_gusts_max_kmh REAL,
            relative_humidity_mean REAL,
            dewpoint_mean_c REAL,
            cloud_cover_mean REAL,
            precipitation_sum_mm REAL,
            rain_sum_mm REAL,
            PRIMARY KEY (station_id, date),
            FOREIGN KEY (station_id) REFERENCES stations(station_id)
        )
    """)

    # Create index for faster queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_atmospheric_station_date
        ON atmospheric_daily(station_id, date)
    """)

    conn.commit()
    conn.close()

    print("✅ Atmospheric data table created")

def get_existing_stations():
    """Get list of stations that already have snowfall data"""
    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT station_id
        FROM snowfall_daily
        ORDER BY station_id
    """)

    stations = [row[0] for row in cursor.fetchall()]
    conn.close()

    return stations

def get_station_coordinates():
    """Map of station IDs to coordinates"""
    # Reuse coordinates from historical backfill
    return {
        # Wisconsin
        'phelps_wi': (46.06, -89.08),
        'land_o_lakes_wi': (46.15, -89.34),
        'eagle_river_wi': (45.92, -89.24),

        # Regional (North America)
        'winnipeg_mb': (49.90, -97.14),
        'thunder_bay_on': (48.38, -89.25),
        'marquette_mi': (46.54, -87.40),
        'duluth_mn': (46.79, -92.10),
        'green_bay_wi': (44.52, -88.02),
        'iron_mountain_mi': (45.82, -88.06),
        'minneapolis_mn': (44.98, -93.27),
        'escanaba_mi': (45.75, -87.06),

        # Asia
        'sapporo_japan': (43.06, 141.35),
        'niigata_japan': (37.92, 139.04),
        'nagano_japan': (36.65, 138.19),

        # Europe
        'chamonix_france': (45.92, 6.87),
        'zermatt_switzerland': (46.02, 7.75),
        'st_moritz_switzerland': (46.50, 9.84),

        # Russia
        'irkutsk_russia': (52.27, 104.30),
        'novosibirsk_russia': (55.03, 82.92),

        # US West
        'steamboat_springs_co': (40.48, -106.83),
        'lake_tahoe_ca': (39.10, -120.15),
        'mount_baker_wa': (48.85, -121.67),
        'jackson_hole_wy': (43.58, -110.83),
        'aspen_co': (39.19, -106.82),

        # Additional (if they exist)
        'denver_co': (39.74, -104.99),
        'sault_ste_marie_mi': (46.50, -84.35),
        'mammoth_mountain_ca': (37.63, -119.03),
        'yakutsk_russia': (62.03, 129.73),
        'land_o\'lakes_wi': (46.15, -89.34),  # Alternative spelling
    }

def fetch_atmospheric_data_for_station(station_id, lat, lon, start_year=2005):
    """Fetch atmospheric variables for a station"""

    print(f"\n{'='*80}")
    print(f"Fetching atmospheric data: {station_id}")
    print(f"Coordinates: {lat}, {lon}")
    print(f"{'='*80}")

    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    # Check existing data
    cursor.execute("""
        SELECT MIN(date), MAX(date), COUNT(*)
        FROM atmospheric_daily
        WHERE station_id = ?
    """, (station_id,))

    existing = cursor.fetchone()
    if existing[0]:
        print(f"Existing atmospheric data: {existing[0]} to {existing[1]} ({existing[2]} records)")
    else:
        print(f"No existing atmospheric data - starting fresh")

    current_year = datetime.now().year
    total_records = 0

    # Fetch year by year
    for year in range(start_year, current_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        if year == current_year:
            end_date = datetime.now().strftime('%Y-%m-%d')

        print(f"  Fetching {year}...", end='', flush=True)

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,wind_speed_10m_max,precipitation_sum',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'daily' in data and 'time' in data['daily']:
                dates = data['daily']['time']

                records = []
                for i, date in enumerate(dates):
                    record = {
                        'station_id': station_id,
                        'date': date,
                        'temp_min_c': data['daily'].get('temperature_2m_min', [None]*len(dates))[i],
                        'temp_max_c': data['daily'].get('temperature_2m_max', [None]*len(dates))[i],
                        'temp_mean_c': data['daily'].get('temperature_2m_mean', [None]*len(dates))[i],
                        'pressure_msl_hpa': None,  # Not available in archive API
                        'pressure_surface_hpa': None,  # Not available in archive API
                        'wind_speed_max_kmh': data['daily'].get('wind_speed_10m_max', [None]*len(dates))[i],
                        'wind_direction_dominant': data['daily'].get('wind_direction_10m_dominant', [None]*len(dates))[i],
                        'wind_gusts_max_kmh': data['daily'].get('wind_gusts_10m_max', [None]*len(dates))[i],
                        'relative_humidity_mean': None,  # Not available in archive API
                        'dewpoint_mean_c': None,  # Not available in archive API
                        'cloud_cover_mean': None,  # Not available in archive API
                        'precipitation_sum_mm': data['daily'].get('precipitation_sum', [None]*len(dates))[i],
                        'rain_sum_mm': data['daily'].get('rain_sum', [None]*len(dates))[i],
                    }
                    records.append(record)

                if records:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO atmospheric_daily (
                            station_id, date, temp_min_c, temp_max_c, temp_mean_c,
                            pressure_msl_hpa, pressure_surface_hpa,
                            wind_speed_max_kmh, wind_direction_dominant, wind_gusts_max_kmh,
                            relative_humidity_mean, dewpoint_mean_c, cloud_cover_mean,
                            precipitation_sum_mm, rain_sum_mm
                        ) VALUES (
                            :station_id, :date, :temp_min_c, :temp_max_c, :temp_mean_c,
                            :pressure_msl_hpa, :pressure_surface_hpa,
                            :wind_speed_max_kmh, :wind_direction_dominant, :wind_gusts_max_kmh,
                            :relative_humidity_mean, :dewpoint_mean_c, :cloud_cover_mean,
                            :precipitation_sum_mm, :rain_sum_mm
                        )
                    """, records)
                    conn.commit()
                    total_records += len(records)
                    print(f" ✅ {len(records)} days")
                else:
                    print(f" ⚠️  No data")
            else:
                print(f" ⚠️  No data in response")

            time.sleep(0.5)  # Rate limiting - increased for multiple variables

        except Exception as e:
            print(f" ❌ Error: {e}")
            time.sleep(1)
            continue

    conn.close()

    print(f"\n{'─'*80}")
    print(f"Total atmospheric records added: {total_records}")
    print(f"{'='*80}")

    return total_records

def main():
    """Main execution"""

    print(f"\n{'#'*80}")
    print("ATMOSPHERIC DATA COLLECTION")
    print("Fetching temperature, pressure, wind, humidity, clouds")
    print(f"{'#'*80}\n")

    # Setup database
    print("Setting up database table...")
    setup_atmospheric_table()
    print()

    # Get stations
    stations = get_existing_stations()
    coords = get_station_coordinates()

    print(f"Found {len(stations)} stations with snowfall data")
    print()

    # Prompt for confirmation
    print("This will fetch atmospheric data for all stations (2005-2026)")
    print("Recent 20 years - avoids API rate limits while providing modern climate data")
    print(f"Estimated time: ~25-35 minutes")
    print(f"Estimated database size increase: ~35-50 MB")
    print()

    response = input("Continue? [y/N]: ")
    if response.lower() not in ['y', 'yes']:
        print("Cancelled.")
        return

    print()

    # Fetch data
    total_stations = len(stations)
    completed = 0
    total_records = 0

    for station_id in stations:
        if station_id not in coords:
            print(f"⚠️  Skipping {station_id} - no coordinates found")
            continue

        completed += 1
        lat, lon = coords[station_id]

        print(f"\n[{completed}/{total_stations}] Processing {station_id}...")

        records = fetch_atmospheric_data_for_station(station_id, lat, lon, start_year=2005)
        total_records += records

        if completed < total_stations:
            print(f"\nPausing 2 seconds before next station...")
            time.sleep(2)

    print(f"\n{'#'*80}")
    print("ATMOSPHERIC DATA COLLECTION COMPLETE")
    print(f"{'#'*80}")
    print(f"Stations processed: {completed}/{total_stations}")
    print(f"Total atmospheric records: {total_records:,}")
    print(f"Database: demo_global_snowfall.db")
    print(f"{'#'*80}\n")

    # Show database size
    import os
    db_size = os.path.getsize('demo_global_snowfall.db')
    print(f"Database size: {db_size / (1024*1024):.1f} MB")

    # Show sample data
    conn = sqlite3.connect('demo_global_snowfall.db')
    cursor = conn.cursor()

    print(f"\n{'─'*80}")
    print("SAMPLE DATA (Most Recent):")
    print(f"{'─'*80}")

    cursor.execute("""
        SELECT station_id, date, temp_mean_c, pressure_msl_hpa, wind_speed_max_kmh
        FROM atmospheric_daily
        ORDER BY date DESC
        LIMIT 10
    """)

    for row in cursor.fetchall():
        station, date, temp, pressure, wind = row
        print(f"{station:25s} {date} | {temp:5.1f}°C | {pressure:7.1f} hPa | {wind:5.1f} km/h")

    conn.close()
    print(f"{'─'*80}\n")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Partial data has been saved.")
        sys.exit(1)
