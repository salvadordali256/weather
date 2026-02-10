#!/usr/bin/env python3
"""
Collect Hourly Weather Data
Rolling 30-day window of hourly observations for all stations.
Uses Open-Meteo Archive API with comprehensive variable set.
"""

import requests
import sqlite3
import os
import time
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')

# Full hourly variable set
HOURLY_VARIABLES = (
    'temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,'
    'pressure_msl,surface_pressure,'
    'precipitation,rain,snowfall,snow_depth,'
    'cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,'
    'wind_speed_10m,wind_direction_10m,wind_gusts_10m,'
    'visibility,weather_code,'
    'cape,freezing_level_height,'
    'soil_temperature_0cm,soil_moisture_0_to_1cm'
)

from collect_world_data import WORLD_STATIONS, migrate_schema


def setup_logging():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"hourly_collect_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger('hourly')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(sh)
    return logger


def setup_hourly_table(conn):
    """Create hourly observations table"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hourly_observations (
            station_id TEXT NOT NULL,
            datetime TEXT NOT NULL,
            temperature_2m REAL,
            relative_humidity_2m REAL,
            dew_point_2m REAL,
            apparent_temperature REAL,
            pressure_msl REAL,
            surface_pressure REAL,
            precipitation REAL,
            rain REAL,
            snowfall REAL,
            snow_depth REAL,
            cloud_cover REAL,
            cloud_cover_low REAL,
            cloud_cover_mid REAL,
            cloud_cover_high REAL,
            wind_speed_10m REAL,
            wind_direction_10m REAL,
            wind_gusts_10m REAL,
            visibility REAL,
            weather_code INTEGER,
            cape REAL,
            freezing_level_height REAL,
            soil_temperature_0cm REAL,
            soil_moisture_0_to_1cm REAL,
            PRIMARY KEY (station_id, datetime)
        )
    """)
    conn.commit()


def collect_station_hourly(station_id, lat, lon, name, start_date, end_date):
    """Fetch hourly data for one station"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date,
        'end_date': end_date,
        'hourly': HOURLY_VARIABLES,
        'timezone': 'UTC'
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if 'hourly' not in data or 'time' not in data['hourly']:
        return 0, []

    h = data['hourly']
    times = h['time']
    n = len(times)

    def get(key):
        return h.get(key, [None] * n)

    records = []
    for i in range(n):
        snow_raw = get('snowfall')[i]
        snow_val = snow_raw * 10.0 if snow_raw is not None else None

        records.append((
            station_id, times[i],
            get('temperature_2m')[i], get('relative_humidity_2m')[i],
            get('dew_point_2m')[i], get('apparent_temperature')[i],
            get('pressure_msl')[i], get('surface_pressure')[i],
            get('precipitation')[i], get('rain')[i],
            snow_val, get('snow_depth')[i],
            get('cloud_cover')[i], get('cloud_cover_low')[i],
            get('cloud_cover_mid')[i], get('cloud_cover_high')[i],
            get('wind_speed_10m')[i], get('wind_direction_10m')[i],
            get('wind_gusts_10m')[i],
            get('visibility')[i], get('weather_code')[i],
            get('cape')[i], get('freezing_level_height')[i],
            get('soil_temperature_0cm')[i], get('soil_moisture_0_to_1cm')[i],
        ))

    return len(records), records


def main():
    parser = argparse.ArgumentParser(description='Collect Hourly Weather Data')
    parser.add_argument('--days', type=int, default=30,
                        help='Days of hourly data to fetch (default: 30)')
    parser.add_argument('--rate-limit', type=float, default=0.5,
                        help='Seconds between API calls (default: 0.5)')
    parser.add_argument('--budget', type=float, default=3000,
                        help='Max weighted API calls (default: 3000)')
    args = parser.parse_args()

    logger = setup_logging()

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')

    # Build station list
    all_stations = []
    for region, station_list in WORLD_STATIONS.items():
        for station_id, lat, lon, name in station_list:
            all_stations.append((station_id, lat, lon, name, region))

    num_vars = len(HOURLY_VARIABLES.split(','))
    cost_per_station = max(num_vars / 10, (num_vars / 10) * (args.days / 7))

    logger.info("=" * 80)
    logger.info("HOURLY DATA COLLECTION")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Stations: {len(all_stations)}")
    logger.info(f"Variables: {num_vars}")
    logger.info(f"Cost per station: {cost_per_station:.1f} weighted calls")
    logger.info(f"Total estimated cost: {cost_per_station * len(all_stations):.0f}")
    logger.info(f"Budget: {args.budget}")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    setup_hourly_table(conn)

    weighted_used = 0.0
    success = 0
    failed = 0

    for station_id, lat, lon, name, region in all_stations:
        if weighted_used + cost_per_station > args.budget:
            logger.info(f"Budget exhausted ({weighted_used:.1f}/{args.budget})")
            break

        try:
            count, records = collect_station_hourly(
                station_id, lat, lon, name, start_date, end_date
            )

            if count > 0:
                conn.executemany("""
                    INSERT OR REPLACE INTO hourly_observations
                    (station_id, datetime,
                     temperature_2m, relative_humidity_2m,
                     dew_point_2m, apparent_temperature,
                     pressure_msl, surface_pressure,
                     precipitation, rain, snowfall, snow_depth,
                     cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high,
                     wind_speed_10m, wind_direction_10m, wind_gusts_10m,
                     visibility, weather_code,
                     cape, freezing_level_height,
                     soil_temperature_0cm, soil_moisture_0_to_1cm)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, records)
                conn.commit()
                success += 1
                logger.info(f"  {name}: {count} hourly records")
            else:
                logger.info(f"  {name}: no data")

            weighted_used += cost_per_station

        except Exception as e:
            failed += 1
            logger.warning(f"  {name}: FAILED — {e}")
            weighted_used += cost_per_station

        time.sleep(args.rate_limit)

    # Clean old data beyond rolling window
    cutoff = (datetime.now() - timedelta(days=args.days + 7)).strftime('%Y-%m-%dT00:00')
    deleted = conn.execute("DELETE FROM hourly_observations WHERE datetime < ?", (cutoff,))
    conn.commit()
    logger.info(f"Cleaned {deleted.rowcount} old hourly records (before {cutoff})")

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("HOURLY COLLECTION COMPLETE")
    logger.info(f"Successful: {success}/{len(all_stations)}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Weighted calls: {weighted_used:.1f}/{args.budget}")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
