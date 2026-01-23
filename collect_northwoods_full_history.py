"""
Northern Wisconsin Snowfall - FULL HISTORICAL DATA COLLECTION
==============================================================

Collects the MAXIMUM available data from Open-Meteo (1940-present)
That's 85+ years of snowfall history!
"""

import sys
import sqlite3
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_database(db_path: str):
    """Create the snowfall database schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stations (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        latitude REAL,
        longitude REAL,
        elevation REAL,
        state TEXT,
        country TEXT,
        min_date TEXT,
        max_date TEXT,
        data_source TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS snowfall_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_id TEXT NOT NULL,
        date TEXT NOT NULL,
        snowfall_mm REAL,
        snow_depth_mm REAL,
        temp_max_celsius REAL,
        temp_min_celsius REAL,
        precipitation_mm REAL,
        data_quality TEXT,
        UNIQUE(station_id, date)
    )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_station_date ON snowfall_daily(station_id, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON snowfall_daily(date)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS station_summaries (
        station_id TEXT PRIMARY KEY,
        total_years INTEGER,
        total_snowfall_mm REAL,
        avg_annual_snowfall_mm REAL,
        max_daily_snowfall_mm REAL,
        max_daily_snowfall_date TEXT,
        max_snow_depth_mm REAL,
        max_snow_depth_date TEXT,
        first_snow_date TEXT,
        last_snow_date TEXT,
        days_with_snow INTEGER
    )
    """)

    conn.commit()
    conn.close()
    logger.info(f"✓ Database created: {db_path}")


def collect_full_history(db_path: str):
    """Collect ALL available Open-Meteo data (1940-present)"""

    # Open-Meteo archive starts in 1940
    start_date = "1940-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("=" * 80)
    logger.info("FULL HISTORICAL DATA COLLECTION - NORTHERN WISCONSIN")
    logger.info("=" * 80)
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"That's {datetime.now().year - 1940} years of data!")
    logger.info(f"Database: {db_path}")
    logger.info("Source: Open-Meteo Historical Archive")
    logger.info("")

    create_database(db_path)

    fetcher = OpenMeteoWeatherFetcher()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    locations = [
        {"name": "Eagle River, WI", "lat": 45.9169, "lon": -89.2443, "county": "Vilas"},
        {"name": "Land O'Lakes, WI", "lat": 46.1558, "lon": -89.3318, "county": "Vilas"},
        {"name": "Phelps, WI", "lat": 46.0608, "lon": -89.0793, "county": "Vilas"},
        {"name": "Iron River, WI", "lat": 46.5666, "lon": -91.4016, "county": "Bayfield"},
        {"name": "Palmer, WI", "lat": 46.4833, "lon": -90.8500, "county": "Ashland"},
        {"name": "St. Germain, WI", "lat": 45.9080, "lon": -89.4685, "county": "Vilas"},
        {"name": "Boulder Junction, WI", "lat": 46.1119, "lon": -89.6418, "county": "Vilas"},
        {"name": "Mercer, WI", "lat": 46.1666, "lon": -90.0666, "county": "Iron"},
        {"name": "Hurley, WI", "lat": 46.4497, "lon": -90.1857, "county": "Iron"},
    ]

    logger.info(f"Collecting 85 years of data from {len(locations)} locations")
    logger.info("This will take several minutes...")
    logger.info("")

    total_stations = 0
    total_records = 0

    for location in locations:
        station_id = f"WI_{location['name'].replace(', WI', '').replace(' ', '_')}"
        logger.info(f"Processing {location['name']}...")

        try:
            cursor.execute("""
            INSERT OR REPLACE INTO stations
            (station_id, name, latitude, longitude, elevation, state, country,
             min_date, max_date, data_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                station_id,
                location['name'],
                location['lat'],
                location['lon'],
                None,
                "Wisconsin",
                "USA",
                start_date,
                end_date,
                "Open-Meteo Historical Archive"
            ))

            # Get historical weather data
            result = fetcher.fetch_historical_data(
                latitude=location['lat'],
                longitude=location['lon'],
                start_date=start_date,
                end_date=end_date,
                daily_params=[
                    "snowfall_sum",
                    "snow_depth_mean",
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum"
                ]
            )

            if result and 'daily' in result:
                daily_df = result['daily']

                if not daily_df.empty:
                    for idx, row in daily_df.iterrows():
                        date = row.get('time', idx)
                        snowfall_cm = row.get('snowfall_sum')
                        snow_depth_cm = row.get('snow_depth_mean')
                        temp_max = row.get('temperature_2m_max')
                        temp_min = row.get('temperature_2m_min')
                        precip_mm = row.get('precipitation_sum')

                        snowfall_mm = snowfall_cm * 10 if pd.notna(snowfall_cm) else None
                        snow_depth_mm = snow_depth_cm * 10 if pd.notna(snow_depth_cm) else None

                        cursor.execute("""
                        INSERT OR REPLACE INTO snowfall_daily
                        (station_id, date, snowfall_mm, snow_depth_mm,
                         temp_max_celsius, temp_min_celsius, precipitation_mm, data_quality)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            station_id,
                            str(date),
                            snowfall_mm,
                            snow_depth_mm,
                            temp_max if pd.notna(temp_max) else None,
                            temp_min if pd.notna(temp_min) else None,
                            precip_mm if pd.notna(precip_mm) else None,
                            'good'
                        ))
                        total_records += 1

                    logger.info(f"  ✓ Collected {len(daily_df):,} days ({len(daily_df)//365} years)")
                    total_stations += 1
                    conn.commit()

        except Exception as e:
            logger.warning(f"  ✗ Failed: {e}")
            continue

    # Calculate summary statistics
    logger.info("")
    logger.info("Calculating summary statistics...")

    for location in locations:
        station_id = f"WI_{location['name'].replace(', WI', '').replace(' ', '_')}"

        cursor.execute("""
        SELECT
            COUNT(DISTINCT strftime('%Y', date)) as years,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) as total_snow,
            MAX(snowfall_mm) as max_daily,
            MAX(snow_depth_mm) as max_depth,
            COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as days_with_snow
        FROM snowfall_daily
        WHERE station_id = ?
        """, (station_id,))

        result = cursor.fetchone()
        if result and result[0] > 0:
            years, total_snow, max_daily, max_depth, days_with_snow = result
            avg_annual = total_snow / years if years > 0 else 0

            cursor.execute("""
            SELECT date FROM snowfall_daily
            WHERE station_id = ? AND snowfall_mm = ?
            LIMIT 1
            """, (station_id, max_daily))
            max_result = cursor.fetchone()
            max_date = max_result[0] if max_result else None

            cursor.execute("""
            SELECT date FROM snowfall_daily
            WHERE station_id = ? AND snow_depth_mm = ?
            LIMIT 1
            """, (station_id, max_depth))
            max_depth_result = cursor.fetchone()
            max_depth_date = max_depth_result[0] if max_depth_result else None

            cursor.execute("""
            SELECT MIN(date), MAX(date)
            FROM snowfall_daily
            WHERE station_id = ? AND snowfall_mm > 0
            """, (station_id,))
            first_snow, last_snow = cursor.fetchone()

            cursor.execute("""
            INSERT OR REPLACE INTO station_summaries
            (station_id, total_years, total_snowfall_mm, avg_annual_snowfall_mm,
             max_daily_snowfall_mm, max_daily_snowfall_date, max_snow_depth_mm,
             max_snow_depth_date, days_with_snow, first_snow_date, last_snow_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                station_id, years, total_snow, avg_annual,
                max_daily, max_date, max_depth, max_depth_date,
                days_with_snow, first_snow, last_snow
            ))

    conn.commit()
    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("COLLECTION COMPLETE!")
    logger.info("=" * 80)
    logger.info(f"Total Locations: {total_stations}")
    logger.info(f"Total Records: {total_records:,}")
    logger.info(f"Date Range: 1940 - {datetime.now().year}")
    logger.info(f"Years of Data: {datetime.now().year - 1940}")
    logger.info(f"Database: {db_path}")
    logger.info(f"Database Size: {Path(db_path).stat().st_size / (1024*1024):.1f} MB")
    logger.info("")

    return total_stations, total_records


if __name__ == "__main__":
    print("=" * 80)
    print("FULL HISTORICAL SNOWFALL COLLECTION")
    print("=" * 80)
    print()
    print("This will collect 85 YEARS of data (1940-2025)")
    print("for all 9 Northern Wisconsin locations.")
    print()
    print("⏱️  This will take 3-5 minutes to complete.")
    print("💾 Database size will be ~25-30 MB")
    print()

    input("Press ENTER to start collection...")
    print()

    db_path = "./northwoods_full_history.db"
    stations, records = collect_full_history(db_path)

    print()
    print(f"✅ Success! Collected {records:,} records from {stations} locations")
    print(f"📊 That's 85 years of snowfall history!")
    print()
    print("Next: Run ./venv/bin/python analyze_northwoods.py to analyze trends")
    print()
