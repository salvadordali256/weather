"""
Upper Peninsula Michigan Snowfall Data Collection
==================================================

Collect historical snowfall data for U.P. Michigan including:
- Watersmeet, MI
- Ironwood, MI
- Bergland, MI
- Other nearby U.P. locations

This is the heavy lake-effect snow belt from Lake Superior.
"""

import sys
import sqlite3
import logging
import pandas as pd
from datetime import datetime, timedelta
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

    # Stations table
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

    # Daily snowfall data
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

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_station_date ON snowfall_daily(station_id, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON snowfall_daily(date)")

    # Station summaries
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


def collect_up_data(db_path: str, years_back: int = 85):
    """
    Collect snowfall data for Upper Peninsula Michigan

    Args:
        db_path: Path to database file
        years_back: How many years of historical data to collect (default: 85)
    """

    # Calculate date range - Open-Meteo historical goes back to 1940
    end_date = datetime.now().date()
    start_date = datetime(1940, 1, 1).date()  # Maximum historical range

    logger.info("=" * 80)
    logger.info("UPPER PENINSULA MICHIGAN SNOWFALL DATA COLLECTION")
    logger.info("=" * 80)
    logger.info(f"Date Range: {start_date} to {end_date} (~{years_back} years)")
    logger.info(f"Database: {db_path}")
    logger.info("Source: Open-Meteo Historical API")
    logger.info("")

    # Create database
    create_database(db_path)

    # Initialize Open-Meteo fetcher
    fetcher = OpenMeteoWeatherFetcher()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Upper Peninsula Michigan locations
    # Focus on the western U.P. heavy snow belt
    locations = [
        # Gogebic County (heaviest snow in U.P.)
        {"name": "Watersmeet, MI", "lat": 46.2694, "lon": -89.1807, "county": "Gogebic"},
        {"name": "Ironwood, MI", "lat": 46.4547, "lon": -90.1710, "county": "Gogebic"},
        {"name": "Bergland, MI", "lat": 46.5878, "lon": -89.5489, "county": "Gogebic"},

        # Ontonagon County (Lake Superior snow belt)
        {"name": "Ontonagon, MI", "lat": 46.8708, "lon": -89.3143, "county": "Ontonagon"},

        # Houghton County
        {"name": "Houghton, MI", "lat": 47.1186, "lon": -88.5694, "county": "Houghton"},

        # Iron County
        {"name": "Iron River, MI", "lat": 46.0981, "lon": -88.6432, "county": "Iron"},
        {"name": "Crystal Falls, MI", "lat": 46.0977, "lon": -88.3337, "county": "Iron"},
    ]

    logger.info(f"Collecting data from {len(locations)} U.P. Michigan locations")
    logger.info(f"Counties: Gogebic, Ontonagon, Houghton, Iron")
    logger.info("")

    total_stations = 0
    total_records = 0

    for location in locations:
        station_id = f"MI_{location['name'].replace(', MI', '').replace(' ', '_')}"
        logger.info(f"Processing {location['name']} ({location['county']} County)...")

        try:
            # Save station info
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
                None,  # Elevation from Open-Meteo
                "Michigan",
                "USA",
                str(start_date),
                str(end_date),
                "Open-Meteo Historical"
            ))

            # Get historical weather data
            result = fetcher.fetch_historical_data(
                latitude=location['lat'],
                longitude=location['lon'],
                start_date=str(start_date),
                end_date=str(end_date),
                daily_params=[
                    "snowfall_sum",
                    "snow_depth_mean",
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum"
                ]
            )

            # Extract daily DataFrame
            if result and 'daily' in result:
                daily_df = result['daily']

                if not daily_df.empty:
                    # Iterate through the DataFrame
                    for idx, row in daily_df.iterrows():
                        date = row.get('time', idx)
                        snowfall_cm = row.get('snowfall_sum')
                        snow_depth_cm = row.get('snow_depth_mean')
                        temp_max = row.get('temperature_2m_max')
                        temp_min = row.get('temperature_2m_min')
                        precip_mm = row.get('precipitation_sum')

                        # Convert cm to mm for consistency
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

                    logger.info(f"  ✓ Collected {len(daily_df)} days of data")
                    total_stations += 1
                    conn.commit()

        except Exception as e:
            logger.warning(f"  ✗ Failed: {e}")
            continue

    # Calculate summary statistics for each station
    logger.info("")
    logger.info("Calculating summary statistics...")

    for location in locations:
        station_id = f"MI_{location['name'].replace(', MI', '').replace(' ', '_')}"

        cursor.execute("""
        SELECT
            COUNT(DISTINCT strftime('%Y', date)) as years,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) as total_snow,
            MAX(snowfall_mm) as max_daily,
            COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as days_with_snow
        FROM snowfall_daily
        WHERE station_id = ?
        """, (station_id,))

        result = cursor.fetchone()
        if result and result[0] > 0:
            years, total_snow, max_daily, days_with_snow = result
            avg_annual = total_snow / years if years > 0 else 0

            # Get max daily snowfall date
            cursor.execute("""
            SELECT date FROM snowfall_daily
            WHERE station_id = ? AND snowfall_mm = ?
            LIMIT 1
            """, (station_id, max_daily))
            max_result = cursor.fetchone()
            max_date = max_result[0] if max_result else None

            # Get first and last snow dates
            cursor.execute("""
            SELECT MIN(date), MAX(date)
            FROM snowfall_daily
            WHERE station_id = ? AND snowfall_mm > 0
            """, (station_id,))
            first_snow, last_snow = cursor.fetchone()

            cursor.execute("""
            INSERT OR REPLACE INTO station_summaries
            (station_id, total_years, total_snowfall_mm, avg_annual_snowfall_mm,
             max_daily_snowfall_mm, max_daily_snowfall_date, days_with_snow,
             first_snow_date, last_snow_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                station_id,
                years,
                total_snow,
                avg_annual,
                max_daily,
                max_date,
                days_with_snow,
                first_snow,
                last_snow
            ))

    conn.commit()
    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("COLLECTION COMPLETE!")
    logger.info("=" * 80)
    logger.info(f"Total Locations: {total_stations}")
    logger.info(f"Total Records: {total_records}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Database: {db_path}")
    logger.info("")

    return total_stations, total_records


if __name__ == "__main__":
    db_path = "./up_michigan_snowfall.db"
    stations, records = collect_up_data(db_path, years_back=85)
    print(f"\n✅ Success! Collected {records:,} records from {stations} U.P. Michigan locations")
