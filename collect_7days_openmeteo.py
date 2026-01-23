"""
Quick 7-Day Snowfall Data Collection (Open-Meteo - No API Key Required!)
=========================================================================

Collects snowfall data for the past 7 days using Open-Meteo API.
No API token needed!
"""

import sys
import sqlite3
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path
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

    conn.commit()
    conn.close()
    logger.info(f"✓ Database created: {db_path}")


def collect_7day_data(db_path: str):
    """Collect last 7 days of snowfall data from major ski areas and snowy cities"""

    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)

    logger.info("=" * 80)
    logger.info("7-DAY SNOWFALL DATA COLLECTION (Open-Meteo)")
    logger.info("=" * 80)
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Database: {db_path}")
    logger.info("Source: Open-Meteo API (no token required!)")
    logger.info("")

    # Create database
    create_database(db_path)

    # Initialize Open-Meteo fetcher
    fetcher = OpenMeteoWeatherFetcher()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Major snowy locations across US and Canada
    locations = [
        # Colorado Ski Areas
        {"name": "Aspen, CO", "lat": 39.1911, "lon": -106.8175, "state": "Colorado", "country": "USA"},
        {"name": "Vail, CO", "lat": 39.6403, "lon": -106.3742, "state": "Colorado", "country": "USA"},
        {"name": "Breckenridge, CO", "lat": 39.4817, "lon": -106.0384, "state": "Colorado", "country": "USA"},
        {"name": "Steamboat Springs, CO", "lat": 40.4850, "lon": -106.8317, "state": "Colorado", "country": "USA"},

        # Wyoming
        {"name": "Jackson Hole, WY", "lat": 43.4799, "lon": -110.7624, "state": "Wyoming", "country": "USA"},

        # Montana
        {"name": "Big Sky, MT", "lat": 45.2847, "lon": -111.3973, "state": "Montana", "country": "USA"},
        {"name": "Whitefish, MT", "lat": 48.4111, "lon": -114.3377, "state": "Montana", "country": "USA"},

        # California
        {"name": "Lake Tahoe, CA", "lat": 39.0968, "lon": -120.0324, "state": "California", "country": "USA"},
        {"name": "Mammoth Lakes, CA", "lat": 37.6485, "lon": -118.9721, "state": "California", "country": "USA"},

        # Washington
        {"name": "Stevens Pass, WA", "lat": 47.7452, "lon": -121.0890, "state": "Washington", "country": "USA"},

        # Utah
        {"name": "Park City, UT", "lat": 40.6461, "lon": -111.4980, "state": "Utah", "country": "USA"},
        {"name": "Alta, UT", "lat": 40.5885, "lon": -111.6377, "state": "Utah", "country": "USA"},

        # Vermont
        {"name": "Stowe, VT", "lat": 44.4654, "lon": -72.6874, "state": "Vermont", "country": "USA"},

        # New York
        {"name": "Lake Placid, NY", "lat": 44.2795, "lon": -73.9799, "state": "New York", "country": "USA"},

        # Canada - Alberta
        {"name": "Banff, AB", "lat": 51.1784, "lon": -115.5708, "state": "Alberta", "country": "Canada"},
        {"name": "Lake Louise, AB", "lat": 51.4254, "lon": -116.1773, "state": "Alberta", "country": "Canada"},

        # Canada - British Columbia
        {"name": "Whistler, BC", "lat": 50.1163, "lon": -122.9574, "state": "British Columbia", "country": "Canada"},
        {"name": "Revelstoke, BC", "lat": 50.9981, "lon": -118.1957, "state": "British Columbia", "country": "Canada"},

        # Canada - Quebec
        {"name": "Mont-Tremblant, QC", "lat": 46.1184, "lon": -74.5960, "state": "Quebec", "country": "Canada"},
    ]

    logger.info(f"Collecting data from {len(locations)} locations")
    logger.info("")

    total_stations = 0
    total_records = 0

    for location in locations:
        station_id = f"OM_{location['name'].replace(', ', '_').replace(' ', '_')}"
        logger.info(f"Processing {location['name']}...")

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
                None,  # Elevation not provided by Open-Meteo for free
                location['state'],
                location['country'],
                str(start_date),
                str(end_date),
                "Open-Meteo"
            ))

            # Get historical weather data
            result = fetcher.fetch_historical_data(
                latitude=location['lat'],
                longitude=location['lon'],
                start_date=str(start_date),
                end_date=str(end_date),
                daily_params=[
                    "snowfall_sum",
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
                        temp_max = row.get('temperature_2m_max')
                        temp_min = row.get('temperature_2m_min')
                        precip_mm = row.get('precipitation_sum')

                        # Convert cm to mm for consistency
                        snowfall_mm = snowfall_cm * 10 if pd.notna(snowfall_cm) else None

                        cursor.execute("""
                        INSERT OR REPLACE INTO snowfall_daily
                        (station_id, date, snowfall_mm, snow_depth_mm,
                         temp_max_celsius, temp_min_celsius, precipitation_mm, data_quality)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            station_id,
                            str(date),
                            snowfall_mm,
                            None,  # snow_depth not in daily params
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

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("COLLECTION COMPLETE!")
    logger.info("=" * 80)
    logger.info(f"Total Locations: {total_stations}")
    logger.info(f"Total Records: {total_records}")
    logger.info(f"Database: {db_path}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. View data: sqlite3 snowfall_7day.db")
    logger.info("  2. Analyze with DuckDB: ./venv/bin/python -c \"from snowfall_duckdb import SnowfallDuckDB; engine = SnowfallDuckDB('./snowfall_7day.db'); print(engine.snowfall_by_state())\"")
    logger.info("")

    return total_stations, total_records


def main():
    """Main entry point"""

    print("=" * 80)
    print("7-DAY SNOWFALL DATA COLLECTOR")
    print("=" * 80)
    print()
    print("Using Open-Meteo API (no token required!)")
    print()
    print("Collecting from 19 major ski areas and snowy cities:")
    print("  - US: Colorado, Wyoming, Montana, California, Utah, Vermont, New York")
    print("  - Canada: Alberta, British Columbia, Quebec")
    print()

    db_path = "./snowfall_7day.db"
    print(f"Database: {db_path}")
    print()

    input("Press ENTER to start collection...")
    print()

    # Run collection
    stations, records = collect_7day_data(db_path)

    print()
    print(f"✅ Success! Collected {records} records from {stations} locations")
    print()


if __name__ == "__main__":
    main()
