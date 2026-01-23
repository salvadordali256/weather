"""
Quick 7-Day Snowfall Data Collection
====================================

Collects snowfall data for the past 7 days from select snowy states.
This is a quick test to verify the data collection system works.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from noaa_weather_fetcher import NOAAWeatherFetcher
from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher
import sqlite3
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
    logger.info(f"Database created: {db_path}")


def collect_7day_data(api_token: str, db_path: str):
    """Collect last 7 days of snowfall data"""

    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)

    logger.info("=" * 80)
    logger.info("7-DAY SNOWFALL DATA COLLECTION")
    logger.info("=" * 80)
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Database: {db_path}")
    logger.info("")

    # Create database
    create_database(db_path)

    # Initialize NOAA fetcher
    noaa = NOAAWeatherFetcher(api_token)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Select snowy states for testing
    states = {
        "08": "Colorado",
        "30": "Montana",
        "56": "Wyoming",
        "06": "California",
        "53": "Washington"
    }

    logger.info(f"Collecting data from {len(states)} states: {', '.join(states.values())}")
    logger.info("")

    total_stations = 0
    total_records = 0

    for state_code, state_name in states.items():
        logger.info(f"Processing {state_name} (state code: {state_code})...")

        try:
            # Find stations in this state
            stations = noaa.find_stations(
                datatypeid=["SNOW", "SNWD"],
                locationid=f"FIPS:{state_code}",
                startdate=str(start_date),
                enddate=str(end_date),
                limit=10  # Limit to 10 stations per state for quick test
            )

            if not stations:
                logger.warning(f"  No stations found for {state_name}")
                continue

            logger.info(f"  Found {len(stations)} stations")

            for station in stations:
                station_id = station['id']

                # Save station info
                cursor.execute("""
                INSERT OR REPLACE INTO stations
                (station_id, name, latitude, longitude, elevation, state, country,
                 min_date, max_date, data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    station_id,
                    station.get('name', 'Unknown'),
                    station.get('latitude'),
                    station.get('longitude'),
                    station.get('elevation'),
                    state_name,
                    "USA",
                    str(start_date),
                    str(end_date),
                    "NOAA"
                ))

                # Get snowfall data
                try:
                    data = noaa.get_data(
                        datasetid="GHCND",
                        datatypeid=["SNOW", "SNWD", "TMAX", "TMIN", "PRCP"],
                        stationid=station_id,
                        startdate=str(start_date),
                        enddate=str(end_date),
                        limit=1000
                    )

                    if data:
                        # Organize by date
                        daily_data = {}
                        for record in data:
                            date = record['date'][:10]  # YYYY-MM-DD
                            if date not in daily_data:
                                daily_data[date] = {}

                            datatype = record['datatype']
                            value = record['value']

                            if datatype == 'SNOW':
                                daily_data[date]['snowfall_mm'] = value
                            elif datatype == 'SNWD':
                                daily_data[date]['snow_depth_mm'] = value
                            elif datatype == 'TMAX':
                                daily_data[date]['temp_max_celsius'] = value / 10.0
                            elif datatype == 'TMIN':
                                daily_data[date]['temp_min_celsius'] = value / 10.0
                            elif datatype == 'PRCP':
                                daily_data[date]['precipitation_mm'] = value / 10.0

                        # Insert daily records
                        for date, values in daily_data.items():
                            cursor.execute("""
                            INSERT OR REPLACE INTO snowfall_daily
                            (station_id, date, snowfall_mm, snow_depth_mm,
                             temp_max_celsius, temp_min_celsius, precipitation_mm, data_quality)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                station_id,
                                date,
                                values.get('snowfall_mm'),
                                values.get('snow_depth_mm'),
                                values.get('temp_max_celsius'),
                                values.get('temp_min_celsius'),
                                values.get('precipitation_mm'),
                                'good'
                            ))
                            total_records += 1

                        logger.info(f"    ✓ {station.get('name', station_id)}: {len(daily_data)} days")
                        total_stations += 1
                        conn.commit()

                except Exception as e:
                    logger.warning(f"    ✗ Failed to get data for {station_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"  Error processing {state_name}: {e}")
            continue

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("COLLECTION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total Stations: {total_stations}")
    logger.info(f"Total Records: {total_records}")
    logger.info(f"Database: {db_path}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Analyze with: ./venv/bin/python snowfall_duckdb.py")
    logger.info("  2. View in DuckDB for fast queries")
    logger.info("")


def main():
    """Main entry point"""

    print("=" * 80)
    print("7-DAY SNOWFALL DATA COLLECTOR")
    print("=" * 80)
    print()
    print("This script will collect snowfall data from the past 7 days")
    print("for select snowy states: Colorado, Montana, Wyoming, California, Washington")
    print()

    # Get NOAA API token
    api_token = input("Enter your NOAA API token: ").strip()

    if not api_token or api_token == "YOUR_NOAA_TOKEN_HERE":
        print()
        print("❌ Error: Please provide a valid NOAA API token")
        print()
        print("Get a free token at: https://www.ncdc.noaa.gov/cdo-web/token")
        print("(It takes about 5 minutes to receive via email)")
        return

    # Set database path
    db_path = "./snowfall_7day.db"

    print()
    print(f"Starting collection...")
    print(f"Database will be saved to: {db_path}")
    print()

    # Run collection
    collect_7day_data(api_token, db_path)


if __name__ == "__main__":
    main()
