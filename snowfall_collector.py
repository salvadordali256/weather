"""
Snowfall Data Collector for US & Canada
========================================

Specialized collector for historical snowfall data spanning 100+ years.
Focuses on:
  - SNOW: Daily snowfall (mm)
  - SNWD: Snow depth (mm)
  - Coverage: United States and Canada
  - Period: 1920-present (100+ years)

Data is stored in SQLite database for easy querying and portability.
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm
import time
import json

from noaa_weather_fetcher import NOAAWeatherFetcher
from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SnowfallDatabase:
    """SQLite database for snowfall data"""

    def __init__(self, db_path: str = "./snowfall_data.db"):
        self.db_path = db_path
        self.conn = None
        self.init_database()

    def init_database(self):
        """Initialize database with schema"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()

        # Create stations table
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
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Create snowfall measurements table
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
            FOREIGN KEY (station_id) REFERENCES stations(station_id),
            UNIQUE(station_id, date)
        )
        """)

        # Create indexes for fast queries
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_snowfall_date
        ON snowfall_daily(date)
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_snowfall_station_date
        ON snowfall_daily(station_id, date)
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_snowfall_station
        ON snowfall_daily(station_id)
        """)

        # Create summary statistics table
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
            days_with_snow INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_id) REFERENCES stations(station_id)
        )
        """)

        # Create metadata table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        self.conn.commit()
        logger.info(f"Database initialized at {self.db_path}")

    def add_station(self, station_info: Dict):
        """Add or update station information"""
        cursor = self.conn.cursor()

        cursor.execute("""
        INSERT OR REPLACE INTO stations
        (station_id, name, latitude, longitude, elevation, state, country,
         min_date, max_date, data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            station_info.get('station_id'),
            station_info.get('name'),
            station_info.get('latitude'),
            station_info.get('longitude'),
            station_info.get('elevation'),
            station_info.get('state'),
            station_info.get('country'),
            station_info.get('min_date'),
            station_info.get('max_date'),
            station_info.get('data_source', 'NOAA')
        ))

        self.conn.commit()

    def add_snowfall_data(self, df: pd.DataFrame, station_id: str):
        """Add snowfall measurements from DataFrame"""

        # Prepare data
        records = []
        for _, row in df.iterrows():
            records.append({
                'station_id': station_id,
                'date': row.get('date'),
                'snowfall_mm': row.get('SNOW'),
                'snow_depth_mm': row.get('SNWD'),
                'temp_max_celsius': row.get('TMAX', 0) / 10.0 if pd.notna(row.get('TMAX')) else None,
                'temp_min_celsius': row.get('TMIN', 0) / 10.0 if pd.notna(row.get('TMIN')) else None,
                'precipitation_mm': row.get('PRCP', 0) / 10.0 if pd.notna(row.get('PRCP')) else None,
                'data_quality': 'good'
            })

        # Batch insert
        cursor = self.conn.cursor()

        for record in records:
            cursor.execute("""
            INSERT OR REPLACE INTO snowfall_daily
            (station_id, date, snowfall_mm, snow_depth_mm,
             temp_max_celsius, temp_min_celsius, precipitation_mm, data_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record['station_id'],
                record['date'],
                record['snowfall_mm'],
                record['snow_depth_mm'],
                record['temp_max_celsius'],
                record['temp_min_celsius'],
                record['precipitation_mm'],
                record['data_quality']
            ))

        self.conn.commit()
        logger.info(f"Added {len(records)} records for {station_id}")

    def update_station_summary(self, station_id: str):
        """Calculate and update station summary statistics"""
        cursor = self.conn.cursor()

        cursor.execute("""
        INSERT OR REPLACE INTO station_summaries
        SELECT
            station_id,
            COUNT(DISTINCT strftime('%Y', date)) as total_years,
            SUM(snowfall_mm) as total_snowfall_mm,
            AVG(yearly_total) as avg_annual_snowfall_mm,
            MAX(snowfall_mm) as max_daily_snowfall_mm,
            (SELECT date FROM snowfall_daily WHERE station_id = ?
             ORDER BY snowfall_mm DESC LIMIT 1) as max_daily_snowfall_date,
            MAX(snow_depth_mm) as max_snow_depth_mm,
            (SELECT date FROM snowfall_daily WHERE station_id = ?
             ORDER BY snow_depth_mm DESC LIMIT 1) as max_snow_depth_date,
            MIN(CASE WHEN snowfall_mm > 0 THEN date END) as first_snow_date,
            MAX(CASE WHEN snowfall_mm > 0 THEN date END) as last_snow_date,
            SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
            CURRENT_TIMESTAMP
        FROM (
            SELECT
                station_id,
                date,
                snowfall_mm,
                snow_depth_mm,
                SUM(snowfall_mm) OVER (
                    PARTITION BY strftime('%Y', date)
                ) as yearly_total
            FROM snowfall_daily
            WHERE station_id = ?
        )
        WHERE station_id = ?
        """, (station_id, station_id, station_id, station_id))

        self.conn.commit()

    def get_station_count(self) -> int:
        """Get total number of stations"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stations")
        return cursor.fetchone()[0]

    def get_total_records(self) -> int:
        """Get total number of snowfall records"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM snowfall_daily")
        return cursor.fetchone()[0]

    def export_to_csv(self, output_dir: str = "./snowfall_exports"):
        """Export data to CSV files"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        # Export stations
        df_stations = pd.read_sql("SELECT * FROM stations", self.conn)
        df_stations.to_csv(output_path / "stations.csv", index=False)

        # Export summaries
        df_summaries = pd.read_sql("SELECT * FROM station_summaries", self.conn)
        df_summaries.to_csv(output_path / "station_summaries.csv", index=False)

        logger.info(f"Exported data to {output_path}")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


class SnowfallCollector:
    """Collects snowfall data for US and Canada"""

    # US States with significant snowfall (FIPS codes)
    US_SNOW_STATES = {
        "02": "Alaska", "04": "Arizona", "06": "California", "08": "Colorado",
        "09": "Connecticut", "16": "Idaho", "17": "Illinois", "18": "Indiana",
        "19": "Iowa", "20": "Kansas", "23": "Maine", "24": "Maryland",
        "25": "Massachusetts", "26": "Michigan", "27": "Minnesota", "28": "Mississippi",
        "29": "Missouri", "30": "Montana", "31": "Nebraska", "32": "Nevada",
        "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico", "36": "New York",
        "38": "North Dakota", "39": "Ohio", "41": "Oregon", "42": "Pennsylvania",
        "44": "Rhode Island", "46": "South Dakota", "47": "Tennessee", "49": "Utah",
        "50": "Vermont", "51": "Virginia", "53": "Washington", "54": "West Virginia",
        "55": "Wisconsin", "56": "Wyoming"
    }

    # Major Canadian cities (Open-Meteo)
    CANADIAN_CITIES = [
        ("Toronto_ON", 43.6532, -79.3832),
        ("Montreal_QC", 45.5017, -73.5673),
        ("Vancouver_BC", 49.2827, -123.1207),
        ("Calgary_AB", 51.0447, -114.0719),
        ("Edmonton_AB", 53.5461, -113.4938),
        ("Ottawa_ON", 45.4215, -75.6972),
        ("Quebec_City_QC", 46.8139, -71.2080),
        ("Winnipeg_MB", 49.8951, -97.1384),
        ("Halifax_NS", 44.6488, -63.5752),
        ("Regina_SK", 50.4452, -104.6189),
        ("Saskatoon_SK", 52.1332, -106.6700),
        ("St_Johns_NL", 47.5615, -52.7126),
        ("Thunder_Bay_ON", 48.3809, -89.2477),
        ("Whitehorse_YT", 60.7212, -135.0568),
        ("Yellowknife_NT", 62.4540, -114.3718)
    ]

    def __init__(self, noaa_api_token: str, db_path: str = "./snowfall_data.db"):
        self.noaa_fetcher = NOAAWeatherFetcher(
            noaa_api_token,
            storage_path="./temp_snowfall"
        )
        self.openmeteo_fetcher = OpenMeteoWeatherFetcher(
            storage_path="./temp_snowfall"
        )
        self.db = SnowfallDatabase(db_path)

        # Progress tracking
        self.progress_file = Path("snowfall_progress.json")
        self.load_progress()

    def load_progress(self):
        """Load progress from previous runs"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                self.progress = json.load(f)
        else:
            self.progress = {
                "completed_stations": [],
                "failed_stations": [],
                "last_run": None
            }

    def save_progress(self):
        """Save progress"""
        self.progress["last_run"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)

    def collect_us_snowfall(self,
                           states: Optional[List[str]] = None,
                           start_date: str = "1920-01-01",
                           max_stations_per_state: int = 100):
        """
        Collect snowfall data for US states

        Args:
            states: List of state FIPS codes (None for all snow states)
            start_date: Start date (YYYY-MM-DD)
            max_stations_per_state: Max stations per state
        """
        if states is None:
            states = list(self.US_SNOW_STATES.keys())

        end_date = datetime.now().strftime("%Y-%m-%d")

        total_stations = 0
        successful = 0
        failed = 0

        for state_fips in tqdm(states, desc="Processing US states"):
            state_name = self.US_SNOW_STATES.get(state_fips, f"State_{state_fips}")

            logger.info(f"\n{'='*80}")
            logger.info(f"Processing {state_name}...")
            logger.info(f"{'='*80}")

            # Find stations with snowfall data
            stations = self.noaa_fetcher.find_stations(
                locationid=f"FIPS:{state_fips}",
                datatypeid=["SNOW"],
                limit=max_stations_per_state
            )

            if stations.empty:
                logger.warning(f"No snowfall stations found for {state_name}")
                continue

            total_stations += len(stations)
            logger.info(f"Found {len(stations)} stations in {state_name}")

            # Process each station
            for idx, station in stations.iterrows():
                station_id = station['id']

                # Skip if already processed
                if station_id in self.progress["completed_stations"]:
                    logger.info(f"Skipping {station_id} (already processed)")
                    continue

                try:
                    logger.info(f"Fetching {station_id}...")

                    # Fetch snowfall data
                    data = self.noaa_fetcher.fetch_date_range_chunked(
                        station_id,
                        start_date,
                        end_date,
                        chunk_days=365
                    )

                    if not data.empty:
                        # Filter for records with snowfall
                        if 'SNOW' in data.columns:
                            snow_records = data[data['SNOW'].notna()]

                            if len(snow_records) > 0:
                                # Add station info
                                station_info = {
                                    'station_id': station_id,
                                    'name': station.get('name', 'Unknown'),
                                    'latitude': station.get('latitude'),
                                    'longitude': station.get('longitude'),
                                    'elevation': station.get('elevation'),
                                    'state': state_name,
                                    'country': 'USA',
                                    'min_date': station.get('mindate'),
                                    'max_date': station.get('maxdate'),
                                    'data_source': 'NOAA'
                                }

                                self.db.add_station(station_info)
                                self.db.add_snowfall_data(data, station_id)
                                self.db.update_station_summary(station_id)

                                successful += 1
                                self.progress["completed_stations"].append(station_id)
                                self.save_progress()

                                logger.info(f"✓ {station_id}: {len(snow_records)} snowfall records")
                            else:
                                logger.info(f"○ {station_id}: No snowfall data")
                        else:
                            logger.info(f"○ {station_id}: SNOW column not found")
                    else:
                        logger.warning(f"○ {station_id}: No data returned")

                except Exception as e:
                    logger.error(f"✗ {station_id}: {e}")
                    failed += 1
                    self.progress["failed_stations"].append({
                        "station_id": station_id,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                    self.save_progress()

                # Rate limiting
                time.sleep(0.3)

        logger.info(f"\n{'='*80}")
        logger.info("US Collection Summary")
        logger.info(f"{'='*80}")
        logger.info(f"Total stations found: {total_stations}")
        logger.info(f"Successfully processed: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Database has {self.db.get_total_records()} total records")

    def collect_canada_snowfall(self,
                                start_date: str = "1940-01-01"):
        """
        Collect snowfall data for Canadian cities using Open-Meteo

        Args:
            start_date: Start date (YYYY-MM-DD)
        """
        end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"\n{'='*80}")
        logger.info("Collecting Canadian Snowfall Data")
        logger.info(f"{'='*80}")

        for city_name, lat, lon in tqdm(self.CANADIAN_CITIES, desc="Canadian cities"):
            logger.info(f"Fetching {city_name}...")

            try:
                # Fetch data
                data = self.openmeteo_fetcher.fetch_date_range_chunked(
                    lat, lon,
                    start_date, end_date,
                    chunk_years=10,
                    daily_params=[
                        "temperature_2m_max",
                        "temperature_2m_min",
                        "precipitation_sum",
                        "snowfall_sum",
                        "snow_depth_mean"
                    ],
                    hourly_params=None  # Daily only for Canada
                )

                if "daily" in data and not data["daily"].empty:
                    df = data["daily"].copy()

                    # Rename columns to match database schema
                    df = df.rename(columns={
                        'temperature_2m_max': 'TMAX',
                        'temperature_2m_min': 'TMIN',
                        'precipitation_sum': 'PRCP',
                        'snowfall_sum': 'SNOW',
                        'snow_depth_mean': 'SNWD'
                    })

                    # Convert temperature to tenths of Celsius (to match NOAA)
                    df['TMAX'] = df['TMAX'] * 10
                    df['TMIN'] = df['TMIN'] * 10
                    df['PRCP'] = df['PRCP'] * 10

                    # Rename time to date
                    df = df.rename(columns={'time': 'date'})

                    # Add station info
                    station_id = f"OPENMETEO_{city_name}"
                    station_info = {
                        'station_id': station_id,
                        'name': city_name.replace('_', ' '),
                        'latitude': lat,
                        'longitude': lon,
                        'elevation': data.get('metadata', {}).get('elevation'),
                        'state': city_name.split('_')[-1],  # Province code
                        'country': 'Canada',
                        'min_date': df['date'].min(),
                        'max_date': df['date'].max(),
                        'data_source': 'Open-Meteo'
                    }

                    self.db.add_station(station_info)
                    self.db.add_snowfall_data(df, station_id)
                    self.db.update_station_summary(station_id)

                    logger.info(f"✓ {city_name}: {len(df)} records")

            except Exception as e:
                logger.error(f"✗ {city_name}: {e}")

        logger.info(f"Database now has {self.db.get_total_records()} total records")

    def generate_report(self):
        """Generate collection report"""
        report = []
        report.append("=" * 80)
        report.append("SNOWFALL DATA COLLECTION REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Database: {self.db.db_path}")
        report.append("")
        report.append(f"Total Stations: {self.db.get_station_count()}")
        report.append(f"Total Records: {self.db.get_total_records():,}")
        report.append(f"Completed Stations: {len(self.progress['completed_stations'])}")
        report.append(f"Failed Stations: {len(self.progress['failed_stations'])}")
        report.append("")

        # Get some statistics
        cursor = self.db.conn.cursor()

        # Date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM snowfall_daily")
        min_date, max_date = cursor.fetchone()
        report.append(f"Date Range: {min_date} to {max_date}")

        # Top snowfall stations
        report.append("")
        report.append("Top 10 Snowiest Stations (Total Snowfall):")
        report.append("-" * 80)
        cursor.execute("""
        SELECT s.name, s.state, s.country, ss.total_snowfall_mm / 1000.0 as total_m
        FROM station_summaries ss
        JOIN stations s ON ss.station_id = s.station_id
        ORDER BY ss.total_snowfall_mm DESC
        LIMIT 10
        """)

        for row in cursor.fetchall():
            name, state, country, total_m = row
            report.append(f"  {name}, {state}, {country}: {total_m:,.1f} meters")

        report_text = "\n".join(report)
        print(report_text)

        # Save report
        with open("snowfall_collection_report.txt", 'w') as f:
            f.write(report_text)

        return report_text


def main():
    """Main collection script"""

    # Configuration
    NOAA_API_TOKEN = "YOUR_NOAA_TOKEN_HERE"  # Replace with your token
    DB_PATH = "/path/to/network/storage/snowfall_data.db"  # Use network storage path

    # Initialize collector
    collector = SnowfallCollector(NOAA_API_TOKEN, DB_PATH)

    # Collect US snowfall data (start with a few states for testing)
    logger.info("Starting US snowfall collection...")
    collector.collect_us_snowfall(
        states=["08", "30", "56"],  # Colorado, Montana, Wyoming for testing
        start_date="1920-01-01",
        max_stations_per_state=50
    )

    # Collect Canadian data
    logger.info("Starting Canadian snowfall collection...")
    collector.collect_canada_snowfall(start_date="1940-01-01")

    # Generate report
    collector.generate_report()

    # Export to CSV
    collector.db.export_to_csv("./snowfall_exports")

    # Close database
    collector.db.close()

    logger.info("Collection complete!")


if __name__ == "__main__":
    main()
