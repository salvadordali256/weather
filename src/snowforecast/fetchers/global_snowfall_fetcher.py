#!/usr/bin/env python3
"""
Global Snowfall Data Fetcher
Collects worldwide snowfall data to analyze teleconnections and predict
snowfall patterns in Phelps and Land O'Lakes, Wisconsin.

Regions covered:
- North America: Western US (CA, CO ski resorts), Northern Wisconsin (target)
- Asia: Japan, China, Russia/Siberia
- Europe: Alps, Scandinavia (for comprehensive Northern Hemisphere coverage)

Uses Open-Meteo Historical Weather API (1940-present, global coverage)
"""

import requests
import sqlite3
import json
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import time
import os

# Global location grid organized by region and teleconnection significance
GLOBAL_LOCATIONS = {
    # ===== PRIMARY TARGET (Northern Wisconsin) =====
    "northern_wisconsin": [
        {"name": "Phelps, WI", "lat": 46.0638, "lon": -89.0787, "elevation": 518, "country": "USA", "significance": "Primary forecast target"},
        {"name": "Land O'Lakes, WI", "lat": 46.1535, "lon": -89.3207, "elevation": 518, "country": "USA", "significance": "Primary forecast target"},
        {"name": "Eagle River, WI", "lat": 45.9169, "lon": -89.2443, "elevation": 503, "country": "USA", "significance": "Regional reference station"},
    ],

    # ===== WESTERN NORTH AMERICA (Pacific Pattern Indicators) =====
    "california_ski": [
        {"name": "Mammoth Mountain, CA", "lat": 37.6308, "lon": -119.0326, "elevation": 2743, "country": "USA", "significance": "Sierra Nevada snowpack, atmospheric river indicator"},
        {"name": "Lake Tahoe (Squaw Valley), CA", "lat": 39.1969, "lon": -120.2356, "elevation": 1890, "country": "USA", "significance": "Northern Sierra, Pacific storm track"},
        {"name": "Mount Shasta, CA", "lat": 41.4092, "lon": -122.1949, "elevation": 2134, "country": "USA", "significance": "Cascades, northernmost CA ski region"},
        {"name": "Big Bear, CA", "lat": 34.2406, "lon": -116.9114, "elevation": 2134, "country": "USA", "significance": "Southern California patterns"},
    ],

    "colorado_rockies": [
        {"name": "Aspen, CO", "lat": 39.1911, "lon": -106.8175, "elevation": 2438, "country": "USA", "significance": "Central Rockies, upslope events"},
        {"name": "Vail, CO", "lat": 39.6403, "lon": -106.3742, "elevation": 2500, "country": "USA", "significance": "I-70 corridor, Front Range"},
        {"name": "Steamboat Springs, CO", "lat": 40.4850, "lon": -106.8317, "elevation": 2042, "country": "USA", "significance": "Northern Colorado, 'Champagne Powder'"},
        {"name": "Telluride, CO", "lat": 37.9375, "lon": -107.8123, "elevation": 2667, "country": "USA", "significance": "Southwest Colorado, San Juan Mountains"},
        {"name": "Breckenridge, CO", "lat": 39.4817, "lon": -106.0384, "elevation": 2926, "country": "USA", "significance": "High elevation, continental divide"},
    ],

    "pacific_northwest": [
        {"name": "Mount Baker, WA", "lat": 48.8568, "lon": -121.6714, "elevation": 1280, "country": "USA", "significance": "World record snowfall location, Pacific moisture"},
        {"name": "Stevens Pass, WA", "lat": 47.7465, "lon": -121.0890, "elevation": 1219, "country": "USA", "significance": "Cascade snowpack"},
        {"name": "Mount Hood, OR", "lat": 45.3306, "lon": -121.7081, "elevation": 1828, "country": "USA", "significance": "Oregon Cascades"},
    ],

    # ===== RUSSIA / SIBERIA (Cold Air Source Region) =====
    "siberia_west": [
        {"name": "Novosibirsk, Russia", "lat": 55.0084, "lon": 82.9357, "elevation": 150, "country": "Russia", "significance": "Western Siberia, continental cold source"},
        {"name": "Omsk, Russia", "lat": 54.9885, "lon": 73.3242, "elevation": 85, "country": "Russia", "significance": "West Siberian Plain"},
        {"name": "Tomsk, Russia", "lat": 56.4977, "lon": 84.9744, "elevation": 117, "country": "Russia", "significance": "Ob River region"},
    ],

    "siberia_central": [
        {"name": "Krasnoyarsk, Russia", "lat": 56.0153, "lon": 92.8932, "elevation": 140, "country": "Russia", "significance": "Central Siberia, Yenisei River"},
        {"name": "Irkutsk, Russia", "lat": 52.2978, "lon": 104.2964, "elevation": 440, "country": "Russia", "significance": "Lake Baikal region, moisture source"},
        {"name": "Yakutsk, Russia", "lat": 62.0355, "lon": 129.6755, "elevation": 100, "country": "Russia", "significance": "Coldest city on Earth, extreme continental cold"},
    ],

    "siberia_east": [
        {"name": "Magadan, Russia", "lat": 59.5686, "lon": 150.8103, "elevation": 118, "country": "Russia", "significance": "Sea of Okhotsk, Pacific influence"},
        {"name": "Kamchatka (Petropavlovsk), Russia", "lat": 53.0245, "lon": 158.6433, "elevation": 30, "country": "Russia", "significance": "Pacific volcanic peninsula, Aleutian Low"},
    ],

    "arctic_russia": [
        {"name": "Murmansk, Russia", "lat": 68.9585, "lon": 33.0827, "elevation": 50, "country": "Russia", "significance": "Arctic Europe, Kola Peninsula"},
        {"name": "Norilsk, Russia", "lat": 69.3558, "lon": 88.1893, "elevation": 30, "country": "Russia", "significance": "Arctic Siberia, Taymyr Peninsula"},
        {"name": "Tiksi, Russia", "lat": 71.6419, "lon": 128.8739, "elevation": 5, "country": "Russia", "significance": "Arctic Ocean coast, Laptev Sea"},
    ],

    # ===== JAPAN (East Asian Jet Stream Indicator) =====
    "japan_north": [
        {"name": "Sapporo, Japan", "lat": 43.0642, "lon": 141.3469, "elevation": 17, "country": "Japan", "significance": "Hokkaido, northernmost major city, heavy snowfall"},
        {"name": "Niseko, Japan", "lat": 42.8048, "lon": 140.6874, "elevation": 308, "country": "Japan", "significance": "World-class ski resort, 'Japow' powder"},
        {"name": "Asahikawa, Japan", "lat": 43.7706, "lon": 142.3650, "elevation": 112, "country": "Japan", "significance": "Central Hokkaido, continental influence"},
    ],

    "japan_sea_coast": [
        {"name": "Niigata, Japan", "lat": 37.9026, "lon": 139.0232, "elevation": 0, "country": "Japan", "significance": "Sea of Japan lake-effect snow, extreme snowfall"},
        {"name": "Toyama, Japan", "lat": 36.6959, "lon": 137.2137, "elevation": 7, "country": "Japan", "significance": "Japanese Alps, sea-effect convergence"},
        {"name": "Kanazawa, Japan", "lat": 36.5613, "lon": 136.6562, "elevation": 6, "country": "Japan", "significance": "Hokuriku region, winter monsoon"},
    ],

    "japan_mountains": [
        {"name": "Nagano (Japanese Alps), Japan", "lat": 36.6513, "lon": 138.1809, "elevation": 362, "country": "Japan", "significance": "1998 Olympics, mountain snowpack"},
        {"name": "Hakuba, Japan", "lat": 36.6996, "lon": 137.8616, "elevation": 760, "country": "Japan", "significance": "Northern Alps ski resort"},
    ],

    # ===== CHINA (Tibetan High & Continental Patterns) =====
    "china_northeast": [
        {"name": "Harbin, China", "lat": 45.8038, "lon": 126.5340, "elevation": 150, "country": "China", "significance": "Manchuria, Siberian cold influence, Ice Festival city"},
        {"name": "Changchun, China", "lat": 43.8171, "lon": 125.3235, "elevation": 237, "country": "China", "significance": "Northeast China industrial region"},
        {"name": "Shenyang, China", "lat": 41.8057, "lon": 123.4328, "elevation": 41, "country": "China", "significance": "Southern Manchuria"},
    ],

    "china_northwest": [
        {"name": "Urumqi, China", "lat": 43.8256, "lon": 87.6168, "elevation": 800, "country": "China", "significance": "Xinjiang, Tian Shan mountains, continental interior"},
        {"name": "Lanzhou, China", "lat": 36.0611, "lon": 103.8343, "elevation": 1520, "country": "China", "significance": "Yellow River upper basin"},
    ],

    "china_tibetan": [
        {"name": "Lhasa, Tibet", "lat": 29.6500, "lon": 91.1000, "elevation": 3656, "country": "China", "significance": "Tibetan Plateau, high-altitude snowpack, global circulation driver"},
        {"name": "Xining, China", "lat": 36.6171, "lon": 101.7782, "elevation": 2275, "country": "China", "significance": "Qinghai, Tibetan Plateau edge"},
    ],

    "china_north": [
        {"name": "Beijing, China", "lat": 39.9042, "lon": 116.4074, "elevation": 43, "country": "China", "significance": "North China Plain, 2022 Olympics"},
    ],

    # ===== EUROPE (Northern Hemisphere Completeness) =====
    "alps_western": [
        {"name": "Chamonix, France", "lat": 45.9237, "lon": 6.8694, "elevation": 1035, "country": "France", "significance": "Mont Blanc region, Atlantic influence"},
        {"name": "Zermatt, Switzerland", "lat": 46.0207, "lon": 7.7491, "elevation": 1620, "country": "Switzerland", "significance": "Matterhorn, central Alps"},
        {"name": "St. Moritz, Switzerland", "lat": 46.4908, "lon": 9.8355, "elevation": 1822, "country": "Switzerland", "significance": "Upper Engadine, dry continental"},
    ],

    "alps_eastern": [
        {"name": "Innsbruck, Austria", "lat": 47.2692, "lon": 11.4041, "elevation": 574, "country": "Austria", "significance": "Northern Alps, föhn winds"},
        {"name": "Cortina d'Ampezzo, Italy", "lat": 46.5369, "lon": 12.1357, "elevation": 1224, "country": "Italy", "significance": "Dolomites, 2026 Olympics"},
    ],

    "scandinavia": [
        {"name": "Tromsø, Norway", "lat": 69.6492, "lon": 18.9553, "elevation": 10, "country": "Norway", "significance": "Arctic Norway, polar low systems"},
        {"name": "Kiruna, Sweden", "lat": 67.8558, "lon": 20.2253, "elevation": 530, "country": "Sweden", "significance": "Arctic Sweden, northernmost town"},
        {"name": "Rovaniemi, Finland", "lat": 66.5039, "lon": 25.7294, "elevation": 117, "country": "Finland", "significance": "Arctic Circle, Lapland"},
    ],

    # ===== CANADA (Great Lakes & Arctic Influence) =====
    "canada_west": [
        {"name": "Whistler, BC", "lat": 50.1163, "lon": -122.9574, "elevation": 658, "country": "Canada", "significance": "Coastal mountains, 2010 Olympics"},
        {"name": "Revelstoke, BC", "lat": 50.9981, "lon": -118.1957, "elevation": 443, "country": "Canada", "significance": "Interior BC, deep powder"},
    ],

    "canada_rockies": [
        {"name": "Banff, AB", "lat": 51.1784, "lon": -115.5708, "elevation": 1383, "country": "Canada", "significance": "Canadian Rockies, continental divide"},
        {"name": "Lake Louise, AB", "lat": 51.4254, "lon": -116.1773, "elevation": 1646, "country": "Canada", "significance": "High Rockies"},
    ],

    "canada_east": [
        {"name": "Thunder Bay, ON", "lat": 48.3809, "lon": -89.2477, "elevation": 199, "country": "Canada", "significance": "Lake Superior lake-effect"},
        {"name": "Quebec City, QC", "lat": 46.8139, "lon": -71.2080, "elevation": 74, "country": "Canada", "significance": "St. Lawrence valley, nor'easters"},
    ],
}


class GlobalSnowfallFetcher:
    """Fetch and store global snowfall data using Open-Meteo Historical Weather API"""

    def __init__(self, db_path: str = "global_snowfall.db"):
        self.db_path = db_path
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.init_database()

    def init_database(self):
        """Initialize SQLite database with schema for global snowfall data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Stations/locations table with regional organization
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                elevation REAL,
                country TEXT,
                region TEXT,
                significance TEXT,
                data_source TEXT DEFAULT 'open-meteo',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Daily weather measurements
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snowfall_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                date TEXT NOT NULL,
                snowfall_mm REAL,
                snow_depth_mm REAL,
                temp_max_celsius REAL,
                temp_min_celsius REAL,
                temp_mean_celsius REAL,
                precipitation_mm REAL,
                pressure_msl REAL,
                wind_speed_max REAL,
                data_quality TEXT DEFAULT 'good',
                UNIQUE(station_id, date),
                FOREIGN KEY (station_id) REFERENCES stations(station_id)
            )
        """)

        # Indexes for fast queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snowfall_date ON snowfall_daily(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snowfall_station_date ON snowfall_daily(station_id, date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_station_region ON stations(region)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_station_country ON stations(country)")

        # Regional summaries for quick access
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS regional_summaries (
                region TEXT PRIMARY KEY,
                num_stations INTEGER,
                total_snowfall_mm REAL,
                avg_annual_snowfall_mm REAL,
                max_daily_snowfall_mm REAL,
                max_daily_date TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Cross-regional correlation cache
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS correlations (
                region_a TEXT,
                region_b TEXT,
                correlation_coefficient REAL,
                lag_days INTEGER,
                sample_size INTEGER,
                significance_p_value REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (region_a, region_b, lag_days)
            )
        """)

        conn.commit()
        conn.close()
        print(f"✓ Database initialized: {self.db_path}")

    def register_locations(self):
        """Register all global locations in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        total_added = 0
        for region_name, locations in GLOBAL_LOCATIONS.items():
            for loc in locations:
                station_id = f"{loc['name'].replace(' ', '_').replace(',', '')}_{loc['country']}"

                cursor.execute("""
                    INSERT OR REPLACE INTO stations
                    (station_id, name, latitude, longitude, elevation, country, region, significance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    station_id,
                    loc['name'],
                    loc['lat'],
                    loc['lon'],
                    loc.get('elevation'),
                    loc['country'],
                    region_name,
                    loc.get('significance', '')
                ))
                total_added += 1

        conn.commit()
        conn.close()
        print(f"✓ Registered {total_added} stations across {len(GLOBAL_LOCATIONS)} regions")

    def fetch_location_data(self, lat: float, lon: float, start_date: str, end_date: str) -> Dict:
        """
        Fetch historical weather data from Open-Meteo for a single location

        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dictionary with daily weather data
        """
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,snowfall_sum,snow_depth_mean,pressure_msl_mean,wind_speed_10m_max',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ API error for ({lat}, {lon}): {e}")
            return None

    def store_location_data(self, station_id: str, data: Dict):
        """Store fetched data in database"""
        if not data or 'daily' not in data:
            return 0

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        daily = data['daily']
        dates = daily.get('time', [])

        rows_inserted = 0
        for i, date in enumerate(dates):
            cursor.execute("""
                INSERT OR REPLACE INTO snowfall_daily
                (station_id, date, snowfall_mm, snow_depth_mm, temp_max_celsius, temp_min_celsius,
                 temp_mean_celsius, precipitation_mm, pressure_msl, wind_speed_max)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                station_id,
                date,
                daily.get('snowfall_sum', [None] * len(dates))[i],
                daily.get('snow_depth_mean', [None] * len(dates))[i],
                daily.get('temperature_2m_max', [None] * len(dates))[i],
                daily.get('temperature_2m_min', [None] * len(dates))[i],
                daily.get('temperature_2m_mean', [None] * len(dates))[i],
                daily.get('precipitation_sum', [None] * len(dates))[i],
                daily.get('pressure_msl_mean', [None] * len(dates))[i],
                daily.get('wind_speed_10m_max', [None] * len(dates))[i]
            ))
            rows_inserted += 1

        conn.commit()
        conn.close()
        return rows_inserted

    def collect_all_regions(self, start_date: str = "1940-01-01", end_date: str = None, rate_limit_delay: float = 1.0):
        """
        Collect historical data for all registered locations

        Args:
            start_date: Start date (default: 1940-01-01 for full historical record)
            end_date: End date (default: today)
            rate_limit_delay: Seconds to wait between API calls (default: 1.0)
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        print(f"\n{'='*80}")
        print(f"GLOBAL SNOWFALL DATA COLLECTION")
        print(f"{'='*80}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Regions: {len(GLOBAL_LOCATIONS)}")

        total_stations = sum(len(locs) for locs in GLOBAL_LOCATIONS.values())
        print(f"Locations: {total_stations}")
        print(f"{'='*80}\n")

        current_station = 0
        for region_name, locations in GLOBAL_LOCATIONS.items():
            print(f"\n[{region_name.upper().replace('_', ' ')}]")
            print(f"{'─'*80}")

            for loc in locations:
                current_station += 1
                station_id = f"{loc['name'].replace(' ', '_').replace(',', '')}_{loc['country']}"

                print(f"  [{current_station}/{total_stations}] {loc['name']}, {loc['country']}...", end=" ", flush=True)

                data = self.fetch_location_data(loc['lat'], loc['lon'], start_date, end_date)

                if data:
                    rows = self.store_location_data(station_id, data)
                    print(f"✓ {rows:,} days stored")
                else:
                    print(f"✗ Failed")

                # Rate limiting to be nice to the API
                time.sleep(rate_limit_delay)

        print(f"\n{'='*80}")
        print(f"✓ COLLECTION COMPLETE")
        print(f"{'='*80}\n")

    def get_station_count(self) -> Dict[str, int]:
        """Get count of stations by region"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT region, COUNT(*) as count
            FROM stations
            GROUP BY region
            ORDER BY count DESC
        """)

        results = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return results

    def get_data_summary(self) -> Dict:
        """Get summary statistics for collected data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM stations")
        total_stations = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM snowfall_daily")
        total_records = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(date), MAX(date) FROM snowfall_daily")
        date_range = cursor.fetchone()

        cursor.execute("""
            SELECT AVG(snowfall_mm), MAX(snowfall_mm), SUM(snowfall_mm)
            FROM snowfall_daily
            WHERE snowfall_mm IS NOT NULL
        """)
        snow_stats = cursor.fetchone()

        conn.close()

        return {
            'total_stations': total_stations,
            'total_records': total_records,
            'date_range': date_range,
            'avg_daily_snowfall_mm': snow_stats[0],
            'max_daily_snowfall_mm': snow_stats[1],
            'total_snowfall_mm': snow_stats[2]
        }


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Global Snowfall Data Fetcher')
    parser.add_argument('--db', default='global_snowfall.db', help='Database file path')
    parser.add_argument('--start', default='1940-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='End date (YYYY-MM-DD, default: today)')
    parser.add_argument('--register-only', action='store_true', help='Only register locations without fetching data')
    parser.add_argument('--summary', action='store_true', help='Show data summary')
    parser.add_argument('--rate-limit', type=float, default=1.0, help='Seconds between API calls (default: 1.0)')

    args = parser.parse_args()

    fetcher = GlobalSnowfallFetcher(db_path=args.db)

    if args.summary:
        stats = fetcher.get_data_summary()
        regions = fetcher.get_station_count()

        print(f"\n{'='*80}")
        print(f"GLOBAL SNOWFALL DATABASE SUMMARY")
        print(f"{'='*80}")
        print(f"Database: {args.db}")
        print(f"Total Stations: {stats['total_stations']:,}")
        print(f"Total Records: {stats['total_records']:,}")
        print(f"Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")
        print(f"Average Daily Snowfall: {stats['avg_daily_snowfall_mm']:.2f} mm")
        print(f"Maximum Daily Snowfall: {stats['max_daily_snowfall_mm']:.2f} mm")
        print(f"Total Snowfall Recorded: {stats['total_snowfall_mm']:,.0f} mm")
        print(f"\nStations by Region:")
        for region, count in sorted(regions.items()):
            print(f"  {region.replace('_', ' ').title()}: {count}")
        print(f"{'='*80}\n")
        return

    fetcher.register_locations()

    if not args.register_only:
        fetcher.collect_all_regions(
            start_date=args.start,
            end_date=args.end,
            rate_limit_delay=args.rate_limit
        )

        # Show final summary
        stats = fetcher.get_data_summary()
        print(f"\n{'='*80}")
        print(f"FINAL STATISTICS")
        print(f"{'='*80}")
        print(f"Total Stations: {stats['total_stations']:,}")
        print(f"Total Records: {stats['total_records']:,}")
        print(f"Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")
        print(f"{'='*80}\n")


if __name__ == '__main__':
    main()
