#!/usr/bin/env python3
"""
Regional Station Data Collection
Adds critical nearby stations for local event detection

Target Stations:
- Winnipeg, MB (Alberta Clipper track)
- Marquette, MI (Lake effect indicator)
- Duluth, MN (Regional track)
- Green Bay, WI (Local confirmation)
- Minneapolis, MN (Track indicator)
- Sault Ste Marie, MI (Great Lakes flow)
"""

import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time

class RegionalStationCollector:
    """Collect data for regional stations around Wisconsin"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # Critical regional stations
        self.stations = {
            'winnipeg_mb': {
                'lat': 49.90,
                'lon': -97.14,
                'name': 'Winnipeg, MB',
                'importance': 'CRITICAL - Alberta Clipper track indicator',
                'distance_from_wi': '250 miles NW'
            },
            'marquette_mi': {
                'lat': 46.54,
                'lon': -87.40,
                'name': 'Marquette, MI',
                'importance': 'CRITICAL - Lake effect snow indicator',
                'distance_from_wi': '100 miles E'
            },
            'duluth_mn': {
                'lat': 46.79,
                'lon': -92.10,
                'name': 'Duluth, MN',
                'importance': 'HIGH - Regional track indicator',
                'distance_from_wi': '150 miles W'
            },
            'green_bay_wi': {
                'lat': 44.52,
                'lon': -88.02,
                'name': 'Green Bay, WI',
                'importance': 'CRITICAL - Local confirmation (same state)',
                'distance_from_wi': '100 miles S'
            },
            'minneapolis_mn': {
                'lat': 44.98,
                'lon': -93.27,
                'name': 'Minneapolis, MN',
                'importance': 'HIGH - Southwest track indicator',
                'distance_from_wi': '250 miles SW'
            },
            'sault_ste_marie_mi': {
                'lat': 46.50,
                'lon': -84.35,
                'name': 'Sault Ste Marie, MI',
                'importance': 'MODERATE - Great Lakes flow indicator',
                'distance_from_wi': '200 miles E'
            },
            'iron_mountain_mi': {
                'lat': 45.82,
                'lon': -88.06,
                'name': 'Iron Mountain, MI',
                'importance': 'HIGH - Adjacent to Wisconsin, lake effect',
                'distance_from_wi': '50 miles NE'
            },
            'escanaba_mi': {
                'lat': 45.75,
                'lon': -87.06,
                'name': 'Escanaba, MI',
                'importance': 'MODERATE - Lake Michigan effect',
                'distance_from_wi': '120 miles E'
            }
        }

    def fetch_station_data(self, station_id, lat, lon, start_year=1995):
        """Fetch historical snowfall data from Open-Meteo"""

        print(f"\nFetching data for {self.stations[station_id]['name']}...")
        print(f"  Location: {lat}, {lon}")
        print(f"  Importance: {self.stations[station_id]['importance']}")

        # Open-Meteo Historical API
        url = "https://archive-api.open-meteo.com/v1/archive"

        end_date = datetime.now()
        start_date = datetime(start_year, 1, 1)

        all_data = []
        current_start = start_date

        # Fetch in yearly chunks to avoid timeout
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=365), end_date)

            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': current_start.strftime('%Y-%m-%d'),
                'end_date': current_end.strftime('%Y-%m-%d'),
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

                    for date, snow in zip(dates, snowfall):
                        if snow is not None:
                            # Convert cm to mm
                            snow_mm = snow * 10.0
                            all_data.append({
                                'station_id': station_id,
                                'date': date,
                                'snowfall_mm': snow_mm
                            })

                print(f"  ✅ Fetched {current_start.year}: {len([d for d in all_data if d['date'].startswith(str(current_start.year))])} records")

            except Exception as e:
                print(f"  ⚠️  Error fetching {current_start.year}: {e}")

            current_start = current_end
            time.sleep(0.5)  # Rate limiting

        print(f"  📊 Total records: {len(all_data)}")
        return all_data

    def save_to_database(self, station_id, data):
        """Save station data to database"""

        if not data:
            print(f"  ⚠️  No data to save for {station_id}")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Insert data
        cursor.executemany("""
            INSERT OR REPLACE INTO snowfall_daily (station_id, date, snowfall_mm)
            VALUES (?, ?, ?)
        """, [(d['station_id'], d['date'], d['snowfall_mm']) for d in data])

        conn.commit()
        conn.close()

        print(f"  💾 Saved {len(data)} records to database")

    def verify_data(self, station_id):
        """Verify data was saved correctly"""

        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                COUNT(*) as total_records,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                AVG(snowfall_mm) as avg_snowfall,
                MAX(snowfall_mm) as max_snowfall
            FROM snowfall_daily
            WHERE station_id = ?
        """

        df = pd.read_sql_query(query, conn, params=(station_id,))
        conn.close()

        if not df.empty and df.iloc[0]['total_records'] > 0:
            row = df.iloc[0]
            print(f"\n  ✅ VERIFICATION PASSED:")
            print(f"     Records: {row['total_records']}")
            print(f"     Date Range: {row['earliest_date']} to {row['latest_date']}")
            print(f"     Avg Snowfall: {row['avg_snowfall']:.1f}mm")
            print(f"     Max Snowfall: {row['max_snowfall']:.1f}mm")
            return True
        else:
            print(f"\n  ❌ VERIFICATION FAILED: No data in database")
            return False

    def collect_all_stations(self):
        """Main collection process"""

        print(f"\n{'='*80}")
        print(f"REGIONAL STATION DATA COLLECTION")
        print(f"{'='*80}")
        print(f"Purpose: Add nearby stations for local event detection")
        print(f"Stations to collect: {len(self.stations)}")
        print(f"{'='*80}\n")

        success_count = 0
        failed_stations = []

        for station_id, info in self.stations.items():
            print(f"\n{'─'*80}")
            print(f"STATION: {info['name']}")
            print(f"{'─'*80}")

            try:
                # Fetch data
                data = self.fetch_station_data(
                    station_id,
                    info['lat'],
                    info['lon']
                )

                # Save to database
                self.save_to_database(station_id, data)

                # Verify
                if self.verify_data(station_id):
                    success_count += 1
                    print(f"\n✅ {info['name']} - SUCCESS")
                else:
                    failed_stations.append(info['name'])
                    print(f"\n❌ {info['name']} - VERIFICATION FAILED")

            except Exception as e:
                failed_stations.append(info['name'])
                print(f"\n❌ {info['name']} - FAILED: {e}")

            # Rate limiting
            time.sleep(1)

        # Final summary
        print(f"\n{'='*80}")
        print(f"COLLECTION SUMMARY")
        print(f"{'='*80}\n")

        print(f"Stations Attempted: {len(self.stations)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {len(failed_stations)}")

        if failed_stations:
            print(f"\nFailed Stations:")
            for name in failed_stations:
                print(f"  ❌ {name}")

        print(f"\n{'='*80}\n")

        return success_count == len(self.stations)

    def show_database_summary(self):
        """Show summary of all stations in database"""

        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                station_id,
                COUNT(*) as records,
                MIN(date) as earliest,
                MAX(date) as latest,
                MAX(snowfall_mm) as max_snow
            FROM snowfall_daily
            GROUP BY station_id
            ORDER BY station_id
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        print(f"\n{'='*80}")
        print(f"DATABASE SUMMARY - ALL STATIONS")
        print(f"{'='*80}\n")

        print(f"{'Station ID':<30s} | {'Records':>8s} | {'Date Range':>23s} | {'Max Snow':>10s}")
        print(f"{'─'*80}")

        for _, row in df.iterrows():
            station_name = row['station_id'].replace('_', ' ').title()
            date_range = f"{row['earliest'][:10]} to {row['latest'][:10]}"
            print(f"{station_name:<30s} | {row['records']:>8d} | {date_range:>23s} | {row['max_snow']:>8.1f}mm")

        print(f"\n{'='*80}\n")


def main():
    collector = RegionalStationCollector()

    print(f"\n🌨️  REGIONAL STATION DATA COLLECTION")
    print(f"{'='*80}")
    print(f"This will add critical nearby stations for better local event detection:")
    print(f"{'='*80}\n")

    for station_id, info in collector.stations.items():
        print(f"  📍 {info['name']:<25s} - {info['importance']}")

    print(f"\n{'='*80}")
    input("Press ENTER to start data collection...")

    # Collect all stations
    success = collector.collect_all_stations()

    if success:
        print(f"\n✅ ALL STATIONS COLLECTED SUCCESSFULLY!")
    else:
        print(f"\n⚠️  SOME STATIONS FAILED - Check errors above")

    # Show final database summary
    collector.show_database_summary()

    print(f"\n🎉 Data collection complete!")
    print(f"\nNext steps:")
    print(f"  1. Run comprehensive forecast to test new stations")
    print(f"  2. Analyze correlations with Wisconsin")
    print(f"  3. Add to forecast models\n")


if __name__ == '__main__':
    main()
