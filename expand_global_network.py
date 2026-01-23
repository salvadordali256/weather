#!/usr/bin/env python3
"""
Expand Global Network - 30-Year Collection
Carefully add more global locations with 30 years of data (1995-2025)

Strategy:
- Conservative 8-second rate limit to avoid API throttling
- Focus on scientifically critical teleconnection regions
- Add 15-20 locations incrementally
- Can be interrupted and resumed (checks for existing data)
"""

import sqlite3
import requests
import time
from datetime import datetime
import os

# Expanded global network - prioritized by teleconnection significance
EXPANSION_LOCATIONS = [
    # === HIGH PRIORITY: Strong teleconnection predictors ===

    # Additional Russia/Siberia (Cold air source expansion)
    {"name": "Novosibirsk, Russia", "lat": 55.0084, "lon": 82.9357, "region": "siberia", "priority": 1,
     "significance": "Western Siberia continental cold source"},
    {"name": "Yakutsk, Russia", "lat": 62.0355, "lon": 129.6755, "region": "siberia", "priority": 1,
     "significance": "Coldest city on Earth, extreme continental signal"},

    # Additional Japan (East Asian jet expansion)
    {"name": "Niigata, Japan", "lat": 37.9026, "lon": 139.0232, "region": "japan", "priority": 1,
     "significance": "Sea of Japan extreme lake-effect snow"},
    {"name": "Nagano, Japan", "lat": 36.6513, "lon": 138.1809, "region": "japan", "priority": 2,
     "significance": "Japanese Alps orographic enhancement"},

    # Additional California (Pacific pattern expansion)
    {"name": "Lake Tahoe, CA", "lat": 39.1969, "lon": -120.2356, "region": "california", "priority": 2,
     "significance": "Northern Sierra atmospheric river indicator"},

    # Additional Colorado (Continental indicator expansion)
    {"name": "Steamboat Springs, CO", "lat": 40.4850, "lon": -106.8317, "region": "colorado", "priority": 2,
     "significance": "Northern Colorado upslope snow"},
    {"name": "Vail, CO", "lat": 39.6403, "lon": -106.3742, "region": "colorado", "priority": 3,
     "significance": "Central Rockies I-70 corridor"},

    # === MEDIUM PRIORITY: Regional context ===

    # Pacific Northwest (Pacific moisture source)
    {"name": "Mount Baker, WA", "lat": 48.8568, "lon": -121.6714, "region": "pacific_nw", "priority": 2,
     "significance": "World record snowfall location, Pacific moisture"},
    {"name": "Stevens Pass, WA", "lat": 47.7465, "lon": -121.0890, "region": "pacific_nw", "priority": 3,
     "significance": "Washington Cascades snowpack"},

    # Canada (Regional proximity)
    {"name": "Whistler, BC", "lat": 50.1163, "lon": -122.9574, "region": "canada_west", "priority": 2,
     "significance": "Coastal mountains, 2010 Olympics"},
    {"name": "Banff, AB", "lat": 51.1784, "lon": -115.5708, "region": "canada_rockies", "priority": 3,
     "significance": "Canadian Rockies interior"},

    # China (Tibetan High / Continental patterns)
    {"name": "Harbin, China", "lat": 45.8038, "lon": 126.5340, "region": "china", "priority": 2,
     "significance": "Manchuria Siberian influence"},
    {"name": "Urumqi, China", "lat": 43.8256, "lon": 87.6168, "region": "china", "priority": 3,
     "significance": "Xinjiang Tian Shan interior continental"},

    # === LOWER PRIORITY: Hemispheric completeness ===

    # Europe Alps (Northern Hemisphere context)
    {"name": "Chamonix, France", "lat": 45.9237, "lon": 6.8694, "region": "europe_alps", "priority": 3,
     "significance": "Mont Blanc Atlantic influence"},
    {"name": "Zermatt, Switzerland", "lat": 46.0207, "lon": 7.7491, "region": "europe_alps", "priority": 3,
     "significance": "Central Alps Matterhorn"},

    # Scandinavia (Arctic patterns)
    {"name": "Tromsø, Norway", "lat": 69.6492, "lon": 18.9553, "region": "scandinavia", "priority": 3,
     "significance": "Arctic Norway polar low systems"},

    # Additional Wisconsin (Target area density)
    {"name": "Phelps, WI", "lat": 46.0638, "lon": -89.0787, "region": "target", "priority": 1,
     "significance": "Primary forecast target - Phelps"},
    {"name": "Land O'Lakes, WI", "lat": 46.1535, "lon": -89.3207, "region": "target", "priority": 1,
     "significance": "Primary forecast target - Land O'Lakes"},
]


class NetworkExpander:
    """Expand global snowfall network with 30 years of data"""

    def __init__(self, db_path: str = "demo_global_snowfall.db"):
        self.db_path = db_path

    def fetch_location_30yr(self, loc: dict, start_year: int = 1995, rate_limit: float = 8.0):
        """
        Fetch 30 years of data for a location with conservative rate limiting

        Args:
            loc: Location dictionary
            start_year: Start year (default: 1995 for 30-year record)
            rate_limit: Seconds between API calls (default: 8.0)
        """
        station_id = loc['name'].replace(' ', '_').replace(',', '').lower()

        # Check if already exists with sufficient data
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*), MIN(date), MAX(date)
            FROM snowfall_daily
            WHERE station_id = ?
        """, (station_id,))

        result = cursor.fetchone()
        existing_count = result[0] if result else 0

        if existing_count > 9000:  # ~25 years
            print(f"✓ {loc['name']:35s} | Already have {existing_count:,} days | SKIP")
            conn.close()
            return True

        conn.close()

        # Fetch from API
        print(f"  {loc['name']:35s} | P{loc['priority']} | ", end="", flush=True)

        start_date = f"{start_year}-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': loc['lat'],
            'longitude': loc['lon'],
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'snowfall_sum,temperature_2m_mean,temperature_2m_max,temperature_2m_min',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            # Store in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Register station
            cursor.execute("""
                INSERT OR REPLACE INTO stations
                (station_id, name, latitude, longitude, region, significance)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (station_id, loc['name'], loc['lat'], loc['lon'], loc['region'], loc['significance']))

            # Insert daily data
            daily = data.get('daily', {})
            dates = daily.get('time', [])
            snowfall = daily.get('snowfall_sum', [])
            temp_mean = daily.get('temperature_2m_mean', [])

            count = 0
            for i, date in enumerate(dates):
                cursor.execute("""
                    INSERT OR REPLACE INTO snowfall_daily
                    (station_id, date, snowfall_mm, temp_mean_celsius)
                    VALUES (?, ?, ?, ?)
                """, (station_id, date, snowfall[i] if i < len(snowfall) else None,
                     temp_mean[i] if i < len(temp_mean) else None))
                count += 1

            conn.commit()
            conn.close()

            years = count / 365.25
            print(f"✓ {count:,} days ({years:.1f} years)")

            # Rate limiting
            print(f"    Waiting {rate_limit}s... ", end="", flush=True)
            time.sleep(rate_limit)
            print("✓")

            return True

        except requests.exceptions.HTTPError as e:
            if '429' in str(e):
                print(f"⚠ RATE LIMITED - Increase delay and retry")
                return False
            else:
                print(f"✗ API error: {e}")
                return False
        except Exception as e:
            print(f"✗ Error: {e}")
            return False

    def expand_network(self, priority_filter: int = 3, start_year: int = 1995, rate_limit: float = 8.0):
        """
        Expand network by priority level

        Args:
            priority_filter: Only collect locations with priority <= this (1=highest, 3=lowest)
            start_year: Start year for data collection
            rate_limit: Seconds between API calls
        """
        print(f"\n{'='*90}")
        print(f"GLOBAL NETWORK EXPANSION - 30-YEAR DATA COLLECTION")
        print(f"{'='*90}")
        print(f"Period: {start_year}-01-01 to present (~30 years)")
        print(f"Rate Limit: {rate_limit} seconds per location")
        print(f"Priority Filter: P1-P{priority_filter} (1=highest)")
        print(f"Database: {self.db_path}")
        print(f"{'='*90}\n")

        # Filter by priority
        locations = [loc for loc in EXPANSION_LOCATIONS if loc['priority'] <= priority_filter]

        print(f"Selected {len(locations)} locations (Priority 1-{priority_filter}):\n")

        # Group by priority
        by_priority = {}
        for loc in locations:
            p = loc['priority']
            if p not in by_priority:
                by_priority[p] = []
            by_priority[p].append(loc)

        # Collect by priority
        total_collected = 0
        total_failed = 0

        for priority in sorted(by_priority.keys()):
            locs = by_priority[priority]
            print(f"\n{'─'*90}")
            print(f"PRIORITY {priority}: {len(locs)} locations")
            print(f"{'─'*90}\n")

            for loc in locs:
                success = self.fetch_location_30yr(loc, start_year, rate_limit)
                if success:
                    total_collected += 1
                else:
                    total_failed += 1
                    if '429' in str(success):  # Rate limited
                        print(f"\n⚠ Rate limiting detected. Suggest:")
                        print(f"  1. Wait 1 hour and resume")
                        print(f"  2. Increase --rate-limit to 10 or 15 seconds")
                        print(f"  3. Run with --priority 1 to collect only highest priority")
                        break

        print(f"\n{'='*90}")
        print(f"EXPANSION COMPLETE")
        print(f"{'='*90}")
        print(f"Successfully collected: {total_collected}")
        print(f"Failed/Skipped: {total_failed}")
        print(f"Database: {self.db_path}")
        print(f"{'='*90}\n")

        # Show database summary
        self.show_summary()

    def show_summary(self):
        """Show current database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM stations")
        num_stations = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM snowfall_daily")
        num_records = cursor.fetchone()[0]

        cursor.execute("""
            SELECT region, COUNT(DISTINCT station_id) as count
            FROM stations
            GROUP BY region
            ORDER BY count DESC
        """)
        regions = cursor.fetchall()

        conn.close()

        print(f"\nDATABASE SUMMARY:")
        print(f"─────────────────────────────────────────────────")
        print(f"Total Stations: {num_stations}")
        print(f"Total Records: {num_records:,} days")
        print(f"Average per Station: {num_records/num_stations if num_stations > 0 else 0:,.0f} days")
        print(f"\nStations by Region:")
        for region, count in regions:
            print(f"  {region:20s}: {count:2d} stations")
        print(f"─────────────────────────────────────────────────\n")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Expand global snowfall network with 30-year data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect PRIORITY 1 locations only (highest importance, ~5 locations)
  python expand_global_network.py --priority 1 --rate-limit 10

  # Collect PRIORITY 1-2 locations (~12 locations, recommended)
  python expand_global_network.py --priority 2 --rate-limit 8

  # Collect ALL locations (18 locations, takes ~2.5 hours at 8s rate)
  python expand_global_network.py --priority 3 --rate-limit 8

  # Use faster rate if previous run succeeded (risky)
  python expand_global_network.py --priority 2 --rate-limit 5

  # Show summary only
  python expand_global_network.py --summary
        """
    )

    parser.add_argument('--priority', type=int, default=2,
                       help='Collect locations with priority <= this (1=highest, 3=all)')
    parser.add_argument('--start-year', type=int, default=1995,
                       help='Start year for collection (default: 1995 for 30 years)')
    parser.add_argument('--rate-limit', type=float, default=8.0,
                       help='Seconds between API calls (default: 8.0, increase if rate limited)')
    parser.add_argument('--db', default='demo_global_snowfall.db',
                       help='Database file path')
    parser.add_argument('--summary', action='store_true',
                       help='Show database summary only')

    args = parser.parse_args()

    expander = NetworkExpander(db_path=args.db)

    if args.summary:
        expander.show_summary()
    else:
        expander.expand_network(
            priority_filter=args.priority,
            start_year=args.start_year,
            rate_limit=args.rate_limit
        )


if __name__ == '__main__':
    main()
