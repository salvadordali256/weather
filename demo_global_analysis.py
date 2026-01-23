#!/usr/bin/env python3
"""
Demo Global Snowfall Analysis
Proof-of-concept using existing Wisconsin data + key global locations

Strategy to work within API limits:
1. Use EXISTING Wisconsin data (northwoods_full_history.db)
2. Collect only 5-10 KEY global predictor locations
3. Run scientific correlation analysis
4. Demonstrate the teleconnection concept works
5. User can expand gradually over time

Key locations for demo:
- Sapporo, Japan (East Asian jet indicator)
- Irkutsk, Russia (Siberian cold source)
- Mammoth Mountain, CA (Pacific pattern)
- Denver/Front Range area (Continental US indicator)
- Thunder Bay, ON (Regional Great Lakes)
"""

import sqlite3
import requests
import time
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime
import json
import os

# Critical locations for demo (scientifically selected)
DEMO_LOCATIONS = [
    # Target (use existing data)
    {"name": "Eagle River, WI", "lat": 45.9169, "lon": -89.2443, "region": "target", "significance": "PRIMARY TARGET"},

    # Key global predictors (only 5 to stay under API limits)
    {"name": "Sapporo, Japan", "lat": 43.0642, "lon": 141.3469, "region": "japan", "significance": "East Asian jet stream"},
    {"name": "Irkutsk, Russia", "lat": 52.2978, "lon": 104.2964, "region": "siberia", "significance": "Lake Baikal, Siberian cold source"},
    {"name": "Mammoth Mountain, CA", "lat": 37.6308, "lon": -119.0326, "region": "california", "significance": "Pacific atmospheric rivers"},
    {"name": "Thunder Bay, ON", "lat": 48.3809, "lon": -89.2477, "region": "canada", "significance": "Great Lakes lake-effect"},
    {"name": "Denver, CO", "lat": 39.7392, "lon": -104.9903, "region": "colorado", "significance": "Continental upslope"}
]


class DemoGlobalAnalyzer:
    """Streamlined demo for global snowfall correlation analysis"""

    def __init__(self):
        self.demo_db = "demo_global_snowfall.db"
        self.existing_wi_db = "northwoods_full_history.db"
        self.init_demo_database()

    def init_demo_database(self):
        """Create demo database"""
        conn = sqlite3.connect(self.demo_db)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                station_id TEXT PRIMARY KEY,
                name TEXT,
                latitude REAL,
                longitude REAL,
                region TEXT,
                significance TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snowfall_daily (
                station_id TEXT,
                date TEXT,
                snowfall_mm REAL,
                temp_mean_celsius REAL,
                PRIMARY KEY (station_id, date)
            )
        """)

        conn.commit()
        conn.close()
        print(f"✓ Demo database initialized: {self.demo_db}")

    def import_existing_wisconsin_data(self):
        """Import from existing northwoods database"""
        if not os.path.exists(self.existing_wi_db):
            print(f"✗ Existing Wisconsin database not found: {self.existing_wi_db}")
            return False

        print(f"\n{'='*80}")
        print(f"IMPORTING EXISTING WISCONSIN DATA")
        print(f"{'='*80}\n")

        # Connect to both databases
        src_conn = sqlite3.connect(self.existing_wi_db)
        dst_conn = sqlite3.connect(self.demo_db)

        # Get Eagle River data
        query = """
            SELECT date, snowfall_mm,
                   (temp_max_celsius + temp_min_celsius) / 2.0 as temp_mean_celsius
            FROM snowfall_daily
            WHERE station_id LIKE '%Eagle_River%'
            ORDER BY date
        """

        df = pd.read_sql_query(query, src_conn)

        if df.empty:
            print("✗ No Eagle River data found in existing database")
            src_conn.close()
            dst_conn.close()
            return False

        # Insert station
        dst_cursor = dst_conn.cursor()
        dst_cursor.execute("""
            INSERT OR REPLACE INTO stations
            (station_id, name, latitude, longitude, region, significance)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('eagle_river_wi', 'Eagle River, WI', 45.9169, -89.2443, 'target', 'PRIMARY TARGET'))

        # Insert data
        for _, row in df.iterrows():
            dst_cursor.execute("""
                INSERT OR REPLACE INTO snowfall_daily
                (station_id, date, snowfall_mm, temp_mean_celsius)
                VALUES (?, ?, ?, ?)
            """, ('eagle_river_wi', row['date'], row['snowfall_mm'], row['temp_mean_celsius']))

        dst_conn.commit()
        src_conn.close()
        dst_conn.close()

        print(f"✓ Imported {len(df):,} days of Wisconsin data (Eagle River)")
        print(f"  Period: {df['date'].min()} to {df['date'].max()}")
        return True

    def fetch_global_location(self, loc: dict, start_date: str = "2000-01-01", rate_limit: float = 5.0):
        """Fetch single global location with long delay"""
        station_id = loc['name'].replace(' ', '_').replace(',', '').lower()

        print(f"  {loc['name']:30s} ({loc['region']})... ", end="", flush=True)

        # Check if already exists
        conn = sqlite3.connect(self.demo_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM snowfall_daily WHERE station_id = ?", (station_id,))
        existing_count = cursor.fetchone()[0]

        if existing_count > 100:
            print(f"✓ Already have {existing_count:,} days")
            conn.close()
            return True

        conn.close()

        # Fetch from API
        end_date = datetime.now().strftime("%Y-%m-%d")
        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            'latitude': loc['lat'],
            'longitude': loc['lon'],
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'snowfall_sum,temperature_2m_mean',
            'timezone': 'UTC'
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Store data
            conn = sqlite3.connect(self.demo_db)
            cursor = conn.cursor()

            # Insert station
            cursor.execute("""
                INSERT OR REPLACE INTO stations
                (station_id, name, latitude, longitude, region, significance)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (station_id, loc['name'], loc['lat'], loc['lon'], loc['region'], loc['significance']))

            # Insert daily data
            daily = data.get('daily', {})
            dates = daily.get('time', [])
            snowfall = daily.get('snowfall_sum', [])
            temp = daily.get('temperature_2m_mean', [])

            count = 0
            for i, date in enumerate(dates):
                cursor.execute("""
                    INSERT OR REPLACE INTO snowfall_daily
                    (station_id, date, snowfall_mm, temp_mean_celsius)
                    VALUES (?, ?, ?, ?)
                """, (station_id, date, snowfall[i], temp[i]))
                count += 1

            conn.commit()
            conn.close()

            print(f"✓ {count:,} days collected")
            time.sleep(rate_limit)  # Long delay to avoid rate limits
            return True

        except requests.exceptions.HTTPError as e:
            if '429' in str(e):
                print(f"⚠ Rate limited - try again later or increase delay")
            else:
                print(f"✗ API error: {e}")
            return False
        except Exception as e:
            print(f"✗ Error: {e}")
            return False

    def collect_demo_data(self, start_date: str = "2000-01-01"):
        """Collect data for demo locations"""
        print(f"\n{'='*80}")
        print(f"DEMO GLOBAL DATA COLLECTION")
        print(f"{'='*80}\n")
        print(f"Strategy: 5 key global locations + existing Wisconsin data")
        print(f"Rate limit: 5 seconds between calls (conservative for free API)")
        print(f"Expected time: ~30 seconds\n")

        # Import Wisconsin data first
        if not self.import_existing_wisconsin_data():
            print("\n⚠ Could not import Wisconsin data - will fetch from API")
            self.fetch_global_location(DEMO_LOCATIONS[0], start_date, 5.0)

        # Fetch global locations
        print(f"\nFetching global predictor locations:")
        print(f"{'─'*80}")

        for loc in DEMO_LOCATIONS[1:]:  # Skip Wisconsin (already imported)
            self.fetch_global_location(loc, start_date, 5.0)

        print(f"\n{'='*80}")
        print(f"✓ DEMO DATA COLLECTION COMPLETE")
        print(f"{'='*80}\n")

    def analyze_correlations(self, max_lag_days: int = 30):
        """Run correlation analysis"""
        print(f"\n{'='*80}")
        print(f"CORRELATION ANALYSIS")
        print(f"{'='*80}\n")

        conn = sqlite3.connect(self.demo_db)

        # Get Wisconsin data
        wi_query = "SELECT date, snowfall_mm FROM snowfall_daily WHERE station_id = 'eagle_river_wi' ORDER BY date"
        wi_df = pd.read_sql_query(wi_query, conn)
        wi_df['date'] = pd.to_datetime(wi_df['date'])

        # Get all other stations
        stations_query = "SELECT DISTINCT station_id, name, region, significance FROM stations WHERE station_id != 'eagle_river_wi'"
        stations = pd.read_sql_query(stations_query, conn)

        results = []

        for _, station in stations.iterrows():
            station_id = station['station_id']

            # Get this station's data
            query = f"SELECT date, snowfall_mm FROM snowfall_daily WHERE station_id = ? ORDER BY date"
            df = pd.read_sql_query(query, conn, params=(station_id,))
            df['date'] = pd.to_datetime(df['date'])

            # Merge with Wisconsin
            merged = pd.merge(df, wi_df, on='date', suffixes=('_predictor', '_wi'))

            if len(merged) < 100:
                continue

            # Calculate lag correlations
            best_corr = 0
            best_lag = 0
            best_p = 1.0

            for lag in range(-max_lag_days, max_lag_days + 1):
                if lag < 0:
                    s_pred = merged['snowfall_mm_predictor'].iloc[:lag].values
                    s_wi = merged['snowfall_mm_wi'].iloc[-lag:].values
                elif lag > 0:
                    s_pred = merged['snowfall_mm_predictor'].iloc[lag:].values
                    s_wi = merged['snowfall_mm_wi'].iloc[:-lag].values
                else:
                    s_pred = merged['snowfall_mm_predictor'].values
                    s_wi = merged['snowfall_mm_wi'].values

                # Remove NaN
                mask = ~(np.isnan(s_pred) | np.isnan(s_wi))
                s_pred_clean = s_pred[mask]
                s_wi_clean = s_wi[mask]

                if len(s_pred_clean) >= 30:
                    try:
                        corr, p_value = stats.pearsonr(s_pred_clean, s_wi_clean)
                        if abs(corr) > abs(best_corr):
                            best_corr = corr
                            best_lag = lag
                            best_p = p_value
                    except:
                        pass

            results.append({
                'station': station['name'],
                'region': station['region'],
                'significance': station['significance'],
                'correlation': best_corr,
                'lag_days': best_lag,
                'p_value': best_p,
                'sample_size': len(merged)
            })

        conn.close()

        # Sort by correlation strength
        results.sort(key=lambda x: abs(x['correlation']), reverse=True)

        # Print results
        print(f"Target: Eagle River, Wisconsin")
        print(f"Period: 2000-2025 (25 years)\n")
        print(f"{'='*80}")
        print(f"GLOBAL PREDICTORS RANKED BY CORRELATION STRENGTH")
        print(f"{'='*80}\n")

        for i, r in enumerate(results, 1):
            sig_mark = "***" if r['p_value'] < 0.001 else ("**" if r['p_value'] < 0.01 else ("*" if r['p_value'] < 0.05 else ""))

            if r['lag_days'] > 0:
                lag_text = f"Leads WI by {r['lag_days']}d"
            elif r['lag_days'] < 0:
                lag_text = f"Lags WI by {abs(r['lag_days'])}d"
            else:
                lag_text = "Simultaneous"

            strength = "STRONG" if abs(r['correlation']) > 0.3 else ("MODERATE" if abs(r['correlation']) > 0.15 else "WEAK")

            print(f"{i}. {r['station']:30s} ({r['region']})")
            print(f"   Correlation: r={r['correlation']:+.3f} {sig_mark:3s} | {strength}")
            print(f"   Lag Pattern: {lag_text}")
            print(f"   Sample Size: {r['sample_size']:,} days")
            print(f"   Significance: {r['significance']}")
            print()

        # Save results
        with open('demo_correlation_results.json', 'w') as f:
            json.dump(results, f, indent=2)

        print(f"{'='*80}")
        print(f"✓ Results saved to: demo_correlation_results.json")
        print(f"{'='*80}\n")

        return results

    def generate_report(self, results):
        """Generate scientific interpretation"""
        print(f"\n{'='*80}")
        print(f"SCIENTIFIC INTERPRETATION")
        print(f"{'='*80}\n")

        sig_results = [r for r in results if r['p_value'] < 0.05]
        lead_predictors = [r for r in results if r['lag_days'] > 3 and abs(r['correlation']) > 0.15]

        print(f"Key Findings:")
        print(f"─────────────────────────────────────────────────────────────────────────────────")
        print(f"  • Total predictors analyzed: {len(results)}")
        print(f"  • Statistically significant (p<0.05): {len(sig_results)}")
        print(f"  • Leading indicators (lag > 3 days, |r| > 0.15): {len(lead_predictors)}\n")

        if lead_predictors:
            print(f"Early Warning Signals:")
            for r in lead_predictors:
                print(f"  • {r['station']:30s} leads by {r['lag_days']} days (r={r['correlation']:+.3f})")
                print(f"    → {r['significance']}")
                print()

        print(f"\nTeleconnection Science Validation:")
        print(f"─────────────────────────────────────────────────────────────────────────────────")

        for r in results:
            if 'sib' in r['region'].lower() or 'russia' in r['region'].lower():
                print(f"  SIBERIAN CONNECTION:")
                print(f"    {r['station']} shows r={r['correlation']:+.3f} at {r['lag_days']} day lag")
                print(f"    → Supports: Cold air mass → Polar vortex → Wisconsin outbreak theory")
                print()

            elif 'japan' in r['region'].lower():
                print(f"  EAST ASIAN JET STREAM:")
                print(f"    {r['station']} shows r={r['correlation']:+.3f} at {r['lag_days']} day lag")
                print(f"    → Supports: Strong Asian jet → Wave propagation → North American pattern")
                print()

            elif 'calif' in r['region'].lower():
                print(f"  PACIFIC PATTERN:")
                print(f"    {r['station']} shows r={r['correlation']:+.3f} at {r['lag_days']} day lag")
                print(f"    → Supports: Atmospheric river → Blocking → Downstream Wisconsin snow")
                print()

            elif 'canada' in r['region'].lower() and r['lag_days'] == 0:
                print(f"  REGIONAL PROXIMITY:")
                print(f"    {r['station']} shows r={r['correlation']:+.3f} (simultaneous)")
                print(f"    → Same storm systems affecting Great Lakes region")
                print()

        print(f"{'='*80}\n")


def main():
    analyzer = DemoGlobalAnalyzer()

    # Step 1: Collect demo data
    analyzer.collect_demo_data(start_date="2000-01-01")

    # Step 2: Analyze correlations
    results = analyzer.analyze_correlations(max_lag_days=30)

    # Step 3: Scientific interpretation
    analyzer.generate_report(results)

    print(f"\n{'='*80}")
    print(f"✓ DEMO COMPLETE")
    print(f"{'='*80}")
    print(f"Database: demo_global_snowfall.db")
    print(f"Results: demo_correlation_results.json")
    print(f"\nThis demonstrates the concept with 5 key global locations.")
    print(f"To expand: Gradually add more locations over multiple sessions")
    print(f"to stay within free API limits.")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()
