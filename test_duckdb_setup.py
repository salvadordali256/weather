"""
Test DuckDB Setup and Create Demo Database
==========================================

Creates a small sample snowfall database to test DuckDB functionality.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

def create_sample_database(db_path: str = "./snowfall_demo.db"):
    """Create a small sample database for testing DuckDB"""

    print("Creating sample snowfall database...")

    # Connect to SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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
        data_source TEXT
    )
    """)

    # Create snowfall_daily table
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

    # Create station_summaries table
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

    # Sample stations
    stations = [
        ("DEMO_ASPEN_CO", "Aspen, Colorado", 39.1911, -106.8175, 2438, "Colorado", "USA"),
        ("DEMO_TAHOE_CA", "Lake Tahoe, California", 39.0968, -120.0324, 1899, "California", "USA"),
        ("DEMO_JACKSON_WY", "Jackson Hole, Wyoming", 43.4799, -110.7624, 1924, "Wyoming", "USA"),
        ("DEMO_BANFF_AB", "Banff, Alberta", 51.1784, -115.5708, 1383, "Alberta", "Canada"),
        ("DEMO_WHISTLER_BC", "Whistler, British Columbia", 50.1163, -122.9574, 675, "British Columbia", "Canada"),
    ]

    for station in stations:
        cursor.execute("""
        INSERT OR REPLACE INTO stations
        (station_id, name, latitude, longitude, elevation, state, country, min_date, max_date, data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*station, "2000-01-01", "2024-12-31", "DEMO"))

    print(f"  ✓ Created {len(stations)} sample stations")

    # Generate sample snowfall data (25 years)
    np.random.seed(42)

    total_records = 0
    for station_id, name, lat, lon, elev, state, country in stations:
        # Higher elevation = more snow
        base_snowfall = elev / 100  # mm per snow day
        snow_probability = min(0.3 + (elev / 5000), 0.6)  # probability of snow

        records = []
        start_date = datetime(2000, 1, 1)
        end_date = datetime(2024, 12, 31)
        current_date = start_date

        while current_date <= end_date:
            # Winter months have more snow
            month = current_date.month
            is_winter = month in [11, 12, 1, 2, 3]

            # Simulate snowfall
            if is_winter and np.random.random() < snow_probability:
                # Snow day!
                snowfall = np.random.exponential(base_snowfall) * 10
                snow_depth = snowfall + np.random.uniform(0, snowfall * 2)
                temp_max = np.random.uniform(-15, 5)
                temp_min = temp_max - np.random.uniform(5, 15)
                precip = snowfall * 1.2
            else:
                snowfall = None
                snow_depth = None
                temp_max = np.random.uniform(-10, 25)
                temp_min = temp_max - np.random.uniform(5, 15)
                precip = None if not is_winter else np.random.exponential(5)

            records.append((
                station_id,
                current_date.strftime("%Y-%m-%d"),
                snowfall,
                snow_depth,
                temp_max,
                temp_min,
                precip,
                "good"
            ))

            current_date += timedelta(days=1)

        # Insert all records for this station
        cursor.executemany("""
        INSERT OR REPLACE INTO snowfall_daily
        (station_id, date, snowfall_mm, snow_depth_mm, temp_max_celsius,
         temp_min_celsius, precipitation_mm, data_quality)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

        total_records += len(records)

        # Calculate summary statistics
        df = pd.DataFrame(records, columns=[
            'station_id', 'date', 'snowfall_mm', 'snow_depth_mm',
            'temp_max_celsius', 'temp_min_celsius', 'precipitation_mm', 'data_quality'
        ])

        df['date'] = pd.to_datetime(df['date'])
        df_snow = df[df['snowfall_mm'].notna()]

        if len(df_snow) > 0:
            total_snowfall = df_snow['snowfall_mm'].sum()
            years = df['date'].dt.year.nunique()

            cursor.execute("""
            INSERT OR REPLACE INTO station_summaries
            (station_id, total_years, total_snowfall_mm, avg_annual_snowfall_mm,
             max_daily_snowfall_mm, max_daily_snowfall_date, max_snow_depth_mm,
             max_snow_depth_date, first_snow_date, last_snow_date, days_with_snow)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                station_id,
                years,
                total_snowfall,
                total_snowfall / years,
                df_snow['snowfall_mm'].max(),
                df_snow.loc[df_snow['snowfall_mm'].idxmax(), 'date'].strftime("%Y-%m-%d"),
                df_snow['snow_depth_mm'].max(),
                df_snow.loc[df_snow['snow_depth_mm'].idxmax(), 'date'].strftime("%Y-%m-%d"),
                df_snow['date'].min().strftime("%Y-%m-%d"),
                df_snow['date'].max().strftime("%Y-%m-%d"),
                len(df_snow)
            ))

    conn.commit()
    conn.close()

    print(f"  ✓ Created {total_records:,} sample records")
    print(f"  ✓ Database saved to: {db_path}")
    print(f"  ✓ Database size: {Path(db_path).stat().st_size / (1024*1024):.2f} MB")

    return db_path


def test_duckdb_queries(db_path: str):
    """Test DuckDB queries on the sample database"""

    print("\n" + "="*80)
    print("Testing DuckDB with Sample Data")
    print("="*80)

    try:
        from snowfall_duckdb import SnowfallDuckDB

        # Initialize DuckDB engine
        engine = SnowfallDuckDB(db_path)

        print("\n1. Top Snowiest Stations")
        print("-"*80)
        df = engine.top_snowiest_stations(5)
        print(df.to_string())

        print("\n2. Snowfall by Year (Last 5 Years)")
        print("-"*80)
        df = engine.snowfall_by_year(2020, 2024)
        print(df.to_string())

        print("\n3. Snowfall by State")
        print("-"*80)
        df = engine.snowfall_by_state()
        print(df.to_string())

        print("\n4. Biggest Snowstorms (Top 10)")
        print("-"*80)
        df = engine.biggest_blizzards(10)
        print(df.to_string())

        print("\n5. Climate Change Analysis")
        print("-"*80)
        df = engine.climate_change_analysis(
            baseline_start=2000,
            baseline_end=2010,
            recent_start=2015,
            recent_end=2024
        )
        print(df.to_string())

        print("\n6. Rolling 5-Year Average")
        print("-"*80)
        df = engine.rolling_annual_snowfall(window_years=5)
        print(df.head(10).to_string())

        engine.close()

        print("\n" + "="*80)
        print("✅ ALL DUCKDB TESTS PASSED!")
        print("="*80)

    except Exception as e:
        print(f"\n❌ Error testing DuckDB: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Create sample database
    db_path = create_sample_database()

    # Test DuckDB queries
    test_duckdb_queries(db_path)

    print("\n" + "="*80)
    print("🎉 DuckDB Setup Complete!")
    print("="*80)
    print("\nNext steps:")
    print("  1. Collect real data: python snowfall_collector.py")
    print("  2. Analyze with DuckDB: python snowfall_duckdb.py")
    print("  3. Read DUCKDB_GUIDE.md for more examples")
    print()
