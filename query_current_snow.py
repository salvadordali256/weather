"""
Query current snowfall totals from all databases
January 4, 2026
"""

import duckdb
import pandas as pd
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

print("=" * 80)
print("CURRENT SNOWFALL DATA - January 4, 2026")
print("=" * 80)
print()

# Query Northwoods database
print("=" * 80)
print("1. NORTHWOODS WISCONSIN (Phelps, Land O'Lakes area)")
print("=" * 80)
try:
    conn = duckdb.connect('northwoods_snowfall.db')

    # Get table names
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchdf()
    print("\nTables:", tables['table_name'].tolist())

    # Latest records
    print("\n--- Latest 10 Records ---")
    result = conn.execute('SELECT * FROM snowfall_daily ORDER BY date DESC LIMIT 10').fetchdf()
    print(result)

    # January 2026 totals
    print("\n--- January 2026 Totals (Jan 1-4) ---")
    jan_total = conn.execute("""
        SELECT location,
               SUM(snowfall_cm) as total_snow_cm,
               SUM(snowfall_cm) * 0.393701 as total_snow_inches,
               COUNT(*) as days,
               MIN(date) as first_date,
               MAX(date) as last_date
        FROM snowfall_daily
        WHERE date >= '2026-01-01' AND date <= '2026-01-04'
        GROUP BY location
        ORDER BY total_snow_cm DESC
    """).fetchdf()
    print(jan_total)

    conn.close()
except Exception as e:
    print(f"Error: {e}")

print()

# Query 7-day database
print("=" * 80)
print("2. RECENT 7-DAY DATA")
print("=" * 80)
try:
    conn = duckdb.connect('snowfall_7day.db')

    print("\n--- Latest 15 Records ---")
    result = conn.execute('SELECT * FROM snowfall_daily ORDER BY date DESC LIMIT 15').fetchdf()
    print(result)

    # Get date range
    date_range = conn.execute('SELECT MIN(date) as oldest, MAX(date) as newest FROM snowfall_daily').fetchdf()
    print("\n--- Date Range in Database ---")
    print(date_range)

    conn.close()
except Exception as e:
    print(f"Error: {e}")

print()

# Query UP Michigan database
print("=" * 80)
print("3. UP MICHIGAN (Watersmeet, Big Powderhorn area)")
print("=" * 80)
try:
    conn = duckdb.connect('up_michigan_snowfall.db')

    # Latest records
    print("\n--- Latest 10 Records ---")
    result = conn.execute('SELECT * FROM snowfall_daily ORDER BY date DESC LIMIT 10').fetchdf()
    print(result)

    # January 2026 totals
    print("\n--- January 2026 Totals (Jan 1-4) ---")
    jan_total = conn.execute("""
        SELECT location,
               SUM(snowfall_cm) as total_snow_cm,
               SUM(snowfall_cm) * 0.393701 as total_snow_inches,
               COUNT(*) as days,
               MIN(date) as first_date,
               MAX(date) as last_date
        FROM snowfall_daily
        WHERE date >= '2026-01-01' AND date <= '2026-01-04'
        GROUP BY location
        ORDER BY total_snow_cm DESC
    """).fetchdf()
    print(jan_total)

    conn.close()
except Exception as e:
    print(f"Error: {e}")

print()
print("=" * 80)
print("Query completed:", datetime.now().strftime('%B %d, %Y at %I:%M %p'))
print("=" * 80)
