"""
Analyze the 7-Day Snowfall Data
================================
"""

from snowfall_duckdb import SnowfallDuckDB
import pandas as pd

# Initialize DuckDB engine
engine = SnowfallDuckDB("./snowfall_7day.db")

print("=" * 80)
print("7-DAY SNOWFALL ANALYSIS (Nov 9-16, 2025)")
print("=" * 80)
print()

# 1. Locations with snowfall
print("❄️  LOCATIONS WITH SNOW IN PAST 7 DAYS")
print("-" * 80)
sql = """
SELECT
    s.name,
    s.state,
    s.country,
    ROUND(SUM(sd.snowfall_mm) / 10.0, 1) as total_snowfall_cm,
    COUNT(*) as days_with_data,
    SUM(CASE WHEN sd.snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
    ROUND(AVG(sd.temp_min_celsius), 1) as avg_temp_low_c,
    ROUND(AVG(sd.temp_max_celsius), 1) as avg_temp_high_c
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
GROUP BY s.name, s.state, s.country
ORDER BY total_snowfall_cm DESC
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 2. Daily snowfall totals across all locations
print("\n📅 DAILY TOTALS (All Locations Combined)")
print("-" * 80)
sql = """
SELECT
    date,
    COUNT(DISTINCT station_id) as locations_reporting,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0, 1) as total_snowfall_cm,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as locations_with_snow,
    ROUND(AVG(temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(temp_max_celsius), 1) as avg_high_c
FROM snowfall.snowfall_daily
GROUP BY date
ORDER BY date
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 3. Snowiest single days at any location
print("\n🌨️  SNOWIEST SINGLE DAYS")
print("-" * 80)
sql = """
SELECT
    sd.date,
    s.name,
    s.state,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.temp_max_celsius, 1) as temp_high_c,
    ROUND(sd.temp_min_celsius, 1) as temp_low_c
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE sd.snowfall_mm > 0
ORDER BY sd.snowfall_mm DESC
LIMIT 10
"""
df = engine.query(sql)
if not df.empty:
    print(df.to_string(index=False))
else:
    print("No snowfall recorded in the past 7 days")
print()

# 4. Summary statistics
print("\n📊 SUMMARY STATISTICS")
print("-" * 80)
sql = """
SELECT
    COUNT(DISTINCT station_id) as total_locations,
    COUNT(*) as total_measurements,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0, 1) as total_snowfall_cm,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as measurements_with_snow,
    ROUND(100.0 * COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) / COUNT(*), 1) as pct_with_snow,
    ROUND(AVG(temp_min_celsius), 1) as overall_avg_low_c,
    ROUND(AVG(temp_max_celsius), 1) as overall_avg_high_c
FROM snowfall.snowfall_daily
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

engine.close()

print("=" * 80)
print("✅ Analysis complete!")
print("=" * 80)
print()
print("Database: ./snowfall_7day.db")
print("Size: ", end="")
