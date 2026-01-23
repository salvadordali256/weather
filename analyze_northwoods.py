"""
Northern Wisconsin Snowfall Analysis
=====================================

Analyze snowfall patterns in the Wisconsin Northwoods region
"""

from snowfall_duckdb import SnowfallDuckDB
import pandas as pd

# Initialize DuckDB engine
engine = SnowfallDuckDB("./northwoods_snowfall.db")

print("=" * 80)
print("NORTHERN WISCONSIN SNOWFALL ANALYSIS")
print("=" * 80)
print()

# Get date range
sql = "SELECT MIN(date) as first_date, MAX(date) as last_date FROM snowfall.snowfall_daily"
date_range = engine.query(sql)
print(f"Data Period: {date_range['first_date'][0]} to {date_range['last_date'][0]}")
print()

# 1. Overall snowfall by location
print("❄️  TOTAL SNOWFALL BY LOCATION")
print("-" * 80)
sql = """
SELECT
    s.name,
    ss.total_years as years,
    ROUND(ss.total_snowfall_mm / 10.0, 1) as total_cm,
    ROUND(ss.avg_annual_snowfall_mm / 10.0, 1) as avg_annual_cm,
    ROUND(ss.avg_annual_snowfall_mm / 25.4, 1) as avg_annual_inches,
    ss.days_with_snow,
    ROUND(ss.max_daily_snowfall_mm / 10.0, 1) as max_daily_cm,
    ss.max_daily_snowfall_date as max_snow_date
FROM snowfall.station_summaries ss
JOIN snowfall.stations s ON ss.station_id = s.station_id
ORDER BY ss.avg_annual_snowfall_mm DESC
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 2. Monthly snowfall patterns
print("\n📅 MONTHLY SNOWFALL AVERAGES (All Years Combined)")
print("-" * 80)
sql = """
SELECT
    CAST(MONTH(CAST(date AS DATE)) AS INTEGER) as month,
    CASE CAST(MONTH(CAST(date AS DATE)) AS INTEGER)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,
    ROUND(AVG(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0, 1) as avg_daily_cm,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 /
          COUNT(DISTINCT YEAR(CAST(date AS DATE))), 1) as avg_monthly_total_cm,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as total_snow_days,
    ROUND(MAX(snowfall_mm) / 10.0, 1) as record_daily_cm
FROM snowfall.snowfall_daily
WHERE CAST(MONTH(CAST(date AS DATE)) AS INTEGER) IN (1,2,3,4,9,10,11,12)
GROUP BY month
ORDER BY month
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 3. Snowiest winters by location
print("\n🌨️  SNOWIEST WINTERS (Top 10 by Location)")
print("-" * 80)
sql = """
WITH winter_totals AS (
    SELECT
        station_id,
        CASE
            WHEN CAST(MONTH(CAST(date AS DATE)) AS INTEGER) >= 7
            THEN CAST(YEAR(CAST(date AS DATE)) AS VARCHAR) || '-' ||
                 CAST(CAST(YEAR(CAST(date AS DATE)) AS INTEGER) + 1 AS VARCHAR)
            ELSE CAST(CAST(YEAR(CAST(date AS DATE)) AS INTEGER) - 1 AS VARCHAR) || '-' ||
                 CAST(YEAR(CAST(date AS DATE)) AS VARCHAR)
        END as winter_season,
        SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) as total_mm
    FROM snowfall.snowfall_daily
    GROUP BY station_id, winter_season
)
SELECT
    s.name,
    wt.winter_season,
    ROUND(wt.total_mm / 10.0, 1) as total_cm,
    ROUND(wt.total_mm / 25.4, 1) as total_inches
FROM winter_totals wt
JOIN snowfall.stations s ON wt.station_id = s.station_id
ORDER BY wt.total_mm DESC
LIMIT 10
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 4. Recent 7-day snowfall
print("\n📊 LAST 7 DAYS SNOWFALL")
print("-" * 80)
sql = """
SELECT
    s.name,
    ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 10.0, 1) as total_cm,
    COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as days_with_snow,
    ROUND(MAX(sd.snowfall_mm) / 10.0, 1) as max_daily_cm,
    ROUND(AVG(sd.temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(sd.temp_max_celsius), 1) as avg_high_c,
    MAX(sd.date) as latest_data
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE CAST(sd.date AS DATE) >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY s.name
ORDER BY total_cm DESC
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 5. Biggest single-day snowfalls (all time)
print("\n💨 RECORD SINGLE-DAY SNOWFALLS (Top 20)")
print("-" * 80)
sql = """
SELECT
    sd.date,
    s.name,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches,
    ROUND(sd.temp_max_celsius, 1) as temp_high_c,
    ROUND(sd.temp_min_celsius, 1) as temp_low_c
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE sd.snowfall_mm > 0
ORDER BY sd.snowfall_mm DESC
LIMIT 20
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 6. Location comparison - which town gets the most snow?
print("\n🏆 SNOWFALL RANKINGS")
print("-" * 80)
sql = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ss.avg_annual_snowfall_mm DESC) as rank,
    s.name,
    ROUND(ss.avg_annual_snowfall_mm / 25.4, 1) as avg_annual_inches,
    ROUND(ss.avg_annual_snowfall_mm / 10.0, 1) as avg_annual_cm,
    ROUND(ss.max_daily_snowfall_mm / 25.4, 1) as record_daily_inches,
    ss.days_with_snow as total_snow_days
FROM snowfall.station_summaries ss
JOIN snowfall.stations s ON ss.station_id = s.station_id
ORDER BY ss.avg_annual_snowfall_mm DESC
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

# 7. Snow season length
print("\n📆 SNOW SEASON CHARACTERISTICS")
print("-" * 80)
sql = """
SELECT
    s.name,
    ss.first_snow_date as earliest_snow,
    ss.last_snow_date as latest_snow,
    CAST((CAST(ss.last_snow_date AS DATE) - CAST(ss.first_snow_date AS DATE)) AS INTEGER) as season_length_days,
    ss.days_with_snow as total_snow_days
FROM snowfall.station_summaries ss
JOIN snowfall.stations s ON ss.station_id = s.station_id
ORDER BY season_length_days DESC
"""
df = engine.query(sql)
print(df.to_string(index=False))
print()

engine.close()

print("=" * 80)
print("✅ Analysis complete!")
print("=" * 80)
print()
print("Database: ./northwoods_snowfall.db")
print()
