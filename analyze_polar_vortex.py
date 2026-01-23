"""
Polar Vortex Pattern Analysis for Wisconsin Northwoods
========================================================

Identify polar vortex conditions and associated snowfall patterns
for Phelps WI, Land O'Lakes WI, and Watersmeet MI area
"""

from snowfall_duckdb import SnowfallDuckDB
import pandas as pd

# Initialize DuckDB engine
engine = SnowfallDuckDB("./northwoods_snowfall.db")

print("=" * 80)
print("POLAR VORTEX PATTERN ANALYSIS - WISCONSIN NORTHWOODS")
print("=" * 80)
print("Locations: Phelps WI, Land O'Lakes WI, Watersmeet MI Area")
print()

# Define polar vortex criteria
EXTREME_COLD_THRESHOLD_C = -15.0  # -15°C = 5°F
VERY_COLD_THRESHOLD_C = -20.0     # -20°C = -4°F
MIN_COLD_SNAP_DAYS = 3            # Minimum days for a cold snap

# Get date range
sql = "SELECT MIN(date) as first_date, MAX(date) as last_date FROM snowfall.snowfall_daily"
date_range = engine.query(sql)
print(f"Data Period: {date_range['first_date'][0]} to {date_range['last_date'][0]}")
print()

# 1. Identify extreme cold periods (potential polar vortex events)
print("🌡️  EXTREME COLD EVENTS (Polar Vortex Indicators)")
print("-" * 80)
print(f"Criteria: Daily low temperature ≤ {EXTREME_COLD_THRESHOLD_C}°C ({int(EXTREME_COLD_THRESHOLD_C * 9/5 + 32)}°F)")
print()

sql = """
SELECT
    sd.date,
    s.name,
    ROUND(sd.temp_min_celsius, 1) as temp_low_c,
    ROUND(sd.temp_min_celsius * 9.0/5.0 + 32.0, 1) as temp_low_f,
    ROUND(sd.temp_max_celsius, 1) as temp_high_c,
    ROUND(sd.temp_max_celsius * 9.0/5.0 + 32.0, 1) as temp_high_f,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE sd.temp_min_celsius <= -15.0
  AND s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
ORDER BY sd.temp_min_celsius ASC, sd.date
"""
df_extreme_cold = engine.query(sql)
if len(df_extreme_cold) > 0:
    print(df_extreme_cold.to_string(index=False))
    print(f"\n📊 Found {len(df_extreme_cold)} extreme cold days")
else:
    print("No extreme cold events meeting polar vortex criteria found in dataset.")
print()

# 2. Multi-day cold snaps (3+ consecutive days)
print("\n❄️  SUSTAINED COLD SNAPS (3+ Consecutive Days Below -10°C/14°F)")
print("-" * 80)

sql = """
WITH daily_temps AS (
    SELECT
        sd.date,
        s.name,
        sd.temp_min_celsius,
        sd.temp_max_celsius,
        sd.snowfall_mm,
        ROW_NUMBER() OVER (PARTITION BY s.name ORDER BY sd.date) as rn,
        CAST(sd.date AS DATE) as date_only
    FROM snowfall.snowfall_daily sd
    JOIN snowfall.stations s ON sd.station_id = s.station_id
    WHERE s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
      AND sd.temp_min_celsius <= -10.0
),
cold_groups AS (
    SELECT
        name,
        date,
        temp_min_celsius,
        temp_max_celsius,
        snowfall_mm,
        date_only,
        rn,
        rn - ROW_NUMBER() OVER (PARTITION BY name ORDER BY date) as grp
    FROM daily_temps
)
SELECT
    name,
    MIN(date) as cold_snap_start,
    MAX(date) as cold_snap_end,
    COUNT(*) as days_duration,
    ROUND(MIN(temp_min_celsius), 1) as coldest_low_c,
    ROUND(MIN(temp_min_celsius) * 9.0/5.0 + 32.0, 1) as coldest_low_f,
    ROUND(AVG(temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(temp_max_celsius), 1) as avg_high_c,
    ROUND(SUM(snowfall_mm) / 10.0, 1) as total_snow_cm,
    ROUND(SUM(snowfall_mm) / 25.4, 1) as total_snow_inches
FROM cold_groups
GROUP BY name, grp
HAVING COUNT(*) >= 3
ORDER BY coldest_low_c ASC, days_duration DESC
"""
df_cold_snaps = engine.query(sql)
if len(df_cold_snaps) > 0:
    print(df_cold_snaps.to_string(index=False))
    print(f"\n📊 Found {len(df_cold_snaps)} multi-day cold snaps")
else:
    print("No sustained cold snaps found in dataset.")
print()

# 3. Heavy snow events during cold periods
print("\n🌨️  HEAVY SNOW DURING EXTREME COLD (Snowfall ≥ 5cm during temp ≤ -10°C)")
print("-" * 80)

sql = """
SELECT
    sd.date,
    s.name,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches,
    ROUND(sd.temp_min_celsius, 1) as temp_low_c,
    ROUND(sd.temp_min_celsius * 9.0/5.0 + 32.0, 1) as temp_low_f,
    ROUND(sd.temp_max_celsius, 1) as temp_high_c,
    ROUND(sd.temp_max_celsius * 9.0/5.0 + 32.0, 1) as temp_high_f
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE sd.temp_min_celsius <= -10.0
  AND sd.snowfall_mm >= 50.0
  AND s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
ORDER BY sd.snowfall_mm DESC, sd.temp_min_celsius ASC
"""
df_snow_cold = engine.query(sql)
if len(df_snow_cold) > 0:
    print(df_snow_cold.to_string(index=False))
    print(f"\n📊 Found {len(df_snow_cold)} heavy snow events during extreme cold")
else:
    print("No heavy snow events during extreme cold found in dataset.")
print()

# 4. Temperature and snowfall correlation by month
print("\n📈 MONTHLY TEMPERATURE & SNOWFALL PATTERNS")
print("-" * 80)

sql = """
SELECT
    CASE CAST(MONTH(CAST(date AS DATE)) AS INTEGER)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,
    COUNT(*) as total_days,
    COUNT(CASE WHEN temp_min_celsius <= -15.0 THEN 1 END) as extreme_cold_days,
    COUNT(CASE WHEN temp_min_celsius <= -10.0 THEN 1 END) as very_cold_days,
    ROUND(AVG(temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(temp_max_celsius), 1) as avg_high_c,
    ROUND(SUM(snowfall_mm) / 10.0, 1) as total_snow_cm,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as snow_days
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE CAST(MONTH(CAST(date AS DATE)) AS INTEGER) IN (1,2,3,11,12)
  AND s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
GROUP BY month_name
ORDER BY CASE month_name
    WHEN 'November' THEN 1
    WHEN 'December' THEN 2
    WHEN 'January' THEN 3
    WHEN 'February' THEN 4
    WHEN 'March' THEN 5
END
"""
df_monthly = engine.query(sql)
print(df_monthly.to_string(index=False))
print()

# 5. Coldest days with any snowfall
print("\n🥶 COLDEST DAYS WITH SNOWFALL (Top 15)")
print("-" * 80)

sql = """
SELECT
    sd.date,
    s.name,
    ROUND(sd.temp_min_celsius, 1) as temp_low_c,
    ROUND(sd.temp_min_celsius * 9.0/5.0 + 32.0, 1) as temp_low_f,
    ROUND(sd.temp_max_celsius, 1) as temp_high_c,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE sd.snowfall_mm > 0
  AND s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
ORDER BY sd.temp_min_celsius ASC
LIMIT 15
"""
df_coldest_snow = engine.query(sql)
print(df_coldest_snow.to_string(index=False))
print()

# Summary statistics
print("\n📊 POLAR VORTEX SUMMARY STATISTICS")
print("-" * 80)

sql = """
SELECT
    s.name,
    COUNT(*) as total_days_in_dataset,
    COUNT(CASE WHEN temp_min_celsius <= -20.0 THEN 1 END) as extreme_polar_days,
    COUNT(CASE WHEN temp_min_celsius <= -15.0 THEN 1 END) as polar_vortex_days,
    COUNT(CASE WHEN temp_min_celsius <= -10.0 THEN 1 END) as very_cold_days,
    ROUND(MIN(temp_min_celsius), 1) as record_low_c,
    ROUND(MIN(temp_min_celsius) * 9.0/5.0 + 32.0, 1) as record_low_f,
    ROUND(SUM(CASE WHEN temp_min_celsius <= -10.0 THEN snowfall_mm ELSE 0 END) / 10.0, 1) as snow_during_cold_cm
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
GROUP BY s.name
ORDER BY polar_vortex_days DESC
"""
df_summary = engine.query(sql)
print(df_summary.to_string(index=False))
print()

engine.close()

print("=" * 80)
print("✅ Polar Vortex Analysis Complete!")
print("=" * 80)
print()
print("POLAR VORTEX INDICATORS:")
print("  • Extreme cold: ≤ -15°C (5°F)")
print("  • Very cold: ≤ -20°C (-4°F)")
print("  • Cold snap: 3+ consecutive days ≤ -10°C (14°F)")
print()
