"""
85-Year Climate Trend Analysis
Northern Wisconsin Snowfall (1940-2025)
"""

from snowfall_duckdb import SnowfallDuckDB
import pandas as pd

engine = SnowfallDuckDB("./northwoods_full_history.db")

print("=" * 80)
print("85-YEAR SNOWFALL CLIMATE ANALYSIS")
print("Northern Wisconsin: 1940-2025")
print("=" * 80)
print()

# 1. Overall summary by location
print("📍 LOCATION SUMMARIES (85 Years)")
print("-" * 80)
sql = """
SELECT
    s.name,
    ss.total_years as years,
    ROUND(ss.avg_annual_snowfall_mm / 10.0, 1) as avg_annual_cm,
    ROUND(ss.avg_annual_snowfall_mm / 25.4, 1) as avg_annual_inches,
    ss.days_with_snow as total_snow_days,
    ROUND(ss.max_daily_snowfall_mm / 10.0, 1) as record_daily_cm,
    ss.max_daily_snowfall_date as record_date
FROM snowfall.station_summaries ss
JOIN snowfall.stations s ON ss.station_id = s.station_id
ORDER BY ss.avg_annual_snowfall_mm DESC
"""
print(engine.query(sql).to_string(index=False))
print()

# 2. Decade-by-decade trends
print("\n📊 SNOWFALL BY DECADE (1940s - 2020s)")
print("-" * 80)
sql = """
SELECT
    (CAST(YEAR(CAST(date AS DATE)) AS INTEGER) / 10) * 10 as decade,
    COUNT(DISTINCT station_id) as stations,
    ROUND(AVG(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0, 2) as avg_daily_cm,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 /
          COUNT(DISTINCT YEAR(CAST(date AS DATE))), 1) as avg_annual_cm,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as total_snow_days,
    ROUND(MAX(snowfall_mm) / 10.0, 1) as max_daily_cm
FROM snowfall.snowfall_daily
GROUP BY decade
ORDER BY decade
"""
print(engine.query(sql).to_string(index=False))
print()

# 3. Climate change: 1940s-1970s vs 1990s-2020s
print("\n🌡️  CLIMATE CHANGE COMPARISON")
print("-" * 80)
sql = """
WITH historical AS (
    SELECT
        'Historical (1940-1979)' as period,
        ROUND(AVG(annual_total), 1) as avg_annual_cm,
        ROUND(STDDEV(annual_total), 1) as stddev_cm,
        MIN(annual_total) as min_annual_cm,
        MAX(annual_total) as max_annual_cm
    FROM (
        SELECT
            YEAR(CAST(date AS DATE)) as year,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as annual_total
        FROM snowfall.snowfall_daily
        WHERE YEAR(CAST(date AS DATE)) BETWEEN 1940 AND 1979
        GROUP BY station_id, year
    )
),
recent AS (
    SELECT
        'Recent (1990-2025)' as period,
        ROUND(AVG(annual_total), 1) as avg_annual_cm,
        ROUND(STDDEV(annual_total), 1) as stddev_cm,
        MIN(annual_total) as min_annual_cm,
        MAX(annual_total) as max_annual_cm
    FROM (
        SELECT
            YEAR(CAST(date AS DATE)) as year,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as annual_total
        FROM snowfall.snowfall_daily
        WHERE YEAR(CAST(date AS DATE)) BETWEEN 1990 AND 2025
        GROUP BY station_id, year
    )
)
SELECT * FROM historical
UNION ALL
SELECT * FROM recent
"""
df = engine.query(sql)
print(df.to_string(index=False))

if len(df) == 2:
    change = df.iloc[1]['avg_annual_cm'] - df.iloc[0]['avg_annual_cm']
    pct_change = (change / df.iloc[0]['avg_annual_cm']) * 100
    print()
    print(f"Change: {change:+.1f} cm ({pct_change:+.1f}%)")
    if change > 0:
        print(f"📈 Snowfall has INCREASED by {abs(change):.1f} cm")
    else:
        print(f"📉 Snowfall has DECREASED by {abs(change):.1f} cm")
print()

# 4. Year-by-year trends
print("\n📈 ANNUAL SNOWFALL TRENDS (All Years)")
print("-" * 80)
sql = """
SELECT
    CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
    ROUND(AVG(annual_total), 1) as avg_cm,
    ROUND(MIN(annual_total), 1) as min_cm,
    ROUND(MAX(annual_total), 1) as max_cm
FROM (
    SELECT
        date,
        station_id,
        SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) OVER (
            PARTITION BY station_id, YEAR(CAST(date AS DATE))
        ) / 10.0 as annual_total
    FROM snowfall.snowfall_daily
)
GROUP BY year
ORDER BY year DESC
LIMIT 20
"""
print(engine.query(sql).to_string(index=False))
print("... (showing recent 20 years)")
print()

# 5. Extreme events
print("\n💨 TOP 20 BIGGEST SNOWSTORMS (1940-2025)")
print("-" * 80)
sql = """
SELECT
    sd.date,
    s.name,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches,
    CAST(YEAR(CAST(sd.date AS DATE)) AS INTEGER) as year,
    CAST((CAST(YEAR(CAST(sd.date AS DATE)) AS INTEGER) / 10) * 10 AS INTEGER) as decade
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE sd.snowfall_mm > 0
ORDER BY sd.snowfall_mm DESC
LIMIT 20
"""
print(engine.query(sql).to_string(index=False))
print()

# 6. Snowiest and least snowy years
print("\n🏆 RECORD YEARS")
print("-" * 80)
sql = """
WITH yearly_totals AS (
    SELECT
        CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
        station_id,
        SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as total_cm
    FROM snowfall.snowfall_daily
    GROUP BY year, station_id
)
SELECT
    year,
    ROUND(AVG(total_cm), 1) as avg_cm,
    ROUND(MAX(total_cm), 1) as max_cm,
    ROUND(MIN(total_cm), 1) as min_cm
FROM yearly_totals
GROUP BY year
ORDER BY avg_cm DESC
LIMIT 10
"""
print("Snowiest Years (Top 10):")
print(engine.query(sql).to_string(index=False))
print()

sql = """
WITH yearly_totals AS (
    SELECT
        CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
        station_id,
        SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as total_cm
    FROM snowfall.snowfall_daily
    GROUP BY year, station_id
)
SELECT
    year,
    ROUND(AVG(total_cm), 1) as avg_cm,
    ROUND(MAX(total_cm), 1) as max_cm,
    ROUND(MIN(total_cm), 1) as min_cm
FROM yearly_totals
GROUP BY year
ORDER BY avg_cm ASC
LIMIT 10
"""
print("Least Snowy Years (Bottom 10):")
print(engine.query(sql).to_string(index=False))
print()

engine.close()

print("=" * 80)
print("✅ 85-Year Climate Analysis Complete!")
print("=" * 80)
print()
print("Database: ./northwoods_full_history.db")
print("Period: 1940-2025 (85 years)")
print("Locations: Eagle River, Land O'Lakes, Phelps")
print()
