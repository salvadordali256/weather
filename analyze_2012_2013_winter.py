"""
2013-2014 Winter Analysis - Northern Wisconsin
================================================

Compare the 2013-2014 winter season to historical averages and ENSO patterns.

Historical Context:
- 2013-2014: NEUTRAL ENSO transitioning to weak El Niño
- Famous for the "Polar Vortex" events
- One of the coldest winters in decades
"""

import sqlite3
import pandas as pd
from datetime import datetime

db_path = "./northwoods_full_history.db"
conn = sqlite3.connect(db_path)

print("=" * 80)
print("2013-2014 WINTER ANALYSIS - NORTHERN WISCONSIN NORTHWOODS")
print("=" * 80)
print()

# Winter season: July 2013 - June 2014 (meteorological winter)
# Peak winter: December 2013 - February 2014
winter_2013_14_start = "2013-07-01"
winter_2013_14_end = "2014-06-30"

# =============================================================================
# 1. 2013-2014 Winter Totals by Location
# =============================================================================
print("❄️  2013-2014 WINTER SEASON TOTALS")
print("-" * 80)

sql = """
SELECT
    s.name,
    ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 10.0, 1) as total_cm,
    ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches,
    COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as days_with_snow,
    ROUND(MAX(sd.snowfall_mm) / 10.0, 1) as max_daily_cm,
    ROUND(AVG(sd.temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(sd.temp_max_celsius), 1) as avg_high_c,
    MIN(CASE WHEN sd.snowfall_mm > 0 THEN sd.date END) as first_snow,
    MAX(CASE WHEN sd.snowfall_mm > 0 THEN sd.date END) as last_snow
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE sd.date >= ? AND sd.date <= ?
GROUP BY s.name
ORDER BY total_cm DESC
"""
df_2013_14 = pd.read_sql(sql, conn, params=(winter_2013_14_start, winter_2013_14_end))
print(df_2013_14.to_string(index=False))
print()

# =============================================================================
# 2. Compare to Historical Average (1940-2024)
# =============================================================================
print("\n📊 COMPARISON TO HISTORICAL AVERAGE (1940-2024)")
print("-" * 80)

sql = """
WITH winter_seasons AS (
    SELECT
        s.station_id,
        s.name,
        CASE
            WHEN CAST(strftime('%m', sd.date) AS INTEGER) >= 7
            THEN CAST(strftime('%Y', sd.date) AS INTEGER) || '-' ||
                 CAST(CAST(strftime('%Y', sd.date) AS INTEGER) + 1 AS VARCHAR)
            ELSE CAST(CAST(strftime('%Y', sd.date) AS INTEGER) - 1 AS VARCHAR) || '-' ||
                 CAST(strftime('%Y', sd.date) AS VARCHAR)
        END as winter_season,
        SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) as total_mm
    FROM snowfall_daily sd
    JOIN stations s ON sd.station_id = s.station_id
    GROUP BY s.station_id, s.name, winter_season
),
historical_avg AS (
    SELECT
        station_id,
        name,
        AVG(total_mm) as avg_mm,
        MIN(total_mm) as min_mm,
        MAX(total_mm) as max_mm,
        COUNT(*) as num_winters
    FROM winter_seasons
    WHERE winter_season != '2013-2014'  -- Exclude 2013-14 from average
    GROUP BY station_id, name
),
winter_2013_14 AS (
    SELECT
        station_id,
        name,
        total_mm as actual_mm
    FROM winter_seasons
    WHERE winter_season = '2013-2014'
)
SELECT
    ha.name,
    ROUND(w14.actual_mm / 25.4, 1) as actual_inches_2013_14,
    ROUND(ha.avg_mm / 25.4, 1) as historical_avg_inches,
    ROUND((w14.actual_mm - ha.avg_mm) / 25.4, 1) as difference_inches,
    ROUND(((w14.actual_mm - ha.avg_mm) / ha.avg_mm) * 100, 1) as percent_of_normal,
    ROUND(ha.min_mm / 25.4, 1) as record_low_inches,
    ROUND(ha.max_mm / 25.4, 1) as record_high_inches
FROM historical_avg ha
JOIN winter_2013_14 w14 ON ha.station_id = w14.station_id
ORDER BY percent_of_normal DESC
"""
df_comparison = pd.read_sql(sql, conn)
print(df_comparison.to_string(index=False))
print()

# =============================================================================
# 3. Monthly Breakdown for 2013-2014 Winter
# =============================================================================
print("\n📅 MONTHLY BREAKDOWN - 2013-2014 WINTER")
print("-" * 80)

sql = """
SELECT
    CASE CAST(strftime('%m', sd.date) AS INTEGER)
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
    strftime('%Y-%m', sd.date) as year_month,
    ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches,
    COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as days_with_snow,
    ROUND(MAX(sd.snowfall_mm) / 10.0, 1) as max_daily_cm,
    ROUND(AVG(sd.temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(sd.temp_max_celsius), 1) as avg_high_c
FROM snowfall_daily sd
WHERE sd.date >= ? AND sd.date <= ?
AND CAST(strftime('%m', sd.date) AS INTEGER) IN (9,10,11,12,1,2,3,4,5)
GROUP BY year_month
ORDER BY year_month
"""
df_monthly = pd.read_sql(sql, conn, params=(winter_2013_14_start, winter_2013_14_end))
print(df_monthly.to_string(index=False))
print()

# =============================================================================
# 4. Biggest Snowstorms During 2013-2014 Winter
# =============================================================================
print("\n💨 TOP 10 SNOWSTORMS - 2013-2014 WINTER")
print("-" * 80)

sql = """
SELECT
    sd.date,
    s.name,
    ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
    ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches,
    ROUND(sd.temp_max_celsius, 1) as high_c,
    ROUND(sd.temp_min_celsius, 1) as low_c,
    ROUND(sd.snow_depth_mm / 10.0, 1) as snow_depth_cm
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE sd.date >= ? AND sd.date <= ?
AND sd.snowfall_mm > 0
ORDER BY sd.snowfall_mm DESC
LIMIT 10
"""
df_storms = pd.read_sql(sql, conn, params=(winter_2013_14_start, winter_2013_14_end))
print(df_storms.to_string(index=False))
print()

# =============================================================================
# 5. Peak Winter Analysis (Dec-Feb)
# =============================================================================
print("\n🌨️  PEAK WINTER COMPARISON (December - February)")
print("-" * 80)

sql = """
WITH peak_winter_2013_14 AS (
    SELECT
        s.name,
        SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) as total_mm,
        COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as snow_days,
        AVG(sd.temp_min_celsius) as avg_low,
        AVG(sd.temp_max_celsius) as avg_high
    FROM snowfall_daily sd
    JOIN stations s ON sd.station_id = s.station_id
    WHERE sd.date >= '2013-12-01' AND sd.date <= '2014-02-28'
    GROUP BY s.name
),
historical_peak_data AS (
    SELECT
        st.station_id,
        st.name,
        strftime('%Y', sd.date) as year,
        SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) as winter_total
    FROM snowfall_daily sd
    JOIN stations st ON sd.station_id = st.station_id
    WHERE CAST(strftime('%m', sd.date) AS INTEGER) IN (12,1,2)
    AND sd.date < '2013-12-01'
    GROUP BY st.station_id, st.name, year
),
historical_peak AS (
    SELECT
        name,
        AVG(winter_total) as avg_mm
    FROM historical_peak_data
    GROUP BY name
)
SELECT
    pw.name,
    ROUND(pw.total_mm / 25.4, 1) as actual_inches_2013_14,
    ROUND(hp.avg_mm / 25.4, 1) as historical_avg_inches,
    ROUND((pw.total_mm - hp.avg_mm) / 25.4, 1) as difference_inches,
    ROUND(((pw.total_mm - hp.avg_mm) / hp.avg_mm) * 100, 1) as percent_of_normal,
    pw.snow_days,
    ROUND(pw.avg_low, 1) as avg_low_c,
    ROUND(pw.avg_high, 1) as avg_high_c
FROM peak_winter_2013_14 pw
JOIN historical_peak hp ON pw.name = hp.name
ORDER BY percent_of_normal DESC
"""
df_peak = pd.read_sql(sql, conn)
print(df_peak.to_string(index=False))
print()

# =============================================================================
# 6. How Does It Rank?
# =============================================================================
print("\n🏆 HOW DOES 2012-2013 RANK? (All Winter Seasons)")
print("-" * 80)

sql = """
WITH winter_totals AS (
    SELECT
        s.station_id,
        s.name,
        CASE
            WHEN CAST(strftime('%m', sd.date) AS INTEGER) >= 7
            THEN CAST(strftime('%Y', sd.date) AS INTEGER) || '-' ||
                 CAST(CAST(strftime('%Y', sd.date) AS INTEGER) + 1 AS VARCHAR)
            ELSE CAST(CAST(strftime('%Y', sd.date) AS INTEGER) - 1 AS VARCHAR) || '-' ||
                 CAST(strftime('%Y', sd.date) AS VARCHAR)
        END as winter_season,
        SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) as total_mm
    FROM snowfall_daily sd
    JOIN stations s ON sd.station_id = s.station_id
    GROUP BY s.station_id, s.name, winter_season
),
ranked_winters AS (
    SELECT
        name,
        winter_season,
        total_mm,
        ROW_NUMBER() OVER (PARTITION BY name ORDER BY total_mm DESC) as rank,
        COUNT(*) OVER (PARTITION BY name) as total_seasons
    FROM winter_totals
)
SELECT
    name,
    winter_season,
    ROUND(total_mm / 25.4, 1) as total_inches,
    rank as rank_out_of,
    total_seasons,
    ROUND((CAST(rank AS REAL) / total_seasons) * 100, 1) as percentile
FROM ranked_winters
WHERE winter_season = '2013-2014'
ORDER BY rank
"""
df_rank = pd.read_sql(sql, conn)
print(df_rank.to_string(index=False))
print()

# =============================================================================
# 7. ENSO Context & Polar Vortex
# =============================================================================
print("\n🌊 ENSO CONTEXT & POLAR VORTEX")
print("-" * 80)
print("Winter 2013-2014: NEUTRAL ENSO (transitioning to weak El Niño)")
print("  - Weak El Niño developing by late winter")
print("  - FAMOUS for 'Polar Vortex' events in January 2014")
print("  - Multiple Arctic air outbreaks brought record cold")
print("  - One of the coldest winters in decades for Upper Midwest")
print()

# Compare to known ENSO years
print("For context, comparing to known strong ENSO winters:")
print()

enso_winters = {
    "Strong El Niño": ["1997-1998", "2015-2016"],
    "Strong La Niña": ["2010-2011", "2022-2023"],
}

for enso_type, seasons in enso_winters.items():
    print(f"\n{enso_type} Examples:")
    for season in seasons:
        sql = """
        WITH winter_total AS (
            SELECT
                CASE
                    WHEN CAST(strftime('%m', sd.date) AS INTEGER) >= 7
                    THEN CAST(strftime('%Y', sd.date) AS INTEGER) || '-' ||
                         CAST(CAST(strftime('%Y', sd.date) AS INTEGER) + 1 AS VARCHAR)
                    ELSE CAST(CAST(strftime('%Y', sd.date) AS INTEGER) - 1 AS VARCHAR) || '-' ||
                         CAST(strftime('%Y', sd.date) AS VARCHAR)
                END as winter_season,
                ROUND(AVG(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END), 2) as avg_total_mm
            FROM snowfall_daily sd
            WHERE winter_season = ?
        )
        SELECT
            ROUND(avg_total_mm / 25.4, 1) as avg_snowfall_inches
        FROM winter_total
        """
        result = pd.read_sql(sql, conn, params=(season,))
        if not result.empty and result['avg_snowfall_inches'].iloc[0]:
            print(f"  {season}: {result['avg_snowfall_inches'].iloc[0]} inches (avg across all locations)")

conn.close()

print("\n" + "=" * 80)
print("✅ Analysis Complete!")
print("=" * 80)
print()
print("KEY FINDINGS:")
print("- Compare 2013-2014 actual snowfall to historical averages above")
print("- Check the percentile ranking to see if it was an unusual winter")
print("- The 2013-2014 winter was famous for the 'Polar Vortex'")
print("- Despite extreme cold, was snowfall above or below average?")
print()
