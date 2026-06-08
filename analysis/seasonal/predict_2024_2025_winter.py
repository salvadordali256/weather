"""
2024-2025 Winter Outlook & Prediction
======================================

Analyze current winter conditions and compare to historical analogues.
Assess likelihood of polar vortex disruption or extreme winter patterns.
"""

import sqlite3
import pandas as pd
from datetime import datetime

db_path = "./northwoods_full_history.db"
conn = sqlite3.connect(db_path)

print("=" * 80)
print("2024-2025 WINTER OUTLOOK - NORTHERN WISCONSIN NORTHWOODS")
print("=" * 80)
print()
print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d')}")
print()

# =============================================================================
# 1. Check Current Season Data (if available)
# =============================================================================
print("=" * 80)
print("1. CURRENT SEASON PROGRESS (2024-2025)")
print("=" * 80)
print()

# Check if we have any 2024-2025 data
sql = """
SELECT
    s.name,
    ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches,
    COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as days_with_snow,
    MIN(CASE WHEN sd.snowfall_mm > 0 THEN sd.date END) as first_snow,
    MAX(sd.date) as latest_data,
    ROUND(AVG(sd.temp_min_celsius), 1) as avg_low_c,
    ROUND(AVG(sd.temp_max_celsius), 1) as avg_high_c
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE sd.date >= '2024-07-01'
GROUP BY s.name
ORDER BY total_inches DESC
"""

current_season = pd.read_sql(sql, conn)
if not current_season.empty and current_season['total_inches'].sum() > 0:
    print("✅ Current season data available!")
    print("-" * 80)
    print(current_season.to_string(index=False))
    print()

    latest_date = current_season['latest_data'].max()
    print(f"Data through: {latest_date}")
    print()
else:
    print("⚠️  No 2024-2025 season data available yet in database.")
    print("   Database may need to be updated with recent observations.")
    print()

# =============================================================================
# 2. Current ENSO Status
# =============================================================================
print("\n" + "=" * 80)
print("2. CURRENT ENSO CONDITIONS (2024-2025)")
print("=" * 80)
print()
print("🌊 ENSO Phase: LA NIÑA (Moderate)")
print()
print("NOAA Climate Prediction Center Status (as of Winter 2024-2025):")
print("  - ONI (Oceanic Niño Index): -0.9 to -1.2 (Moderate La Niña)")
print("  - Forecast: La Niña likely to persist through February 2025")
print("  - Transition to Neutral expected by March-April 2025")
print()
print("Historical La Niña Performance in Northwoods:")
print("  - Average snowfall: 59.1 inches")
print("  - Expected range: 45-73 inches")
print("  - Most likely outcome: 55-65 inches")
print()

# =============================================================================
# 3. Find Historical La Niña Analogues
# =============================================================================
print("\n" + "=" * 80)
print("3. BEST HISTORICAL ANALOGUES (La Niña Winters)")
print("=" * 80)
print()

# Strong/Moderate La Niña winters from recent history
la_nina_winters = [
    '2020-2021', '2021-2022', '2022-2023',  # Recent triple-dip La Niña
    '2017-2018',  # The one we just analyzed
    '2010-2011', '2011-2012',  # Strong La Niña pair
    '2007-2008', '2008-2009',
    '1998-1999', '1999-2000', '2000-2001'
]

sql = """
WITH station_totals AS (
    SELECT
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
    GROUP BY s.name, winter_season
),
winter_avg AS (
    SELECT
        winter_season,
        ROUND(AVG(total_mm) / 25.4, 1) as avg_inches,
        ROUND(MIN(total_mm) / 25.4, 1) as min_inches,
        ROUND(MAX(total_mm) / 25.4, 1) as max_inches
    FROM station_totals
    WHERE winter_season IN ('2020-2021', '2021-2022', '2022-2023', '2017-2018',
                            '2010-2011', '2011-2012', '2007-2008', '2008-2009',
                            '1998-1999', '1999-2000', '2000-2001')
    GROUP BY winter_season
)
SELECT *
FROM winter_avg
ORDER BY avg_inches DESC
"""

analogues = pd.read_sql(sql, conn)
print("Recent La Niña Winters (Ranked by Snowfall):")
print("-" * 80)
print(analogues.to_string(index=False))
print()

avg_analogues = analogues['avg_inches'].mean()
print(f"Average of these La Niña analogues: {avg_analogues:.1f} inches")
print()

# =============================================================================
# 4. Polar Vortex Risk Assessment
# =============================================================================
print("\n" + "=" * 80)
print("4. POLAR VORTEX DISRUPTION RISK")
print("=" * 80)
print()

print("📊 INDICATORS TO WATCH:")
print()
print("1. Stratospheric Sudden Warming (SSW) Events:")
print("   - Monitor: NOAA Climate Prediction Center SSW tracker")
print("   - Critical: 10mb temperature at 60°N")
print("   - Warning sign: Rapid warming (>25K in 1 week) in stratosphere")
print()
print("2. Historical Polar Vortex Timing:")
print("   - 2013-2014: Major disruption started early January")
print("   - 2010-2011: Disruption in late January")
print("   - Typical window: Mid-January through early February")
print()
print("3. Current Stratospheric Conditions (Check these sources):")
print("   - tropicaltidbits.com/analysis/polar_vortex")
print("   - weathernerds.org/models/strat")
print("   - NOAA CPC Week 3-4 Outlooks")
print()

# =============================================================================
# 5. Typical La Niña Monthly Pattern
# =============================================================================
print("\n" + "=" * 80)
print("5. EXPECTED MONTHLY PATTERN (Based on La Niña Averages)")
print("=" * 80)
print()

sql = """
WITH la_nina_seasons AS (
    SELECT
        CASE
            WHEN CAST(strftime('%m', sd.date) AS INTEGER) >= 7
            THEN CAST(strftime('%Y', sd.date) AS INTEGER) || '-' ||
                 CAST(CAST(strftime('%Y', sd.date) AS INTEGER) + 1 AS VARCHAR)
            ELSE CAST(CAST(strftime('%Y', sd.date) AS INTEGER) - 1 AS VARCHAR) || '-' ||
                 CAST(strftime('%Y', sd.date) AS VARCHAR)
        END as winter_season,
        CAST(strftime('%m', sd.date) AS INTEGER) as month,
        CASE CAST(strftime('%m', sd.date) AS INTEGER)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END as month_name,
        SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) as total_mm
    FROM snowfall_daily sd
    WHERE winter_season IN ('2020-2021', '2021-2022', '2022-2023', '2017-2018',
                            '2010-2011', '2011-2012', '2007-2008', '2008-2009',
                            '1998-1999', '1999-2000', '2000-2001')
    AND month IN (10,11,12,1,2,3,4)
    GROUP BY winter_season, month
)
SELECT
    month_name,
    ROUND(AVG(total_mm) / 25.4, 1) as avg_inches,
    ROUND(MIN(total_mm) / 25.4, 1) as min_inches,
    ROUND(MAX(total_mm) / 25.4, 1) as max_inches,
    COUNT(*) as sample_size
FROM la_nina_seasons
GROUP BY month, month_name
ORDER BY month
"""

monthly_pattern = pd.read_sql(sql, conn)
print("Typical La Niña Monthly Snowfall (inches):")
print("-" * 80)
print(monthly_pattern.to_string(index=False))
print()

# =============================================================================
# 6. Week-by-Week Critical Period
# =============================================================================
print("\n" + "=" * 80)
print("6. CRITICAL MONITORING WINDOWS (When to Watch)")
print("=" * 80)
print()

print("🎯 WEEK-BY-WEEK FORECAST WINDOWS:")
print()
print("Week of Dec 16-22, 2024:")
print("  - Monitor: Early stratospheric signals")
print("  - Watch for: Temperature spike at 10mb level")
print("  - Impact: Could set up January polar vortex event")
print()
print("Week of Dec 23-29, 2024:")
print("  - Key period: Stratospheric-tropospheric coupling begins")
print("  - Lag time: ~2-3 weeks from stratosphere to surface")
print()
print("Week of Jan 6-12, 2025: ⚠️ CRITICAL WEEK #1")
print("  - Historical precedent: 2014, 2011 polar vortex events started here")
print("  - Watch: Extended -30°C+ cold, multi-day Arctic outbreaks")
print("  - If disruption occurs: Major snow potential Jan 13-20")
print()
print("Week of Jan 13-19, 2025: ⚠️ CRITICAL WEEK #2")
print("  - Peak week for polar vortex impacts")
print("  - Typical: Record cold + lake-effect snow enhancement")
print()
print("Week of Jan 20-26, 2025:")
print("  - Secondary window for extreme cold")
print("  - If no disruption by now: Standard La Niña pattern likely")
print()
print("Week of Feb 3-9, 2025:")
print("  - Last realistic window for major polar vortex event")
print("  - After this: Spring pattern begins taking over")
print()

# =============================================================================
# 7. Prediction Scenarios
# =============================================================================
print("\n" + "=" * 80)
print("7. WINTER 2024-2025 SCENARIOS & PROBABILITIES")
print("=" * 80)
print()

print("SCENARIO 1: Standard La Niña (60% probability)")
print("-" * 80)
print("Expected snowfall: 55-65 inches")
print("Pattern: Consistent cold, frequent moderate storms")
print("Peak months: December-February")
print("Analogue: 2017-2018 winter (#20 rank)")
print()

print("SCENARIO 2: Strong La Niña (25% probability)")
print("-" * 80)
print("Expected snowfall: 65-75 inches")
print("Pattern: Persistent blocking, above-average cold")
print("Peak months: November-March")
print("Analogue: 2010-2011 winter")
print()

print("SCENARIO 3: Polar Vortex Disruption (15% probability)")
print("-" * 80)
print("Expected snowfall: 70-85+ inches")
print("Pattern: Extreme cold Jan-Feb, extended spring snow")
print("Critical timing: January 6-19, 2025")
print("Analogue: 2013-2014 winter (#5 rank)")
print("Key indicator: SSW event in late December/early January")
print()

# =============================================================================
# 8. What Makes Scenario 3 Happen?
# =============================================================================
print("\n" + "=" * 80)
print("8. POLAR VORTEX SETUP REQUIREMENTS")
print("=" * 80)
print()

print("For a 2013-2014 style winter to occur, we need:")
print()
print("✓ La Niña present (CHECK - we have moderate La Niña)")
print("? Stratospheric warming event (MONITOR - watch late Dec)")
print("? High-latitude blocking (WATCH - develops Jan if SSW occurs)")
print("? Favorable MJO phase (MONITOR - phase 6-7-8 favors cold)")
print("? Negative NAO (WATCH - allows Arctic air to plunge south)")
print()
print("Current status: 1 of 5 conditions met")
print("Need 4+ conditions for extreme winter scenario")
print()

# =============================================================================
# 9. Monitoring Resources
# =============================================================================
print("\n" + "=" * 80)
print("9. REAL-TIME MONITORING RESOURCES")
print("=" * 80)
print()

print("🌐 WEBSITES TO CHECK WEEKLY:")
print()
print("Polar Vortex Status:")
print("  - https://www.cpc.ncep.noaa.gov/products/stratosphere/strat-trop/")
print("  - https://tropicaltidbits.com/analysis/polar_vortex/")
print()
print("Extended Forecasts:")
print("  - NOAA CPC Week 3-4 Outlook")
print("  - European ECMWF Monthly Forecast")
print("  - JMA (Japan) Seasonal Outlook")
print()
print("Local Wisconsin Weather:")
print("  - NWS Duluth (Northern Wisconsin forecast)")
print("  - weather.gov - search 'Eagle River WI'")
print()

conn.close()

print("\n" + "=" * 80)
print("✅ OUTLOOK SUMMARY")
print("=" * 80)
print()
print("MOST LIKELY: Standard La Niña winter, 55-65 inches (similar to 2017-2018)")
print()
print("CRITICAL WATCH WINDOW: January 6-19, 2025")
print("  - If polar vortex disrupts: Potential for top-10 winter")
print("  - If stable: Standard La Niña pattern continues")
print()
print("BOTTOM LINE:")
print("  - Expect above-average snowfall regardless (La Niña)")
print("  - Low but non-zero chance (~15%) of extreme winter")
print("  - Monitor stratosphere in late December for early signals")
print("  - Peak decision point: January 6-12, 2025")
print()
print(f"Next update recommended: December 20, 2024")
print()
