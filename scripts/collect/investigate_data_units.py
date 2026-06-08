"""
Investigate Data Units - Find the Problem
==========================================

Check if there's a unit conversion issue or data problem
"""

import sqlite3
import pandas as pd

db_path = "./up_michigan_snowfall.db"
conn = sqlite3.connect(db_path)

print("=" * 80)
print("DATA INVESTIGATION - Finding the Unit Issue")
print("=" * 80)
print()

# Check a known heavy snow month - January 2014 (polar vortex)
print("1. JANUARY 2014 - POLAR VORTEX MONTH")
print("=" * 80)
print()

sql = """
SELECT
    sd.date,
    s.name,
    sd.snowfall_mm,
    ROUND(sd.snowfall_mm / 25.4, 2) as snowfall_inches,
    sd.temp_min_celsius,
    sd.temp_max_celsius
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE s.name = 'Ironwood, MI'
AND sd.date >= '2014-01-01' AND sd.date <= '2014-01-31'
AND sd.snowfall_mm > 0
ORDER BY sd.date
"""

jan_2014 = pd.read_sql(sql, conn)
print("Ironwood MI - January 2014 daily snowfall:")
print("-" * 80)
print(jan_2014.to_string(index=False))
print()
print(f"Total for month: {jan_2014['snowfall_inches'].sum():.1f} inches")
print()

# Check a known big storm - November 26, 2025 (recent)
print("\n2. RECENT BIG STORM - November 26, 2025")
print("=" * 80)
print()

sql = """
SELECT
    sd.date,
    s.name,
    sd.snowfall_mm,
    ROUND(sd.snowfall_mm / 25.4, 2) as snowfall_inches,
    sd.snow_depth_mm,
    ROUND(sd.snow_depth_mm / 25.4, 2) as snow_depth_inches
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE sd.date = '2025-11-26'
ORDER BY sd.snowfall_mm DESC
"""

big_storm = pd.read_sql(sql, conn)
print(big_storm.to_string(index=False))
print()

# Check Open-Meteo documentation
print("\n3. OPEN-METEO UNITS CHECK")
print("=" * 80)
print()
print("According to Open-Meteo Archive API documentation:")
print()
print("Parameter: snowfall_sum")
print("Description: Sum of daily snowfall")
print("Unit: cm (centimeters)")
print()
print("OUR CONVERSION:")
print("  - We receive: snowfall_sum in CM")
print("  - We convert: cm * 10 = mm")
print("  - We store: mm in database")
print("  - We display: mm / 25.4 = inches")
print()
print("WAIT - Let's check if Open-Meteo actually returns CM or MM...")
print()

# Sample some raw values
sql = """
SELECT
    sd.date,
    s.name,
    sd.snowfall_mm as stored_value_mm,
    ROUND(sd.snowfall_mm / 10.0, 2) as if_this_was_cm,
    ROUND(sd.snowfall_mm / 25.4, 2) as as_inches_from_mm
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE sd.snowfall_mm > 100
ORDER BY sd.snowfall_mm DESC
LIMIT 10
"""

samples = pd.read_sql(sql, conn)
print("SAMPLE HIGH SNOWFALL VALUES:")
print("-" * 80)
print(samples.to_string(index=False))
print()

print("\n4. HYPOTHESIS")
print("=" * 80)
print()
print("If Open-Meteo returns CM but we're treating it as MM:")
print("  - We multiply by 10 (thinking cm->mm)")
print("  - But it might already BE in mm")
print("  - Result: We're OVER-storing by 10x")
print("  - Then dividing by 25.4 gives us wrong inches")
print()
print("OR... Open-Meteo might return mm directly, not cm")
print("Let's check what the documentation says vs what we get...")
print()

# Check metadata from our collection
print("\n5. COLLECTION METADATA")
print("=" * 80)
print()

sql = """
SELECT
    s.name,
    s.data_source,
    MIN(sd.date) as first_date,
    MAX(sd.date) as last_date,
    COUNT(*) as total_records,
    ROUND(AVG(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm END), 2) as avg_snowfall_when_snowing,
    ROUND(MAX(sd.snowfall_mm), 2) as max_snowfall
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
GROUP BY s.name
"""

metadata = pd.read_sql(sql, conn)
print(metadata.to_string(index=False))
print()

conn.close()

print("\n" + "=" * 80)
print("DIAGNOSIS")
print("=" * 80)
print()
print("The max snowfall values (120-135 mm) converted to inches:")
print("  - 135 mm / 25.4 = 5.3 inches")
print("  - But if it should be 135 cm converted:")
print("  - 135 cm = 1350 mm = 53.1 inches")
print()
print("This seems too high for a single day...")
print()
print("OR if Open-Meteo returns in cm and we should NOT multiply by 10:")
print("  - 13.5 cm (not 135 mm)")
print("  - 13.5 cm = 135 mm = 5.3 inches ✓ This makes sense!")
print()
print("LIKELY ISSUE: Open-Meteo returns cm, we multiply by 10 to get mm")
print("But then our seasonal totals are too low by a factor of...")
print()
print("Wait, let me recalculate to find the actual issue...")
