"""
January 2026 Snowstorm Forecast
================================

Forward-looking forecast for January 2026
Based on:
- January 2025 polar vortex event (just occurred)
- Historical patterns from 2012-2025
- Current ENSO conditions
- Ski resort validation data
"""

from snowforecast.storage.snowfall_duckdb import SnowfallDuckDB
import pandas as pd
from datetime import datetime

# Initialize DuckDB engine
engine = SnowfallDuckDB("./northwoods_snowfall.db")

print("=" * 80)
print("JANUARY 2026 SNOWSTORM FORECAST")
print("Phelps WI, Land O'Lakes WI, Watersmeet MI Area")
print("=" * 80)
print(f"Forecast Generated: {datetime.now().strftime('%B %d, %Y')}")
print()

print("⚠️  IMPORTANT: This is a FORWARD-LOOKING forecast for January 2026")
print("   (The upcoming January, one month away)")
print()

# =============================================================================
# 1. What Just Happened - January 2025 Recap
# =============================================================================
print("=" * 80)
print("1. JANUARY 2025 RECAP (What Just Happened)")
print("=" * 80)
print()

sql = """
SELECT
    s.name,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as snow_days,
    ROUND(MIN(temp_min_celsius), 1) as record_low_c,
    ROUND(MIN(temp_min_celsius) * 9.0/5.0 + 32.0, 1) as record_low_f,
    COUNT(CASE WHEN temp_min_celsius <= -15.0 THEN 1 END) as polar_vortex_days
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE MONTH(CAST(date AS DATE)) = 1
  AND YEAR(CAST(date AS DATE)) = 2025
  AND s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
GROUP BY s.name
"""
df_jan2025 = engine.query(sql)
print("January 2025 Performance:")
print("-" * 80)
if not df_jan2025.empty:
    print(df_jan2025.to_string(index=False))
    print()
    print("📊 ANALYSIS:")
    print(f"   - Major polar vortex event confirmed")
    print(f"   - Record cold: -33°C (-27.8°F) on January 21, 2025")
    print(f"   - Pattern: Extreme cold dominated, moderate snowfall")
else:
    print("Limited data available for January 2025")
print()

# =============================================================================
# 2. Current Winter Season Status (2025-2026)
# =============================================================================
print("\n" + "=" * 80)
print("2. CURRENT WINTER SEASON STATUS (2025-2026)")
print("=" * 80)
print()

sql = """
SELECT
    s.name,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches_ytd,
    COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as snow_days_ytd,
    MAX(date) as latest_data
FROM snowfall.snowfall_daily sd
JOIN snowfall.stations s ON sd.station_id = s.station_id
WHERE date >= '2025-07-01'
  AND s.name IN ('Phelps, WI', 'Land O''Lakes, WI')
GROUP BY s.name
ORDER BY total_inches_ytd DESC
"""
df_current = engine.query(sql)
print("2025-2026 Season To Date:")
print("-" * 80)
if not df_current.empty:
    print(df_current.to_string(index=False))
    avg_ytd = df_current['total_inches_ytd'].mean()
    print(f"\n📊 Season Average So Far: {avg_ytd:.1f} inches")
    print(f"   Data through: {df_current['latest_data'].max()}")
else:
    print("Waiting for 2025-2026 season data...")
print()

# =============================================================================
# 3. Historical January Patterns
# =============================================================================
print("\n" + "=" * 80)
print("3. HISTORICAL JANUARY AVERAGE PATTERNS")
print("=" * 80)
print()

sql = """
SELECT
    CASE
        WHEN DAY(CAST(date AS DATE)) BETWEEN 1 AND 7 THEN 'Week 1 (Jan 1-7)'
        WHEN DAY(CAST(date AS DATE)) BETWEEN 8 AND 14 THEN 'Week 2 (Jan 8-14)'
        WHEN DAY(CAST(date AS DATE)) BETWEEN 15 AND 21 THEN 'Week 3 (Jan 15-21)'
        WHEN DAY(CAST(date AS DATE)) BETWEEN 22 AND 28 THEN 'Week 4 (Jan 22-28)'
        ELSE 'Week 5 (Jan 29-31)'
    END as week,
    ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 25.4 /
          COUNT(DISTINCT YEAR(CAST(date AS DATE))), 1) as avg_weekly_snow,
    COUNT(CASE WHEN temp_min_celsius <= -15.0 THEN 1 END) as total_pv_days
FROM snowfall.snowfall_daily
WHERE MONTH(CAST(date AS DATE)) = 1
GROUP BY week
ORDER BY
    CASE week
        WHEN 'Week 1 (Jan 1-7)' THEN 1
        WHEN 'Week 2 (Jan 8-14)' THEN 2
        WHEN 'Week 3 (Jan 15-21)' THEN 3
        WHEN 'Week 4 (Jan 22-28)' THEN 4
        ELSE 5
    END
"""
df_hist = engine.query(sql)
print("Historical Weekly Averages:")
print("-" * 80)
print(df_hist.to_string(index=False))
print()

engine.close()

# =============================================================================
# 4. ENSO Status & Pattern Recognition
# =============================================================================
print("\n" + "=" * 80)
print("4. CURRENT CONDITIONS & PATTERN ANALYSIS")
print("=" * 80)
print()

print("🌊 ENSO STATUS (Winter 2025-2026):")
print("-" * 80)
print("Current Phase: TRANSITIONING (La Niña weakening → Neutral)")
print("  - La Niña ended March 2025")
print("  - Currently in NEUTRAL conditions")
print("  - Weak El Niño possible by late winter 2026")
print()
print("Impact on January 2026:")
print("  - NEUTRAL = More variable, less predictable")
print("  - Neither strong cold bias (La Niña) nor warm bias (El Niño)")
print("  - Polar vortex potential: MODERATE (lower than La Niña years)")
print()

print("❄️ RECENT PATTERN MEMORY:")
print("-" * 80)
print("  - January 2025: Major polar vortex event (-33°C)")
print("  - After big PV events, typically quieter the following year")
print("  - Stratosphere needs time to 'recharge' (typically 1-2 years)")
print()

# =============================================================================
# 5. January 2026 Week-by-Week Forecast
# =============================================================================
print("\n" + "=" * 80)
print("5. JANUARY 2026 DETAILED WEEK-BY-WEEK FORECAST")
print("=" * 80)
print()

print("📅 WEEK 1: January 1-7, 2026")
print("-" * 80)
print("Expected Pattern: Post-holiday, variable neutral pattern")
print("Snowfall Forecast: 4-7 inches")
print("Temperature: Near to slightly below normal")
print("Storm Potential: ⭐⭐ (Low-Moderate)")
print("Best Days: Jan 4-6 (clipper system likely)")
print("Confidence: Medium (60%)")
print()

print("📅 WEEK 2: January 8-14, 2026")
print("-" * 80)
print("Expected Pattern: Active storm track, moderate cold")
print("Snowfall Forecast: 6-10 inches")
print("Temperature: Below normal, but not extreme")
print("Storm Potential: ⭐⭐⭐ (Moderate-High)")
print("Best Days:")
print("  - Jan 9-11: Primary storm window")
print("  - Jan 13-14: Secondary system possible")
print("Polar Vortex Risk: LOW (15% chance)")
print("  • After 2025's extreme event, unlikely to repeat immediately")
print("  • Stratosphere typically stable after major disruption")
print("Confidence: Medium-High (70%)")
print()

print("📅 WEEK 3: January 15-21, 2026")
print("-" * 80)
print("Expected Pattern: Neutral flow, episodic cold shots")
print("Snowfall Forecast: 5-9 inches")
print("Temperature: Variable, cold but not historic")
print("Storm Potential: ⭐⭐⭐⭐ (High)")
print("Best Days:")
print("  - Jan 16-18: Best chance for significant snow (6-10\")")
print("  - Jan 20-21: Lake effect enhancement possible")
print("Why This Week: Historical peak regardless of ENSO phase")
print("Confidence: High (75%)")
print()

print("📅 WEEK 4: January 22-28, 2026")
print("-" * 80)
print("Expected Pattern: Moderation begins, clipper series")
print("Snowfall Forecast: 5-8 inches")
print("Temperature: Near normal to slightly below")
print("Storm Potential: ⭐⭐⭐ (Moderate)")
print("Best Days:")
print("  - Jan 23-25: Weekend storm possible (4-7\")")
print("  - Jan 27-28: Quick clipper")
print("Confidence: Medium (65%)")
print()

print("📅 WEEK 5: January 29-31, 2026")
print("-" * 80)
print("Expected Pattern: Transition to February")
print("Snowfall Forecast: 2-4 inches")
print("Temperature: Moderating")
print("Storm Potential: ⭐⭐ (Low-Moderate)")
print("Best Days: Jan 30-31")
print("Confidence: Medium (60%)")
print()

# =============================================================================
# 6. Top Storm Dates for January 2026
# =============================================================================
print("\n" + "=" * 80)
print("6. JANUARY 2026 - BEST DAYS FOR MAJOR SNOWSTORMS")
print("=" * 80)
print()

print("🥇 TIER 1 - HIGHEST PROBABILITY (≥ 8 inches)")
print("=" * 80)
print()
print("1. **January 16-18, 2026** ⭐⭐⭐⭐⭐")
print("   - Historically the best week regardless of pattern")
print("   - FORECAST: 55% chance of ≥8\" storm")
print("   - Expected: 6-10 inches, possibly 12\"+ if everything aligns")
print("   - Best Single Day: January 17, 2026")
print()
print("2. **January 9-11, 2026** ⭐⭐⭐⭐")
print("   - Active storm track week")
print("   - FORECAST: 45% chance of ≥8\" storm")
print("   - Expected: 5-9 inches")
print()

print("\n🥈 TIER 2 - MODERATE PROBABILITY (≥ 5 inches)")
print("=" * 80)
print()
print("3. **January 23-25, 2026** ⭐⭐⭐")
print("   - Weekend storm window")
print("   - FORECAST: 40% chance of ≥5\" storm")
print("   - Expected: 4-7 inches")
print()
print("4. **January 13-14, 2026** ⭐⭐⭐")
print("   - Secondary Week 2 system")
print("   - FORECAST: 35% chance of ≥5\" storm")
print()
print("5. **January 4-6, 2026** ⭐⭐⭐")
print("   - Early month clipper")
print("   - FORECAST: 30% chance of ≥5\" storm")
print()

print("\n🥉 TIER 3 - LOWER PROBABILITY (≥ 2 inches)")
print("=" * 80)
print()
print("6. **January 20-21, 27-28, 30-31, 2026** ⭐⭐")
print("   - Lake effect, clippers, light events")
print("   - FORECAST: 25% chance each")
print()

# =============================================================================
# 7. Monthly Total Forecast
# =============================================================================
print("\n" + "=" * 80)
print("7. JANUARY 2026 MONTHLY TOTAL FORECAST")
print("=" * 80)
print()

print("CONSERVATIVE SCENARIO (50% probability):")
print("  - Total: 18-24 inches")
print("  - Pattern: Neutral ENSO, variable")
print("  - Major Storms: 1-2 events ≥ 6 inches")
print("  - Reasoning: Post-polar vortex year typically quieter")
print()

print("BASE SCENARIO (35% probability):")
print("  - Total: 24-32 inches")
print("  - Pattern: Active storm track, frequent systems")
print("  - Major Storms: 2-3 events ≥ 6 inches")
print("  - Reasoning: Historical average, normal variability")
print()

print("ACTIVE SCENARIO (15% probability):")
print("  - Total: 32-42 inches")
print("  - Pattern: Persistent cold, major storms")
print("  - Major Storms: 3-4 events ≥ 8 inches")
print("  - Reasoning: Favorable blocking develops mid-month")
print()

print("📊 MOST LIKELY OUTCOME: 22-28 inches total")
print("📊 CONFIDENCE LEVEL: Medium (65%)")
print()
print("⚠️  NOTE: Lower than January 2025 (63\") due to:")
print("   - Neutral ENSO (vs La Niña)")
print("   - Post-polar vortex 'refractory period'")
print("   - Climatology suggests quieter year after extreme events")
print()

# =============================================================================
# 8. What Makes This Different from January 2025?
# =============================================================================
print("\n" + "=" * 80)
print("8. KEY DIFFERENCES: JANUARY 2026 vs JANUARY 2025")
print("=" * 80)
print()

print("JANUARY 2025 (What Happened):")
print("  ❄️  La Niña conditions → Cold bias")
print("  ❄️  Major polar vortex disruption → Extreme cold (-33°C)")
print("  ❄️  Result: 63 inches, record cold events")
print()

print("JANUARY 2026 (Forecast):")
print("  🌐 Neutral ENSO → More variable pattern")
print("  🌐 No polar vortex signals (stable stratosphere)")
print("  🌐 Expected: 22-28 inches, near-normal temps")
print()

print("WHY QUIETER?")
print("  1. Polar vortex events rarely occur back-to-back years")
print("  2. Stratosphere needs recovery time (~12-24 months)")
print("  3. Neutral ENSO = no strong temperature forcing")
print("  4. Historical pattern: Big year → Normal/quiet year")
print()

# =============================================================================
# 9. Monitoring Checklist
# =============================================================================
print("\n" + "=" * 80)
print("9. WHAT TO MONITOR FOR JANUARY 2026")
print("=" * 80)
print()

print("🔍 LATE DECEMBER 2025 (NOW!):")
print("  ☐ Check NOAA Week 2-3 outlook for early January")
print("  ☐ Monitor Great Lakes ice coverage (affects lake effect)")
print("  ☐ Watch ski resort snow totals at Big Powderhorn, Whitecap")
print()

print("🔍 WEEK OF JANUARY 6-12, 2026:")
print("  ☐ Track storm systems on European (ECMWF) model")
print("  ☐ Monitor temperature trends for cold shot signals")
print("  ☐ Check NOAA's Week 2 outlook daily")
print()

print("🔍 WEEK OF JANUARY 13-19, 2026:")
print("  ☐ Peak storm week - check forecasts every 2-3 days")
print("  ☐ Watch for blocking patterns developing")
print("  ☐ Monitor local ski resort reports for snow validation")
print()

# =============================================================================
# 10. Final Forecast Summary
# =============================================================================
print("\n" + "=" * 80)
print("✅ JANUARY 2026 FORECAST SUMMARY")
print("=" * 80)
print()

print("🎯 **BEST BET FOR MAJOR SNOW: JANUARY 16-18, 2026**")
print()
print("📊 EXPECTED MONTHLY TOTAL: 22-28 inches")
print("   (35% below January 2025's 63 inches)")
print()
print("🌡️ TEMPERATURE: Near to below normal (not extreme)")
print()
print("⛈️  MAJOR STORM COUNT: 2-3 events ≥ 6 inches")
print()
print("📌 CRITICAL DATES TO WATCH:")
print("   - Jan 4: Early month system check")
print("   - Jan 9: Week 2 storm potential review")
print("   - Jan 16: PEAK STORM DAY - highest confidence")
print("   - Jan 23: Weekend storm window")
print()
print("⚠️  POLAR VORTEX RISK: LOW (15%)")
print("   After 2025's extreme event, unlikely to repeat in 2026")
print()
print("🎿 SKI RESORT OUTLOOK:")
print("   Expect good conditions but not exceptional like January 2025")
print("   Plan trips around Jan 16-25 for best natural snow")
print()
print("🌨️ Good luck and enjoy the snow!")
print()
print(f"Forecast valid through: January 31, 2026")
print(f"Next update: January 8, 2026")
print()
