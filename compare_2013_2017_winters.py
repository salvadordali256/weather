"""
2013-2014 vs 2017-2018 Winter Comparison - Northern Wisconsin
==============================================================

Compare two notable winters:
- 2013-2014: Neutral ENSO, "Polar Vortex" winter, #5 snowiest on record
- 2017-2018: La Niña winter

How did La Niña 2017-2018 compare to the extreme Neutral 2013-2014?
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set up plotting
sns.set_style("whitegrid")

db_path = "./northwoods_full_history.db"
conn = sqlite3.connect(db_path)

print("=" * 80)
print("WINTER COMPARISON: 2013-2014 vs 2017-2018")
print("=" * 80)
print()
print("2013-2014: Neutral ENSO → Weak El Niño (Polar Vortex Winter)")
print("2017-2018: La Niña")
print()

winters = ['2013-2014', '2017-2018']
winter_labels = {
    '2013-2014': '2013-2014 (Neutral/Polar Vortex)',
    '2017-2018': '2017-2018 (La Niña)'
}

# =============================================================================
# 1. Overall Season Totals
# =============================================================================
print("=" * 80)
print("1. OVERALL SEASON TOTALS (July-June)")
print("=" * 80)
print()

for winter in winters:
    if winter == '2013-2014':
        start = '2013-07-01'
        end = '2014-06-30'
    else:
        start = '2017-07-01'
        end = '2018-06-30'

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

    df = pd.read_sql(sql, conn, params=(start, end))
    print(f"{winter_labels[winter]}")
    print("-" * 80)
    print(df.to_string(index=False))
    print()

# =============================================================================
# 2. Monthly Breakdown Comparison
# =============================================================================
print("\n" + "=" * 80)
print("2. MONTHLY BREAKDOWN COMPARISON")
print("=" * 80)
print()

monthly_data = {}
for winter in winters:
    if winter == '2013-2014':
        start = '2013-07-01'
        end = '2014-06-30'
    else:
        start = '2017-07-01'
        end = '2018-06-30'

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

    df = pd.read_sql(sql, conn, params=(start, end))
    monthly_data[winter] = df
    print(f"{winter_labels[winter]}")
    print("-" * 80)
    print(df.to_string(index=False))
    print()

# =============================================================================
# 3. Peak Winter Comparison (Dec-Feb)
# =============================================================================
print("\n" + "=" * 80)
print("3. PEAK WINTER COMPARISON (December - February)")
print("=" * 80)
print()

for winter in winters:
    if winter == '2013-2014':
        start = '2013-12-01'
        end = '2014-02-28'
    else:
        start = '2017-12-01'
        end = '2018-02-28'

    sql = """
    SELECT
        s.name,
        ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches,
        COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as snow_days,
        ROUND(AVG(sd.temp_min_celsius), 1) as avg_low_c,
        ROUND(AVG(sd.temp_max_celsius), 1) as avg_high_c,
        ROUND(MIN(sd.temp_min_celsius), 1) as coldest_low_c,
        ROUND(MAX(sd.temp_max_celsius), 1) as warmest_high_c
    FROM snowfall_daily sd
    JOIN stations s ON sd.station_id = s.station_id
    WHERE sd.date >= ? AND sd.date <= ?
    GROUP BY s.name
    ORDER BY total_inches DESC
    """

    df = pd.read_sql(sql, conn, params=(start, end))
    print(f"{winter_labels[winter]}")
    print("-" * 80)
    print(df.to_string(index=False))
    print()

# =============================================================================
# 4. Top Storms Comparison
# =============================================================================
print("\n" + "=" * 80)
print("4. TOP 10 SNOWSTORMS COMPARISON")
print("=" * 80)
print()

for winter in winters:
    if winter == '2013-2014':
        start = '2013-07-01'
        end = '2014-06-30'
    else:
        start = '2017-07-01'
        end = '2018-06-30'

    sql = """
    SELECT
        sd.date,
        s.name,
        ROUND(sd.snowfall_mm / 10.0, 1) as snowfall_cm,
        ROUND(sd.snowfall_mm / 25.4, 1) as snowfall_inches,
        ROUND(sd.temp_max_celsius, 1) as high_c,
        ROUND(sd.temp_min_celsius, 1) as low_c
    FROM snowfall_daily sd
    JOIN stations s ON sd.station_id = s.station_id
    WHERE sd.date >= ? AND sd.date <= ?
    AND sd.snowfall_mm > 0
    ORDER BY sd.snowfall_mm DESC
    LIMIT 10
    """

    df = pd.read_sql(sql, conn, params=(start, end))
    print(f"{winter_labels[winter]}")
    print("-" * 80)
    print(df.to_string(index=False))
    print()

# =============================================================================
# 5. Side-by-Side Statistics
# =============================================================================
print("\n" + "=" * 80)
print("5. SIDE-BY-SIDE STATISTICAL COMPARISON")
print("=" * 80)
print()

comparison_stats = []
for winter in winters:
    if winter == '2013-2014':
        start = '2013-07-01'
        end = '2014-06-30'
    else:
        start = '2017-07-01'
        end = '2018-06-30'

    sql = """
    SELECT
        ROUND(AVG(total_inches), 1) as avg_total_inches,
        ROUND(MAX(total_inches), 1) as max_location_inches,
        ROUND(MIN(total_inches), 1) as min_location_inches,
        ROUND(AVG(days_with_snow), 0) as avg_snow_days,
        ROUND(AVG(avg_low_c), 1) as avg_winter_low_c,
        ROUND(AVG(avg_high_c), 1) as avg_winter_high_c
    FROM (
        SELECT
            s.name,
            ROUND(SUM(CASE WHEN sd.snowfall_mm > 0 THEN sd.snowfall_mm ELSE 0 END) / 25.4, 1) as total_inches,
            COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as days_with_snow,
            ROUND(AVG(sd.temp_min_celsius), 1) as avg_low_c,
            ROUND(AVG(sd.temp_max_celsius), 1) as avg_high_c
        FROM snowfall_daily sd
        JOIN stations s ON sd.station_id = s.station_id
        WHERE sd.date >= ? AND sd.date <= ?
        GROUP BY s.name
    )
    """

    df = pd.read_sql(sql, conn, params=(start, end))
    comparison_stats.append({
        'Winter': winter_labels[winter],
        'Avg Snowfall': f"{df['avg_total_inches'].iloc[0]}\"",
        'Max Location': f"{df['max_location_inches'].iloc[0]}\"",
        'Min Location': f"{df['min_location_inches'].iloc[0]}\"",
        'Avg Snow Days': int(df['avg_snow_days'].iloc[0]),
        'Avg Low (°C)': df['avg_winter_low_c'].iloc[0],
        'Avg High (°C)': df['avg_winter_high_c'].iloc[0]
    })

comparison_df = pd.DataFrame(comparison_stats)
print(comparison_df.to_string(index=False))
print()

# =============================================================================
# 6. Historical Context
# =============================================================================
print("\n" + "=" * 80)
print("6. HISTORICAL RANKING")
print("=" * 80)
print()

sql = """
WITH station_totals AS (
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
winter_totals AS (
    SELECT
        winter_season,
        AVG(total_mm) as avg_mm
    FROM station_totals
    GROUP BY winter_season
),
ranked AS (
    SELECT
        winter_season,
        ROUND(avg_mm / 25.4, 1) as avg_inches,
        ROW_NUMBER() OVER (ORDER BY avg_mm DESC) as rank,
        COUNT(*) OVER () as total_winters
    FROM winter_totals
)
SELECT
    winter_season,
    avg_inches,
    rank,
    total_winters,
    ROUND((CAST(rank AS REAL) / total_winters) * 100, 1) as percentile
FROM ranked
WHERE winter_season IN ('2013-2014', '2017-2018')
ORDER BY rank
"""

df = pd.read_sql(sql, conn)
print(df.to_string(index=False))
print()

# =============================================================================
# 7. Extreme Cold Days
# =============================================================================
print("\n" + "=" * 80)
print("7. EXTREME COLD ANALYSIS")
print("=" * 80)
print()

for winter in winters:
    if winter == '2013-2014':
        start = '2013-12-01'
        end = '2014-02-28'
        label = '2013-2014 (Polar Vortex Period: Dec-Feb)'
    else:
        start = '2017-12-01'
        end = '2018-02-28'
        label = '2017-2018 (Peak Winter: Dec-Feb)'

    sql = """
    SELECT
        COUNT(CASE WHEN temp_min_celsius < -20 THEN 1 END) as days_below_minus20c,
        COUNT(CASE WHEN temp_min_celsius < -25 THEN 1 END) as days_below_minus25c,
        COUNT(CASE WHEN temp_min_celsius < -30 THEN 1 END) as days_below_minus30c,
        COUNT(CASE WHEN temp_max_celsius < -10 THEN 1 END) as days_high_below_minus10c,
        ROUND(MIN(temp_min_celsius), 1) as coldest_temp_c,
        MIN(date) as coldest_date
    FROM snowfall_daily
    WHERE date >= ? AND date <= ?
    """

    df = pd.read_sql(sql, conn, params=(start, end))
    print(f"{label}")
    print("-" * 80)
    print(f"Days below -20°C (-4°F):  {df['days_below_minus20c'].iloc[0]}")
    print(f"Days below -25°C (-13°F): {df['days_below_minus25c'].iloc[0]}")
    print(f"Days below -30°C (-22°F): {df['days_below_minus30c'].iloc[0]}")
    print(f"Days with high < -10°C (14°F): {df['days_high_below_minus10c'].iloc[0]}")
    print(f"Coldest temperature: {df['coldest_temp_c'].iloc[0]}°C on {df['coldest_date'].iloc[0]}")
    print()

conn.close()

# =============================================================================
# Create Visualization
# =============================================================================

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Load monthly data for plotting
winter_2013 = monthly_data['2013-2014']
winter_2017 = monthly_data['2017-2018']

# 1. Monthly Snowfall Comparison
ax1 = axes[0, 0]
x = range(len(winter_2013))
width = 0.35
ax1.bar([i - width/2 for i in x], winter_2013['total_inches'], width,
        label='2013-2014', color='#d62728', alpha=0.7)
ax1.bar([i + width/2 for i in x], winter_2017['total_inches'], width,
        label='2017-2018', color='#1f77b4', alpha=0.7)
ax1.set_xlabel('Month', fontsize=11)
ax1.set_ylabel('Snowfall (inches)', fontsize=11)
ax1.set_title('Monthly Snowfall Comparison', fontsize=13, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(winter_2013['month_name'], rotation=45, ha='right')
ax1.legend()
ax1.grid(True, alpha=0.3)

# 2. Snow Days Comparison
ax2 = axes[0, 1]
ax2.bar([i - width/2 for i in x], winter_2013['days_with_snow'], width,
        label='2013-2014', color='#d62728', alpha=0.7)
ax2.bar([i + width/2 for i in x], winter_2017['days_with_snow'], width,
        label='2017-2018', color='#1f77b4', alpha=0.7)
ax2.set_xlabel('Month', fontsize=11)
ax2.set_ylabel('Days with Snow', fontsize=11)
ax2.set_title('Snow Days per Month Comparison', fontsize=13, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels(winter_2013['month_name'], rotation=45, ha='right')
ax2.legend()
ax2.grid(True, alpha=0.3)

# 3. Temperature Comparison
ax3 = axes[1, 0]
ax3.plot(winter_2013['month_name'], winter_2013['avg_low_c'],
         'o-', color='#d62728', linewidth=2, markersize=8, label='2013-14 Low')
ax3.plot(winter_2013['month_name'], winter_2013['avg_high_c'],
         's-', color='#d62728', linewidth=2, markersize=8, alpha=0.5, label='2013-14 High')
ax3.plot(winter_2017['month_name'], winter_2017['avg_low_c'],
         'o-', color='#1f77b4', linewidth=2, markersize=8, label='2017-18 Low')
ax3.plot(winter_2017['month_name'], winter_2017['avg_high_c'],
         's-', color='#1f77b4', linewidth=2, markersize=8, alpha=0.5, label='2017-18 High')
ax3.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax3.set_xlabel('Month', fontsize=11)
ax3.set_ylabel('Temperature (°C)', fontsize=11)
ax3.set_title('Average Temperature Comparison', fontsize=13, fontweight='bold')
plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
ax3.legend(loc='lower right', fontsize=9)
ax3.grid(True, alpha=0.3)

# 4. Cumulative Snowfall
ax4 = axes[1, 1]
cumsum_2013 = winter_2013['total_inches'].cumsum()
cumsum_2017 = winter_2017['total_inches'].cumsum()
ax4.plot(winter_2013['month_name'], cumsum_2013,
         'o-', color='#d62728', linewidth=3, markersize=10, label='2013-2014')
ax4.plot(winter_2017['month_name'], cumsum_2017,
         'o-', color='#1f77b4', linewidth=3, markersize=10, label='2017-2018')
ax4.set_xlabel('Month', fontsize=11)
ax4.set_ylabel('Cumulative Snowfall (inches)', fontsize=11)
ax4.set_title('Cumulative Snowfall Through Season', fontsize=13, fontweight='bold')
plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
ax4.legend()
ax4.grid(True, alpha=0.3)

# Add final totals
final_2013 = cumsum_2013.iloc[-1]
final_2017 = cumsum_2017.iloc[-1]
ax4.text(len(winter_2013)-1, final_2013, f'{final_2013:.1f}"',
         ha='right', va='bottom', fontsize=10, fontweight='bold',
         bbox=dict(boxstyle='round', facecolor='#d62728', alpha=0.3))
ax4.text(len(winter_2017)-1, final_2017, f'{final_2017}"',
         ha='right', va='top', fontsize=10, fontweight='bold',
         bbox=dict(boxstyle='round', facecolor='#1f77b4', alpha=0.3))

plt.suptitle('2013-2014 (Polar Vortex/Neutral) vs 2017-2018 (La Niña) Winter Comparison',
             fontsize=15, fontweight='bold', y=0.998)
plt.tight_layout()
plt.savefig('snowfall_graphs/2013_vs_2017_comparison.png', dpi=300, bbox_inches='tight')
print("✅ Saved: snowfall_graphs/2013_vs_2017_comparison.png")

print("\n" + "=" * 80)
print("✅ Analysis Complete!")
print("=" * 80)
print()
print("KEY TAKEAWAYS:")
print("- Check the comparison chart: snowfall_graphs/2013_vs_2017_comparison.png")
print("- Review the monthly patterns and temperature differences")
print("- Note the extreme cold days during Polar Vortex 2013-2014")
print()
