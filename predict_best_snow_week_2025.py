"""
Best Snow Week Prediction for 2025 - Northern Wisconsin
========================================================

Uses historical data to predict the highest probability snow weeks
for winter 2024-2025 based on:
1. La Niña historical patterns
2. Polar vortex disruption signatures
3. Week-by-week snow accumulation analysis
4. Current season early indicators
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

db_path = "./northwoods_full_history.db"
conn = sqlite3.connect(db_path)

print("=" * 80)
print("BEST SNOW WEEK PREDICTION FOR 2025 - NORTHERN WISCONSIN")
print("=" * 80)
print()
print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d')}")
print()

# =============================================================================
# 1. Analyze week-by-week patterns from La Niña winters
# =============================================================================
print("=" * 80)
print("STEP 1: ANALYZING LA NIÑA WEEKLY SNOW PATTERNS")
print("=" * 80)
print()

# La Niña winters we're using as analogues
la_nina_winters = [
    '2022-2023', '2021-2022', '2020-2021',
    '2017-2018', '2011-2012', '2010-2011',
    '2008-2009', '2007-2008', '2000-2001',
    '1999-2000', '1998-1999'
]

# Also include 2013-2014 as a "polar vortex" reference
pv_winters = ['2013-2014', '2010-2011']

# Get daily data for all these winters
sql = """
WITH winter_classification AS (
    SELECT
        sd.date,
        CASE
            WHEN CAST(strftime('%m', sd.date) AS INTEGER) >= 7
            THEN CAST(strftime('%Y', sd.date) AS INTEGER) || '-' ||
                 CAST(CAST(strftime('%Y', sd.date) AS INTEGER) + 1 AS VARCHAR)
            ELSE CAST(CAST(strftime('%Y', sd.date) AS INTEGER) - 1 AS VARCHAR) || '-' ||
                 CAST(strftime('%Y', sd.date) AS VARCHAR)
        END as winter_season,
        AVG(sd.snowfall_mm) as avg_snowfall_mm,
        AVG(sd.temp_min_celsius) as avg_temp_min,
        AVG(sd.temp_max_celsius) as avg_temp_max,
        COUNT(CASE WHEN sd.snowfall_mm > 0 THEN 1 END) as stations_with_snow
    FROM snowfall_daily sd
    GROUP BY sd.date
)
SELECT
    winter_season,
    date,
    avg_snowfall_mm,
    avg_temp_min,
    avg_temp_max,
    stations_with_snow
FROM winter_classification
WHERE winter_season IN (
    '2022-2023', '2021-2022', '2020-2021', '2017-2018',
    '2011-2012', '2010-2011', '2008-2009', '2007-2008',
    '2000-2001', '1999-2000', '1998-1999', '2013-2014'
)
AND CAST(strftime('%m', date) AS INTEGER) IN (10,11,12,1,2,3,4)
ORDER BY date
"""

daily_data = pd.read_sql(sql, conn)
daily_data['date'] = pd.to_datetime(daily_data['date'])
daily_data['week_of_year'] = daily_data['date'].dt.isocalendar().week
daily_data['month'] = daily_data['date'].dt.month
daily_data['day_of_season'] = (daily_data['date'] - pd.to_datetime(daily_data['winter_season'].str[:4] + '-07-01')).dt.days

print(f"Analyzing {len(daily_data)} days across {len(la_nina_winters) + 1} winters")
print()

# Calculate weekly statistics
weekly_stats = daily_data.groupby(['winter_season', 'week_of_year']).agg({
    'avg_snowfall_mm': 'sum',
    'avg_temp_min': 'mean',
    'avg_temp_max': 'mean',
    'stations_with_snow': 'sum'
}).reset_index()

weekly_stats['avg_snowfall_inches'] = weekly_stats['avg_snowfall_mm'] / 25.4

# =============================================================================
# 2. Find highest snow weeks in each winter type
# =============================================================================
print("=" * 80)
print("STEP 2: TOP SNOW WEEKS BY WINTER TYPE")
print("=" * 80)
print()

print("LA NIÑA WINTERS - Top 5 Snow Weeks:")
print("-" * 80)
la_nina_weeks = weekly_stats[weekly_stats['winter_season'].isin(la_nina_winters)]
top_la_nina = la_nina_weeks.nlargest(20, 'avg_snowfall_inches')[['winter_season', 'week_of_year', 'avg_snowfall_inches', 'avg_temp_min']]

# Convert week number to approximate dates
def week_to_date_range(week_num):
    """Convert ISO week number to approximate month/day"""
    # Weeks 1-4 = Jan, 5-8 = Feb, 9-13 = Mar, 14-17 = Apr
    # Weeks 40-43 = Oct, 44-48 = Nov, 49-52 = Dec
    if week_num >= 49:
        return f"Dec {(week_num-48)*7}-{(week_num-48)*7+6}"
    elif week_num >= 44:
        return f"Nov {(week_num-43)*7}-{(week_num-43)*7+6}"
    elif week_num >= 40:
        return f"Oct {(week_num-39)*7}-{(week_num-39)*7+6}"
    elif week_num >= 14:
        return f"Apr {(week_num-13)*7}-{(week_num-13)*7+6}"
    elif week_num >= 9:
        return f"Mar {(week_num-8)*7}-{(week_num-8)*7+6}"
    elif week_num >= 5:
        return f"Feb {(week_num-4)*7}-{(week_num-4)*7+6}"
    else:
        return f"Jan {week_num*7}-{week_num*7+6}"

top_la_nina['date_range'] = top_la_nina['week_of_year'].apply(week_to_date_range)
print(top_la_nina[['winter_season', 'week_of_year', 'date_range', 'avg_snowfall_inches', 'avg_temp_min']].head(15).to_string(index=False))
print()

print("POLAR VORTEX WINTER (2013-2014) - Top Snow Weeks:")
print("-" * 80)
pv_weeks = weekly_stats[weekly_stats['winter_season'] == '2013-2014']
top_pv = pv_weeks.nlargest(10, 'avg_snowfall_inches')[['week_of_year', 'avg_snowfall_inches', 'avg_temp_min']]
top_pv['date_range'] = top_pv['week_of_year'].apply(week_to_date_range)
print(top_pv[['week_of_year', 'date_range', 'avg_snowfall_inches', 'avg_temp_min']].to_string(index=False))
print()

# =============================================================================
# 3. Calculate probability by week for 2025
# =============================================================================
print("\n" + "=" * 80)
print("STEP 3: 2025 WEEKLY SNOW PROBABILITY FORECAST")
print("=" * 80)
print()

# Group all La Niña weeks by week number
week_probabilities = la_nina_weeks.groupby('week_of_year').agg({
    'avg_snowfall_inches': ['mean', 'max', 'std', 'count']
}).reset_index()

week_probabilities.columns = ['week_of_year', 'mean_snow', 'max_snow', 'std_snow', 'sample_size']
week_probabilities = week_probabilities[week_probabilities['sample_size'] >= 3]  # At least 3 samples
week_probabilities['date_range_2025'] = week_probabilities['week_of_year'].apply(
    lambda w: week_to_date_range(w).replace('2024', '2025').replace('2023', '2025')
)

# Calculate a "big snow week" score
week_probabilities['big_week_score'] = (
    week_probabilities['mean_snow'] * 0.4 +
    week_probabilities['max_snow'] * 0.4 +
    (week_probabilities['std_snow'] * 0.2)  # Higher variability = potential for big weeks
)

# Sort by score
week_probabilities = week_probabilities.sort_values('big_week_score', ascending=False)

print("TOP 10 HIGHEST PROBABILITY SNOW WEEKS FOR 2025:")
print("-" * 80)
print("Week# | Date Range 2025    | Avg Snow | Max Ever | Score  | Sample")
print("-" * 80)
for idx, row in week_probabilities.head(10).iterrows():
    print(f"  {row['week_of_year']:2.0f}  | {row['date_range_2025']:18s} | {row['mean_snow']:5.1f}\"  | {row['max_snow']:5.1f}\"  | {row['big_week_score']:5.1f} | n={row['sample_size']:.0f}")
print()

# =============================================================================
# 4. Incorporate Polar Vortex Timing
# =============================================================================
print("\n" + "=" * 80)
print("STEP 4: POLAR VORTEX SCENARIO WEEKS")
print("=" * 80)
print()

print("If Polar Vortex disrupts (15% chance):")
print("-" * 80)
pv_top_weeks = pv_weeks.nlargest(5, 'avg_snowfall_inches')
print("Week# | Date Range 2025    | Expected Snow (2013-14 pattern)")
print("-" * 80)
for idx, row in pv_top_weeks.iterrows():
    date_range = week_to_date_range(row['week_of_year'])
    print(f"  {row['week_of_year']:2.0f}  | {date_range:18s} | {row['avg_snowfall_inches']:5.1f}\"")
print()

# =============================================================================
# 5. Current Season Analysis
# =============================================================================
print("\n" + "=" * 80)
print("STEP 5: CURRENT 2024-2025 SEASON INDICATORS")
print("=" * 80)
print()

sql = """
SELECT
    strftime('%Y-%m-%d', date) as date,
    AVG(snowfall_mm) as avg_snowfall_mm,
    AVG(temp_min_celsius) as avg_temp_min
FROM snowfall_daily
WHERE date >= '2024-10-01' AND date <= '2024-12-31'
GROUP BY date
ORDER BY avg_snowfall_mm DESC
LIMIT 10
"""

current_top_days = pd.read_sql(sql, conn)
if not current_top_days.empty and current_top_days['avg_snowfall_mm'].sum() > 0:
    current_top_days['snowfall_inches'] = current_top_days['avg_snowfall_mm'] / 25.4
    print("Top 10 Snow Days So Far (Oct-Dec 2024):")
    print("-" * 80)
    print(current_top_days[['date', 'snowfall_inches', 'avg_temp_min']].to_string(index=False))
    print()

    # Check early season pace
    sql_current_total = """
    SELECT
        SUM(snowfall_mm) / 25.4 as total_inches,
        AVG(temp_min_celsius) as avg_temp
    FROM snowfall_daily
    WHERE date >= '2024-10-01' AND date < '2025-01-01'
    """
    current_total = pd.read_sql(sql_current_total, conn)
    print(f"Oct-Dec 2024 total: {current_total['total_inches'].iloc[0]:.1f} inches")
    print(f"Average temp: {current_total['avg_temp'].iloc[0]:.1f}°C")
    print()

# =============================================================================
# 6. Final Prediction with Confidence Intervals
# =============================================================================
print("\n" + "=" * 80)
print("FINAL PREDICTION: BEST SNOW WEEKS IN 2025")
print("=" * 80)
print()

# Identify the absolute top weeks
top_week = week_probabilities.iloc[0]
second_week = week_probabilities.iloc[1]
third_week = week_probabilities.iloc[2]

print("🎯 MOST LIKELY SCENARIO (60% probability - Standard La Niña):")
print("-" * 80)
print(f"#1 BEST WEEK: Week {top_week['week_of_year']:.0f} ({top_week['date_range_2025']})")
print(f"   Expected: {top_week['mean_snow']:.1f}\" (range: {top_week['mean_snow']-top_week['std_snow']:.1f}\"-{top_week['max_snow']:.1f}\")")
print()
print(f"#2 RUNNER UP: Week {second_week['week_of_year']:.0f} ({second_week['date_range_2025']})")
print(f"   Expected: {second_week['mean_snow']:.1f}\" (range: {second_week['mean_snow']-second_week['std_snow']:.1f}\"-{second_week['max_snow']:.1f}\")")
print()
print(f"#3 THIRD BEST: Week {third_week['week_of_year']:.0f} ({third_week['date_range_2025']})")
print(f"   Expected: {third_week['mean_snow']:.1f}\" (range: {third_week['mean_snow']-third_week['std_snow']:.1f}\"-{third_week['max_snow']:.1f}\")")
print()

# Polar vortex scenario
pv_best_week = pv_top_weeks.iloc[0]
print("⚠️  POLAR VORTEX SCENARIO (15% probability):")
print("-" * 80)
print(f"BEST WEEK: Week {pv_best_week['week_of_year']:.0f} ({week_to_date_range(pv_best_week['week_of_year'])})")
print(f"   Expected: {pv_best_week['avg_snowfall_inches']:.1f}\" (based on 2013-14)")
print()

# =============================================================================
# 7. Create Visualization
# =============================================================================

fig, axes = plt.subplots(2, 1, figsize=(16, 10))

# Plot 1: Weekly snow probability
ax1 = axes[0]
weeks_to_plot = week_probabilities[week_probabilities['week_of_year'].isin(range(40, 53)) |
                                     week_probabilities['week_of_year'].isin(range(1, 18))]
weeks_to_plot = weeks_to_plot.sort_values('week_of_year')

x_labels = [week_to_date_range(w) for w in weeks_to_plot['week_of_year']]
x_pos = range(len(weeks_to_plot))

bars = ax1.bar(x_pos, weeks_to_plot['mean_snow'],
               yerr=weeks_to_plot['std_snow'],
               capsize=3, alpha=0.7, color='steelblue', edgecolor='black')

# Highlight top 3 weeks
top_3_weeks = week_probabilities.head(3)['week_of_year'].values
for i, (idx, row) in enumerate(weeks_to_plot.iterrows()):
    if row['week_of_year'] in top_3_weeks:
        bars[i].set_color('red')
        bars[i].set_alpha(0.9)

ax1.set_xlabel('Week (2025)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Expected Snowfall (inches)', fontsize=12, fontweight='bold')
ax1.set_title('2025 Weekly Snowfall Forecast - Northern Wisconsin (La Niña Pattern)',
              fontsize=14, fontweight='bold')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)
ax1.grid(True, alpha=0.3)
ax1.legend(['Standard La Niña Weeks', 'Top 3 Predicted Weeks'], loc='upper right')

# Plot 2: Polar vortex comparison
ax2 = axes[1]
pv_weeks_plot = pv_weeks[pv_weeks['week_of_year'].isin(range(40, 53)) |
                          pv_weeks['week_of_year'].isin(range(1, 18))]
pv_weeks_plot = pv_weeks_plot.sort_values('week_of_year')

la_nina_avg = la_nina_weeks.groupby('week_of_year')['avg_snowfall_inches'].mean().reset_index()
la_nina_avg = la_nina_avg[la_nina_avg['week_of_year'].isin(range(40, 53)) |
                           la_nina_avg['week_of_year'].isin(range(1, 18))]
la_nina_avg = la_nina_avg.sort_values('week_of_year')

x_labels2 = [week_to_date_range(w) for w in la_nina_avg['week_of_year']]
x_pos2 = range(len(la_nina_avg))

width = 0.35
ax2.bar([x - width/2 for x in x_pos2], la_nina_avg['avg_snowfall_inches'],
        width, label='La Niña Average', color='steelblue', alpha=0.7)

# Align polar vortex data
pv_aligned = []
for week in la_nina_avg['week_of_year']:
    pv_val = pv_weeks_plot[pv_weeks_plot['week_of_year'] == week]['avg_snowfall_inches']
    pv_aligned.append(pv_val.iloc[0] if len(pv_val) > 0 else 0)

ax2.bar([x + width/2 for x in x_pos2], pv_aligned,
        width, label='2013-14 Polar Vortex', color='darkred', alpha=0.7)

ax2.set_xlabel('Week', fontsize=12, fontweight='bold')
ax2.set_ylabel('Snowfall (inches)', fontsize=12, fontweight='bold')
ax2.set_title('La Niña vs Polar Vortex Winter Pattern Comparison',
              fontsize=14, fontweight='bold')
ax2.set_xticks(x_pos2)
ax2.set_xticklabels(x_labels2, rotation=45, ha='right', fontsize=9)
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('snowfall_graphs/best_snow_week_2025_forecast.png', dpi=300, bbox_inches='tight')
print("\n✅ Saved forecast visualization: snowfall_graphs/best_snow_week_2025_forecast.png")

conn.close()

print("\n" + "=" * 80)
print("✅ ANALYSIS COMPLETE")
print("=" * 80)
print()
