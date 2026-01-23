"""
ENSO vs Snowfall Analysis - Northern Wisconsin
================================================

Visualize how El Niño, La Niña, and Neutral winters affect snowfall
in the Wisconsin Northwoods over 85 years (1940-2024)
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set up plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 12)

db_path = "./northwoods_full_history.db"
conn = sqlite3.connect(db_path)

print("=" * 80)
print("ENSO PHASE vs SNOWFALL ANALYSIS - NORTHERN WISCONSIN")
print("=" * 80)
print()

# =============================================================================
# ENSO Phase Classification (1940-2024)
# Based on historical ONI (Oceanic Niño Index) data
# =============================================================================

# Historical ENSO classifications for winter seasons (Dec-Jan-Feb)
# El Niño: ONI >= +0.5, La Niña: ONI <= -0.5, Neutral: -0.5 < ONI < 0.5
enso_phases = {
    # 1940s
    '1940-1941': 'Neutral', '1941-1942': 'La Niña', '1942-1943': 'El Niño',
    '1943-1944': 'El Niño', '1944-1945': 'Neutral', '1945-1946': 'Neutral',
    '1946-1947': 'El Niño', '1947-1948': 'La Niña', '1948-1949': 'La Niña',
    '1949-1950': 'La Niña',

    # 1950s
    '1950-1951': 'La Niña', '1951-1952': 'El Niño', '1952-1953': 'El Niño',
    '1953-1954': 'El Niño', '1954-1955': 'La Niña', '1955-1956': 'La Niña',
    '1956-1957': 'La Niña', '1957-1958': 'El Niño', '1958-1959': 'Neutral',
    '1959-1960': 'Neutral',

    # 1960s
    '1960-1961': 'Neutral', '1961-1962': 'Neutral', '1962-1963': 'El Niño',
    '1963-1964': 'La Niña', '1964-1965': 'La Niña', '1965-1966': 'El Niño',
    '1966-1967': 'Neutral', '1967-1968': 'Neutral', '1968-1969': 'El Niño',
    '1969-1970': 'El Niño',

    # 1970s
    '1970-1971': 'La Niña', '1971-1972': 'La Niña', '1972-1973': 'El Niño',
    '1973-1974': 'La Niña', '1974-1975': 'La Niña', '1975-1976': 'La Niña',
    '1976-1977': 'El Niño', '1977-1978': 'El Niño', '1978-1979': 'Neutral',
    '1979-1980': 'Neutral',

    # 1980s
    '1980-1981': 'Neutral', '1981-1982': 'Neutral', '1982-1983': 'El Niño',
    '1983-1984': 'La Niña', '1984-1985': 'La Niña', '1985-1986': 'El Niño',
    '1986-1987': 'El Niño', '1987-1988': 'El Niño', '1988-1989': 'La Niña',
    '1989-1990': 'Neutral',

    # 1990s
    '1990-1991': 'El Niño', '1991-1992': 'El Niño', '1992-1993': 'El Niño',
    '1993-1994': 'Neutral', '1994-1995': 'El Niño', '1995-1996': 'La Niña',
    '1996-1997': 'Neutral', '1997-1998': 'El Niño', '1998-1999': 'La Niña',
    '1999-2000': 'La Niña',

    # 2000s
    '2000-2001': 'La Niña', '2001-2002': 'Neutral', '2002-2003': 'El Niño',
    '2003-2004': 'Neutral', '2004-2005': 'El Niño', '2005-2006': 'La Niña',
    '2006-2007': 'El Niño', '2007-2008': 'La Niña', '2008-2009': 'La Niña',
    '2009-2010': 'El Niño',

    # 2010s
    '2010-2011': 'La Niña', '2011-2012': 'La Niña', '2012-2013': 'Neutral',
    '2013-2014': 'Neutral', '2014-2015': 'El Niño', '2015-2016': 'El Niño',
    '2016-2017': 'La Niña', '2017-2018': 'La Niña', '2018-2019': 'El Niño',
    '2019-2020': 'Neutral',

    # 2020s
    '2020-2021': 'La Niña', '2021-2022': 'La Niña', '2022-2023': 'La Niña',
    '2023-2024': 'El Niño', '2024-2025': 'La Niña',
}

# =============================================================================
# Get all winter snowfall totals
# =============================================================================

sql = """
WITH winter_seasons AS (
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
)
SELECT
    winter_season,
    name,
    ROUND(total_mm / 25.4, 1) as total_inches
FROM winter_seasons
WHERE winter_season >= '1940-1941'
ORDER BY winter_season, name
"""

df = pd.read_sql(sql, conn)
conn.close()

# Add ENSO phase
df['enso_phase'] = df['winter_season'].map(enso_phases)

# Filter out any seasons without ENSO data
df = df[df['enso_phase'].notna()]

print(f"Total winter seasons analyzed: {df['winter_season'].nunique()}")
print(f"Total records: {len(df)}")
print()

# Calculate statistics by ENSO phase
print("SNOWFALL STATISTICS BY ENSO PHASE")
print("-" * 80)
stats = df.groupby('enso_phase')['total_inches'].agg([
    ('count', 'count'),
    ('mean', 'mean'),
    ('median', 'median'),
    ('std', 'std'),
    ('min', 'min'),
    ('max', 'max')
]).round(1)
print(stats)
print()

# Highlight 2012-2013 and 2013-2014
print("2012-2013 and 2013-2014 WINTERS:")
print("-" * 80)
special_winters = df[df['winter_season'].isin(['2012-2013', '2013-2014'])]
print(special_winters.to_string(index=False))
print()

# =============================================================================
# CREATE VISUALIZATIONS
# =============================================================================

fig = plt.figure(figsize=(20, 14))

# Color scheme
colors = {
    'El Niño': '#d62728',      # Red
    'La Niña': '#1f77b4',      # Blue
    'Neutral': '#7f7f7f'       # Gray
}

# 1. Box plot comparing ENSO phases
ax1 = plt.subplot(3, 2, 1)
enso_order = ['La Niña', 'Neutral', 'El Niño']
box_data = [df[df['enso_phase'] == phase]['total_inches'] for phase in enso_order]
bp = ax1.boxplot(box_data, labels=enso_order, patch_artist=True)
for patch, phase in zip(bp['boxes'], enso_order):
    patch.set_facecolor(colors[phase])
    patch.set_alpha(0.6)
ax1.set_ylabel('Snowfall (inches)', fontsize=12)
ax1.set_title('Snowfall Distribution by ENSO Phase (1940-2024)', fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3)

# Add sample sizes
for i, phase in enumerate(enso_order, 1):
    count = len(df[df['enso_phase'] == phase])
    ax1.text(i, ax1.get_ylim()[0] + 5, f'n={count}', ha='center', fontsize=10)

# 2. Violin plot for distribution
ax2 = plt.subplot(3, 2, 2)
for phase in enso_order:
    data = df[df['enso_phase'] == phase]['total_inches']
    parts = ax2.violinplot([data], positions=[enso_order.index(phase)],
                           widths=0.7, showmeans=True, showmedians=True)
    for pc in parts['bodies']:
        pc.set_facecolor(colors[phase])
        pc.set_alpha(0.6)
ax2.set_xticks(range(len(enso_order)))
ax2.set_xticklabels(enso_order)
ax2.set_ylabel('Snowfall (inches)', fontsize=12)
ax2.set_title('Snowfall Density by ENSO Phase', fontsize=14, fontweight='bold')
ax2.grid(True, alpha=0.3)

# 3. Time series showing all winters colored by ENSO
ax3 = plt.subplot(3, 1, 2)
# Get average across locations per winter
df_avg = df.groupby(['winter_season', 'enso_phase'])['total_inches'].mean().reset_index()
df_avg['year'] = df_avg['winter_season'].str[:4].astype(int)

for phase in enso_order:
    phase_data = df_avg[df_avg['enso_phase'] == phase]
    ax3.scatter(phase_data['year'], phase_data['total_inches'],
               c=colors[phase], label=phase, s=50, alpha=0.7, edgecolors='black', linewidth=0.5)

# Highlight 2012-2013 and 2013-2014
special_avg = df_avg[df_avg['winter_season'].isin(['2012-2013', '2013-2014'])]
ax3.scatter(special_avg['year'], special_avg['total_inches'],
           s=300, facecolors='none', edgecolors='gold', linewidth=3,
           label='2012-13 & 2013-14', zorder=10)

# Add trend line
from scipy.stats import linregress
slope, intercept, r_value, p_value, std_err = linregress(df_avg['year'], df_avg['total_inches'])
trend_line = slope * df_avg['year'] + intercept
ax3.plot(df_avg['year'], trend_line, 'k--', alpha=0.5, linewidth=2,
         label=f'Trend (slope={slope:.2f} in/yr)')

ax3.set_xlabel('Year', fontsize=12)
ax3.set_ylabel('Average Snowfall (inches)', fontsize=12)
ax3.set_title('85-Year Snowfall History by ENSO Phase (1940-2024)', fontsize=14, fontweight='bold')
ax3.legend(loc='upper right', fontsize=10)
ax3.grid(True, alpha=0.3)

# 4. Histogram comparing distributions
ax4 = plt.subplot(3, 2, 5)
bins = range(0, 110, 5)
for phase in enso_order:
    data = df[df['enso_phase'] == phase]['total_inches']
    ax4.hist(data, bins=bins, alpha=0.5, label=phase, color=colors[phase], edgecolor='black')
ax4.set_xlabel('Snowfall (inches)', fontsize=12)
ax4.set_ylabel('Frequency', fontsize=12)
ax4.set_title('Snowfall Frequency Distribution by ENSO Phase', fontsize=14, fontweight='bold')
ax4.legend()
ax4.grid(True, alpha=0.3)

# 5. Statistical summary table
ax5 = plt.subplot(3, 2, 6)
ax5.axis('off')

# Calculate percentages
summary_data = []
for phase in enso_order:
    phase_df = df[df['enso_phase'] == phase]
    summary_data.append([
        phase,
        len(phase_df),
        f"{phase_df['total_inches'].mean():.1f}",
        f"{phase_df['total_inches'].median():.1f}",
        f"{phase_df['total_inches'].std():.1f}",
        f"{phase_df['total_inches'].min():.1f}",
        f"{phase_df['total_inches'].max():.1f}"
    ])

table = ax5.table(cellText=summary_data,
                  colLabels=['ENSO Phase', 'Count', 'Mean', 'Median', 'Std Dev', 'Min', 'Max'],
                  loc='center',
                  cellLoc='center')
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 2)

# Color code the rows
for i, phase in enumerate(enso_order, 1):
    table[(i, 0)].set_facecolor(colors[phase])
    table[(i, 0)].set_alpha(0.3)

ax5.set_title('Statistical Summary by ENSO Phase (inches)', fontsize=14, fontweight='bold', pad=20)

plt.tight_layout()
plt.savefig('snowfall_graphs/enso_snowfall_analysis.png', dpi=300, bbox_inches='tight')
print("✅ Saved: snowfall_graphs/enso_snowfall_analysis.png")

# =============================================================================
# Create second figure: Location-specific analysis
# =============================================================================

fig2 = plt.figure(figsize=(20, 10))

locations = df['name'].unique()
for idx, location in enumerate(locations, 1):
    ax = plt.subplot(1, len(locations), idx)
    location_df = df[df['name'] == location]

    # Box plot for this location
    box_data = [location_df[location_df['enso_phase'] == phase]['total_inches']
                for phase in enso_order]
    bp = ax.boxplot(box_data, labels=enso_order, patch_artist=True)
    for patch, phase in zip(bp['boxes'], enso_order):
        patch.set_facecolor(colors[phase])
        patch.set_alpha(0.6)

    # Highlight 2012-2013 and 2013-2014 for this location
    special_location = location_df[location_df['winter_season'].isin(['2012-2013', '2013-2014'])]
    for _, row in special_location.iterrows():
        phase_idx = enso_order.index(row['enso_phase']) + 1
        ax.scatter(phase_idx, row['total_inches'], s=300, facecolors='none',
                  edgecolors='gold', linewidth=3, zorder=10)

    ax.set_ylabel('Snowfall (inches)', fontsize=11)
    ax.set_title(location, fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # Add mean values
    for i, phase in enumerate(enso_order, 1):
        mean_val = location_df[location_df['enso_phase'] == phase]['total_inches'].mean()
        ax.text(i, ax.get_ylim()[1] - 5, f'{mean_val:.1f}"', ha='center',
               fontsize=9, fontweight='bold', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.suptitle('Snowfall by ENSO Phase - Location Comparison (Gold circles = 2012-13 & 2013-14)',
             fontsize=16, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig('snowfall_graphs/enso_by_location.png', dpi=300, bbox_inches='tight')
print("✅ Saved: snowfall_graphs/enso_by_location.png")

# =============================================================================
# Key Insights
# =============================================================================

print("\n" + "=" * 80)
print("KEY INSIGHTS")
print("=" * 80)
print()

# Calculate percentage differences
la_nina_mean = df[df['enso_phase'] == 'La Niña']['total_inches'].mean()
el_nino_mean = df[df['enso_phase'] == 'El Niño']['total_inches'].mean()
neutral_mean = df[df['enso_phase'] == 'Neutral']['total_inches'].mean()

print(f"1. ENSO IMPACT ON SNOWFALL:")
print(f"   La Niña winters:  {la_nina_mean:.1f}\" average (baseline)")
print(f"   Neutral winters:  {neutral_mean:.1f}\" average ({((neutral_mean/la_nina_mean-1)*100):+.1f}% vs La Niña)")
print(f"   El Niño winters:  {el_nino_mean:.1f}\" average ({((el_nino_mean/la_nina_mean-1)*100):+.1f}% vs La Niña)")
print()

# Find where 2012-2013 and 2013-2014 rank
for winter in ['2012-2013', '2013-2014']:
    winter_avg = df[df['winter_season'] == winter]['total_inches'].mean()
    phase = enso_phases[winter]
    all_winters = df.groupby('winter_season')['total_inches'].mean().sort_values(ascending=False)
    rank = list(all_winters.index).index(winter) + 1
    percentile = (rank / len(all_winters)) * 100

    print(f"2. {winter} ({phase}):")
    print(f"   Average: {winter_avg:.1f}\" across all locations")
    print(f"   Rank: #{rank} out of {len(all_winters)} winters ({100-percentile:.1f}th percentile)")

    phase_data = df[df['enso_phase'] == phase]['total_inches']
    phase_percentile = (phase_data < winter_avg).sum() / len(phase_data) * 100
    print(f"   Among {phase} winters: {phase_percentile:.1f}th percentile")
    print()

print(f"3. CLIMATE TREND:")
print(f"   Overall trend: {slope:.3f} inches per year")
if abs(slope) < 0.05:
    print(f"   Interpretation: No significant trend (stable snowfall)")
elif slope > 0:
    print(f"   Interpretation: Slight increase in snowfall over time")
else:
    print(f"   Interpretation: Slight decrease in snowfall over time")
print()

print("=" * 80)
print("✅ Analysis Complete!")
print("=" * 80)
print()
print("Generated files:")
print("  - snowfall_graphs/enso_snowfall_analysis.png")
print("  - snowfall_graphs/enso_by_location.png")
print()
