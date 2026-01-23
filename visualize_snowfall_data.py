#!/usr/bin/env python3
"""
Northern Wisconsin Snowfall Data Visualizations
================================================

Creates comprehensive graphs showing 85 years of snowfall history
(1940-2025) for Northern Wisconsin locations.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from snowfall_duckdb import SnowfallDuckDB
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

# Create output directory for graphs
OUTPUT_DIR = Path(__file__).parent / "snowfall_graphs"
OUTPUT_DIR.mkdir(exist_ok=True)

print("=" * 80)
print("NORTHERN WISCONSIN SNOWFALL VISUALIZATIONS")
print("85 Years of Climate Data (1940-2025)")
print("=" * 80)
print()
print(f"Output directory: {OUTPUT_DIR}")
print()


def create_annual_snowfall_chart(engine):
    """Graph 1: Annual Snowfall by Location (1940-2025)"""
    print("Creating Graph 1: Annual Snowfall Trends (1940-2025)...")

    sql = """
    SELECT
        CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
        station_id,
        ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0, 1) as total_cm
    FROM snowfall.snowfall_daily
    GROUP BY year, station_id
    ORDER BY year, station_id
    """

    df = engine.query(sql)

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 8))

    # Plot each location
    for station in df['station_id'].unique():
        station_data = df[df['station_id'] == station]
        label = station.replace('WI_', '').replace('_', ' ')
        ax.plot(station_data['year'], station_data['total_cm'],
                marker='o', markersize=2, linewidth=1.5, label=label, alpha=0.8)

    # Add trend line for average
    yearly_avg = df.groupby('year')['total_cm'].mean()
    z = np.polyfit(yearly_avg.index, yearly_avg.values, 1)
    p = np.poly1d(z)
    ax.plot(yearly_avg.index, p(yearly_avg.index),
            "r--", linewidth=2, label=f'Trend Line (slope: {z[0]:.2f} cm/year)', alpha=0.7)

    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Annual Snowfall (cm)', fontsize=12, fontweight='bold')
    ax.set_title('Northern Wisconsin Annual Snowfall (1940-2025)\n85 Years of Climate Data',
                 fontsize=14, fontweight='bold')
    ax.legend(loc='best', framealpha=0.9)
    ax.grid(True, alpha=0.3)

    # Save
    output_path = OUTPUT_DIR / "01_annual_snowfall_trends.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def create_decade_comparison(engine):
    """Graph 2: Snowfall by Decade"""
    print("Creating Graph 2: Decade-by-Decade Comparison...")

    sql = """
    SELECT
        (CAST(YEAR(CAST(date AS DATE)) AS INTEGER) / 10) * 10 as decade,
        ROUND(AVG(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0, 2) as avg_daily_cm,
        ROUND(SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 /
              COUNT(DISTINCT YEAR(CAST(date AS DATE))), 1) as avg_annual_cm,
        COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as total_snow_days
    FROM snowfall.snowfall_daily
    GROUP BY decade
    ORDER BY decade
    """

    df = engine.query(sql)

    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Plot 1: Average Annual Snowfall
    colors = plt.cm.coolwarm(np.linspace(0, 1, len(df)))
    bars = ax1.bar(df['decade'].astype(str), df['avg_annual_cm'], color=colors, edgecolor='black', linewidth=0.5)
    ax1.set_xlabel('Decade', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Average Annual Snowfall (cm)', fontsize=12, fontweight='bold')
    ax1.set_title('Average Annual Snowfall by Decade', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.tick_params(axis='x', rotation=45)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=8)

    # Plot 2: Total Snow Days per Decade
    bars2 = ax2.bar(df['decade'].astype(str), df['total_snow_days'],
                    color=colors, edgecolor='black', linewidth=0.5)
    ax2.set_xlabel('Decade', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Total Snow Days', fontsize=12, fontweight='bold')
    ax2.set_title('Total Days with Snowfall by Decade', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.tick_params(axis='x', rotation=45)

    # Add value labels
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=8)

    plt.suptitle('Northern Wisconsin Snowfall: Decade-by-Decade Analysis (1940s-2020s)',
                 fontsize=14, fontweight='bold', y=0.995)

    # Save
    output_path = OUTPUT_DIR / "02_decade_comparison.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def create_climate_change_comparison(engine):
    """Graph 3: Climate Change - Historical vs Recent"""
    print("Creating Graph 3: Climate Change Analysis...")

    sql = """
    WITH historical AS (
        SELECT
            'Historical (1940-1979)' as period,
            CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as annual_total
        FROM snowfall.snowfall_daily
        WHERE YEAR(CAST(date AS DATE)) BETWEEN 1940 AND 1979
        GROUP BY year
    ),
    recent AS (
        SELECT
            'Recent (1990-2025)' as period,
            CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as annual_total
        FROM snowfall.snowfall_daily
        WHERE YEAR(CAST(date AS DATE)) BETWEEN 1990 AND 2025
        GROUP BY year
    )
    SELECT * FROM historical
    UNION ALL
    SELECT * FROM recent
    ORDER BY period, year
    """

    df = engine.query(sql)

    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # Plot 1: Box plot comparison
    historical_data = df[df['period'] == 'Historical (1940-1979)']['annual_total']
    recent_data = df[df['period'] == 'Recent (1990-2025)']['annual_total']

    bp = ax1.boxplot([historical_data, recent_data],
                      labels=['1940-1979', '1990-2025'],
                      patch_artist=True, showmeans=True)

    # Color the boxes
    colors = ['lightblue', 'lightcoral']
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)

    ax1.set_ylabel('Annual Snowfall (cm)', fontsize=12, fontweight='bold')
    ax1.set_title('Snowfall Distribution: Historical vs Recent', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')

    # Add statistics text
    hist_mean = historical_data.mean()
    recent_mean = recent_data.mean()
    change = recent_mean - hist_mean
    pct_change = (change / hist_mean) * 100

    stats_text = f'Historical Avg: {hist_mean:.1f} cm\n'
    stats_text += f'Recent Avg: {recent_mean:.1f} cm\n'
    stats_text += f'Change: {change:+.1f} cm ({pct_change:+.1f}%)'

    ax1.text(0.5, 0.02, stats_text, transform=ax1.transAxes,
            fontsize=11, verticalalignment='bottom',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

    # Plot 2: Time series with both periods
    for period in df['period'].unique():
        period_data = df[df['period'] == period]
        color = 'blue' if '1940' in period else 'red'
        ax2.plot(period_data['year'], period_data['annual_total'],
                marker='o', markersize=3, linewidth=1.5, label=period,
                color=color, alpha=0.6)

        # Add trend line
        z = np.polyfit(period_data['year'], period_data['annual_total'], 1)
        p = np.poly1d(z)
        ax2.plot(period_data['year'], p(period_data['year']),
                '--', linewidth=2, color=color, alpha=0.8)

    ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Annual Snowfall (cm)', fontsize=12, fontweight='bold')
    ax2.set_title('Annual Snowfall Trends: Two Periods', fontsize=13, fontweight='bold')
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)

    plt.suptitle('Climate Change Analysis: Northern Wisconsin Snowfall',
                 fontsize=14, fontweight='bold')

    # Save
    output_path = OUTPUT_DIR / "03_climate_change_analysis.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def create_monthly_patterns(engine):
    """Graph 4: Monthly Snowfall Patterns"""
    print("Creating Graph 4: Monthly Snowfall Patterns...")

    sql = """
    SELECT
        CAST(MONTH(CAST(date AS DATE)) AS INTEGER) as month,
        CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
        SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as total_cm
    FROM snowfall.snowfall_daily
    WHERE CAST(MONTH(CAST(date AS DATE)) AS INTEGER) IN (1,2,3,4,10,11,12)
    GROUP BY year, month
    ORDER BY year, month
    """

    df = engine.query(sql)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))

    # Calculate average by month
    month_avg = df.groupby('month')['total_cm'].mean()
    month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
                   10: 'Oct', 11: 'Nov', 12: 'Dec'}

    months = [month_names[m] for m in sorted(month_avg.index)]
    values = [month_avg[m] for m in sorted(month_avg.index)]

    colors = ['#d4e6f1', '#a9cce3', '#7fb3d5', '#5499c7',
              '#aed6f1', '#85c1e9', '#5dade2']

    bars = ax.bar(months, values, color=colors, edgecolor='black', linewidth=1.5)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average Monthly Snowfall (cm)', fontsize=12, fontweight='bold')
    ax.set_title('Average Monthly Snowfall Patterns (1940-2025)\nNorthern Wisconsin - 85 Year Average',
                 fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')

    # Save
    output_path = OUTPUT_DIR / "04_monthly_patterns.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def create_biggest_storms(engine):
    """Graph 5: Top 20 Biggest Snowstorms"""
    print("Creating Graph 5: Biggest Snowstorms in History...")

    sql = """
    SELECT
        date,
        station_id,
        ROUND(snowfall_mm / 10.0, 1) as snowfall_cm,
        CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year
    FROM snowfall.snowfall_daily
    WHERE snowfall_mm > 0
    ORDER BY snowfall_mm DESC
    LIMIT 20
    """

    df = engine.query(sql)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 10))

    # Create labels
    df['label'] = df.apply(lambda x: f"{x['date'][:10]}\n{x['station_id'].replace('WI_', '').replace('_', ' ')}", axis=1)

    # Color by decade
    decades = (df['year'] // 10) * 10
    unique_decades = sorted(decades.unique())
    colors_map = plt.cm.tab10(np.linspace(0, 1, len(unique_decades)))
    decade_colors = {decade: colors_map[i] for i, decade in enumerate(unique_decades)}
    bar_colors = [decade_colors[d] for d in decades]

    # Create horizontal bar chart
    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df['snowfall_cm'], color=bar_colors, edgecolor='black', linewidth=0.5)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(df['label'], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Snowfall (cm)', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Biggest Snowstorms in Northern Wisconsin History (1940-2025)',
                 fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='x')

    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, df['snowfall_cm'])):
        ax.text(val + 0.3, i, f'{val:.1f} cm',
                va='center', fontsize=9, fontweight='bold')

    # Add legend for decades
    legend_elements = [plt.Rectangle((0,0),1,1, facecolor=decade_colors[d],
                                     edgecolor='black', label=f"{d}s")
                      for d in unique_decades]
    ax.legend(handles=legend_elements, loc='lower right', title='Decade', framealpha=0.9)

    # Save
    output_path = OUTPUT_DIR / "05_biggest_storms.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def create_extreme_years(engine):
    """Graph 6: Snowiest and Least Snowy Years"""
    print("Creating Graph 6: Record Years...")

    sql = """
    WITH yearly_totals AS (
        SELECT
            CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
            station_id,
            SUM(CASE WHEN snowfall_mm > 0 THEN snowfall_mm ELSE 0 END) / 10.0 as annual_total
        FROM snowfall.snowfall_daily
        GROUP BY station_id, year
    )
    SELECT
        year,
        ROUND(AVG(annual_total), 1) as avg_cm
    FROM yearly_totals
    GROUP BY year
    ORDER BY avg_cm DESC
    """

    df = engine.query(sql)

    # Get top and bottom 10
    top10 = df.head(10)
    bottom10 = df.tail(10)

    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

    # Snowiest years
    colors1 = plt.cm.Blues(np.linspace(0.4, 0.9, len(top10)))
    bars1 = ax1.barh(range(len(top10)), top10['avg_cm'], color=colors1, edgecolor='black', linewidth=0.5)
    ax1.set_yticks(range(len(top10)))
    ax1.set_yticklabels(top10['year'].astype(str))
    ax1.invert_yaxis()
    ax1.set_xlabel('Average Snowfall (cm)', fontsize=12, fontweight='bold')
    ax1.set_title('Top 10 Snowiest Years', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='x')

    # Add value labels
    for i, (bar, val) in enumerate(zip(bars1, top10['avg_cm'])):
        ax1.text(val + 2, i, f'{val:.1f} cm', va='center', fontsize=10, fontweight='bold')

    # Least snowy years
    colors2 = plt.cm.Reds(np.linspace(0.4, 0.9, len(bottom10)))
    bars2 = ax2.barh(range(len(bottom10)), bottom10['avg_cm'], color=colors2, edgecolor='black', linewidth=0.5)
    ax2.set_yticks(range(len(bottom10)))
    ax2.set_yticklabels(bottom10['year'].astype(str))
    ax2.invert_yaxis()
    ax2.set_xlabel('Average Snowfall (cm)', fontsize=12, fontweight='bold')
    ax2.set_title('Top 10 Least Snowy Years', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='x')

    # Add value labels
    for i, (bar, val) in enumerate(zip(bars2, bottom10['avg_cm'])):
        ax2.text(val + 2, i, f'{val:.1f} cm', va='center', fontsize=10, fontweight='bold')

    plt.suptitle('Record Snowfall Years: Northern Wisconsin (1940-2025)',
                 fontsize=14, fontweight='bold')

    # Save
    output_path = OUTPUT_DIR / "06_record_years.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def create_summary_dashboard(engine):
    """Graph 7: Summary Dashboard"""
    print("Creating Graph 7: Summary Dashboard...")

    # Get overall stats
    stats_sql = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT station_id) as locations,
        MIN(date) as first_date,
        MAX(date) as last_date,
        ROUND(SUM(snowfall_mm) / 10.0, 1) as total_snow_cm,
        ROUND(AVG(temp_max_celsius), 1) as avg_high_c,
        ROUND(AVG(temp_min_celsius), 1) as avg_low_c,
        COUNT(CASE WHEN snowfall_mm > 0 THEN 1 END) as snow_days
    FROM snowfall.snowfall_daily
    """

    stats = engine.query(stats_sql).iloc[0]

    # Create figure
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3)

    # Title
    fig.suptitle('Northern Wisconsin Snowfall: 85-Year Summary Dashboard (1940-2025)',
                 fontsize=16, fontweight='bold', y=0.98)

    # Create text summary in top section
    ax_text = fig.add_subplot(gs[0, :])
    ax_text.axis('off')

    summary_text = f"""
    DATABASE SUMMARY
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Total Records: {int(stats['total_records']):,}  |  Locations: {int(stats['locations'])}  |  Period: {stats['first_date'][:10]} to {stats['last_date'][:10]} (85 years)

    Total Snowfall Recorded: {stats['total_snow_cm']:,.0f} cm ({stats['total_snow_cm']/2.54:,.0f} inches) = {stats['total_snow_cm']/100:,.0f} meters

    Days with Snow: {int(stats['snow_days']):,}  |  Average Temperature: High {stats['avg_high_c']:.1f}°C / Low {stats['avg_low_c']:.1f}°C
    """

    ax_text.text(0.5, 0.5, summary_text, ha='center', va='center',
                fontsize=11, family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

    # Add small charts in remaining spaces
    # This is a placeholder - the main graphs above are more detailed
    ax_text2 = fig.add_subplot(gs[1:, :])
    ax_text2.axis('off')

    info_text = """
    DETAILED VISUALIZATIONS CREATED:

    1. Annual Snowfall Trends (1940-2025) - Line graph showing year-over-year snowfall with trend line
    2. Decade-by-Decade Comparison - Bar charts comparing snowfall across decades
    3. Climate Change Analysis - Historical (1940-1979) vs Recent (1990-2025) comparison
    4. Monthly Snowfall Patterns - Average snowfall by month over 85 years
    5. Biggest Snowstorms in History - Top 20 single-day snowfall events
    6. Record Years - Snowiest and least snowy years on record

    All graphs saved to: ./snowfall_graphs/
    """

    ax_text2.text(0.5, 0.5, info_text, ha='center', va='center',
                 fontsize=12, family='monospace',
                 bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))

    # Save
    output_path = OUTPUT_DIR / "00_summary_dashboard.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved: {output_path.name}")
    plt.close()


def main():
    """Generate all visualizations"""

    db_path = "./northwoods_full_history.db"

    print(f"\nOpening database: {db_path}")

    try:
        engine = SnowfallDuckDB(db_path)

        print("\nGenerating visualizations...")
        print()

        # Create all graphs
        create_annual_snowfall_chart(engine)
        create_decade_comparison(engine)
        create_climate_change_comparison(engine)
        create_monthly_patterns(engine)
        create_biggest_storms(engine)
        create_extreme_years(engine)
        create_summary_dashboard(engine)

        engine.close()

        print()
        print("=" * 80)
        print("✅ ALL VISUALIZATIONS COMPLETE!")
        print("=" * 80)
        print()
        print(f"Location: {OUTPUT_DIR}")
        print()
        print("Generated graphs:")
        for file in sorted(OUTPUT_DIR.glob("*.png")):
            print(f"  • {file.name}")
        print()
        print("You can open these images to view the 85-year snowfall history!")
        print()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
