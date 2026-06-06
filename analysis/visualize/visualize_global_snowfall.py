#!/usr/bin/env python3
"""
Global Snowfall Visualization System
Creates comprehensive visualizations of worldwide snowfall patterns and correlations

Visualizations:
1. Global snowfall map (heatmap)
2. Correlation matrix (regions vs Wisconsin)
3. Lag pattern visualization
4. Time series comparisons
5. Extreme event co-occurrence
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import seaborn as sns
from datetime import datetime
import json
import os


class GlobalSnowfallVisualizer:
    """Visualize global snowfall patterns and correlations"""

    def __init__(self, db_path: str = "global_snowfall.db", output_dir: str = "snowfall_graphs"):
        self.db_path = db_path
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        # Set style
        sns.set_style("darkgrid")
        plt.rcParams['figure.figsize'] = (16, 10)
        plt.rcParams['font.size'] = 10

    def load_model(self, model_file: str = "phelps_prediction_model.json") -> dict:
        """Load prediction model"""
        if os.path.exists(model_file):
            with open(model_file, 'r') as f:
                return json.load(f)
        return None

    def get_regional_coordinates(self) -> pd.DataFrame:
        """Get average coordinates for each region"""
        conn = sqlite3.connect(self.db_path)
        query = """
            SELECT
                region,
                AVG(latitude) as lat,
                AVG(longitude) as lon,
                COUNT(*) as num_stations
            FROM stations
            GROUP BY region
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_correlation_data(self) -> pd.DataFrame:
        """Get cached correlation data"""
        conn = sqlite3.connect(self.db_path)
        query = """
            SELECT
                region_a,
                region_b,
                correlation_coefficient,
                lag_days,
                sample_size,
                significance_p_value
            FROM correlations
            ORDER BY ABS(correlation_coefficient) DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def plot_correlation_heatmap(self, model_file: str = "phelps_prediction_model.json"):
        """Create correlation heatmap for top predictors"""
        model = self.load_model(model_file)
        if not model:
            print("✗ No model file found")
            return

        fig, ax = plt.subplots(figsize=(14, 10))

        predictors = model['predictors'][:20]  # Top 20
        regions = [p['region'].replace('_', ' ').title() for p in predictors]
        correlations = [p['correlation'] for p in predictors]
        lags = [p['lag_days'] for p in predictors]
        significant = [p['significant'] for p in predictors]

        # Create data for heatmap
        data = np.array([[corr] for corr in correlations])

        # Plot
        im = ax.imshow(data, cmap='RdBu_r', aspect='auto', vmin=-1, vmax=1)

        # Set ticks and labels
        ax.set_yticks(np.arange(len(regions)))
        ax.set_yticklabels(regions)
        ax.set_xticks([0])
        ax.set_xticklabels(['Northern Wisconsin\n(Phelps/Land O\'Lakes)'])

        # Add correlation values and lag info
        for i, (corr, lag, sig) in enumerate(zip(correlations, lags, significant)):
            text_color = 'white' if abs(corr) > 0.5 else 'black'
            sig_mark = '***' if sig else ''

            if lag > 0:
                lag_text = f'→ {lag}d'
            elif lag < 0:
                lag_text = f'← {abs(lag)}d'
            else:
                lag_text = '0d'

            ax.text(0, i, f'{corr:+.2f}{sig_mark}\n{lag_text}',
                   ha="center", va="center", color=text_color, fontsize=9)

        # Colorbar
        cbar = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cbar.set_label('Correlation Coefficient', rotation=270, labelpad=20)

        plt.title(f'Global Snowfall Correlations → Northern Wisconsin\n'
                 f'(*** = statistically significant, arrows show lead/lag pattern)',
                 fontsize=14, fontweight='bold', pad=20)

        plt.tight_layout()
        output_file = os.path.join(self.output_dir, 'global_correlation_heatmap.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()

    def plot_lag_distribution(self, model_file: str = "phelps_prediction_model.json"):
        """Plot distribution of lag patterns"""
        model = self.load_model(model_file)
        if not model:
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        predictors = model['predictors']
        lags = [p['lag_days'] for p in predictors]
        correlations = [p['correlation'] for p in predictors]

        # Histogram of lags
        ax1.hist(lags, bins=range(min(lags)-1, max(lags)+2), edgecolor='black', alpha=0.7)
        ax1.axvline(x=0, color='red', linestyle='--', linewidth=2, label='Simultaneous')
        ax1.axvline(x=np.mean(lags), color='green', linestyle='--', linewidth=2,
                   label=f'Mean lag: {np.mean(lags):.1f} days')
        ax1.set_xlabel('Lag (days)', fontsize=12)
        ax1.set_ylabel('Number of Regions', fontsize=12)
        ax1.set_title('Distribution of Lag Patterns\n(Positive = Region leads Wisconsin)', fontsize=12, fontweight='bold')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # Scatter: correlation vs lag
        colors = ['red' if p['significant'] else 'gray' for p in predictors]
        ax2.scatter(lags, correlations, c=colors, alpha=0.6, s=100, edgecolors='black', linewidth=0.5)
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax2.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
        ax2.set_xlabel('Lag (days)', fontsize=12)
        ax2.set_ylabel('Correlation Coefficient', fontsize=12)
        ax2.set_title('Correlation Strength vs Lag Pattern\n(Red = significant, Gray = not significant)',
                     fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3)

        # Add annotations for strongest correlations
        for i, pred in enumerate(predictors[:5]):
            ax2.annotate(pred['region'].replace('_', ' '),
                        (pred['lag_days'], pred['correlation']),
                        xytext=(10, 10), textcoords='offset points',
                        fontsize=8, alpha=0.7,
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3))

        plt.tight_layout()
        output_file = os.path.join(self.output_dir, 'lag_distribution.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()

    def plot_regional_comparison(self, regions: list = None, start_year: int = 2020):
        """Plot time series comparison of multiple regions"""
        if regions is None:
            regions = ['northern_wisconsin', 'siberia_central', 'japan_north', 'california_ski']

        fig, axes = plt.subplots(len(regions), 1, figsize=(16, 3*len(regions)), sharex=True)
        if len(regions) == 1:
            axes = [axes]

        conn = sqlite3.connect(self.db_path)

        for ax, region in zip(axes, regions):
            query = """
                SELECT
                    d.date,
                    SUM(d.snowfall_mm) as total_snowfall_mm
                FROM snowfall_daily d
                JOIN stations s ON d.station_id = s.station_id
                WHERE s.region = ? AND d.date >= ?
                GROUP BY d.date
                ORDER BY d.date
            """
            df = pd.read_sql_query(query, conn, params=(region, f"{start_year}-01-01"))
            df['date'] = pd.to_datetime(df['date'])

            # Plot
            ax.plot(df['date'], df['total_snowfall_mm'], linewidth=0.8, alpha=0.7)
            ax.fill_between(df['date'], 0, df['total_snowfall_mm'], alpha=0.3)

            # Styling
            ax.set_ylabel('Snowfall (mm)', fontsize=10)
            ax.set_title(f"{region.replace('_', ' ').title()}", fontsize=11, fontweight='bold')
            ax.grid(True, alpha=0.3)

            # Add statistics
            total = df['total_snowfall_mm'].sum()
            mean_daily = df['total_snowfall_mm'].mean()
            max_daily = df['total_snowfall_mm'].max()
            ax.text(0.02, 0.95, f'Total: {total:.0f} mm | Avg: {mean_daily:.1f} mm/day | Max: {max_daily:.0f} mm',
                   transform=ax.transAxes, fontsize=9, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        conn.close()

        axes[-1].set_xlabel('Date', fontsize=12)
        plt.suptitle(f'Global Snowfall Comparison ({start_year}-Present)', fontsize=14, fontweight='bold')
        plt.tight_layout()

        output_file = os.path.join(self.output_dir, f'regional_comparison_{start_year}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()

    def plot_global_map(self, model_file: str = "phelps_prediction_model.json"):
        """Plot global map showing correlation strength"""
        model = self.load_model(model_file)
        if not model:
            return

        # Get regional coordinates
        coords = self.get_regional_coordinates()

        fig, ax = plt.subplots(figsize=(18, 10))

        # Create correlation lookup
        corr_lookup = {p['region']: p for p in model['predictors']}

        # Plot all regions
        for _, row in coords.iterrows():
            region = row['region']
            lat, lon = row['lat'], row['lon']

            if region in corr_lookup:
                pred = corr_lookup[region]
                corr = pred['correlation']
                size = abs(corr) * 500 + 50
                color = 'red' if corr > 0 else 'blue'
                alpha = min(abs(corr) + 0.3, 1.0)
                marker = 'o'

                ax.scatter(lon, lat, s=size, c=color, alpha=alpha, edgecolors='black',
                          linewidth=1, marker=marker, zorder=3)

                # Add label for strong correlations
                if abs(corr) > 0.2:
                    ax.text(lon, lat+2, region.replace('_', '\n'),
                           fontsize=7, ha='center', va='bottom',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7))
            else:
                # Non-predictor regions (gray)
                ax.scatter(lon, lat, s=30, c='gray', alpha=0.3, edgecolors='black',
                          linewidth=0.5, zorder=2)

        # Highlight target (Northern Wisconsin)
        wi_coords = coords[coords['region'] == 'northern_wisconsin']
        if not wi_coords.empty:
            ax.scatter(wi_coords['lon'], wi_coords['lat'], s=800, c='gold',
                      marker='*', edgecolors='black', linewidth=2, zorder=5,
                      label='Target: Phelps/Land O\'Lakes, WI')

        # Styling
        ax.set_xlabel('Longitude', fontsize=12)
        ax.set_ylabel('Latitude', fontsize=12)
        ax.set_title('Global Snowfall Prediction Network for Northern Wisconsin\n'
                    '(Red = positive correlation, Blue = negative correlation, Size = strength)',
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3)
        ax.legend(loc='lower left', fontsize=12)

        # Add colorbar
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='red', alpha=0.7, label='Positive Correlation'),
            Patch(facecolor='blue', alpha=0.7, label='Negative Correlation'),
            Patch(facecolor='gray', alpha=0.3, label='Weak/No Correlation'),
            Patch(facecolor='gold', label='Target Location')
        ]
        ax.legend(handles=legend_elements, loc='upper left', fontsize=10)

        plt.tight_layout()
        output_file = os.path.join(self.output_dir, 'global_correlation_map.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()

    def plot_prediction_model_summary(self, model_file: str = "phelps_prediction_model.json"):
        """Create comprehensive model summary visualization"""
        model = self.load_model(model_file)
        if not model:
            return

        fig = plt.figure(figsize=(18, 12))
        gs = GridSpec(3, 2, figure=fig, hspace=0.3, wspace=0.3)

        predictors = model['predictors'][:15]

        # 1. Correlation bar chart
        ax1 = fig.add_subplot(gs[0, :])
        regions = [p['region'].replace('_', ' ').title() for p in predictors]
        correlations = [p['correlation'] for p in predictors]
        colors = ['red' if c > 0 else 'blue' for c in correlations]

        bars = ax1.barh(regions, correlations, color=colors, alpha=0.7, edgecolor='black')
        ax1.axvline(x=0, color='black', linewidth=1)
        ax1.set_xlabel('Correlation Coefficient', fontsize=11)
        ax1.set_title('Top 15 Global Predictors for Northern Wisconsin Snowfall',
                     fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='x')

        # 2. Model weights pie chart
        ax2 = fig.add_subplot(gs[1, 0])
        top5 = predictors[:5]
        other_weight = sum(p['normalized_weight'] for p in predictors[5:])
        weights = [p['normalized_weight'] for p in top5] + [other_weight]
        labels = [p['region'].replace('_', '\n') for p in top5] + ['Other\nRegions']

        ax2.pie(weights, labels=labels, autopct='%1.1f%%', startangle=90)
        ax2.set_title('Model Weight Distribution\n(Top 5 + Others)', fontsize=12, fontweight='bold')

        # 3. Lag pattern breakdown
        ax3 = fig.add_subplot(gs[1, 1])
        lags = [p['lag_days'] for p in predictors]
        lag_categories = {
            'Leads by 10+ days': sum(1 for l in lags if l >= 10),
            'Leads by 5-9 days': sum(1 for l in lags if 5 <= l < 10),
            'Leads by 1-4 days': sum(1 for l in lags if 1 <= l < 5),
            'Simultaneous': sum(1 for l in lags if l == 0),
            'Lags by 1-4 days': sum(1 for l in lags if -4 <= l < 0),
            'Lags by 5+ days': sum(1 for l in lags if l <= -5),
        }

        ax3.bar(lag_categories.keys(), lag_categories.values(), color='steelblue',
               alpha=0.7, edgecolor='black')
        ax3.set_ylabel('Number of Regions', fontsize=11)
        ax3.set_title('Lag Pattern Distribution', fontsize=12, fontweight='bold')
        ax3.tick_params(axis='x', rotation=45)
        ax3.grid(True, alpha=0.3, axis='y')

        # 4. Regional breakdown
        ax4 = fig.add_subplot(gs[2, :])
        region_types = {
            'Russia/Siberia': sum(1 for p in predictors if 'russia' in p['region'] or 'siberia' in p['region']),
            'Japan': sum(1 for p in predictors if 'japan' in p['region']),
            'China': sum(1 for p in predictors if 'china' in p['region']),
            'Western US': sum(1 for p in predictors if 'california' in p['region'] or 'colorado' in p['region']),
            'Canada': sum(1 for p in predictors if 'canada' in p['region']),
            'Europe': sum(1 for p in predictors if 'alps' in p['region'] or 'scandinavia' in p['region']),
            'Pacific NW': sum(1 for p in predictors if 'pacific_northwest' in p['region']),
        }

        ax4.bar(region_types.keys(), region_types.values(), color='forestgreen',
               alpha=0.7, edgecolor='black')
        ax4.set_ylabel('Number of Predictors', fontsize=11)
        ax4.set_xlabel('Geographic Region', fontsize=11)
        ax4.set_title('Top Predictors by Geographic Region', fontsize=12, fontweight='bold')
        ax4.grid(True, alpha=0.3, axis='y')

        plt.suptitle(f"Global Snowfall Prediction Model Summary\nTarget: Phelps & Land O'Lakes, Wisconsin",
                    fontsize=14, fontweight='bold')

        output_file = os.path.join(self.output_dir, 'prediction_model_summary.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()

    def generate_all_visualizations(self, model_file: str = "phelps_prediction_model.json"):
        """Generate complete visualization suite"""
        print(f"\n{'='*80}")
        print(f"GENERATING GLOBAL SNOWFALL VISUALIZATIONS")
        print(f"{'='*80}\n")

        self.plot_correlation_heatmap(model_file)
        self.plot_lag_distribution(model_file)
        self.plot_global_map(model_file)
        self.plot_prediction_model_summary(model_file)
        self.plot_regional_comparison(start_year=2020)

        print(f"\n{'='*80}")
        print(f"✓ ALL VISUALIZATIONS COMPLETE")
        print(f"Output directory: {self.output_dir}")
        print(f"{'='*80}\n")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Global Snowfall Visualization')
    parser.add_argument('--db', default='global_snowfall.db', help='Database file')
    parser.add_argument('--model', default='phelps_prediction_model.json', help='Model file')
    parser.add_argument('--output-dir', default='snowfall_graphs', help='Output directory')

    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"✗ Database not found: {args.db}")
        print(f"  Run data collection first")
        return

    visualizer = GlobalSnowfallVisualizer(db_path=args.db, output_dir=args.output_dir)
    visualizer.generate_all_visualizations(model_file=args.model)


if __name__ == '__main__':
    main()
