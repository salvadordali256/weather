#!/usr/bin/env python3
"""
Comprehensive Backtesting Report Generator
Tests forecast systems against 25 years of historical snow events (2000-2025)

This script:
1. Identifies all major snow events from 2000-2025
2. Runs forecast systems retroactively on each event
3. Calculates comprehensive accuracy metrics
4. Generates detailed report with visualizations
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json
from pathlib import Path

class ComprehensiveBacktestingReport:
    """Generate comprehensive backtesting report for forecast systems"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # Event thresholds
        self.THRESHOLDS = {
            'trace': 5.0,      # 5mm = light event
            'significant': 20.0,  # 20mm = significant event
            'major': 50.0,     # 50mm = major event
            'extreme': 100.0   # 100mm = extreme event
        }

        # Predictor stations with validated lag patterns
        self.predictors = {
            'thunder_bay_on': {'name': 'Thunder Bay', 'lag': 0, 'weight': 0.468, 'correlation': 0.468},
            'sapporo_japan': {'name': 'Sapporo', 'lag': 6, 'weight': 0.120, 'correlation': 0.120},
            'chamonix_france': {'name': 'Chamonix', 'lag': 5, 'weight': 0.115, 'correlation': 0.115},
            'mammoth_mountain_ca': {'name': 'Mammoth', 'lag': -3, 'weight': 0.111, 'correlation': 0.111},
            'denver_co': {'name': 'Denver', 'lag': -1, 'weight': 0.094, 'correlation': 0.094},
            'irkutsk_russia': {'name': 'Irkutsk', 'lag': 7, 'weight': 0.074, 'correlation': 0.074},
        }

    def get_wisconsin_snow_events(self, start_year=2000, end_year=2025) -> pd.DataFrame:
        """
        Get all Wisconsin snow events from specified time period
        Returns: DataFrame with date, snowfall_mm, station_id
        """
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                date,
                snowfall_mm,
                station_id
            FROM snowfall_daily
            WHERE station_id IN ('phelps_wi', 'land_o_lakes_wi', 'eagle_river_wi')
              AND date >= ?
              AND date < ?
              AND snowfall_mm > 0
            ORDER BY date, station_id
        """

        df = pd.read_sql_query(query, conn, params=(
            f'{start_year}-01-01',
            f'{end_year}-12-31'
        ))

        conn.close()

        df['date'] = pd.to_datetime(df['date'], format='mixed')
        return df

    def categorize_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add event category column"""
        df = df.copy()

        def categorize(snow_mm):
            if snow_mm >= self.THRESHOLDS['extreme']:
                return 'extreme'
            elif snow_mm >= self.THRESHOLDS['major']:
                return 'major'
            elif snow_mm >= self.THRESHOLDS['significant']:
                return 'significant'
            elif snow_mm >= self.THRESHOLDS['trace']:
                return 'trace'
            else:
                return 'none'

        df['category'] = df['snowfall_mm'].apply(categorize)
        return df

    def get_predictor_snow(self, station_id: str, target_date: datetime, window_days: int = 1) -> float:
        """
        Get average snowfall at a predictor station around a target date
        Returns: Average snowfall in mm (or None if no data)
        """
        conn = sqlite3.connect(self.db_path)

        start_date = target_date - timedelta(days=window_days)
        end_date = target_date + timedelta(days=window_days)

        query = """
            SELECT AVG(snowfall_mm) as avg_snow, MAX(snowfall_mm) as max_snow
            FROM snowfall_daily
            WHERE station_id = ?
              AND date >= ?
              AND date <= ?
        """

        df = pd.read_sql_query(query, conn, params=(
            station_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))

        conn.close()

        if not df.empty and pd.notna(df.iloc[0]['avg_snow']):
            return df.iloc[0]['avg_snow']
        return None

    def run_ensemble_forecast_retroactive(self, event_date: datetime) -> Dict:
        """
        Run ensemble forecast retroactively for a specific date
        Uses validated predictor correlations and lag patterns

        Returns: Dict with forecast details
        """
        ensemble_score = 0.0
        total_weight = 0.0
        active_predictors = []

        for station_id, config in self.predictors.items():
            # Calculate the date to check based on lag
            lag_days = config['lag']
            check_date = event_date - timedelta(days=lag_days)

            # Get predictor snow
            predictor_snow = self.get_predictor_snow(station_id, check_date, window_days=1)

            if predictor_snow is not None:
                # Categorize predictor activity
                if predictor_snow >= 25.0:
                    activity = 1.0  # Heavy
                elif predictor_snow >= 15.0:
                    activity = 0.6  # Moderate
                elif predictor_snow >= 5.0:
                    activity = 0.3  # Light
                else:
                    activity = 0.0  # Trace/None

                contribution = config['weight'] * activity
                ensemble_score += contribution
                total_weight += config['weight']

                if activity > 0:
                    active_predictors.append({
                        'station': config['name'],
                        'snow_mm': predictor_snow,
                        'lag_days': lag_days,
                        'contribution': contribution
                    })

        # Normalize score
        normalized_score = ensemble_score / total_weight if total_weight > 0 else 0.0

        # Generate probability estimate based on ensemble score
        # These thresholds are based on correlation strength
        if normalized_score >= 0.70:
            probability = 85  # High confidence
            forecast_category = 'major'
        elif normalized_score >= 0.50:
            probability = 70  # Moderate-high confidence
            forecast_category = 'significant'
        elif normalized_score >= 0.30:
            probability = 50  # Moderate confidence
            forecast_category = 'trace'
        elif normalized_score >= 0.15:
            probability = 30  # Low-moderate confidence
            forecast_category = 'trace'
        else:
            probability = 10  # Low confidence
            forecast_category = 'none'

        return {
            'ensemble_score': normalized_score,
            'probability': probability,
            'forecast_category': forecast_category,
            'active_predictors': active_predictors,
            'predictor_count': len(active_predictors)
        }

    def backtest_all_events(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Run retroactive forecasts for all events
        Returns: DataFrame with events and forecasts
        """
        results = []

        print(f"\n{'='*80}")
        print(f"BACKTESTING: Running retroactive forecasts on {len(events_df)} events")
        print(f"{'='*80}\n")

        total = len(events_df)
        for idx, event in events_df.iterrows():
            if idx % 100 == 0:
                print(f"Progress: {idx}/{total} events processed...")

            event_date = event['date']
            actual_snow = event['snowfall_mm']
            actual_category = event['category']

            # Run ensemble forecast
            forecast = self.run_ensemble_forecast_retroactive(event_date)

            # Determine if forecast was correct
            forecast_hit = False
            if actual_category == 'extreme' or actual_category == 'major':
                # For major/extreme events, forecast should have high probability
                forecast_hit = forecast['probability'] >= 50
            elif actual_category == 'significant':
                # For significant events, moderate probability acceptable
                forecast_hit = forecast['probability'] >= 30
            elif actual_category == 'trace':
                # For trace events, any positive probability
                forecast_hit = forecast['probability'] >= 10

            results.append({
                'date': event_date,
                'station_id': event['station_id'],
                'actual_snow_mm': actual_snow,
                'actual_category': actual_category,
                'forecast_probability': forecast['probability'],
                'forecast_category': forecast['forecast_category'],
                'ensemble_score': forecast['ensemble_score'],
                'active_predictors': forecast['predictor_count'],
                'forecast_hit': forecast_hit
            })

        print(f"✅ Completed: {total} events backtested\n")

        return pd.DataFrame(results)

    def calculate_metrics(self, results_df: pd.DataFrame) -> Dict:
        """
        Calculate comprehensive verification metrics
        """
        # Overall statistics
        n_total = len(results_df)

        # Metrics by category
        metrics = {}

        for category in ['trace', 'significant', 'major', 'extreme']:
            category_events = results_df[results_df['actual_category'] == category]
            n_events = len(category_events)

            if n_events == 0:
                continue

            # Hit rate (how many were correctly forecast)
            hits = category_events['forecast_hit'].sum()
            hit_rate = hits / n_events if n_events > 0 else 0

            # Average probability assigned to this category
            avg_probability = category_events['forecast_probability'].mean()

            # Average ensemble score
            avg_ensemble = category_events['ensemble_score'].mean()

            metrics[category] = {
                'n_events': n_events,
                'hits': hits,
                'hit_rate': hit_rate,
                'avg_probability': avg_probability,
                'avg_ensemble_score': avg_ensemble
            }

        # Overall performance
        total_hits = results_df['forecast_hit'].sum()
        overall_hit_rate = total_hits / n_total if n_total > 0 else 0

        # False alarm analysis (days with high probability but low actual snow)
        # Create quiet days dataset (need to query separately)

        # Correlation between ensemble score and actual snow
        correlation = results_df['ensemble_score'].corr(results_df['actual_snow_mm'])

        return {
            'by_category': metrics,
            'overall': {
                'n_total_events': n_total,
                'total_hits': total_hits,
                'hit_rate': overall_hit_rate,
                'score_snow_correlation': correlation
            }
        }

    def generate_report(self):
        """Main report generation"""

        print(f"\n{'='*80}")
        print(f"COMPREHENSIVE BACKTESTING REPORT")
        print(f"Snowfall Forecast System Validation (2000-2025)")
        print(f"{'='*80}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: {self.db_path}")
        print(f"{'='*80}\n")

        # Step 1: Load all Wisconsin snow events
        print("STEP 1: Loading historical Wisconsin snow events...")
        events_df = self.get_wisconsin_snow_events(start_year=2000, end_year=2025)
        events_df = self.categorize_events(events_df)

        print(f"✅ Loaded {len(events_df)} snow events (2000-2025)")
        print(f"\nEvent Distribution:")
        category_counts = events_df['category'].value_counts()
        for cat, count in category_counts.items():
            print(f"  {cat:15s}: {count:5d} events")

        # Step 2: Run backtesting
        print(f"\nSTEP 2: Running retroactive forecasts...")
        results_df = self.backtest_all_events(events_df)

        # Step 3: Calculate metrics
        print(f"STEP 3: Calculating verification metrics...")
        metrics = self.calculate_metrics(results_df)

        # Step 4: Display results
        self._display_metrics_report(metrics, results_df)

        # Step 5: Save detailed results
        self._save_results(results_df, metrics)

        return results_df, metrics

    def _display_metrics_report(self, metrics: Dict, results_df: pd.DataFrame):
        """Display detailed metrics report"""

        print(f"\n{'='*80}")
        print(f"BACKTESTING RESULTS")
        print(f"{'='*80}\n")

        # Overall performance
        overall = metrics['overall']
        print(f"OVERALL PERFORMANCE:")
        print(f"{'─'*80}")
        print(f"Total events tested:        {overall['n_total_events']:,}")
        print(f"Successfully forecast:      {overall['total_hits']:,}")
        print(f"Overall hit rate:           {overall['hit_rate']:.1%}")
        print(f"Correlation (score vs snow): {overall['score_snow_correlation']:.3f}")
        print()

        # Performance by category
        print(f"PERFORMANCE BY EVENT CATEGORY:")
        print(f"{'─'*80}")
        print(f"{'Category':<15s} | {'Events':>8s} | {'Hits':>8s} | {'Hit Rate':>10s} | {'Avg Prob':>10s} | {'Avg Score':>10s}")
        print(f"{'─'*80}")

        by_category = metrics['by_category']
        category_order = ['extreme', 'major', 'significant', 'trace']

        for cat in category_order:
            if cat in by_category:
                m = by_category[cat]
                print(f"{cat:<15s} | {m['n_events']:>8,} | {m['hits']:>8,} | "
                      f"{m['hit_rate']:>9.1%} | {m['avg_probability']:>9.0f}% | "
                      f"{m['avg_ensemble_score']:>10.3f}")

        print(f"\n{'─'*80}")

        # Interpretation
        print(f"\nINTERPRETATION:")
        print(f"{'─'*80}\n")

        # Grade overall performance
        hit_rate = overall['hit_rate']
        if hit_rate >= 0.70:
            grade = "🟢 EXCELLENT"
            interpretation = "System successfully predicts >70% of snow events"
        elif hit_rate >= 0.50:
            grade = "🟡 GOOD"
            interpretation = "System shows useful skill but has room for improvement"
        elif hit_rate >= 0.30:
            grade = "🟠 FAIR"
            interpretation = "System shows some skill but needs refinement"
        else:
            grade = "🔴 NEEDS WORK"
            interpretation = "System requires significant improvements"

        print(f"Overall Grade: {grade}")
        print(f"  {interpretation}")
        print()

        # Category-specific insights
        if 'major' in by_category:
            major_hit_rate = by_category['major']['hit_rate']
            print(f"Major Events: {major_hit_rate:.1%} detection rate")
            if major_hit_rate >= 0.60:
                print(f"  ✓ Good skill at detecting major snow events")
            else:
                print(f"  ⚠ Needs improvement for major event detection")

        if 'significant' in by_category:
            sig_hit_rate = by_category['significant']['hit_rate']
            print(f"Significant Events: {sig_hit_rate:.1%} detection rate")
            if sig_hit_rate >= 0.50:
                print(f"  ✓ Reasonable skill for significant events")
            else:
                print(f"  ⚠ Could improve significant event detection")

        print()

        # Top 10 best forecasts
        print(f"\n{'='*80}")
        print(f"TOP 10 BEST FORECASTS (Major Events)")
        print(f"{'='*80}\n")

        major_events = results_df[results_df['actual_category'].isin(['major', 'extreme'])]
        best_forecasts = major_events.nlargest(10, 'ensemble_score')

        print(f"{'Date':<12s} | {'Actual':>8s} | {'Probability':>12s} | {'Score':>8s} | {'Result':>10s}")
        print(f"{'─'*80}")

        for _, row in best_forecasts.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            result = "✅ HIT" if row['forecast_hit'] else "❌ MISS"
            print(f"{date_str:<12s} | {row['actual_snow_mm']:>7.1f}mm | "
                  f"{row['forecast_probability']:>11.0f}% | {row['ensemble_score']:>8.3f} | {result:>10s}")

        # Top 10 misses
        print(f"\n{'='*80}")
        print(f"TOP 10 MISSED MAJOR EVENTS (Needs Investigation)")
        print(f"{'='*80}\n")

        missed = major_events[~major_events['forecast_hit']].nlargest(10, 'actual_snow_mm')

        if len(missed) > 0:
            print(f"{'Date':<12s} | {'Actual':>8s} | {'Probability':>12s} | {'Score':>8s} | {'Predictors':>11s}")
            print(f"{'─'*80}")

            for _, row in missed.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                print(f"{date_str:<12s} | {row['actual_snow_mm']:>7.1f}mm | "
                      f"{row['forecast_probability']:>11.0f}% | {row['ensemble_score']:>8.3f} | "
                      f"{row['active_predictors']:>11d}")
        else:
            print("✅ No major events missed!")

        print(f"\n{'='*80}\n")

    def _save_results(self, results_df: pd.DataFrame, metrics: Dict):
        """Save detailed results to files"""

        # Save full results to CSV
        output_csv = 'backtesting_results_2000_2025.csv'
        results_df.to_csv(output_csv, index=False)
        print(f"✅ Detailed results saved to: {output_csv}")

        # Save metrics to JSON
        # Convert datetime objects to strings for JSON serialization
        results_for_json = results_df.copy()
        results_for_json['date'] = results_for_json['date'].dt.strftime('%Y-%m-%d')

        output_json = {
            'generated': datetime.now().isoformat(),
            'time_period': '2000-2025',
            'metrics': metrics,
            'sample_results': results_for_json.head(100).to_dict('records')
        }

        with open('backtesting_metrics_2000_2025.json', 'w') as f:
            json.dump(output_json, f, indent=2, default=str)

        print(f"✅ Metrics summary saved to: backtesting_metrics_2000_2025.json")

        # Generate summary report text file
        with open('BACKTESTING_SUMMARY_2000_2025.txt', 'w') as f:
            f.write("="*80 + "\n")
            f.write("SNOWFALL FORECAST SYSTEM BACKTESTING REPORT\n")
            f.write("Historical Validation: 2000-2025\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            overall = metrics['overall']
            f.write("OVERALL PERFORMANCE:\n")
            f.write(f"  Total events tested: {overall['n_total_events']:,}\n")
            f.write(f"  Successfully forecast: {overall['total_hits']:,}\n")
            f.write(f"  Hit rate: {overall['hit_rate']:.1%}\n")
            f.write(f"  Correlation: {overall['score_snow_correlation']:.3f}\n\n")

            f.write("PERFORMANCE BY CATEGORY:\n")
            by_category = metrics['by_category']
            for cat in ['extreme', 'major', 'significant', 'trace']:
                if cat in by_category:
                    m = by_category[cat]
                    f.write(f"\n{cat.upper()}:\n")
                    f.write(f"  Events: {m['n_events']:,}\n")
                    f.write(f"  Hits: {m['hits']:,}\n")
                    f.write(f"  Hit rate: {m['hit_rate']:.1%}\n")
                    f.write(f"  Avg probability: {m['avg_probability']:.0f}%\n")
                    f.write(f"  Avg ensemble score: {m['avg_ensemble_score']:.3f}\n")

            f.write("\n" + "="*80 + "\n")
            f.write("KEY FINDINGS:\n")
            f.write("="*80 + "\n\n")

            hit_rate = overall['hit_rate']
            if hit_rate >= 0.70:
                f.write("✓ EXCELLENT: System shows strong predictive skill (>70% hit rate)\n")
            elif hit_rate >= 0.50:
                f.write("✓ GOOD: System demonstrates useful forecasting capability\n")
            else:
                f.write("⚠ NEEDS IMPROVEMENT: System requires refinement\n")

            f.write(f"\nThe ensemble forecast system, using validated global teleconnections,\n")
            f.write(f"was tested on {overall['n_total_events']:,} historical snow events spanning 25 years.\n")
            f.write(f"The system correctly identified {overall['hit_rate']:.0%} of events.\n\n")

            if 'major' in by_category:
                major_hr = by_category['major']['hit_rate']
                f.write(f"For major events (≥50mm), the system achieved a {major_hr:.0%} hit rate,\n")
                f.write(f"demonstrating {'strong' if major_hr >= 0.60 else 'moderate'} skill at detecting significant snowfall.\n")

        print(f"✅ Summary report saved to: BACKTESTING_SUMMARY_2000_2025.txt")
        print(f"\n{'='*80}\n")


def main():
    """Run comprehensive backtesting report"""

    # Check if database exists
    db_path = 'demo_global_snowfall.db'
    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        print(f"   Please ensure you're in the correct directory")
        return

    print("\n" + "="*80)
    print("COMPREHENSIVE BACKTESTING REPORT GENERATOR")
    print("="*80)
    print("\nThis will test the forecast system against 25 years of historical data.")
    print("Expected runtime: 2-5 minutes for ~10,000+ events\n")

    # Run report
    reporter = ComprehensiveBacktestingReport(db_path=db_path)
    results_df, metrics = reporter.generate_report()

    print("\n" + "="*80)
    print("BACKTESTING COMPLETE!")
    print("="*80)
    print("\nFiles generated:")
    print("  1. backtesting_results_2000_2025.csv - Full detailed results")
    print("  2. backtesting_metrics_2000_2025.json - Metrics summary")
    print("  3. BACKTESTING_SUMMARY_2000_2025.txt - Human-readable report")
    print("\n" + "="*80 + "\n")


if __name__ == '__main__':
    main()
