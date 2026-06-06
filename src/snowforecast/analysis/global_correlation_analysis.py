#!/usr/bin/env python3
"""
Global Snowfall Correlation Analysis
Analyzes cross-regional snowfall patterns to predict Phelps and Land O'Lakes, WI snowfall

Key Questions:
1. When Russia/Siberia gets heavy snow, does Wisconsin follow 5-10 days later?
2. When California/Colorado ski resorts get pounded, what happens in Wisconsin?
3. When Japan sees extreme snowfall, does it indicate amplified jet stream affecting North America?
4. What are the strongest global predictors for Northern Wisconsin snowfall?

Scientific Basis:
- Teleconnections: Global atmospheric patterns linked across continents
- Wave propagation: Asian jet stream → Stratosphere → North America (5-10 day lag)
- Cold air source: Siberian snow → Polar vortex disruption → Wisconsin outbreaks
- Pacific patterns: Western US snow → Atmospheric river/blocking → Downstream effects
"""

import sqlite3
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json


class GlobalCorrelationAnalyzer:
    """Analyze global snowfall correlations and teleconnections"""

    def __init__(self, db_path: str = "global_snowfall.db"):
        self.db_path = db_path

    def get_regional_timeseries(self, region: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Get aggregated daily snowfall timeseries for a region

        Args:
            region: Region name from GLOBAL_LOCATIONS
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            DataFrame with date and total regional snowfall
        """
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                d.date,
                SUM(d.snowfall_mm) as total_snowfall_mm,
                AVG(d.snowfall_mm) as avg_snowfall_mm,
                MAX(d.snowfall_mm) as max_snowfall_mm,
                COUNT(DISTINCT d.station_id) as num_stations
            FROM snowfall_daily d
            JOIN stations s ON d.station_id = s.station_id
            WHERE s.region = ?
        """

        params = [region]
        if start_date:
            query += " AND d.date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND d.date <= ?"
            params.append(end_date)

        query += " GROUP BY d.date ORDER BY d.date"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        df['date'] = pd.to_datetime(df['date'])
        return df

    def get_station_timeseries(self, station_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Get daily snowfall timeseries for a specific station"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                date,
                snowfall_mm,
                snow_depth_mm,
                temp_mean_celsius,
                precipitation_mm
            FROM snowfall_daily
            WHERE station_id = ?
        """

        params = [station_id]
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)

        query += " ORDER BY date"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        df['date'] = pd.to_datetime(df['date'])
        return df

    def calculate_lag_correlation(self, series_a: pd.Series, series_b: pd.Series, max_lag_days: int = 30) -> Dict:
        """
        Calculate correlation between two timeseries at various lag intervals

        Args:
            series_a: First timeseries (predictor)
            series_b: Second timeseries (target)
            max_lag_days: Maximum lag to test (default: 30 days)

        Returns:
            Dictionary with best correlation, lag, and full results
        """
        results = []

        for lag in range(-max_lag_days, max_lag_days + 1):
            if lag < 0:
                # series_a leads series_b (series_a happens first)
                s_a = series_a.iloc[:lag].values
                s_b = series_b.iloc[-lag:].values
            elif lag > 0:
                # series_b leads series_a (series_b happens first)
                s_a = series_a.iloc[lag:].values
                s_b = series_b.iloc[:-lag].values
            else:
                # No lag (simultaneous)
                s_a = series_a.values
                s_b = series_b.values

            # Need at least 30 overlapping points for meaningful correlation
            if len(s_a) >= 30 and len(s_b) >= 30:
                # Remove NaN values
                mask = ~(np.isnan(s_a) | np.isnan(s_b))
                s_a_clean = s_a[mask]
                s_b_clean = s_b[mask]

                if len(s_a_clean) >= 30:
                    corr, p_value = stats.pearsonr(s_a_clean, s_b_clean)
                    results.append({
                        'lag_days': lag,
                        'correlation': corr,
                        'p_value': p_value,
                        'sample_size': len(s_a_clean),
                        'significant': p_value < 0.05
                    })

        if not results:
            return None

        # Find best correlation (by absolute value)
        best = max(results, key=lambda x: abs(x['correlation']))

        return {
            'best_correlation': best['correlation'],
            'best_lag_days': best['lag_days'],
            'p_value': best['p_value'],
            'sample_size': best['sample_size'],
            'significant': best['significant'],
            'all_lags': results
        }

    def analyze_region_pair(self, region_a: str, region_b: str, max_lag_days: int = 30,
                           start_date: str = "1940-01-01", end_date: str = None) -> Dict:
        """
        Analyze correlation between two regions at various lag intervals

        Args:
            region_a: First region (predictor)
            region_b: Second region (target, usually northern_wisconsin)
            max_lag_days: Maximum lag to test
            start_date: Analysis start date
            end_date: Analysis end date

        Returns:
            Comprehensive correlation analysis
        """
        print(f"Analyzing: {region_a} → {region_b}")

        # Get timeseries
        df_a = self.get_regional_timeseries(region_a, start_date, end_date)
        df_b = self.get_regional_timeseries(region_b, start_date, end_date)

        if df_a.empty or df_b.empty:
            print(f"  ✗ Insufficient data")
            return None

        # Merge on date to ensure alignment
        merged = pd.merge(df_a[['date', 'total_snowfall_mm']],
                         df_b[['date', 'total_snowfall_mm']],
                         on='date', suffixes=('_a', '_b'))

        if len(merged) < 100:
            print(f"  ✗ Insufficient overlapping data ({len(merged)} days)")
            return None

        # Calculate lag correlation
        lag_results = self.calculate_lag_correlation(
            merged['total_snowfall_mm_a'],
            merged['total_snowfall_mm_b'],
            max_lag_days
        )

        if not lag_results:
            print(f"  ✗ No valid correlations")
            return None

        # Calculate basic stats
        analysis = {
            'region_a': region_a,
            'region_b': region_b,
            'period': f"{merged['date'].min().strftime('%Y-%m-%d')} to {merged['date'].max().strftime('%Y-%m-%d')}",
            'sample_days': len(merged),
            **lag_results,
            'region_a_total_snow_mm': merged['total_snowfall_mm_a'].sum(),
            'region_b_total_snow_mm': merged['total_snowfall_mm_b'].sum(),
            'region_a_avg_daily_mm': merged['total_snowfall_mm_a'].mean(),
            'region_b_avg_daily_mm': merged['total_snowfall_mm_b'].mean(),
        }

        # Interpretation
        corr = analysis['best_correlation']
        lag = analysis['best_lag_days']
        sig = "✓" if analysis['significant'] else "✗"

        if abs(corr) > 0.3:
            strength = "STRONG"
        elif abs(corr) > 0.15:
            strength = "MODERATE"
        else:
            strength = "WEAK"

        if lag > 0:
            lag_text = f"{region_a} leads by {lag} days"
        elif lag < 0:
            lag_text = f"{region_b} leads by {abs(lag)} days"
        else:
            lag_text = "simultaneous"

        print(f"  {sig} {strength}: r={corr:.3f} (p={analysis['p_value']:.4f}) | {lag_text}")

        analysis['interpretation'] = {
            'strength': strength,
            'lag_text': lag_text,
            'significant': analysis['significant']
        }

        return analysis

    def analyze_all_regions_vs_target(self, target_region: str = "northern_wisconsin",
                                     max_lag_days: int = 30,
                                     start_date: str = "1940-01-01",
                                     end_date: str = None) -> List[Dict]:
        """
        Analyze all regions against target region (Phelps/Land O'Lakes area)

        Args:
            target_region: Target region to predict (default: northern_wisconsin)
            max_lag_days: Maximum lag to test
            start_date: Analysis start date
            end_date: Analysis end date

        Returns:
            List of correlation analyses sorted by strength
        """
        print(f"\n{'='*80}")
        print(f"GLOBAL CORRELATION ANALYSIS: All Regions → {target_region.upper()}")
        print(f"{'='*80}\n")

        # Get all unique regions
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT region FROM stations WHERE region != ?", (target_region,))
        regions = [row[0] for row in cursor.fetchall()]
        conn.close()

        results = []
        for region in sorted(regions):
            analysis = self.analyze_region_pair(region, target_region, max_lag_days, start_date, end_date)
            if analysis:
                results.append(analysis)

        # Sort by correlation strength (absolute value)
        results.sort(key=lambda x: abs(x['best_correlation']), reverse=True)

        print(f"\n{'='*80}")
        print(f"TOP CORRELATIONS (sorted by strength)")
        print(f"{'='*80}\n")

        for i, r in enumerate(results[:15], 1):
            corr = r['best_correlation']
            lag = r['best_lag_days']
            strength = r['interpretation']['strength']
            sig_mark = "***" if r['significant'] else ""

            print(f"{i:2d}. {r['region_a']:25s} | r={corr:+.3f} {sig_mark:3s} | lag={lag:+3d}d | {strength}")

        return results

    def analyze_extreme_events(self, region_a: str, region_b: str,
                              threshold_percentile: float = 90,
                              window_days: int = 14) -> Dict:
        """
        Analyze if extreme snow events in region_a predict events in region_b

        Args:
            region_a: Predictor region
            region_b: Target region
            threshold_percentile: Percentile for "extreme" (default: 90th = top 10%)
            window_days: Days before/after to check for co-occurrence

        Returns:
            Analysis of extreme event relationships
        """
        df_a = self.get_regional_timeseries(region_a)
        df_b = self.get_regional_timeseries(region_b)

        if df_a.empty or df_b.empty:
            return None

        # Define extreme events
        threshold_a = df_a['total_snowfall_mm'].quantile(threshold_percentile / 100)
        threshold_b = df_b['total_snowfall_mm'].quantile(threshold_percentile / 100)

        extreme_a = df_a[df_a['total_snowfall_mm'] >= threshold_a]
        extreme_b = df_b[df_b['total_snowfall_mm'] >= threshold_b]

        # Check co-occurrence within window
        matches = 0
        for date_a in extreme_a['date']:
            window_start = date_a - timedelta(days=window_days)
            window_end = date_a + timedelta(days=window_days)

            if any((extreme_b['date'] >= window_start) & (extreme_b['date'] <= window_end)):
                matches += 1

        match_rate = matches / len(extreme_a) if len(extreme_a) > 0 else 0

        return {
            'region_a': region_a,
            'region_b': region_b,
            'threshold_percentile': threshold_percentile,
            'threshold_a_mm': threshold_a,
            'threshold_b_mm': threshold_b,
            'extreme_events_a': len(extreme_a),
            'extreme_events_b': len(extreme_b),
            'matches_within_window': matches,
            'match_rate': match_rate,
            'window_days': window_days
        }

    def find_best_predictors(self, target_station: str = "Phelps_WI_USA",
                            min_correlation: float = 0.15,
                            max_lag_days: int = 30) -> List[Dict]:
        """
        Find best global predictors for a specific target station

        Args:
            target_station: Station ID to predict (default: Phelps, WI)
            min_correlation: Minimum correlation threshold
            max_lag_days: Maximum lag to test

        Returns:
            List of best predictor regions/stations
        """
        # Get all regions
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT region FROM stations")
        regions = [row[0] for row in cursor.fetchall()]
        conn.close()

        target_data = self.get_station_timeseries(target_station)
        if target_data.empty:
            print(f"✗ No data for target station: {target_station}")
            return []

        results = []
        for region in regions:
            region_data = self.get_regional_timeseries(region)
            if region_data.empty:
                continue

            # Merge datasets
            merged = pd.merge(
                region_data[['date', 'total_snowfall_mm']],
                target_data[['date', 'snowfall_mm']],
                on='date'
            )

            if len(merged) < 100:
                continue

            # Calculate correlation
            lag_results = self.calculate_lag_correlation(
                merged['total_snowfall_mm'],
                merged['snowfall_mm'],
                max_lag_days
            )

            if lag_results and abs(lag_results['best_correlation']) >= min_correlation:
                results.append({
                    'predictor_region': region,
                    'target_station': target_station,
                    **lag_results
                })

        results.sort(key=lambda x: abs(x['best_correlation']), reverse=True)
        return results

    def generate_prediction_model(self, target_region: str = "northern_wisconsin",
                                 top_n_predictors: int = 10) -> Dict:
        """
        Generate a multi-region prediction model for target region

        Args:
            target_region: Region to predict
            top_n_predictors: Number of top predictors to include

        Returns:
            Prediction model configuration
        """
        # Analyze all regions
        all_correlations = self.analyze_all_regions_vs_target(target_region)

        # Get top predictors
        top_predictors = all_correlations[:top_n_predictors]

        model = {
            'target_region': target_region,
            'created_at': datetime.now().isoformat(),
            'num_predictors': len(top_predictors),
            'predictors': []
        }

        for pred in top_predictors:
            model['predictors'].append({
                'region': pred['region_a'],
                'correlation': pred['best_correlation'],
                'lag_days': pred['best_lag_days'],
                'p_value': pred['p_value'],
                'significant': pred['significant'],
                'weight': abs(pred['best_correlation'])  # For weighted ensemble
            })

        # Normalize weights
        total_weight = sum(p['weight'] for p in model['predictors'])
        for p in model['predictors']:
            p['normalized_weight'] = p['weight'] / total_weight

        return model

    def export_correlations_to_db(self, results: List[Dict]):
        """Export correlation results to database for caching"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for r in results:
            cursor.execute("""
                INSERT OR REPLACE INTO correlations
                (region_a, region_b, correlation_coefficient, lag_days, sample_size, significance_p_value)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                r['region_a'],
                r['region_b'],
                r['best_correlation'],
                r['best_lag_days'],
                r['sample_size'],
                r['p_value']
            ))

        conn.commit()
        conn.close()
        print(f"✓ Exported {len(results)} correlations to database")


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(description='Global Snowfall Correlation Analysis')
    parser.add_argument('--db', default='global_snowfall.db', help='Database file')
    parser.add_argument('--target', default='northern_wisconsin', help='Target region to analyze')
    parser.add_argument('--max-lag', type=int, default=30, help='Maximum lag days to test')
    parser.add_argument('--start', default='1940-01-01', help='Start date')
    parser.add_argument('--end', default=None, help='End date')
    parser.add_argument('--export', action='store_true', help='Export results to database')
    parser.add_argument('--model', action='store_true', help='Generate prediction model')
    parser.add_argument('--model-file', default='phelps_prediction_model.json', help='Model output file')

    args = parser.parse_args()

    analyzer = GlobalCorrelationAnalyzer(db_path=args.db)

    # Run full analysis
    results = analyzer.analyze_all_regions_vs_target(
        target_region=args.target,
        max_lag_days=args.max_lag,
        start_date=args.start,
        end_date=args.end
    )

    if args.export and results:
        analyzer.export_correlations_to_db(results)

    if args.model and results:
        print(f"\n{'='*80}")
        print(f"GENERATING PREDICTION MODEL")
        print(f"{'='*80}\n")

        model = analyzer.generate_prediction_model(target_region=args.target)

        with open(args.model_file, 'w') as f:
            json.dump(model, f, indent=2)

        print(f"✓ Prediction model saved to: {args.model_file}")
        print(f"\nTop 5 Predictors:")
        for i, pred in enumerate(model['predictors'][:5], 1):
            print(f"  {i}. {pred['region']:25s} | r={pred['correlation']:+.3f} | lag={pred['lag_days']:+3d}d | weight={pred['normalized_weight']:.3f}")


if __name__ == '__main__':
    main()
