#!/usr/bin/env python3
"""
Historical Pattern Matching Forecast System
Uses past similar conditions to predict Wisconsin snowfall

Approach:
1. Identify current global conditions (Thunder Bay, Sapporo, Chamonix, etc.)
2. Search historical data for similar condition patterns
3. See what happened in Wisconsin when those patterns occurred
4. Use historical outcomes to generate probabilistic forecast
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class PatternMatchingForecast:
    """Generate forecasts by matching current conditions to historical patterns"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # Predictors to use for pattern matching
        self.predictors = {
            'thunder_bay_on': {'lag': 0, 'weight': 1.0},
            'sapporo_japan': {'lag': 6, 'weight': 0.8},
            'chamonix_france': {'lag': 5, 'weight': 0.6},
            'irkutsk_russia': {'lag': 7, 'weight': 0.5},
        }

        # Snow thresholds (mm)
        self.thresholds = {
            'heavy': 25.0,
            'moderate': 15.0,
            'light': 5.0,
            'trace': 1.0
        }

    def get_current_conditions(self):
        """Get current snowfall conditions at all predictor stations"""
        conn = sqlite3.connect(self.db_path)

        conditions = {}

        for station_id, config in self.predictors.items():
            lag = config['lag']
            target_date = datetime.now() - timedelta(days=lag)

            # Get 3-day average centered on target date
            start_date = target_date - timedelta(days=1)
            end_date = target_date + timedelta(days=1)

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

            if not df.empty and pd.notna(df.iloc[0]['avg_snow']):
                conditions[station_id] = {
                    'avg': df.iloc[0]['avg_snow'],
                    'max': df.iloc[0]['max_snow'],
                    'category': self._categorize_snow(df.iloc[0]['avg_snow'])
                }
            else:
                conditions[station_id] = {
                    'avg': 0.0,
                    'max': 0.0,
                    'category': 'none'
                }

        conn.close()
        return conditions

    def _categorize_snow(self, amount):
        """Categorize snowfall amount"""
        if amount >= self.thresholds['heavy']:
            return 'heavy'
        elif amount >= self.thresholds['moderate']:
            return 'moderate'
        elif amount >= self.thresholds['light']:
            return 'light'
        elif amount >= self.thresholds['trace']:
            return 'trace'
        else:
            return 'none'

    def find_historical_analogs(self, current_conditions, similarity_threshold=0.7):
        """
        Find historical dates with similar global conditions

        Returns: List of dates where conditions were similar, with similarity scores
        """
        conn = sqlite3.connect(self.db_path)

        # Get all historical dates (from 1995 onward for global data, winter months only)
        query = """
            SELECT DISTINCT date
            FROM snowfall_daily
            WHERE date >= '1995-01-01'
              AND date < ?
              AND (CAST(strftime('%m', date) AS INTEGER) IN (1, 2, 3, 11, 12))
            ORDER BY date
        """

        today = datetime.now().strftime('%Y-%m-%d')
        all_dates = pd.read_sql_query(query, conn, params=(today,))

        analogs = []

        print(f"\n🔍 Searching {len(all_dates)} historical winter days for similar patterns...")
        print(f"Current conditions:")
        for station, cond in current_conditions.items():
            print(f"  {station}: {cond['category']} ({cond['avg']:.1f}mm avg)")
        print()

        # For each historical date, calculate similarity
        for idx, row in all_dates.iterrows():
            hist_date = pd.to_datetime(row['date'])
            similarity_score = 0.0
            total_weight = 0.0

            hist_conditions = {}

            # Get conditions at each predictor station on this historical date
            for station_id, config in self.predictors.items():
                lag = config['lag']
                weight = config['weight']

                # Get historical snow at this station
                check_date = hist_date - timedelta(days=lag)

                hist_query = """
                    SELECT AVG(snowfall_mm) as avg_snow
                    FROM snowfall_daily
                    WHERE station_id = ?
                      AND date >= ?
                      AND date <= ?
                """

                hist_df = pd.read_sql_query(hist_query, conn, params=(
                    station_id,
                    (check_date - timedelta(days=1)).strftime('%Y-%m-%d'),
                    (check_date + timedelta(days=1)).strftime('%Y-%m-%d')
                ))

                if not hist_df.empty and pd.notna(hist_df.iloc[0]['avg_snow']):
                    hist_avg = hist_df.iloc[0]['avg_snow']
                    hist_category = self._categorize_snow(hist_avg)

                    hist_conditions[station_id] = {
                        'avg': hist_avg,
                        'category': hist_category
                    }

                    # Calculate similarity for this station
                    if hist_category == current_conditions[station_id]['category']:
                        # Exact category match
                        station_similarity = 1.0
                    elif abs(hist_avg - current_conditions[station_id]['avg']) < 5.0:
                        # Close match (within 5mm)
                        station_similarity = 0.8
                    elif abs(hist_avg - current_conditions[station_id]['avg']) < 10.0:
                        # Moderate match (within 10mm)
                        station_similarity = 0.5
                    else:
                        # Poor match
                        station_similarity = 0.0

                    similarity_score += station_similarity * weight
                    total_weight += weight

            # Normalize similarity score
            if total_weight > 0:
                normalized_similarity = similarity_score / total_weight

                if normalized_similarity >= similarity_threshold:
                    analogs.append({
                        'date': hist_date,
                        'similarity': normalized_similarity,
                        'conditions': hist_conditions
                    })

        conn.close()

        # Sort by similarity (highest first)
        analogs = sorted(analogs, key=lambda x: x['similarity'], reverse=True)

        print(f"✅ Found {len(analogs)} analog dates with similarity >= {similarity_threshold:.0%}")

        return analogs

    def get_wisconsin_outcomes(self, analog_dates):
        """
        For each analog date, get what happened in Wisconsin
        in the following 7 days
        """
        conn = sqlite3.connect(self.db_path)

        outcomes = []

        for analog in analog_dates[:50]:  # Limit to top 50 analogs
            date = analog['date']

            # Get Wisconsin snow in next 7 days
            query = """
                SELECT
                    date,
                    snowfall_mm
                FROM snowfall_daily
                WHERE station_id IN ('phelps_wi', 'land_o_lakes_wi')
                  AND date >= ?
                  AND date < ?
                ORDER BY date
            """

            df = pd.read_sql_query(query, conn, params=(
                date.strftime('%Y-%m-%d'),
                (date + timedelta(days=7)).strftime('%Y-%m-%d')
            ))

            if not df.empty:
                total_snow = df['snowfall_mm'].sum()
                max_daily = df['snowfall_mm'].max()
                avg_daily = df['snowfall_mm'].mean()

                outcomes.append({
                    'analog_date': date,
                    'similarity': analog['similarity'],
                    'total_7day': total_snow,
                    'max_daily': max_daily,
                    'avg_daily': avg_daily,
                    'conditions': analog['conditions']
                })

        conn.close()
        return outcomes

    def generate_probabilistic_forecast(self, outcomes):
        """Generate forecast based on historical outcomes"""

        if len(outcomes) == 0:
            return {
                'forecast': 'INSUFFICIENT DATA',
                'confidence': 'NONE',
                'probability': '0%',
                'expected_7day': 0.0
            }

        # Weight outcomes by similarity
        weighted_totals = []
        weights = []

        for outcome in outcomes:
            weighted_totals.append(outcome['total_7day'])
            weights.append(outcome['similarity'])

        # Calculate statistics
        weighted_avg = np.average(weighted_totals, weights=weights)
        median_total = np.median(weighted_totals)
        percentile_75 = np.percentile(weighted_totals, 75)
        percentile_90 = np.percentile(weighted_totals, 90)

        # Probability of significant snow (>20mm in 7 days)
        significant_count = sum(1 for t in weighted_totals if t > 20.0)
        probability_significant = (significant_count / len(weighted_totals)) * 100

        # Probability of major snow (>50mm in 7 days)
        major_count = sum(1 for t in weighted_totals if t > 50.0)
        probability_major = (major_count / len(weighted_totals)) * 100

        # Determine forecast
        if weighted_avg >= 50.0:
            forecast = "MAJOR SNOW LIKELY"
            confidence = "HIGH"
        elif weighted_avg >= 20.0:
            forecast = "SIGNIFICANT SNOW PROBABLE"
            confidence = "MODERATE-HIGH"
        elif weighted_avg >= 10.0:
            forecast = "MODERATE SNOW POSSIBLE"
            confidence = "MODERATE"
        elif weighted_avg >= 5.0:
            forecast = "LIGHT SNOW EXPECTED"
            confidence = "MODERATE"
        else:
            forecast = "QUIET PATTERN"
            confidence = "MODERATE"

        return {
            'forecast': forecast,
            'confidence': confidence,
            'analog_count': len(outcomes),
            'expected_7day': weighted_avg,
            'median_7day': median_total,
            'percentile_75': percentile_75,
            'percentile_90': percentile_90,
            'prob_significant': probability_significant,
            'prob_major': probability_major,
            'distribution': weighted_totals
        }

    def run_forecast(self):
        """Main forecast generation"""

        print(f"\n{'='*80}")
        print(f"HISTORICAL PATTERN MATCHING FORECAST")
        print(f"Target: Phelps & Land O'Lakes, Wisconsin")
        print(f"{'='*80}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Method: Analog pattern matching (1995-present)")
        print(f"{'='*80}\n")

        # Step 1: Get current conditions
        print("STEP 1: Analyzing current global conditions...")
        current = self.get_current_conditions()

        # Step 2: Find historical analogs
        print("\nSTEP 2: Finding historical analogs...")
        analogs = self.find_historical_analogs(current, similarity_threshold=0.6)

        if len(analogs) == 0:
            print("\n❌ No similar historical patterns found. Try lowering threshold.")
            return

        # Show top analogs
        print(f"\nTop 10 Most Similar Historical Patterns:")
        print(f"{'─'*80}")
        for i, analog in enumerate(analogs[:10], 1):
            date_str = analog['date'].strftime('%Y-%m-%d')
            sim_pct = analog['similarity'] * 100
            print(f"{i:2d}. {date_str} | Similarity: {sim_pct:5.1f}%")

        # Step 3: Get outcomes
        print(f"\n{'─'*80}")
        print(f"\nSTEP 3: Analyzing Wisconsin outcomes from {len(analogs[:50])} analogs...")
        outcomes = self.get_wisconsin_outcomes(analogs)

        # Show sample outcomes
        print(f"\nSample Historical Outcomes (Wisconsin 7-day totals):")
        print(f"{'─'*80}")
        for i, outcome in enumerate(outcomes[:10], 1):
            date_str = outcome['analog_date'].strftime('%Y-%m-%d')
            sim_pct = outcome['similarity'] * 100
            snow = outcome['total_7day']
            max_d = outcome['max_daily']
            print(f"{i:2d}. {date_str} | Sim: {sim_pct:5.1f}% | 7-day: {snow:6.1f}mm | Max daily: {max_d:6.1f}mm")

        # Step 4: Generate forecast
        print(f"\n{'='*80}")
        print(f"STEP 4: Probabilistic Forecast")
        print(f"{'='*80}\n")

        forecast_result = self.generate_probabilistic_forecast(outcomes)

        print(f"Forecast: {forecast_result['forecast']}")
        print(f"Confidence: {forecast_result['confidence']}")
        print(f"Based on: {forecast_result['analog_count']} historical analogs\n")

        print(f"Expected 7-Day Snowfall:")
        print(f"  Weighted Average: {forecast_result['expected_7day']:.1f}mm")
        print(f"  Median:           {forecast_result['median_7day']:.1f}mm")
        print(f"  75th Percentile:  {forecast_result['percentile_75']:.1f}mm")
        print(f"  90th Percentile:  {forecast_result['percentile_90']:.1f}mm")

        print(f"\nProbabilities:")
        print(f"  Significant Snow (>20mm): {forecast_result['prob_significant']:.1f}%")
        print(f"  Major Snow (>50mm):       {forecast_result['prob_major']:.1f}%")

        # Distribution
        print(f"\n{'─'*80}")
        print(f"Historical Distribution (7-day totals from analogs):")
        print(f"{'─'*80}")

        dist = forecast_result['distribution']
        ranges = [
            (0, 5, "Trace/None"),
            (5, 20, "Light"),
            (20, 50, "Significant"),
            (50, 100, "Major"),
            (100, 1000, "Extreme")
        ]

        for min_val, max_val, label in ranges:
            count = sum(1 for d in dist if min_val <= d < max_val)
            pct = (count / len(dist)) * 100 if len(dist) > 0 else 0
            bar = "█" * int(pct / 5)
            print(f"  {label:12s} ({min_val:3d}-{max_val:3d}mm): {count:3d} ({pct:5.1f}%) {bar}")

        print(f"\n{'='*80}")
        print(f"INTERPRETATION:")
        print(f"{'='*80}\n")
        print(f"This forecast uses {forecast_result['analog_count']} historical periods")
        print(f"with similar global snowfall patterns to current conditions.")
        print(f"When these patterns occurred in the past, Wisconsin received")
        print(f"an average of {forecast_result['expected_7day']:.1f}mm over the following 7 days.\n")
        print(f"{'='*80}\n")

        return forecast_result


def main():
    forecaster = PatternMatchingForecast()
    result = forecaster.run_forecast()


if __name__ == '__main__':
    main()
