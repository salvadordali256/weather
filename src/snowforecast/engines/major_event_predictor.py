#!/usr/bin/env python3
"""
Major Snow Event Predictor
Analyzes what global conditions preceded historical major Wisconsin snow events
Then checks if those conditions exist now or are forecast

Approach:
1. Find all major Wisconsin snow events (>50mm) in history
2. For each event, analyze global conditions 1-7 days BEFORE
3. Identify common precursor patterns
4. Check if current/recent conditions match those patterns
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class MajorEventPredictor:
    """Predict major snow events by pattern matching precursor conditions"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # Stations to check for precursor signals
        self.precursor_stations = {
            'thunder_bay_on': {'name': 'Thunder Bay', 'check_lags': [0, 1]},
            'sapporo_japan': {'name': 'Sapporo', 'check_lags': [5, 6, 7]},
            'chamonix_france': {'name': 'Chamonix', 'check_lags': [4, 5, 6]},
            'irkutsk_russia': {'name': 'Irkutsk', 'check_lags': [6, 7, 8]},
            'niigata_japan': {'name': 'Niigata', 'check_lags': [2, 3, 4]},
            'steamboat_springs_co': {'name': 'Steamboat', 'check_lags': [0, 1, 2]},
            'mount_baker_wa': {'name': 'Mt Baker', 'check_lags': [0, 1, 2]},
            'lake_tahoe_ca': {'name': 'Lake Tahoe', 'check_lags': [0, 1, 2]},
        }

    def find_major_events(self, threshold_mm=50.0, start_year=1995):
        """Find all major Wisconsin snow events"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                date,
                snowfall_mm,
                station_id
            FROM snowfall_daily
            WHERE station_id IN ('phelps_wi', 'land_o_lakes_wi', 'eagle_river_wi')
              AND snowfall_mm >= ?
              AND date >= ?
            ORDER BY snowfall_mm DESC, date DESC
        """

        df = pd.read_sql_query(query, conn, params=(
            threshold_mm,
            f'{start_year}-01-01'
        ))

        conn.close()

        print(f"\n{'='*80}")
        print(f"HISTORICAL MAJOR SNOW EVENTS (≥{threshold_mm}mm)")
        print(f"{'='*80}\n")

        if df.empty:
            print(f"No events found ≥{threshold_mm}mm since {start_year}")
            return df

        print(f"Found {len(df)} major events:\n")
        for idx, row in df.head(20).iterrows():
            print(f"  {row['date']:12s} | {row['station_id']:20s} | {row['snowfall_mm']:6.1f}mm")

        if len(df) > 20:
            print(f"\n  ... and {len(df)-20} more events")

        return df

    def analyze_precursors(self, event_date, event_snow):
        """
        For a given event, analyze what global conditions existed 1-7 days before

        Returns: Dictionary of precursor conditions
        """
        conn = sqlite3.connect(self.db_path)

        event_dt = pd.to_datetime(event_date)
        precursors = {}

        for station_id, config in self.precursor_stations.items():
            station_precursors = []

            for lag in config['check_lags']:
                check_date = event_dt - timedelta(days=lag)

                # Get 3-day window around check date
                start_date = check_date - timedelta(days=1)
                end_date = check_date + timedelta(days=1)

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
                    avg_snow = df.iloc[0]['avg_snow']
                    max_snow = df.iloc[0]['max_snow']

                    station_precursors.append({
                        'lag': lag,
                        'date': check_date,
                        'avg_snow': avg_snow,
                        'max_snow': max_snow
                    })

            if station_precursors:
                # Find the maximum snow in the check window
                max_entry = max(station_precursors, key=lambda x: x['max_snow'])
                precursors[station_id] = {
                    'name': config['name'],
                    'best_lag': max_entry['lag'],
                    'avg_snow': max_entry['avg_snow'],
                    'max_snow': max_entry['max_snow'],
                    'all_lags': station_precursors
                }

        conn.close()
        return precursors

    def find_common_precursor_patterns(self, events_df):
        """Analyze all major events to find common precursor patterns"""

        print(f"\n{'='*80}")
        print(f"ANALYZING PRECURSOR CONDITIONS FOR {len(events_df)} MAJOR EVENTS")
        print(f"{'='*80}\n")

        all_precursors = []

        for idx, event in events_df.iterrows():
            precursors = self.analyze_precursors(event['date'], event['snowfall_mm'])

            all_precursors.append({
                'event_date': event['date'],
                'event_snow': event['snowfall_mm'],
                'precursors': precursors
            })

        # Aggregate statistics
        print(f"PRECURSOR PATTERN ANALYSIS:")
        print(f"{'─'*80}\n")

        station_stats = {}

        for station_id, config in self.precursor_stations.items():
            station_name = config['name']
            snow_values = []
            active_count = 0  # Heavy snow (>20mm) before events

            for event_data in all_precursors:
                if station_id in event_data['precursors']:
                    max_snow = event_data['precursors'][station_id]['max_snow']
                    snow_values.append(max_snow)
                    if max_snow >= 20.0:
                        active_count += 1

            if snow_values:
                station_stats[station_id] = {
                    'name': station_name,
                    'avg_precursor_snow': np.mean(snow_values),
                    'median_precursor_snow': np.median(snow_values),
                    'max_precursor_snow': np.max(snow_values),
                    'active_signal_rate': (active_count / len(snow_values)) * 100,
                    'sample_size': len(snow_values)
                }

        # Print results
        print(f"{'Station':<20s} | {'Avg Snow':<10s} | {'Median':<10s} | {'Max':<10s} | {'Active %':<10s}")
        print(f"{'─'*80}")

        for station_id in sorted(station_stats.keys(),
                                 key=lambda x: station_stats[x]['avg_precursor_snow'],
                                 reverse=True):
            stats = station_stats[station_id]
            print(f"{stats['name']:<20s} | {stats['avg_precursor_snow']:8.1f}mm | "
                  f"{stats['median_precursor_snow']:8.1f}mm | {stats['max_precursor_snow']:8.1f}mm | "
                  f"{stats['active_signal_rate']:7.1f}%")

        return all_precursors, station_stats

    def check_current_for_precursor_match(self, station_stats, threshold_percentile=75):
        """
        Check if current conditions match historical precursor patterns
        """
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*80}")
        print(f"CHECKING CURRENT CONDITIONS FOR MAJOR EVENT SIGNALS")
        print(f"{'='*80}\n")

        current_time = datetime.now()
        match_score = 0.0
        max_score = 0.0
        active_signals = []

        for station_id, stats in station_stats.items():
            station_name = stats['name']

            # Calculate threshold for "active" signal
            # Use 75th percentile of historical precursor snow
            threshold = np.percentile([stats['median_precursor_snow'], stats['avg_precursor_snow']],
                                     threshold_percentile)

            # Check each lag that was historically relevant
            config = self.precursor_stations[station_id]
            max_recent_snow = 0.0
            best_lag = None

            for lag in config['check_lags']:
                check_date = current_time - timedelta(days=lag)

                query = """
                    SELECT AVG(snowfall_mm) as avg_snow, MAX(snowfall_mm) as max_snow
                    FROM snowfall_daily
                    WHERE station_id = ?
                      AND date >= ?
                      AND date <= ?
                """

                df = pd.read_sql_query(query, conn, params=(
                    station_id,
                    (check_date - timedelta(days=1)).strftime('%Y-%m-%d'),
                    (check_date + timedelta(days=1)).strftime('%Y-%m-%d')
                ))

                if not df.empty and pd.notna(df.iloc[0]['max_snow']):
                    if df.iloc[0]['max_snow'] > max_recent_snow:
                        max_recent_snow = df.iloc[0]['max_snow']
                        best_lag = lag

            # Score this station
            weight = stats['active_signal_rate'] / 100.0  # Weight by historical success rate
            max_score += weight

            if max_recent_snow >= threshold:
                contribution = weight
                match_score += contribution

                active_signals.append({
                    'station': station_name,
                    'recent_snow': max_recent_snow,
                    'threshold': threshold,
                    'lag': best_lag,
                    'contribution': contribution
                })

                status = "🔴 ACTIVE"
            elif max_recent_snow >= stats['median_precursor_snow']:
                contribution = weight * 0.5
                match_score += contribution
                status = "🟡 ELEVATED"
            else:
                status = "⚪ QUIET"
                contribution = 0.0

            print(f"{status} {station_name:<20s} | Recent: {max_recent_snow:6.1f}mm | "
                  f"Threshold: {threshold:6.1f}mm | +{contribution:.2f}")

        conn.close()

        # Calculate probability
        if max_score > 0:
            probability_pct = (match_score / max_score) * 100
        else:
            probability_pct = 0.0

        print(f"\n{'─'*80}")
        print(f"MAJOR EVENT PROBABILITY ASSESSMENT:")
        print(f"{'─'*80}\n")

        print(f"Match Score: {match_score:.2f} / {max_score:.2f} ({probability_pct:.1f}%)")
        print(f"Active Signals: {len(active_signals)}/{len(station_stats)}")

        if probability_pct >= 70:
            risk_level = "🔴 HIGH RISK - Major event likely within 0-3 days"
        elif probability_pct >= 50:
            risk_level = "🟡 ELEVATED RISK - Major event possible within 0-5 days"
        elif probability_pct >= 30:
            risk_level = "🟢 MODERATE RISK - Increased probability, monitor closely"
        elif probability_pct >= 15:
            risk_level = "⚪ LOW RISK - Background probability"
        else:
            risk_level = "⚪ MINIMAL RISK - Conditions not favorable"

        print(f"\nRisk Level: {risk_level}")

        if active_signals:
            print(f"\nActive Precursor Signals:")
            for sig in sorted(active_signals, key=lambda x: x['contribution'], reverse=True):
                print(f"  • {sig['station']:<20s} {sig['recent_snow']:6.1f}mm "
                      f"({sig['lag']}d ago) [+{sig['contribution']:.2f}]")

        return {
            'probability': probability_pct,
            'risk_level': risk_level,
            'match_score': match_score,
            'max_score': max_score,
            'active_signals': active_signals
        }

    def run_analysis(self, threshold_mm=50.0):
        """Main analysis pipeline"""

        print(f"\n{'='*80}")
        print(f"MAJOR SNOW EVENT PREDICTION SYSTEM")
        print(f"Target: Northern Wisconsin (Phelps, Land O'Lakes, Eagle River)")
        print(f"{'='*80}")
        print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Event Threshold: ≥{threshold_mm}mm daily snowfall")
        print(f"{'='*80}")

        # Step 1: Find historical major events
        events_df = self.find_major_events(threshold_mm=threshold_mm)

        if events_df.empty:
            print("\nNo historical events found. Try lowering threshold.")
            return

        # Step 2: Analyze precursor patterns
        all_precursors, station_stats = self.find_common_precursor_patterns(events_df)

        # Step 3: Check current conditions
        current_risk = self.check_current_for_precursor_match(station_stats)

        # Step 4: Show examples
        print(f"\n{'='*80}")
        print(f"EXAMPLE: CONDITIONS BEFORE PAST MAJOR EVENTS")
        print(f"{'='*80}\n")

        for i, event_data in enumerate(all_precursors[:5], 1):
            print(f"{i}. {event_data['event_date']} - {event_data['event_snow']:.1f}mm event")
            print(f"   Precursor conditions:")

            for station_id, precursor in sorted(event_data['precursors'].items(),
                                               key=lambda x: x[1]['max_snow'],
                                               reverse=True)[:5]:
                print(f"     • {precursor['name']:<20s} {precursor['max_snow']:6.1f}mm "
                      f"({precursor['best_lag']}d before)")
            print()

        print(f"{'='*80}")
        print(f"SUMMARY")
        print(f"{'='*80}\n")

        print(f"Analyzed: {len(events_df)} historical major events (≥{threshold_mm}mm)")
        print(f"Current Risk: {current_risk['risk_level']}")
        print(f"Probability: {current_risk['probability']:.1f}%")

        if current_risk['probability'] >= 50:
            print(f"\n⚠️  WARNING: Elevated risk detected. Monitor NWS forecasts closely.")

        print(f"\n{'='*80}\n")

        return {
            'events': events_df,
            'precursors': all_precursors,
            'station_stats': station_stats,
            'current_risk': current_risk
        }


def main():
    predictor = MajorEventPredictor()

    # Run analysis for different thresholds
    print("\n" + "="*80)
    print("RUNNING ANALYSIS FOR MAJOR EVENTS (≥50mm)")
    print("="*80)
    result_50 = predictor.run_analysis(threshold_mm=50.0)

    print("\n\n" + "="*80)
    print("RUNNING ANALYSIS FOR EXTREME EVENTS (≥100mm)")
    print("="*80)
    result_100 = predictor.run_analysis(threshold_mm=100.0)


if __name__ == '__main__':
    main()
