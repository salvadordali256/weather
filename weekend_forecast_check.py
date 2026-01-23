#!/usr/bin/env python3
"""
Weekend Snow Potential Analyzer
Checks if any precursor signals are building for the upcoming weekend

Current: Tuesday, January 6, 2026
Target Weekend: Saturday Jan 11 - Sunday Jan 12 (5-6 days out)

Analysis approach:
1. Check stations with 5-7 day lead times (should be showing activity NOW)
2. Check stations with 3-4 day lead times (should show activity Wed-Thu)
3. Look for building pattern trends
4. Assess weekend snow potential
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class WeekendForecastChecker:
    """Analyze weekend snow potential based on current precursor signals"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # Today is Tuesday, weekend is Sat-Sun (5-6 days out)
        self.today = datetime.now()
        self.weekend_start = self.today + timedelta(days=5)  # Saturday
        self.weekend_end = self.today + timedelta(days=6)    # Sunday

    def get_station_snow(self, station_id, days_ago, window=3):
        """Get snow for a station N days ago"""
        conn = sqlite3.connect(self.db_path)

        target_date = datetime.now() - timedelta(days=days_ago)
        start_date = target_date - timedelta(days=window-1)

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
            target_date.strftime('%Y-%m-%d')
        ))

        conn.close()

        if df.empty or pd.isna(df.iloc[0]['avg_snow']):
            return 0.0, 0.0

        return df.iloc[0]['avg_snow'], df.iloc[0]['max_snow']

    def check_weekend_precursors(self):
        """Check if precursor signals are building for weekend"""

        print(f"\n{'='*80}")
        print(f"WEEKEND SNOW POTENTIAL ANALYSIS")
        print(f"{'='*80}")
        print(f"Current Date: {self.today.strftime('%A, %B %d, %Y')}")
        print(f"Target Weekend: {self.weekend_start.strftime('%A, %B %d')} - {self.weekend_end.strftime('%A, %B %d')}")
        print(f"Days Out: 5-6 days")
        print(f"{'='*80}\n")

        # Define which stations should be active NOW for weekend snow
        weekend_predictors = {
            # For Saturday (5 days out), check these stations TODAY
            'saturday': [
                {'station': 'chamonix_france', 'lag': 5, 'name': 'Chamonix', 'check_days_ago': 0},
                {'station': 'zermatt_switzerland', 'lag': 5, 'name': 'Zermatt', 'check_days_ago': 0},
                {'station': 'sapporo_japan', 'lag': 6, 'name': 'Sapporo', 'check_days_ago': 1},  # Check yesterday for 6-day lead
                {'station': 'irkutsk_russia', 'lag': 7, 'name': 'Irkutsk', 'check_days_ago': 2},  # Check 2 days ago for 7-day lead
            ],

            # For Sunday (6 days out), also check
            'sunday': [
                {'station': 'sapporo_japan', 'lag': 6, 'name': 'Sapporo', 'check_days_ago': 0},  # Check today for Sunday
                {'station': 'irkutsk_russia', 'lag': 7, 'name': 'Irkutsk', 'check_days_ago': 1},  # Check yesterday
            ]
        }

        # Check Saturday signals
        print(f"SATURDAY ({self.weekend_start.strftime('%B %d')}) PRECURSOR CHECK:")
        print(f"{'─'*80}\n")
        print(f"Looking for activity at lead-time stations (5-7 days ahead):\n")

        saturday_score = 0.0
        saturday_signals = []

        for predictor in weekend_predictors['saturday']:
            avg, max_snow = self.get_station_snow(
                predictor['station'],
                predictor['check_days_ago']
            )

            check_date = self.today - timedelta(days=predictor['check_days_ago'])

            # Score the signal
            if max_snow >= 25:
                score = 1.0
                level = "🔴 HEAVY"
                saturday_signals.append({
                    'name': predictor['name'],
                    'snow': max_snow,
                    'level': 'HEAVY',
                    'lag': predictor['lag']
                })
            elif max_snow >= 15:
                score = 0.6
                level = "🟡 MODERATE"
                saturday_signals.append({
                    'name': predictor['name'],
                    'snow': max_snow,
                    'level': 'MODERATE',
                    'lag': predictor['lag']
                })
            elif max_snow >= 5:
                score = 0.3
                level = "🟢 LIGHT"
            else:
                score = 0.0
                level = "⚪ QUIET"

            saturday_score += score

            print(f"{level} {predictor['name']:<20s} | {check_date.strftime('%b %d')}: {max_snow:6.1f}mm | "
                  f"{predictor['lag']}d lead → Saturday")

        # Check Sunday signals
        print(f"\n{'─'*80}")
        print(f"SUNDAY ({self.weekend_end.strftime('%B %d')}) PRECURSOR CHECK:")
        print(f"{'─'*80}\n")

        sunday_score = 0.0
        sunday_signals = []

        for predictor in weekend_predictors['sunday']:
            avg, max_snow = self.get_station_snow(
                predictor['station'],
                predictor['check_days_ago']
            )

            check_date = self.today - timedelta(days=predictor['check_days_ago'])

            if max_snow >= 25:
                score = 1.0
                level = "🔴 HEAVY"
                sunday_signals.append({
                    'name': predictor['name'],
                    'snow': max_snow,
                    'level': 'HEAVY',
                    'lag': predictor['lag']
                })
            elif max_snow >= 15:
                score = 0.6
                level = "🟡 MODERATE"
                sunday_signals.append({
                    'name': predictor['name'],
                    'snow': max_snow,
                    'level': 'MODERATE',
                    'lag': predictor['lag']
                })
            elif max_snow >= 5:
                score = 0.3
                level = "🟢 LIGHT"
            else:
                score = 0.0
                level = "⚪ QUIET"

            sunday_score += score

            print(f"{level} {predictor['name']:<20s} | {check_date.strftime('%b %d')}: {max_snow:6.1f}mm | "
                  f"{predictor['lag']}d lead → Sunday")

        # Also check shorter-lead stations (3-4 days) - these would show Wed-Thu
        print(f"\n{'─'*80}")
        print(f"MID-RANGE SIGNALS (3-4 day lead - watch Wed-Thu for updates):")
        print(f"{'─'*80}\n")

        mid_range = [
            {'station': 'niigata_japan', 'lag': 3, 'name': 'Niigata', 'check': 'Thursday'},
            {'station': 'steamboat_springs_co', 'lag': 1, 'name': 'Steamboat', 'check': 'Friday'},
            {'station': 'mount_baker_wa', 'lag': 1, 'name': 'Mt Baker', 'check': 'Friday'},
        ]

        for predictor in mid_range:
            print(f"⏰ {predictor['name']:<20s} | Check {predictor['check']} for {predictor['lag']}d lead signal")

        # Pacific moisture (doesn't predict timing, but shows moisture available)
        print(f"\n{'─'*80}")
        print(f"PACIFIC MOISTURE CHECK (availability, not delivery):")
        print(f"{'─'*80}\n")

        pacific_stations = [
            ('mount_baker_wa', 'Mt Baker'),
            ('lake_tahoe_ca', 'Lake Tahoe')
        ]

        pacific_score = 0.0

        for station_id, name in pacific_stations:
            avg, max_snow = self.get_station_snow(station_id, 0)

            if max_snow >= 20:
                level = "🔴 HEAVY"
                pacific_score += 0.5
            elif max_snow >= 10:
                level = "🟡 MODERATE"
                pacific_score += 0.3
            elif max_snow >= 5:
                level = "🟢 LIGHT"
            else:
                level = "⚪ QUIET"

            print(f"{level} {name:<20s} | Recent: {max_snow:6.1f}mm")

        # Overall weekend assessment
        print(f"\n{'='*80}")
        print(f"WEEKEND SNOW POTENTIAL SUMMARY")
        print(f"{'='*80}\n")

        # Saturday
        saturday_prob = min(saturday_score * 25, 100)  # Convert to percentage
        if saturday_prob >= 70:
            sat_forecast = "🔴 HIGH - Major snow likely"
        elif saturday_prob >= 50:
            sat_forecast = "🟡 ELEVATED - Significant snow possible"
        elif saturday_prob >= 30:
            sat_forecast = "🟢 MODERATE - Some snow possible"
        elif saturday_prob >= 15:
            sat_forecast = "⚪ LOW-MODERATE - Light snow chance"
        else:
            sat_forecast = "⚪ LOW - Minimal snow expected"

        # Sunday
        sunday_prob = min(sunday_score * 25, 100)
        if sunday_prob >= 70:
            sun_forecast = "🔴 HIGH - Major snow likely"
        elif sunday_prob >= 50:
            sun_forecast = "🟡 ELEVATED - Significant snow possible"
        elif sunday_prob >= 30:
            sun_forecast = "🟢 MODERATE - Some snow possible"
        elif sunday_prob >= 15:
            sun_forecast = "⚪ LOW-MODERATE - Light snow chance"
        else:
            sun_forecast = "⚪ LOW - Minimal snow expected"

        print(f"SATURDAY, {self.weekend_start.strftime('%B %d')}:")
        print(f"  Precursor Score: {saturday_score:.2f}")
        print(f"  Probability: {saturday_prob:.1f}%")
        print(f"  Forecast: {sat_forecast}")
        if saturday_signals:
            print(f"  Active Signals:")
            for sig in saturday_signals:
                print(f"    • {sig['name']} ({sig['level']}, {sig['snow']:.1f}mm, {sig['lag']}d lead)")
        else:
            print(f"  Active Signals: None")

        print(f"\nSUNDAY, {self.weekend_end.strftime('%B %d')}:")
        print(f"  Precursor Score: {sunday_score:.2f}")
        print(f"  Probability: {sunday_prob:.1f}%")
        print(f"  Forecast: {sun_forecast}")
        if sunday_signals:
            print(f"  Active Signals:")
            for sig in sunday_signals:
                print(f"    • {sig['name']} ({sig['level']}, {sig['snow']:.1f}mm, {sig['lag']}d lead)")
        else:
            print(f"  Active Signals: None")

        print(f"\nPacific Moisture:")
        if pacific_score >= 0.5:
            print(f"  ✅ Abundant moisture available")
        elif pacific_score >= 0.3:
            print(f"  🟡 Some moisture available")
        else:
            print(f"  ⚪ Limited moisture")

        print(f"\n{'─'*80}")
        print(f"INTERPRETATION:")
        print(f"{'─'*80}\n")

        weekend_max_prob = max(saturday_prob, sunday_prob)

        if weekend_max_prob >= 50:
            print(f"🎯 WEEKEND SNOW POTENTIAL: {weekend_max_prob:.0f}%")
            print(f"Early warning signals are showing activity at lead-time stations.")
            print(f"Monitor Thursday-Friday for confirmation from mid-range predictors.")
            print(f"Check NWS forecast for timing and amounts.")
        elif weekend_max_prob >= 30:
            print(f"⚪ WEEKEND SNOW POTENTIAL: {weekend_max_prob:.0f}%")
            print(f"Some precursor signals present but not strong.")
            print(f"Light to moderate snow possible, but not a major event.")
        else:
            print(f"⚪ WEEKEND SNOW POTENTIAL: {weekend_max_prob:.0f}%")
            print(f"Minimal precursor signals for weekend snow.")
            print(f"Current pattern suggests quiet conditions.")

        print(f"\n⏰ KEY CHECKPOINTS:")
        print(f"  • Wednesday PM: Check Niigata for Thursday signal")
        print(f"  • Thursday AM: Review daily forecast for updated outlook")
        print(f"  • Friday AM: Check Mt Baker/Steamboat for confirmation")
        print(f"  • Friday PM: Final NWS forecast should be available")

        print(f"\n{'='*80}\n")

        return {
            'saturday': {
                'probability': saturday_prob,
                'score': saturday_score,
                'forecast': sat_forecast,
                'signals': saturday_signals
            },
            'sunday': {
                'probability': sunday_prob,
                'score': sunday_score,
                'forecast': sun_forecast,
                'signals': sunday_signals
            },
            'weekend_max_probability': weekend_max_prob
        }


def main():
    checker = WeekendForecastChecker()
    result = checker.check_weekend_precursors()


if __name__ == '__main__':
    main()
