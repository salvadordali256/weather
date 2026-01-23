#!/usr/bin/env python3
"""
14-Day Snowfall Probability Outlook
Uses recent global patterns to project Wisconsin snowfall probability

Key Insight:
- If Sapporo had heavy snow 5 days ago → predict WI snow in 1 day (6-day lag)
- If Sapporo had heavy snow TODAY → predict WI snow in 6 days (6-day lag)
- Scan recent 14-day history to project forward 14 days

IMPORTANT: Beyond 7 days, confidence drops significantly!
This is PROBABILISTIC outlook, not deterministic forecast.
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Predictive stations with positive lag
PREDICTORS = {
    'thunder_bay_on': {'name': 'Thunder Bay, ON', 'lag': 0, 'weight': 0.50},
    'irkutsk_russia': {'name': 'Irkutsk, Russia', 'lag': 7, 'weight': 0.15},
    'sapporo_japan': {'name': 'Sapporo, Japan', 'lag': 6, 'weight': 0.15},
    'chamonix_france': {'name': 'Chamonix, France', 'lag': 5, 'weight': 0.10},
    'zermatt_switzerland': {'name': 'Zermatt, Switzerland', 'lag': 5, 'weight': 0.05},
    'niigata_japan': {'name': 'Niigata, Japan', 'lag': 3, 'weight': 0.05},
}

HEAVY_THRESHOLD = 25.0
MODERATE_THRESHOLD = 15.0
LIGHT_THRESHOLD = 5.0


class FourteenDayOutlook:
    """Generate 14-day probabilistic snowfall outlook"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

    def get_station_history(self, station_id, days=14):
        """Get recent snowfall history for a station"""
        conn = sqlite3.connect(self.db_path)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = """
            SELECT date, snowfall_mm
            FROM snowfall_daily
            WHERE station_id = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date DESC
        """

        df = pd.read_sql_query(query, conn, params=(
            station_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))

        conn.close()
        return df

    def calculate_daily_probability(self):
        """Calculate snowfall probability for each of next 14 days"""

        print(f"\n{'='*80}")
        print(f"14-DAY SNOWFALL PROBABILITY OUTLOOK")
        print(f"Target: Phelps & Land O'Lakes, Wisconsin")
        print(f"{'='*80}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Method: Global early warning signals projection")
        print(f"{'='*80}\n")

        print(f"⚠️  CONFIDENCE DISCLAIMER:")
        print(f"  Days 1-3:  MODERATE confidence (early warning signals)")
        print(f"  Days 4-7:  LOW-MODERATE confidence (pattern level)")
        print(f"  Days 8-14: LOW confidence (long-range pattern only)")
        print(f"  Beyond 14 days: NOT RELIABLE (use NWS extended outlook)\n")
        print(f"{'─'*80}\n")

        # Initialize probability array for next 14 days
        today = datetime.now().date()
        forecast_days = []
        daily_probability = []
        daily_signals = []

        for day_offset in range(14):
            forecast_date = today + timedelta(days=day_offset)
            forecast_days.append(forecast_date)
            daily_probability.append(0.0)
            daily_signals.append([])

        # Scan each predictor's recent history
        for station_id, config in PREDICTORS.items():
            lag = config['lag']
            weight = config['weight']
            name = config['name']

            # Get recent history (14 days back)
            history = self.get_station_history(station_id, days=14)

            if history.empty:
                continue

            # For each day in history, project forward by lag
            for _, row in history.iterrows():
                snow = row['snowfall_mm']
                obs_date = pd.to_datetime(row['date']).date()

                # Project forward: if this station had snow on obs_date,
                # Wisconsin might have snow on (obs_date + lag days)
                wi_forecast_date = obs_date + timedelta(days=lag)

                # Find which forecast day this corresponds to
                if wi_forecast_date < today:
                    continue  # Already passed

                days_from_today = (wi_forecast_date - today).days

                if days_from_today >= 14:
                    continue  # Beyond our forecast window

                # Calculate contribution based on snow amount
                if snow >= HEAVY_THRESHOLD:
                    activity = 1.0
                    level = "HEAVY"
                elif snow >= MODERATE_THRESHOLD:
                    activity = 0.6
                    level = "MODERATE"
                elif snow >= LIGHT_THRESHOLD:
                    activity = 0.3
                    level = "LIGHT"
                else:
                    activity = 0.0
                    level = None

                if activity > 0:
                    contribution = weight * activity
                    daily_probability[days_from_today] += contribution
                    daily_signals[days_from_today].append({
                        'station': name,
                        'obs_date': obs_date,
                        'snow': snow,
                        'level': level,
                        'contribution': contribution
                    })

        # Generate forecast
        print(f"DAY-BY-DAY PROBABILITY OUTLOOK:")
        print(f"{'─'*80}\n")

        for i, (date, prob, signals) in enumerate(zip(forecast_days, daily_probability, daily_signals)):
            day_name = date.strftime('%a')
            date_str = date.strftime('%Y-%m-%d')
            days_out = i

            # Determine outlook
            if prob >= 0.70:
                outlook = "🔴 HIGH - Major snow likely"
                bar = "█" * 20
            elif prob >= 0.50:
                outlook = "🟡 ELEVATED - Snow probable"
                bar = "█" * int(prob * 20)
            elif prob >= 0.30:
                outlook = "🟢 MODERATE - Snow possible"
                bar = "█" * int(prob * 20)
            elif prob >= 0.15:
                outlook = "⚪ LOW-MODERATE - Background chance"
                bar = "█" * int(prob * 20)
            else:
                outlook = "⚪ QUIET - Low probability"
                bar = "░" * 5

            # Confidence based on days out
            if days_out <= 3:
                confidence = "MODERATE"
            elif days_out <= 7:
                confidence = "LOW-MODERATE"
            else:
                confidence = "LOW"

            print(f"Day {days_out+1:2d} | {day_name} {date_str} | Prob: {prob:5.1%} {bar:20s} | {outlook}")

            # Show contributing signals
            if signals:
                for sig in signals[:3]:  # Top 3 signals
                    print(f"       ↳ {sig['station']:25s} had {sig['level']:8s} snow on {sig['obs_date']} (+{sig['contribution']:.1%})")

        # Summary statistics
        print(f"\n{'─'*80}")
        print(f"OUTLOOK SUMMARY:")
        print(f"{'─'*80}\n")

        high_prob_days = sum(1 for p in daily_probability[:7] if p >= 0.50)
        moderate_prob_days = sum(1 for p in daily_probability[:7] if 0.30 <= p < 0.50)
        quiet_days = sum(1 for p in daily_probability[:7] if p < 0.15)

        print(f"Next 7 Days (Higher Confidence):")
        print(f"  High probability days (>50%): {high_prob_days}")
        print(f"  Moderate probability (30-50%): {moderate_prob_days}")
        print(f"  Quiet days (<15%): {quiet_days}")

        avg_prob_week1 = np.mean(daily_probability[:7])
        avg_prob_week2 = np.mean(daily_probability[7:14])

        print(f"\nAverage Probability:")
        print(f"  Week 1 (Days 1-7):  {avg_prob_week1:.1%}")
        print(f"  Week 2 (Days 8-14): {avg_prob_week2:.1%}")

        # Pattern assessment
        if avg_prob_week1 >= 0.30:
            pattern = "🔴 ACTIVE - Favorable pattern for snowfall"
        elif avg_prob_week1 >= 0.15:
            pattern = "🟡 MIXED - Some snow chances, variable pattern"
        else:
            pattern = "⚪ QUIET - Generally quiet pattern"

        print(f"\nPattern Assessment: {pattern}")

        # Peak probability day
        max_prob = max(daily_probability[:14])
        max_day = daily_probability.index(max_prob)
        peak_date = forecast_days[max_day]

        print(f"\nPeak Probability: {peak_date.strftime('%a %Y-%m-%d')} (Day {max_day+1}) at {max_prob:.1%}")

        # Confidence warning
        print(f"\n{'='*80}")
        print(f"⚠️  IMPORTANT USAGE NOTES:")
        print(f"{'='*80}\n")
        print(f"1. This is PATTERN-LEVEL outlook, not event-specific forecast")
        print(f"2. Probabilities represent POTENTIAL based on early warning signals")
        print(f"3. Days 1-3: Use as advance watch (check NWS for details)")
        print(f"4. Days 4-7: General pattern guidance only")
        print(f"5. Days 8-14: Very low confidence, pattern awareness only")
        print(f"6. For specific forecasts, ALWAYS check NWS/NOAA")
        print(f"7. Local lake-effect events may occur without global signals")
        print(f"\n{'='*80}\n")

        return {
            'dates': forecast_days,
            'probabilities': daily_probability,
            'signals': daily_signals,
            'pattern': pattern
        }

    def generate_calendar_view(self, result):
        """Generate calendar-style view"""
        print(f"\n{'='*80}")
        print(f"14-DAY CALENDAR VIEW")
        print(f"{'='*80}\n")

        dates = result['dates']
        probs = result['probabilities']

        # Group by week
        week1 = list(zip(dates[:7], probs[:7]))
        week2 = list(zip(dates[7:14], probs[7:14]))

        print(f"WEEK 1:")
        print(f"  Mon    Tue    Wed    Thu    Fri    Sat    Sun")
        print(f"  " + "       ".join([f"{p:5.1%}" if d.weekday() == i else "      " for i in range(7) for d, p in week1 if d.weekday() == i] or ["     "] * 7))

        print(f"\nWEEK 2:")
        print(f"  Mon    Tue    Wed    Thu    Fri    Sat    Sun")
        print(f"  " + "       ".join([f"{p:5.1%}" if d.weekday() == i else "      " for i in range(7) for d, p in week2 if d.weekday() == i] or ["     "] * 7))

        print(f"\n{'='*80}\n")


def main():
    outlook = FourteenDayOutlook()
    result = outlook.calculate_daily_probability()

    # Save to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"forecast_14day_{timestamp}.txt"
    print(f"💾 14-day outlook saved to: {output_file}")


if __name__ == '__main__':
    main()
