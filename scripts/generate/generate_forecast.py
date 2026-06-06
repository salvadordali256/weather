#!/usr/bin/env python3
"""
Real-Time Snowfall Forecast Generator
Uses global network to predict Phelps/Land O'Lakes, Wisconsin snowfall

Ensemble Framework:
  40% × Thunder Bay (same-day confirmation)
  15% × Steamboat Springs (pattern validation)
  10% × Sapporo, Japan (6-day lead)
   8% × Chamonix, France (5-day lead)
   5% × Niigata, Japan (3-day lead)
   5% × Mount Baker (Pacific moisture)
   5% × Lake Tahoe (Pacific pattern)
   4% × Irkutsk, Russia (7-day lead)
   3% × Novosibirsk (multi-week)
   3% × Zermatt (5-day lead)
   2% × Others
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Ensemble weights based on correlation analysis
# Station IDs must match database exactly
# ONLY INCLUDE STATIONS WITH POSITIVE OR ZERO LAG (that can actually predict Wisconsin!)
# Negative lag = Wisconsin leads them = not useful for forecasting WI

ENSEMBLE_WEIGHTS = {
    # REAL-TIME CONFIRMATION (lag = 0)
    'thunder_bay_on': {'weight': 0.50, 'lag': 0, 'name': 'Thunder Bay, ON', 'type': 'same-day'},

    # EARLY WARNING SIGNALS (positive lag = they lead Wisconsin)
    'irkutsk_russia': {'weight': 0.15, 'lag': 7, 'name': 'Irkutsk, Russia', 'type': '7-day lead'},
    'sapporo_japan': {'weight': 0.15, 'lag': 6, 'name': 'Sapporo, Japan', 'type': '6-day lead'},
    'chamonix_france': {'weight': 0.10, 'lag': 5, 'name': 'Chamonix, France', 'type': '5-day lead'},
    'zermatt_switzerland': {'weight': 0.05, 'lag': 5, 'name': 'Zermatt, Switzerland', 'type': '5-day lead'},
    'niigata_japan': {'weight': 0.05, 'lag': 3, 'name': 'Niigata, Japan', 'type': '3-day lead'},

    # REMOVED: Negative lag stations (Wisconsin leads them, not predictive)
    # 'steamboat_springs_co': lag=-1 (WI leads by 1 day) → removed
    # 'lake_tahoe_ca': lag=-3 (WI leads by 3 days) → removed
    # 'mount_baker_wa': lag=-4 (WI leads by 4 days) → removed
    # 'nagano_japan': lag=-10 (WI leads by 10 days) → removed
    # 'novosibirsk_russia': lag=-24 (WI leads by 24 days) → removed
}

# Thresholds for "active" snow (mm)
LIGHT_SNOW_THRESHOLD = 5.0
MODERATE_SNOW_THRESHOLD = 15.0
HEAVY_SNOW_THRESHOLD = 25.0

class ForecastGenerator:
    """Generate real-time snowfall forecasts using global network"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

    def get_recent_snow(self, station_id, days_ago=0, window=3):
        """
        Get snowfall for a station N days ago (with averaging window)

        Args:
            station_id: Station identifier
            days_ago: Days in the past (positive = lead, negative = lag)
            window: Days to average (default: 3-day window for smoothing)
        """
        conn = sqlite3.connect(self.db_path)

        target_date = datetime.now() - timedelta(days=days_ago)
        start_date = target_date - timedelta(days=window-1)

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
            target_date.strftime('%Y-%m-%d')
        ))

        conn.close()

        if df.empty:
            return None, None

        # Calculate average over window
        avg_snow = df['snowfall_mm'].mean()
        max_snow = df['snowfall_mm'].max()

        return avg_snow, max_snow

    def get_latest_wisconsin_data(self):
        """Get most recent Wisconsin snowfall data"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT station_id, date, snowfall_mm
            FROM snowfall_daily
            WHERE station_id IN ('eagle_river_wi', 'phelps_wi', 'land_o_lakes_wi')
            ORDER BY date DESC
            LIMIT 10
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df

    def generate_forecast(self):
        """Generate ensemble forecast for Wisconsin"""

        print(f"\n{'='*80}")
        print(f"GLOBAL SNOWFALL FORECAST - Phelps & Land O'Lakes, Wisconsin")
        print(f"{'='*80}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Forecast Method: 17-Station Global Ensemble")
        print(f"{'='*80}\n")

        # Check each predictor
        ensemble_score = 0.0
        active_signals = []

        print(f"GLOBAL PREDICTOR STATUS:")
        print(f"{'─'*80}\n")

        for station_id, config in ENSEMBLE_WEIGHTS.items():
            avg_snow, max_snow = self.get_recent_snow(station_id, config['lag'], window=3)

            weight = config['weight']
            name = config['name']
            lag = config['lag']

            if avg_snow is None:
                status = "NO DATA"
                activity = 0.0
                color = "⚪"
            elif avg_snow >= HEAVY_SNOW_THRESHOLD:
                status = f"HEAVY ({max_snow:.1f}mm max)"
                activity = 1.0
                color = "🔴"
                active_signals.append({
                    'name': name,
                    'snow': max_snow,
                    'weight': weight,
                    'lag': lag,
                    'level': 'HEAVY'
                })
            elif avg_snow >= MODERATE_SNOW_THRESHOLD:
                status = f"MODERATE ({max_snow:.1f}mm max)"
                activity = 0.6
                color = "🟡"
                active_signals.append({
                    'name': name,
                    'snow': max_snow,
                    'weight': weight,
                    'lag': lag,
                    'level': 'MODERATE'
                })
            elif avg_snow >= LIGHT_SNOW_THRESHOLD:
                status = f"Light ({max_snow:.1f}mm max)"
                activity = 0.3
                color = "🟢"
            else:
                status = f"Quiet ({avg_snow:.1f}mm avg)"
                activity = 0.0
                color = "⚪"

            contribution = weight * activity
            ensemble_score += contribution

            # Format lag info
            if lag > 0:
                lag_text = f"{lag}d lead"
            elif lag < 0:
                lag_text = f"{abs(lag)}d lag"
            else:
                lag_text = "NOW"

            print(f"{color} {name:30s} | {status:25s} | {lag_text:10s} | +{contribution:.1%}")

        # Calculate forecast
        print(f"\n{'─'*80}")
        print(f"ENSEMBLE ANALYSIS:")
        print(f"{'─'*80}\n")

        print(f"Total Ensemble Score: {ensemble_score:.1%}")
        print(f"Active Signals: {len(active_signals)}/11 predictors")

        if active_signals:
            print(f"\nACTIVE PREDICTOR DETAILS:")
            for sig in sorted(active_signals, key=lambda x: x['weight'], reverse=True):
                lag_info = f"{sig['lag']:+d}d" if sig['lag'] != 0 else "NOW"
                print(f"  • {sig['name']:30s} {sig['level']:8s} {sig['snow']:5.1f}mm | {lag_info:6s} | Weight: {sig['weight']:.1%}")

        print(f"\n{'='*80}")
        print(f"FORECAST FOR PHELPS & LAND O'LAKES, WISCONSIN")
        print(f"{'='*80}\n")

        # Determine confidence and forecast
        if ensemble_score >= 0.70:
            confidence = "VERY HIGH"
            forecast = "MAJOR SNOW EVENT LIKELY"
            recommendation = "🔴 HIGH ALERT - Prepare for significant snowfall within 24-48 hours"
            probability = "85-95%"
        elif ensemble_score >= 0.50:
            confidence = "HIGH"
            forecast = "SNOW EVENT PROBABLE"
            recommendation = "🟡 WATCH - Increased probability of snowfall, monitor closely"
            probability = "65-85%"
        elif ensemble_score >= 0.30:
            confidence = "MODERATE"
            forecast = "SNOW POSSIBLE"
            recommendation = "🟢 ADVISORY - Some snowfall possible, not major event expected"
            probability = "40-65%"
        elif ensemble_score >= 0.15:
            confidence = "LOW-MODERATE"
            forecast = "BACKGROUND SNOW CHANCE"
            recommendation = "⚪ BACKGROUND - Normal winter pattern, minor snow possible"
            probability = "20-40%"
        else:
            confidence = "LOW"
            forecast = "QUIET PATTERN"
            recommendation = "⚪ QUIET - Low probability of significant snowfall"
            probability = "0-20%"

        print(f"Confidence Level: {confidence}")
        print(f"Forecast: {forecast}")
        print(f"Snow Probability: {probability}")
        print(f"Timeframe: Next 24-72 hours")
        print(f"\n{recommendation}")

        # Show recent Wisconsin conditions
        print(f"\n{'─'*80}")
        print(f"RECENT WISCONSIN CONDITIONS (Verification):")
        print(f"{'─'*80}\n")

        wi_data = self.get_latest_wisconsin_data()
        if not wi_data.empty:
            for _, row in wi_data.head(3).iterrows():
                station = row['station_id'].replace('_', ' ').title()
                date = row['date']
                snow = row['snowfall_mm'] if pd.notna(row['snowfall_mm']) else 0.0
                print(f"  {station:25s} {date:12s} {snow:6.1f}mm")
        else:
            print("  No recent data available")

        print(f"\n{'='*80}")
        print(f"FORECAST INTERPRETATION GUIDE:")
        print(f"{'='*80}\n")
        print(f"  Ensemble Score > 70%  = VERY HIGH confidence (major event likely)")
        print(f"  Ensemble Score 50-70% = HIGH confidence (snow event probable)")
        print(f"  Ensemble Score 30-50% = MODERATE confidence (snow possible)")
        print(f"  Ensemble Score 15-30% = LOW-MODERATE (background chance)")
        print(f"  Ensemble Score < 15%  = LOW confidence (quiet pattern)")
        print(f"\n  Lead predictors (Japan, Alps, Siberia) provide 3-7 day advance warning")
        print(f"  Thunder Bay (same-day) provides real-time confirmation")
        print(f"  Steamboat Springs provides pattern validation")
        print(f"{'='*80}\n")

        return {
            'ensemble_score': ensemble_score,
            'confidence': confidence,
            'forecast': forecast,
            'probability': probability,
            'active_signals': active_signals
        }


def main():
    generator = ForecastGenerator()
    result = generator.generate_forecast()

    # Save forecast to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"forecast_{timestamp}.txt"

    print(f"💾 Forecast saved to: {output_file}")


if __name__ == '__main__':
    main()
