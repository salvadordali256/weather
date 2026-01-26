#!/usr/bin/env python3
"""
Daily Automated Forecast Runner
Generates forecasts for Wisconsin ski areas and saves to JSON

Runs daily to provide:
- 24-48 hour short-range forecast
- 7-day outlook
- Event type identification
- Confidence levels
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
from pathlib import Path

# Load .env file
load_dotenv()

from enhanced_regional_forecast_system import EnhancedRegionalForecastSystem

# Default paths, can be overridden via environment variables
DEFAULT_DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')
DEFAULT_OUTPUT_DIR = os.environ.get('FORECAST_OUTPUT_DIR', 'forecast_output')

class DailyForecastRunner:
    """Automated daily forecast generation"""

    def __init__(self, db_path=None, output_dir=None):
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        if output_dir is None:
            output_dir = DEFAULT_OUTPUT_DIR
        self.db_path = db_path
        self.forecast_system = EnhancedRegionalForecastSystem(db_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Target locations
        self.locations = {
            'phelps_wi': {
                'name': 'Phelps, WI',
                'ski_resorts': ['Big Snow Resort (nearby)'],
                'display_name': 'Northern Wisconsin - Phelps'
            },
            'land_o_lakes_wi': {
                'name': 'Land O\'Lakes, WI',
                'ski_resorts': ['Gateway area'],
                'display_name': 'Northern Wisconsin - Land O\'Lakes'
            },
            'eagle_river_wi': {
                'name': 'Eagle River, WI',
                'ski_resorts': ['Numerous area resorts'],
                'display_name': 'Northern Wisconsin - Eagle River'
            }
        }

    def generate_multi_day_forecast(self, days_ahead=7):
        """
        Generate forecast for next N days
        Returns list of daily forecasts
        """
        forecasts = []
        today = datetime.now()

        print(f"\n{'='*80}")
        print(f"GENERATING {days_ahead}-DAY FORECAST")
        print(f"{'='*80}")
        print(f"Generated: {today.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")

        for day_offset in range(1, days_ahead + 1):
            target_date = today + timedelta(days=day_offset)

            print(f"Day {day_offset}: {target_date.strftime('%Y-%m-%d')} ({target_date.strftime('%A')})")

            # Generate forecast
            forecast = self.forecast_system.generate_ensemble_forecast(target_date)

            # Add date info
            forecast['target_date'] = target_date.strftime('%Y-%m-%d')
            forecast['day_of_week'] = target_date.strftime('%A')
            forecast['day_offset'] = day_offset

            # Simplify for display
            forecast_summary = {
                'date': forecast['target_date'],
                'day_of_week': forecast['day_of_week'],
                'day_number': day_offset,
                'probability': forecast['probability'],
                'event_type': forecast['event_type'],
                'confidence': forecast['confidence'],
                'lead_time': forecast['lead_time'],
                'ensemble_score': round(forecast['ensemble_score'], 3),
                'regional_score': round(forecast['regional_score'], 3),
                'global_score': round(forecast['global_score'], 3),
                'forecast_category': forecast['forecast_category'],
                'active_signals': forecast['total_active_signals']
            }

            # Add expected snowfall range
            if forecast['probability'] >= 70:
                snowfall_range = "20-50mm (1-2 inches)"
            elif forecast['probability'] >= 50:
                snowfall_range = "10-30mm (0.5-1.5 inches)"
            elif forecast['probability'] >= 30:
                snowfall_range = "5-20mm (trace-1 inch)"
            elif forecast['probability'] >= 15:
                snowfall_range = "0-10mm (trace-0.5 inch)"
            else:
                snowfall_range = "0-5mm (trace or none)"

            forecast_summary['expected_snowfall'] = snowfall_range

            # Add icon/emoji for display
            if forecast['probability'] >= 70:
                forecast_summary['icon'] = '❄️'
                forecast_summary['alert_level'] = 'HIGH'
            elif forecast['probability'] >= 50:
                forecast_summary['icon'] = '🌨️'
                forecast_summary['alert_level'] = 'MODERATE'
            elif forecast['probability'] >= 30:
                forecast_summary['icon'] = '☁️'
                forecast_summary['alert_level'] = 'LOW'
            else:
                forecast_summary['icon'] = '⚪'
                forecast_summary['alert_level'] = 'MINIMAL'

            forecasts.append(forecast_summary)

            print(f"  {forecast_summary['icon']} {forecast_summary['probability']}% - "
                  f"{forecast_summary['event_type']} - {forecast_summary['expected_snowfall']}")

        print(f"\n{'='*80}\n")
        return forecasts

    def get_recent_observations(self, days=7):
        """Get recent snowfall observations"""
        conn = sqlite3.connect(self.db_path)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = """
            SELECT
                s.name as station_name,
                sd.date,
                sd.snowfall_mm
            FROM snowfall_daily sd
            JOIN stations s ON sd.station_id = s.station_id
            WHERE sd.station_id IN ('phelps_wi', 'land_o_lakes_wi', 'eagle_river_wi')
              AND sd.date >= ?
              AND sd.date <= ?
            ORDER BY sd.date DESC, s.name
        """

        df = pd.read_sql_query(query, conn, params=(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))

        conn.close()

        if df.empty:
            return []

        observations = []
        for _, row in df.iterrows():
            observations.append({
                'station': row['station_name'],
                'date': row['date'],
                'snowfall_mm': round(row['snowfall_mm'], 1),
                'snowfall_inches': round(row['snowfall_mm'] / 25.4, 1)
            })

        return observations

    def generate_summary_text(self, forecasts):
        """Generate human-readable summary"""

        # Find most significant day
        max_prob_day = max(forecasts, key=lambda x: x['probability'])

        # Count snow days
        snow_days = sum(1 for f in forecasts if f['probability'] >= 30)

        # Event types
        event_types = [f['event_type'] for f in forecasts if f['event_type'] != 'QUIET']

        summary = f"""
7-DAY FORECAST SUMMARY for Northern Wisconsin
Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}

OVERVIEW:
  • {snow_days} day(s) with snow potential (≥30% probability)
  • Highest probability: {max_prob_day['probability']}% on {max_prob_day['day_of_week']}, {max_prob_day['date']}
  • Primary patterns: {', '.join(set(event_types)) if event_types else 'Quiet conditions'}

DETAILED OUTLOOK:
"""

        for forecast in forecasts:
            summary += f"\n{forecast['day_of_week']}, {forecast['date']}:\n"
            summary += f"  {forecast['icon']} {forecast['probability']}% probability - {forecast['event_type']}\n"
            summary += f"  Expected: {forecast['expected_snowfall']}\n"
            summary += f"  Confidence: {forecast['confidence']}\n"

        summary += f"""
ALERT LEVELS:
  ❄️  HIGH (≥70%) - Major snow likely
  🌨️  MODERATE (50-69%) - Significant snow possible
  ☁️  LOW (30-49%) - Light snow possible
  ⚪ MINIMAL (<30%) - Little to no snow expected

Lead times:
  • Short-range (12-48h): Alberta Clippers, Lake Effect
  • Long-range (5-7d): Global atmospheric patterns
"""

        return summary

    def save_forecast(self, forecasts, observations):
        """Save forecast to JSON file"""

        output = {
            'generated_at': datetime.now().isoformat(),
            'generated_at_human': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
            'forecast_days': len(forecasts),
            'forecasts': forecasts,
            'recent_observations': observations,
            'summary_text': self.generate_summary_text(forecasts),
            'locations': self.locations
        }

        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = self.output_dir / f'forecast_{timestamp}.json'

        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"✅ Forecast saved to: {output_file}")

        # Also save as "latest" for web dashboard
        latest_file = self.output_dir / 'latest_forecast.json'
        with open(latest_file, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"✅ Latest forecast saved to: {latest_file}")

        return output_file

    def run_daily_forecast(self):
        """Main daily forecast generation"""

        print("\n" + "="*80)
        print("DAILY AUTOMATED FORECAST RUNNER")
        print("Wisconsin Snowfall Forecast System")
        print("="*80 + "\n")

        # Generate 7-day forecast
        forecasts = self.generate_multi_day_forecast(days_ahead=7)

        # Get recent observations
        print("Loading recent observations...")
        observations = self.get_recent_observations(days=7)
        print(f"✅ Loaded {len(observations)} recent observations\n")

        # Display summary
        summary = self.generate_summary_text(forecasts)
        print(summary)

        # Save to file
        output_file = self.save_forecast(forecasts, observations)

        print("\n" + "="*80)
        print("FORECAST GENERATION COMPLETE")
        print("="*80)
        print(f"\nForecast files generated:")
        print(f"  • Timestamped: {output_file}")
        print(f"  • Latest: {self.output_dir / 'latest_forecast.json'}")
        print(f"\nRun web dashboard to view: python3 forecast_web_dashboard.py")
        print("="*80 + "\n")

        return forecasts


def main():
    """Run daily forecast"""
    runner = DailyForecastRunner()
    runner.run_daily_forecast()


if __name__ == '__main__':
    main()
