#!/usr/bin/env python3
"""
Automated Daily Forecast System
Runs all forecast models daily and saves results for tracking

Features:
- Runs integrated forecast system
- Saves forecast history to JSON
- Generates daily summary report
- Tracks forecast accuracy over time
- Email/notification ready (optional)
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

class DailyForecastRunner:
    """Automated daily forecast generation and tracking"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path
        self.history_file = 'daily_forecast_history.json'
        self.reports_dir = Path('forecast_reports')
        self.reports_dir.mkdir(exist_ok=True)

    def load_forecast_history(self):
        """Load previous forecasts for tracking"""
        if Path(self.history_file).exists():
            with open(self.history_file, 'r') as f:
                return json.load(f)
        return {'forecasts': []}

    def save_forecast_history(self, history):
        """Save forecast history"""
        with open(self.history_file, 'w') as f:
            json.dump(history, f, indent=2, default=str)

    def get_current_conditions(self):
        """Get current Wisconsin snow conditions for verification"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT date, station_id, snowfall_mm
            FROM snowfall_daily
            WHERE station_id IN ('phelps_wi', 'land_o_lakes_wi', 'eagle_river_wi')
              AND date >= date('now', '-7 days')
            ORDER BY date DESC, snowfall_mm DESC
            LIMIT 5
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df

    def run_all_models(self):
        """Run all forecast models"""
        from integrated_forecast_system import IntegratedForecastSystem

        print(f"\n{'='*80}")
        print(f"🌨️  AUTOMATED DAILY FORECAST")
        print(f"{'='*80}")
        print(f"📅 Date: {datetime.now().strftime('%A, %B %d, %Y')}")
        print(f"⏰ Time: {datetime.now().strftime('%I:%M %p')}")
        print(f"{'='*80}\n")

        # Run integrated system
        system = IntegratedForecastSystem()
        result = system.run_integrated_forecast()

        return result

    def verify_previous_forecasts(self, history):
        """Check accuracy of previous forecasts"""

        print(f"\n{'='*80}")
        print(f"📊 FORECAST VERIFICATION")
        print(f"{'='*80}\n")

        if len(history['forecasts']) == 0:
            print("No previous forecasts to verify yet.\n")
            return

        # Get actual snow from last 7 days
        actual_snow = self.get_current_conditions()

        if actual_snow.empty:
            print("No recent snow data available for verification.\n")
            return

        # Check last forecast
        recent_forecasts = [f for f in history['forecasts']
                          if (datetime.now() - datetime.fromisoformat(f['timestamp'])).days <= 7]

        if not recent_forecasts:
            print("No recent forecasts to verify.\n")
            return

        print(f"Verifying last {len(recent_forecasts)} forecasts:\n")

        for forecast in recent_forecasts[-3:]:  # Last 3 forecasts
            fc_date = datetime.fromisoformat(forecast['timestamp'])
            days_ago = (datetime.now() - fc_date).days

            # Get actual snow since that forecast
            forecast_period_snow = actual_snow[
                pd.to_datetime(actual_snow['date']) > fc_date.strftime('%Y-%m-%d')
            ]['snowfall_mm'].sum()

            predicted = forecast['result']['expected_amount_mm']
            actual = forecast_period_snow

            error = abs(predicted - actual)
            error_pct = (error / max(predicted, 1)) * 100

            if error_pct < 20:
                accuracy = "✅ EXCELLENT"
            elif error_pct < 40:
                accuracy = "🟢 GOOD"
            elif error_pct < 60:
                accuracy = "🟡 FAIR"
            else:
                accuracy = "🔴 POOR"

            print(f"{fc_date.strftime('%b %d')} ({days_ago}d ago):")
            print(f"  Predicted: {predicted:.1f}mm | Actual: {actual:.1f}mm | Error: {error:.1f}mm ({error_pct:.0f}%) {accuracy}")

        print()

    def generate_daily_report(self, result):
        """Generate human-readable daily report"""

        today = datetime.now()
        report_date = today.strftime('%Y-%m-%d')
        report_file = self.reports_dir / f"forecast_{report_date}.txt"

        report = []
        report.append("="*80)
        report.append("DAILY SNOW FORECAST - Northern Wisconsin")
        report.append("="*80)
        report.append(f"Date: {today.strftime('%A, %B %d, %Y')}")
        report.append(f"Time: {today.strftime('%I:%M %p')}")
        report.append(f"Location: Phelps, Land O'Lakes, Eagle River, WI")
        report.append("="*80)
        report.append("")

        # Current conditions
        report.append("CURRENT CONDITIONS:")
        report.append("─"*80)
        current = self.get_current_conditions()
        if not current.empty:
            latest = current.iloc[0]
            report.append(f"Latest: {latest['station_id'].replace('_', ' ').title()}")
            report.append(f"Date: {latest['date']}")
            report.append(f"Snow: {latest['snowfall_mm']:.1f}mm")
        else:
            report.append("No recent data available")
        report.append("")

        # Forecast
        report.append("7-DAY FORECAST:")
        report.append("─"*80)
        report.append(f"Risk Level: {result['risk_level']}")
        report.append(f"Forecast: {result['forecast']}")
        report.append(f"Probability: {result['final_probability']:.1f}%")
        report.append(f"Expected 7-day Total: {result['expected_amount_mm']:.1f}mm ({result['expected_amount_mm']/25.4:.1f} inches)")
        report.append("")

        # Model breakdown
        report.append("MODEL CONTRIBUTIONS:")
        report.append("─"*80)
        for model, prob in result['model_contributions'].items():
            report.append(f"  {model.replace('_', ' ').title()}: {prob:.1f}%")
        report.append("")

        # Warnings
        if result['filters_failed']:
            report.append("⚠️  WARNINGS:")
            report.append("─"*80)
            for warning in result['filters_failed']:
                report.append(f"  • {warning}")
            report.append("")

        # Interpretation
        report.append("INTERPRETATION:")
        report.append("─"*80)
        if result['risk_level'] == 'HIGH':
            report.append("🔴 Major snow event likely. Prepare for significant accumulation.")
            report.append("   Monitor NWS forecasts for timing and amounts.")
        elif result['risk_level'] in ['MODERATE-HIGH', 'MODERATE']:
            report.append("🟡 Some snow expected. Normal winter conditions.")
            report.append("   Check NWS for specific timing.")
        else:
            report.append("⚪ Quiet pattern. Light snow or flurries possible.")
            report.append("   No major events expected.")
        report.append("")

        # Next update
        tomorrow = today + timedelta(days=1)
        report.append(f"Next Update: {tomorrow.strftime('%A, %B %d at %I:%M %p')}")
        report.append("="*80)

        # Save report
        with open(report_file, 'w') as f:
            f.write('\n'.join(report))

        # Print report
        print('\n'.join(report))
        print()

        return report_file

    def run_daily_forecast(self):
        """Main daily forecast routine"""

        # Load history
        history = self.load_forecast_history()

        # Verify previous forecasts
        self.verify_previous_forecasts(history)

        # Run forecast models
        result = self.run_all_models()

        # Save to history
        history['forecasts'].append({
            'timestamp': datetime.now().isoformat(),
            'result': result
        })
        self.save_forecast_history(history)

        # Generate report
        report_file = self.generate_daily_report(result)

        print(f"{'='*80}")
        print(f"✅ DAILY FORECAST COMPLETE")
        print(f"{'='*80}\n")
        print(f"📁 Report saved to: {report_file}")
        print(f"📊 History updated: {self.history_file}")
        print(f"📈 Total forecasts tracked: {len(history['forecasts'])}")
        print(f"\n{'='*80}\n")

        return result


def main():
    runner = DailyForecastRunner()
    runner.run_daily_forecast()


if __name__ == '__main__':
    main()
