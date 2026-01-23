#!/usr/bin/env python3
"""
Forecast Verification Dashboard
Tracks forecast accuracy, computes metrics, displays performance

Metrics tracked:
- Hit rate (correctly predicted events)
- False alarm rate
- Probability calibration
- Bias (over/under prediction)
- Brier score (probabilistic accuracy)
- Skill score vs climatology
"""

import sys
sys.path.append('/Users/kyle.jurgens/weather')

import json
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt


class ForecastVerificationDashboard:
    """Tracks and displays forecast verification metrics"""

    def __init__(self):
        self.history_file = Path('enhanced_forecast_history.json')
        self.db_path = 'northwoods_snowfall.db'  # Actual observations

        # Event threshold (what counts as "snow event")
        self.EVENT_THRESHOLD_MM = 5.0  # 5mm+ = snow event

    def load_forecast_history(self) -> List[Dict]:
        """Load forecast history"""
        if not self.history_file.exists():
            return []

        with open(self.history_file, 'r') as f:
            return json.load(f)

    def load_observations(self, station: str = "Eagle River Wi") -> pd.DataFrame:
        """Load actual snowfall observations from database"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT date, snowfall_mm
            FROM snowfall_daily
            WHERE station_id = (
                SELECT station_id FROM stations WHERE name = ?
            )
            ORDER BY date DESC
            LIMIT 90
        """

        df = pd.read_sql_query(query, conn, params=(station,))
        conn.close()

        df['date'] = pd.to_datetime(df['date'])
        return df

    def match_forecasts_to_observations(self, forecasts: List[Dict], observations: pd.DataFrame) -> List[Dict]:
        """Match each forecast to actual observed snowfall in the 7-day window"""

        matched = []

        for forecast in forecasts:
            # Parse forecast timestamp
            fcst_time = datetime.fromisoformat(forecast['timestamp'])
            fcst_date = fcst_time.date()

            # Get 7-day window
            window_start = fcst_date
            window_end = fcst_date + timedelta(days=7)

            # Get observations in window
            obs_in_window = observations[
                (observations['date'].dt.date >= window_start) &
                (observations['date'].dt.date < window_end)
            ]

            if len(obs_in_window) == 0:
                continue  # No observations yet for this forecast

            # Calculate actual snowfall in 7-day window
            actual_7day_mm = obs_in_window['snowfall_mm'].sum()

            # Was there a snow event?
            event_occurred = actual_7day_mm >= self.EVENT_THRESHOLD_MM

            matched.append({
                'forecast_date': fcst_date,
                'forecast_time': fcst_time,
                'predicted_probability': forecast['probability'],
                'predicted_amount_mm': forecast.get('expected_7day_mm', 0),
                'actual_amount_mm': actual_7day_mm,
                'event_occurred': event_occurred,
                'event_predicted': forecast['probability'] >= 50,  # 50% threshold
                'confidence': forecast.get('confidence', 'MEDIUM'),
                'atmospheric_patterns': len(forecast.get('atmospheric_patterns', []))
            })

        return matched

    def compute_metrics(self, matched: List[Dict]) -> Dict:
        """Compute verification metrics"""

        if not matched:
            return {}

        df = pd.DataFrame(matched)

        # Basic counts
        n = len(df)
        events_occurred = df['event_occurred'].sum()
        events_predicted = df['event_predicted'].sum()

        # Contingency table
        hits = ((df['event_occurred'] == True) & (df['event_predicted'] == True)).sum()
        false_alarms = ((df['event_occurred'] == False) & (df['event_predicted'] == True)).sum()
        misses = ((df['event_occurred'] == True) & (df['event_predicted'] == False)).sum()
        correct_negatives = ((df['event_occurred'] == False) & (df['event_predicted'] == False)).sum()

        # Hit rate (probability of detection)
        hit_rate = hits / events_occurred if events_occurred > 0 else 0

        # False alarm rate
        false_alarm_rate = false_alarms / events_predicted if events_predicted > 0 else 0

        # Accuracy
        accuracy = (hits + correct_negatives) / n if n > 0 else 0

        # Bias (forecast frequency / observed frequency)
        bias = events_predicted / events_occurred if events_occurred > 0 else 0

        # Brier score (mean squared error of probabilities)
        brier_score = np.mean((df['predicted_probability']/100.0 - df['event_occurred'].astype(float))**2)

        # Amount error metrics
        amount_errors = df['predicted_amount_mm'] - df['actual_amount_mm']
        mean_error = amount_errors.mean()  # Bias
        mean_absolute_error = amount_errors.abs().mean()
        rmse = np.sqrt((amount_errors**2).mean())

        # Skill score (vs climatology)
        # Climatology = always predict the event rate
        climatology_prob = events_occurred / n
        brier_score_climatology = np.mean((climatology_prob - df['event_occurred'].astype(float))**2)
        skill_score = 1 - (brier_score / brier_score_climatology) if brier_score_climatology > 0 else 0

        return {
            'n_forecasts': n,
            'events_occurred': int(events_occurred),
            'events_predicted': int(events_predicted),
            'hits': int(hits),
            'false_alarms': int(false_alarms),
            'misses': int(misses),
            'correct_negatives': int(correct_negatives),
            'hit_rate': hit_rate,
            'false_alarm_rate': false_alarm_rate,
            'accuracy': accuracy,
            'bias': bias,
            'brier_score': brier_score,
            'skill_score': skill_score,
            'mean_error_mm': mean_error,
            'mean_absolute_error_mm': mean_absolute_error,
            'rmse_mm': rmse,
            'matched_data': df
        }

    def display_dashboard(self):
        """Display comprehensive verification dashboard"""

        print("=" * 80)
        print("FORECAST VERIFICATION DASHBOARD")
        print("=" * 80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Load data
        print("Loading forecast history and observations...")
        forecasts = self.load_forecast_history()

        if not forecasts:
            print("❌ No forecast history found")
            print("   Run forecasts first to build history")
            return

        observations = self.load_observations()
        print(f"✅ Loaded {len(forecasts)} forecasts and {len(observations)} days of observations")
        print()

        # Match forecasts to observations
        matched = self.match_forecasts_to_observations(forecasts, observations)

        if not matched:
            print("⚠️  No verified forecasts yet (need observations for forecast periods)")
            print("   Forecasts are stored and will be verified as observations come in")
            return

        print(f"✅ Verified {len(matched)} forecasts")
        print()

        # Compute metrics
        metrics = self.compute_metrics(matched)

        # Display metrics
        self._display_metrics(metrics)

        # Display recent forecasts
        self._display_recent_forecasts(matched)

        # Optionally save plots
        self._generate_plots(metrics)

    def _display_metrics(self, metrics: Dict):
        """Display verification metrics"""

        print("=" * 80)
        print("VERIFICATION METRICS")
        print("=" * 80)
        print()

        print("EVENT DETECTION PERFORMANCE:")
        print("─" * 80)
        print(f"  Total forecasts verified:  {metrics['n_forecasts']}")
        print(f"  Events observed:           {metrics['events_occurred']}")
        print(f"  Events predicted:          {metrics['events_predicted']}")
        print()
        print(f"  Hits (correct yes):        {metrics['hits']}")
        print(f"  Misses (said no, was yes): {metrics['misses']}")
        print(f"  False alarms (said yes, was no): {metrics['false_alarms']}")
        print(f"  Correct negatives:         {metrics['correct_negatives']}")
        print()

        # Hit rate (with interpretation)
        hr = metrics['hit_rate']
        hr_grade = self._grade_metric(hr, 0.8, 0.6)
        print(f"  Hit Rate:                  {hr:.1%} {hr_grade}")
        print(f"    (Successfully detected {hr:.0%} of events)")
        print()

        # False alarm rate (with interpretation)
        far = metrics['false_alarm_rate']
        far_grade = self._grade_metric(1-far, 0.8, 0.6)  # Inverse (lower is better)
        print(f"  False Alarm Rate:          {far:.1%} {far_grade}")
        print(f"    (False alarms on {far:.0%} of predictions)")
        print()

        # Accuracy
        acc = metrics['accuracy']
        acc_grade = self._grade_metric(acc, 0.75, 0.60)
        print(f"  Overall Accuracy:          {acc:.1%} {acc_grade}")
        print()

        # Bias
        bias = metrics['bias']
        bias_grade = self._grade_bias(bias)
        print(f"  Bias (pred/obs ratio):     {bias:.2f} {bias_grade}")
        if bias > 1.2:
            print(f"    (Tendency to over-predict)")
        elif bias < 0.8:
            print(f"    (Tendency to under-predict)")
        else:
            print(f"    (Well-calibrated)")
        print()

        print("PROBABILISTIC PERFORMANCE:")
        print("─" * 80)
        # Brier score
        bs = metrics['brier_score']
        bs_grade = self._grade_metric(1-bs, 0.85, 0.75)  # Inverse
        print(f"  Brier Score:               {bs:.3f} {bs_grade}")
        print(f"    (0 = perfect, 1 = worst)")
        print()

        # Skill score
        ss = metrics['skill_score']
        ss_grade = self._grade_metric(ss, 0.25, 0.10)
        print(f"  Skill Score vs Climate:    {ss:.2f} {ss_grade}")
        if ss > 0:
            print(f"    (Better than climatology)")
        else:
            print(f"    (Worse than climatology)")
        print()

        print("AMOUNT PREDICTION PERFORMANCE:")
        print("─" * 80)
        print(f"  Mean Error:                {metrics['mean_error_mm']:.1f} mm")
        print(f"  Mean Absolute Error:       {metrics['mean_absolute_error_mm']:.1f} mm")
        print(f"  RMSE:                      {metrics['rmse_mm']:.1f} mm")
        print()

    def _grade_metric(self, value: float, excellent: float, good: float) -> str:
        """Grade a metric (higher is better)"""
        if value >= excellent:
            return "🟢 EXCELLENT"
        elif value >= good:
            return "🟡 GOOD"
        else:
            return "🔴 NEEDS IMPROVEMENT"

    def _grade_bias(self, bias: float) -> str:
        """Grade bias (1.0 is perfect)"""
        if 0.9 <= bias <= 1.1:
            return "🟢 EXCELLENT"
        elif 0.8 <= bias <= 1.2:
            return "🟡 GOOD"
        else:
            return "🔴 NEEDS IMPROVEMENT"

    def _display_recent_forecasts(self, matched: List[Dict]):
        """Display recent forecast verification details"""

        print("=" * 80)
        print("RECENT FORECAST VERIFICATION (Last 10)")
        print("=" * 80)
        print()

        df = pd.DataFrame(matched)
        recent = df.tail(10).sort_values('forecast_date', ascending=False)

        for _, row in recent.iterrows():
            print(f"Date: {row['forecast_date']}")
            print(f"  Predicted: {row['predicted_probability']:.0f}% prob, {row['predicted_amount_mm']:.1f}mm")
            print(f"  Actual: {row['actual_amount_mm']:.1f}mm")

            # Verification
            if row['event_occurred']:
                if row['event_predicted']:
                    result = "✅ HIT (correctly predicted event)"
                else:
                    result = "❌ MISS (failed to predict event)"
            else:
                if row['event_predicted']:
                    result = "⚠️  FALSE ALARM (predicted event that didn't occur)"
                else:
                    result = "✅ CORRECT NEGATIVE (correctly predicted no event)"

            print(f"  {result}")
            print()

    def _generate_plots(self, metrics: Dict):
        """Generate verification plots"""

        try:
            df = metrics['matched_data']

            fig, axes = plt.subplots(2, 2, figsize=(12, 10))

            # Plot 1: Predicted vs Actual amounts
            axes[0, 0].scatter(df['predicted_amount_mm'], df['actual_amount_mm'], alpha=0.6)
            axes[0, 0].plot([0, df['actual_amount_mm'].max()],
                           [0, df['actual_amount_mm'].max()], 'r--', label='Perfect')
            axes[0, 0].set_xlabel('Predicted Amount (mm)')
            axes[0, 0].set_ylabel('Actual Amount (mm)')
            axes[0, 0].set_title('Predicted vs Actual Snowfall')
            axes[0, 0].legend()
            axes[0, 0].grid(True, alpha=0.3)

            # Plot 2: Probability calibration
            # Bin probabilities and check event frequency
            df['prob_bin'] = pd.cut(df['predicted_probability'], bins=[0, 20, 40, 60, 80, 100])
            calibration = df.groupby('prob_bin')['event_occurred'].mean() * 100

            bin_centers = [10, 30, 50, 70, 90]
            axes[0, 1].plot(bin_centers[:len(calibration)], calibration.values, 'o-', label='Observed')
            axes[0, 1].plot([0, 100], [0, 100], 'r--', label='Perfect calibration')
            axes[0, 1].set_xlabel('Forecast Probability (%)')
            axes[0, 1].set_ylabel('Observed Frequency (%)')
            axes[0, 1].set_title('Probability Calibration')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3)

            # Plot 3: Forecast error over time
            axes[1, 0].plot(df['forecast_date'], df['predicted_amount_mm'] - df['actual_amount_mm'], 'o-')
            axes[1, 0].axhline(y=0, color='r', linestyle='--')
            axes[1, 0].set_xlabel('Forecast Date')
            axes[1, 0].set_ylabel('Error (mm)')
            axes[1, 0].set_title('Forecast Error Time Series')
            axes[1, 0].grid(True, alpha=0.3)
            plt.setp(axes[1, 0].xaxis.get_majorticklabels(), rotation=45)

            # Plot 4: Performance by atmospheric patterns
            pattern_performance = df.groupby('atmospheric_patterns').agg({
                'event_occurred': 'sum',
                'event_predicted': 'sum'
            })

            if len(pattern_performance) > 1:
                x = pattern_performance.index
                width = 0.35
                axes[1, 1].bar([i - width/2 for i in x], pattern_performance['event_occurred'],
                              width, label='Events Occurred', alpha=0.8)
                axes[1, 1].bar([i + width/2 for i in x], pattern_performance['event_predicted'],
                              width, label='Events Predicted', alpha=0.8)
                axes[1, 1].set_xlabel('Number of Atmospheric Patterns Detected')
                axes[1, 1].set_ylabel('Count')
                axes[1, 1].set_title('Performance by Pattern Detection')
                axes[1, 1].legend()
                axes[1, 1].grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig('forecast_verification_plots.png', dpi=150, bbox_inches='tight')
            print("=" * 80)
            print(f"✅ Plots saved to: forecast_verification_plots.png")
            print("=" * 80)

        except Exception as e:
            print(f"\n⚠️  Could not generate plots: {e}")


def main():
    """Run verification dashboard"""
    dashboard = ForecastVerificationDashboard()
    dashboard.display_dashboard()


if __name__ == "__main__":
    main()
