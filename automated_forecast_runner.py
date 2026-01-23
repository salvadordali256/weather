#!/usr/bin/env python3
"""
Automated Forecast Runner with Smart Alerting
Runs daily forecasts and sends notifications when significant events are detected

Features:
- Scheduled execution (cron-compatible)
- Threshold-based alerting
- Email notifications
- Change detection (probability jumps)
- Forecast history tracking
"""

import sys
sys.path.append('/Users/kyle.jurgens/weather')

import json
import smtplib
import subprocess
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from enhanced_forecast_system import EnhancedForecastSystem


class AlertConfig:
    """Configuration for alerting system"""

    # Probability thresholds
    HIGH_PROBABILITY_THRESHOLD = 60  # Alert if probability > 60%
    MODERATE_PROBABILITY_THRESHOLD = 40  # Alert if probability > 40%

    # Change detection
    SIGNIFICANT_CHANGE_THRESHOLD = 20  # Alert if probability changes by >20%

    # Alert methods
    ENABLE_EMAIL = False  # Set to True to enable email alerts
    ENABLE_NOTIFICATION = True  # macOS notification center
    ENABLE_CONSOLE = True  # Always print to console

    # Email config (fill in if ENABLE_EMAIL = True)
    EMAIL_FROM = "your-email@example.com"
    EMAIL_TO = "your-email@example.com"
    EMAIL_SMTP_SERVER = "smtp.gmail.com"
    EMAIL_SMTP_PORT = 587
    EMAIL_PASSWORD = ""  # Use app password for Gmail


class AutomatedForecastRunner:
    """Runs forecasts and sends smart alerts"""

    def __init__(self, config: AlertConfig = None):
        self.config = config or AlertConfig()
        self.forecast_system = EnhancedForecastSystem()
        self.history_file = Path('enhanced_forecast_history.json')

    def run(self):
        """Execute forecast and send appropriate alerts"""

        print("=" * 80)
        print("AUTOMATED FORECAST RUNNER")
        print("=" * 80)
        print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Generate forecast
        try:
            forecast = self.forecast_system.generate_forecast()
        except Exception as e:
            self._send_error_alert(f"Forecast generation failed: {e}")
            raise

        # Check for alert conditions
        alerts = self._check_alert_conditions(forecast)

        # Send alerts
        if alerts:
            self._send_alerts(forecast, alerts)
        else:
            print("\n✅ No alerts triggered - quiet forecast")

        return forecast

    def _check_alert_conditions(self, forecast) -> list:
        """Check if any alert conditions are met"""

        alerts = []
        prob = forecast['final_probability']
        patterns = forecast.get('atmospheric_patterns', [])

        # 1. High probability alert
        if prob >= self.config.HIGH_PROBABILITY_THRESHOLD:
            alerts.append({
                'level': 'HIGH',
                'type': 'high_probability',
                'message': f"High snow probability: {prob:.1f}%"
            })

        # 2. Moderate probability alert
        elif prob >= self.config.MODERATE_PROBABILITY_THRESHOLD:
            alerts.append({
                'level': 'MODERATE',
                'type': 'moderate_probability',
                'message': f"Moderate snow probability: {prob:.1f}%"
            })

        # 3. Significant change from previous forecast
        prev_prob = self._get_previous_probability()
        if prev_prob is not None:
            change = prob - prev_prob
            if abs(change) >= self.config.SIGNIFICANT_CHANGE_THRESHOLD:
                direction = "increased" if change > 0 else "decreased"
                alerts.append({
                    'level': 'INFO',
                    'type': 'significant_change',
                    'message': f"Probability {direction} by {abs(change):.1f}% ({prev_prob:.1f}% → {prob:.1f}%)"
                })

        # 4. Atmospheric pattern detected
        for pattern in patterns:
            if pattern['confidence'] > 0.6:  # High confidence patterns
                alerts.append({
                    'level': 'INFO',
                    'type': 'pattern_detected',
                    'message': f"{pattern['pattern_type'].replace('_', ' ').title()} detected ({pattern['confidence']*100:.0f}% confidence)"
                })

        return alerts

    def _get_previous_probability(self):
        """Get probability from previous forecast"""
        try:
            if not self.history_file.exists():
                return None

            with open(self.history_file, 'r') as f:
                history = json.load(f)

            if len(history) < 2:
                return None

            # Get second-to-last (current is already saved)
            return history[-2]['probability']

        except Exception:
            return None

    def _send_alerts(self, forecast, alerts):
        """Send alerts via configured methods"""

        print("\n" + "=" * 80)
        print("ALERTS TRIGGERED")
        print("=" * 80)

        for alert in alerts:
            print(f"\n{self._get_alert_emoji(alert['level'])} {alert['level']}: {alert['message']}")

        # Console output (always enabled)
        if self.config.ENABLE_CONSOLE:
            self._console_alert(forecast, alerts)

        # macOS notification
        if self.config.ENABLE_NOTIFICATION:
            self._macos_notification(forecast, alerts)

        # Email notification
        if self.config.ENABLE_EMAIL:
            self._email_alert(forecast, alerts)

    def _get_alert_emoji(self, level):
        """Get emoji for alert level"""
        return {
            'HIGH': '🔴',
            'MODERATE': '🟠',
            'INFO': 'ℹ️'
        }.get(level, 'ℹ️')

    def _console_alert(self, forecast, alerts):
        """Print detailed alert to console"""

        prob = forecast['final_probability']
        expected = forecast.get('expected_7day_mm', 0)
        confidence = forecast.get('confidence_level', 'MEDIUM')

        print("\n" + "─" * 80)
        print("FORECAST DETAILS")
        print("─" * 80)
        print(f"Probability: {prob:.1f}%")
        print(f"Confidence: {confidence}")
        print(f"Expected: {expected:.1f}mm ({expected/25.4:.1f} inches)")

        patterns = forecast.get('atmospheric_patterns', [])
        if patterns:
            print("\nPatterns:")
            for pattern in patterns:
                print(f"  • {pattern['pattern_type'].replace('_', ' ').title()}")
                print(f"    Confidence: {pattern['confidence']*100:.0f}%, Lead time: {pattern['lead_time_hours']}h")

    def _macos_notification(self, forecast, alerts):
        """Send macOS notification center alert"""

        try:
            # Get highest priority alert
            high_alerts = [a for a in alerts if a['level'] == 'HIGH']
            if high_alerts:
                title = "🔴 High Snow Probability Alert"
                message = high_alerts[0]['message']
            else:
                title = "❄️ Snow Forecast Update"
                message = alerts[0]['message']

            # Add forecast details
            prob = forecast['final_probability']
            message += f"\n\nProbability: {prob:.1f}%"

            # Send notification using osascript
            subprocess.run([
                'osascript', '-e',
                f'display notification "{message}" with title "{title}"'
            ], check=False)

            print("\n✅ macOS notification sent")

        except Exception as e:
            print(f"\n⚠️  Could not send macOS notification: {e}")

    def _email_alert(self, forecast, alerts):
        """Send email alert"""

        try:
            prob = forecast['final_probability']
            expected = forecast.get('expected_7day_mm', 0)
            confidence = forecast.get('confidence_level', 'MEDIUM')

            # Create email
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Snow Forecast Alert: {prob:.0f}% Probability"
            msg['From'] = self.config.EMAIL_FROM
            msg['To'] = self.config.EMAIL_TO

            # Plain text version
            text_body = f"""
Snow Forecast Alert
{'=' * 60}

FORECAST SUMMARY:
  Probability: {prob:.1f}%
  Confidence: {confidence}
  Expected 7-day: {expected:.1f}mm ({expected/25.4:.1f} inches)

ALERTS:
"""
            for alert in alerts:
                text_body += f"  {self._get_alert_emoji(alert['level'])} {alert['message']}\n"

            patterns = forecast.get('atmospheric_patterns', [])
            if patterns:
                text_body += "\nATMOSPHERIC PATTERNS:\n"
                for pattern in patterns:
                    text_body += f"  • {pattern['pattern_type'].replace('_', ' ').title()}\n"
                    text_body += f"    Confidence: {pattern['confidence']*100:.0f}%, Lead time: {pattern['lead_time_hours']}h\n"

            text_body += f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

            msg.attach(MIMEText(text_body, 'plain'))

            # Send email
            with smtplib.SMTP(self.config.EMAIL_SMTP_SERVER, self.config.EMAIL_SMTP_PORT) as server:
                server.starttls()
                if self.config.EMAIL_PASSWORD:
                    server.login(self.config.EMAIL_FROM, self.config.EMAIL_PASSWORD)
                server.send_message(msg)

            print("\n✅ Email alert sent")

        except Exception as e:
            print(f"\n⚠️  Could not send email alert: {e}")

    def _send_error_alert(self, error_message):
        """Send alert about forecast generation error"""

        print(f"\n❌ ERROR: {error_message}")

        if self.config.ENABLE_NOTIFICATION:
            try:
                subprocess.run([
                    'osascript', '-e',
                    f'display notification "{error_message}" with title "⚠️ Forecast Error"'
                ], check=False)
            except Exception:
                pass


def main():
    """Run automated forecast with alerting"""

    # Create config (can be customized)
    config = AlertConfig()

    # Run forecast
    runner = AutomatedForecastRunner(config)

    try:
        forecast = runner.run()
        print("\n" + "=" * 80)
        print("✅ AUTOMATED FORECAST COMPLETE")
        print("=" * 80)
        return forecast

    except Exception as e:
        print("\n" + "=" * 80)
        print(f"❌ FORECAST FAILED: {e}")
        print("=" * 80)
        raise


if __name__ == "__main__":
    main()
