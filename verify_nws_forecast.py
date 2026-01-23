#!/usr/bin/env python3
"""
NWS Forecast Verification
Fetches current NWS forecast for northern Wisconsin and compares with our model
"""

import requests
import json
from datetime import datetime
import re

class NWSForecastVerifier:
    """Fetch and compare NWS forecasts with our predictions"""

    def __init__(self):
        self.headers = {
            'User-Agent': '(Weather Analysis System, kyle.jurgens@example.com)',
            'Accept': 'application/geo+json'
        }

        # Northern Wisconsin locations
        self.locations = {
            'Phelps': {'lat': 46.06, 'lon': -89.08},
            'Land O Lakes': {'lat': 46.15, 'lon': -89.34},
            'Eagle River': {'lat': 45.92, 'lon': -89.24}
        }

    def get_forecast(self, lat, lon, location_name):
        """Get NWS forecast for a location"""

        print(f"\n{'─'*80}")
        print(f"Fetching NWS forecast for {location_name} ({lat}, {lon})")
        print(f"{'─'*80}")

        try:
            # Step 1: Get grid point
            points_url = f"https://api.weather.gov/points/{lat},{lon}"
            print(f"Getting grid point: {points_url}")

            response = requests.get(points_url, headers=self.headers, timeout=10)

            if response.status_code != 200:
                print(f"❌ Failed to get grid point: HTTP {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return None

            points_data = response.json()

            # Extract forecast URL
            forecast_url = points_data['properties']['forecast']
            forecast_hourly_url = points_data['properties']['forecastHourly']
            office = points_data['properties']['cwa']
            grid_x = points_data['properties']['gridX']
            grid_y = points_data['properties']['gridY']

            print(f"✅ Grid point: {office} ({grid_x}, {grid_y})")
            print(f"Forecast URL: {forecast_url}")

            # Step 2: Get detailed forecast
            print(f"\nFetching detailed forecast...")
            forecast_response = requests.get(forecast_url, headers=self.headers, timeout=10)

            if forecast_response.status_code != 200:
                print(f"❌ Failed to get forecast: HTTP {forecast_response.status_code}")
                return None

            forecast_data = forecast_response.json()

            print(f"✅ Forecast retrieved successfully")

            # Get update time if available
            updated = forecast_data['properties'].get('updated',
                     forecast_data['properties'].get('updateTime',
                     datetime.now().isoformat()))

            return {
                'location': location_name,
                'office': office,
                'forecast_url': forecast_url,
                'forecast_periods': forecast_data['properties']['periods'],
                'updated': updated
            }

        except Exception as e:
            print(f"❌ Error fetching forecast: {e}")
            return None

    def extract_snow_info(self, forecast_text):
        """Extract snow information from forecast text"""

        text_lower = forecast_text.lower()

        # Check for snow mentions
        has_snow = any(word in text_lower for word in [
            'snow', 'flurries', 'squalls', 'accumulation', 'inches'
        ])

        if not has_snow:
            return {
                'has_snow': False,
                'amount': None,
                'confidence': 'none'
            }

        # Try to extract amounts
        # Look for patterns like "3 to 5 inches", "around 2 inches", "1 inch"
        amount_patterns = [
            r'(\d+)\s*to\s*(\d+)\s*inch',
            r'around\s*(\d+)\s*inch',
            r'up\s*to\s*(\d+)\s*inch',
            r'(\d+)\s*inch',
        ]

        amounts = []
        for pattern in amount_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                if isinstance(matches[0], tuple):
                    # Range like "3 to 5"
                    amounts.extend([int(m) for m in matches[0]])
                else:
                    amounts.append(int(matches[0]))

        if amounts:
            min_amount = min(amounts)
            max_amount = max(amounts)
            avg_amount = sum(amounts) / len(amounts)
        else:
            min_amount = None
            max_amount = None
            avg_amount = None

        # Classify severity
        if max_amount and max_amount >= 6:
            severity = 'MAJOR'
        elif max_amount and max_amount >= 3:
            severity = 'SIGNIFICANT'
        elif max_amount and max_amount >= 1:
            severity = 'MODERATE'
        elif has_snow:
            severity = 'LIGHT'
        else:
            severity = 'NONE'

        return {
            'has_snow': has_snow,
            'min_inches': min_amount,
            'max_inches': max_amount,
            'avg_inches': avg_amount,
            'severity': severity,
            'raw_text': forecast_text
        }

    def analyze_forecast(self, forecast_data):
        """Analyze forecast periods for snow"""

        print(f"\n{'='*80}")
        print(f"NWS FORECAST ANALYSIS - {forecast_data['location']}")
        print(f"{'='*80}")
        print(f"Forecast Office: {forecast_data['office']}")
        print(f"Updated: {forecast_data['updated']}")
        print(f"{'='*80}\n")

        periods = forecast_data['forecast_periods']

        total_snow_inches = 0
        snow_periods = []

        print(f"FORECAST PERIODS (Next 7 days):")
        print(f"{'─'*80}\n")

        for i, period in enumerate(periods[:14]):  # Next 7 days (14 periods = day/night)
            name = period['name']
            temp = period['temperature']
            temp_unit = period['temperatureUnit']
            short = period['shortForecast']
            detailed = period['detailedForecast']

            snow_info = self.extract_snow_info(detailed)

            if snow_info['has_snow']:
                marker = "❄️"
                snow_periods.append({
                    'period': name,
                    'snow_info': snow_info
                })

                if snow_info['max_inches']:
                    total_snow_inches += snow_info['avg_inches'] or snow_info['max_inches']
            else:
                marker = "⚪"

            print(f"{marker} {name:20s} | {temp:3d}°{temp_unit} | {short}")

            if snow_info['has_snow']:
                if snow_info['max_inches']:
                    if snow_info['min_inches'] and snow_info['min_inches'] != snow_info['max_inches']:
                        amount_str = f"{snow_info['min_inches']}-{snow_info['max_inches']} inches"
                    else:
                        amount_str = f"{snow_info['max_inches']} inches"
                    print(f"   └─> Expected: {amount_str} | Severity: {snow_info['severity']}")
                else:
                    print(f"   └─> {snow_info['severity']} snow mentioned")

        # Summary
        print(f"\n{'='*80}")
        print(f"FORECAST SUMMARY")
        print(f"{'='*80}\n")

        if snow_periods:
            print(f"Snow Periods: {len(snow_periods)}")
            print(f"Total Expected: ~{total_snow_inches:.1f} inches ({total_snow_inches * 25.4:.1f}mm)")

            max_severity = max([sp['snow_info']['severity'] for sp in snow_periods],
                             key=lambda x: ['NONE', 'LIGHT', 'MODERATE', 'SIGNIFICANT', 'MAJOR'].index(x))

            print(f"Maximum Severity: {max_severity}")

            print(f"\nSnow Timeline:")
            for sp in snow_periods:
                print(f"  • {sp['period']:20s} - {sp['snow_info']['severity']}")

        else:
            print(f"No snow in forecast")
            max_severity = 'NONE'
            total_snow_inches = 0

        return {
            'has_snow': len(snow_periods) > 0,
            'snow_periods': snow_periods,
            'total_inches': total_snow_inches,
            'total_mm': total_snow_inches * 25.4,
            'max_severity': max_severity
        }

    def compare_with_model(self, nws_summary, model_prediction):
        """Compare NWS forecast with our model prediction"""

        print(f"\n{'='*80}")
        print(f"MODEL vs NWS COMPARISON")
        print(f"{'='*80}\n")

        print(f"OUR MODEL PREDICTION:")
        print(f"  Risk Level: {model_prediction['risk_level']}")
        print(f"  Probability: {model_prediction['probability']:.1f}%")
        print(f"  Timeframe: 0-3 days")
        print(f"  Expected: Major event (≥50mm / ≥2 inches)")

        print(f"\nNWS FORECAST:")
        print(f"  Has Snow: {'YES' if nws_summary['has_snow'] else 'NO'}")
        print(f"  Total Expected: {nws_summary['total_inches']:.1f} inches ({nws_summary['total_mm']:.1f}mm)")
        print(f"  Max Severity: {nws_summary['max_severity']}")
        print(f"  Periods with Snow: {len(nws_summary['snow_periods'])}")

        print(f"\n{'─'*80}")
        print(f"VERIFICATION RESULT:")
        print(f"{'─'*80}\n")

        # Compare
        model_predicts_major = model_prediction['probability'] >= 70
        nws_predicts_major = nws_summary['max_severity'] in ['MAJOR', 'SIGNIFICANT']

        if model_predicts_major and nws_predicts_major:
            result = "✅ AGREEMENT - Both predict major snow event"
            agreement = "HIGH"
        elif model_predicts_major and nws_summary['has_snow']:
            result = "⚠️  PARTIAL AGREEMENT - Model predicts major, NWS predicts some snow"
            agreement = "MODERATE"
        elif model_predicts_major and not nws_summary['has_snow']:
            result = "❌ DISAGREEMENT - Model predicts major, NWS shows no snow"
            agreement = "LOW"
        elif not model_predicts_major and nws_predicts_major:
            result = "❌ DISAGREEMENT - NWS predicts major, model shows low risk"
            agreement = "LOW"
        else:
            result = "✅ AGREEMENT - Both predict quiet/light conditions"
            agreement = "HIGH"

        print(f"{result}")
        print(f"Agreement Level: {agreement}")

        # Detailed comparison
        if nws_summary['total_mm'] >= 50:
            nws_category = "MAJOR EVENT (≥50mm)"
        elif nws_summary['total_mm'] >= 20:
            nws_category = "SIGNIFICANT (20-50mm)"
        elif nws_summary['total_mm'] >= 5:
            nws_category = "LIGHT (5-20mm)"
        else:
            nws_category = "TRACE/NONE (<5mm)"

        model_category = "MAJOR EVENT (≥50mm)" if model_prediction['probability'] >= 70 else \
                        "MODERATE RISK" if model_prediction['probability'] >= 30 else \
                        "LOW RISK"

        print(f"\nModel Category: {model_category}")
        print(f"NWS Category: {nws_category}")

        print(f"\n{'='*80}\n")

        return {
            'agreement': agreement,
            'result': result,
            'model_category': model_category,
            'nws_category': nws_category
        }

    def run_verification(self):
        """Main verification process"""

        print(f"\n{'='*80}")
        print(f"NWS FORECAST VERIFICATION SYSTEM")
        print(f"{'='*80}")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")

        # Our model prediction (from major_event_predictor.py output)
        model_prediction = {
            'risk_level': '🔴 HIGH RISK - Major event likely within 0-3 days',
            'probability': 85.5,
            'active_signals': 5,
            'key_indicators': ['Mt Baker 26.2mm', 'Lake Tahoe 23.7mm']
        }

        # Get NWS forecasts for all locations
        all_forecasts = []

        for location_name, coords in self.locations.items():
            forecast = self.get_forecast(coords['lat'], coords['lon'], location_name)

            if forecast:
                summary = self.analyze_forecast(forecast)
                all_forecasts.append({
                    'location': location_name,
                    'summary': summary
                })

        # Compare with model
        if all_forecasts:
            print(f"\n{'='*80}")
            print(f"OVERALL VERIFICATION")
            print(f"{'='*80}\n")

            # Use the forecast with the most snow
            max_forecast = max(all_forecasts, key=lambda x: x['summary']['total_mm'])

            comparison = self.compare_with_model(max_forecast['summary'], model_prediction)

            return {
                'model': model_prediction,
                'nws_forecasts': all_forecasts,
                'comparison': comparison
            }


def main():
    verifier = NWSForecastVerifier()
    result = verifier.run_verification()


if __name__ == '__main__':
    main()
