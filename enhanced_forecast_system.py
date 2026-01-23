#!/usr/bin/env python3
"""
Enhanced Forecast System v4.0
Integrates atmospheric pattern detection with existing forecast models

New additions:
- Real-time GFS atmospheric data
- Alberta Clipper pre-detection (24-48 hour lead time)
- Lake effect setup detection
- Atmospheric confidence scoring
"""

import sys
sys.path.append('/Users/kyle.jurgens/weather')

from gfs_atmospheric_fetcher import GFSAtmosphericFetcher
from comprehensive_forecast_system import ComprehensiveForecastSystem
from datetime import datetime
import json


class EnhancedForecastSystem:
    """Enhanced forecast system with atmospheric pattern detection"""

    def __init__(self):
        self.atmospheric = GFSAtmosphericFetcher()
        self.base_system = ComprehensiveForecastSystem()

    def generate_forecast(self):
        """Generate comprehensive forecast with atmospheric enhancements"""

        print("=" * 80)
        print("ENHANCED FORECAST SYSTEM v4.0")
        print("With Real-Time Atmospheric Pattern Detection")
        print("=" * 80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Step 1: Detect atmospheric patterns (NEW)
        print("STEP 1: ATMOSPHERIC PATTERN DETECTION")
        print("─" * 80)
        atmospheric_patterns = self.atmospheric.analyze_patterns()
        print()

        # Step 2: Run base forecast models
        print("STEP 2: BASE FORECAST MODELS")
        print("─" * 80)
        forecast = self.base_system.run_comprehensive_forecast()
        print()

        # Step 3: Integrate atmospheric patterns
        print("STEP 3: ATMOSPHERIC INTEGRATION")
        print("─" * 80)
        enhanced_forecast = self._integrate_atmospheric_patterns(
            forecast,
            atmospheric_patterns
        )
        print()

        # Step 4: Final forecast
        print("=" * 80)
        print("FINAL ENHANCED FORECAST")
        print("=" * 80)
        self._display_forecast(enhanced_forecast)

        # Save to history
        self._save_forecast(enhanced_forecast)

        return enhanced_forecast

    def _integrate_atmospheric_patterns(self, base_forecast, patterns):
        """Integrate atmospheric patterns into base forecast"""

        enhanced = base_forecast.copy()
        enhanced['atmospheric_patterns'] = patterns
        enhanced['atmospheric_adjustments'] = []

        # Start with base probability
        original_prob = base_forecast['final_probability']
        adjusted_prob = original_prob

        print(f"Base forecast probability: {original_prob:.1f}%")

        if not patterns:
            print("No atmospheric patterns detected - using base forecast")
            enhanced['final_probability'] = adjusted_prob
            enhanced['confidence_level'] = base_forecast.get('confidence_level', 'MEDIUM')
            return enhanced

        # Process each detected pattern
        for pattern in patterns:
            pattern_type = pattern['pattern_type']
            confidence = pattern['confidence']
            lead_time = pattern['lead_time_hours']

            print(f"\nPattern detected: {pattern_type.upper()}")
            print(f"  Confidence: {confidence*100:.0f}%")
            print(f"  Lead time: {lead_time} hours")

            if pattern_type == 'alberta_clipper':
                # Alberta Clipper detected - boost probability if base is low
                # This catches systems that base models might miss
                boost = confidence * 20  # Up to 20% boost for high confidence

                # Only boost if base forecast is relatively low
                if original_prob < 40:
                    adjusted_prob = min(adjusted_prob + boost, 70)
                    enhanced['atmospheric_adjustments'].append({
                        'type': 'alberta_clipper_boost',
                        'boost': boost,
                        'reason': f'Alberta Clipper detected with {confidence*100:.0f}% confidence'
                    })
                    print(f"  → Probability boost: +{boost:.1f}% (clipper detection)")

                # Upgrade confidence level
                if confidence > 0.7:
                    enhanced['confidence_level'] = 'HIGH'
                    print(f"  → Confidence upgraded to HIGH")

            elif pattern_type == 'lake_effect':
                # Lake effect setup detected
                boost = confidence * 15  # Up to 15% boost

                if original_prob < 50:
                    adjusted_prob = min(adjusted_prob + boost, 65)
                    enhanced['atmospheric_adjustments'].append({
                        'type': 'lake_effect_boost',
                        'boost': boost,
                        'reason': f'Lake effect setup detected with {confidence*100:.0f}% confidence'
                    })
                    print(f"  → Probability boost: +{boost:.1f}% (lake effect setup)")

                # Increase expected amount (lake effect can enhance snowfall)
                if 'expected_7day_mm' in enhanced:
                    original_amount = enhanced['expected_7day_mm']
                    enhancement_factor = 1 + (confidence * 0.3)  # Up to 30% enhancement
                    enhanced['expected_7day_mm'] = original_amount * enhancement_factor
                    print(f"  → Expected amount: {original_amount:.1f}mm → {enhanced['expected_7day_mm']:.1f}mm")

        # Final probability
        enhanced['original_probability'] = original_prob
        enhanced['final_probability'] = adjusted_prob

        if adjusted_prob != original_prob:
            print(f"\n✅ Atmospheric adjustment: {original_prob:.1f}% → {adjusted_prob:.1f}%")
        else:
            print(f"\n✅ No adjustment needed - patterns confirm base forecast")

        return enhanced

    def _display_forecast(self, forecast):
        """Display the final enhanced forecast"""

        prob = forecast['final_probability']
        confidence = forecast.get('confidence_level', 'MEDIUM')
        event_type = forecast.get('event_type', 'UNKNOWN')
        expected = forecast.get('expected_7day_mm', 0)

        print(f"\n{'─'*80}")
        print(f"FORECAST SUMMARY")
        print(f"{'─'*80}")
        print(f"Probability: {prob:.1f}%")
        print(f"Confidence: {confidence}")
        print(f"Event Type: {event_type}")
        print(f"Expected 7-day: {expected:.1f}mm ({expected/25.4:.1f} inches)")

        # Risk level
        if prob < 20:
            risk = "⚪ LOW"
            desc = "Quiet pattern"
        elif prob < 40:
            risk = "🟡 LOW-MODERATE"
            desc = "Light snow possible"
        elif prob < 60:
            risk = "🟠 MODERATE"
            desc = "Snow likely"
        elif prob < 80:
            risk = "🔴 MODERATE-HIGH"
            desc = "Significant snow expected"
        else:
            risk = "🔴 HIGH"
            desc = "Major snow event expected"

        print(f"Risk Level: {risk}")
        print(f"Outlook: {desc}")

        # Atmospheric patterns
        patterns = forecast.get('atmospheric_patterns', [])
        if patterns:
            print(f"\n{'─'*80}")
            print(f"ATMOSPHERIC PATTERNS DETECTED")
            print(f"{'─'*80}")
            for pattern in patterns:
                print(f"\n{pattern['pattern_type'].upper().replace('_', ' ')}")
                print(f"  Confidence: {pattern['confidence']*100:.0f}%")
                print(f"  Lead time: {pattern['lead_time_hours']} hours")
                print(f"  Indicators:")
                for indicator in pattern['indicators']:
                    print(f"    • {indicator}")

        # Adjustments made
        adjustments = forecast.get('atmospheric_adjustments', [])
        if adjustments:
            print(f"\n{'─'*80}")
            print(f"ATMOSPHERIC ADJUSTMENTS")
            print(f"{'─'*80}")
            original = forecast.get('original_probability', prob)
            print(f"Original forecast: {original:.1f}%")
            for adj in adjustments:
                print(f"  • {adj['reason']}: +{adj['boost']:.1f}%")
            print(f"Final forecast: {prob:.1f}%")

        print(f"\n{'─'*80}")

    def _save_forecast(self, forecast):
        """Save forecast to history file"""
        try:
            # Load existing history
            try:
                with open('enhanced_forecast_history.json', 'r') as f:
                    history = json.load(f)
            except FileNotFoundError:
                history = []

            # Add new forecast
            forecast_record = {
                'timestamp': datetime.now().isoformat(),
                'probability': forecast['final_probability'],
                'original_probability': forecast.get('original_probability'),
                'confidence': forecast.get('confidence_level'),
                'event_type': forecast.get('event_type'),
                'expected_7day_mm': forecast.get('expected_7day_mm'),
                'atmospheric_patterns': [
                    {
                        'type': p['pattern_type'],
                        'confidence': p['confidence'],
                        'lead_time': p['lead_time_hours']
                    }
                    for p in forecast.get('atmospheric_patterns', [])
                ],
                'adjustments': forecast.get('atmospheric_adjustments', [])
            }

            history.append(forecast_record)

            # Save
            with open('enhanced_forecast_history.json', 'w') as f:
                json.dump(history, f, indent=2)

            print(f"✅ Forecast saved to enhanced_forecast_history.json")

        except Exception as e:
            print(f"⚠️  Warning: Could not save forecast history: {e}")


def main():
    """Run enhanced forecast"""
    system = EnhancedForecastSystem()
    forecast = system.generate_forecast()
    return forecast


if __name__ == "__main__":
    main()
