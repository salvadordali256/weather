#!/usr/bin/env python3
"""
Comprehensive Snowfall Forecast System v3.0
Combines GLOBAL and LOCAL event detection for complete coverage

Architecture:
┌─────────────────────────────────────────────────────────────┐
│  COMPREHENSIVE FORECAST SYSTEM v3.0                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐       ┌──────────────────┐          │
│  │  GLOBAL MODELS   │       │  LOCAL MODELS    │          │
│  │  (60% weight)    │       │  (40% weight)    │          │
│  ├──────────────────┤       ├──────────────────┤          │
│  │ • Pattern Match  │       │ • Clipper Detect │          │
│  │ • Asian/Euro     │       │ • Lake Effect    │          │
│  │ • Pacific        │       │ • Regional Low   │          │
│  │ • Jet Stream     │       │ • Thunder Bay    │          │
│  └──────────────────┘       └──────────────────┘          │
│           │                          │                      │
│           └──────────┬───────────────┘                      │
│                      ▼                                      │
│            ┌──────────────────┐                            │
│            │  EVENT CLASSIFIER │                            │
│            │  Determines type  │                            │
│            └──────────────────┘                            │
│                      │                                      │
│                      ▼                                      │
│            ┌──────────────────┐                            │
│            │  FINAL FORECAST  │                            │
│            │  With confidence │                            │
│            └──────────────────┘                            │
└─────────────────────────────────────────────────────────────┘
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Import our existing systems
import sys
sys.path.append('/Users/kyle.jurgens/weather')

class ComprehensiveForecastSystem:
    """Ultimate forecasting system - combines all detection methods"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # Model weights (adjusted based on event type)
        self.base_weights = {
            'pattern_matching': 0.30,      # Historical analogs
            'global_predictors': 0.30,     # Asian/Euro/Pacific signals
            'local_detectors': 0.40,       # Regional/clipper/lake effect
        }

    def run_global_models(self):
        """Run global-scale prediction models"""
        from integrated_forecast_system import IntegratedForecastSystem

        print(f"{'─'*80}")
        print(f"GLOBAL MODELS")
        print(f"{'─'*80}\n")

        print(f"Running integrated global forecast system...")
        system = IntegratedForecastSystem()

        # Get just the components we need
        pattern = system.run_pattern_matching()
        predictors = system.run_global_predictors()
        jetstream = system.run_jet_stream_analysis()

        # Calculate global score
        global_score = (
            (pattern['expected_7day_mm'] / 50.0) * 0.4 +  # Pattern matching
            predictors['adjusted_score'] * 0.4 +            # Global predictors
            (0.4 if jetstream['favorability'] == 'MODERATE-HIGH' else
             0.6 if jetstream['favorability'] == 'HIGH' else 0.2) * 0.2  # Jet stream
        )

        global_probability = min(global_score * 100, 100)

        print(f"✅ Global Models Complete")
        print(f"   Pattern Expected: {pattern['expected_7day_mm']:.1f}mm")
        print(f"   Predictor Score: {predictors['adjusted_score']:.1%}")
        print(f"   Jet Stream: {jetstream['favorability']}")
        print(f"   Global Probability: {global_probability:.1f}%\n")

        return {
            'probability': global_probability,
            'expected_amount': pattern['expected_7day_mm'],
            'pattern_result': pattern,
            'predictor_result': predictors,
            'jetstream_result': jetstream
        }

    def run_local_models(self):
        """Run local/regional detection models"""
        from local_event_analyzer import LocalEventDetector

        print(f"{'─'*80}")
        print(f"LOCAL/REGIONAL MODELS")
        print(f"{'─'*80}\n")

        print(f"Running local event detection...")
        detector = LocalEventDetector()
        result = detector.run_local_detection()

        print(f"✅ Local Models Complete")
        print(f"   Clipper: {result['clipper']['probability']:.1f}%")
        print(f"   Lake Effect: {result['lake_effect']['probability']:.1f}%")
        print(f"   Regional: {result['regional']['probability']:.1f}%")
        print(f"   Max Local Probability: {result['max_probability']:.1f}%\n")

        return result

    def classify_event_type(self, global_result, local_result):
        """Determine what type of event this is"""

        print(f"{'─'*80}")
        print(f"EVENT TYPE CLASSIFICATION")
        print(f"{'─'*80}\n")

        global_prob = global_result['probability']
        local_prob = local_result['max_probability']

        # Decision logic
        if global_prob >= 50 and local_prob >= 30:
            event_type = "HYBRID"
            description = "Large-scale pattern with local enhancement"
            confidence = "HIGH"
            weights = {'global': 0.6, 'local': 0.4}
        elif global_prob >= 50:
            event_type = "PATTERN-DRIVEN"
            description = "Large-scale atmospheric pattern event"
            confidence = "HIGH"
            weights = {'global': 0.8, 'local': 0.2}
        elif local_prob >= 40:
            event_type = "LOCAL/REGIONAL"
            description = f"{local_result['likely_event_type']} - local development"
            confidence = "MODERATE"
            weights = {'global': 0.2, 'local': 0.8}
        elif global_prob >= 30 or local_prob >= 30:
            event_type = "MIXED SIGNALS"
            description = "Some indicators present, not strong"
            confidence = "LOW-MODERATE"
            weights = {'global': 0.5, 'local': 0.5}
        else:
            event_type = "QUIET"
            description = "Minimal snow signals"
            confidence = "MODERATE"
            weights = {'global': 0.5, 'local': 0.5}

        print(f"Event Type: {event_type}")
        print(f"Description: {description}")
        print(f"Confidence: {confidence}")
        print(f"Weight Distribution: Global {weights['global']:.0%}, Local {weights['local']:.0%}\n")

        return {
            'type': event_type,
            'description': description,
            'confidence': confidence,
            'weights': weights
        }

    def generate_final_forecast(self, global_result, local_result, classification):
        """Combine all models into final forecast"""

        print(f"{'='*80}")
        print(f"FINAL INTEGRATED FORECAST")
        print(f"{'='*80}\n")

        # Weight by event type
        weights = classification['weights']

        final_probability = (
            global_result['probability'] * weights['global'] +
            local_result['max_probability'] * weights['local']
        )

        # Determine expected amount
        if classification['type'] == 'LOCAL/REGIONAL':
            # Local events typically lighter
            if local_result['likely_event_type'] == 'Alberta Clipper':
                expected_range = "5-30mm"
            elif local_result['likely_event_type'] == 'Lake Effect Snow':
                expected_range = "10-50mm (localized)"
            else:
                expected_range = "10-40mm"
        else:
            # Pattern-driven events
            expected_mm = global_result['expected_amount']
            expected_range = f"{expected_mm*0.7:.0f}-{expected_mm*1.3:.0f}mm"

        # Risk level
        if final_probability >= 70:
            risk_level = "🔴 HIGH"
            forecast = "Major snow event likely"
        elif final_probability >= 50:
            risk_level = "🟡 ELEVATED"
            forecast = "Significant snow probable"
        elif final_probability >= 30:
            risk_level = "🟢 MODERATE"
            forecast = "Snow possible"
        elif final_probability >= 15:
            risk_level = "⚪ LOW-MODERATE"
            forecast = "Light snow chance"
        else:
            risk_level = "⚪ LOW"
            forecast = "Minimal snow expected"

        print(f"FORECAST COMPONENTS:")
        print(f"  Global Models:     {global_result['probability']:5.1f}% × {weights['global']:.0%} = {global_result['probability']*weights['global']:.1f}%")
        print(f"  Local Models:      {local_result['max_probability']:5.1f}% × {weights['local']:.0%} = {local_result['max_probability']*weights['local']:.1f}%")
        print(f"  ───────────────────────────────────────")
        print(f"  FINAL PROBABILITY: {final_probability:.1f}%")

        print(f"\nRISK LEVEL: {risk_level}")
        print(f"FORECAST: {forecast}")
        print(f"EVENT TYPE: {classification['type']}")
        print(f"EXPECTED AMOUNT: {expected_range}")
        print(f"TIMEFRAME: Next 1-3 days")
        print(f"CONFIDENCE: {classification['confidence']}")

        print(f"\n{'─'*80}")
        print(f"DETAILED BREAKDOWN:")
        print(f"{'─'*80}\n")

        print(f"Event Classification: {classification['type']}")
        print(f"  {classification['description']}")

        if classification['type'] in ['LOCAL/REGIONAL', 'HYBRID']:
            print(f"\nLocal Event Indicators:")
            if local_result['clipper']['probability'] >= 30:
                print(f"  🔴 Alberta Clipper ({local_result['clipper']['probability']:.0f}%)")
            if local_result['lake_effect']['probability'] >= 30:
                print(f"  🔴 Lake Effect ({local_result['lake_effect']['probability']:.0f}%)")
            if local_result['regional']['probability'] >= 30:
                print(f"  🔴 Regional System ({local_result['regional']['probability']:.0f}%)")

        if classification['type'] in ['PATTERN-DRIVEN', 'HYBRID']:
            print(f"\nGlobal Pattern Signals:")
            if global_result['predictor_result']['active_regions'] > 0:
                print(f"  ✅ Active in {global_result['predictor_result']['active_regions']} region(s)")
            print(f"  Pattern Expected: {global_result['expected_amount']:.1f}mm (7-day)")

        print(f"\n{'='*80}")
        print(f"RECOMMENDATIONS:")
        print(f"{'='*80}\n")

        if final_probability >= 50:
            print(f"🔴 ALERT: Significant snow potential detected")
            print(f"   • Check NWS forecast for timing and amounts")
            print(f"   • Monitor weather radar for development")
            print(f"   • Prepare for {expected_range} snowfall")
            print(f"   • Event type: {classification['type']}")
        elif final_probability >= 30:
            print(f"🟡 WATCH: Moderate snow potential")
            print(f"   • Review NWS forecasts regularly")
            print(f"   • Local conditions favorable for {classification['type']}")
            print(f"   • Possible {expected_range} accumulation")
        else:
            print(f"⚪ QUIET: Low snow probability")
            print(f"   • Light snow or flurries possible")
            print(f"   • No major events expected")

        print(f"\n{'='*80}\n")

        return {
            'final_probability': final_probability,
            'risk_level': risk_level,
            'forecast': forecast,
            'event_type': classification['type'],
            'expected_range': expected_range,
            'confidence': classification['confidence']
        }

    def run_comprehensive_forecast(self):
        """Main forecast generation - runs everything"""

        print(f"\n{'='*80}")
        print(f"COMPREHENSIVE SNOWFALL FORECAST SYSTEM v3.0")
        print(f"{'='*80}")
        print(f"Target: Northern Wisconsin (Phelps, Land O'Lakes, Eagle River)")
        print(f"Generated: {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')}")
        print(f"{'='*80}\n")

        print(f"RUNNING FORECAST MODELS:")
        print(f"{'='*80}\n")

        # Step 1: Run global models
        global_result = self.run_global_models()

        # Step 2: Run local models
        local_result = self.run_local_models()

        # Step 3: Classify event type
        classification = self.classify_event_type(global_result, local_result)

        # Step 4: Generate final forecast
        final = self.generate_final_forecast(global_result, local_result, classification)

        # Summary
        print(f"{'='*80}")
        print(f"FORECAST SUMMARY")
        print(f"{'='*80}\n")

        print(f"📊 {final['risk_level']} - {final['forecast']}")
        print(f"📈 Probability: {final['final_probability']:.1f}%")
        print(f"❄️  Expected: {final['expected_range']}")
        print(f"🎯 Event Type: {final['event_type']}")
        print(f"✅ Confidence: {final['confidence']}")
        print(f"\n{'='*80}\n")

        return final


def main():
    system = ComprehensiveForecastSystem()
    result = system.run_comprehensive_forecast()


if __name__ == '__main__':
    main()
