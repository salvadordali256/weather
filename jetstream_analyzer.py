#!/usr/bin/env python3
"""
Jet Stream Analysis for Snowfall Prediction
Analyzes upper-level winds and flow patterns to determine if conditions
are favorable for transporting Pacific moisture to Wisconsin

Key Concepts:
- Jet stream = upper-level winds (250-300mb, ~30,000-35,000 ft)
- Zonal flow = west-to-east (brings Pacific moisture to Wisconsin)
- Meridional flow = north-south waves (can block or redirect)
- Trough over western US + ridge building east = snow setup for WI
- Polar jet position determines storm track
"""

import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json

class JetStreamAnalyzer:
    """Analyze jet stream patterns for Wisconsin snowfall potential"""

    def __init__(self):
        self.headers = {
            'User-Agent': '(Weather Analysis, kyle.jurgens@example.com)'
        }

        # Key analysis points
        self.analysis_points = {
            'pacific_coast': {'lat': 45.0, 'lon': -125.0, 'name': 'Pacific Coast'},
            'rockies': {'lat': 45.0, 'lon': -110.0, 'name': 'Rocky Mountains'},
            'plains': {'lat': 45.0, 'lon': -100.0, 'name': 'Great Plains'},
            'wisconsin': {'lat': 46.0, 'lon': -89.0, 'name': 'Northern Wisconsin'},
            'great_lakes': {'lat': 46.0, 'lon': -84.0, 'name': 'Great Lakes'}
        }

    def fetch_gfs_winds(self, lat, lon, level_mb=250):
        """
        Fetch GFS model wind data at specified pressure level

        Note: This is a simplified version. In production, you'd use:
        - NOAA NOMADS server for GFS GRIB2 data
        - Or pre-processed wind data from weather APIs
        """

        # For demonstration, we'll outline the approach
        # Real implementation would fetch from NOMADS or weather API

        print(f"Fetching {level_mb}mb winds for ({lat}, {lon})...")

        # Placeholder for actual API call
        # Real URL would be something like:
        # https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl

        # For now, return synthetic data structure
        return {
            'location': {'lat': lat, 'lon': lon},
            'level': level_mb,
            'timestamp': datetime.now().isoformat(),
            'forecast_hours': list(range(0, 168, 6)),  # 7 days, 6-hour intervals
            'note': 'Real implementation would fetch from NOAA NOMADS'
        }

    def analyze_flow_pattern(self):
        """
        Analyze current and forecast jet stream flow pattern

        Key patterns to identify:
        1. ZONAL (west-to-east): Good for Pacific moisture transport
        2. AMPLIFIED (big waves): Can bring extreme weather
        3. BLOCKING: Prevents moisture from reaching region
        4. PROGRESSIVE: Fast-moving systems
        """

        print(f"\n{'='*80}")
        print(f"JET STREAM PATTERN ANALYSIS")
        print(f"{'='*80}")
        print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target: Northern Wisconsin Snowfall Potential")
        print(f"{'='*80}\n")

        # In real implementation, would fetch actual wind data
        # For now, we'll use NWS text products that describe the pattern

        pattern_analysis = self.get_pattern_from_discussion()

        return pattern_analysis

    def get_pattern_from_discussion(self):
        """
        Fetch NWS Area Forecast Discussion which often describes
        jet stream and upper-level pattern
        """

        print("Fetching NWS Area Forecast Discussion (AFD)...")
        print("AFD contains meteorologist analysis of upper-level patterns\n")

        # NWS Green Bay (GRB) office covers northern Wisconsin
        office = "GRB"

        try:
            url = f"https://api.weather.gov/products/types/AFD/locations/{office}"

            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code != 200:
                print(f"❌ Failed to fetch AFD: HTTP {response.status_code}")
                return self.analyze_pattern_manually()

            data = response.json()

            # Get most recent discussion
            if '@graph' in data and len(data['@graph']) > 0:
                latest_product_url = data['@graph'][0]['@id']

                # Fetch the actual discussion text
                product_response = requests.get(latest_product_url, headers=self.headers, timeout=10)

                if product_response.status_code == 200:
                    product_data = product_response.json()
                    discussion_text = product_data.get('productText', '')

                    print(f"✅ Retrieved AFD from NWS {office}")
                    print(f"{'─'*80}\n")

                    # Extract key sections
                    analysis = self.parse_discussion(discussion_text)
                    return analysis

            print(f"⚠️  Could not retrieve discussion text, using manual analysis")
            return self.analyze_pattern_manually()

        except Exception as e:
            print(f"❌ Error fetching AFD: {e}")
            return self.analyze_pattern_manually()

    def parse_discussion(self, text):
        """Parse meteorologist discussion for jet stream info"""

        text_lower = text.lower()

        # Look for key jet stream indicators
        indicators = {
            'jet_stream_mentioned': any(word in text_lower for word in
                ['jet', 'jet stream', 'upper level', 'upper-level', '250mb', '300mb', '500mb']),

            'zonal_flow': any(word in text_lower for word in
                ['zonal', 'west to east', 'westerly flow', 'progressive']),

            'trough': any(word in text_lower for word in
                ['trough', 'troughing', 'upper low', 'shortwave']),

            'ridge': any(word in text_lower for word in
                ['ridge', 'ridging', 'upper high', 'high pressure aloft']),

            'pacific_moisture': any(word in text_lower for word in
                ['pacific', 'atmospheric river', 'moisture plume', 'tap', 'pineapple express']),

            'favorable_pattern': any(word in text_lower for word in
                ['favorable for', 'setup for', 'snow potential', 'wintry']),

            'blocking': any(word in text_lower for word in
                ['blocking', 'block', 'cutoff low', 'omega block']),
        }

        # Print relevant sections
        print("KEY METEOROLOGICAL INDICATORS FROM AFD:")
        print(f"{'─'*80}\n")

        for key, found in indicators.items():
            status = "✅" if found else "⚪"
            print(f"{status} {key.replace('_', ' ').title()}: {found}")

        # Determine pattern type
        if indicators['zonal_flow'] and indicators['pacific_moisture']:
            pattern_type = "ZONAL_PACIFIC"
            pattern_desc = "Zonal flow with Pacific moisture - FAVORABLE for WI snow"
            favorability = "HIGH"
        elif indicators['trough'] and indicators['favorable_pattern']:
            pattern_type = "AMPLIFIED_FAVORABLE"
            pattern_desc = "Amplified pattern with favorable setup"
            favorability = "MODERATE-HIGH"
        elif indicators['blocking']:
            pattern_type = "BLOCKED"
            pattern_desc = "Blocking pattern - moisture may not reach WI"
            favorability = "LOW"
        elif indicators['zonal_flow']:
            pattern_type = "ZONAL"
            pattern_desc = "Zonal flow - progressive systems possible"
            favorability = "MODERATE"
        else:
            pattern_type = "UNCLEAR"
            pattern_desc = "Pattern unclear from discussion"
            favorability = "UNKNOWN"

        print(f"\n{'─'*80}")
        print(f"PATTERN ASSESSMENT:")
        print(f"{'─'*80}\n")
        print(f"Pattern Type: {pattern_type}")
        print(f"Description: {pattern_desc}")
        print(f"Favorability for WI Snow: {favorability}")

        # Show excerpt with jet stream mentions
        if indicators['jet_stream_mentioned']:
            print(f"\n{'─'*80}")
            print(f"RELEVANT EXCERPTS (jet stream mentions):")
            print(f"{'─'*80}\n")

            lines = text.split('\n')
            for i, line in enumerate(lines):
                if any(word in line.lower() for word in ['jet', '250', '300', '500', 'upper']):
                    print(f"  {line.strip()[:120]}")
                    if i < len(lines) - 1:
                        print(f"  {lines[i+1].strip()[:120]}\n")
                    if i > 5:  # Limit to first few mentions
                        break

        return {
            'pattern_type': pattern_type,
            'favorability': favorability,
            'indicators': indicators,
            'description': pattern_desc
        }

    def analyze_pattern_manually(self):
        """Manual pattern analysis when AFD not available"""

        print(f"\n{'─'*80}")
        print(f"MANUAL PATTERN ANALYSIS")
        print(f"{'─'*80}\n")
        print("Using simplified pattern recognition...")
        print("(In production, would analyze GFS/ECMWF wind fields)")

        # This is a placeholder - real implementation would analyze model data
        return {
            'pattern_type': 'MANUAL',
            'favorability': 'UNKNOWN',
            'description': 'Manual analysis mode - requires model data'
        }

    def get_historical_favorable_patterns(self):
        """
        Identify jet stream patterns that preceded major Wisconsin snow events
        This would require historical reanalysis data (NCEP/NCAR reanalysis)
        """

        print(f"\n{'='*80}")
        print(f"HISTORICAL FAVORABLE JET STREAM PATTERNS")
        print(f"{'='*80}\n")

        # Based on climatology and historical analysis
        favorable_patterns = [
            {
                'name': 'Pacific Tap',
                'description': 'Deep trough over western US, zonal flow into Great Lakes',
                'setup': 'Pacific moisture + westerly jet directly into Wisconsin',
                'frequency': 'Common (30-40% of major events)',
                'typical_amounts': '50-150mm',
                'lead_time': '2-4 days'
            },
            {
                'name': 'Miller Type A',
                'description': 'Low pressure from Colorado, tracks northeast toward Great Lakes',
                'setup': 'Southern jet stream brings Gulf moisture, phases with northern jet',
                'frequency': 'Common (25-35% of major events)',
                'typical_amounts': '75-200mm',
                'lead_time': '3-5 days'
            },
            {
                'name': 'Alberta Clipper Enhanced',
                'description': 'Fast-moving low from Alberta picks up Great Lakes moisture',
                'setup': 'Northern jet stream, lake effect enhancement',
                'frequency': 'Moderate (15-25% of major events)',
                'typical_amounts': '30-100mm',
                'lead_time': '1-2 days'
            },
            {
                'name': 'Phased System',
                'description': 'Two upper-level systems phase (combine) over Wisconsin',
                'setup': 'Pacific shortwave + southern stream energy converge',
                'frequency': 'Less common but intense (10-20% of major events)',
                'typical_amounts': '100-250mm',
                'lead_time': '3-6 days'
            },
            {
                'name': 'Omega Block Backside',
                'description': 'Block to east, trough digs in on west side',
                'setup': 'Slow-moving, prolonged snow event',
                'frequency': 'Rare but extreme (5-10%)',
                'typical_amounts': '100-300mm',
                'lead_time': '5-10 days'
            }
        ]

        for i, pattern in enumerate(favorable_patterns, 1):
            print(f"{i}. {pattern['name']}")
            print(f"   Description: {pattern['description']}")
            print(f"   Setup: {pattern['setup']}")
            print(f"   Frequency: {pattern['frequency']}")
            print(f"   Typical Amounts: {pattern['typical_amounts']}")
            print(f"   Lead Time: {pattern['lead_time']}\n")

        return favorable_patterns

    def combine_with_snow_predictors(self, pattern_analysis, snow_predictors):
        """
        Combine jet stream analysis with our existing snow predictor signals

        Logic:
        - High snow predictor signals + favorable jet stream = HIGH confidence
        - High snow predictor signals + unfavorable jet stream = FALSE POSITIVE (filter out)
        - Low snow predictor signals + favorable jet stream = WATCH (potential)
        - Low snow predictor signals + unfavorable jet stream = LOW confidence
        """

        print(f"\n{'='*80}")
        print(f"INTEGRATED FORECAST: JET STREAM + SNOW PREDICTORS")
        print(f"{'='*80}\n")

        # Snow predictor input (from major_event_predictor.py)
        print(f"SNOW PREDICTOR SIGNALS:")
        print(f"  Mt Baker: {snow_predictors.get('mt_baker', 0):.1f}mm")
        print(f"  Lake Tahoe: {snow_predictors.get('lake_tahoe', 0):.1f}mm")
        print(f"  Thunder Bay: {snow_predictors.get('thunder_bay', 0):.1f}mm")
        print(f"  Sapporo: {snow_predictors.get('sapporo', 0):.1f}mm")
        print(f"  Raw Probability: {snow_predictors.get('probability', 0):.1f}%\n")

        # Jet stream analysis
        print(f"JET STREAM PATTERN:")
        print(f"  Type: {pattern_analysis.get('pattern_type', 'UNKNOWN')}")
        print(f"  Favorability: {pattern_analysis.get('favorability', 'UNKNOWN')}")
        print(f"  Description: {pattern_analysis.get('description', 'N/A')}\n")

        # Integration logic
        raw_probability = snow_predictors.get('probability', 0)
        favorability = pattern_analysis.get('favorability', 'UNKNOWN')

        # Adjust probability based on jet stream
        if favorability == 'HIGH':
            multiplier = 1.2
            adjustment = "✅ ENHANCING"
        elif favorability == 'MODERATE-HIGH':
            multiplier = 1.1
            adjustment = "✅ SLIGHTLY ENHANCING"
        elif favorability == 'MODERATE':
            multiplier = 1.0
            adjustment = "⚪ NEUTRAL"
        elif favorability == 'LOW':
            multiplier = 0.3
            adjustment = "❌ SUPPRESSING"
        else:
            multiplier = 0.7
            adjustment = "⚠️  UNCERTAIN"

        adjusted_probability = min(raw_probability * multiplier, 100.0)

        print(f"{'─'*80}")
        print(f"INTEGRATED ASSESSMENT:")
        print(f"{'─'*80}\n")
        print(f"Raw Snow Signal Probability: {raw_probability:.1f}%")
        print(f"Jet Stream Adjustment: {adjustment} (×{multiplier:.1f})")
        print(f"FINAL PROBABILITY: {adjusted_probability:.1f}%\n")

        # Final forecast
        if adjusted_probability >= 70:
            final_forecast = "🔴 HIGH CONFIDENCE - Major event likely"
            confidence = "HIGH"
        elif adjusted_probability >= 50:
            final_forecast = "🟡 MODERATE-HIGH - Significant snow probable"
            confidence = "MODERATE-HIGH"
        elif adjusted_probability >= 30:
            final_forecast = "🟢 MODERATE - Snow possible"
            confidence = "MODERATE"
        elif adjusted_probability >= 15:
            final_forecast = "⚪ LOW-MODERATE - Background chance"
            confidence = "LOW-MODERATE"
        else:
            final_forecast = "⚪ LOW - Unlikely"
            confidence = "LOW"

        print(f"FORECAST: {final_forecast}")
        print(f"Confidence: {confidence}\n")

        # Explanation
        print(f"{'─'*80}")
        print(f"EXPLANATION:")
        print(f"{'─'*80}\n")

        if multiplier < 0.5 and raw_probability > 70:
            print(f"⚠️  FALSE POSITIVE FILTER ACTIVATED")
            print(f"Snow predictor signals are strong (Pacific moisture available)")
            print(f"BUT jet stream pattern is unfavorable (won't transport to WI)")
            print(f"This explains our earlier 85% prediction vs NWS quiet forecast!\n")
        elif multiplier > 1.0 and raw_probability > 30:
            print(f"✅ CONFIDENCE BOOST")
            print(f"Both snow predictors AND jet stream pattern are favorable")
            print(f"Multiple independent signals agree - high confidence forecast\n")
        elif multiplier == 1.0:
            print(f"⚪ NEUTRAL PATTERN")
            print(f"Jet stream neither enhances nor suppresses the snow signal")
            print(f"Rely primarily on snow predictor signals\n")
        else:
            print(f"Pattern analysis provides context for snow predictor signals\n")

        print(f"{'='*80}\n")

        return {
            'raw_probability': raw_probability,
            'adjusted_probability': adjusted_probability,
            'multiplier': multiplier,
            'adjustment': adjustment,
            'forecast': final_forecast,
            'confidence': confidence,
            'pattern': pattern_analysis
        }


def main():
    analyzer = JetStreamAnalyzer()

    # Step 1: Analyze current jet stream pattern
    pattern = analyzer.analyze_flow_pattern()

    # Step 2: Show historical favorable patterns
    historical = analyzer.get_historical_favorable_patterns()

    # Step 3: Integrate with snow predictors
    # (Use values from our earlier major_event_predictor run)
    snow_signals = {
        'mt_baker': 26.2,
        'lake_tahoe': 23.7,
        'thunder_bay': 6.2,
        'sapporo': 5.9,
        'probability': 85.5  # Raw probability from major_event_predictor
    }

    integrated = analyzer.combine_with_snow_predictors(pattern, snow_signals)

    # Final summary
    print(f"{'='*80}")
    print(f"FINAL SUMMARY")
    print(f"{'='*80}\n")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: Northern Wisconsin\n")
    print(f"Snow Predictor Signals: Strong Pacific moisture (Mt Baker, Lake Tahoe)")
    print(f"Jet Stream Pattern: {pattern.get('pattern_type', 'UNKNOWN')}")
    print(f"Final Assessment: {integrated['forecast']}")
    print(f"Probability: {integrated['adjusted_probability']:.1f}%\n")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()
