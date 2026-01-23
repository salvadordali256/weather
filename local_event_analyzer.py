#!/usr/bin/env python3
"""
Local Event Detection System
Enhances the global model to catch regional/local snow events

Problem: Global predictors miss local events like:
- Alberta Clippers (fast-moving lows from Canada)
- Lake effect snow (Great Lakes moisture)
- Upper Midwest systems (regional development)

Solution: Add regional/local indicators:
1. Temperature gradients (cold air advection)
2. Regional snowfall patterns (Thunder Bay, Duluth, Marquette)
3. Storm track indicators (systems heading toward Wisconsin)
4. Lake effect setup (wind direction, lake temp differential)
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class LocalEventDetector:
    """Detect local/regional snow events without global precursors"""

    def __init__(self, db_path='demo_global_snowfall.db'):
        self.db_path = db_path

        # REGIONAL STATIONS - same region/nearby (NOW WITH REAL DATA!)
        self.regional_stations = {
            # Great Lakes / Upper Midwest
            'thunder_bay_on': {'weight': 0.30, 'lag': 0, 'name': 'Thunder Bay, ON', 'type': 'same_day'},

            # Canadian Prairie (Alberta Clipper source region) ✅ NOW AVAILABLE
            'winnipeg_mb': {'weight': 0.35, 'lag': 1, 'name': 'Winnipeg, MB', 'type': 'clipper'},

            # Great Lakes effect indicators ✅ NOW AVAILABLE
            'marquette_mi': {'weight': 0.30, 'lag': 0, 'name': 'Marquette, MI', 'type': 'lake_effect'},
            'duluth_mn': {'weight': 0.25, 'lag': 0, 'name': 'Duluth, MN', 'type': 'regional'},

            # Upper Midwest track ✅ NOW AVAILABLE
            'minneapolis_mn': {'weight': 0.20, 'lag': 0, 'name': 'Minneapolis, MN', 'type': 'regional'},
            'green_bay_wi': {'weight': 0.40, 'lag': 0, 'name': 'Green Bay, WI', 'type': 'local'},
            'iron_mountain_mi': {'weight': 0.35, 'lag': 0, 'name': 'Iron Mountain, MI', 'type': 'lake_effect'},
        }

        # PATTERN INDICATORS (things that suggest local events)
        self.pattern_indicators = {
            'clipper': {
                'description': 'Alberta Clipper - fast system from Canada',
                'indicators': [
                    'Thunder Bay active same day or 1 day before',
                    'Cold temperatures (< -5°C)',
                    'Rapid system movement',
                    'Light to moderate amounts (5-30mm typical)'
                ]
            },
            'lake_effect': {
                'description': 'Lake effect enhancement',
                'indicators': [
                    'Northwest winds (fetch across Lake Superior)',
                    'Cold air over warm lake',
                    'Thunder Bay active + local cold snap',
                    'Can produce 20-100mm in narrow bands'
                ]
            },
            'regional_low': {
                'description': 'Regional low pressure development',
                'indicators': [
                    'Thunder Bay quiet but local activity',
                    'Moisture from south/southwest',
                    'Can develop quickly',
                    'Moderate to heavy amounts (20-80mm)'
                ]
            }
        }

    def get_station_snow(self, station_id, days_ago=0, window=3):
        """Get snowfall for a station"""
        conn = sqlite3.connect(self.db_path)

        target_date = datetime.now() - timedelta(days=days_ago)
        start_date = target_date - timedelta(days=window-1)

        query = """
            SELECT AVG(snowfall_mm) as avg_snow, MAX(snowfall_mm) as max_snow
            FROM snowfall_daily
            WHERE station_id = ?
              AND date >= ?
              AND date <= ?
        """

        df = pd.read_sql_query(query, conn, params=(
            station_id,
            start_date.strftime('%Y-%m-%d'),
            target_date.strftime('%Y-%m-%d')
        ))

        conn.close()

        if df.empty or pd.isna(df.iloc[0]['avg_snow']):
            return 0.0, 0.0

        return df.iloc[0]['avg_snow'], df.iloc[0]['max_snow']

    def detect_alberta_clipper_setup(self):
        """Check for Alberta Clipper indicators"""

        print(f"\n{'─'*80}")
        print(f"ALBERTA CLIPPER DETECTION")
        print(f"{'─'*80}\n")

        # Check Winnipeg (clipper source region) - CRITICAL INDICATOR
        wpg_avg_yesterday, wpg_max_yesterday = self.get_station_snow('winnipeg_mb', 1, 1)
        wpg_avg_2days, wpg_max_2days = self.get_station_snow('winnipeg_mb', 2, 1)

        # Check Thunder Bay (if active recently, clipper may be coming)
        tb_avg_today, tb_max_today = self.get_station_snow('thunder_bay_on', 0, 1)
        tb_avg_yesterday, tb_max_yesterday = self.get_station_snow('thunder_bay_on', 1, 1)

        clipper_score = 0.0
        indicators = []

        # Indicator 1: Winnipeg activity (MOST IMPORTANT - clipper track!)
        if wpg_max_yesterday >= 15 or wpg_max_2days >= 15:
            clipper_score += 0.5
            indicators.append(f"✅ Winnipeg active ({max(wpg_max_yesterday, wpg_max_2days):.1f}mm) - CLIPPER TRACK!")
        elif wpg_max_yesterday >= 5 or wpg_max_2days >= 5:
            clipper_score += 0.3
            indicators.append(f"🟡 Winnipeg light snow ({max(wpg_max_yesterday, wpg_max_2days):.1f}mm)")
        else:
            indicators.append(f"⚪ Winnipeg quiet ({max(wpg_max_yesterday, wpg_max_2days):.1f}mm)")

        # Indicator 2: Thunder Bay activity today or yesterday
        if tb_max_today >= 10 or tb_max_yesterday >= 10:
            clipper_score += 0.3
            indicators.append(f"✅ Thunder Bay active ({max(tb_max_today, tb_max_yesterday):.1f}mm)")
        else:
            indicators.append(f"⚪ Thunder Bay quiet ({max(tb_max_today, tb_max_yesterday):.1f}mm)")

        # Indicator 2: Check Wisconsin recent trend (rapid onset suggests clipper)
        wi_today, _ = self.get_station_snow('phelps_wi', 0, 1)
        wi_yesterday, _ = self.get_station_snow('phelps_wi', 1, 1)

        if wi_today > wi_yesterday * 2 and wi_today > 5:
            clipper_score += 0.3
            indicators.append(f"✅ Rapid onset in Wisconsin (jumped from {wi_yesterday:.1f} to {wi_today:.1f}mm)")

        # Indicator 3: Pattern - clippers are frequent in winter
        current_month = datetime.now().month
        if current_month in [12, 1, 2]:  # Peak clipper season
            clipper_score += 0.2
            indicators.append(f"✅ Peak clipper season (Dec-Feb)")
        else:
            indicators.append(f"⚪ Outside peak season")

        print(f"Clipper Probability: {clipper_score * 100:.1f}%\n")
        for ind in indicators:
            print(f"  {ind}")

        return {
            'score': clipper_score,
            'probability': clipper_score * 100,
            'indicators': indicators
        }

    def detect_lake_effect_setup(self):
        """Check for lake effect snow indicators"""

        print(f"\n{'─'*80}")
        print(f"LAKE EFFECT SNOW DETECTION")
        print(f"{'─'*80}\n")

        lake_effect_score = 0.0
        indicators = []

        # Lake effect is tricky - needs:
        # 1. Northwest wind (fetch across Lake Superior)
        # 2. Cold air mass
        # 3. Warm lake water

        # We can infer some indicators from snow patterns

        # Indicator 1: Marquette getting snow (BEST lake effect indicator!)
        mq_avg, mq_max = self.get_station_snow('marquette_mi', 0, 1)
        if mq_max >= 20:
            lake_effect_score += 0.5
            indicators.append(f"✅ Marquette heavy snow (strong lake effect) - {mq_max:.1f}mm")
        elif mq_max >= 10:
            lake_effect_score += 0.3
            indicators.append(f"🟡 Marquette moderate snow (lake effect) - {mq_max:.1f}mm")
        elif mq_max >= 5:
            lake_effect_score += 0.2
            indicators.append(f"🟢 Marquette light snow - {mq_max:.1f}mm")
        else:
            indicators.append(f"⚪ Marquette quiet - {mq_max:.1f}mm")

        # Indicator 2: Thunder Bay getting snow (northwest flow)
        tb_avg, tb_max = self.get_station_snow('thunder_bay_on', 0, 1)
        if tb_max >= 5:
            lake_effect_score += 0.2
            indicators.append(f"✅ Thunder Bay active (northwest flow likely) - {tb_max:.1f}mm")
        else:
            indicators.append(f"⚪ Thunder Bay quiet - {tb_max:.1f}mm")

        # Indicator 2: Winter season (lake temperatures matter)
        current_month = datetime.now().month
        if current_month in [11, 12, 1]:  # Early winter (lake still warm)
            lake_effect_score += 0.3
            indicators.append(f"✅ Early winter - Lake Superior still retains heat")
        elif current_month in [2, 3]:  # Late winter (lake colder, less effect)
            lake_effect_score += 0.1
            indicators.append(f"⚪ Late winter - Lake effect diminishing")

        # Indicator 3: Pattern of narrow band (hard to detect without radar)
        # This would need wind data or multiple station comparison
        indicators.append(f"⚠️  Wind/fetch data not available (would improve detection)")

        print(f"Lake Effect Probability: {lake_effect_score * 100:.1f}%\n")
        for ind in indicators:
            print(f"  {ind}")

        return {
            'score': lake_effect_score,
            'probability': lake_effect_score * 100,
            'indicators': indicators
        }

    def detect_regional_system(self):
        """Check for regional low pressure systems"""

        print(f"\n{'─'*80}")
        print(f"REGIONAL SYSTEM DETECTION")
        print(f"{'─'*80}\n")

        regional_score = 0.0
        indicators = []

        # Regional systems: develop locally without strong global signals

        # Check if Wisconsin is getting snow without Thunder Bay
        wi_avg, wi_max = self.get_station_snow('phelps_wi', 0, 1)
        tb_avg, tb_max = self.get_station_snow('thunder_bay_on', 0, 1)

        if wi_max >= 10 and tb_max < 5:
            regional_score += 0.5
            indicators.append(f"✅ Wisconsin snow ({wi_max:.1f}mm) without Thunder Bay ({tb_max:.1f}mm)")
            indicators.append(f"   → Suggests regional/local development")
        elif wi_max >= 5:
            regional_score += 0.2
            indicators.append(f"⚪ Some Wisconsin snow ({wi_max:.1f}mm)")

        # Check for Rocky Mountain influence (steamboat as proxy)
        # Regional systems sometimes develop along track from Rockies
        sb_avg, sb_max = self.get_station_snow('steamboat_springs_co', 1, 2)
        if sb_max >= 20:
            regional_score += 0.3
            indicators.append(f"✅ Steamboat active ({sb_max:.1f}mm) - potential track from west")

        print(f"Regional System Probability: {regional_score * 100:.1f}%\n")
        for ind in indicators:
            print(f"  {ind}")

        return {
            'score': regional_score,
            'probability': regional_score * 100,
            'indicators': indicators
        }

    def run_local_detection(self):
        """Run all local event detection methods"""

        print(f"\n{'='*80}")
        print(f"LOCAL EVENT DETECTION SYSTEM")
        print(f"{'='*80}")
        print(f"Date: {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
        print(f"Purpose: Detect local/regional events without global precursors")
        print(f"{'='*80}")

        # Run all detection methods
        clipper = self.detect_alberta_clipper_setup()
        lake_effect = self.detect_lake_effect_setup()
        regional = self.detect_regional_system()

        # Combine scores
        print(f"\n{'='*80}")
        print(f"LOCAL EVENT PROBABILITY SUMMARY")
        print(f"{'='*80}\n")

        max_local_prob = max(clipper['probability'], lake_effect['probability'], regional['probability'])

        # Identify most likely event type
        if clipper['probability'] == max_local_prob and clipper['probability'] > 30:
            event_type = "Alberta Clipper"
            expected_amount = "5-30mm (light to moderate)"
            timeframe = "Next 0-24 hours"
        elif lake_effect['probability'] == max_local_prob and lake_effect['probability'] > 30:
            event_type = "Lake Effect Snow"
            expected_amount = "10-50mm (moderate, localized bands)"
            timeframe = "Duration of northwest flow"
        elif regional['probability'] == max_local_prob and regional['probability'] > 30:
            event_type = "Regional System"
            expected_amount = "10-40mm (moderate)"
            timeframe = "Next 12-36 hours"
        else:
            event_type = "None"
            expected_amount = "N/A"
            timeframe = "N/A"

        print(f"EVENT TYPE PROBABILITIES:")
        print(f"  Alberta Clipper:   {clipper['probability']:5.1f}%")
        print(f"  Lake Effect:       {lake_effect['probability']:5.1f}%")
        print(f"  Regional System:   {regional['probability']:5.1f}%")
        print(f"\nMost Likely: {event_type} ({max_local_prob:.1f}%)")

        if max_local_prob >= 50:
            alert_level = "🔴 HIGH"
        elif max_local_prob >= 30:
            alert_level = "🟡 MODERATE"
        elif max_local_prob >= 15:
            alert_level = "🟢 LOW"
        else:
            alert_level = "⚪ MINIMAL"

        print(f"Alert Level: {alert_level}")

        if max_local_prob >= 30:
            print(f"Expected Amount: {expected_amount}")
            print(f"Timeframe: {timeframe}")

        print(f"\n{'='*80}")
        print(f"RECOMMENDATIONS")
        print(f"{'='*80}\n")

        if max_local_prob >= 50:
            print(f"🔴 HIGH local event probability detected!")
            print(f"   • Check NWS forecast for confirmation")
            print(f"   • Monitor radar for development")
            print(f"   • Expect {expected_amount} snowfall")
        elif max_local_prob >= 30:
            print(f"🟡 MODERATE local event potential")
            print(f"   • Watch for rapid development")
            print(f"   • Check NWS updates")
            print(f"   • Local conditions favor {event_type}")
        else:
            print(f"⚪ Low local event probability")
            print(f"   • Rely on global pattern models")
            print(f"   • No strong local signals detected")

        print(f"\n{'='*80}\n")

        return {
            'clipper': clipper,
            'lake_effect': lake_effect,
            'regional': regional,
            'max_probability': max_local_prob,
            'likely_event_type': event_type,
            'alert_level': alert_level
        }


def main():
    detector = LocalEventDetector()
    result = detector.run_local_detection()


if __name__ == '__main__':
    main()
