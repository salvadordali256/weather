#!/usr/bin/env python3
"""
GFS Atmospheric Data Fetcher
Fetches real-time atmospheric conditions from NOAA GFS model via Open-Meteo
For detecting weather systems BEFORE they produce snowfall

Key atmospheric indicators:
- 500mb height (mid-level steering patterns)
- 250mb winds (jet stream position/speed)
- Surface pressure (low pressure systems)
- Temperature gradients (cold air advection)
- Precipitable water (moisture availability)
"""

import requests
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json


class GFSAtmosphericFetcher:
    """Fetch atmospheric data from Open-Meteo's GFS forecast API"""

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Key locations for atmospheric monitoring
    MONITORING_LOCATIONS = {
        # Target region
        'eagle_river': {'lat': 45.92, 'lon': -89.24, 'name': 'Eagle River, WI'},

        # Alberta Clipper source region
        'alberta': {'lat': 55.0, 'lon': -115.0, 'name': 'Alberta, Canada'},
        'winnipeg': {'lat': 49.90, 'lon': -97.14, 'name': 'Winnipeg, MB'},

        # Great Lakes moisture sources
        'superior_west': {'lat': 47.5, 'lon': -88.0, 'name': 'Western Lake Superior'},
        'superior_east': {'lat': 47.5, 'lon': -86.0, 'name': 'Eastern Lake Superior'},

        # Regional flow indicators
        'north_plains': {'lat': 48.0, 'lon': -100.0, 'name': 'Northern Plains'},
        'upper_midwest': {'lat': 46.0, 'lon': -92.0, 'name': 'Upper Midwest'},
    }

    def __init__(self, db_path: str = "atmospheric_data.db"):
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize database schema for atmospheric data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Table for atmospheric conditions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS atmospheric_conditions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location TEXT NOT NULL,
                forecast_time TEXT NOT NULL,
                valid_time TEXT NOT NULL,

                -- Surface conditions
                surface_pressure_hpa REAL,
                temperature_2m_c REAL,
                wind_speed_10m_kmh REAL,
                wind_direction_10m_deg REAL,

                -- Upper air (if available from model)
                geopotential_height_500mb_m REAL,
                temperature_500mb_c REAL,
                wind_speed_250mb_kmh REAL,
                wind_direction_250mb_deg REAL,

                -- Moisture and precipitation
                precipitable_water_mm REAL,
                precipitation_mm REAL,
                snowfall_cm REAL,

                -- Computed indicators
                temperature_gradient_c REAL,
                pressure_tendency_hpa REAL,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(location, forecast_time, valid_time)
            )
        """)

        # Table for detected atmospheric patterns
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS atmospheric_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detection_time TEXT NOT NULL,
                pattern_type TEXT NOT NULL,
                confidence REAL,
                lead_time_hours INTEGER,
                description TEXT,
                indicators TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()

    def fetch_atmospheric_data(self, hours_ahead: int = 72) -> Dict:
        """
        Fetch atmospheric forecast data for key locations

        Args:
            hours_ahead: How many hours ahead to fetch (default 72 = 3 days)

        Returns:
            Dictionary with atmospheric data by location
        """
        results = {}
        forecast_time = datetime.utcnow().isoformat()

        for loc_key, loc_info in self.MONITORING_LOCATIONS.items():
            print(f"Fetching atmospheric data for {loc_info['name']}...")

            params = {
                'latitude': loc_info['lat'],
                'longitude': loc_info['lon'],
                'hourly': [
                    'temperature_2m',
                    'surface_pressure',
                    'precipitation',
                    'snowfall',
                    'windspeed_10m',
                    'winddirection_10m',
                    'precipitation_probability',
                ],
                'temperature_unit': 'celsius',
                'windspeed_unit': 'kmh',
                'precipitation_unit': 'mm',
                'forecast_days': min(7, (hours_ahead // 24) + 1),
            }

            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Store in database
                self._store_atmospheric_data(loc_key, forecast_time, data)
                results[loc_key] = data

            except Exception as e:
                print(f"  ⚠️  Error fetching {loc_info['name']}: {e}")
                results[loc_key] = None

        return results

    def _store_atmospheric_data(self, location: str, forecast_time: str, data: Dict):
        """Store atmospheric data in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        hourly = data.get('hourly', {})
        times = hourly.get('time', [])

        for i, valid_time in enumerate(times):
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO atmospheric_conditions
                    (location, forecast_time, valid_time,
                     surface_pressure_hpa, temperature_2m_c,
                     wind_speed_10m_kmh, wind_direction_10m_deg,
                     precipitation_mm, snowfall_cm)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    location,
                    forecast_time,
                    valid_time,
                    hourly.get('surface_pressure', [None] * len(times))[i],
                    hourly.get('temperature_2m', [None] * len(times))[i],
                    hourly.get('windspeed_10m', [None] * len(times))[i],
                    hourly.get('winddirection_10m', [None] * len(times))[i],
                    hourly.get('precipitation', [None] * len(times))[i],
                    hourly.get('snowfall', [None] * len(times))[i],
                ))
            except Exception as e:
                print(f"  Warning: Could not store data point: {e}")

        conn.commit()
        conn.close()

    def detect_alberta_clipper(self, data: Dict) -> Optional[Dict]:
        """
        Detect Alberta Clipper formation indicators

        Signature:
        - Low pressure developing in Alberta
        - Strong NW winds (300-330 degrees)
        - Temperature drop
        - Moving southeast toward Great Lakes

        Returns:
            Detection result with confidence and lead time
        """
        alberta_data = data.get('alberta')
        winnipeg_data = data.get('winnipeg')

        if not alberta_data or not winnipeg_data:
            return None

        # Check for clipper indicators
        indicators = []
        confidence = 0.0

        # 1. Pressure drops in Alberta
        alberta_hourly = alberta_data.get('hourly', {})
        pressures = alberta_hourly.get('surface_pressure', [])
        if len(pressures) >= 24:
            pressure_drop = pressures[0] - min(pressures[0:24])
            if pressure_drop > 5:  # 5 hPa drop in 24 hours
                indicators.append(f"Pressure falling {pressure_drop:.1f} hPa in Alberta")
                confidence += 0.25

        # 2. Strong NW winds expected
        winds_speed = alberta_hourly.get('windspeed_10m', [])
        winds_dir = alberta_hourly.get('winddirection_10m', [])
        for i in range(min(48, len(winds_speed))):
            if winds_speed[i] and winds_dir[i]:
                if winds_speed[i] > 30 and 300 <= winds_dir[i] <= 340:
                    indicators.append(f"Strong NW winds ({winds_speed[i]:.0f} km/h)")
                    confidence += 0.25
                    break

        # 3. Snow forecast in clipper track
        snowfall = alberta_hourly.get('snowfall', [])
        total_snow = sum([s for s in snowfall if s is not None])
        if total_snow > 2:  # 2+ cm snow forecast
            indicators.append(f"Snow forecast in Alberta ({total_snow:.1f} cm)")
            confidence += 0.20

        # 4. System moving toward Winnipeg (check Winnipeg pressure/snow)
        winnipeg_hourly = winnipeg_data.get('hourly', {})
        winnipeg_snow = winnipeg_hourly.get('snowfall', [])
        winnipeg_snow_total = sum([s for s in winnipeg_snow[12:36] if s is not None])  # 12-36h ahead
        if winnipeg_snow_total > 1:
            indicators.append(f"System tracking to Winnipeg ({winnipeg_snow_total:.1f} cm)")
            confidence += 0.30

        if confidence > 0.5:  # Threshold for detection
            # Estimate lead time based on when snow appears in forecast
            lead_time_hours = 24  # Default
            for i, snow in enumerate(alberta_hourly.get('snowfall', [])):
                if snow and snow > 0.5:
                    lead_time_hours = i
                    break

            return {
                'pattern_type': 'alberta_clipper',
                'confidence': confidence,
                'lead_time_hours': lead_time_hours,
                'indicators': indicators,
                'description': f"Alberta Clipper detected with {confidence*100:.0f}% confidence"
            }

        return None

    def detect_lake_effect_setup(self, data: Dict) -> Optional[Dict]:
        """
        Detect lake effect snow setup

        Signature:
        - Strong NW winds crossing Lake Superior
        - Cold air (temps well below freezing)
        - Fetch distance aligned with wind direction

        Returns:
            Detection result
        """
        superior_west = data.get('superior_west')
        superior_east = data.get('superior_east')
        eagle_river = data.get('eagle_river')

        if not all([superior_west, superior_east, eagle_river]):
            return None

        indicators = []
        confidence = 0.0

        # Check for lake effect wind pattern
        er_hourly = eagle_river.get('hourly', {})
        winds_speed = er_hourly.get('windspeed_10m', [])
        winds_dir = er_hourly.get('winddirection_10m', [])
        temps = er_hourly.get('temperature_2m', [])

        # Look for sustained NW winds + cold temps
        lake_effect_hours = 0
        for i in range(min(48, len(winds_speed))):
            if winds_speed[i] and winds_dir[i] and temps[i]:
                # Lake effect favorable: 280-340 deg, >20 km/h, temp < -5C
                if 280 <= winds_dir[i] <= 340 and winds_speed[i] > 20 and temps[i] < -5:
                    lake_effect_hours += 1

        if lake_effect_hours >= 6:  # 6+ hours of favorable conditions
            indicators.append(f"Lake effect winds for {lake_effect_hours} hours")
            confidence += 0.40

        # Check for upstream snow (enhancement mechanism)
        sw_hourly = superior_west.get('hourly', {})
        sw_snow = sw_hourly.get('snowfall', [])
        if sw_snow and sum([s for s in sw_snow[:24] if s is not None]) > 1:
            indicators.append("Upstream snow for enhancement")
            confidence += 0.30

        # Cold air mass present
        if temps and min([t for t in temps[:48] if t is not None]) < -10:
            indicators.append(f"Very cold air mass (min {min([t for t in temps[:48] if t is not None]):.1f}°C)")
            confidence += 0.30

        if confidence > 0.5:
            return {
                'pattern_type': 'lake_effect',
                'confidence': confidence,
                'lead_time_hours': 24,
                'indicators': indicators,
                'description': f"Lake effect snow setup detected ({confidence*100:.0f}% confidence)"
            }

        return None

    def analyze_patterns(self) -> List[Dict]:
        """
        Analyze atmospheric data to detect patterns

        Returns:
            List of detected patterns
        """
        # Fetch latest atmospheric data
        data = self.fetch_atmospheric_data(hours_ahead=72)

        patterns = []

        # Check for Alberta Clipper
        clipper = self.detect_alberta_clipper(data)
        if clipper:
            patterns.append(clipper)
            self._store_pattern(clipper)
            print(f"\n🌀 {clipper['description']}")
            print(f"   Lead time: {clipper['lead_time_hours']} hours")
            for indicator in clipper['indicators']:
                print(f"   • {indicator}")

        # Check for Lake Effect
        lake_effect = self.detect_lake_effect_setup(data)
        if lake_effect:
            patterns.append(lake_effect)
            self._store_pattern(lake_effect)
            print(f"\n❄️  {lake_effect['description']}")
            print(f"   Lead time: {lake_effect['lead_time_hours']} hours")
            for indicator in lake_effect['indicators']:
                print(f"   • {indicator}")

        if not patterns:
            print("\n✅ No significant atmospheric patterns detected")

        return patterns

    def _store_pattern(self, pattern: Dict):
        """Store detected pattern in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO atmospheric_patterns
            (detection_time, pattern_type, confidence, lead_time_hours,
             description, indicators)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            pattern['pattern_type'],
            pattern['confidence'],
            pattern['lead_time_hours'],
            pattern['description'],
            json.dumps(pattern['indicators'])
        ))

        conn.commit()
        conn.close()

    def get_recent_patterns(self, hours_back: int = 48) -> List[Dict]:
        """Get patterns detected in recent hours"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cutoff = (datetime.utcnow() - timedelta(hours=hours_back)).isoformat()

        cursor.execute("""
            SELECT detection_time, pattern_type, confidence, lead_time_hours,
                   description, indicators
            FROM atmospheric_patterns
            WHERE detection_time > ?
            ORDER BY detection_time DESC
        """, (cutoff,))

        patterns = []
        for row in cursor.fetchall():
            patterns.append({
                'detection_time': row[0],
                'pattern_type': row[1],
                'confidence': row[2],
                'lead_time_hours': row[3],
                'description': row[4],
                'indicators': json.loads(row[5])
            })

        conn.close()
        return patterns


def main():
    """Run atmospheric data fetch and pattern analysis"""
    print("=" * 80)
    print("GFS ATMOSPHERIC PATTERN DETECTION")
    print("=" * 80)
    print(f"Analysis Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print()

    fetcher = GFSAtmosphericFetcher()
    patterns = fetcher.analyze_patterns()

    print("\n" + "=" * 80)
    print(f"✅ Analysis complete - {len(patterns)} pattern(s) detected")
    print("=" * 80)

    return patterns


if __name__ == "__main__":
    main()
