#!/usr/bin/env python3
"""
Generate Station Forecasts for Snow Trip Planner
Fetches 16-day forecasts from Open-Meteo for all world stations,
queries historical climatology, and outputs station_data.json.

Runs nightly on NAS after daily_automated_forecast.py.
"""

import requests
import sqlite3
import os
import sys
import json
import time
import math
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from collect_world_data import WORLD_STATIONS

DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')
OUTPUT_DIR = os.environ.get('FORECAST_OUTPUT_DIR', 'forecast_output')

FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_VARIABLES = (
    "snowfall_sum,temperature_2m_max,temperature_2m_min,"
    "precipitation_sum,precipitation_probability_max,"
    "wind_speed_10m_max,wind_gusts_10m_max,"
    "weather_code,cloud_cover_mean"
)
RATE_LIMIT = 0.3  # seconds between API calls


def flatten_stations():
    """Return flat list of (station_id, lat, lon, name, region) from WORLD_STATIONS."""
    stations = []
    for region, station_list in WORLD_STATIONS.items():
        for station_id, lat, lon, name in station_list:
            stations.append((station_id, lat, lon, name, region))
    return stations


def fetch_forecast(lat, lon):
    """Fetch 16-day daily forecast from Open-Meteo."""
    params = {
        'latitude': lat,
        'longitude': lon,
        'daily': FORECAST_VARIABLES,
        'timezone': 'UTC',
        'forecast_days': 16,
    }
    try:
        resp = requests.get(FORECAST_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if 'daily' not in data or 'time' not in data['daily']:
            return None
        return data['daily']
    except Exception as e:
        print(f"    Forecast API error: {e}")
        return None


def build_forecast_dict(daily):
    """Convert Open-Meteo daily response to our forecast format."""
    n = len(daily['time'])

    def get(key):
        vals = daily.get(key, [None] * n)
        return [v if v is not None else 0 for v in vals]

    # Open-Meteo returns snowfall in cm; convert to cm (keep as-is)
    snowfall_raw = daily.get('snowfall_sum', [None] * n)
    snowfall_cm = [round(v, 1) if v is not None else 0.0 for v in snowfall_raw]

    return {
        'dates': daily['time'],
        'snowfall_cm': snowfall_cm,
        'temp_max_c': [round(v, 1) if v is not None else None
                       for v in daily.get('temperature_2m_max', [None] * n)],
        'temp_min_c': [round(v, 1) if v is not None else None
                       for v in daily.get('temperature_2m_min', [None] * n)],
        'precipitation_mm': [round(v, 1) if v is not None else 0.0
                             for v in daily.get('precipitation_sum', [None] * n)],
        'weather_code': [v if v is not None else 0
                         for v in daily.get('weather_code', [None] * n)],
        'precip_probability_pct': [v if v is not None else 0
                                   for v in daily.get('precipitation_probability_max', [None] * n)],
    }


def get_climatology(cursor, station_id):
    """Query historical climatology grouped by ISO week number."""
    cursor.execute("""
        SELECT
            CAST(strftime('%%W', date) AS INTEGER) as week_num,
            AVG(snowfall_mm) as avg_daily_snowfall_mm,
            AVG(CASE WHEN snowfall_mm > 1.0 THEN 1.0 ELSE 0.0 END) as snow_day_probability,
            AVG(temp_max_celsius) as avg_temp_max_c,
            AVG(temp_min_celsius) as avg_temp_min_c,
            MAX(snowfall_mm) as max_recorded_snowfall_mm,
            COUNT(DISTINCT strftime('%%Y', date)) as years_of_data
        FROM snowfall_daily
        WHERE station_id = ?
          AND snowfall_mm IS NOT NULL
        GROUP BY week_num
        ORDER BY week_num
    """, (station_id,))

    rows = cursor.fetchall()
    weeks = {}
    for row in rows:
        week_num = row[0]
        # Compute approximate week label
        # Week 0 starts Jan 1; use Monday of that week
        jan1 = datetime(datetime.now().year, 1, 1)
        week_start = jan1 + timedelta(weeks=week_num)
        week_end = week_start + timedelta(days=6)
        week_label = f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d')}"

        weeks[str(week_num)] = {
            'week_label': week_label,
            'avg_daily_snowfall_mm': round(row[1], 1) if row[1] else 0.0,
            'snow_day_probability': round(row[2], 2) if row[2] else 0.0,
            'avg_temp_max_c': round(row[3], 1) if row[3] else None,
            'avg_temp_min_c': round(row[4], 1) if row[4] else None,
            'max_recorded_snowfall_mm': round(row[5], 1) if row[5] else 0.0,
            'years_of_data': row[6] or 0,
        }

    return {'weeks': weeks}


def get_recent_observations(cursor, station_id, days=7):
    """Get last N days of actual observations."""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    cursor.execute("""
        SELECT date, snowfall_mm, temp_max_celsius, temp_min_celsius, weather_code
        FROM snowfall_daily
        WHERE station_id = ?
          AND date >= ? AND date <= ?
        ORDER BY date DESC
    """, (station_id, start_date, end_date))

    rows = cursor.fetchall()
    observations = []
    for row in rows:
        observations.append({
            'date': row[0],
            'snowfall_mm': round(row[1], 1) if row[1] is not None else 0.0,
            'temp_max_c': round(row[2], 1) if row[2] is not None else None,
            'temp_min_c': round(row[3], 1) if row[3] is not None else None,
            'weather_code': row[4],
        })
    return observations


def compute_snow_score(forecast_dict, climatology):
    """
    Compute snow_score (0-100) for map color coding.
    Based on: upcoming forecast snowfall + historical snow probability for current week.
    """
    # Forecast component: total snowfall in next 7 days (cm)
    snowfall_7d = sum(forecast_dict['snowfall_cm'][:7])
    # Scale: 0cm=0, 5cm=30, 15cm=60, 30cm+=80
    if snowfall_7d >= 30:
        forecast_score = 80
    elif snowfall_7d >= 15:
        forecast_score = 60 + (snowfall_7d - 15) / 15 * 20
    elif snowfall_7d >= 5:
        forecast_score = 30 + (snowfall_7d - 5) / 10 * 30
    elif snowfall_7d > 0:
        forecast_score = snowfall_7d / 5 * 30
    else:
        forecast_score = 0

    # Historical component: snow day probability for current week
    current_week = str(int(datetime.now().strftime('%W')))
    week_data = climatology.get('weeks', {}).get(current_week, {})
    hist_prob = week_data.get('snow_day_probability', 0)
    hist_score = hist_prob * 20  # 0-20 points from historical

    score = min(100, round(forecast_score + hist_score))
    return max(0, score)


def build_region_index(all_stations):
    """Build region display names and station lists."""
    region_names = {
        'northern_wisconsin': 'Northern Wisconsin',
        'lake_superior': 'Lake Superior Snow Belt',
        'regional': 'Great Lakes Regional',
        'california_ski': 'California Ski',
        'colorado_rockies': 'Colorado Rockies',
        'pacific_northwest': 'Pacific Northwest',
        'siberia_west': 'Western Siberia',
        'siberia_central': 'Central Siberia',
        'siberia_east': 'Eastern Siberia',
        'arctic_russia': 'Arctic Russia',
        'japan_north': 'Northern Japan',
        'japan_sea_coast': 'Japan Sea Coast',
        'japan_mountains': 'Japan Mountains',
        'china_northeast': 'Northeast China',
        'china_northwest': 'Northwest China',
        'china_tibetan': 'Tibetan Plateau',
        'alps_western': 'Western Alps',
        'alps_eastern': 'Eastern Alps',
        'scandinavia': 'Scandinavia',
        'canada_west': 'Western Canada',
        'canada_rockies': 'Canadian Rockies',
        'south_america': 'South America',
        'africa_mountains': 'African Mountains',
        'central_asia': 'Central Asia',
        'middle_east_mountains': 'Middle East Mountains',
        'australia_nz': 'Australia & New Zealand',
        'canada_east': 'Eastern Canada',
        'canada_north': 'Northern Canada',
        'appalachian_eastern_us': 'Appalachian / Eastern US',
        'southern_hemisphere_ref': 'Southern Hemisphere',
        'europe_additional': 'Europe Additional',
    }

    regions = {}
    for region_key in WORLD_STATIONS:
        station_ids = [s[0] for s in WORLD_STATIONS[region_key]]
        regions[region_key] = {
            'display_name': region_names.get(region_key, region_key.replace('_', ' ').title()),
            'stations': station_ids,
        }
    return regions


def generate_all():
    """Main generation loop."""
    print(f"\n{'='*80}")
    print("STATION FORECAST GENERATION — Snow Trip Planner")
    print(f"{'='*80}")
    print(f"Database: {DB_PATH}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    stations = flatten_stations()
    total = len(stations)
    print(f"Stations: {total}")
    print(f"{'='*80}\n")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=DELETE")
    cursor = conn.cursor()

    station_data = {}
    success = 0
    fail = 0

    for i, (station_id, lat, lon, name, region) in enumerate(stations, 1):
        print(f"  [{i}/{total}] {name}...", end=" ", flush=True)

        # Fetch 16-day forecast
        daily = fetch_forecast(lat, lon)
        if daily is None:
            print("SKIP (forecast failed)")
            fail += 1
            time.sleep(RATE_LIMIT)
            continue

        forecast_dict = build_forecast_dict(daily)

        # Historical climatology
        climatology = get_climatology(cursor, station_id)

        # Recent observations
        recent = get_recent_observations(cursor, station_id)

        # Snow score
        snow_score = compute_snow_score(forecast_dict, climatology)

        station_data[station_id] = {
            'name': name,
            'region': region,
            'lat': lat,
            'lon': lon,
            'forecast': forecast_dict,
            'climatology': climatology,
            'recent_observations': recent,
            'snow_score': snow_score,
        }

        success += 1
        print(f"OK  score={snow_score}  snow_7d={sum(forecast_dict['snowfall_cm'][:7]):.1f}cm")
        time.sleep(RATE_LIMIT)

    conn.close()

    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'generated_at_human': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
        'station_count': len(station_data),
        'stations': station_data,
        'regions': build_region_index(station_data),
    }

    # Write JSON
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / 'station_data.json'
    with open(out_file, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    file_size = out_file.stat().st_size
    print(f"\n{'='*80}")
    print(f"GENERATION COMPLETE")
    print(f"{'='*80}")
    print(f"Stations: {success}/{total} successful, {fail} failed")
    print(f"Output: {out_file} ({file_size / 1024:.0f} KB)")
    print(f"{'='*80}\n")

    return output


if __name__ == '__main__':
    generate_all()
