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

# Current ENSO phase for the 2025-2026 winter season
# Update annually based on NOAA ONI data
CURRENT_ENSO_PHASE = 'la_nina'

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
        'wind_speed_max_kmh': [round(v, 1) if v is not None else None
                               for v in daily.get('wind_speed_10m_max', [None] * n)],
        'wind_gusts_max_kmh': [round(v, 1) if v is not None else None
                               for v in daily.get('wind_gusts_10m_max', [None] * n)],
        'cloud_cover_pct': [round(v) if v is not None else None
                            for v in daily.get('cloud_cover_mean', [None] * n)],
    }


def iso_week_label(week_num):
    """Get date range label for an ISO week number in the current year."""
    # ISO week 1 contains the first Thursday of the year
    # Find the Monday of the given ISO week
    jan4 = datetime(datetime.now().year, 1, 4)  # Jan 4 is always in ISO week 1
    week1_monday = jan4 - timedelta(days=jan4.weekday())
    week_start = week1_monday + timedelta(weeks=week_num - 1)
    week_end = week_start + timedelta(days=6)
    return f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d')}"


def detect_schema(cursor):
    """Detect available columns in snowfall_daily table (call once)."""
    cursor.execute("PRAGMA table_info(snowfall_daily)")
    columns = {row[1] for row in cursor.fetchall()}
    return {
        'has_temp': 'temp_max_celsius' in columns and 'temp_min_celsius' in columns,
        'has_wcode': 'weather_code' in columns,
    }


def get_climatology(cursor, station_id, schema=None):
    """Query historical climatology grouped by ISO week number."""
    has_temp = schema['has_temp'] if schema else False

    if has_temp:
        temp_select = "AVG(temp_max_celsius) as avg_temp_max_c, AVG(temp_min_celsius) as avg_temp_min_c,"
    else:
        temp_select = "NULL as avg_temp_max_c, NULL as avg_temp_min_c,"

    cursor.execute(f"""
        SELECT
            CAST(strftime('%%W', date) AS INTEGER) as week_num,
            AVG(snowfall_mm) as avg_daily_snowfall_mm,
            AVG(CASE WHEN snowfall_mm > 1.0 THEN 1.0 ELSE 0.0 END) as snow_day_probability,
            {temp_select}
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
        # Clamp to 1-53 range for ISO-style keys
        iso_week = max(1, min(53, week_num + 1))
        week_label = iso_week_label(iso_week)

        weeks[str(iso_week)] = {
            'week_label': week_label,
            'avg_daily_snowfall_mm': round(row[1], 1) if row[1] else 0.0,
            'snow_day_probability': round(row[2], 2) if row[2] else 0.0,
            'avg_temp_max_c': round(row[3], 1) if row[3] else None,
            'avg_temp_min_c': round(row[4], 1) if row[4] else None,
            'max_recorded_snowfall_mm': round(row[5], 1) if row[5] else 0.0,
            'years_of_data': row[6] or 0,
        }

    return {'weeks': weeks}


def get_recent_observations(cursor, station_id, days=7, schema=None):
    """Get last N days of actual observations."""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    has_temp = schema['has_temp'] if schema else False
    has_wcode = schema['has_wcode'] if schema else False

    select_cols = "date, snowfall_mm"
    if has_temp:
        select_cols += ", temp_max_celsius, temp_min_celsius"
    if has_wcode:
        select_cols += ", weather_code"

    cursor.execute(f"""
        SELECT {select_cols}
        FROM snowfall_daily
        WHERE station_id = ?
          AND date >= ? AND date <= ?
        ORDER BY date DESC
    """, (station_id, start_date, end_date))

    rows = cursor.fetchall()
    observations = []
    for row in rows:
        obs = {
            'date': row[0],
            'snowfall_mm': round(row[1], 1) if row[1] is not None else 0.0,
            'temp_max_c': None,
            'temp_min_c': None,
            'weather_code': None,
        }
        idx = 2
        if has_temp:
            obs['temp_max_c'] = round(row[idx], 1) if row[idx] is not None else None
            obs['temp_min_c'] = round(row[idx+1], 1) if row[idx+1] is not None else None
            idx += 2
        if has_wcode:
            obs['weather_code'] = row[idx]
        observations.append(obs)
    return observations


def compute_snow_score(forecast_dict, climatology):
    """
    Compute snow_score (0-100) for map color coding.
    Recalibrated so the map shows meaningful color spread.

    Components:
    - Forecast snowfall (0-70 pts): total cm in next 7 days
    - Forecast probability (0-15 pts): avg precip probability for snow days
    - Historical/fallback (0-15 pts): snow day probability for current week
    """
    # Forecast snowfall component: total in next 7 days (cm)
    snowfall_7d = sum(forecast_dict['snowfall_cm'][:7])
    # Recalibrated: 0cm=0, 2cm=20, 8cm=40, 20cm=60, 40cm+=70
    if snowfall_7d >= 40:
        fc_snow = 70
    elif snowfall_7d >= 20:
        fc_snow = 60 + (snowfall_7d - 20) / 20 * 10
    elif snowfall_7d >= 8:
        fc_snow = 40 + (snowfall_7d - 8) / 12 * 20
    elif snowfall_7d >= 2:
        fc_snow = 20 + (snowfall_7d - 2) / 6 * 20
    elif snowfall_7d > 0:
        fc_snow = snowfall_7d / 2 * 20
    else:
        fc_snow = 0

    # Forecast probability component: avg precip probability on days with cold temps
    probs = forecast_dict.get('precip_probability_pct', [])[:7]
    temps = forecast_dict.get('temp_max_c', [])[:7]
    snow_probs = []
    for i in range(min(len(probs), len(temps))):
        if temps[i] is not None and temps[i] <= 2:
            snow_probs.append(probs[i])
    avg_prob = sum(snow_probs) / len(snow_probs) if snow_probs else 0
    fc_prob = avg_prob / 100 * 15  # 0-15 pts

    # Historical component: snow day probability for current ISO week
    # Use ISO week: %W + 1 to match our key convention
    current_week = str(max(1, int(datetime.now().strftime('%W')) + 1))
    week_data = climatology.get('weeks', {}).get(current_week, {})
    hist_prob = week_data.get('snow_day_probability', 0)

    if hist_prob > 0:
        hist_score = hist_prob * 15  # 0-15 pts from historical
    else:
        # Fallback for stations without historical data:
        # Use forecast cold-day count as a proxy
        cold_days = sum(1 for t in temps if t is not None and t <= 0)
        hist_score = min(15, cold_days * 2.5)

    score = min(100, round(fc_snow + fc_prob + hist_score))
    return max(0, score)


def compute_futurecast(climatology, enso_phase='neutral'):
    """
    Compute 6-month futurecast scores from weekly climatology.

    Each week gets a score (0-100) based on:
    - Snowfall component (0-60 pts): normalized to station's own peak week
    - Probability component (0-30 pts): snow_day_probability * 30
    - ENSO modulation (NH winter weeks 44-14): La Nina +8%, El Nino -8%

    Only emits weeks with score > 0 to keep JSON compact.
    """
    weeks = climatology.get('weeks', {})
    if not weeks:
        return {}

    # Find peak snowfall week for normalization
    peak_snow = max(
        (w.get('avg_daily_snowfall_mm', 0) for w in weeks.values()),
        default=0,
    )
    if peak_snow <= 0:
        peak_snow = 1  # avoid division by zero

    enso_mod = {
        'la_nina': 1.08,
        'el_nino': 0.92,
        'neutral': 1.0,
    }
    mod = enso_mod.get(enso_phase, 1.0)

    futurecast = {}
    for week_key, w in weeks.items():
        wk = int(week_key)
        avg_snow = w.get('avg_daily_snowfall_mm', 0)
        snow_prob = w.get('snow_day_probability', 0)

        # Snowfall component: normalized to station peak (0-60)
        snow_pts = min(60, (avg_snow / peak_snow) * 60)

        # Probability component (0-30)
        prob_pts = snow_prob * 30

        raw = snow_pts + prob_pts

        # ENSO modulation: NH winter weeks only (Nov-Mar approx)
        if wk >= 44 or wk <= 14:
            raw *= mod

        score = min(100, max(0, round(raw)))
        if score == 0:
            continue

        futurecast[week_key] = {
            'score': score,
            'avg_daily_snow_mm': round(avg_snow, 1),
            'snow_day_prob': round(snow_prob, 2),
            'week_label': w.get('week_label', ''),
            'max_recorded_mm': round(w.get('max_recorded_snowfall_mm', 0), 1),
        }

    return futurecast


def compute_never_summer(station_data):
    """
    Compute year-round snow-chasing itinerary across both hemispheres.

    For each of 54 ISO weeks, finds the best snow destination on Earth.
    Groups consecutive weeks at the same station into legs (min 2 weeks).
    """
    # Collect weekly snowfall data across all stations
    weekly_best = {}  # week -> {station_id, score, snow_mm, station_name}
    weeks_with_data = set()

    for station_id, s in station_data.items():
        fc = s.get('futurecast', {})
        for week_key, wd in fc.items():
            wk = int(week_key)
            snow_mm = wd.get('avg_daily_snow_mm', 0)
            prob = wd.get('snow_day_prob', 0)
            score = wd.get('score', 0)

            weeks_with_data.add(wk)

            current_best = weekly_best.get(wk)
            # Rank by absolute snowfall, tiebreak by probability
            if current_best is None or snow_mm > current_best['snow_mm'] or \
               (snow_mm == current_best['snow_mm'] and prob > current_best.get('prob', 0)):
                weekly_best[wk] = {
                    'station_id': station_id,
                    'station_name': s['name'],
                    'region': s['region'],
                    'score': score,
                    'snow_mm': round(snow_mm, 1),
                    'prob': round(prob, 2),
                }

    coverage_pct = round(len(weeks_with_data) / 54 * 100)

    if not weekly_best:
        return {
            'coverage_pct': 0,
            'avg_score': 0,
            'total_legs': 0,
            'legs': [],
            'weekly_best': {},
        }

    # Greedy assignment with grouping (min 2-week stays)
    # First pass: raw best per week
    assigned = {}  # week -> station_id
    for wk in range(1, 55):
        if wk in weekly_best:
            assigned[wk] = weekly_best[wk]['station_id']

    # Second pass: smooth short 1-week hops
    # If a station appears for only 1 week between two runs of the same station, absorb it
    # unless the score difference is > 30%
    assigned_list = sorted(assigned.keys())
    for i in range(1, len(assigned_list) - 1):
        wk = assigned_list[i]
        prev_wk = assigned_list[i - 1]
        next_wk = assigned_list[i + 1]
        if assigned[prev_wk] == assigned[next_wk] and assigned[wk] != assigned[prev_wk]:
            # Single-week hop — check if difference justifies it
            prev_snow = weekly_best.get(wk, {}).get('snow_mm', 0)
            alt_snow = 0
            alt_station = assigned[prev_wk]
            # Look up what the previous station scores this week
            alt_s = station_data.get(alt_station, {})
            alt_fc = alt_s.get('futurecast', {}).get(str(wk), {})
            alt_snow = alt_fc.get('avg_daily_snow_mm', 0)
            if prev_snow > 0 and alt_snow > 0:
                diff = (prev_snow - alt_snow) / prev_snow
                if diff < 0.3:
                    assigned[wk] = alt_station

    # Also: prefer staying if same region and within 10%
    for i in range(1, len(assigned_list)):
        wk = assigned_list[i]
        prev_wk = assigned_list[i - 1]
        if assigned[wk] != assigned[prev_wk]:
            curr_s = station_data.get(assigned[wk], {})
            prev_s = station_data.get(assigned[prev_wk], {})
            if curr_s.get('region') == prev_s.get('region'):
                curr_snow = weekly_best.get(wk, {}).get('snow_mm', 0)
                prev_fc = prev_s.get('futurecast', {}).get(str(wk), {})
                prev_snow = prev_fc.get('avg_daily_snow_mm', 0)
                if curr_snow > 0 and prev_snow > 0 and prev_snow >= curr_snow * 0.9:
                    assigned[wk] = assigned[prev_wk]

    # Build legs from consecutive same-station weeks
    legs = []
    sorted_weeks = sorted(assigned.keys())
    if sorted_weeks:
        current_leg = {
            'station_id': assigned[sorted_weeks[0]],
            'start_week': sorted_weeks[0],
            'end_week': sorted_weeks[0],
            'weeks': [sorted_weeks[0]],
        }
        for i in range(1, len(sorted_weeks)):
            wk = sorted_weeks[i]
            # Check if same station and consecutive (or close)
            if assigned[wk] == current_leg['station_id'] and wk - current_leg['end_week'] <= 1:
                current_leg['end_week'] = wk
                current_leg['weeks'].append(wk)
            else:
                legs.append(current_leg)
                current_leg = {
                    'station_id': assigned[wk],
                    'start_week': wk,
                    'end_week': wk,
                    'weeks': [wk],
                }
        legs.append(current_leg)

    # Enrich legs with scores and station info
    enriched_legs = []
    for leg in legs:
        s = station_data.get(leg['station_id'], {})
        fc = s.get('futurecast', {})
        scores = []
        snows = []
        for wk in leg['weeks']:
            wd = fc.get(str(wk), {})
            scores.append(wd.get('score', 0))
            snows.append(wd.get('avg_daily_snow_mm', 0))
        avg_score = round(sum(scores) / len(scores)) if scores else 0
        avg_snow = round(sum(snows) / len(snows), 1) if snows else 0

        enriched_legs.append({
            'station_id': leg['station_id'],
            'station_name': s.get('name', leg['station_id']),
            'region': s.get('region', ''),
            'start_week': leg['start_week'],
            'end_week': leg['end_week'],
            'avg_score': avg_score,
            'avg_snow_mm': avg_snow,
            'hemisphere': 'S' if s.get('lat', 0) < 0 else 'N',
        })

    # Weekly best output (compact)
    wb_output = {}
    for wk, best in weekly_best.items():
        wb_output[str(wk)] = {
            'station_id': best['station_id'],
            'station_name': best['station_name'],
            'score': best['score'],
            'snow_mm': best['snow_mm'],
        }

    all_scores = [best['score'] for best in weekly_best.values()]
    avg_score = round(sum(all_scores) / len(all_scores)) if all_scores else 0

    return {
        'coverage_pct': coverage_pct,
        'avg_score': avg_score,
        'total_legs': len(enriched_legs),
        'legs': enriched_legs,
        'weekly_best': wb_output,
    }


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
        'us_rockies_north': 'Northern Rockies',
        'us_rockies_utah': 'Utah Ski',
        'us_rockies_co_expanded': 'Colorado Ski (Expanded)',
        'us_northeast_ski': 'Northeast Ski',
        'us_midwest_ski': 'Midwest Ski',
        'us_pacific_ski': 'Pacific Ski',
        'canada_bc_ski': 'British Columbia Ski',
        'canada_ab_ski': 'Alberta Ski',
        'canada_east_ski': 'Eastern Canada Ski',
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
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        cursor = conn.cursor()
        schema = detect_schema(cursor)

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
            climatology = get_climatology(cursor, station_id, schema)

            # Recent observations
            recent = get_recent_observations(cursor, station_id, schema=schema)

            # Snow score
            snow_score = compute_snow_score(forecast_dict, climatology)

            # Futurecast
            futurecast = compute_futurecast(climatology, CURRENT_ENSO_PHASE)

            station_data[station_id] = {
                'name': name,
                'region': region,
                'lat': lat,
                'lon': lon,
                'forecast': forecast_dict,
                'climatology': climatology,
                'futurecast': futurecast,
                'recent_observations': recent,
                'snow_score': snow_score,
            }

            success += 1
            print(f"OK  score={snow_score}  snow_7d={sum(forecast_dict['snowfall_cm'][:7]):.1f}cm")
            time.sleep(RATE_LIMIT)
    finally:
        conn.close()

    # Compute Never Summer itinerary
    never_summer = compute_never_summer(station_data)
    print(f"\nNever Summer: {never_summer['coverage_pct']}% coverage, "
          f"{never_summer['total_legs']} legs, avg score {never_summer['avg_score']}")

    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'generated_at_human': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
        'station_count': len(station_data),
        'enso_phase': CURRENT_ENSO_PHASE,
        'stations': station_data,
        'regions': build_region_index(station_data),
        'never_summer': never_summer,
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
