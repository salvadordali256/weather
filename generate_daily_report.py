#!/usr/bin/env python3
"""
Generate Daily Executive Snow Report
Reads station_data.json + latest_forecast.json, outputs daily_report.json.
No API calls or DB queries — purely transforms existing pipeline outputs.

Runs on NAS after generate_station_forecasts.py, before push_forecast.sh.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = os.environ.get('FORECAST_OUTPUT_DIR', 'forecast_output')

# Region display names (matches generate_station_forecasts.py)
REGION_NAMES = {
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


def load_station_data():
    """Load station_data.json from output dir."""
    path = Path(OUTPUT_DIR) / 'station_data.json'
    if not path.exists():
        print(f"  ERROR: {path} not found")
        return None
    with open(path) as f:
        return json.load(f)


def load_forecast_data():
    """Load latest_forecast.json from output dir."""
    path = Path(OUTPUT_DIR) / 'latest_forecast.json'
    if not path.exists():
        print(f"  WARNING: {path} not found, skipping Wisconsin summary")
        return None
    with open(path) as f:
        return json.load(f)


def classify_alert(snow_score, snowfall_7d_cm):
    """Classify alert level based on score and snowfall."""
    if snow_score >= 60 or snowfall_7d_cm >= 20:
        return 'HIGH'
    elif snow_score >= 30 or snowfall_7d_cm >= 5:
        return 'MODERATE'
    return 'LOW'


def build_snow_alerts(stations):
    """Build sorted list of station alerts with snowfall details."""
    alerts = []
    for sid, s in stations.items():
        fc = s.get('forecast', {})
        snow_cm = fc.get('snowfall_cm', [])
        temps_max = fc.get('temp_max_c', [])
        temps_min = fc.get('temp_min_c', [])
        dates = fc.get('dates', [])

        snowfall_7d = sum(snow_cm[:7])
        snowfall_3d = sum(snow_cm[:3])

        # Find peak day
        peak_idx = 0
        peak_val = 0
        for i, v in enumerate(snow_cm[:7]):
            if v > peak_val:
                peak_val = v
                peak_idx = i

        # Temp range for next 7 days
        valid_max = [t for t in temps_max[:7] if t is not None]
        valid_min = [t for t in temps_min[:7] if t is not None]
        if valid_max and valid_min:
            temp_range = f"{min(valid_min):.0f} to {max(valid_max):.0f}\u00b0C"
        else:
            temp_range = "N/A"

        alert_level = classify_alert(s.get('snow_score', 0), snowfall_7d)

        alerts.append({
            'station_id': sid,
            'name': s['name'],
            'region': REGION_NAMES.get(s.get('region', ''), s.get('region', '')),
            'snow_score': s.get('snow_score', 0),
            'snowfall_7d_cm': round(snowfall_7d, 1),
            'snowfall_3d_cm': round(snowfall_3d, 1),
            'peak_day': dates[peak_idx] if peak_idx < len(dates) else '',
            'peak_day_cm': round(peak_val, 1),
            'temp_range': temp_range,
            'alert_level': alert_level,
        })

    alerts.sort(key=lambda a: a['snow_score'], reverse=True)
    return alerts


def build_regional_summary(stations):
    """Aggregate stats by region."""
    regions = {}
    for sid, s in stations.items():
        region = s.get('region', 'unknown')
        if region not in regions:
            regions[region] = {
                'region': region,
                'display_name': REGION_NAMES.get(region, region.replace('_', ' ').title()),
                'scores': [],
                'snowfall_7d': [],
                'stations': [],
            }
        snow_7d = sum(s.get('forecast', {}).get('snowfall_cm', [])[:7])
        regions[region]['scores'].append(s.get('snow_score', 0))
        regions[region]['snowfall_7d'].append(snow_7d)
        regions[region]['stations'].append((sid, s['name'], s.get('snow_score', 0)))

    summary = []
    for rk, rv in regions.items():
        top = max(rv['stations'], key=lambda x: x[2])
        summary.append({
            'region': rk,
            'display_name': rv['display_name'],
            'station_count': len(rv['scores']),
            'avg_score': round(sum(rv['scores']) / len(rv['scores'])),
            'max_score': max(rv['scores']),
            'top_station': top[1],
            'total_7d_cm': round(sum(rv['snowfall_7d']), 1),
        })

    summary.sort(key=lambda r: r['max_score'], reverse=True)
    return summary


def build_wisconsin_summary(forecast_data):
    """Extract Wisconsin forecast summary."""
    if not forecast_data:
        return None

    forecasts = forecast_data.get('forecasts', [])
    snow_days = sum(1 for f in forecasts if f.get('probability', 0) >= 30)
    max_prob = max((f.get('probability', 0) for f in forecasts), default=0)

    # Find highest expected snowfall
    max_snowfall = ''
    for f in forecasts:
        if f.get('probability', 0) == max_prob:
            max_snowfall = f.get('expected_snowfall', '')
            break

    return {
        'forecast_days': forecast_data.get('forecast_days', 7),
        'snow_days': snow_days,
        'max_probability': max_prob,
        'max_probability_snowfall': max_snowfall,
        'summary_text': forecast_data.get('summary_text', ''),
    }


def build_score_distribution(stations):
    """Count stations in each score band."""
    high = moderate = low = 0
    for s in stations.values():
        score = s.get('snow_score', 0)
        if score >= 60:
            high += 1
        elif score >= 30:
            moderate += 1
        else:
            low += 1
    return {'high': high, 'moderate': moderate, 'low': low}


def generate_report():
    """Main report generation."""
    print(f"\n{'='*60}")
    print("DAILY EXECUTIVE SNOW REPORT")
    print(f"{'='*60}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    station_data = load_station_data()
    if not station_data:
        print("FATAL: No station data available")
        sys.exit(1)

    forecast_data = load_forecast_data()

    stations = station_data.get('stations', {})
    print(f"Stations loaded: {len(stations)}")

    # Pipeline health
    pipeline = {
        'station_count': len(stations),
        'stations_with_forecast': sum(
            1 for s in stations.values()
            if len(s.get('forecast', {}).get('dates', [])) > 0
        ),
        'stations_missing': [
            sid for sid, s in stations.items()
            if len(s.get('forecast', {}).get('dates', [])) == 0
        ],
        'data_freshness': station_data.get('generated_at', ''),
    }

    # Snow alerts (all stations, sorted by score)
    all_alerts = build_snow_alerts(stations)
    snow_alerts = [a for a in all_alerts if a['alert_level'] in ('HIGH', 'MODERATE')]

    # Top 10
    top_10 = all_alerts[:10]

    # Regional summary
    regional = build_regional_summary(stations)

    # Wisconsin
    wisconsin = build_wisconsin_summary(forecast_data)

    # Score distribution
    distribution = build_score_distribution(stations)

    report = {
        'generated_at': datetime.now().isoformat(),
        'generated_at_human': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
        'pipeline': pipeline,
        'snow_alerts': snow_alerts,
        'top_10_stations': top_10,
        'regional_summary': regional,
        'wisconsin_summary': wisconsin,
        'score_distribution': distribution,
    }

    # Write output
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / 'daily_report.json'
    with open(out_file, 'w') as f:
        json.dump(report, f, separators=(',', ':'))

    file_size = out_file.stat().st_size
    print(f"\nReport written: {out_file} ({file_size / 1024:.1f} KB)")
    print(f"Pipeline: {pipeline['stations_with_forecast']}/{pipeline['station_count']} stations OK")
    print(f"Alerts: {len(snow_alerts)} ({distribution['high']} high, {distribution['moderate']} moderate)")
    print(f"Top station: {top_10[0]['name']} (score {top_10[0]['snow_score']}, {top_10[0]['snowfall_7d_cm']}cm/7d)")
    print(f"{'='*60}\n")

    return report


if __name__ == '__main__':
    generate_report()
