#!/usr/bin/env python3
"""
Research NOAA Station IDs for Ski Resorts

One-time script that queries the NOAA CDO API to find the best GHCN station
within 50km of each ski resort (lat/lon). Ranks by distance, data coverage,
and station type (prefer USC/CA core stations).

Outputs noaa_station_mappings.json for review.

Usage:
    python research_noaa_stations.py
    python research_noaa_stations.py --radius 75   # wider search
    python research_noaa_stations.py --resort "Jackson Hole"  # single resort
"""

import requests
import json
import math
import time
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

NOAA_TOKEN = os.environ.get('NOAA_API_TOKEN', '')
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

# All NA ski resorts we want NOAA coverage for
# Format: station_id -> (lat, lon, display_name)
TARGET_RESORTS = {
    # === EXISTING STATIONS (already in collect_world_data.py) ===
    # Colorado Rockies
    'aspen_co': (39.1911, -106.8175, 'Aspen, CO'),
    'vail_co': (39.6403, -106.3742, 'Vail, CO'),
    'steamboat_springs_co': (40.4850, -106.8317, 'Steamboat Springs, CO'),
    'telluride_co': (37.9375, -107.8123, 'Telluride, CO'),
    'breckenridge_co': (39.4817, -106.0384, 'Breckenridge, CO'),
    # Pacific Northwest
    'mount_baker_wa': (48.8568, -121.6714, 'Mount Baker, WA'),
    'stevens_pass_wa': (47.7465, -121.0890, 'Stevens Pass, WA'),
    'mount_hood_or': (45.3306, -121.7081, 'Mount Hood, OR'),
    # California Ski
    'mammoth_mountain_ca': (37.6308, -119.0326, 'Mammoth Mountain, CA'),
    'lake_tahoe_ca': (39.1969, -120.2356, 'Lake Tahoe, CA'),
    'mount_shasta_ca': (41.4092, -122.1949, 'Mount Shasta, CA'),
    'big_bear_ca': (34.2406, -116.9114, 'Big Bear, CA'),
    # Canada West
    'whistler_bc': (50.1163, -122.9574, 'Whistler, BC'),
    'revelstoke_bc': (50.9981, -118.1957, 'Revelstoke, BC'),
    # Canada Rockies
    'banff_ab': (51.1784, -115.5708, 'Banff, AB'),
    'lake_louise_ab': (51.4254, -116.1773, 'Lake Louise, AB'),
    # Appalachian / Eastern US
    'mount_washington_nh': (44.2706, -71.3033, 'Mount Washington, NH'),
    'syracuse_ny': (43.0481, -76.1474, 'Syracuse, NY'),
    'burlington_vt': (44.4759, -73.2121, 'Burlington, VT'),
    # Great Lakes
    'duluth_mn': (46.79, -92.10, 'Duluth, MN'),
    'marquette_mi': (46.54, -87.39, 'Marquette, MI'),
    'green_bay_wi': (44.51, -88.02, 'Green Bay, WI'),
    'iron_mountain_mi': (45.82, -88.07, 'Iron Mountain, MI'),
    'thunder_bay_on': (48.38, -89.25, 'Thunder Bay, ON'),
    # Canada East/North
    'quebec_city_qc': (46.8139, -71.2080, 'Quebec City, QC'),
    'edmonton_ab': (53.5461, -113.4938, 'Edmonton, AB'),
    'churchill_mb': (58.7684, -94.1636, 'Churchill, MB'),
    'winnipeg_mb': (49.90, -97.14, 'Winnipeg, MB'),

    # === NEW STATIONS ===
    # US Rockies
    'jackson_hole_wy': (43.5877, -110.8279, 'Jackson Hole, WY'),
    'grand_targhee_wy': (43.7903, -110.9572, 'Grand Targhee, WY'),
    'big_sky_mt': (45.2833, -111.4014, 'Big Sky, MT'),
    'sun_valley_id': (43.6952, -114.3514, 'Sun Valley, ID'),
    'park_city_ut': (40.6461, -111.4980, 'Park City, UT'),
    'alta_ut': (40.5884, -111.6386, 'Alta/Snowbird, UT'),
    'taos_nm': (36.5964, -105.4545, 'Taos Ski Valley, NM'),
    'winter_park_co': (39.8841, -105.7625, 'Winter Park, CO'),
    'crested_butte_co': (38.8697, -106.9878, 'Crested Butte, CO'),

    # US Northeast
    'stowe_vt': (44.5303, -72.7814, 'Stowe, VT'),
    'killington_vt': (43.6045, -72.8201, 'Killington, VT'),
    'jay_peak_vt': (44.9275, -72.5053, 'Jay Peak, VT'),
    'sugarloaf_me': (45.0314, -70.3131, 'Sugarloaf, ME'),
    'sunday_river_me': (44.4731, -70.8564, 'Sunday River, ME'),
    'whiteface_ny': (44.3659, -73.9026, 'Whiteface, NY'),
    'gore_mountain_ny': (43.6728, -74.0062, 'Gore Mountain, NY'),

    # US Midwest
    'lutsen_mn': (47.6633, -90.7183, 'Lutsen, MN'),
    'spirit_mountain_mn': (46.7186, -92.2170, 'Spirit Mountain, MN'),
    'granite_peak_wi': (44.9333, -89.6823, 'Granite Peak, WI'),

    # US Pacific
    'crystal_mountain_wa': (46.9350, -121.5047, 'Crystal Mountain, WA'),
    'snoqualmie_pass_wa': (47.4207, -121.4138, 'Snoqualmie Pass, WA'),
    'mt_bachelor_or': (43.9792, -121.6886, 'Mt. Bachelor, OR'),

    # US Additional West
    'schweitzer_id': (48.3678, -116.6228, 'Schweitzer, ID'),
    'bridger_bowl_mt': (45.8172, -110.8969, 'Bridger Bowl, MT'),
    'heavenly_ca': (38.9353, -119.9400, 'Heavenly, CA'),
    'northstar_ca': (39.2746, -120.1210, 'Northstar, CA'),
    'monarch_co': (38.5122, -106.3322, 'Monarch, CO'),
    'ski_cooper_co': (39.3611, -106.3003, 'Ski Cooper, CO'),

    # US Additional East
    'smugglers_notch_vt': (44.5858, -72.7929, 'Smugglers\' Notch, VT'),
    'okemo_vt': (43.4014, -72.7171, 'Okemo, VT'),
    'loon_mountain_nh': (44.0364, -71.6214, 'Loon Mountain, NH'),
    'bretton_woods_nh': (44.2553, -71.4400, 'Bretton Woods, NH'),
    'cannon_mountain_nh': (44.1567, -71.6989, 'Cannon Mountain, NH'),

    # Canada
    'tremblant_qc': (46.2092, -74.5858, 'Tremblant, QC'),
    'big_white_bc': (49.7258, -118.9314, 'Big White, BC'),
    'sun_peaks_bc': (50.8833, -119.8833, 'Sun Peaks, BC'),
    'fernie_bc': (49.4628, -115.0872, 'Fernie, BC'),
    'kicking_horse_bc': (51.2975, -116.9517, 'Kicking Horse, BC'),
    'marmot_basin_ab': (52.8003, -117.6500, 'Marmot Basin, AB'),
    'red_mountain_bc': (49.1047, -117.8456, 'Red Mountain, BC'),
    'blue_mountain_on': (44.5000, -80.3167, 'Blue Mountain, ON'),

    # Additional Canada
    'mont_sainte_anne_qc': (47.0753, -70.9067, 'Mont-Sainte-Anne, QC'),
    'le_massif_qc': (47.2833, -70.6333, 'Le Massif, QC'),
    'panorama_bc': (50.4600, -116.2367, 'Panorama, BC'),
    'nakiska_ab': (50.9433, -115.1517, 'Nakiska, AB'),
}


def haversine_km(lat1, lon1, lat2, lon2):
    """Distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def search_stations_near(lat, lon, radius_km=50):
    """Find GHCN-Daily stations near a location."""
    # NOAA CDO API uses a bounding box via extent parameter
    # But easier to search by coordinates
    headers = {'token': NOAA_TOKEN}

    # Use the stations endpoint with extent (lat/lon bounding box)
    # Approximate bounding box from radius
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * math.cos(math.radians(lat)))

    params = {
        'datasetid': 'GHCND',
        'extent': f'{lat - lat_delta},{lon - lon_delta},{lat + lat_delta},{lon + lon_delta}',
        'limit': 100,
        'sortfield': 'name',
    }

    try:
        response = requests.get(f'{BASE_URL}/stations', headers=headers, params=params, timeout=30)
        if response.status_code == 429:
            print("  Rate limited! Waiting 60s...")
            time.sleep(60)
            response = requests.get(f'{BASE_URL}/stations', headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except Exception as e:
        print(f"  API error: {e}")
        return []


def score_station(station, target_lat, target_lon):
    """Score a NOAA station for suitability. Higher = better."""
    station_id = station['id'].replace('GHCND:', '')
    dist = haversine_km(target_lat, target_lon,
                        station.get('latitude', 0), station.get('longitude', 0))

    # Data coverage score (0-40 points)
    min_date = station.get('mindate', '2020-01-01')
    max_date = station.get('maxdate', '2020-01-01')
    try:
        years = (int(max_date[:4]) - int(min_date[:4]))
    except (ValueError, TypeError):
        years = 0
    coverage_score = min(years, 40)

    # Recency score (0-20 points) - prefer stations current through 2025+
    try:
        max_year = int(max_date[:4])
        recency_score = max(0, min(20, (max_year - 2020) * 5))
    except (ValueError, TypeError):
        recency_score = 0

    # Distance score (0-30 points) - closer is better
    distance_score = max(0, 30 - dist)

    # Station type bonus (0-10 points)
    type_bonus = 0
    if station_id.startswith('USC') or station_id.startswith('CA0'):
        type_bonus = 10  # Core cooperative / Canadian stations
    elif station_id.startswith('USW'):
        type_bonus = 8   # First-order weather stations
    elif station_id.startswith('USS'):
        type_bonus = 5   # Summary of the day

    total = coverage_score + recency_score + distance_score + type_bonus

    return {
        'noaa_id': station_id,
        'name': station.get('name', ''),
        'lat': station.get('latitude'),
        'lon': station.get('longitude'),
        'distance_km': round(dist, 1),
        'min_date': min_date,
        'max_date': max_date,
        'years': years,
        'coverage_score': coverage_score,
        'recency_score': recency_score,
        'distance_score': round(distance_score, 1),
        'type_bonus': type_bonus,
        'total_score': round(total, 1),
        'datacoverage': station.get('datacoverage', 0),
    }


def research_all(radius_km=50, single_resort=None):
    """Research NOAA station mappings for all target resorts."""
    if not NOAA_TOKEN or 'YOUR_' in NOAA_TOKEN:
        print("ERROR: NOAA_API_TOKEN not set in .env")
        return

    results = {}
    targets = TARGET_RESORTS
    if single_resort:
        targets = {k: v for k, v in TARGET_RESORTS.items()
                   if single_resort.lower() in v[2].lower() or single_resort.lower() in k}

    total = len(targets)
    print(f"\nSearching for NOAA stations near {total} ski resorts (radius={radius_km}km)")
    print("=" * 80)

    for i, (station_id, (lat, lon, name)) in enumerate(targets.items(), 1):
        print(f"\n[{i}/{total}] {name} ({lat}, {lon})")

        stations = search_stations_near(lat, lon, radius_km)
        if not stations:
            print(f"  No GHCN stations found within {radius_km}km")
            results[station_id] = {
                'resort_name': name,
                'lat': lat,
                'lon': lon,
                'best_match': None,
                'candidates': [],
            }
            time.sleep(0.3)
            continue

        # Score and rank candidates
        scored = [score_station(s, lat, lon) for s in stations]
        scored.sort(key=lambda x: x['total_score'], reverse=True)

        best = scored[0]
        print(f"  Best: {best['noaa_id']} ({best['name']}) — "
              f"{best['distance_km']}km, {best['years']}yr coverage, "
              f"score={best['total_score']}")

        if len(scored) > 1:
            runner_up = scored[1]
            print(f"  2nd:  {runner_up['noaa_id']} ({runner_up['name']}) — "
                  f"{runner_up['distance_km']}km, score={runner_up['total_score']}")

        results[station_id] = {
            'resort_name': name,
            'lat': lat,
            'lon': lon,
            'best_match': best,
            'candidates': scored[:5],  # top 5
        }

        time.sleep(0.3)  # Rate limit

    # Write results
    output_file = 'noaa_station_mappings.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    # Print summary
    print(f"\n{'=' * 80}")
    print("RESEARCH COMPLETE")
    print(f"{'=' * 80}")

    matched = sum(1 for r in results.values() if r['best_match'])
    unmatched = sum(1 for r in results.values() if not r['best_match'])
    print(f"Matched: {matched}/{total}")
    print(f"Unmatched: {unmatched}/{total}")

    if unmatched:
        print(f"\nUnmatched resorts (try --radius 75):")
        for sid, r in results.items():
            if not r['best_match']:
                print(f"  {r['resort_name']}")

    # Print mapping dict for copy-paste into collect_noaa_data.py
    print(f"\n{'=' * 80}")
    print("NOAA_STATIONS dict for collect_noaa_data.py:")
    print(f"{'=' * 80}")
    print("NOAA_STATIONS = {")
    for sid, r in results.items():
        if r['best_match']:
            b = r['best_match']
            print(f"    '{b['noaa_id']}': {{'station_id': '{sid}', 'name': '{r['resort_name']}'}},")
    print("}")

    print(f"\nFull results saved to {output_file}")
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Research NOAA Station IDs for Ski Resorts')
    parser.add_argument('--radius', type=int, default=50,
                        help='Search radius in km (default: 50)')
    parser.add_argument('--resort', type=str, default=None,
                        help='Search for a single resort (partial name match)')
    args = parser.parse_args()

    research_all(radius_km=args.radius, single_resort=args.resort)
