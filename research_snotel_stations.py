#!/usr/bin/env python3
"""
Research SNOTEL Station IDs for Ski Resorts

Queries the NRCS Report Generator to find all SNOTEL stations in western US states,
then matches the closest station(s) to each ski resort.

No auth required — SNOTEL data is free and public.

Usage:
    python research_snotel_stations.py
    python research_snotel_stations.py --max-distance 30
    python research_snotel_stations.py --resort mammoth
"""

import requests
import json
import math
import time
import csv
import io
import argparse


# NRCS Report Generator base URL
REPORT_URL = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv"

# States with SNOTEL stations relevant to our ski resorts
SNOTEL_STATES = ['CO', 'UT', 'WY', 'MT', 'ID', 'WA', 'OR', 'CA', 'NM', 'NV', 'AZ']

# Western ski resorts to match — station_id -> (lat, lon, name)
TARGET_RESORTS = {
    # Colorado
    'aspen_co': (39.1911, -106.8175, 'Aspen, CO'),
    'vail_co': (39.6403, -106.3742, 'Vail, CO'),
    'steamboat_springs_co': (40.4850, -106.8317, 'Steamboat Springs, CO'),
    'telluride_co': (37.9375, -107.8123, 'Telluride, CO'),
    'breckenridge_co': (39.4817, -106.0384, 'Breckenridge, CO'),
    'winter_park_co': (39.8841, -105.7625, 'Winter Park, CO'),
    'crested_butte_co': (38.8697, -106.9878, 'Crested Butte, CO'),
    'monarch_co': (38.5122, -106.3322, 'Monarch, CO'),
    'ski_cooper_co': (39.3611, -106.3003, 'Ski Cooper, CO'),
    # Wyoming
    'jackson_hole_wy': (43.5877, -110.8279, 'Jackson Hole, WY'),
    'grand_targhee_wy': (43.7903, -110.9572, 'Grand Targhee, WY'),
    # Montana
    'big_sky_mt': (45.2833, -111.4014, 'Big Sky, MT'),
    'bridger_bowl_mt': (45.8172, -110.8969, 'Bridger Bowl, MT'),
    # Idaho
    'sun_valley_id': (43.6952, -114.3514, 'Sun Valley, ID'),
    'schweitzer_id': (48.3678, -116.6228, 'Schweitzer, ID'),
    # Utah
    'park_city_ut': (40.6461, -111.4980, 'Park City, UT'),
    'alta_ut': (40.5884, -111.6386, 'Alta/Snowbird, UT'),
    # New Mexico
    'taos_nm': (36.5964, -105.4545, 'Taos Ski Valley, NM'),
    # Washington
    'mount_baker_wa': (48.8568, -121.6714, 'Mount Baker, WA'),
    'stevens_pass_wa': (47.7465, -121.0890, 'Stevens Pass, WA'),
    'crystal_mountain_wa': (46.9350, -121.5047, 'Crystal Mountain, WA'),
    'snoqualmie_pass_wa': (47.4207, -121.4138, 'Snoqualmie Pass, WA'),
    # Oregon
    'mount_hood_or': (45.3306, -121.7081, 'Mount Hood, OR'),
    'mt_bachelor_or': (43.9792, -121.6886, 'Mt. Bachelor, OR'),
    # California
    'mammoth_mountain_ca': (37.6308, -119.0326, 'Mammoth Mountain, CA'),
    'lake_tahoe_ca': (39.1969, -120.2356, 'Lake Tahoe, CA'),
    'mount_shasta_ca': (41.4092, -122.1949, 'Mount Shasta, CA'),
    'big_bear_ca': (34.2406, -116.9114, 'Big Bear, CA'),
    'heavenly_ca': (38.9353, -119.9400, 'Heavenly, CA'),
    'northstar_ca': (39.2746, -120.1210, 'Northstar, CA'),
    # Canada BC/AB — SNOTEL doesn't cover Canada, but including for reference
    'whistler_bc': (50.1163, -122.9574, 'Whistler, BC'),
    'revelstoke_bc': (50.9981, -118.1957, 'Revelstoke, BC'),
    'banff_ab': (51.1784, -115.5708, 'Banff, AB'),
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


def fetch_state_stations(state):
    """Fetch all SNOTEL stations in a state from NRCS yearcount listing."""
    import re

    url = f"https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state={state}&counttype=statelist"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"  Error fetching {state} stations: {e}")
        return []

    # Parse HTML table — each station has 10 <td> elements
    tds = re.findall(r'<td>(.*?)</td>', response.text)
    stations = []
    for i in range(0, len(tds), 10):
        row = tds[i:i + 10]
        if len(row) >= 8:
            name_match = re.match(r'(.+?)\s*\((\d+)\)', row[2].strip())
            if name_match:
                name = name_match.group(1).strip()
                num = name_match.group(2)
                try:
                    lat = float(row[5].strip())
                    lon = float(row[6].strip())
                    elev = int(float(row[7].strip()))
                    stations.append({
                        'snotel_id': num,
                        'triplet': f"{num}:{state}:SNTL",
                        'name': name,
                        'elevation_ft': elev,
                        'lat': lat,
                        'lon': lon,
                        'state': state,
                    })
                except (ValueError, IndexError):
                    continue

    return stations


def find_best_snotel(resort_lat, resort_lon, all_stations, max_distance_km=40):
    """Find SNOTEL stations near a resort, ranked by distance."""
    candidates = []
    for station in all_stations:
        dist = haversine_km(resort_lat, resort_lon, station['lat'], station['lon'])
        if dist <= max_distance_km:
            candidates.append({
                **station,
                'distance_km': round(dist, 1),
            })

    candidates.sort(key=lambda x: x['distance_km'])
    return candidates


def research_all(max_distance=40, single_resort=None):
    """Find SNOTEL stations for all western ski resorts."""

    # Step 1: Fetch all SNOTEL stations
    print("Fetching SNOTEL station inventory...")
    print("=" * 80)
    all_stations = []
    for state in SNOTEL_STATES:
        stations = fetch_state_stations(state)
        print(f"  {state}: {len(stations)} SNOTEL stations")
        all_stations.extend(stations)
        time.sleep(0.5)

    print(f"\nTotal SNOTEL stations found: {len(all_stations)}")
    print("=" * 80)

    # Step 2: Match resorts to stations
    targets = TARGET_RESORTS
    if single_resort:
        targets = {k: v for k, v in TARGET_RESORTS.items()
                   if single_resort.lower() in v[2].lower() or single_resort.lower() in k}

    results = {}
    print(f"\nMatching {len(targets)} resorts to nearby SNOTEL stations (max {max_distance}km)")
    print("=" * 80)

    for station_id, (lat, lon, name) in targets.items():
        matches = find_best_snotel(lat, lon, all_stations, max_distance)

        if matches:
            best = matches[0]
            print(f"\n  {name}")
            print(f"    Best: {best['triplet']} ({best['name']}) — "
                  f"{best['distance_km']}km, {best['elevation_ft']}ft")
            if len(matches) > 1:
                print(f"    2nd:  {matches[1]['triplet']} ({matches[1]['name']}) — "
                      f"{matches[1]['distance_km']}km, {matches[1]['elevation_ft']}ft")
        else:
            print(f"\n  {name} — NO SNOTEL STATION within {max_distance}km")

        results[station_id] = {
            'resort_name': name,
            'lat': lat,
            'lon': lon,
            'best_match': matches[0] if matches else None,
            'candidates': matches[:5],
        }

    # Save full results
    output_file = 'snotel_station_mappings.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    # Print summary
    print(f"\n{'=' * 80}")
    print("SNOTEL RESEARCH COMPLETE")
    print(f"{'=' * 80}")

    matched = sum(1 for r in results.values() if r['best_match'])
    unmatched = sum(1 for r in results.values() if not r['best_match'])
    print(f"Matched: {matched}/{len(targets)}")
    print(f"Unmatched: {unmatched}/{len(targets)}")

    if unmatched:
        print(f"\nNo SNOTEL coverage (try --max-distance 60):")
        for sid, r in results.items():
            if not r['best_match']:
                print(f"  {r['resort_name']}")

    # Print dict for collect_snotel_data.py
    print(f"\n{'=' * 80}")
    print("SNOTEL_STATIONS dict for collect_snotel_data.py:")
    print(f"{'=' * 80}")
    print("SNOTEL_STATIONS = {")
    for sid, r in results.items():
        if r['best_match']:
            b = r['best_match']
            print(f"    '{b['triplet']}': {{'station_id': '{sid}', "
                  f"'name': '{r['resort_name']}', "
                  f"'elevation_ft': {b['elevation_ft']}}},")
    print("}")

    # Save station inventory too
    inventory_file = 'snotel_all_stations.json'
    with open(inventory_file, 'w') as f:
        json.dump(all_stations, f, indent=2)

    print(f"\nFull results: {output_file}")
    print(f"Station inventory: {inventory_file}")
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Research SNOTEL Stations for Ski Resorts')
    parser.add_argument('--max-distance', type=int, default=40,
                        help='Max distance in km (default: 40)')
    parser.add_argument('--resort', type=str, default=None,
                        help='Single resort search (partial name match)')
    args = parser.parse_args()

    research_all(max_distance=args.max_distance, single_resort=args.resort)
