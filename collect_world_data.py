#!/usr/bin/env python3
"""
Collect World Data - Update all global stations with recent data
Designed to run daily on the Pi and write to NAS-mounted database
"""

import requests
import sqlite3
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Database path - uses NAS mount on Pi, local file otherwise
DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')

# All global stations organized by region
WORLD_STATIONS = {
    # Northern Wisconsin (Primary targets)
    "northern_wisconsin": [
        ("phelps_wi", 46.0638, -89.0787, "Phelps, WI"),
        ("land_o_lakes_wi", 46.1535, -89.3207, "Land O'Lakes, WI"),
        ("eagle_river_wi", 45.9169, -89.2443, "Eagle River, WI"),
    ],
    # Lake Superior Snow Belt
    "lake_superior": [
        ("houghton_mi", 47.12, -88.57, "Houghton, MI (Keweenaw)"),
        ("hancock_mi", 47.13, -88.58, "Hancock, MI"),
        ("ashland_wi", 46.59, -90.88, "Ashland, WI"),
        ("ironwood_mi", 46.45, -90.17, "Ironwood, MI"),
        ("grand_marais_mn", 47.75, -90.34, "Grand Marais, MN"),
        ("marquette_mi", 46.54, -87.39, "Marquette, MI"),
        ("duluth_mn", 46.79, -92.10, "Duluth, MN"),
    ],
    # Regional Reference
    "regional": [
        ("green_bay_wi", 44.51, -88.02, "Green Bay, WI"),
        ("iron_mountain_mi", 45.82, -88.07, "Iron Mountain, MI"),
        ("thunder_bay_on", 48.38, -89.25, "Thunder Bay, ON"),
        ("winnipeg_mb", 49.90, -97.14, "Winnipeg, MB"),
    ],
    # California Ski
    "california_ski": [
        ("mammoth_mountain_ca", 37.6308, -119.0326, "Mammoth Mountain, CA"),
        ("lake_tahoe_ca", 39.1969, -120.2356, "Lake Tahoe, CA"),
        ("mount_shasta_ca", 41.4092, -122.1949, "Mount Shasta, CA"),
        ("big_bear_ca", 34.2406, -116.9114, "Big Bear, CA"),
    ],
    # Colorado Rockies
    "colorado_rockies": [
        ("aspen_co", 39.1911, -106.8175, "Aspen, CO"),
        ("vail_co", 39.6403, -106.3742, "Vail, CO"),
        ("steamboat_springs_co", 40.4850, -106.8317, "Steamboat Springs, CO"),
        ("telluride_co", 37.9375, -107.8123, "Telluride, CO"),
        ("breckenridge_co", 39.4817, -106.0384, "Breckenridge, CO"),
    ],
    # Pacific Northwest
    "pacific_northwest": [
        ("mount_baker_wa", 48.8568, -121.6714, "Mount Baker, WA"),
        ("stevens_pass_wa", 47.7465, -121.0890, "Stevens Pass, WA"),
        ("mount_hood_or", 45.3306, -121.7081, "Mount Hood, OR"),
    ],
    # Siberia West
    "siberia_west": [
        ("novosibirsk_russia", 55.0084, 82.9357, "Novosibirsk, Russia"),
        ("omsk_russia", 54.9885, 73.3242, "Omsk, Russia"),
        ("tomsk_russia", 56.4977, 84.9744, "Tomsk, Russia"),
    ],
    # Siberia Central
    "siberia_central": [
        ("krasnoyarsk_russia", 56.0153, 92.8932, "Krasnoyarsk, Russia"),
        ("irkutsk_russia", 52.2978, 104.2964, "Irkutsk, Russia"),
        ("yakutsk_russia", 62.0355, 129.6755, "Yakutsk, Russia"),
    ],
    # Siberia East
    "siberia_east": [
        ("magadan_russia", 59.5686, 150.8103, "Magadan, Russia"),
        ("kamchatka_russia", 53.0245, 158.6433, "Kamchatka, Russia"),
    ],
    # Arctic Russia
    "arctic_russia": [
        ("murmansk_russia", 68.9585, 33.0827, "Murmansk, Russia"),
        ("norilsk_russia", 69.3558, 88.1893, "Norilsk, Russia"),
        ("tiksi_russia", 71.6419, 128.8739, "Tiksi, Russia"),
    ],
    # Japan North
    "japan_north": [
        ("sapporo_japan", 43.0642, 141.3469, "Sapporo, Japan"),
        ("niseko_japan", 42.8048, 140.6874, "Niseko, Japan"),
        ("asahikawa_japan", 43.7706, 142.3650, "Asahikawa, Japan"),
    ],
    # Japan Sea Coast
    "japan_sea_coast": [
        ("niigata_japan", 37.9026, 139.0232, "Niigata, Japan"),
        ("toyama_japan", 36.6959, 137.2137, "Toyama, Japan"),
        ("kanazawa_japan", 36.5613, 136.6562, "Kanazawa, Japan"),
    ],
    # Japan Mountains
    "japan_mountains": [
        ("nagano_japan", 36.6513, 138.1809, "Nagano, Japan"),
        ("hakuba_japan", 36.6996, 137.8616, "Hakuba, Japan"),
    ],
    # China Northeast
    "china_northeast": [
        ("harbin_china", 45.8038, 126.5340, "Harbin, China"),
        ("changchun_china", 43.8171, 125.3235, "Changchun, China"),
        ("shenyang_china", 41.8057, 123.4328, "Shenyang, China"),
    ],
    # China Northwest
    "china_northwest": [
        ("urumqi_china", 43.8256, 87.6168, "Urumqi, China"),
        ("lanzhou_china", 36.0611, 103.8343, "Lanzhou, China"),
    ],
    # China Tibet
    "china_tibetan": [
        ("lhasa_tibet", 29.6500, 91.1000, "Lhasa, Tibet"),
        ("xining_china", 36.6171, 101.7782, "Xining, China"),
    ],
    # Alps Western
    "alps_western": [
        ("chamonix_france", 45.9237, 6.8694, "Chamonix, France"),
        ("zermatt_switzerland", 46.0207, 7.7491, "Zermatt, Switzerland"),
        ("st_moritz_switzerland", 46.4908, 9.8355, "St. Moritz, Switzerland"),
    ],
    # Alps Eastern
    "alps_eastern": [
        ("innsbruck_austria", 47.2692, 11.4041, "Innsbruck, Austria"),
        ("cortina_italy", 46.5369, 12.1357, "Cortina d'Ampezzo, Italy"),
    ],
    # Scandinavia
    "scandinavia": [
        ("tromso_norway", 69.6492, 18.9553, "Tromsø, Norway"),
        ("kiruna_sweden", 67.8558, 20.2253, "Kiruna, Sweden"),
        ("rovaniemi_finland", 66.5039, 25.7294, "Rovaniemi, Finland"),
    ],
    # Canada West
    "canada_west": [
        ("whistler_bc", 50.1163, -122.9574, "Whistler, BC"),
        ("revelstoke_bc", 50.9981, -118.1957, "Revelstoke, BC"),
    ],
    # Canada Rockies
    "canada_rockies": [
        ("banff_ab", 51.1784, -115.5708, "Banff, AB"),
        ("lake_louise_ab", 51.4254, -116.1773, "Lake Louise, AB"),
    ],
}


def ensure_station_exists(cursor, station_id, lat, lon, name, region):
    """Make sure station is registered in the database"""
    cursor.execute("""
        INSERT OR IGNORE INTO stations (station_id, name, latitude, longitude, region, data_source)
        VALUES (?, ?, ?, ?, ?, 'open-meteo')
    """, (station_id, name, lat, lon, region))


def update_station(cursor, station_id, lat, lon, name, days_back=14):
    """Update a single station with recent data"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'daily': 'snowfall_sum,temperature_2m_max,temperature_2m_min,precipitation_sum',
        'timezone': 'UTC'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'daily' not in data or 'time' not in data['daily']:
            return 0, None

        dates = data['daily']['time']
        snowfall = data['daily'].get('snowfall_sum', [None] * len(dates))
        temp_max = data['daily'].get('temperature_2m_max', [None] * len(dates))
        temp_min = data['daily'].get('temperature_2m_min', [None] * len(dates))
        precip = data['daily'].get('precipitation_sum', [None] * len(dates))

        records = 0
        recent_snow = []

        for i, date in enumerate(dates):
            snow_mm = snowfall[i] * 10.0 if snowfall[i] is not None else None

            cursor.execute("""
                INSERT OR REPLACE INTO snowfall_daily
                (station_id, date, snowfall_mm, temp_max_celsius, temp_min_celsius, precipitation_mm)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (station_id, date, snow_mm, temp_max[i], temp_min[i], precip[i]))
            records += 1

            if snow_mm and snow_mm > 5:
                recent_snow.append((date, snow_mm))

        return records, recent_snow

    except Exception as e:
        return -1, str(e)


def collect_world_data(days_back=14, rate_limit=0.3):
    """Collect recent data for all world stations"""

    print(f"\n{'='*80}")
    print("WORLD DATA COLLECTION")
    print(f"{'='*80}")
    print(f"Database: {DB_PATH}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Days back: {days_back}")

    # Count total stations
    total_stations = sum(len(stations) for stations in WORLD_STATIONS.values())
    print(f"Total stations: {total_stations}")
    print(f"{'='*80}\n")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    success_count = 0
    fail_count = 0
    current = 0

    for region, stations in WORLD_STATIONS.items():
        print(f"\n[{region.upper().replace('_', ' ')}]")
        print("-" * 60)

        for station_id, lat, lon, name in stations:
            current += 1

            # Ensure station is registered
            ensure_station_exists(cursor, station_id, lat, lon, name, region)

            print(f"  [{current}/{total_stations}] {name}...", end=" ", flush=True)

            records, result = update_station(cursor, station_id, lat, lon, name, days_back)

            if records >= 0:
                success_count += 1
                print(f"✅ {records} records")
                if result:  # recent snow events
                    for date, snow_mm in result[-3:]:  # show last 3
                        print(f"       {date}: {snow_mm:.1f}mm ❄️")
            else:
                fail_count += 1
                print(f"❌ Error: {result}")

            conn.commit()
            time.sleep(rate_limit)

    conn.close()

    print(f"\n{'='*80}")
    print("COLLECTION COMPLETE")
    print(f"{'='*80}")
    print(f"Successful: {success_count}/{total_stations}")
    print(f"Failed: {fail_count}/{total_stations}")
    print(f"{'='*80}\n")

    return success_count, fail_count


def get_database_stats():
    """Print database statistics"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(DISTINCT station_id) FROM snowfall_daily")
    stations = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM snowfall_daily")
    records = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(date), MAX(date) FROM snowfall_daily")
    date_range = cursor.fetchone()

    cursor.execute("""
        SELECT station_id, COUNT(*) as cnt, MAX(date) as latest
        FROM snowfall_daily
        GROUP BY station_id
        ORDER BY latest DESC
        LIMIT 10
    """)
    recent = cursor.fetchall()

    conn.close()

    print(f"\n{'='*80}")
    print("DATABASE STATISTICS")
    print(f"{'='*80}")
    print(f"Database: {DB_PATH}")
    print(f"Stations: {stations}")
    print(f"Records: {records:,}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    print(f"\nMost recently updated stations:")
    for station_id, cnt, latest in recent:
        print(f"  {station_id}: {cnt:,} records, latest: {latest}")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Collect World Weather Data')
    parser.add_argument('--days', type=int, default=14, help='Days of history to fetch (default: 14)')
    parser.add_argument('--rate-limit', type=float, default=0.3, help='Seconds between API calls (default: 0.3)')
    parser.add_argument('--stats', action='store_true', help='Show database statistics only')

    args = parser.parse_args()

    if args.stats:
        get_database_stats()
    else:
        collect_world_data(days_back=args.days, rate_limit=args.rate_limit)
        get_database_stats()
