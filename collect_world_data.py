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

# Expanded daily variables for comprehensive collection
DAILY_VARIABLES = (
    'snowfall_sum,precipitation_sum,rain_sum,'
    'temperature_2m_max,temperature_2m_min,temperature_2m_mean,'
    'apparent_temperature_max,apparent_temperature_min,'
    'wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,'
    'shortwave_radiation_sum,sunshine_duration,'
    'precipitation_hours,weather_code,'
    'et0_fao_evapotranspiration'
)


def migrate_schema(conn):
    """Add new columns to snowfall_daily if they don't exist"""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(snowfall_daily)")
    existing = {row[1] for row in cursor.fetchall()}

    new_columns = {
        'precipitation_mm': 'REAL', 'rain_mm': 'REAL',
        'temp_max_celsius': 'REAL', 'temp_min_celsius': 'REAL',
        'apparent_temp_max': 'REAL', 'apparent_temp_min': 'REAL',
        'wind_speed_max': 'REAL', 'wind_gusts_max': 'REAL',
        'wind_direction_dominant': 'INTEGER',
        'radiation_sum': 'REAL', 'sunshine_duration': 'REAL',
        'precipitation_hours': 'REAL', 'weather_code': 'INTEGER',
        'evapotranspiration': 'REAL',
        'data_source': "TEXT DEFAULT 'open-meteo'",
    }

    for col, dtype in new_columns.items():
        if col not in existing:
            cursor.execute(f"ALTER TABLE snowfall_daily ADD COLUMN {col} {dtype}")
            print(f"  Added column: {col}")

    conn.commit()


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
    # South America
    "south_america": [
        ("bariloche_argentina", -41.1335, -71.3103, "Bariloche, Argentina"),
        ("ushuaia_argentina", -54.8019, -68.3030, "Ushuaia, Argentina"),
        ("santiago_chile", -33.3500, -70.3200, "Santiago/Farellones, Chile"),
        ("punta_arenas_chile", -53.1638, -70.9171, "Punta Arenas, Chile"),
        ("mendoza_argentina", -32.8895, -68.8458, "Mendoza, Argentina"),
    ],
    # Africa Mountains
    "africa_mountains": [
        ("ifrane_morocco", 33.5228, -5.1110, "Ifrane, Morocco"),
        ("midelt_morocco", 32.6801, -4.7400, "Midelt, Morocco"),
        ("mount_kenya_area", -0.0167, 37.0667, "Nanyuki, Kenya"),
        ("lesotho_highlands", -29.2892, 29.0728, "Mokhotlong, Lesotho"),
    ],
    # Central Asia
    "central_asia": [
        ("astana_kazakhstan", 51.1694, 71.4491, "Astana, Kazakhstan"),
        ("almaty_kazakhstan", 43.2220, 76.8512, "Almaty, Kazakhstan"),
        ("bishkek_kyrgyzstan", 42.8746, 74.5698, "Bishkek, Kyrgyzstan"),
        ("dushanbe_tajikistan", 38.5598, 68.7740, "Dushanbe, Tajikistan"),
        ("kabul_afghanistan", 34.5553, 69.2075, "Kabul, Afghanistan"),
    ],
    # Middle East Mountains
    "middle_east_mountains": [
        ("erzurum_turkey", 39.9055, 41.2658, "Erzurum, Turkey"),
        ("kars_turkey", 40.6013, 43.0975, "Kars, Turkey"),
        ("tabriz_iran", 38.0962, 46.2738, "Tabriz, Iran"),
        ("tehran_iran", 35.7448, 51.3753, "Tehran, Iran"),
    ],
    # Australia / New Zealand
    "australia_nz": [
        ("mount_buller_australia", -37.1456, 146.4408, "Mount Buller, Australia"),
        ("thredbo_australia", -36.5059, 148.3064, "Thredbo, Australia"),
        ("queenstown_nz", -45.0312, 168.6626, "Queenstown, NZ"),
        ("mount_cook_nz", -43.7352, 170.0962, "Mount Cook Village, NZ"),
    ],
    # Canada East
    "canada_east": [
        ("quebec_city_qc", 46.8139, -71.2080, "Quebec City, QC"),
        ("sept_iles_qc", 50.2120, -66.3750, "Sept-Iles, QC"),
        ("st_johns_nl", 47.5615, -52.7126, "St. John's, NL"),
    ],
    # Canada North
    "canada_north": [
        ("edmonton_ab", 53.5461, -113.4938, "Edmonton, AB"),
        ("yellowknife_nt", 62.4540, -114.3718, "Yellowknife, NT"),
        ("churchill_mb", 58.7684, -94.1636, "Churchill, MB"),
    ],
    # Appalachian / Eastern US
    "appalachian_eastern_us": [
        ("mount_washington_nh", 44.2706, -71.3033, "Mount Washington, NH"),
        ("syracuse_ny", 43.0481, -76.1474, "Syracuse, NY"),
        ("burlington_vt", 44.4759, -73.2121, "Burlington, VT"),
        ("elkins_wv", 38.9262, -79.8467, "Elkins, WV"),
        ("boone_nc", 36.2168, -81.6746, "Boone, NC"),
    ],
    # Southern Hemisphere Reference
    "southern_hemisphere_ref": [
        ("grytviken_south_georgia", -54.2811, -36.5092, "Grytviken, South Georgia"),
        ("hobart_australia", -42.8821, 147.3272, "Hobart, Australia"),
        ("stanley_falklands", -51.6975, -57.8517, "Stanley, Falkland Islands"),
    ],
    # Europe Additional
    "europe_additional": [
        ("tbilisi_georgia", 41.7151, 44.8271, "Tbilisi, Georgia"),
        ("moscow_russia", 55.7558, 37.6173, "Moscow, Russia"),
        ("helsinki_finland", 60.1699, 24.9384, "Helsinki, Finland"),
    ],
    # US Rockies (expanded)
    "us_rockies_north": [
        ("jackson_hole_wy", 43.5877, -110.8279, "Jackson Hole, WY"),
        ("grand_targhee_wy", 43.7903, -110.9572, "Grand Targhee, WY"),
        ("big_sky_mt", 45.2833, -111.4014, "Big Sky, MT"),
        ("sun_valley_id", 43.6952, -114.3514, "Sun Valley, ID"),
        ("bridger_bowl_mt", 45.8172, -110.8969, "Bridger Bowl, MT"),
        ("schweitzer_id", 48.3678, -116.6228, "Schweitzer, ID"),
    ],
    "us_rockies_utah": [
        ("park_city_ut", 40.6461, -111.4980, "Park City, UT"),
        ("alta_ut", 40.5884, -111.6386, "Alta/Snowbird, UT"),
    ],
    "us_rockies_co_expanded": [
        ("winter_park_co", 39.8841, -105.7625, "Winter Park, CO"),
        ("crested_butte_co", 38.8697, -106.9878, "Crested Butte, CO"),
        ("monarch_co", 38.5122, -106.3322, "Monarch, CO"),
        ("ski_cooper_co", 39.3611, -106.3003, "Ski Cooper, CO"),
        ("taos_nm", 36.5964, -105.4545, "Taos Ski Valley, NM"),
    ],
    # US Northeast Ski
    "us_northeast_ski": [
        ("stowe_vt", 44.5303, -72.7814, "Stowe, VT"),
        ("killington_vt", 43.6045, -72.8201, "Killington, VT"),
        ("jay_peak_vt", 44.9275, -72.5053, "Jay Peak, VT"),
        ("smugglers_notch_vt", 44.5858, -72.7929, "Smugglers' Notch, VT"),
        ("okemo_vt", 43.4014, -72.7171, "Okemo, VT"),
        ("sugarloaf_me", 45.0314, -70.3131, "Sugarloaf, ME"),
        ("sunday_river_me", 44.4731, -70.8564, "Sunday River, ME"),
        ("whiteface_ny", 44.3659, -73.9026, "Whiteface, NY"),
        ("gore_mountain_ny", 43.6728, -74.0062, "Gore Mountain, NY"),
        ("loon_mountain_nh", 44.0364, -71.6214, "Loon Mountain, NH"),
        ("bretton_woods_nh", 44.2553, -71.4400, "Bretton Woods, NH"),
        ("cannon_mountain_nh", 44.1567, -71.6989, "Cannon Mountain, NH"),
    ],
    # US Midwest Ski
    "us_midwest_ski": [
        ("lutsen_mn", 47.6633, -90.7183, "Lutsen, MN"),
        ("spirit_mountain_mn", 46.7186, -92.2170, "Spirit Mountain, MN"),
        ("granite_peak_wi", 44.9333, -89.6823, "Granite Peak, WI"),
    ],
    # US Pacific Expanded
    "us_pacific_ski": [
        ("crystal_mountain_wa", 46.9350, -121.5047, "Crystal Mountain, WA"),
        ("snoqualmie_pass_wa", 47.4207, -121.4138, "Snoqualmie Pass, WA"),
        ("mt_bachelor_or", 43.9792, -121.6886, "Mt. Bachelor, OR"),
        ("heavenly_ca", 38.9353, -119.9400, "Heavenly, CA"),
        ("northstar_ca", 39.2746, -120.1210, "Northstar, CA"),
    ],
    # Canada BC Ski
    "canada_bc_ski": [
        ("big_white_bc", 49.7258, -118.9314, "Big White, BC"),
        ("sun_peaks_bc", 50.8833, -119.8833, "Sun Peaks, BC"),
        ("fernie_bc", 49.4628, -115.0872, "Fernie, BC"),
        ("kicking_horse_bc", 51.2975, -116.9517, "Kicking Horse, BC"),
        ("red_mountain_bc", 49.1047, -117.8456, "Red Mountain, BC"),
        ("panorama_bc", 50.4600, -116.2367, "Panorama, BC"),
    ],
    # Canada Alberta Ski
    "canada_ab_ski": [
        ("marmot_basin_ab", 52.8003, -117.6500, "Marmot Basin, AB"),
        ("nakiska_ab", 50.9433, -115.1517, "Nakiska, AB"),
    ],
    # Canada East Ski
    "canada_east_ski": [
        ("tremblant_qc", 46.2092, -74.5858, "Tremblant, QC"),
        ("mont_sainte_anne_qc", 47.0753, -70.9067, "Mont-Sainte-Anne, QC"),
        ("le_massif_qc", 47.2833, -70.6333, "Le Massif, QC"),
        ("blue_mountain_on", 44.5000, -80.3167, "Blue Mountain, ON"),
    ],
}


def ensure_station_exists(cursor, station_id, lat, lon, name, region):
    """Make sure station is registered in the database"""
    cursor.execute("""
        INSERT OR IGNORE INTO stations (station_id, name, latitude, longitude, region, significance)
        VALUES (?, ?, ?, ?, ?, 'open-meteo')
    """, (station_id, name, lat, lon, region))


def update_station(cursor, station_id, lat, lon, name, days_back=14):
    """Update a single station with recent data (expanded variables)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'daily': DAILY_VARIABLES,
        'timezone': 'UTC'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'daily' not in data or 'time' not in data['daily']:
            return 0, None

        d = data['daily']
        dates = d['time']
        n = len(dates)

        def get(key):
            return d.get(key, [None] * n)

        records = 0
        recent_snow = []

        for i, date in enumerate(dates):
            snow_raw = get('snowfall_sum')[i]
            snow_mm = snow_raw * 10.0 if snow_raw is not None else None
            temp_mean = None
            t_max = get('temperature_2m_max')[i]
            t_min = get('temperature_2m_min')[i]
            if t_max is not None and t_min is not None:
                temp_mean = (t_max + t_min) / 2.0

            cursor.execute("""
                INSERT INTO snowfall_daily
                (station_id, date, snowfall_mm, temp_mean_celsius,
                 precipitation_mm, rain_mm, temp_max_celsius, temp_min_celsius,
                 apparent_temp_max, apparent_temp_min,
                 wind_speed_max, wind_gusts_max, wind_direction_dominant,
                 radiation_sum, sunshine_duration,
                 precipitation_hours, weather_code, evapotranspiration,
                 data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open-meteo')
                ON CONFLICT(station_id, date) DO UPDATE SET
                    snowfall_mm = excluded.snowfall_mm,
                    temp_mean_celsius = excluded.temp_mean_celsius,
                    precipitation_mm = excluded.precipitation_mm,
                    rain_mm = excluded.rain_mm,
                    temp_max_celsius = excluded.temp_max_celsius,
                    temp_min_celsius = excluded.temp_min_celsius,
                    apparent_temp_max = excluded.apparent_temp_max,
                    apparent_temp_min = excluded.apparent_temp_min,
                    wind_speed_max = excluded.wind_speed_max,
                    wind_gusts_max = excluded.wind_gusts_max,
                    wind_direction_dominant = excluded.wind_direction_dominant,
                    radiation_sum = excluded.radiation_sum,
                    sunshine_duration = excluded.sunshine_duration,
                    precipitation_hours = excluded.precipitation_hours,
                    weather_code = excluded.weather_code,
                    evapotranspiration = excluded.evapotranspiration,
                    data_source = 'open-meteo'
                WHERE snowfall_daily.data_source != 'noaa'
            """, (
                station_id, date, snow_mm, temp_mean,
                get('precipitation_sum')[i], get('rain_sum')[i],
                t_max, t_min,
                get('apparent_temperature_max')[i], get('apparent_temperature_min')[i],
                get('wind_speed_10m_max')[i], get('wind_gusts_10m_max')[i],
                get('wind_direction_10m_dominant')[i],
                get('shortwave_radiation_sum')[i], get('sunshine_duration')[i],
                get('precipitation_hours')[i], get('weather_code')[i],
                get('et0_fao_evapotranspiration')[i],
            ))
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
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        migrate_schema(conn)
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
    finally:
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
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
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
    finally:
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
