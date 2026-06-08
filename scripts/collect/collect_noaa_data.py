#!/usr/bin/env python3
"""
Collect NOAA GHCN-Daily Data — Unified Collector

Writes directly to snowfall_daily with data_source='noaa'.
Uses the Climate Data Online (CDO) API v2 for observed station data.

Modes:
  --mode daily   : Fetch last N days (default 14) for all stations
  --mode backfill: Year-by-year historical collection, budget-aware

NOAA GHCN-Daily includes: TMAX, TMIN, PRCP, SNOW, SNWD, AWND — some stations back to 1800s.
With units=metric, values come in standard units (C, mm) — no division needed.

Rate limits: 5 requests/second, 10,000 requests/day
"""

import requests
import sqlite3
import os
import time
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')
NOAA_TOKEN = os.environ.get('NOAA_API_TOKEN', '')

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

# GHCN-Daily datatypes we want
DATATYPES = 'TMAX,TMIN,PRCP,SNOW,SNWD,AWND,WSF2,WSF5,TAVG,WESD'

# NOAA station IDs mapped to our station network
# Format: GHCN station ID -> {station_id, name}
# Found via research_noaa_stations.py and NOAA CDO station search
NOAA_STATIONS = {
    # === Great Lakes / Wisconsin ===
    'USW00014898': {'station_id': 'green_bay_wi', 'name': 'Green Bay, WI'},
    'USW00014839': {'station_id': 'duluth_mn', 'name': 'Duluth, MN'},
    'USW00014858': {'station_id': 'marquette_mi', 'name': 'Marquette, MI'},
    'USW00094892': {'station_id': 'iron_mountain_mi', 'name': 'Iron Mountain, MI'},
    'USW00014733': {'station_id': 'minneapolis_mn', 'name': 'Minneapolis, MN'},
    'USW00094847': {'station_id': 'detroit_mi', 'name': 'Detroit, MI'},
    'USW00014850': {'station_id': 'milwaukee_wi', 'name': 'Milwaukee, WI'},
    'USW00094860': {'station_id': 'chicago_il', 'name': 'Chicago, IL'},

    # === Eastern US / Appalachian ===
    'USW00014771': {'station_id': 'syracuse_ny', 'name': 'Syracuse, NY'},
    'USW00014742': {'station_id': 'burlington_vt', 'name': 'Burlington, VT'},
    'USW00014735': {'station_id': 'buffalo_ny', 'name': 'Buffalo, NY'},
    'USW00014764': {'station_id': 'mount_washington_nh', 'name': 'Mount Washington, NH'},
    'USW00014740': {'station_id': 'caribou_me', 'name': 'Caribou, ME'},

    # === Colorado Rockies ===
    'USW00093073': {'station_id': 'steamboat_springs_co', 'name': 'Steamboat Springs, CO'},
    'USW00023062': {'station_id': 'aspen_co', 'name': 'Aspen, CO'},
    'USC00052281': {'station_id': 'vail_co', 'name': 'Vail, CO'},
    'USC00058204': {'station_id': 'telluride_co', 'name': 'Telluride, CO'},
    'USC00050848': {'station_id': 'breckenridge_co', 'name': 'Breckenridge, CO'},
    'USC00059175': {'station_id': 'winter_park_co', 'name': 'Winter Park, CO'},
    'USC00051959': {'station_id': 'crested_butte_co', 'name': 'Crested Butte, CO'},
    'USC00055722': {'station_id': 'monarch_co', 'name': 'Monarch, CO'},
    'USC00054834': {'station_id': 'ski_cooper_co', 'name': 'Ski Cooper, CO'},
    'USC00058429': {'station_id': 'taos_nm', 'name': 'Taos Ski Valley, NM'},

    # === US Rockies North ===
    'USC00486428': {'station_id': 'jackson_hole_wy', 'name': 'Jackson Hole, WY'},
    'USC00480027': {'station_id': 'grand_targhee_wy', 'name': 'Grand Targhee, WY'},
    'USC00241044': {'station_id': 'big_sky_mt', 'name': 'Big Sky, MT'},
    'USW00024141': {'station_id': 'sun_valley_id', 'name': 'Sun Valley, ID'},
    'USC00241083': {'station_id': 'bridger_bowl_mt', 'name': 'Bridger Bowl, MT'},
    'USC00108380': {'station_id': 'schweitzer_id', 'name': 'Schweitzer, ID'},

    # === US Rockies Utah ===
    'USC00426357': {'station_id': 'park_city_ut', 'name': 'Park City, UT'},
    'USC00420072': {'station_id': 'alta_ut', 'name': 'Alta/Snowbird, UT'},

    # === Pacific Northwest ===
    'USW00024233': {'station_id': 'mount_hood_or', 'name': 'Mount Hood, OR'},
    'USC00454764': {'station_id': 'mount_baker_wa', 'name': 'Mount Baker, WA'},
    'USC00457773': {'station_id': 'stevens_pass_wa', 'name': 'Stevens Pass, WA'},
    'USC00451992': {'station_id': 'crystal_mountain_wa', 'name': 'Crystal Mountain, WA'},
    'USC00457507': {'station_id': 'snoqualmie_pass_wa', 'name': 'Snoqualmie Pass, WA'},
    'USC00350694': {'station_id': 'mt_bachelor_or', 'name': 'Mt. Bachelor, OR'},

    # === California Ski ===
    'USC00045400': {'station_id': 'mammoth_mountain_ca', 'name': 'Mammoth Mountain, CA'},
    'USC00048758': {'station_id': 'lake_tahoe_ca', 'name': 'Lake Tahoe, CA'},
    'USC00045693': {'station_id': 'mount_shasta_ca', 'name': 'Mount Shasta, CA'},
    'USC00040741': {'station_id': 'big_bear_ca', 'name': 'Big Bear, CA'},
    'USC00043714': {'station_id': 'heavenly_ca', 'name': 'Heavenly, CA'},
    'USC00046730': {'station_id': 'northstar_ca', 'name': 'Northstar, CA'},

    # === US Northeast Ski ===
    'USC00438247': {'station_id': 'stowe_vt', 'name': 'Stowe, VT'},
    'USC00434551': {'station_id': 'killington_vt', 'name': 'Killington, VT'},
    'USC00434277': {'station_id': 'jay_peak_vt', 'name': 'Jay Peak, VT'},
    'USC00438270': {'station_id': 'smugglers_notch_vt', 'name': "Smugglers' Notch, VT"},
    'USC00436237': {'station_id': 'okemo_vt', 'name': 'Okemo, VT'},
    'USC00178817': {'station_id': 'sugarloaf_me', 'name': 'Sugarloaf, ME'},
    'USC00170844': {'station_id': 'sunday_river_me', 'name': 'Sunday River, ME'},
    'USC00309670': {'station_id': 'whiteface_ny', 'name': 'Whiteface, NY'},
    'USC00303184': {'station_id': 'gore_mountain_ny', 'name': 'Gore Mountain, NY'},
    'USC00274399': {'station_id': 'loon_mountain_nh', 'name': 'Loon Mountain, NH'},
    'USC00270913': {'station_id': 'bretton_woods_nh', 'name': 'Bretton Woods, NH'},
    'USC00271105': {'station_id': 'cannon_mountain_nh', 'name': 'Cannon Mountain, NH'},

    # === US Midwest Ski ===
    'USC00214884': {'station_id': 'lutsen_mn', 'name': 'Lutsen, MN'},
    'USC00218477': {'station_id': 'spirit_mountain_mn', 'name': 'Spirit Mountain, MN'},
    'USC00478905': {'station_id': 'granite_peak_wi', 'name': 'Granite Peak, WI'},

    # === Canada West ===
    'CA001048898': {'station_id': 'whistler_bc', 'name': 'Whistler, BC'},
    'CA001176749': {'station_id': 'revelstoke_bc', 'name': 'Revelstoke, BC'},

    # === Canada Rockies ===
    'CA003050519': {'station_id': 'banff_ab', 'name': 'Banff, AB'},
    'CA003053600': {'station_id': 'lake_louise_ab', 'name': 'Lake Louise, AB'},
    'CA003053536': {'station_id': 'marmot_basin_ab', 'name': 'Marmot Basin, AB'},
    'CA003054120': {'station_id': 'nakiska_ab', 'name': 'Nakiska, AB'},

    # === Canada BC Ski ===
    'CA001160899': {'station_id': 'big_white_bc', 'name': 'Big White, BC'},
    'CA001168520': {'station_id': 'sun_peaks_bc', 'name': 'Sun Peaks, BC'},
    'CA001152850': {'station_id': 'fernie_bc', 'name': 'Fernie, BC'},
    'CA001173514': {'station_id': 'kicking_horse_bc', 'name': 'Kicking Horse, BC'},
    'CA001146900': {'station_id': 'red_mountain_bc', 'name': 'Red Mountain, BC'},
    'CA001165969': {'station_id': 'panorama_bc', 'name': 'Panorama, BC'},

    # === Canada East ===
    'CA007025250': {'station_id': 'quebec_city_qc', 'name': 'Quebec City, QC'},
    'CA007024745': {'station_id': 'tremblant_qc', 'name': 'Tremblant, QC'},
    'CA007053042': {'station_id': 'mont_sainte_anne_qc', 'name': 'Mont-Sainte-Anne, QC'},
    'CA007053600': {'station_id': 'le_massif_qc', 'name': 'Le Massif, QC'},
    'CA006110607': {'station_id': 'blue_mountain_on', 'name': 'Blue Mountain, ON'},
    'CA004016560': {'station_id': 'thunder_bay_on', 'name': 'Thunder Bay, ON'},

    # === Canada North ===
    'CA005060600': {'station_id': 'edmonton_ab', 'name': 'Edmonton, AB'},
    'CA007060400': {'station_id': 'churchill_mb', 'name': 'Churchill, MB'},
    'CA002040150': {'station_id': 'winnipeg_mb', 'name': 'Winnipeg, MB'},
}


def setup_logging():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"noaa_collect_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger('noaa')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(sh)
    return logger


def setup_progress_table(conn):
    """Create progress tracking table (keeps noaa_daily for legacy compatibility)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS noaa_collection_progress (
            noaa_station_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            records_inserted INTEGER DEFAULT 0,
            completed_at TEXT,
            PRIMARY KEY (noaa_station_id, year)
        )
    """)
    conn.commit()


def noaa_api_request(endpoint, params, token, logger=None, max_retries=3):
    """Make a NOAA API request with 429 retry logic."""
    headers = {'token': token}
    url = f"{BASE_URL}/{endpoint}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 429:
                wait = min(60 * (attempt + 1), 300)  # 60s, 120s, 300s
                if logger:
                    logger.warning(f"  429 rate limited, waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                else:
                    print(f"  429 rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()

            if response.status_code == 200:
                return response.json()
            return None

        except requests.exceptions.Timeout:
            if logger:
                logger.warning(f"  Timeout (attempt {attempt + 1}/{max_retries})")
            time.sleep(5)
        except requests.exceptions.ConnectionError:
            if logger:
                logger.warning(f"  Connection error (attempt {attempt + 1}/{max_retries})")
            time.sleep(10)

    return None


def fetch_station_daterange(noaa_id, station_info, start_date, end_date, token, logger=None):
    """Fetch GHCN-Daily data for a station over a date range.

    Returns list of tuples ready for snowfall_daily insertion:
    (station_id, date, snowfall_mm, temp_mean, precip_mm, None(rain),
     temp_max, temp_min, None*4(apparent/gusts/dir/rad),
     None(sunshine), None(precip_hours), None(weather_code), None(et0),
     'noaa')
    """
    all_results = []
    offset = 1
    limit = 1000

    while True:
        params = {
            'datasetid': 'GHCND',
            'stationid': f'GHCND:{noaa_id}',
            'startdate': start_date,
            'enddate': end_date,
            'datatypeid': DATATYPES,
            'units': 'metric',
            'limit': limit,
            'offset': offset,
        }

        data = noaa_api_request('data', params, token, logger)
        if not data or 'results' not in data:
            break

        all_results.extend(data['results'])

        metadata = data.get('metadata', {}).get('resultset', {})
        total = metadata.get('count', 0)
        if offset + limit > total:
            break
        offset += limit
        time.sleep(0.25)  # Rate limit: 5/sec

    # Pivot results into daily records
    daily = {}
    for result in all_results:
        date = result['date'][:10]
        dtype = result['datatype']
        value = result['value']

        if date not in daily:
            daily[date] = {}
        daily[date][dtype] = value

    # Map to snowfall_daily schema
    records = []
    for date, v in sorted(daily.items()):
        tmax = v.get('TMAX')
        tmin = v.get('TMIN')
        tavg = v.get('TAVG')
        if tavg is None and tmax is not None and tmin is not None:
            tavg = (tmax + tmin) / 2.0

        # NOAA SNOW is already in mm with units=metric
        snow_mm = v.get('SNOW')

        # AWND is average wind — use as approximate wind_speed_max
        wind = v.get('AWND')
        # Prefer peak gust if available
        wsf = v.get('WSF5') or v.get('WSF2') or wind

        records.append((
            station_info['station_id'],
            date,
            snow_mm,              # snowfall_mm
            tavg,                 # temp_mean_celsius
            v.get('PRCP'),        # precipitation_mm
            None,                 # rain_mm (not available from NOAA)
            tmax,                 # temp_max_celsius
            tmin,                 # temp_min_celsius
            None,                 # apparent_temp_max
            None,                 # apparent_temp_min
            wsf,                  # wind_speed_max
            None,                 # wind_gusts_max
            None,                 # wind_direction_dominant
            None,                 # radiation_sum
            None,                 # sunshine_duration
            None,                 # precipitation_hours
            None,                 # weather_code
            None,                 # evapotranspiration
        ))

    return records


def insert_noaa_records(conn, records):
    """Insert NOAA records into snowfall_daily with data_source='noaa'.
    Always overwrites — NOAA is the authority for NA stations."""
    conn.executemany("""
        INSERT INTO snowfall_daily
        (station_id, date, snowfall_mm, temp_mean_celsius,
         precipitation_mm, rain_mm, temp_max_celsius, temp_min_celsius,
         apparent_temp_max, apparent_temp_min,
         wind_speed_max, wind_gusts_max, wind_direction_dominant,
         radiation_sum, sunshine_duration,
         precipitation_hours, weather_code, evapotranspiration,
         data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'noaa')
        ON CONFLICT(station_id, date) DO UPDATE SET
            snowfall_mm = excluded.snowfall_mm,
            temp_mean_celsius = excluded.temp_mean_celsius,
            precipitation_mm = excluded.precipitation_mm,
            temp_max_celsius = excluded.temp_max_celsius,
            temp_min_celsius = excluded.temp_min_celsius,
            wind_speed_max = excluded.wind_speed_max,
            data_source = 'noaa'
    """, records)


def collect_daily(days=14, rate_limit=0.3):
    """Daily mode: fetch recent N days for all NOAA stations."""
    logger = setup_logging()

    if not NOAA_TOKEN or 'YOUR_' in NOAA_TOKEN:
        logger.error("NOAA_API_TOKEN not set in .env")
        return

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    logger.info("=" * 80)
    logger.info("NOAA DAILY COLLECTION")
    logger.info(f"Stations: {len(NOAA_STATIONS)}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=60000")

    success = 0
    failed = 0

    for noaa_id, info in NOAA_STATIONS.items():
        try:
            records = fetch_station_daterange(
                noaa_id, info, start_date, end_date, NOAA_TOKEN, logger)

            if records:
                insert_noaa_records(conn, records)
                conn.commit()
                success += 1
                logger.info(f"  {info['name']}: {len(records)} days")
            else:
                success += 1  # No data is OK for daily mode
                logger.info(f"  {info['name']}: no recent data")

        except Exception as e:
            failed += 1
            logger.warning(f"  {info['name']}: FAILED - {e}")

        time.sleep(rate_limit)

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("NOAA DAILY COLLECTION COMPLETE")
    logger.info(f"Successful: {success}, Failed: {failed}")
    logger.info("=" * 80)


def collect_backfill(budget=5000, rate_limit=0.3, start_year=1940):
    """Backfill mode: year-by-year historical collection, budget-aware."""
    logger = setup_logging()

    if not NOAA_TOKEN or 'YOUR_' in NOAA_TOKEN:
        logger.error("NOAA_API_TOKEN not set in .env")
        return

    logger.info("=" * 80)
    logger.info("NOAA BACKFILL COLLECTION")
    logger.info(f"Stations: {len(NOAA_STATIONS)}")
    logger.info(f"Budget: {budget} API calls")
    logger.info(f"Year range: {start_year}-{datetime.now().year}")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=60000")
    setup_progress_table(conn)

    current_year = datetime.now().year
    calls_used = 0
    success = 0
    failed = 0

    for noaa_id, info in NOAA_STATIONS.items():
        for year in range(current_year, start_year - 1, -1):
            if calls_used >= budget:
                logger.info(f"Budget exhausted ({calls_used}/{budget})")
                conn.close()
                logger.info(f"Completed: {success}, Failed: {failed}")
                return

            # Check if already collected
            existing = conn.execute("""
                SELECT status FROM noaa_collection_progress
                WHERE noaa_station_id = ? AND year = ?
            """, (noaa_id, year)).fetchone()

            if existing and existing[0] in ('completed', 'no_data'):
                continue

            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            if year == current_year:
                end_date = datetime.now().strftime('%Y-%m-%d')

            try:
                records = fetch_station_daterange(
                    noaa_id, info, start_date, end_date, NOAA_TOKEN, logger)

                if records:
                    insert_noaa_records(conn, records)

                    conn.execute("""
                        INSERT OR REPLACE INTO noaa_collection_progress
                        (noaa_station_id, year, status, records_inserted, completed_at)
                        VALUES (?, ?, 'completed', ?, ?)
                    """, (noaa_id, year, len(records), datetime.now().isoformat()))

                    conn.commit()
                    success += 1
                    logger.info(f"  {info['name']} {year}: {len(records)} days")
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO noaa_collection_progress
                        (noaa_station_id, year, status, records_inserted, completed_at)
                        VALUES (?, ?, 'no_data', 0, ?)
                    """, (noaa_id, year, datetime.now().isoformat()))
                    conn.commit()
                    logger.info(f"  {info['name']} {year}: no data")

                # Each year fetch = ~1-4 API calls (paginated)
                calls_used += 4

            except Exception as e:
                failed += 1
                logger.warning(f"  {info['name']} {year}: FAILED - {e}")
                conn.execute("""
                    INSERT OR REPLACE INTO noaa_collection_progress
                    (noaa_station_id, year, status, records_inserted, completed_at)
                    VALUES (?, ?, 'failed', 0, ?)
                """, (noaa_id, year, datetime.now().isoformat()))
                conn.commit()
                calls_used += 1

            time.sleep(rate_limit)

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("NOAA BACKFILL COMPLETE")
    logger.info(f"Successful: {success}")
    logger.info(f"Failed: {failed}")
    logger.info(f"API calls used: ~{calls_used}")
    logger.info("=" * 80)


def show_status():
    """Show NOAA collection progress."""
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=60000")

    # Progress table stats
    try:
        cursor = conn.execute("""
            SELECT status, COUNT(*) FROM noaa_collection_progress GROUP BY status
        """)
        counts = dict(cursor.fetchall())
    except Exception:
        counts = {}

    # snowfall_daily NOAA records
    try:
        noaa_count = conn.execute(
            "SELECT COUNT(*) FROM snowfall_daily WHERE data_source = 'noaa'"
        ).fetchone()[0]
        noaa_range = conn.execute(
            "SELECT MIN(date), MAX(date) FROM snowfall_daily WHERE data_source = 'noaa'"
        ).fetchone()
        noaa_stations = conn.execute(
            "SELECT COUNT(DISTINCT station_id) FROM snowfall_daily WHERE data_source = 'noaa'"
        ).fetchone()[0]
    except Exception:
        noaa_count = 0
        noaa_range = (None, None)
        noaa_stations = 0

    conn.close()

    print(f"\n{'='*80}")
    print("NOAA GHCN-DAILY COLLECTION STATUS")
    print(f"{'='*80}")
    if counts:
        print(f"Backfill station-years completed: {counts.get('completed', 0)}")
        print(f"No data: {counts.get('no_data', 0)}")
        print(f"Failed: {counts.get('failed', 0)}")
    print(f"\nRecords in snowfall_daily (source=noaa): {noaa_count:,}")
    print(f"Stations with NOAA data: {noaa_stations}")
    if noaa_range[0]:
        print(f"Date range: {noaa_range[0]} to {noaa_range[1]}")
    print(f"Total NOAA station mappings: {len(NOAA_STATIONS)}")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description='Collect NOAA GHCN-Daily Data')
    parser.add_argument('--mode', choices=['daily', 'backfill'], default='daily',
                        help='Collection mode (default: daily)')
    parser.add_argument('--days', type=int, default=14,
                        help='Days to fetch in daily mode (default: 14)')
    parser.add_argument('--budget', type=int, default=5000,
                        help='Max API calls for backfill mode (default: 5000)')
    parser.add_argument('--rate-limit', type=float, default=0.3,
                        help='Seconds between API calls (default: 0.3)')
    parser.add_argument('--start-year', type=int, default=1940,
                        help='Earliest year for backfill (default: 1940)')
    parser.add_argument('--status', action='store_true',
                        help='Show collection progress')

    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.mode == 'daily':
        collect_daily(days=args.days, rate_limit=args.rate_limit)
    elif args.mode == 'backfill':
        collect_backfill(
            budget=args.budget,
            rate_limit=args.rate_limit,
            start_year=args.start_year,
        )


if __name__ == '__main__':
    main()
