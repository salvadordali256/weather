#!/usr/bin/env python3
"""
Collect NOAA GHCN-Daily Data
Uses the Climate Data Online (CDO) API v2 to fetch observed station data.
Complements Open-Meteo with actual ground-truth observations.

GHCN-Daily data includes: TMAX, TMIN, PRCP, SNOW, SNWD (snow depth),
AWND (avg wind), and more — some stations back to the 1800s.

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
# Format: GHCN station ID -> our station_id
# Found via: https://www.ncei.noaa.gov/cdo-web/api/v2/stations?datasetid=GHCND&locationid=FIPS:US
NOAA_STATIONS = {
    # Wisconsin (primary targets)
    'USW00014898': {'station_id': 'green_bay_wi', 'name': 'Green Bay, WI'},
    'USW00014839': {'station_id': 'duluth_mn', 'name': 'Duluth, MN'},
    'USW00014858': {'station_id': 'marquette_mi', 'name': 'Marquette, MI'},
    'USW00094892': {'station_id': 'iron_mountain_mi', 'name': 'Iron Mountain, MI'},

    # Great Lakes / Midwest
    'USW00014733': {'station_id': 'minneapolis_mn', 'name': 'Minneapolis, MN'},
    'USW00094847': {'station_id': 'detroit_mi', 'name': 'Detroit, MI'},
    'USW00014850': {'station_id': 'milwaukee_wi', 'name': 'Milwaukee, WI'},
    'USW00094860': {'station_id': 'chicago_il', 'name': 'Chicago, IL'},

    # Eastern US / Appalachian
    'USW00014771': {'station_id': 'syracuse_ny', 'name': 'Syracuse, NY'},
    'USW00014742': {'station_id': 'burlington_vt', 'name': 'Burlington, VT'},
    'USW00014735': {'station_id': 'buffalo_ny', 'name': 'Buffalo, NY'},
    'USW00014764': {'station_id': 'mount_washington_nh', 'name': 'Mount Washington, NH'},
    'USW00014740': {'station_id': 'caribou_me', 'name': 'Caribou, ME'},

    # Colorado Rockies
    'USW00093073': {'station_id': 'steamboat_springs_co', 'name': 'Steamboat Springs, CO'},
    'USW00023062': {'station_id': 'aspen_co', 'name': 'Aspen, CO'},

    # Pacific Northwest
    'USW00024233': {'station_id': 'mount_hood_or', 'name': 'Mount Hood, OR'},

    # Canada
    'CA007025250': {'station_id': 'quebec_city_qc', 'name': 'Quebec City, QC'},
    'CA005060600': {'station_id': 'edmonton_ab', 'name': 'Edmonton, AB'},
    'CA007060400': {'station_id': 'churchill_mb', 'name': 'Churchill, MB'},
    'CA004016560': {'station_id': 'thunder_bay_on', 'name': 'Thunder Bay, ON'},
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


def setup_noaa_table(conn):
    """Create NOAA observations table"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS noaa_daily (
            station_id TEXT NOT NULL,
            noaa_station_id TEXT NOT NULL,
            date TEXT NOT NULL,
            tmax_c REAL,
            tmin_c REAL,
            tavg_c REAL,
            prcp_mm REAL,
            snow_mm REAL,
            snwd_mm REAL,
            awnd_ms REAL,
            wsf2_ms REAL,
            wsf5_ms REAL,
            wesd_mm REAL,
            PRIMARY KEY (station_id, date)
        )
    """)

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


def noaa_api_request(endpoint, params, token):
    """Make a rate-limited NOAA API request"""
    headers = {'token': token}
    url = f"{BASE_URL}/{endpoint}"

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    if response.status_code == 200:
        return response.json()
    return None


def fetch_station_year(noaa_id, station_info, year, token):
    """Fetch one year of GHCN-Daily data for a station"""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    if year == datetime.now().year:
        end_date = datetime.now().strftime('%Y-%m-%d')

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

        data = noaa_api_request('data', params, token)
        if not data or 'results' not in data:
            break

        all_results.extend(data['results'])

        # Check if there are more pages
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

    records = []
    for date, values in sorted(daily.items()):
        records.append((
            station_info['station_id'],
            noaa_id,
            date,
            values.get('TMAX'),
            values.get('TMIN'),
            values.get('TAVG'),
            values.get('PRCP'),
            values.get('SNOW'),
            values.get('SNWD'),
            values.get('AWND'),
            values.get('WSF2'),
            values.get('WSF5'),
            values.get('WESD'),
        ))

    return records


def collect_noaa_data(budget=5000, rate_limit=0.3, start_year=1940):
    """Main NOAA data collection"""
    logger = setup_logging()

    if not NOAA_TOKEN or 'YOUR_' in NOAA_TOKEN:
        logger.error("NOAA_API_TOKEN not set in .env")
        return

    logger.info("=" * 80)
    logger.info("NOAA GHCN-DAILY DATA COLLECTION")
    logger.info(f"Stations: {len(NOAA_STATIONS)}")
    logger.info(f"Budget: {budget} API calls")
    logger.info(f"Year range: {start_year}-{datetime.now().year}")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    setup_noaa_table(conn)

    # Build work queue (check what's already done)
    current_year = datetime.now().year
    calls_used = 0
    success = 0
    failed = 0

    for noaa_id, info in NOAA_STATIONS.items():
        # Process years from recent to old
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

            try:
                records = fetch_station_year(noaa_id, info, year, NOAA_TOKEN)

                if records:
                    conn.executemany("""
                        INSERT OR REPLACE INTO noaa_daily
                        (station_id, noaa_station_id, date,
                         tmax_c, tmin_c, tavg_c, prcp_mm, snow_mm,
                         snwd_mm, awnd_ms, wsf2_ms, wsf5_ms, wesd_mm)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, records)

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
                logger.warning(f"  {info['name']} {year}: FAILED — {e}")
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
    logger.info("NOAA COLLECTION COMPLETE")
    logger.info(f"Successful: {success}")
    logger.info(f"Failed: {failed}")
    logger.info(f"API calls used: ~{calls_used}")
    logger.info("=" * 80)


def show_status():
    """Show NOAA collection progress"""
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    try:
        cursor = conn.execute("""
            SELECT status, COUNT(*) FROM noaa_collection_progress GROUP BY status
        """)
        counts = dict(cursor.fetchall())
    except Exception:
        print("No NOAA collection data yet. Run the collector first.")
        conn.close()
        return

    total_records = conn.execute("SELECT COUNT(*) FROM noaa_daily").fetchone()[0]
    date_range = conn.execute("SELECT MIN(date), MAX(date) FROM noaa_daily").fetchone()

    conn.close()

    print(f"\n{'='*80}")
    print("NOAA GHCN-DAILY COLLECTION STATUS")
    print(f"{'='*80}")
    print(f"Station-years completed: {counts.get('completed', 0)}")
    print(f"No data: {counts.get('no_data', 0)}")
    print(f"Failed: {counts.get('failed', 0)}")
    print(f"Total daily records: {total_records:,}")
    if date_range[0]:
        print(f"Date range: {date_range[0]} to {date_range[1]}")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description='Collect NOAA GHCN-Daily Data')
    parser.add_argument('--budget', type=int, default=5000,
                        help='Max API calls per run (default: 5000)')
    parser.add_argument('--rate-limit', type=float, default=0.3,
                        help='Seconds between API calls (default: 0.3)')
    parser.add_argument('--start-year', type=int, default=1940,
                        help='Earliest year to collect (default: 1940)')
    parser.add_argument('--status', action='store_true',
                        help='Show collection progress')

    args = parser.parse_args()

    if args.status:
        show_status()
    else:
        collect_noaa_data(
            budget=args.budget,
            rate_limit=args.rate_limit,
            start_year=args.start_year,
        )


if __name__ == '__main__':
    main()
