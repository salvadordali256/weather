#!/usr/bin/env python3
"""
Collect Radiosonde (Weather Balloon) Data
Fetches upper-air sounding profiles from the University of Wyoming archive.
Uses the siphon library for structured data access.
"""

import sqlite3
import os
import time
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')

# Radiosonde stations (WMO number, name, region)
# These are actual WMO station IDs used by the Wyoming sounding archive
RADIOSONDE_STATIONS = {
    # US Great Lakes / Midwest (primary focus area)
    '72645': {'name': 'Green Bay, WI', 'region': 'us_greatlakes'},
    '72634': {'name': 'Marquette, MI (Gaylord)', 'region': 'us_greatlakes'},
    '72649': {'name': 'Minneapolis, MN', 'region': 'us_greatlakes'},
    '72632': {'name': 'Detroit/White Lake, MI', 'region': 'us_greatlakes'},
    '72528': {'name': 'Buffalo, NY', 'region': 'us_greatlakes'},
    '72451': {'name': 'Davenport, IA', 'region': 'us_midwest'},
    # US East
    '72518': {'name': 'Albany, NY', 'region': 'us_east'},
    '72712': {'name': 'Caribou, ME', 'region': 'us_east'},
    '72403': {'name': 'Sterling, VA (DC area)', 'region': 'us_east'},
    # Canada
    '71913': {'name': 'Churchill, MB', 'region': 'canada'},
    '71119': {'name': 'Edmonton, AB (Stony Plain)', 'region': 'canada'},
    '71924': {'name': 'Resolute, NU (Arctic)', 'region': 'canada'},
    # Global indicators
    '47412': {'name': 'Sapporo, Japan', 'region': 'japan'},
    '27612': {'name': 'Moscow (Dolgoprudny), Russia', 'region': 'russia'},
    '30710': {'name': 'Irkutsk, Russia', 'region': 'russia'},
    '01001': {'name': 'Jan Mayen, Norway (Arctic)', 'region': 'arctic'},
    '10868': {'name': 'Munich, Germany', 'region': 'europe'},
    '71082': {'name': 'Alert, NU (High Arctic)', 'region': 'arctic'},
    '89009': {'name': 'Amundsen-Scott South Pole', 'region': 'antarctic'},
    '94120': {'name': 'Darwin, Australia', 'region': 'australia'},
}


def setup_logging():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"radiosonde_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger('radiosonde')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(sh)
    return logger


def setup_radiosonde_table(conn):
    """Create radiosonde soundings table"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS radiosonde_soundings (
            station_wmo TEXT NOT NULL,
            station_name TEXT,
            datetime TEXT NOT NULL,
            pressure_hpa REAL NOT NULL,
            height_m REAL,
            temperature_c REAL,
            dewpoint_c REAL,
            relative_humidity REAL,
            mixing_ratio REAL,
            wind_direction INTEGER,
            wind_speed_kts REAL,
            theta_e REAL,
            theta_v REAL,
            PRIMARY KEY (station_wmo, datetime, pressure_hpa)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS radiosonde_stations (
            station_wmo TEXT PRIMARY KEY,
            name TEXT,
            region TEXT,
            latitude REAL,
            longitude REAL
        )
    """)
    conn.commit()


def fetch_sounding_siphon(station_wmo, date_time):
    """Fetch a single sounding using siphon library"""
    try:
        from siphon.simplewebservice.wyoming import WyomingUpperAir
    except ImportError:
        raise ImportError("siphon not installed. Run: pip install siphon")

    df = WyomingUpperAir.request_data(date_time, station_wmo)
    return df


def fetch_sounding_raw(station_wmo, date_time):
    """Fallback: fetch sounding via direct HTTP if siphon fails"""
    import requests

    year = date_time.strftime('%Y')
    month = date_time.strftime('%m')
    day_hour = date_time.strftime('%d%H')

    url = (
        f"https://weather.uwyo.edu/cgi-bin/sounding?"
        f"region=naconf&TYPE=TEXT%3ALIST&YEAR={year}&MONTH={month}"
        f"&FROM={day_hour}&TO={day_hour}&STNM={station_wmo}"
    )

    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        return None

    # Parse the text table
    lines = response.text.split('\n')
    records = []
    in_data = False

    for line in lines:
        line = line.strip()
        if line.startswith('---'):
            in_data = not in_data
            continue
        if not in_data or not line:
            continue

        parts = line.split()
        if len(parts) >= 7:
            try:
                pressure = float(parts[0])
                height = float(parts[1]) if parts[1] != '' else None
                temp = float(parts[2]) if parts[2] != '' else None
                dewpt = float(parts[3]) if parts[3] != '' else None
                rh = float(parts[4]) if len(parts) > 4 and parts[4] != '' else None
                mixr = float(parts[5]) if len(parts) > 5 and parts[5] != '' else None
                wdir = int(float(parts[6])) if len(parts) > 6 and parts[6] != '' else None
                wspd = float(parts[7]) if len(parts) > 7 and parts[7] != '' else None
                thte = float(parts[8]) if len(parts) > 8 and parts[8] != '' else None
                thtv = float(parts[9]) if len(parts) > 9 and parts[9] != '' else None

                records.append({
                    'pressure': pressure, 'height': height,
                    'temperature': temp, 'dewpoint': dewpt,
                    'rh': rh, 'mixing_ratio': mixr,
                    'wind_direction': wdir, 'wind_speed': wspd,
                    'theta_e': thte, 'theta_v': thtv,
                })
            except (ValueError, IndexError):
                continue

    return records


def collect_soundings(days_back=7, rate_limit=2.0):
    """Main collection routine"""
    logger = setup_logging()

    logger.info("=" * 80)
    logger.info("RADIOSONDE DATA COLLECTION")
    logger.info(f"Stations: {len(RADIOSONDE_STATIONS)}")
    logger.info(f"Days back: {days_back}")
    logger.info(f"Sounding times: 00Z, 12Z")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    setup_radiosonde_table(conn)

    # Register stations
    for wmo, info in RADIOSONDE_STATIONS.items():
        conn.execute("""
            INSERT OR IGNORE INTO radiosonde_stations (station_wmo, name, region)
            VALUES (?, ?, ?)
        """, (wmo, info['name'], info['region']))
    conn.commit()

    total_records = 0
    success_stations = 0
    failed_stations = 0

    # Generate date/time list (00Z and 12Z for each day)
    sounding_times = []
    for day_offset in range(days_back):
        dt = datetime.utcnow() - timedelta(days=day_offset)
        sounding_times.append(dt.replace(hour=0, minute=0, second=0, microsecond=0))
        sounding_times.append(dt.replace(hour=12, minute=0, second=0, microsecond=0))

    for wmo, info in RADIOSONDE_STATIONS.items():
        station_records = 0
        station_errors = 0
        logger.info(f"\n  [{wmo}] {info['name']}:")

        for snd_time in sounding_times:
            dt_str = snd_time.strftime('%Y-%m-%dT%H:%M')

            # Skip if already collected
            existing = conn.execute("""
                SELECT COUNT(*) FROM radiosonde_soundings
                WHERE station_wmo = ? AND datetime = ?
            """, (wmo, dt_str)).fetchone()[0]

            if existing > 0:
                continue

            try:
                # Try siphon first
                try:
                    df = fetch_sounding_siphon(wmo, snd_time)
                    if df is not None and len(df) > 0:
                        for _, row in df.iterrows():
                            conn.execute("""
                                INSERT OR REPLACE INTO radiosonde_soundings
                                (station_wmo, station_name, datetime, pressure_hpa,
                                 height_m, temperature_c, dewpoint_c,
                                 relative_humidity, mixing_ratio,
                                 wind_direction, wind_speed_kts,
                                 theta_e, theta_v)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                wmo, info['name'], dt_str,
                                row.get('pressure', None),
                                row.get('height', None),
                                row.get('temperature', None),
                                row.get('dewpoint', None),
                                row.get('relative_humidity', None) if hasattr(row, 'get') else None,
                                row.get('mixing_ratio', None) if hasattr(row, 'get') else None,
                                row.get('direction', None),
                                row.get('speed', None),
                                row.get('theta_e', None) if hasattr(row, 'get') else None,
                                row.get('theta_v', None) if hasattr(row, 'get') else None,
                            ))
                        conn.commit()
                        station_records += len(df)
                        continue

                except (ImportError, Exception):
                    pass

                # Fallback to raw HTTP
                records = fetch_sounding_raw(wmo, snd_time)
                if records:
                    for rec in records:
                        conn.execute("""
                            INSERT OR REPLACE INTO radiosonde_soundings
                            (station_wmo, station_name, datetime, pressure_hpa,
                             height_m, temperature_c, dewpoint_c,
                             relative_humidity, mixing_ratio,
                             wind_direction, wind_speed_kts,
                             theta_e, theta_v)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            wmo, info['name'], dt_str,
                            rec['pressure'], rec['height'],
                            rec['temperature'], rec['dewpoint'],
                            rec['rh'], rec['mixing_ratio'],
                            rec['wind_direction'], rec['wind_speed'],
                            rec['theta_e'], rec['theta_v'],
                        ))
                    conn.commit()
                    station_records += len(records)

            except Exception as e:
                station_errors += 1
                if station_errors <= 3:
                    logger.warning(f"    {dt_str}: {e}")

            time.sleep(rate_limit)

        if station_records > 0:
            success_stations += 1
            total_records += station_records
            logger.info(f"    {station_records} levels collected")
        else:
            failed_stations += 1
            logger.info(f"    No data collected")

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("RADIOSONDE COLLECTION COMPLETE")
    logger.info(f"Stations with data: {success_stations}/{len(RADIOSONDE_STATIONS)}")
    logger.info(f"Failed: {failed_stations}")
    logger.info(f"Total pressure levels: {total_records}")
    logger.info("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Collect Radiosonde Data')
    parser.add_argument('--days', type=int, default=7,
                        help='Days of soundings to fetch (default: 7)')
    parser.add_argument('--rate-limit', type=float, default=2.0,
                        help='Seconds between requests (default: 2.0, be nice to university servers)')

    args = parser.parse_args()
    collect_soundings(days_back=args.days, rate_limit=args.rate_limit)


if __name__ == '__main__':
    main()
