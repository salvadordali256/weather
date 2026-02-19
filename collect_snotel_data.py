#!/usr/bin/env python3
"""
Collect SNOTEL Data — Mountain Snow Observations

Fetches daily data from NRCS SNOTEL stations via the Report Generator CSV API.
Writes directly to snowfall_daily with data_source='snotel'.

SNOTEL provides actual mountain-elevation measurements: snow depth, SWE,
precipitation, and temperature — far more accurate at altitude than valley
weather stations (NOAA) or gridded reanalysis (Open-Meteo).

No auth required. No documented rate limits. Data back to ~1980.

Units from SNOTEL: inches (snow/precip), °F (temperature)
We convert to: mm and °C for snowfall_daily.

Modes:
  --mode daily   : Fetch last N days (default 14) for all stations
  --mode backfill: Fetch full period of record, budget-aware by station count

Usage:
    python collect_snotel_data.py --mode daily --days 14
    python collect_snotel_data.py --mode backfill
    python collect_snotel_data.py --status
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

# NRCS Report Generator base URL
REPORT_URL = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv"

# Data elements to fetch
# WTEQ: Snow Water Equivalent (inches)
# SNWD: Snow Depth (inches)
# PREC: Cumulative Precipitation (inches, water year)
# TMAX/TMIN/TAVG: Air Temperature (°F)
ELEMENTS = "WTEQ::value,SNWD::value,PREC::value,TMAX::value,TMIN::value,TAVG::value"

# SNOTEL stations mapped to our ski resort station network
# Triplet format: {station_number}:{state}:SNTL
# Found via research_snotel_stations.py
SNOTEL_STATIONS = {
    # === Colorado ===
    '1101:CO:SNTL': {'station_id': 'aspen_co', 'name': 'Aspen, CO',
                     'snotel_name': 'Chapman Tunnel', 'elevation_ft': 10090},
    '842:CO:SNTL': {'station_id': 'vail_co', 'name': 'Vail, CO',
                    'snotel_name': 'Vail Mountain', 'elevation_ft': 10290},
    '457:CO:SNTL': {'station_id': 'steamboat_springs_co', 'name': 'Steamboat Springs, CO',
                    'snotel_name': 'Dry Lake', 'elevation_ft': 8240},
    '1344:CO:SNTL': {'station_id': 'telluride_co', 'name': 'Telluride, CO',
                     'snotel_name': 'Alta Lakes', 'elevation_ft': 11290},
    '415:CO:SNTL': {'station_id': 'breckenridge_co', 'name': 'Breckenridge, CO',
                    'snotel_name': 'Copper Mountain', 'elevation_ft': 10500},
    '1186:CO:SNTL': {'station_id': 'winter_park_co', 'name': 'Winter Park, CO',
                     'snotel_name': 'Fool Creek', 'elevation_ft': 11130},
    '380:CO:SNTL': {'station_id': 'crested_butte_co', 'name': 'Crested Butte, CO',
                    'snotel_name': 'Butte', 'elevation_ft': 10190},
    '701:CO:SNTL': {'station_id': 'monarch_co', 'name': 'Monarch, CO',
                    'snotel_name': 'Porphyry Creek', 'elevation_ft': 10790},
    '485:CO:SNTL': {'station_id': 'ski_cooper_co', 'name': 'Ski Cooper, CO',
                    'snotel_name': 'Fremont Pass', 'elevation_ft': 11310},

    # === Wyoming ===
    '689:WY:SNTL': {'station_id': 'jackson_hole_wy', 'name': 'Jackson Hole, WY',
                    'snotel_name': 'Phillips Bench', 'elevation_ft': 8170},
    '1082:WY:SNTL': {'station_id': 'grand_targhee_wy', 'name': 'Grand Targhee, WY',
                     'snotel_name': 'Grand Targhee', 'elevation_ft': 9260},

    # === Montana ===
    '590:MT:SNTL': {'station_id': 'big_sky_mt', 'name': 'Big Sky, MT',
                    'snotel_name': 'Lone Mountain', 'elevation_ft': 8820},
    '929:MT:SNTL': {'station_id': 'bridger_bowl_mt', 'name': 'Bridger Bowl, MT',
                    'snotel_name': 'Sacajawea', 'elevation_ft': 6610},

    # === Idaho ===
    '895:ID:SNTL': {'station_id': 'sun_valley_id', 'name': 'Sun Valley, ID',
                    'snotel_name': 'Chocolate Gulch', 'elevation_ft': 6310},
    '738:ID:SNTL': {'station_id': 'schweitzer_id', 'name': 'Schweitzer, ID',
                    'snotel_name': 'Schweitzer Basin', 'elevation_ft': 6090},

    # === Utah ===
    '814:UT:SNTL': {'station_id': 'park_city_ut', 'name': 'Park City, UT',
                    'snotel_name': 'Thaynes Canyon', 'elevation_ft': 9260},
    '1308:UT:SNTL': {'station_id': 'alta_ut', 'name': 'Alta/Snowbird, UT',
                     'snotel_name': 'Atwater', 'elevation_ft': 8750},

    # === New Mexico ===
    '1168:NM:SNTL': {'station_id': 'taos_nm', 'name': 'Taos Ski Valley, NM',
                     'snotel_name': 'Taos Powderhorn', 'elevation_ft': 11020},

    # === Washington ===
    '909:WA:SNTL': {'station_id': 'mount_baker_wa', 'name': 'Mount Baker, WA',
                    'snotel_name': 'Wells Creek', 'elevation_ft': 4040},
    '791:WA:SNTL': {'station_id': 'stevens_pass_wa', 'name': 'Stevens Pass, WA',
                    'snotel_name': 'Stevens Pass', 'elevation_ft': 3940},
    '642:WA:SNTL': {'station_id': 'crystal_mountain_wa', 'name': 'Crystal Mountain, WA',
                    'snotel_name': 'Morse Lake', 'elevation_ft': 5400},
    '672:WA:SNTL': {'station_id': 'snoqualmie_pass_wa', 'name': 'Snoqualmie Pass, WA',
                    'snotel_name': 'Olallie Meadows', 'elevation_ft': 4010},

    # === Oregon ===
    '651:OR:SNTL': {'station_id': 'mount_hood_or', 'name': 'Mount Hood, OR',
                    'snotel_name': 'Mt Hood Test Site', 'elevation_ft': 5380},
    '815:OR:SNTL': {'station_id': 'mt_bachelor_or', 'name': 'Mt. Bachelor, OR',
                    'snotel_name': 'Three Creeks Meadow', 'elevation_ft': 5680},

    # === California ===
    '846:CA:SNTL': {'station_id': 'mammoth_mountain_ca', 'name': 'Mammoth Mountain, CA',
                    'snotel_name': 'Virginia Lakes Ridge', 'elevation_ft': 9400},
    '784:CA:SNTL': {'station_id': 'lake_tahoe_ca', 'name': 'Lake Tahoe, CA',
                    'snotel_name': 'Palisades Tahoe', 'elevation_ft': 8010},
    '518:CA:SNTL': {'station_id': 'heavenly_ca', 'name': 'Heavenly, CA',
                    'snotel_name': 'Heavenly Valley', 'elevation_ft': 8540},
    '834:CA:SNTL': {'station_id': 'northstar_ca', 'name': 'Northstar, CA',
                    'snotel_name': 'Truckee #2', 'elevation_ft': 6500},
}

# Conversion constants
INCHES_TO_MM = 25.4


def f_to_c(f):
    """Convert Fahrenheit to Celsius."""
    if f is None:
        return None
    return round((f - 32) * 5.0 / 9.0, 1)


def setup_logging():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"snotel_collect_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger('snotel')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(sh)
    return logger


def fetch_snotel_csv(triplet, start_date, end_date, logger=None):
    """Fetch daily CSV from SNOTEL Report Generator.

    Returns list of dicts with parsed/converted values, or empty list on failure.
    """
    url = (
        f"{REPORT_URL}/customSingleStationReport/daily/"
        f"{triplet}/{start_date},{end_date}/{ELEMENTS}"
    )

    for attempt in range(3):
        try:
            response = requests.get(url, timeout=60)
            if response.status_code == 503:
                wait = 30 * (attempt + 1)
                if logger:
                    logger.warning(f"  SNOTEL server unavailable, waiting {wait}s")
                time.sleep(wait)
                continue
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(10)
                continue
            if logger:
                logger.warning(f"  Fetch failed after 3 attempts: {e}")
            return []
    else:
        return []

    records = []
    for line in response.text.strip().split('\n'):
        # Skip comments and headers
        if line.startswith('#') or line.startswith('Date') or not line.strip():
            continue

        parts = line.split(',')
        if len(parts) < 7:
            continue

        try:
            date = parts[0].strip()
            # Validate date format
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            continue

        def parse_float(val):
            v = val.strip()
            if v == '' or v == '-' or v.lower() == 'nan':
                return None
            try:
                return float(v)
            except ValueError:
                return None

        wteq_in = parse_float(parts[1])   # SWE inches
        snwd_in = parse_float(parts[2])   # Snow depth inches
        prec_in = parse_float(parts[3])   # Cumulative precip inches
        tmax_f = parse_float(parts[4])    # Max temp °F
        tmin_f = parse_float(parts[5])    # Min temp °F
        tavg_f = parse_float(parts[6])    # Avg temp °F

        records.append({
            'date': date,
            'wteq_in': wteq_in,
            'snwd_in': snwd_in,
            'prec_in': prec_in,
            'tmax_f': tmax_f,
            'tmin_f': tmin_f,
            'tavg_f': tavg_f,
        })

    return records


def compute_daily_snowfall(records):
    """Compute daily snowfall from SNOTEL snow depth changes.

    Uses positive day-to-day snow depth changes as new snowfall estimate.
    Falls back to precipitation when temp < 0°C if snow depth unavailable.
    """
    for i, rec in enumerate(records):
        snow_mm = None

        # Method 1: Snow depth increase from previous day
        if i > 0 and rec['snwd_in'] is not None and records[i - 1]['snwd_in'] is not None:
            depth_change = rec['snwd_in'] - records[i - 1]['snwd_in']
            if depth_change > 0:
                snow_mm = depth_change * INCHES_TO_MM

        # Method 2: If no snow depth data, estimate from precip + temp
        if snow_mm is None and rec['prec_in'] is not None and i > 0:
            prev_prec = records[i - 1]['prec_in']
            if prev_prec is not None:
                precip_inc = rec['prec_in'] - prev_prec
                tavg_c = f_to_c(rec['tavg_f'])
                if precip_inc > 0 and tavg_c is not None and tavg_c <= 2.0:
                    # Estimate with 12:1 snow ratio (conservative mountain ratio)
                    snow_mm = precip_inc * INCHES_TO_MM * 12.0

        rec['snowfall_mm'] = round(snow_mm, 1) if snow_mm is not None else None

        # Also compute precipitation increment
        if i > 0 and rec['prec_in'] is not None and records[i - 1]['prec_in'] is not None:
            precip_inc = rec['prec_in'] - records[i - 1]['prec_in']
            rec['precip_mm'] = round(precip_inc * INCHES_TO_MM, 1) if precip_inc >= 0 else None
        else:
            rec['precip_mm'] = None

    return records


def insert_snotel_records(conn, station_id, records):
    """Insert SNOTEL records into snowfall_daily with data_source='snotel'.

    SNOTEL always overwrites — it's the most accurate mountain data source.
    """
    rows = []
    for rec in records:
        tmax_c = f_to_c(rec['tmax_f'])
        tmin_c = f_to_c(rec['tmin_f'])
        tavg_c = f_to_c(rec['tavg_f'])

        snwd_in = rec.get('snwd_in')
        rows.append((
            station_id,
            rec['date'],
            rec.get('snowfall_mm'),     # snowfall_mm
            tavg_c,                     # temp_mean_celsius
            rec.get('precip_mm'),       # precipitation_mm
            None,                       # rain_mm
            tmax_c,                     # temp_max_celsius
            tmin_c,                     # temp_min_celsius
            None,                       # apparent_temp_max
            None,                       # apparent_temp_min
            None,                       # wind_speed_max
            None,                       # wind_gusts_max
            None,                       # wind_direction_dominant
            None,                       # radiation_sum
            None,                       # sunshine_duration
            None,                       # precipitation_hours
            None,                       # weather_code
            None,                       # evapotranspiration
            round(snwd_in * INCHES_TO_MM, 1) if snwd_in is not None else None,  # snow_depth_mm
        ))

    conn.executemany("""
        INSERT INTO snowfall_daily
        (station_id, date, snowfall_mm, temp_mean_celsius,
         precipitation_mm, rain_mm, temp_max_celsius, temp_min_celsius,
         apparent_temp_max, apparent_temp_min,
         wind_speed_max, wind_gusts_max, wind_direction_dominant,
         radiation_sum, sunshine_duration,
         precipitation_hours, weather_code, evapotranspiration,
         snow_depth_mm,
         data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'snotel')
        ON CONFLICT(station_id, date) DO UPDATE SET
            snowfall_mm = excluded.snowfall_mm,
            temp_mean_celsius = excluded.temp_mean_celsius,
            precipitation_mm = excluded.precipitation_mm,
            temp_max_celsius = excluded.temp_max_celsius,
            temp_min_celsius = excluded.temp_min_celsius,
            snow_depth_mm = excluded.snow_depth_mm,
            data_source = 'snotel'
    """, rows)


def collect_daily(days=14, rate_limit=0.5):
    """Daily mode: fetch recent N days for all SNOTEL stations."""
    logger = setup_logging()

    end_date = datetime.now().strftime('%Y-%m-%d')
    # Fetch extra days so we can compute day-over-day snowfall
    start_date = (datetime.now() - timedelta(days=days + 2)).strftime('%Y-%m-%d')

    logger.info("=" * 80)
    logger.info("SNOTEL DAILY COLLECTION")
    logger.info(f"Stations: {len(SNOTEL_STATIONS)}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=60000")

    success = 0
    failed = 0
    total_records = 0

    for triplet, info in SNOTEL_STATIONS.items():
        try:
            raw = fetch_snotel_csv(triplet, start_date, end_date, logger)
            if not raw:
                logger.info(f"  {info['name']} ({info['snotel_name']}): no data")
                success += 1
                continue

            records = compute_daily_snowfall(raw)
            # Skip first record (no snowfall delta available)
            records = records[1:]

            if records:
                insert_snotel_records(conn, info['station_id'], records)
                conn.commit()
                snow_days = sum(1 for r in records if r.get('snowfall_mm') and r['snowfall_mm'] > 0)
                logger.info(f"  {info['name']} ({info['snotel_name']}): "
                            f"{len(records)} days, {snow_days} snow days")
                total_records += len(records)
            success += 1

        except Exception as e:
            failed += 1
            logger.warning(f"  {info['name']}: FAILED - {e}")

        time.sleep(rate_limit)

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("SNOTEL DAILY COLLECTION COMPLETE")
    logger.info(f"Successful: {success}, Failed: {failed}")
    logger.info(f"Total records: {total_records}")
    logger.info("=" * 80)


def collect_backfill(rate_limit=1.0, start_year=1980):
    """Backfill mode: fetch full period of record for all stations.

    Unlike NOAA, SNOTEL has no daily API call limits, but we're polite
    with rate limiting. Each station is a single HTTP request per year.
    """
    logger = setup_logging()

    logger.info("=" * 80)
    logger.info("SNOTEL BACKFILL COLLECTION")
    logger.info(f"Stations: {len(SNOTEL_STATIONS)}")
    logger.info(f"Year range: {start_year}-{datetime.now().year}")
    logger.info("=" * 80)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=60000")

    # Create progress tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snotel_collection_progress (
            triplet TEXT NOT NULL,
            year INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            records_inserted INTEGER DEFAULT 0,
            completed_at TEXT,
            PRIMARY KEY (triplet, year)
        )
    """)
    conn.commit()

    current_year = datetime.now().year
    success = 0
    skipped = 0
    failed = 0

    for triplet, info in SNOTEL_STATIONS.items():
        logger.info(f"\n{info['name']} ({info['snotel_name']}, {info['elevation_ft']}ft)")

        for year in range(current_year, start_year - 1, -1):
            # Check progress
            existing = conn.execute("""
                SELECT status FROM snotel_collection_progress
                WHERE triplet = ? AND year = ?
            """, (triplet, year)).fetchone()

            if existing and existing[0] in ('completed', 'no_data'):
                skipped += 1
                continue

            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            if year == current_year:
                end_date = datetime.now().strftime('%Y-%m-%d')

            # Fetch extra day before for snowfall delta
            fetch_start = f"{year - 1}-12-31" if year > start_year else start_date

            try:
                raw = fetch_snotel_csv(triplet, fetch_start, end_date, logger)

                if raw:
                    records = compute_daily_snowfall(raw)
                    # Filter to only the target year
                    records = [r for r in records[1:] if r['date'].startswith(str(year))]

                    if records:
                        insert_snotel_records(conn, info['station_id'], records)
                        conn.execute("""
                            INSERT OR REPLACE INTO snotel_collection_progress
                            (triplet, year, status, records_inserted, completed_at)
                            VALUES (?, ?, 'completed', ?, ?)
                        """, (triplet, year, len(records), datetime.now().isoformat()))
                        conn.commit()
                        success += 1
                        logger.info(f"  {year}: {len(records)} days")
                    else:
                        conn.execute("""
                            INSERT OR REPLACE INTO snotel_collection_progress
                            (triplet, year, status, records_inserted, completed_at)
                            VALUES (?, ?, 'no_data', 0, ?)
                        """, (triplet, year, datetime.now().isoformat()))
                        conn.commit()
                        logger.info(f"  {year}: no data")
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO snotel_collection_progress
                        (triplet, year, status, records_inserted, completed_at)
                        VALUES (?, ?, 'no_data', 0, ?)
                    """, (triplet, year, datetime.now().isoformat()))
                    conn.commit()

            except Exception as e:
                failed += 1
                logger.warning(f"  {year}: FAILED - {e}")
                conn.execute("""
                    INSERT OR REPLACE INTO snotel_collection_progress
                    (triplet, year, status, records_inserted, completed_at)
                    VALUES (?, ?, 'failed', 0, ?)
                """, (triplet, year, datetime.now().isoformat()))
                conn.commit()

            time.sleep(rate_limit)

    conn.close()

    logger.info("")
    logger.info("=" * 80)
    logger.info("SNOTEL BACKFILL COMPLETE")
    logger.info(f"Successful: {success}, Skipped: {skipped}, Failed: {failed}")
    logger.info("=" * 80)


def show_status():
    """Show SNOTEL collection progress."""
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=60000")

    try:
        snotel_count = conn.execute(
            "SELECT COUNT(*) FROM snowfall_daily WHERE data_source = 'snotel'"
        ).fetchone()[0]
        snotel_range = conn.execute(
            "SELECT MIN(date), MAX(date) FROM snowfall_daily WHERE data_source = 'snotel'"
        ).fetchone()
        snotel_stations = conn.execute(
            "SELECT COUNT(DISTINCT station_id) FROM snowfall_daily WHERE data_source = 'snotel'"
        ).fetchone()[0]
    except Exception:
        snotel_count = 0
        snotel_range = (None, None)
        snotel_stations = 0

    # Backfill progress
    try:
        cursor = conn.execute("""
            SELECT status, COUNT(*) FROM snotel_collection_progress GROUP BY status
        """)
        counts = dict(cursor.fetchall())
    except Exception:
        counts = {}

    conn.close()

    print(f"\n{'='*80}")
    print("SNOTEL COLLECTION STATUS")
    print(f"{'='*80}")
    print(f"Station mappings: {len(SNOTEL_STATIONS)}")
    print(f"Stations with data: {snotel_stations}")
    print(f"Records in snowfall_daily (source=snotel): {snotel_count:,}")
    if snotel_range[0]:
        print(f"Date range: {snotel_range[0]} to {snotel_range[1]}")
    if counts:
        print(f"\nBackfill progress:")
        print(f"  Completed: {counts.get('completed', 0)}")
        print(f"  No data: {counts.get('no_data', 0)}")
        print(f"  Failed: {counts.get('failed', 0)}")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description='Collect SNOTEL Mountain Snow Data')
    parser.add_argument('--mode', choices=['daily', 'backfill'], default='daily',
                        help='Collection mode (default: daily)')
    parser.add_argument('--days', type=int, default=14,
                        help='Days to fetch in daily mode (default: 14)')
    parser.add_argument('--rate-limit', type=float, default=0.5,
                        help='Seconds between requests (default: 0.5)')
    parser.add_argument('--start-year', type=int, default=1980,
                        help='Earliest year for backfill (default: 1980)')
    parser.add_argument('--status', action='store_true',
                        help='Show collection progress')

    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.mode == 'daily':
        collect_daily(days=args.days, rate_limit=args.rate_limit)
    elif args.mode == 'backfill':
        collect_backfill(rate_limit=args.rate_limit, start_year=args.start_year)


if __name__ == '__main__':
    main()
