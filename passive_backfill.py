#!/usr/bin/env python3
"""
Passive Historical Data Backfill
Runs daily via cron to gradually collect historical weather data for all stations.
Uses Open-Meteo Archive API with budget-aware rate limiting.
"""

import requests
import sqlite3
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get('DB_PATH', 'demo_global_snowfall.db')

# All daily variables to collect
DAILY_VARIABLES = (
    'snowfall_sum,precipitation_sum,rain_sum,'
    'temperature_2m_max,temperature_2m_min,temperature_2m_mean,'
    'apparent_temperature_max,apparent_temperature_min,'
    'wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,'
    'shortwave_radiation_sum,sunshine_duration,'
    'precipitation_hours,weather_code,'
    'et0_fao_evapotranspiration'
)
NUM_VARIABLES = 17

# Import station list from collect_world_data
from collect_world_data import WORLD_STATIONS, migrate_schema


def setup_logging():
    """Configure logging to file and console"""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"passive_backfill_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger('backfill')
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(sh)

    return logger


def calculate_weighted_cost(num_variables, num_days):
    """Open-Meteo weighted API call formula"""
    return max(num_variables / 10, (num_variables / 10) * (num_days / 7))


class BackfillTracker:
    """Manages historical data backfill with progress tracking and budget control"""

    def __init__(self, db_path=DB_PATH, budget=5000, rate_limit=1.5, start_year=1940):
        self.db_path = db_path
        self.budget = budget
        self.rate_limit = rate_limit
        self.start_year = start_year
        self.current_year = datetime.now().year
        self.logger = setup_logging()
        self.weighted_calls_used = 0.0

        # Build flat station list from WORLD_STATIONS
        self.stations = {}
        for region, station_list in WORLD_STATIONS.items():
            for station_id, lat, lon, name in station_list:
                self.stations[station_id] = {
                    'lat': lat, 'lon': lon, 'name': name, 'region': region
                }

    def setup_tables(self, conn):
        """Create progress tracking table and run schema migration"""
        migrate_schema(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backfill_progress (
                station_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                records_inserted INTEGER DEFAULT 0,
                completed_at TEXT,
                error_message TEXT,
                PRIMARY KEY (station_id, year)
            )
        """)
        conn.commit()

    def get_station_coverage(self, conn):
        """Get completed years per station from progress table"""
        cursor = conn.execute("""
            SELECT station_id, year FROM backfill_progress
            WHERE status IN ('completed', 'no_data')
        """)
        coverage = defaultdict(set)
        for station_id, year in cursor:
            coverage[station_id].add(year)
        return coverage

    def get_work_queue(self, conn, max_priority=3):
        """Build prioritized work queue of (station_id, year) pairs"""
        coverage = self.get_station_coverage(conn)

        # Check which stations have ANY data in snowfall_daily
        cursor = conn.execute("""
            SELECT station_id, COUNT(*) as cnt
            FROM snowfall_daily GROUP BY station_id
        """)
        existing_data = {row[0]: row[1] for row in cursor}

        # Priority 1: Stations with NO data at all — recent years first
        priority1 = []
        # Priority 2: Stations with some data — fill gaps
        priority2 = []
        # Priority 3: All remaining station-years
        priority3 = []

        all_years = list(range(self.current_year, self.start_year - 1, -1))

        for station_id in self.stations:
            completed = coverage.get(station_id, set())
            has_data = station_id in existing_data

            for year in all_years:
                if year in completed:
                    continue

                if not has_data:
                    priority1.append((station_id, year))
                elif existing_data.get(station_id, 0) < 20000:
                    priority2.append((station_id, year))
                else:
                    priority3.append((station_id, year))

        # Round-robin within each priority tier
        queue = []
        if max_priority >= 1:
            queue.extend(self._round_robin(priority1))
        if max_priority >= 2:
            queue.extend(self._round_robin(priority2))
        if max_priority >= 3:
            queue.extend(self._round_robin(priority3))

        return queue

    def _round_robin(self, items):
        """Reorder items in round-robin by station (one year per station before repeating)"""
        by_station = defaultdict(list)
        for station_id, year in items:
            by_station[station_id].append(year)

        result = []
        station_ids = list(by_station.keys())
        max_len = max((len(v) for v in by_station.values()), default=0)

        for i in range(max_len):
            for sid in station_ids:
                years = by_station[sid]
                if i < len(years):
                    result.append((sid, years[i]))

        return result

    def fetch_year(self, station_id, year):
        """Fetch one year of daily data for a station"""
        station = self.stations[station_id]
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        if year == self.current_year:
            end_date = datetime.now().strftime('%Y-%m-%d')

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': station['lat'],
            'longitude': station['lon'],
            'start_date': start_date,
            'end_date': end_date,
            'daily': DAILY_VARIABLES,
            'timezone': 'UTC'
        }

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if 'daily' not in data or 'time' not in data['daily']:
            return 0, []

        d = data['daily']
        dates = d['time']
        n = len(dates)

        def get(key):
            return d.get(key, [None] * n)

        records = []
        for i, date in enumerate(dates):
            snow_raw = get('snowfall_sum')[i]
            snow_mm = snow_raw * 10.0 if snow_raw is not None else None
            t_max = get('temperature_2m_max')[i]
            t_min = get('temperature_2m_min')[i]
            temp_mean = get('temperature_2m_mean')[i]
            if temp_mean is None and t_max is not None and t_min is not None:
                temp_mean = (t_max + t_min) / 2.0

            records.append((
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

        return len(records), records

    def insert_records(self, conn, records):
        """Batch insert records into snowfall_daily"""
        conn.executemany("""
            INSERT OR REPLACE INTO snowfall_daily
            (station_id, date, snowfall_mm, temp_mean_celsius,
             precipitation_mm, rain_mm, temp_max_celsius, temp_min_celsius,
             apparent_temp_max, apparent_temp_min,
             wind_speed_max, wind_gusts_max, wind_direction_dominant,
             radiation_sum, sunshine_duration,
             precipitation_hours, weather_code, evapotranspiration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

    def mark_progress(self, conn, station_id, year, status, records=0, error=None):
        """Update backfill progress"""
        conn.execute("""
            INSERT OR REPLACE INTO backfill_progress
            (station_id, year, status, records_inserted, completed_at, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (station_id, year, status, records,
              datetime.now().isoformat() if status != 'pending' else None, error))

    def run(self, dry_run=False):
        """Main backfill loop"""
        self.logger.info("=" * 80)
        self.logger.info("PASSIVE HISTORICAL BACKFILL")
        self.logger.info(f"Budget: {self.budget} weighted calls")
        self.logger.info(f"Rate limit: {self.rate_limit}s between calls")
        self.logger.info(f"Stations: {len(self.stations)}")
        self.logger.info(f"Year range: {self.start_year}-{self.current_year}")
        self.logger.info("=" * 80)

        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA busy_timeout=60000")
        self.setup_tables(conn)

        # Register all stations
        for station_id, info in self.stations.items():
            conn.execute("""
                INSERT OR IGNORE INTO stations
                (station_id, name, latitude, longitude, region, significance)
                VALUES (?, ?, ?, ?, ?, 'open-meteo')
            """, (station_id, info['name'], info['lat'], info['lon'], info['region']))
        conn.commit()

        queue = self.get_work_queue(conn)
        self.logger.info(f"Work queue: {len(queue)} station-years to process")

        if dry_run:
            self.logger.info("DRY RUN — showing first 50 items:")
            for station_id, year in queue[:50]:
                name = self.stations[station_id]['name']
                cost = calculate_weighted_cost(NUM_VARIABLES, 365)
                self.logger.info(f"  {name} ({station_id}) — {year} — cost: {cost:.1f}")
            total_cost = len(queue) * calculate_weighted_cost(NUM_VARIABLES, 365)
            days_needed = total_cost / self.budget if self.budget > 0 else float('inf')
            self.logger.info(f"\nTotal estimated cost: {total_cost:.0f} weighted calls")
            self.logger.info(f"Days to complete at {self.budget}/day budget: {days_needed:.0f}")
            conn.close()
            return

        success = 0
        failed = 0
        skipped = 0
        cost_per_year = calculate_weighted_cost(NUM_VARIABLES, 365)

        for station_id, year in queue:
            if self.weighted_calls_used + cost_per_year > self.budget:
                self.logger.info(f"Budget exhausted ({self.weighted_calls_used:.1f}/{self.budget})")
                break

            name = self.stations[station_id]['name']

            try:
                count, records = self.fetch_year(station_id, year)

                if count > 0:
                    self.insert_records(conn, records)
                    self.mark_progress(conn, station_id, year, 'completed', count)
                    conn.commit()
                    success += 1
                    self.logger.info(f"  {name} {year}: {count} records")
                else:
                    self.mark_progress(conn, station_id, year, 'no_data')
                    conn.commit()
                    skipped += 1
                    self.logger.info(f"  {name} {year}: no data")

                self.weighted_calls_used += cost_per_year
                consecutive_429s = 0

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    consecutive_429s = getattr(self, '_c429', 0) + 1
                    self._c429 = consecutive_429s
                    backoff = min(60, 5 * consecutive_429s)
                    self.logger.warning(f"  {name} {year}: 429 rate limited — waiting {backoff}s")
                    time.sleep(backoff)
                    # Don't mark as failed or count budget — will retry next run
                    if consecutive_429s >= 5:
                        self.logger.info(f"Hit 5 consecutive 429s — stopping to avoid wasting calls")
                        break
                    continue
                else:
                    self.mark_progress(conn, station_id, year, 'failed', error=str(e))
                    conn.commit()
                    failed += 1
                    self.logger.warning(f"  {name} {year}: FAILED — {e}")
                    self.weighted_calls_used += cost_per_year

            except Exception as e:
                self.mark_progress(conn, station_id, year, 'failed', error=str(e))
                conn.commit()
                failed += 1
                self.logger.warning(f"  {name} {year}: FAILED — {e}")
                self.weighted_calls_used += cost_per_year

            time.sleep(self.rate_limit)

        conn.close()

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("BACKFILL COMPLETE")
        self.logger.info(f"Successful: {success}")
        self.logger.info(f"No data: {skipped}")
        self.logger.info(f"Failed: {failed}")
        self.logger.info(f"Weighted calls used: {self.weighted_calls_used:.1f}/{self.budget}")
        self.logger.info("=" * 80)

    def show_status(self):
        """Print backfill progress report"""
        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA busy_timeout=60000")

        # Overall progress
        total_possible = len(self.stations) * (self.current_year - self.start_year + 1)

        cursor = conn.execute("SELECT status, COUNT(*) FROM backfill_progress GROUP BY status")
        status_counts = dict(cursor.fetchall())

        completed = status_counts.get('completed', 0)
        no_data = status_counts.get('no_data', 0)
        failed = status_counts.get('failed', 0)
        done = completed + no_data
        remaining = total_possible - done - failed

        print(f"\n{'='*80}")
        print("BACKFILL PROGRESS")
        print(f"{'='*80}")
        print(f"Total station-years: {total_possible}")
        print(f"Completed: {completed}")
        print(f"No data: {no_data}")
        print(f"Failed: {failed}")
        print(f"Remaining: {remaining}")
        pct = (done / total_possible * 100) if total_possible > 0 else 0
        print(f"Progress: {pct:.1f}%")

        cost_per = calculate_weighted_cost(NUM_VARIABLES, 365)
        days_left = (remaining * cost_per) / self.budget if self.budget > 0 else 0
        print(f"Est. days remaining: {days_left:.0f}")

        # Per-region breakdown
        print(f"\n{'='*80}")
        print("BY REGION:")
        print(f"{'='*80}")

        for region, station_list in WORLD_STATIONS.items():
            station_ids = [s[0] for s in station_list]
            placeholders = ','.join('?' * len(station_ids))
            cursor = conn.execute(f"""
                SELECT COUNT(*) FROM backfill_progress
                WHERE station_id IN ({placeholders})
                AND status IN ('completed', 'no_data')
            """, station_ids)
            region_done = cursor.fetchone()[0]
            region_total = len(station_ids) * (self.current_year - self.start_year + 1)
            region_pct = (region_done / region_total * 100) if region_total > 0 else 0
            bar = '#' * int(region_pct / 5) + '-' * (20 - int(region_pct / 5))
            print(f"  {region:30s} [{bar}] {region_pct:5.1f}% ({region_done}/{region_total})")

        conn.close()
        print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description='Passive Historical Data Backfill')
    parser.add_argument('--budget', type=float, default=5000,
                        help='Max weighted API calls per run (default: 5000)')
    parser.add_argument('--rate-limit', type=float, default=1.5,
                        help='Seconds between API calls (default: 1.5)')
    parser.add_argument('--start-year', type=int, default=1940,
                        help='Earliest year to backfill (default: 1940)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be collected without making API calls')
    parser.add_argument('--status', action='store_true',
                        help='Show backfill progress and exit')

    args = parser.parse_args()

    tracker = BackfillTracker(
        budget=args.budget,
        rate_limit=args.rate_limit,
        start_year=args.start_year,
    )

    if args.status:
        tracker.show_status()
    else:
        tracker.run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
