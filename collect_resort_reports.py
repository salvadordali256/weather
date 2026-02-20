#!/usr/bin/env python3
"""
Collect Resort Snow Reports via RapidAPI Ski Resort Conditions
Outputs forecast_output/resort_conditions.json

Fields per resort:
  base_depth_in, new_snow_24h_in,
  lifts_open, lifts_total, runs_open, runs_total
"""

import requests
import json
import os
import time
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = os.environ.get('FORECAST_OUTPUT_DIR', 'forecast_output')
RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY', '')

BASE_URL = 'https://ski-resort-conditions.p.rapidapi.com'
HEADERS = {
    'x-rapidapi-host': 'ski-resort-conditions.p.rapidapi.com',
    'x-rapidapi-key': RAPIDAPI_KEY,
}

# station_id -> RapidAPI resort id
# Capped at 15 to stay within free tier (15 requests/day hard limit)
RESORT_MAP = {
    # Colorado
    'vail_co':               'f90d2dd9',
    'breckenridge_co':       '24980f7e',
    'aspen_co':              'd569e475',
    'steamboat_springs_co':  'fb79d359',
    'telluride_co':          '65ef052c',
    'winter_park_co':        '8589c2ad',
    # Wyoming
    'jackson_hole_wy':       '775ba5bc',
    # Montana
    'big_sky_mt':            '8194f66c',
    # Utah
    'park_city_ut':          'dd6be828',
    'alta_ut':               '0f1aa628',   # Snowbird (Alta/Snowbird station)
    # California
    'mammoth_mountain_ca':   '05a3d5b2',
    'lake_tahoe_ca':         '36a82293',   # Palisades Tahoe
    # Vermont
    'stowe_vt':              'de58e28c',
    'killington_vt':         '730c76d3',
    'jay_peak_vt':           '1143859b',
}


def setup_logging():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"resort_reports_{datetime.now().strftime('%Y%m%d')}.log")
    logger = logging.getLogger('resort_reports')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(sh)
    return logger


def parse_depth(val):
    """Parse '78"' or '78' -> float inches, or None."""
    if not val or val in ('N/A', 'n/a', '-', ''):
        return None
    try:
        return float(str(val).replace('"', '').replace("'", '').strip())
    except ValueError:
        return None


def parse_fraction(val):
    """Parse '30/33' -> (30, 33), or (None, None)."""
    if not val or val in ('N/A', 'n/a', '-', ''):
        return None, None
    s = str(val)
    if '/' in s:
        parts = s.split('/')
        try:
            return int(parts[0].strip()), int(parts[1].strip())
        except ValueError:
            return None, None
    try:
        return int(s.strip()), None
    except ValueError:
        return None, None


def fetch_report(resort_id, logger):
    """Fetch one resort's snow report. Returns normalized dict or None."""
    url = f'{BASE_URL}/get_snow_report'
    try:
        resp = requests.get(url, headers=HEADERS, params={'id': resort_id}, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        base_in = parse_depth(data.get('base_depth'))
        new24_in = parse_depth(data.get('snowfall24h'))
        lifts_open, lifts_total = parse_fraction(data.get('open_lifts'))
        runs_open, runs_total = parse_fraction(data.get('open_trails'))

        if base_in is None and lifts_open is None:
            return None

        return {
            'base_depth_in': base_in,
            'new_snow_24h_in': new24_in,
            'lifts_open': lifts_open,
            'lifts_total': lifts_total,
            'runs_open': runs_open,
            'runs_total': runs_total,
            'source': 'rapidapi_ski_conditions',
            'as_of': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        }
    except Exception as e:
        logger.warning(f'    {resort_id}: {e}')
        return None


def collect_resort_reports(rate_limit=0.3):
    logger = setup_logging()

    if not RAPIDAPI_KEY:
        logger.error('RAPIDAPI_KEY not set in environment — aborting')
        return {}

    logger.info('=' * 70)
    logger.info('RESORT CONDITIONS COLLECTION')
    logger.info(f'Resorts: {len(RESORT_MAP)}')
    logger.info(f'Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('=' * 70)

    results = {}
    success = 0
    skipped = 0
    failed = 0

    for station_id, resort_id in RESORT_MAP.items():
        try:
            data = fetch_report(resort_id, logger)
            if data:
                results[station_id] = data
                base_str = f"{data['base_depth_in']:.0f}\"" if data['base_depth_in'] else '?'
                new_str = f"+{data['new_snow_24h_in']:.0f}\"" if data['new_snow_24h_in'] else ''
                lifts_str = (f"{data['lifts_open']}/{data['lifts_total']} lifts"
                             if data['lifts_open'] is not None else '')
                runs_str = (f"{data['runs_open']}/{data['runs_total']} runs"
                            if data['runs_open'] is not None else '')
                logger.info(f'  {station_id}: {base_str} {new_str}  {lifts_str}  {runs_str}')
                success += 1
            else:
                logger.info(f'  {station_id}: no data')
                skipped += 1
        except Exception as e:
            logger.warning(f'  {station_id}: FAILED — {e}')
            failed += 1
        time.sleep(rate_limit)

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / 'resort_conditions.json'

    output = {
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'resort_count': len(results),
        'resorts': results,
    }
    with open(out_file, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    logger.info('')
    logger.info('=' * 70)
    logger.info(f'Success: {success}  Skipped: {skipped}  Failed: {failed}')
    logger.info(f'Output: {out_file} ({out_file.stat().st_size / 1024:.1f} KB)')
    logger.info('=' * 70)

    return results


if __name__ == '__main__':
    collect_resort_reports()
