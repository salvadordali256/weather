#!/usr/bin/env python3
"""
Collect Resort Snow Reports
Fetches current resort conditions (base depth, new snow, lifts/runs open)
from multiple sources and outputs forecast_output/resort_conditions.json.

Sources:
  - SnoCountry/Ski3 API  (~60% of US resorts)
  - Vail Resorts conditions pages
  - Mountain resorts API (independent resorts)

Runs daily before generate_station_forecasts.py.
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
OUTPUT_FILE = 'resort_conditions.json'

INCHES_TO_MM = 25.4

# ─── Resort source registry ───────────────────────────────────────────────────
# Each entry: station_id -> {type, ...source-specific params}
#
# type='snocountry' : SnoCountry/Ski3 JSON API
# type='vail'       : Vail Resorts conditions page (HTML scrape)
# type='powder'     : OnTheSnow/powder.com API (public endpoint)
# type='manual'     : No live source — skip gracefully

RESORT_SOURCES = {
    # ── Vermont ────────────────────────────────────────────────────────────
    'stowe_vt':              {'type': 'snocountry', 'id': 'VT019'},
    'killington_vt':         {'type': 'snocountry', 'id': 'VT009'},
    'jay_peak_vt':           {'type': 'snocountry', 'id': 'VT006'},
    'smugglers_notch_vt':    {'type': 'snocountry', 'id': 'VT020'},
    'okemo_vt':              {'type': 'vail',       'slug': 'okemo'},

    # ── Maine ──────────────────────────────────────────────────────────────
    'sugarloaf_me':          {'type': 'snocountry', 'id': 'ME003'},
    'sunday_river_me':       {'type': 'snocountry', 'id': 'ME002'},

    # ── New Hampshire ──────────────────────────────────────────────────────
    'loon_mountain_nh':      {'type': 'snocountry', 'id': 'NH006'},
    'bretton_woods_nh':      {'type': 'snocountry', 'id': 'NH001'},
    'cannon_mountain_nh':    {'type': 'snocountry', 'id': 'NH002'},

    # ── New York ───────────────────────────────────────────────────────────
    'whiteface_ny':          {'type': 'snocountry', 'id': 'NY016'},
    'gore_mountain_ny':      {'type': 'snocountry', 'id': 'NY007'},

    # ── Minnesota ──────────────────────────────────────────────────────────
    'lutsen_mn':             {'type': 'snocountry', 'id': 'MN004'},

    # ── Colorado ───────────────────────────────────────────────────────────
    'vail_co':               {'type': 'vail',       'slug': 'vail'},
    'breckenridge_co':       {'type': 'vail',       'slug': 'breckenridge'},
    'park_city_ut':          {'type': 'vail',       'slug': 'park-city'},
    'northstar_ca':          {'type': 'vail',       'slug': 'northstar'},
    'heavenly_ca':           {'type': 'vail',       'slug': 'heavenly'},
    'stevens_pass_wa':       {'type': 'vail',       'slug': 'stevens-pass'},
    'crystal_mountain_wa':   {'type': 'vail',       'slug': 'crystal-mountain'},
    'stowe_vt':              {'type': 'vail',       'slug': 'stowe'},   # overrides snocountry

    'aspen_co':              {'type': 'powder',     'resort_id': 'aspen'},
    'steamboat_springs_co':  {'type': 'powder',     'resort_id': 'steamboat'},
    'telluride_co':          {'type': 'powder',     'resort_id': 'telluride'},
    'winter_park_co':        {'type': 'powder',     'resort_id': 'winter-park'},
    'crested_butte_co':      {'type': 'powder',     'resort_id': 'crested-butte'},
    'monarch_co':            {'type': 'powder',     'resort_id': 'monarch'},

    # ── Wyoming ────────────────────────────────────────────────────────────
    'jackson_hole_wy':       {'type': 'powder',     'resort_id': 'jackson-hole'},
    'grand_targhee_wy':      {'type': 'powder',     'resort_id': 'grand-targhee'},

    # ── Montana ────────────────────────────────────────────────────────────
    'big_sky_mt':            {'type': 'powder',     'resort_id': 'big-sky'},

    # ── Idaho ──────────────────────────────────────────────────────────────
    'sun_valley_id':         {'type': 'powder',     'resort_id': 'sun-valley'},
    'schweitzer_id':         {'type': 'powder',     'resort_id': 'schweitzer'},

    # ── Utah ───────────────────────────────────────────────────────────────
    'alta_ut':               {'type': 'powder',     'resort_id': 'alta'},

    # ── New Mexico ─────────────────────────────────────────────────────────
    'taos_nm':               {'type': 'powder',     'resort_id': 'taos'},

    # ── California ─────────────────────────────────────────────────────────
    'mammoth_mountain_ca':   {'type': 'powder',     'resort_id': 'mammoth-mountain'},
    'lake_tahoe_ca':         {'type': 'powder',     'resort_id': 'palisades-tahoe'},

    # ── Washington/Oregon ──────────────────────────────────────────────────
    'mount_baker_wa':        {'type': 'powder',     'resort_id': 'mt-baker'},
    'snoqualmie_pass_wa':    {'type': 'powder',     'resort_id': 'summit-at-snoqualmie'},
    'mount_hood_or':         {'type': 'powder',     'resort_id': 'timberline-lodge'},
    'mt_bachelor_or':        {'type': 'powder',     'resort_id': 'mt-bachelor'},

    # ── Canada BC ──────────────────────────────────────────────────────────
    'whistler_bc':           {'type': 'powder',     'resort_id': 'whistler-blackcomb'},
    'big_white_bc':          {'type': 'powder',     'resort_id': 'big-white'},
    'sun_peaks_bc':          {'type': 'powder',     'resort_id': 'sun-peaks'},
    'fernie_bc':             {'type': 'powder',     'resort_id': 'fernie-alpine'},
    'revelstoke_bc':         {'type': 'powder',     'resort_id': 'revelstoke'},

    # ── Canada AB/QC ───────────────────────────────────────────────────────
    'tremblant_qc':          {'type': 'powder',     'resort_id': 'tremblant'},
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


SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (compatible; SnowReportBot/1.0)',
    'Accept': 'application/json, text/html',
})


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ─── SnoCountry API ───────────────────────────────────────────────────────────
SNOCOUNTRY_BASE = 'https://feeds.snocountry.net/conditions.php'

def fetch_snocountry(resort_id, logger):
    """
    Fetch from SnoCountry feeds API.
    Returns normalized dict or None.
    """
    url = f"{SNOCOUNTRY_BASE}?apiKey=snocountry&ids={resort_id}"
    try:
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        # SnoCountry wraps in an array
        items = data.get('items', data) if isinstance(data, dict) else data
        if not items:
            return None
        item = items[0] if isinstance(items, list) else items

        base_in = _safe_float(item.get('baseDepthMax') or item.get('baseDepth'))
        summit_in = _safe_float(item.get('snowDepthSummit'))
        new24_in = _safe_float(item.get('newSnow24') or item.get('newSnow'))
        new48_in = _safe_float(item.get('newSnow48'))
        new7d_in = _safe_float(item.get('snowfall7Day'))
        season_in = _safe_float(item.get('seasonSnowfall'))
        lifts_open = _safe_int(item.get('liftsOpen'))
        lifts_total = _safe_int(item.get('liftsTotal') or item.get('numberOfLifts'))
        runs_open = _safe_int(item.get('trailsOpen') or item.get('runsOpen'))
        runs_total = _safe_int(item.get('trailsTotal') or item.get('totalRuns'))

        if base_in is None and lifts_open is None:
            return None

        return {
            'base_depth_in': base_in,
            'summit_depth_in': summit_in,
            'new_snow_24h_in': new24_in,
            'new_snow_48h_in': new48_in,
            'new_snow_7d_in': new7d_in,
            'season_total_in': season_in,
            'lifts_open': lifts_open,
            'lifts_total': lifts_total,
            'runs_open': runs_open,
            'runs_total': runs_total,
            'source': 'snocountry',
        }
    except Exception as e:
        logger.warning(f"    SnoCountry {resort_id}: {e}")
        return None


# ─── Vail Resorts conditions page ─────────────────────────────────────────────
VAIL_CONDITIONS_URL = 'https://www.snow.com/content/snow/resort-conditions/{slug}/_jcr_content/mainColumnParsys/resortconditions.model.json'

def fetch_vail(slug, logger):
    """
    Fetch Vail Resorts conditions JSON endpoint.
    Returns normalized dict or None.
    """
    url = VAIL_CONDITIONS_URL.format(slug=slug)
    try:
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        # Navigate the Vail JSON structure
        conditions = data.get('snowConditions', data.get('conditions', {}))
        if not conditions:
            return None

        base_in = _safe_float(
            conditions.get('baseDepthMin') or conditions.get('base') or
            data.get('base') or data.get('baseDepth')
        )
        summit_in = _safe_float(
            conditions.get('baseDepthMax') or conditions.get('summit') or data.get('summit')
        )
        new24_in = _safe_float(
            conditions.get('snowfall24Hours') or conditions.get('new24h') or data.get('newSnow24h')
        )
        new48_in = _safe_float(
            conditions.get('snowfall48Hours') or data.get('newSnow48h')
        )
        new7d_in = _safe_float(
            conditions.get('snowfall7Days') or data.get('newSnow7d')
        )
        season_in = _safe_float(
            conditions.get('seasonSnowfall') or data.get('seasonTotal')
        )

        # Lifts and runs
        lifts = data.get('lifts', {})
        trails = data.get('trails', {})
        lifts_open = _safe_int(lifts.get('open') or data.get('liftsOpen'))
        lifts_total = _safe_int(lifts.get('total') or data.get('liftsTotal'))
        runs_open = _safe_int(trails.get('open') or data.get('trailsOpen'))
        runs_total = _safe_int(trails.get('total') or data.get('trailsTotal'))

        if base_in is None and lifts_open is None:
            return None

        return {
            'base_depth_in': base_in,
            'summit_depth_in': summit_in,
            'new_snow_24h_in': new24_in,
            'new_snow_48h_in': new48_in,
            'new_snow_7d_in': new7d_in,
            'season_total_in': season_in,
            'lifts_open': lifts_open,
            'lifts_total': lifts_total,
            'runs_open': runs_open,
            'runs_total': runs_total,
            'source': 'vail_resorts',
        }
    except Exception as e:
        logger.warning(f"    Vail {slug}: {e}")
        return None


# ─── OnTheSnow / Powder.com public API ────────────────────────────────────────
POWDER_API = 'https://www.onthesnow.com/api/v2/resort/{resort_id}/snow-report'

def fetch_powder(resort_id, logger):
    """
    Fetch from OnTheSnow public API.
    Returns normalized dict or None.
    """
    url = POWDER_API.format(resort_id=resort_id)
    try:
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        sr = data.get('snowReport', data.get('data', data))
        if not sr:
            return None

        base_in = _safe_float(sr.get('baseDepth') or sr.get('base'))
        summit_in = _safe_float(sr.get('summitDepth') or sr.get('summit'))
        new24_in = _safe_float(sr.get('newSnow24h') or sr.get('snow24h'))
        new48_in = _safe_float(sr.get('newSnow48h') or sr.get('snow48h'))
        new7d_in = _safe_float(sr.get('newSnow7d') or sr.get('snow7d'))
        season_in = _safe_float(sr.get('seasonTotal') or sr.get('totalSeason'))
        lifts_open = _safe_int(sr.get('liftsOpen') or sr.get('openLifts'))
        lifts_total = _safe_int(sr.get('liftsTotal') or sr.get('totalLifts'))
        runs_open = _safe_int(sr.get('trailsOpen') or sr.get('openTrails'))
        runs_total = _safe_int(sr.get('trailsTotal') or sr.get('totalTrails'))

        if base_in is None and lifts_open is None:
            return None

        return {
            'base_depth_in': base_in,
            'summit_depth_in': summit_in,
            'new_snow_24h_in': new24_in,
            'new_snow_48h_in': new48_in,
            'new_snow_7d_in': new7d_in,
            'season_total_in': season_in,
            'lifts_open': lifts_open,
            'lifts_total': lifts_total,
            'runs_open': runs_open,
            'runs_total': runs_total,
            'source': 'onthesnow',
        }
    except Exception as e:
        logger.warning(f"    OnTheSnow {resort_id}: {e}")
        return None


# ─── Dispatcher ───────────────────────────────────────────────────────────────

def fetch_resort(station_id, source_cfg, logger):
    """Dispatch to the right fetcher based on source type."""
    src_type = source_cfg.get('type')
    raw = None

    if src_type == 'snocountry':
        raw = fetch_snocountry(source_cfg['id'], logger)
    elif src_type == 'vail':
        raw = fetch_vail(source_cfg['slug'], logger)
    elif src_type == 'powder':
        raw = fetch_powder(source_cfg['resort_id'], logger)

    if raw is None:
        return None

    # Attach metadata
    raw['as_of'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    return raw


def collect_resort_reports(rate_limit=0.5):
    logger = setup_logging()

    logger.info('=' * 70)
    logger.info('RESORT CONDITIONS COLLECTION')
    logger.info(f'Resorts: {len(RESORT_SOURCES)}')
    logger.info(f'Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('=' * 70)

    results = {}
    success = 0
    failed = 0
    skipped = 0

    for station_id, source_cfg in RESORT_SOURCES.items():
        src_type = source_cfg.get('type', 'unknown')
        try:
            data = fetch_resort(station_id, source_cfg, logger)
            if data:
                results[station_id] = data
                base_str = f"{data['base_depth_in']:.0f}\"" if data.get('base_depth_in') else 'no base'
                lifts_str = (f"{data['lifts_open']}/{data['lifts_total']} lifts"
                             if data.get('lifts_open') is not None else '')
                logger.info(f"  {station_id} [{src_type}]: {base_str} {lifts_str}")
                success += 1
            else:
                logger.info(f"  {station_id} [{src_type}]: no data")
                skipped += 1
        except Exception as e:
            logger.warning(f"  {station_id}: FAILED — {e}")
            failed += 1
        time.sleep(rate_limit)

    # Write output
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / OUTPUT_FILE

    output = {
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'resort_count': len(results),
        'resorts': results,
    }
    with open(out_file, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    logger.info('')
    logger.info('=' * 70)
    logger.info('RESORT COLLECTION COMPLETE')
    logger.info(f'Success: {success}  Skipped: {skipped}  Failed: {failed}')
    logger.info(f'Output: {out_file} ({out_file.stat().st_size / 1024:.1f} KB)')
    logger.info('=' * 70)

    return results


if __name__ == '__main__':
    collect_resort_reports()
