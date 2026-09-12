#!/usr/bin/env python3
"""
Validate Open-Meteo/ERA5 snowfall against a fresh NOAA pull for the same
station and date range -- a genuine apples-to-apples check, unlike
validate_era5_bias.py's in-DB comparison (which found zero same-date
overlap between existing noaa/open-meteo rows for every dual-sourced
station -- a dead end for validating this way with what's already stored).

Reuses the exact GHCND station IDs already vetted in
scripts/collect/collect_noaa_data.py's NOAA_STATIONS mapping, since those
are proven correct (they're what populated this DB's own 'noaa' rows).

None of the three core "Primary forecast target" stations (eagle_river_wi,
phelps_wi, land_o_lakes_wi) have a NOAA mapping at all -- they're 100%
Open-Meteo/ERA5 with no ground-truth cross-check anywhere in this system.
This script validates nearby/comparable stations instead as a proxy for
whether the bias exists in this region generally.

Requires NOAA_API_TOKEN in .env (same one collect_noaa_data.py uses).

Usage:
    python scripts/collect/validate_against_noaa_external.py
    python scripts/collect/validate_against_noaa_external.py --start 2023-11-01 --end 2024-04-30
"""

import argparse
import os
import time

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from snowforecast.storage.db import get_engine

load_dotenv()

NOAA_TOKEN = os.environ.get("NOAA_API_TOKEN", "")
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

# Subset of collect_noaa_data.py's NOAA_STATIONS -- known-good GHCND IDs,
# chosen as comparable/nearby proxies since the core 3 target stations
# have no NOAA mapping at all.
VALIDATION_STATIONS = {
    "iron_mountain_mi": "USW00094892",
    "granite_peak_wi": "USC00478905",
    "duluth_mn": "USW00014839",
}


def noaa_api_request(endpoint, params, max_retries=3):
    headers = {"token": NOAA_TOKEN}
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 429:
                wait = min(60 * (attempt + 1), 300)
                print(f"  429 rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(5)
    return None


def fetch_noaa_snow(ghcnd_id: str, start_date: str, end_date: str) -> dict:
    """Returns {date: snow_mm} from NOAA for this GHCND station/range."""
    all_results = []
    offset = 1
    limit = 1000
    while True:
        params = {
            "datasetid": "GHCND",
            "stationid": f"GHCND:{ghcnd_id}",
            "startdate": start_date,
            "enddate": end_date,
            "datatypeid": "SNOW",
            "units": "metric",
            "limit": limit,
            "offset": offset,
        }
        data = noaa_api_request("data", params)
        if not data or "results" not in data:
            break
        all_results.extend(data["results"])
        total = data.get("metadata", {}).get("resultset", {}).get("count", 0)
        if offset + limit > total:
            break
        offset += limit
        time.sleep(0.25)

    return {r["date"][:10]: r["value"] for r in all_results}


def fetch_our_open_meteo_snow(engine, station_id: str, start_date: str, end_date: str) -> dict:
    query = text(
        """
        SELECT date, snowfall_mm
        FROM snowfall_daily
        WHERE station_id = :station_id
          AND data_source = 'open-meteo'
          AND date >= :start_date AND date <= :end_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            query, {"station_id": station_id, "start_date": start_date, "end_date": end_date}
        ).fetchall()
    return {r.date: r.snowfall_mm for r in rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2023-11-01")
    parser.add_argument("--end", default="2024-04-30")
    args = parser.parse_args()

    if not NOAA_TOKEN or "YOUR_" in NOAA_TOKEN:
        print("NOAA_API_TOKEN not set in .env -- cannot pull real NOAA data. Aborting.")
        return

    engine = get_engine()

    print("=" * 90)
    print(f"EXTERNAL NOAA VALIDATION: {args.start} to {args.end}")
    print("=" * 90)

    for station_id, ghcnd_id in VALIDATION_STATIONS.items():
        print(f"\n{station_id} (GHCND:{ghcnd_id})")
        print("-" * 90)

        noaa = fetch_noaa_snow(ghcnd_id, args.start, args.end)
        ours = fetch_our_open_meteo_snow(engine, station_id, args.start, args.end)

        overlap_dates = sorted(set(noaa) & set(ours))
        if not overlap_dates:
            print("  No overlapping dates between fresh NOAA pull and our open-meteo rows.")
            continue

        noaa_total = sum(noaa[d] or 0.0 for d in overlap_dates)
        our_total = sum(ours[d] or 0.0 for d in overlap_dates)
        ratio = (noaa_total / our_total) if our_total else float("nan")

        print(f"  {len(overlap_dates)} overlapping days")
        print(f"  NOAA (real) total:        {noaa_total:.1f} mm")
        print(f"  Our open-meteo total:     {our_total:.1f} mm")
        print(f"  Ratio (NOAA / ours):      {ratio:.2f}x")
        if ratio > 1.15:
            print(f"  -> Our data UNDERSTATES actual snowfall by ~{(ratio - 1) * 100:.0f}% for this station/period")
        elif ratio < 0.87:
            print(f"  -> Our data OVERSTATES actual snowfall by ~{(1 - ratio) * 100:.0f}% for this station/period")
        else:
            print("  -> Roughly agrees with NOAA (within ~15%) for this station/period")


if __name__ == "__main__":
    main()
