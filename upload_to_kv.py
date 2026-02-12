#!/usr/bin/env python3
"""
Upload station_data.json to Cloudflare Workers KV.
Reads the generated JSON and writes per-station KV keys + an index key.

Usage:
    python upload_to_kv.py [--namespace-id ID]

Requires wrangler CLI to be installed and authenticated.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = os.environ.get('FORECAST_OUTPUT_DIR', 'forecast_output')
DEFAULT_NS_ID = os.environ.get('KV_NAMESPACE_ID', '')


def run_kv_put(namespace_id, key, value_json):
    """Write a single KV key via wrangler."""
    cmd = [
        'npx', 'wrangler', 'kv:key', 'put',
        '--namespace-id', namespace_id,
        key,
        value_json,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd='worker')
    if result.returncode != 0:
        print(f"  ERROR writing {key}: {result.stderr.strip()}")
        return False
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Upload station data to KV')
    parser.add_argument('--namespace-id', default=DEFAULT_NS_ID,
                        help='KV namespace ID')
    args = parser.parse_args()

    if not args.namespace_id:
        print("ERROR: No namespace ID. Set KV_NAMESPACE_ID in .env or pass --namespace-id")
        sys.exit(1)

    data_path = Path(OUTPUT_DIR) / 'station_data.json'
    if not data_path.exists():
        print(f"ERROR: {data_path} not found. Run generate_station_forecasts.py first.")
        sys.exit(1)

    with open(data_path) as f:
        data = json.load(f)

    stations = data.get('stations', {})
    print(f"Uploading {len(stations)} stations to KV namespace {args.namespace_id}...")

    # Build and upload station index
    index = []
    for sid, s in stations.items():
        index.append({
            'id': sid,
            'name': s['name'],
            'region': s['region'],
            'lat': s['lat'],
            'lon': s['lon'],
            'snow_score': s['snow_score'],
        })

    ok = run_kv_put(args.namespace_id, 'station_index', json.dumps(index))
    if ok:
        print(f"  station_index: OK ({len(index)} entries)")

    # Upload per-station data
    success = 0
    for sid, s in stations.items():
        ok = run_kv_put(args.namespace_id, f'station:{sid}', json.dumps(s))
        if ok:
            success += 1
            print(f"  station:{sid}: OK")

    print(f"\nDone: {success}/{len(stations)} stations uploaded.")


if __name__ == '__main__':
    main()
