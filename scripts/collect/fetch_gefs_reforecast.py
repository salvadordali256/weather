#!/usr/bin/env python3
"""
Extract NOAA GEFSv12 reforecast (control member) at the forecast target stations.

Used only to strengthen the NWP engine's calibration
(scripts/backtest/fit_nwp_calibration.py --gefs-dir): Open-Meteo's archive of
past forecasts starts in winter 2023-24, while the GEFSv12 reforecast covers
2000-2019 with one 00Z run per day. Not part of the daily pipeline.

Source (public, no credentials):
    s3://noaa-gefs-retrospective/GEFSv12/reforecast/YYYY/YYYYMMDD00/c00/Days:1-10/
Files are global 0.25-degree GRIB2; the .idx sidecars let us byte-range download
just the messages needed -- 6-hour precipitation accumulations and 6-hourly 2m
temperature out to 168h -- then take the nearest grid point per station.

Resumable: each finished init is written atomically to <out-dir>/<init>.csv and
skipped on rerun. --every 3 (default) samples every 3rd Nov-Mar day, roughly an
hour on a home connection; --every 1 densifies later.

Output columns: init, station_id, var (apcp mm | tmp K), lead_hour, value

Usage:
    python scripts/collect/fetch_gefs_reforecast.py --out-dir gefs_reforecast --test
    python scripts/collect/fetch_gefs_reforecast.py --out-dir gefs_reforecast --every 3
"""

import argparse
import csv
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import eccodes

from snowforecast.engines.nwp_snowfall_forecast import TARGET_STATIONS

BASE = "https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast"
MAX_HOUR = 168


def get(url, byte_range=None, tries=5):
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url)
            if byte_range:
                req.add_header("Range", f"bytes={byte_range[0]}-{byte_range[1]}")
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read()
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)


def wanted_messages(idx_text, kind):
    """[(start_byte, end_byte_or_None, lead_hour)] for the messages we keep."""
    lines = [line.split(":") for line in idx_text.strip().splitlines()]
    offsets = [int(line[1]) for line in lines]
    keep = []
    for n, line in enumerate(lines):
        end = offsets[n + 1] - 1 if n + 1 < len(lines) else None
        if kind == "apcp":
            m = re.match(r"(\d+)-(\d+) hour acc fcst", line[5])
            # only the 6-hour windows, which sum cleanly into days
            if m and int(m.group(2)) - int(m.group(1)) == 6 and int(m.group(2)) <= MAX_HOUR:
                keep.append((offsets[n], end, int(m.group(2))))
        else:
            m = re.match(r"(\d+) hour fcst", line[5])
            if m and int(m.group(1)) % 6 == 0 and 0 < int(m.group(1)) <= MAX_HOUR:
                keep.append((offsets[n], end, int(m.group(1))))
    return keep


def nearest_values(msg_bytes, points):
    gid = eccodes.codes_new_from_message(msg_bytes)
    try:
        ni = eccodes.codes_get(gid, "Ni")
        lat0 = eccodes.codes_get(gid, "latitudeOfFirstGridPointInDegrees")
        lon0 = eccodes.codes_get(gid, "longitudeOfFirstGridPointInDegrees")
        dlat = eccodes.codes_get(gid, "jDirectionIncrementInDegrees")
        dlon = eccodes.codes_get(gid, "iDirectionIncrementInDegrees")
        values = eccodes.codes_get_values(gid)
    finally:
        eccodes.codes_release(gid)
    out = []
    for lat, lon in points:
        i = int(round((lat0 - lat) / dlat))
        j = int(round(((lon % 360) - lon0) / dlon))
        out.append(float(values[i * ni + j]))
    return out


def process_init(init, out_dir):
    path = os.path.join(out_dir, f"{init}.csv")
    if os.path.exists(path):
        return init, "skip"
    sids, points = list(TARGET_STATIONS), list(TARGET_STATIONS.values())
    rows = []
    for kind, name in (("apcp", "apcp_sfc"), ("tmp", "tmp_2m")):
        url = f"{BASE}/{init[:4]}/{init}/c00/Days%3A1-10/{name}_{init}_c00.grib2"
        idx = get(url + ".idx").decode()
        for start, end, hour in wanted_messages(idx, kind):
            end = end if end is not None else start + 5_000_000  # last message: over-read is harmless
            for sid, v in zip(sids, nearest_values(get(url, (start, end)), points)):
                rows.append((init, sid, kind, hour, v))
    tmp = path + ".part"
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["init", "station_id", "var", "lead_hour", "value"])
        writer.writerows(rows)
    os.replace(tmp, path)  # atomic: a crash never leaves a half-written init marked done
    return init, f"ok ({len(rows)} values)"


def init_dates(every):
    out, d = [], date(2000, 1, 1)
    while d <= date(2019, 12, 31):
        if d.month in (11, 12, 1, 2, 3) and (d - date(2000, 1, 1)).days % every == 0:
            out.append(d.strftime("%Y%m%d") + "00")
        d += timedelta(days=1)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--every", type=int, default=3)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--test", action="store_true", help="fetch a single init and exit")
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    if args.test:
        print(*process_init("2010010100", args.out_dir))
        return

    todo = init_dates(args.every)
    print(f"{len(todo)} inits queued (every {args.every} days, Nov-Mar 2000-2019)", flush=True)
    failed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_init, init, args.out_dir): init for init in todo}
        for n, fut in enumerate(as_completed(futures), 1):
            try:
                init, status = fut.result()
            except Exception as e:
                init, status, failed = futures[fut], f"FAILED: {e}", failed + 1
            print(f"[{n}/{len(todo)}] {init} {status}", flush=True)
    print(f"done, {failed} failed (rerun to retry them)")


if __name__ == "__main__":
    main()
