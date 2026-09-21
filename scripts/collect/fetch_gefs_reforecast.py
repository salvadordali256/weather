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

--members and --max-hour extend the pull to the perturbed members (p01-p04;
the reforecast has 5 members every day) and past day 7 (the Days:10-16 files
carry 240-384 h). Use a separate --out-dir for a different member/hour set:
existing inits are skipped by file name, not by content.

Output columns: init, member, station_id, var (apcp mm | tmp K), lead_hour, value
(files from before the member column exist are control-member only).

Usage:
    python scripts/collect/fetch_gefs_reforecast.py --out-dir gefs_reforecast --test
    python scripts/collect/fetch_gefs_reforecast.py --out-dir gefs_reforecast --every 3
    python scripts/collect/fetch_gefs_reforecast.py --out-dir gefs_ens --every 6 \\
        --members c00,p01,p02,p03,p04 --max-hour 384
"""

import argparse
import csv
import os
import re
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import eccodes
import requests

from snowforecast.engines.nwp_snowfall_forecast import TARGET_STATIONS

BASE = "https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast"
MAX_HOUR = 168
# (path segments to try, hours covered]. Wednesday inits run 11 members to day 35
# and keep days 10+ under Days:10-35 instead of Days:10-16.
DAY_RANGES = ((("Days%3A1-10",), 0, 240), (("Days%3A10-16", "Days%3A10-35"), 240, 384))
_local = threading.local()


def get(url, byte_range=None, tries=5):
    """GET (optionally a byte range) over a per-thread keep-alive session; a 404 is
    raised at once (callers use it to fall back between folder layouts)."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    headers = {"Range": f"bytes={byte_range[0]}-{byte_range[1]}"} if byte_range else {}
    for attempt in range(tries):
        try:
            r = _local.session.get(url, headers=headers, timeout=120)
            if r.status_code == 404:
                raise urllib.error.HTTPError(url, 404, "Not Found", None, None)
            r.raise_for_status()
            return r.content
        except urllib.error.HTTPError:
            raise
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)


def wanted_messages(idx_text, kind, lo=0, hi=MAX_HOUR):
    """[(start_byte, end_byte_or_None, lead_hour)] for the messages we keep: 6-hourly
    values with lo < lead_hour <= hi."""
    lines = [line.split(":") for line in idx_text.strip().splitlines()]
    offsets = [int(line[1]) for line in lines]
    keep = []
    for n, line in enumerate(lines):
        end = offsets[n + 1] - 1 if n + 1 < len(lines) else None
        if kind == "apcp":
            m = re.match(r"(\d+)-(\d+) hour acc fcst", line[5])
            # only the 6-hour windows, which sum cleanly into days
            if m and int(m.group(2)) - int(m.group(1)) == 6 and lo < int(m.group(2)) <= hi:
                keep.append((offsets[n], end, int(m.group(2))))
        else:
            m = re.match(r"(\d+) hour fcst", line[5])
            if m and int(m.group(1)) % 6 == 0 and lo < int(m.group(1)) <= hi:
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


def process_init(init, out_dir, members=("c00",), max_hour=MAX_HOUR):
    path = os.path.join(out_dir, f"{init}.csv")
    if os.path.exists(path):
        return init, "skip"
    sids, points = list(TARGET_STATIONS), list(TARGET_STATIONS.values())
    rows = []
    for member in members:
        for kind, name in (("apcp", "apcp_sfc"), ("tmp", "tmp_2m")):
            for segments, lo, hi in DAY_RANGES:
                if lo >= max_hour:
                    break
                for n, segment in enumerate(segments):
                    url = f"{BASE}/{init[:4]}/{init}/{member}/{segment}/{name}_{init}_{member}.grib2"
                    try:
                        idx = get(url + ".idx").decode()
                        break
                    except urllib.error.HTTPError as e:
                        if e.code != 404 or n == len(segments) - 1:
                            raise
                for start, end, hour in wanted_messages(idx, kind, lo, min(hi, max_hour)):
                    end = end if end is not None else start + 5_000_000  # last message: over-read is harmless
                    for sid, v in zip(sids, nearest_values(get(url, (start, end)), points)):
                        rows.append((init, member, sid, kind, hour, v))
    tmp = path + ".part"
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["init", "member", "station_id", "var", "lead_hour", "value"])
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
    parser.add_argument("--members", default="c00", help="comma-separated: c00,p01,p02,p03,p04")
    parser.add_argument("--max-hour", type=int, default=MAX_HOUR, help="last lead hour to keep (<= 384)")
    parser.add_argument("--test", action="store_true", help="fetch a single init and exit")
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    members = tuple(args.members.split(","))

    if args.test:
        t0 = time.time()
        print(*process_init("2010010100", args.out_dir, members, args.max_hour), f"in {time.time() - t0:.0f}s")
        return

    todo = init_dates(args.every)
    print(f"{len(todo)} inits queued (every {args.every} days, Nov-Mar 2000-2019)", flush=True)
    failed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_init, init, args.out_dir, members, args.max_hour): init for init in todo}
        for n, fut in enumerate(as_completed(futures), 1):
            try:
                init, status = fut.result()
            except Exception as e:
                init, status, failed = futures[fut], f"FAILED: {e}", failed + 1
            print(f"[{n}/{len(todo)}] {init} {status}", flush=True)
    print(f"done, {failed} failed (rerun to retry them)")


if __name__ == "__main__":
    main()
