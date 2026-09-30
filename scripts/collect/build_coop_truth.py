#!/usr/bin/env python3
"""
Build a measured-snowfall answer key from NWS COOP / CoCoRaHS via RCC-ACIS.

The open-meteo "observed" snowfall for the target stations is ERA5
reanalysis, not a measurement, and disagrees with co-located COOP snow-board
readings on about a third of snow days. Any forecast verification or
calibration should use this file instead.

For each station:
  - primary   = nearest COOP station with >= 15 years of record overlap,
                within --radius-km (falls back to the longest-record reporter)
  - neighbors = up to 7 other snowfall reporters in range, used only for QC
QC: a day is flagged `suspect` when >= 3 neighbors reported and the primary
disagrees with ALL of them about whether >= 5mm fell. Requiring 3 (not 2)
keeps real lake-effect events, which have km-scale gradients.

A COOP reading dated D covers roughly 7 AM D-1 to 7 AM D local. Verify
forecasts over that window, and never train on inputs overlapping it.

No API key needed. Output columns: station_id, d, snow_mm, suspect, source

Usage:
    python scripts/collect/build_coop_truth.py --out coop_truth.csv
    python scripts/collect/build_coop_truth.py --out coop_truth.csv --start 2000-01-01
"""

import argparse
import json
import math
import time
import urllib.request

import pandas as pd

from snowforecast.engines.nwp_snowfall_forecast import MEASURABLE_MM, TARGET_STATIONS

ACIS = "https://data.rcc-acis.org"


def km(la1, lo1, la2, lo2):
    p = math.pi / 180
    a = (math.sin((la2 - la1) * p / 2) ** 2
         + math.cos(la1 * p) * math.cos(la2 * p) * math.sin((lo2 - lo1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


def acis(path, payload=None):
    for attempt in range(5):
        try:
            if payload is None:
                req = f"{ACIS}/{path}"
            else:
                req = urllib.request.Request(f"{ACIS}/{path}", json.dumps(payload).encode(),
                                             {"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except Exception:
            if attempt == 4:
                raise
            time.sleep(3 * (attempt + 1))


def snowfall_series(uid, start, end):
    r = acis("StnData", {"uid": uid, "sdate": start, "edate": end, "elems": [{"name": "snow"}]})
    df = pd.DataFrame(r.get("data", []), columns=["d", "snow"])
    if df.empty:
        return pd.Series(dtype=float)
    mm = pd.to_numeric(df["snow"].replace({"T": "0"}), errors="coerce") * 25.4  # inches -> mm, trace = 0
    return pd.Series(mm.values, index=pd.to_datetime(df["d"]))


def build_station(sid, lat, lon, start, end, radius_km):
    bbox = f"{lon - 0.45},{lat - 0.3},{lon + 0.45},{lat + 0.3}"
    meta = acis(f"StnMeta?bbox={bbox}&elems=snow&meta=uid,name,sids,ll,valid_daterange").get("meta", [])
    cands = []
    for m in meta:
        vr = m.get("valid_daterange", [[]])[0]
        if not vr or vr[1] < end[:4] + "-01-01":
            continue
        dist = km(lat, lon, m["ll"][1], m["ll"][0])
        if dist > radius_km:
            continue
        years = (pd.Timestamp(vr[1]) - pd.Timestamp(max(vr[0], start))).days / 365.25
        is_coop = any(x.split()[1] == "2" for x in m["sids"])
        cands.append({"uid": m["uid"], "name": m["name"], "dist": dist, "coop": is_coop, "years": years})
    if not cands:
        print(f"{sid}: no snowfall reporter within {radius_km} km -- skipped")
        return None
    cands.sort(key=lambda c: (0, c["dist"]) if (c["coop"] and c["years"] >= 15)
               else (1, -round(c["years"]), c["dist"]))
    primary, neighbors = cands[0], cands[1:8]

    series = snowfall_series(primary["uid"], start, end)
    nb = pd.DataFrame({n["name"]: snowfall_series(n["uid"], start, end) for n in neighbors})
    nb = nb.loc[:, nb.notna().sum() > 100]
    reporting = nb.reindex(series.index).notna().sum(axis=1)
    yes_count = (nb.reindex(series.index) >= MEASURABLE_MM).sum(axis=1)
    yes = series >= MEASURABLE_MM
    suspect = (reporting >= 3) & ((yes & (yes_count == 0)) | (~yes & (yes_count == reporting)))

    df = pd.DataFrame({"station_id": sid, "d": series.index, "snow_mm": series.values,
                       "suspect": suspect.values, "source": primary["name"]}).dropna(subset=["snow_mm"])
    winter = df[df.d.dt.month.isin([11, 12, 1, 2, 3])]
    print(f"{sid:<18} <- {primary['name']:<26} {'COOP' if primary['coop'] else 'other':<6}"
          f"{primary['dist']:>5.1f} km  neighbors={nb.shape[1]}  winter_days={len(winter)}  "
          f"suspect={100 * winter.suspect.mean():.1f}%")
    return df


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="CSV path to write")
    parser.add_argument("--start", default="2000-01-01")
    parser.add_argument("--end", default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    parser.add_argument("--radius-km", type=float, default=30.0)
    args = parser.parse_args()

    frames = [build_station(sid, lat, lon, args.start, args.end, args.radius_km)
              for sid, (lat, lon) in TARGET_STATIONS.items()]
    frames = [f for f in frames if f is not None]
    pd.concat(frames).to_csv(args.out, index=False)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
