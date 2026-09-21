#!/usr/bin/env python3
"""
Fit the per-lead calibration used by NwpSnowfallForecast, and report its
verified skill.

Inputs:
  - archived Open-Meteo forecasts at each lead (previous-runs API,
    snowfall_previous_dayN; archive starts winter 2023-24) for the target
    stations, cached under --cache-dir
  - measured snowfall from scripts/collect/build_coop_truth.py (--truth)
  - optional: GEFSv12 reforecast extractions (2000-2019) from
    scripts/collect/fetch_gefs_reforecast.py (--gefs-dir)

For each lead k (forecast = 3-station mean snowfall over the 24h ending 7 AM
local, via the engine's own sum_period; label = measured 3-station mean >= 5mm):

  forecast   mean over MODELS (best_match, jma_seamless, icon_seamless) per
             station-hour, then the 3-station mean -- exactly as the engine
             computes it. Chosen by leave-one-winter-out review against
             measured snowfall: the 3-model mean beat best_match alone in
             every winter (mean skill days 1-6: 28% -> 36%), while extra
             variables (temperature, liquid precip), per-station max,
             earlier-lead agreement and gradient boosting added <=1 point
             or hurt. Only models with archive coverage matching best_match
             qualify; GEM and GFS have large holes and were excluded.
  slope      fit on Open-Meteo by default. --gefs-dir instead takes it from
             the GEFSv12 reforecast (20 winters) with the intercept refit on
             Open-Meteo. With the single-model input that matched
             Open-Meteo-only overall; with the 3-model input it is slightly
             worse at 5 of 6 leads (mean 34.8% vs 35.6%), because the GEFS
             slope describes a single derived quantity, not a model mean.
             The shipped calibration therefore uses Open-Meteo slopes.
  intercept  fit on Open-Meteo (refit with slope held fixed when GEFS is used).
  blend w    climatology blend weight minimizing Brier score on Open-Meteo.

Reported skill is leave-one-Open-Meteo-winter-out with every coefficient and
the blend weight tuned only on the training winters. The archive's winter
2023-24 is mostly missing for best_match (Nov-Dec empty, January half); JMA
covers it, so the model mean has ~427 usable days per lead rather than ~345.

Climatology = smoothed day-of-year rate of measurable snow, measured before the
Open-Meteo archive begins (no overlap with the scored winters).

ENSO factors: the same pre-archive winters are binned by the DJF Oceanic Niño
Index (CPC, --oni; downloaded into --cache-dir when absent) and each phase's
Nov-Mar rate of measurable snow is divided by the all-winter rate. Only phases
with an effect larger than winter-to-winter noise are shipped (>= 2 winters and
>= 15% from 1.0); the engine multiplies the climatology by the current
season's factor. On the 2000-2023 record that is strong El Niño only (2009-10,
2015-16: 0.79 at the targets, 0.78 across all 18 truth stations). Applying
it to the held-out strong-El Niño winter 2023-24 cut the climatology Brier score
by 11%; every other phase sat within 11% of 1.0 in both station sets.

Usage:
    python scripts/backtest/fit_nwp_calibration.py --truth coop_truth.csv --cache-dir nwp_cache \\
        --gefs-dir gefs_reforecast
"""

import argparse
import glob
import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score

from snowforecast.engines.nwp_snowfall_forecast import (
    CALIBRATION_PATH, MEASURABLE_MM, MODELS, TARGET_STATIONS, sum_period,
)
from snowforecast.enso import PHASES, phase_from_oni

PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
ONI_URL = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
ENSO_MIN_WINTERS = 2
ENSO_MIN_EFFECT = 0.15
FIRST_ARCHIVE_WINTER = 2023  # previous-runs has no snowfall before Nov 2023
LEADS = range(1, 8)
GEFS_LEADS = range(1, 7)  # 168h of reforecast covers windows through day 6
BLEND_GRID = np.round(np.arange(0, 1.0001, 0.05), 2)
WINTER_MONTHS = [11, 12, 1, 2, 3]


def fetch_previous_runs(sid, lat, lon, winter, cache_dir, model="best_match"):
    # best_match keeps the original cache name; other models are prefixed
    tag = "" if model == "best_match" else f"{model}_"
    path = os.path.join(cache_dir, f"prev_{tag}{sid}_{winter}-11-01.json")
    if not os.path.exists(path):
        # one day either side so windows at the season edges are complete
        params = {"latitude": lat, "longitude": lon, "timezone": "UTC",
                  "hourly": ",".join(f"snowfall_previous_day{k}" for k in LEADS),
                  "start_date": f"{winter}-10-31", "end_date": f"{winter + 1}-04-01"}
        if model != "best_match":
            params["models"] = model
        for attempt in range(10):
            r = requests.get(PREVIOUS_RUNS_URL, params=params, timeout=180)
            if r.status_code != 429:
                break
            time.sleep(60 * (attempt + 1))
        r.raise_for_status()
        with open(path, "w") as f:
            json.dump(r.json(), f)
        time.sleep(1.5)
    with open(path) as f:
        return json.load(f)["hourly"]


def forecast_windows(winters, cache_dir):
    """DataFrame indexed by period date, one column per lead: 3-station mean forecast mm,
    where each station's hourly value is the mean over MODELS (as the engine computes it)."""
    per_station = []
    for sid, (lat, lon) in TARGET_STATIONS.items():
        per_model = []
        for model in MODELS:
            frames = []
            for w in winters:
                h = pd.DataFrame(fetch_previous_runs(sid, lat, lon, w, cache_dir, model))
                h["time"] = pd.to_datetime(h["time"]).dt.tz_localize("UTC")
                frames.append(h.set_index("time"))
            per_model.append(pd.concat(frames).groupby(level=0).first())
        # mean over whichever models have a value that hour (matches the engine)
        per_station.append(pd.concat(per_model, keys=range(len(per_model))).groupby(level=1).mean())
    stacked = pd.concat(per_station, keys=range(len(per_station)))
    counts = stacked.groupby(level=1).count()
    hourly = stacked.groupby(level=1).mean() * 10.0  # cm -> mm
    hourly = hourly.where(counts == len(per_station))  # need every station

    times = list(hourly.index.to_pydatetime())
    rows = {}
    for w in winters:
        for day in pd.date_range(f"{w}-11-01", f"{w + 1}-03-31").date:
            rows[day] = {k: sum_period(times, hourly[col].tolist(), day)
                         for k in LEADS if (col := f"snowfall_previous_day{k}") in hourly}
    return pd.DataFrame.from_dict(rows, orient="index")


def gefs_windows(gefs_dir):
    """{lead: Series(date -> 3-station mean derived snowfall mm)} from GEFS reforecast extractions.

    GEFS has no snowfall field: each 6-hour precip accumulation is counted as snow
    by 2m temperature (all snow <= 0C, all rain >= 2C, linear between, using the
    window's mean start/end temperature), x10 for a 10:1 ratio. Periods are the
    12Z-12Z windows ending on each date -- the 6-hourly boundary nearest 7 AM local.
    """
    raw = pd.concat((pd.read_csv(f) for f in glob.glob(os.path.join(gefs_dir, "*.csv"))), ignore_index=True)
    raw = raw[raw.station_id.isin(TARGET_STATIONS)]
    raw["init"] = pd.to_datetime(raw["init"].astype(str), format="%Y%m%d%H")
    key = ["init", "station_id"]
    apcp = raw[raw["var"] == "apcp"].pivot_table(index=key, columns="lead_hour", values="value")
    temp_c = raw[raw["var"] == "tmp"].pivot_table(index=key, columns="lead_hour", values="value") - 273.15
    out = {}
    for k in GEFS_LEADS:
        snow = 0.0
        for h in (24 * k - 6, 24 * k, 24 * k + 6, 24 * k + 12):
            t = (temp_c[h - 6] + temp_c[h]) / 2 if (h - 6) in temp_c.columns else temp_c[h]
            snow = snow + apcp[h] * np.clip((2.0 - t) / 2.0, 0.0, 1.0) * 10.0
        per_init = snow.groupby(level="init").agg(["mean", "count"])
        s = per_init["mean"].where(per_init["count"] == len(TARGET_STATIONS)).dropna()
        s.index = (s.index + pd.Timedelta(days=k)).normalize()
        out[k] = s
    return out


def measured_labels(truth_path):
    t = pd.read_csv(truth_path, parse_dates=["d"])
    t = t[t.station_id.isin(TARGET_STATIONS) & ~t.suspect.astype(bool)]
    mm = t.pivot_table(index="d", columns="station_id", values="snow_mm")
    label = (mm.mean(axis=1) >= MEASURABLE_MM).astype(float).where(mm.notna().sum(axis=1) >= 2)
    return label.dropna()


def climatology(label):
    """366 smoothed day-of-year rates from measurements before the NWP archive."""
    hist = label[label.index < f"{FIRST_ARCHIVE_WINTER}-07-01"]
    rate = hist.groupby(hist.index.dayofyear).mean().reindex(range(1, 367)).interpolate().bfill().ffill()
    padded = np.r_[rate.values[-7:], rate.values, rate.values[:7]]
    return np.convolve(padded, np.ones(15) / 15, "valid"), hist.index.min().date(), hist.index.max().date()


def winter_oni(oni_path):
    """{winter start year: DJF ONI} from CPC's oni.ascii.txt (DJF of year Y is winter Y-1/Y)."""
    if not os.path.exists(oni_path):
        r = requests.get(ONI_URL, timeout=60)
        r.raise_for_status()
        with open(oni_path, "w") as f:
            f.write(r.text)
    oni = pd.read_fwf(oni_path)
    djf = oni[oni.SEAS == "DJF"].set_index("YR")["ANOM"]
    return {int(year) - 1: float(v) for year, v in djf.items()}


def enso_factors(label, oni_by_winter):
    """Per-phase ratio of the Nov-Mar measurable-snow rate to the all-winter rate,
    over the climatology period. Returns ({phase: factor} for shipped phases, table of all)."""
    hist = label[(label.index < f"{FIRST_ARCHIVE_WINTER}-07-01") & label.index.month.isin(WINTER_MONTHS)]
    winter = np.where(hist.index.month >= 7, hist.index.year, hist.index.year - 1)
    phase = pd.Series([phase_from_oni(oni_by_winter[w]) if w in oni_by_winter else None for w in winter],
                      index=hist.index)
    hist, winter, phase = hist[phase.notna()], winter[phase.notna().values], phase.dropna()
    overall = hist.mean()
    table = {p: {"factor": round(float(hist[phase == p].mean() / overall), 3) if (phase == p).any() else None,
                 "winters": int(len(set(winter[(phase == p).values])))} for p in PHASES}
    shipped = {p: v["factor"] for p, v in table.items()
               if v["factor"] is not None and v["winters"] >= ENSO_MIN_WINTERS
               and abs(v["factor"] - 1.0) >= ENSO_MIN_EFFECT}
    return shipped, table


def labeled_frame(series, label, clim):
    # clip at 0 to match the engine: some models emit tiny negative snowfall
    d = pd.DataFrame({"f": series.clip(lower=0), "y": label.reindex(series.index)}).dropna()
    d = d[d.index.month.isin(WINTER_MONTHS)]
    d["winter"] = np.where(d.index.month >= 7, d.index.year, d.index.year - 1)
    d["clim"] = clim[d.index.dayofyear - 1]
    return d


def fit_intercept(x, y, slope, iterations=50):
    """Maximum-likelihood logistic intercept with the slope held fixed (Newton's method)."""
    b = np.log(max(y.mean(), 1e-3) / max(1 - y.mean(), 1e-3))
    for _ in range(iterations):
        p = 1 / (1 + np.exp(-(b + slope * x)))
        step = np.sum(y - p) / max(np.sum(p * (1 - p)), 1e-9)
        b += step
        if abs(step) < 1e-8:
            break
    return b


def calibrate(train, slope):
    """(intercept, slope, blend_weight) tuned on `train` only. slope=None fits it here too."""
    x, y = np.log1p(train.f.values), train.y.values
    if slope is None:
        m = LogisticRegression().fit(x.reshape(-1, 1), y)
        intercept, slope = m.intercept_[0], m.coef_[0][0]
    else:
        intercept = fit_intercept(x, y, slope)
    p = 1 / (1 + np.exp(-(intercept + slope * x)))
    w = min(BLEND_GRID, key=lambda w: brier_score_loss(y, w * p + (1 - w) * train.clim))
    return intercept, slope, w


def predict(frame, intercept, slope, w):
    p = 1 / (1 + np.exp(-(intercept + slope * np.log1p(frame.f.values))))
    return w * p + (1 - w) * frame.clim.values


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--truth", required=True, help="CSV from scripts/collect/build_coop_truth.py")
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--gefs-dir", help="GEFS reforecast extractions; if omitted, slopes are fit on Open-Meteo")
    parser.add_argument("--oni", help="CPC oni.ascii.txt (default: <cache-dir>/oni.ascii.txt, downloaded if absent)")
    parser.add_argument("--out", default=str(CALIBRATION_PATH))
    parser.add_argument("--last-winter", type=int, default=2025, help="start year of the last complete winter")
    args = parser.parse_args()
    os.makedirs(args.cache_dir, exist_ok=True)

    winters = list(range(FIRST_ARCHIVE_WINTER, args.last_winter + 1))
    label = measured_labels(args.truth)
    clim, clim_start, clim_end = climatology(label)
    enso_shipped, enso_table = enso_factors(label, winter_oni(args.oni or os.path.join(args.cache_dir, "oni.ascii.txt")))
    om = forecast_windows(winters, args.cache_dir)
    om.index = pd.to_datetime(om.index)
    gefs = gefs_windows(args.gefs_dir) if args.gefs_dir else {}

    print(f"Open-Meteo winters {winters[0]}-{winters[-1] + 1}; climatology {clim_start}..{clim_end}; "
          f"GEFS {'on' if gefs else 'off'}")
    print("ENSO climatology factors (Nov-Mar rate / all-winter rate, climatology period):")
    for p, v in enso_table.items():
        print(f"  {p:>15}: {v['factor'] if v['factor'] is not None else 'n/a':>6} over {v['winters']} winters"
              f"{'  -> shipped' if p in enso_shipped else ''}")
    print(f"{'lead':>4}{'n OM':>6}{'n GEFS':>8}{'slope from':>12}{'CV AUC':>8}{'blend w':>9}{'CV skill vs clim':>18}")
    leads_out = {}
    for k in LEADS:
        if k not in om:
            print(f"{k:>4}  no archived forecasts at this lead -- engine publishes climatology for it")
            continue
        d = labeled_frame(om[k], label, clim)
        g = labeled_frame(gefs[k], label, clim) if k in gefs else None
        slope = LogisticRegression().fit(np.log1p(g[["f"]].values), g.y).coef_[0][0] if g is not None else None

        # leave-one-winter-out: intercept + blend weight tuned on training winters only
        held_out = []
        for winter in sorted(d.winter.unique()):
            train, test = d[d.winter != winter], d[d.winter == winter]
            if train.y.nunique() < 2:
                continue
            held_out.append(test.assign(p=predict(test, *calibrate(train, slope))))
        cv = pd.concat(held_out)
        skill = 1 - brier_score_loss(cv.y, cv.p) / brier_score_loss(cv.y, cv.clim)

        intercept, final_slope, w = calibrate(d, slope)
        leads_out[str(k)] = {
            "intercept": round(float(intercept), 5),
            "slope": round(float(final_slope), 5),
            "slope_source": "gefs_reforecast" if g is not None else "open_meteo",
            "blend_weight": float(w),
            "cv_auc": round(float(roc_auc_score(cv.y, cv.f)), 3),
            "cv_brier_skill": round(float(skill), 3),
            "n_days": int(len(d)),
            "n_gefs_days": int(len(g)) if g is not None else 0,
        }
        print(f"{k:>4}{len(d):>6}{len(g) if g is not None else 0:>8}"
              f"{'GEFS' if g is not None else 'Open-Meteo':>12}{leads_out[str(k)]['cv_auc']:>8.3f}"
              f"{w:>9.2f}{skill:>17.1%}")

    out = {
        "generated_at": datetime.now(ZoneInfo("America/Chicago")).isoformat(timespec="seconds"),
        "description": ("Per-lead logistic calibration of 3-station mean NWP snowfall (log1p mm) to "
                        "P(measured 3-station mean >= 5mm) over the 24h ending 7 AM local, blended with "
                        "day-of-year climatology. Slope from GEFSv12 reforecast where available, "
                        "intercept and blend weight from Open-Meteo. Skill is leave-one-winter-out."),
        "nwp_source": f"Open-Meteo previous-runs API, mean of {', '.join(MODELS)}",
        "models": list(MODELS),
        "slope_source": "NOAA GEFSv12 reforecast control member, 2000-2019" if gefs else "Open-Meteo",
        "truth_source": "NWS COOP / CoCoRaHS via RCC-ACIS (scripts/collect/build_coop_truth.py)",
        "cv_winters": [f"{w}-{w + 1}" for w in winters],
        # the engine publishes climatology outside these months (coefficients were never fit there)
        "calibrated_months": sorted(WINTER_MONTHS),
        "climatology_period": [str(clim_start), str(clim_end)],
        # the engine multiplies climatology (calibrated months only) by the current season's factor
        "enso_climatology_factors": enso_shipped,
        "enso_factor_note": (f"Phase = DJF ONI bin (snowforecast.enso). Shipped when >= {ENSO_MIN_WINTERS} winters "
                             f"and >= {ENSO_MIN_EFFECT:.0%} from 1.0; others measured within noise of 1.0: "
                             + ", ".join(f"{p} {v['factor']} ({v['winters']}w)" for p, v in enso_table.items()
                                         if p not in enso_shipped)),
        "leads": leads_out,
        "climatology": [round(float(x), 4) for x in clim],
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
