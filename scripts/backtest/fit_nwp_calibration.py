#!/usr/bin/env python3
"""
Fit the per-lead calibration used by NwpSnowfallForecast, and report its
verified skill.

Inputs:
  - archived NWP forecasts at each lead from Open-Meteo's previous-runs API
    (snowfall_previous_dayN; archive starts winter 2023-24), for the target
    stations, cached under --cache-dir
  - measured snowfall from scripts/collect/build_coop_truth.py (--truth)

For each lead k:
  1. Sum forecast snowfall over the same 24h-ending-7AM windows production
     uses (NwpSnowfallForecast.sum_period), averaged across target stations.
  2. Label = measured 3-station mean >= 5mm (>= 2 stations reporting, QC-suspect
     days excluded).
  3. Leave-one-winter-out CV over two candidate calibrations --
     logistic(log1p(mm)) and logistic(log1p(mm) + logit(climatology)) --
     each with the climatology blend weight w that minimizes CV Brier score.
     The lower-Brier candidate wins. The climatology term keeps a zero-snow
     forecast near zero in months that rarely snow.
  4. Final coefficients refit on all winters.

Climatology = smoothed day-of-year rate of measurable snow from measured data
before the NWP archive begins (no overlap with the CV years).

Usage:
    python scripts/backtest/fit_nwp_calibration.py --truth coop_truth.csv --cache-dir nwp_cache
"""

import argparse
import json
import os
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score

from snowforecast.engines.nwp_snowfall_forecast import (
    CALIBRATION_PATH, MEASURABLE_MM, TARGET_STATIONS, sum_period,
)

PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
FIRST_ARCHIVE_WINTER = 2023  # previous-runs has no snowfall before Nov 2023
LEADS = range(1, 8)
BLEND_GRID = np.round(np.arange(0, 1.0001, 0.05), 2)


def fetch_previous_runs(sid, lat, lon, winter, cache_dir):
    path = os.path.join(cache_dir, f"prev_{sid}_{winter}-11-01.json")
    if not os.path.exists(path):
        # one day either side so windows at the season edges are complete
        params = {"latitude": lat, "longitude": lon, "timezone": "UTC",
                  "hourly": ",".join(f"snowfall_previous_day{k}" for k in LEADS),
                  "start_date": f"{winter}-10-31", "end_date": f"{winter + 1}-04-01"}
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
    """DataFrame indexed by period date, one column per lead: 3-station mean forecast mm."""
    per_station = []
    for sid, (lat, lon) in TARGET_STATIONS.items():
        frames = []
        for w in winters:
            h = pd.DataFrame(fetch_previous_runs(sid, lat, lon, w, cache_dir))
            h["time"] = pd.to_datetime(h["time"]).dt.tz_localize("UTC")
            frames.append(h.set_index("time"))
        per_station.append(pd.concat(frames).groupby(level=0).first())
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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--truth", required=True, help="CSV from scripts/collect/build_coop_truth.py")
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--out", default=str(CALIBRATION_PATH))
    parser.add_argument("--last-winter", type=int, default=2025, help="start year of the last complete winter")
    args = parser.parse_args()
    os.makedirs(args.cache_dir, exist_ok=True)

    winters = list(range(FIRST_ARCHIVE_WINTER, args.last_winter + 1))
    label = measured_labels(args.truth)
    clim, clim_start, clim_end = climatology(label)
    fc = forecast_windows(winters, args.cache_dir)
    winter_of = lambda d: d.year if d.month >= 7 else d.year - 1  # noqa: E731

    print(f"winters {winters[0]}-{winters[-1] + 1}, climatology from {clim_start}..{clim_end}")
    print(f"{'lead':>4}{'n':>6}{'CV AUC':>9}{'features':>11}{'blend w':>9}{'CV skill vs clim':>18}")

    def features(frame, with_clim):
        cols = [np.log1p(frame.f.values)]
        if with_clim:
            cols.append(np.log(np.clip(frame.clim, 1e-3, 1 - 1e-3) / (1 - np.clip(frame.clim, 1e-3, 1 - 1e-3))))
        return np.column_stack(cols)

    leads_out = {}
    for k in LEADS:
        if k not in fc:
            print(f"{k:>4}  no archived forecasts at this lead -- engine publishes climatology for it")
            continue
        df = pd.DataFrame({"f": fc[k], "y": label.reindex(pd.to_datetime(fc.index)).values},
                          index=pd.to_datetime(fc.index)).dropna()
        df["winter"] = [winter_of(d) for d in df.index]
        df["clim"] = clim[df.index.dayofyear - 1]

        # Candidate calibrations, each scored by leave-one-winter-out CV with its best blend weight
        best = None
        for with_clim in (False, True):
            cv = []
            for held in df.winter.unique():
                train, test = df[df.winter != held], df[df.winter == held]
                if train.y.nunique() < 2:
                    continue
                m = LogisticRegression().fit(features(train, with_clim), train.y)
                cv.append(test.assign(p=m.predict_proba(features(test, with_clim))[:, 1]))
            cv = pd.concat(cv)
            brier = {w: brier_score_loss(cv.y, w * cv.p + (1 - w) * cv.clim) for w in BLEND_GRID}
            w = min(brier, key=brier.get)
            if best is None or brier[w] < best["brier"]:
                best = {"with_clim": with_clim, "w": w, "brier": brier[w], "cv": cv}

        final = LogisticRegression().fit(features(df, best["with_clim"]), df.y)
        skill = 1 - best["brier"] / brier_score_loss(best["cv"].y, best["cv"].clim)
        leads_out[str(k)] = {
            "intercept": round(float(final.intercept_[0]), 5),
            "slope": round(float(final.coef_[0][0]), 5),
            "clim_coef": round(float(final.coef_[0][1]), 5) if best["with_clim"] else 0.0,
            "blend_weight": float(best["w"]),
            "cv_auc": round(float(roc_auc_score(best["cv"].y, best["cv"].f)), 3),
            "cv_brier_skill": round(float(skill), 3),
            "n_days": int(len(df)),
        }
        print(f"{k:>4}{len(df):>6}{leads_out[str(k)]['cv_auc']:>9.3f}"
              f"{'mm+clim' if best['with_clim'] else 'mm':>11}{best['w']:>9.2f}{skill:>17.1%}")

    out = {
        "generated_at": datetime.now(ZoneInfo("America/Chicago")).isoformat(timespec="seconds"),
        "description": ("Per-lead logistic calibration of 3-station mean NWP snowfall (log1p mm, "
                        "optionally + logit(climatology) when CV favors it) "
                        "to P(measured 3-station mean >= 5mm) over the 24h ending 7 AM local, "
                        "blended with day-of-year climatology by leave-one-winter-out CV."),
        "nwp_source": "Open-Meteo previous-runs API (best_match)",
        "truth_source": "NWS COOP / CoCoRaHS via RCC-ACIS (scripts/collect/build_coop_truth.py)",
        "cv_winters": [f"{w}-{w + 1}" for w in winters],
        # the engine publishes climatology outside these months (coefficients were never fit there)
        "calibrated_months": sorted({d.month for d in fc.index}),
        "climatology_period": [str(clim_start), str(clim_end)],
        "leads": leads_out,
        "climatology": [round(float(x), 4) for x in clim],
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
