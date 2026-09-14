#!/usr/bin/env python3
"""
Fit predictor weights empirically against historical data, for comparison
against EnhancedRegionalForecastSystem's hand-tuned constants.

The canonical engine (src/snowforecast/engines/enhanced_regional_forecast_system.py)
hard-codes weights like winnipeg_mb=0.50, thunder_bay_on=0.468, duluth_mn=0.35 --
these read as manually assigned, not fit against outcomes. This computes, for
each of the engine's actual 9 predictor stations, the real Pearson correlation
against the target at a range of lags, and derives a weight from correlation
strength -- directly comparable to the current hand-tuned values.

Target = average daily snowfall across the 3 "Primary forecast target"
stations (phelps_wi, land_o_lakes_wi, eagle_river_wi). Uses only the
canonical land_o_lakes_wi id -- the land_o'lakes_wi split (11,328 rows,
25% disagreeing with the canonical id) is a separate unresolved issue;
this fit is missing whatever additional signal sits under that variant
until that's fixed.

This is ANALYSIS ONLY -- it reports fitted vs. current weights side by
side, it does not modify the engine. Swapping in fitted weights is a
separate step that should happen only after backtesting confirms it
actually improves skill, not just because correlation looks stronger.

Usage:
    python scripts/backtest/fit_predictor_weights.py
    python scripts/backtest/fit_predictor_weights.py --max-lag 10
"""

import argparse

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

from snowforecast.storage.db import get_engine

TARGET_STATIONS = ("phelps_wi", "land_o_lakes_wi", "eagle_river_wi")

# From EnhancedRegionalForecastSystem -- the current hand-tuned constants
CURRENT_PREDICTORS = {
    "sapporo_japan": {"lags": [6], "weight": 0.120, "kind": "global"},
    "chamonix_france": {"lags": [5], "weight": 0.115, "kind": "global"},
    "irkutsk_russia": {"lags": [7], "weight": 0.074, "kind": "global"},
    "winnipeg_mb": {"lags": [0, 1, 2], "weight": 0.50, "kind": "regional"},
    "thunder_bay_on": {"lags": [0, 1], "weight": 0.468, "kind": "regional"},
    "duluth_mn": {"lags": [0, 1, 2], "weight": 0.35, "kind": "regional"},
    "marquette_mi": {"lags": [0, 1, 2], "weight": 0.35, "kind": "regional"},
    "green_bay_wi": {"lags": [0, 1], "weight": 0.30, "kind": "regional"},
    "iron_mountain_mi": {"lags": [0, 1], "weight": 0.25, "kind": "regional"},
}


def get_daily_series(engine, station_id: str) -> pd.Series:
    """Daily snowfall_mm for one station, indexed by date, NaN-filled to 0."""
    query = text(
        "SELECT date, snowfall_mm FROM snowfall_daily WHERE station_id = :sid ORDER BY date"
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"sid": station_id}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["date", "snowfall_mm"])
    # Normalize any mixed date-string formats (see eagle_river_wi bug) so
    # this doesn't silently double-count or drop rows for any station.
    df["date"] = df["date"].str.slice(0, 10)
    df = df.groupby("date")["snowfall_mm"].mean()
    df.index = pd.to_datetime(df.index)
    return df.fillna(0.0)


def get_target_series(engine) -> pd.Series:
    series = [get_daily_series(engine, sid) for sid in TARGET_STATIONS]
    combined = pd.concat(series, axis=1)
    return combined.mean(axis=1, skipna=True)


def best_lag_correlation(predictor: pd.Series, target: pd.Series, max_lag: int, min_lag: int = 1):
    """Search lags min_lag..max_lag, return (best_lag, correlation, p_value, n)
    for the lag with the strongest |correlation|. predictor value on day D is
    tested against target value on day D+lag (predictor leads target).

    min_lag defaults to 1, not 0: lag=0 is same-day correlation, which
    mostly reflects nearby stations getting hit by the same storm
    simultaneously, not a genuine advance-warning signal -- a forecast
    issued using prior days' predictor data can't use a same-day value
    that doesn't exist yet at issuance time.
    """
    best = None
    for lag in range(min_lag, max_lag + 1):
        shifted_target = target.shift(-lag)
        aligned = pd.concat([predictor, shifted_target], axis=1, join="inner").dropna()
        if len(aligned) < 30:
            continue
        r, p = stats.pearsonr(aligned.iloc[:, 0], aligned.iloc[:, 1])
        if best is None or abs(r) > abs(best[1]):
            best = (lag, r, p, len(aligned))
    return best


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-lag", type=int, default=10)
    parser.add_argument("--min-lag", type=int, default=1, help="Minimum lag to consider (0 = same-day, excluded by default -- see best_lag_correlation docstring)")
    args = parser.parse_args()

    engine = get_engine()
    target = get_target_series(engine)
    print(f"Target series: {len(target)} days, {target.index.min()} to {target.index.max()}")

    fitted = {}
    for station_id in CURRENT_PREDICTORS:
        predictor = get_daily_series(engine, station_id)
        if predictor.empty:
            print(f"\n{station_id}: NO DATA -- skipping")
            continue
        result = best_lag_correlation(predictor, target, args.max_lag, args.min_lag)
        if result is None:
            print(f"\n{station_id}: insufficient overlapping data -- skipping")
            continue
        lag, r, p, n = result
        fitted[station_id] = {"lag": lag, "correlation": r, "p_value": p, "n": n}

    total_weight = sum(abs(v["correlation"]) for v in fitted.values())

    print("\n" + "=" * 100)
    print(f"{'station':<20} {'current_weight':>15} {'current_lag':>12} | {'fitted_weight':>15} {'fitted_lag':>11} {'r':>7} {'p':>10} {'n':>6}")
    print("-" * 100)
    for station_id, current in CURRENT_PREDICTORS.items():
        if station_id not in fitted:
            print(f"{station_id:<20} {current['weight']:>15.3f} {str(current['lags']):>12} | {'--':>15} {'--':>11}")
            continue
        f = fitted[station_id]
        fitted_weight = abs(f["correlation"]) / total_weight if total_weight else 0.0
        sig = "*" if f["p_value"] < 0.05 else " "
        print(
            f"{station_id:<20} {current['weight']:>15.3f} {str(current['lags']):>12} | "
            f"{fitted_weight:>15.3f} {f['lag']:>11} {f['correlation']:>6.3f}{sig} {f['p_value']:>10.4f} {f['n']:>6}"
        )
    print("\n* = statistically significant (p < 0.05)")
    print("\nThis is analysis only -- current weights in the engine are unchanged.")


if __name__ == "__main__":
    main()
