#!/usr/bin/env python3
"""
Compare the engine's current hand-tuned predictor weights against the
empirically-fitted ones (lag >= 1 only, from fit_predictor_weights.py),
using a proper A/B backtest -- not the existing enhanced_system_backtesting.py,
which compares against hardcoded baseline numbers and only ever tests days
that already had snow (no false-positive measurement at all).

KNOWN LIMITATION -- do not use this alone to justify shipping weights. It
scores historical dates where every predictor's data already exists, which
production never has: the live job forecasts today+1..+7 from a DB holding
only past observations, so a predictor with lag L contributes only at lead
<= L+1. It also grades against open-meteo/ERA5 snowfall, which is reanalysis,
not measurement. The fitted weights "won" here but, under production data
availability, turn days 5-7 into a constant 10% (their global lags are too
short to reach any data). That is why they were reverted.

Reuses the REAL EnhancedRegionalForecastSystem class and its actual
generate_ensemble_forecast()/categorize_activity()/probability-banding
logic -- this only swaps the .global_predictors/.regional_predictors
dicts between runs, so both configurations go through identical scoring
code, not a reimplementation that could subtly diverge from production
behavior.

Both configurations are hardcoded explicitly below (ORIGINAL_* and
FITTED_*) rather than reading either one off the live class defaults. If
the engine's defaults ever equal one side (they briefly equaled FITTED_* on
this branch), comparing "current" against it silently scores a config
against itself and reports a meaningless zero difference. Freezing both
keeps this script answering the same question whatever the defaults are.

Evaluation set: a random sample of dates across the full record (not
filtered to snow days), so false positives on quiet days count against
a configuration, not just hits on active days. Actual outcome = whether
the target (avg of phelps_wi/land_o_lakes_wi/eagle_river_wi) had
measurable snow (>= LIGHT_THRESHOLD mm) on that date.

Metric: Brier score (mean squared error between forecast probability and
binary outcome) -- lower is better, and unlike a hit-rate threshold it
rewards genuine calibration, not just crossing an arbitrary bar.

Usage:
    python scripts/backtest/compare_current_vs_fitted_weights.py
    python scripts/backtest/compare_current_vs_fitted_weights.py --sample-size 500 --seed 42
"""

import argparse
import random
from datetime import datetime, timedelta

from sqlalchemy import bindparam, text

from snowforecast.engines.enhanced_regional_forecast_system import EnhancedRegionalForecastSystem
from snowforecast.storage.db import add_db_path_arg, describe_engine, get_engine

TARGET_STATIONS = ("phelps_wi", "land_o_lakes_wi", "eagle_river_wi")

# The original hand-tuned constants (pre-fit, frozen here for comparison --
# see module docstring for why this can't just read the class defaults).
ORIGINAL_GLOBAL_PREDICTORS = {
    "sapporo_japan": {"name": "Sapporo", "lag": 6, "weight": 0.120},
    "chamonix_france": {"name": "Chamonix", "lag": 5, "weight": 0.115},
    "irkutsk_russia": {"name": "Irkutsk", "lag": 7, "weight": 0.074},
}

ORIGINAL_REGIONAL_PREDICTORS = {
    "winnipeg_mb": {"name": "Winnipeg", "lags": [0, 1, 2], "weight": 0.50, "type": "clipper"},
    "thunder_bay_on": {"name": "Thunder Bay", "lags": [0, 1], "weight": 0.468, "type": "regional"},
    "duluth_mn": {"name": "Duluth", "lags": [0, 1, 2], "weight": 0.35, "type": "lake_effect"},
    "marquette_mi": {"name": "Marquette", "lags": [0, 1, 2], "weight": 0.35, "type": "lake_effect"},
    "green_bay_wi": {"name": "Green Bay", "lags": [0, 1], "weight": 0.30, "type": "regional"},
    "iron_mountain_mi": {"name": "Iron Mountain", "lags": [0, 1], "weight": 0.25, "type": "regional"},
}

# From fit_predictor_weights.py's real NAS run, lag >= 1 only (excludes
# same-day correlation, which isn't a usable advance-warning signal).
# 'name' and 'type' preserved unchanged from the current config -- those
# are physical/geographic labels, not something a correlation fit
# determines.
FITTED_GLOBAL_PREDICTORS = {
    "sapporo_japan": {"name": "Sapporo", "lag": 3, "weight": 0.092},
    "chamonix_france": {"name": "Chamonix", "lag": 1, "weight": 0.075},
    "irkutsk_russia": {"name": "Irkutsk", "lag": 3, "weight": 0.048},
}

FITTED_REGIONAL_PREDICTORS = {
    "winnipeg_mb": {"name": "Winnipeg", "lags": [1], "weight": 0.146, "type": "clipper"},
    "thunder_bay_on": {"name": "Thunder Bay", "lags": [2], "weight": 0.098, "type": "regional"},
    "duluth_mn": {"name": "Duluth", "lags": [1], "weight": 0.096, "type": "lake_effect"},
    "marquette_mi": {"name": "Marquette", "lags": [1], "weight": 0.127, "type": "lake_effect"},
    "green_bay_wi": {"name": "Green Bay", "lags": [1], "weight": 0.172, "type": "regional"},
    "iron_mountain_mi": {"name": "Iron Mountain", "lags": [1], "weight": 0.145, "type": "regional"},
}


def get_target_actuals(engine, dates):
    """date -> 1.0/0.0 for whether target snowed >= LIGHT_THRESHOLD (5mm) on that date."""
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    placeholders = ", ".join(f":d{i}" for i in range(len(date_strs)))
    query = text(
        f"""
        SELECT date, station_id, snowfall_mm
        FROM snowfall_daily
        WHERE station_id IN :stations AND date IN ({placeholders})
        """
    ).bindparams(bindparam("stations", expanding=True))
    params = {f"d{i}": d for i, d in enumerate(date_strs)}
    params["stations"] = list(TARGET_STATIONS)

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    by_date = {}
    for r in rows:
        by_date.setdefault(r.date, []).append(r.snowfall_mm or 0.0)

    outcomes = {}
    for d in date_strs:
        vals = by_date.get(d, [])
        avg_val = sum(vals) / len(vals) if vals else 0.0
        outcomes[d] = 1.0 if avg_val >= 5.0 else 0.0
    return outcomes


def brier_score(probs, outcomes):
    n = len(probs)
    return sum((p - o) ** 2 for p, o in zip(probs, outcomes)) / n if n else float("nan")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sample-size", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-year", type=int, default=1945)  # avoid lag edge effects near 1940 start
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument(
        "--season-only", action="store_true",
        help="Restrict sampled dates to Nov 1 - Mar 31 (the actual operating season). "
        "Uniform whole-year sampling makes most 'quiet days' trivial off-season dates "
        "with zero signal everywhere, diluting the comparison that actually matters.",
    )
    add_db_path_arg(parser)
    args = parser.parse_args()

    random.seed(args.seed)
    engine = get_engine(args.db_path)
    print(f"Target database: {describe_engine(engine)}")

    start = datetime(args.start_year, 1, 1)
    end = datetime(args.end_year, 12, 31)
    total_days = (end - start).days

    if args.season_only:
        sample_dates = []
        while len(sample_dates) < args.sample_size:
            d = start + timedelta(days=random.randint(0, total_days))
            if d.month in (11, 12, 1, 2, 3):
                sample_dates.append(d)
        sample_dates.sort()
        print(f"Season-only mode: restricting to Nov-Mar dates")
    else:
        sample_dates = sorted(start + timedelta(days=random.randint(0, total_days)) for _ in range(args.sample_size))

    print(f"Evaluating {len(sample_dates)} sampled dates, {sample_dates[0].date()} to {sample_dates[-1].date()}")

    actuals = get_target_actuals(engine, sample_dates)

    current_system = EnhancedRegionalForecastSystem(db_path=args.db_path)
    current_system.global_predictors = ORIGINAL_GLOBAL_PREDICTORS
    current_system.regional_predictors = ORIGINAL_REGIONAL_PREDICTORS

    fitted_system = EnhancedRegionalForecastSystem(db_path=args.db_path)
    fitted_system.global_predictors = FITTED_GLOBAL_PREDICTORS
    fitted_system.regional_predictors = FITTED_REGIONAL_PREDICTORS

    current_probs, fitted_probs, outcome_list = [], [], []
    current_hits = current_fp = fitted_hits = fitted_fp = 0
    n_active = n_quiet = 0

    for d in sample_dates:
        date_str = d.strftime("%Y-%m-%d")
        actual = actuals[date_str]
        outcome_list.append(actual)

        current_fc = current_system.generate_ensemble_forecast(d)
        fitted_fc = fitted_system.generate_ensemble_forecast(d)

        current_p = current_fc["probability"] / 100.0
        fitted_p = fitted_fc["probability"] / 100.0
        current_probs.append(current_p)
        fitted_probs.append(fitted_p)

        if actual == 1.0:
            n_active += 1
            if current_fc["probability"] >= 40:
                current_hits += 1
            if fitted_fc["probability"] >= 40:
                fitted_hits += 1
        else:
            n_quiet += 1
            if current_fc["probability"] >= 40:
                current_fp += 1
            if fitted_fc["probability"] >= 40:
                fitted_fp += 1

    print("\n" + "=" * 70)
    print(f"{'':<30} {'current (hand-tuned)':>20} {'fitted (lag>=1)':>18}")
    print("-" * 70)
    print(f"{'Brier score (lower=better)':<30} {brier_score(current_probs, outcome_list):>20.4f} {brier_score(fitted_probs, outcome_list):>18.4f}")
    print(f"{'Hit rate on active days':<30} {f'{current_hits}/{n_active} ({current_hits/n_active:.1%})' if n_active else 'n/a':>20} {f'{fitted_hits}/{n_active} ({fitted_hits/n_active:.1%})' if n_active else 'n/a':>18}")
    print(f"{'False-positive rate (quiet days)':<30} {f'{current_fp}/{n_quiet} ({current_fp/n_quiet:.1%})' if n_quiet else 'n/a':>20} {f'{fitted_fp}/{n_quiet} ({fitted_fp/n_quiet:.1%})' if n_quiet else 'n/a':>18}")
    print(f"{'n active / n quiet':<30} {f'{n_active} / {n_quiet}':>39}")
    print("=" * 70)


if __name__ == "__main__":
    main()
