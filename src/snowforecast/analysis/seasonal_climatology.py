"""
Seasonal climatology outlook.

Computes a historical base-rate outlook for a target calendar month (e.g.
"what does February usually look like"), from however many years of
snowfall_daily history are on file.

This is deliberately NOT a day-specific forecast. At 30-90+ day lead times,
atmospheric predictability has collapsed to noise for any model -- this
system's included -- since the short-lag teleconnection signals the
7-day engine relies on (Winnipeg lag 0-2 days, Sapporo lag 6 days, etc.)
have no physical basis that far out. What's honest at this range is a
climatological tendency: what has this month actually looked like across
the years on record, and where does a given outcome sit relative to that
history. That's the same category of product as NOAA CPC's seasonal
outlooks (above/near/below normal), not a specific-day prediction.

Does not condition on current-year signals (ENSO phase, current snowpack,
etc.) -- this is pure historical climatology. Conditioning on ENSO would
be a natural follow-up if/when an ENSO index is added as a data source.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from snowforecast.storage.db import get_engine

NORTHERN_WI_STATIONS: Tuple[str, ...] = ("phelps_wi", "land_o_lakes_wi", "eagle_river_wi")

MIN_YEARS_FOR_STABLE_TERCILES = 10


@dataclass
class SeasonalOutlook:
    month: int
    station_ids: Tuple[str, ...]
    years_used: list
    yearly_totals_mm: dict
    mean_mm: float
    median_mm: float
    std_mm: float
    min_mm: float
    max_mm: float
    tercile_bounds_mm: Tuple[float, float]


def compute_seasonal_outlook(
    month: int,
    station_ids: Tuple[str, ...] = NORTHERN_WI_STATIONS,
) -> SeasonalOutlook:
    """Historical climatology for `month` (1-12), averaged across `station_ids`.

    For each year with data, sums snowfall_mm across every day in that
    calendar month for each station, then averages across stations so the
    result is comparable to a single-station monthly total regardless of
    how many stations are included. Returns the distribution of those
    yearly totals plus a NOAA-CPC-style tercile split (below/near/above
    normal boundaries).

    Raises ValueError if there's no data at all for this month/station set.
    """
    if not 1 <= month <= 12:
        raise ValueError(f"month must be 1-12, got {month}")

    # Dates are stored as 'YYYY-MM-DD' TEXT (see storage/models.py) -- LIKE
    # with a literal month segment works identically on SQLite and Postgres,
    # unlike strftime() (SQLite-only) or EXTRACT() (Postgres-only).
    query = text(
        """
        SELECT station_id, date, snowfall_mm
        FROM snowfall_daily
        WHERE station_id IN :station_ids
          AND date LIKE :month_pattern
        """
    ).bindparams(bindparam("station_ids", expanding=True))

    df = pd.read_sql_query(
        query,
        get_engine(),
        params={"station_ids": list(station_ids), "month_pattern": f"____-{month:02d}-%"},
    )

    if df.empty:
        raise ValueError(
            f"No snowfall_daily rows found for month={month}, stations={station_ids}"
        )

    df["year"] = df["date"].str.slice(0, 4).astype(int)
    df["snowfall_mm"] = df["snowfall_mm"].fillna(0.0)

    # Sum per station per year, then average across stations for that year
    # so multi-station coverage doesn't inflate the total.
    per_station_year = df.groupby(["year", "station_id"])["snowfall_mm"].sum()
    yearly_totals = per_station_year.groupby("year").mean()

    values = yearly_totals.to_numpy()
    lower_tercile, upper_tercile = np.percentile(values, [33.33, 66.67])

    return SeasonalOutlook(
        month=month,
        station_ids=station_ids,
        years_used=sorted(int(y) for y in yearly_totals.index),
        yearly_totals_mm={int(y): float(v) for y, v in yearly_totals.items()},
        mean_mm=float(np.mean(values)),
        median_mm=float(np.median(values)),
        std_mm=float(np.std(values, ddof=1)) if len(values) > 1 else 0.0,
        min_mm=float(np.min(values)),
        max_mm=float(np.max(values)),
        tercile_bounds_mm=(float(lower_tercile), float(upper_tercile)),
    )


def classify_value(value_mm: float, outlook: SeasonalOutlook) -> str:
    """Classify a monthly total against this outlook's historical terciles."""
    lower, upper = outlook.tercile_bounds_mm
    if value_mm < lower:
        return "below normal"
    if value_mm > upper:
        return "above normal"
    return "near normal"
