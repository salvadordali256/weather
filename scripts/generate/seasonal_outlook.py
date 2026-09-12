#!/usr/bin/env python3
"""
Generate a climatological seasonal outlook for a target month.

This is NOT a day-specific forecast -- at 30-90+ day lead times, atmospheric
predictability has collapsed to noise for any model, including this one.
What this produces instead is a base-rate outlook: what this calendar month
has historically looked like across the years of station data on file, the
same category of product as NOAA CPC's seasonal outlooks (above/near/below
normal), not a specific-day prediction.

Usage:
    python scripts/generate/seasonal_outlook.py --month 2
    python scripts/generate/seasonal_outlook.py --month 2 --output forecast_output/feb_outlook.json
"""

import argparse
import json
import os
from datetime import datetime, timezone

from snowforecast.analysis.seasonal_climatology import compute_seasonal_outlook, MIN_YEARS_FOR_STABLE_TERCILES

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="1-12")
    parser.add_argument("--output", default="forecast_output/seasonal_outlook.json")
    args = parser.parse_args()

    outlook = compute_seasonal_outlook(args.month)
    month_name = MONTH_NAMES[args.month - 1]

    if len(outlook.years_used) < MIN_YEARS_FOR_STABLE_TERCILES:
        print(
            f"WARNING: only {len(outlook.years_used)} years of data for {month_name} -- "
            f"tercile split will be unstable. Recommend >= {MIN_YEARS_FOR_STABLE_TERCILES} years."
        )

    result = {
        "month": args.month,
        "month_name": month_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "station_ids": list(outlook.station_ids),
        "years_used": outlook.years_used,
        "num_years": len(outlook.years_used),
        "yearly_totals_mm": outlook.yearly_totals_mm,
        "climatology": {
            "mean_mm": round(outlook.mean_mm, 1),
            "median_mm": round(outlook.median_mm, 1),
            "std_mm": round(outlook.std_mm, 1),
            "min_mm": round(outlook.min_mm, 1),
            "max_mm": round(outlook.max_mm, 1),
        },
        "tercile_bounds_mm": {
            "below_normal_below": round(outlook.tercile_bounds_mm[0], 1),
            "above_normal_above": round(outlook.tercile_bounds_mm[1], 1),
        },
        "disclaimer": (
            "This is a historical base-rate outlook, not a day-specific forecast. "
            "It does not account for current-year signals (ENSO phase, current "
            "snowpack, etc.) -- it only reflects what this calendar month has "
            "looked like across the years of station data on file."
        ),
    }

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Seasonal outlook for {month_name} written to {args.output}")
    print(f"  {len(outlook.years_used)} years of data: {outlook.years_used}")
    print(
        f"  Mean: {outlook.mean_mm:.1f}mm | Median: {outlook.median_mm:.1f}mm | "
        f"Range: {outlook.min_mm:.1f}-{outlook.max_mm:.1f}mm"
    )


if __name__ == "__main__":
    main()
